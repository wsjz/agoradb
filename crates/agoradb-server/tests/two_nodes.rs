// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Nodes talking to each other over Arrow Flight (3.1a exit criteria):
//! a UCAN-gated federated query, and revocation cutting access.

mod common;

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_engine::{EngineError, QualifiedName, QueryEngine};
use agoradb_identity::ucan::{token_id, Capability, UcanBuilder, ABILITY_QUERY};
use agoradb_identity::{unix_now, unix_now_ms, AuthRequest, NodeIdentity};
use agoradb_network::protocol::{digest, QueryTicket, HEADER_AUTH, HEADER_UCAN, METHOD_DO_GET};
use agoradb_network::{install_crypto_provider, DidCertVerifier, FlightEngine, RemoteSpaceConfig};
use agoradb_node::{AgoraSession, EngineRegistry, NodeConfig, SessionConfig};
use arrow_flight::{FlightClient, Ticket};
use common::{ints, strings, Node};
use futures::TryStreamExt;
use iceberg::io::FileIO;

/// Node A owns `blog.posts` (analytical) and `blog.secrets`, grants the
/// DID `grantee` three of four columns of `posts` and, by row policy, only
/// published posts.
async fn seed_owner(a: &Node, grantee: &str) {
    for sql in [
        "CREATE SPACE blog".to_string(),
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR, draft BOOLEAN)".to_string(),
        "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello', false), (2, 'bob', 'World', false), \
         (3, 'alice', 'Again', false), (4, 'alice', 'Unfinished', true)"
            .to_string(),
        "CREATE TABLE blog.secrets (k VARCHAR, v VARCHAR)".to_string(),
        "INSERT INTO blog.secrets VALUES ('pin', '1234')".to_string(),
        format!("GRANT SELECT (id, author, title) ON blog.posts TO \"{grantee}\""),
        format!("CREATE POLICY published ON blog.posts TO \"{grantee}\" USING (NOT draft)"),
    ] {
        a.sql(&sql).await;
    }
}

/// Node B has a transactional `orders` Space of its own.
async fn seed_consumer(b: &Node) {
    for sql in [
        "CREATE SPACE orders WITH KIND = 'transactional'",
        "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount INTEGER)",
        "INSERT INTO orders.orders VALUES (10, 'alice', 5), (11, 'carol', 7), (12, 'bob', 3)",
    ] {
        b.sql(sql).await;
    }
}

const JOIN: &str = "SELECT o.id, o.customer, p.title \
     FROM orders.orders o JOIN pub.posts p ON p.author = o.customer \
     ORDER BY o.id, p.title";

#[tokio::test]
async fn ucan_gated_federated_query_and_revocation_cuts_access() {
    let a = Node::new(1).serve().await;
    let b = Node::new(2);
    seed_owner(&a, &b.did()).await;
    seed_consumer(&b).await;

    let shown = a.sql("SHOW IDENTITY").await;
    assert_eq!(strings(&shown, 0), vec![a.did()]);

    let (id, token) = a
        .issue(&format!(
            "ISSUE UCAN ON SPACE blog TO '{}' EXPIRES IN '1h'",
            b.did()
        ))
        .await;
    assert_eq!(id, token_id(&token));
    b.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{token}'",
        a.url()
    ))
    .await;

    // Local SQLite ⋈ remote Parquet, the remote side shipped to A.
    let result = b.sql(JOIN).await;
    assert_eq!(ints(&result, 0), vec![10, 10, 12]);
    assert_eq!(strings(&result, 1), vec!["alice", "alice", "bob"]);
    // The draft post is filtered on A by its row policy.
    assert_eq!(strings(&result, 2), vec!["Again", "Hello", "World"]);

    // B sees only the granted columns and relations.
    let all = b.sql("SELECT * FROM pub.posts ORDER BY id").await;
    let schema = all.schema().unwrap();
    let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(columns, vec!["id", "author", "title"]);
    assert_eq!(ints(&all, 0), vec![1, 2, 3]);
    let err = b.sql_err("SELECT * FROM pub.secrets").await;
    assert!(err.contains("not found"), "{err}");

    // Aggregates are pushed to the owner too.
    let plan = b
        .owner
        .explain("SELECT author, count(*) FROM pub.posts GROUP BY author")
        .await
        .unwrap();
    // One leaf, executed by A in A's namespace: `pub` is B's name for it.
    assert!(
        plan.contains(&format!("compute_context=flight:{}/blog", a.url())),
        "{plan}"
    );
    assert!(plan.contains("FROM blog.posts GROUP BY"), "{plan}");

    // B cannot write through a remote Space.
    let err = b
        .sql_err("INSERT INTO pub.posts VALUES (9, 'x', 'y')")
        .await;
    assert!(err.contains("read-only"), "{err}");

    // Revocation on A takes effect on B's next query.
    let affected = a.sql(&format!("REVOKE UCAN '{id}'")).await;
    assert_eq!(affected.rows_affected(), Some(1));
    let err = b.sql_err(JOIN).await;
    assert!(err.contains("permission denied"), "{err}");
    let ucans = a.sql("SHOW UCANS").await;
    assert_eq!(strings(&ucans, 0), vec![id.clone()]);
    assert_eq!(strings(&ucans, 4), vec!["true"]);

    // A fresh token restores access.
    let (_, fresh) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;
    b.sql("DROP SPACE pub").await;
    b.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{fresh}'",
        a.url()
    ))
    .await;
    assert_eq!(b.sql(JOIN).await.num_rows(), 3);
}

#[tokio::test]
async fn delegated_tokens_act_for_the_delegator_and_revocation_propagates() {
    let a = Node::new(11).serve().await;
    let b = Node::new(12);
    let c = Node::new(13);
    seed_owner(&a, &b.did()).await;

    let (root_id, root) = a
        .issue(&format!("ISSUE UCAN ON SPACE * TO '{}'", b.did()))
        .await;
    // B delegates query access to blog, for one hour, to C.
    let delegated = UcanBuilder::new(c.identity.did().clone(), 3600)
        .capability(Capability::space(a.identity.did(), "blog", ABILITY_QUERY))
        .proof(root.clone())
        .sign(&b.identity)
        .unwrap();

    // C cannot mount B's own token: it was issued to B.
    let err = c
        .sql_err(&format!(
            "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{root}'",
            a.url()
        ))
        .await;
    assert!(err.contains("TOKEN"), "{err}");

    c.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{delegated}'",
        a.url()
    ))
    .await;
    // C reads with B's grants and policies.
    let result = c.sql("SELECT id FROM pub.posts ORDER BY id").await;
    assert_eq!(ints(&result, 0), vec![1, 2, 3]);

    // Even bypassing the CREATE SPACE check, A refuses B's token from C.
    let stolen = FlightEngine::new(
        RemoteSpaceConfig {
            peer: a.url().to_string(),
            remote_space: "blog".to_string(),
            owner: a.identity.did().clone(),
            token: root.clone(),
            allow_http: true,
        },
        c.identity.clone(),
    );
    assert!(matches!(
        stolen.query("SELECT id FROM blog.posts", &[]).await,
        Err(EngineError::PermissionDenied(_))
    ));

    // Revoking B's token cuts C off as well.
    a.sql(&format!("REVOKE UCAN '{root_id}'")).await;
    let err = c.sql_err("SELECT id FROM pub.posts").await;
    assert!(err.contains("permission denied"), "{err}");
}

/// Sign one `do_get` for `sql`, as `caller` with `token`.
fn signed_client_headers(
    caller: &NodeIdentity,
    server: &Node,
    payload: &[u8],
    token: &str,
) -> String {
    let digest = digest(METHOD_DO_GET, payload, token);
    AuthRequest::sign(caller, server.identity.did(), unix_now_ms(), &digest)
        .unwrap()
        .to_header()
        .unwrap()
}

async fn raw_do_get(
    owner: &Node,
    auth: Option<&str>,
    token: Option<&str>,
    ticket: Vec<u8>,
) -> Result<usize, tonic::Code> {
    install_crypto_provider();
    let pinned = DidCertVerifier::new(owner.identity.did().public_key().to_bytes());
    let channel = tonic::transport::Endpoint::from_shared(owner.url().to_string())
        .unwrap()
        .tls_config_with_verifier(
            tonic::transport::ClientTlsConfig::new().domain_name("agoradb.peer"),
            pinned,
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightClient::new(channel);
    if let Some(auth) = auth {
        client.add_header(HEADER_AUTH, auth).unwrap();
    }
    if let Some(token) = token {
        client.add_header(HEADER_UCAN, token).unwrap();
    }
    let code = |e: arrow_flight::error::FlightError| match e {
        arrow_flight::error::FlightError::Tonic(status) => status.code(),
        other => panic!("unexpected error {other}"),
    };
    let stream = client.do_get(Ticket::new(ticket)).await.map_err(code)?;
    let batches: Vec<_> = stream.try_collect().await.map_err(code)?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

fn ticket(sql: &str) -> Vec<u8> {
    serde_json::to_vec(&QueryTicket {
        sql: sql.to_string(),
    })
    .unwrap()
}

#[tokio::test]
async fn requests_are_signed_bound_to_their_content_and_single_use() {
    let a = Node::new(21).serve().await;
    let b = Node::new(22);
    seed_owner(&a, &b.did()).await;
    let (_, token) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;

    let sql = ticket("SELECT id FROM blog.posts");
    let auth = signed_client_headers(&b.identity, &a, &sql, &token);
    assert_eq!(
        raw_do_get(&a, Some(&auth), Some(&token), sql.clone()).await,
        Ok(3)
    );
    // The same request again is a replay.
    assert_eq!(
        raw_do_get(&a, Some(&auth), Some(&token), sql.clone()).await,
        Err(tonic::Code::Unauthenticated)
    );
    // A signature over one query does not authorize another.
    let auth = signed_client_headers(&b.identity, &a, &sql, &token);
    assert_eq!(
        raw_do_get(
            &a,
            Some(&auth),
            Some(&token),
            ticket("SELECT k, v FROM blog.secrets")
        )
        .await,
        Err(tonic::Code::Unauthenticated)
    );
    // Unsigned requests are refused.
    assert_eq!(
        raw_do_get(&a, None, Some(&token), sql.clone()).await,
        Err(tonic::Code::Unauthenticated)
    );
    // A request signed for another node is refused.
    let other = Node::new(23);
    let digest = digest(METHOD_DO_GET, &sql, &token);
    let misdirected = AuthRequest::sign(&b.identity, other.identity.did(), unix_now_ms(), &digest)
        .unwrap()
        .to_header()
        .unwrap();
    assert_eq!(
        raw_do_get(&a, Some(&misdirected), Some(&token), sql.clone()).await,
        Err(tonic::Code::Unauthenticated)
    );

    // Well-signed, but not covered by the UCAN: other Space, writes, expiry.
    a.sql("CREATE SPACE private").await;
    a.sql("CREATE TABLE private.notes (n VARCHAR)").await;
    for query in [
        "SELECT n FROM private.notes",
        "DELETE FROM blog.posts",
        "SELECT 1",
    ] {
        let t = ticket(query);
        let auth = signed_client_headers(&b.identity, &a, &t, &token);
        assert_eq!(
            raw_do_get(&a, Some(&auth), Some(&token), t).await,
            Err(tonic::Code::PermissionDenied),
            "{query}"
        );
    }
    let expired = UcanBuilder::new(b.identity.did().clone(), 60)
        .capability(Capability::space(a.identity.did(), "blog", ABILITY_QUERY))
        .issued_at(unix_now() - 3600)
        .sign(&a.identity)
        .unwrap();
    let auth = signed_client_headers(&b.identity, &a, &sql, &expired);
    assert_eq!(
        raw_do_get(&a, Some(&auth), Some(&expired), sql.clone()).await,
        Err(tonic::Code::PermissionDenied)
    );
    // A token B signed for itself is not rooted at A.
    let forged = UcanBuilder::new(b.identity.did().clone(), 3600)
        .capability(Capability::space(a.identity.did(), "blog", ABILITY_QUERY))
        .sign(&b.identity)
        .unwrap();
    let auth = signed_client_headers(&b.identity, &a, &sql, &forged);
    assert_eq!(
        raw_do_get(&a, Some(&auth), Some(&forged), sql).await,
        Err(tonic::Code::PermissionDenied)
    );
}

#[tokio::test]
async fn remote_spaces_are_never_served_onwards() {
    let a = Node::new(31).serve().await;
    let b = Node::new(32).serve().await;
    let c = Node::new(33);
    seed_owner(&a, &b.did()).await;
    // B would even grant C on its mount of A's Space...
    b.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{}'",
        a.url(),
        a.issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
            .await
            .1
    ))
    .await;
    let err = b
        .sql_err(&format!("ISSUE UCAN ON SPACE pub TO '{}'", c.did()))
        .await;
    assert!(err.contains("another node"), "{err}");
    b.sql(&format!("GRANT SELECT ON pub.posts TO \"{}\"", c.did()))
        .await;
    let (_, wildcard) = b
        .issue(&format!("ISSUE UCAN ON SPACE * TO '{}'", c.did()))
        .await;

    // ...but B never forwards C's query to A.
    let engine = FlightEngine::new(
        RemoteSpaceConfig {
            peer: b.url().to_string(),
            remote_space: "pub".to_string(),
            owner: b.identity.did().clone(),
            token: wildcard,
            allow_http: true,
        },
        c.identity.clone(),
    );
    assert!(matches!(
        engine.query("SELECT id FROM pub.posts", &[]).await,
        Err(EngineError::PermissionDenied(_))
    ));
    assert!(matches!(
        engine
            .table_schema(&QualifiedName::new("pub", "posts"))
            .await,
        Err(EngineError::PermissionDenied(_))
    ));
}

#[tokio::test]
async fn mounting_checks_the_token_and_peer_up_front() {
    let a = Node::new(41).serve().await;
    let b = Node::new(42);
    seed_owner(&a, &b.did()).await;
    let (_, token) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;

    for (sql, expected) in [
        (
            format!(
                "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'other', TOKEN = '{token}'",
                a.url()
            ),
            "TOKEN",
        ),
        (
            "CREATE SPACE pub WITH PEER = 'ftp://a:1', TOKEN = 'x.y.z'".to_string(),
            "https://host:port",
        ),
        (
            format!(
                "CREATE SPACE pub WITH PEER = '{}', TOKEN = 'not-a-token'",
                a.url()
            ),
            "TOKEN",
        ),
    ] {
        let err = b.sql_err(&sql).await;
        assert!(err.contains(expected), "{sql}: {err}");
    }
    // A node does not mount a token it issued itself.
    let err = a
        .sql_err(&format!(
            "CREATE SPACE again WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{token}'",
            a.url()
        ))
        .await;
    assert!(err.contains("TOKEN"), "{err}");

    // ISSUE UCAN validates its inputs.
    for sql in [
        "ISSUE UCAN ON SPACE nope TO 'did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw'",
        "ISSUE UCAN ON SPACE blog TO 'did:web:example.com'",
        "ISSUE UCAN ON SPACE blog TO 'did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw' CAN 'space/write'",
        "REVOKE UCAN 'abc'",
    ] {
        a.sql_err(sql).await;
    }

    // Principals cannot manage capabilities.
    let peer = AgoraSession::new(
        a.catalog.clone(),
        a.engines.clone(),
        SessionConfig {
            default_space: None,
            principal: Some(b.did()),
        },
    );
    for sql in [
        "SHOW UCANS".to_string(),
        format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()),
        "REVOKE UCAN '0123456789abcdef0123456789abcdef'".to_string(),
    ] {
        let err = peer.sql(&sql).await.unwrap_err().to_string();
        assert!(err.contains("permission denied"), "{sql}: {err}");
    }

    // The mounted Space survives a restart of B (token kept on disk).
    b.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{token}'",
        a.url()
    ))
    .await;
    let reopened =
        Arc::new(AgoraCatalog::open(FileIO::new_with_fs(), b.catalog.root_path()).unwrap());
    let engines = Arc::new(
        EngineRegistry::new(
            reopened.clone(),
            NodeConfig {
                identity: Some(b.identity.clone()),
                allow_http: true,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let session = AgoraSession::new(reopened, engines, SessionConfig::default());
    let result = session.sql("SELECT count(*) FROM pub.posts").await.unwrap();
    assert_eq!(ints(&result, 0), vec![3]);
}
