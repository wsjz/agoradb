// Copyright 2025 The Oikome Authors
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

//! The browser node mounting a Full Node's Space and joining it with a
//! local one (3.2 exit criteria), run natively: DuckDB stands in for
//! duckdb-wasm, and the transport hands each envelope straight to the Full
//! Node's service. `e2e/` runs the same over a real WebRTC channel.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use iceberg::io::FileIO;
use oikome_catalog::OikomeCatalog;
use oikome_engine::{EngineError, QueryEngine};
use oikome_engine_duckdb::DuckDbEngine;
use oikome_identity::NodeIdentity;
use oikome_node::{EngineRegistry, NodeConfig, OikomeSession, QueryResult, SessionConfig};
use oikome_protocol::webrtc::{AccessRecord, Envelope, RECORD_PREFIX};
use oikome_server::OikomeFlightService;
use oikome_wasm::{BrowserNode, NodeError, Output, Transport};
use oikome_webrtc::{DtlsIdentity, Handler};

/// The WebRTC channel, minus WebRTC: calls go to `service` if they are for
/// the address and fingerprint it is reachable at.
struct Loopback {
    addr: String,
    fingerprint: String,
    service: OikomeFlightService,
}

#[async_trait]
impl Transport for Loopback {
    async fn call(
        &self,
        addr: &str,
        fingerprint: &str,
        message: Vec<u8>,
    ) -> Result<Vec<u8>, EngineError> {
        if addr != self.addr || fingerprint != self.fingerprint {
            return Err(EngineError::Remote("no such endpoint".to_string()));
        }
        let envelope: Envelope =
            serde_json::from_slice(&message).map_err(|e| EngineError::Remote(e.to_string()))?;
        Ok(self.service.call(envelope).await)
    }
}

struct FullNode {
    _dir: tempfile::TempDir,
    owner: OikomeSession,
    record: String,
    transport: Arc<Loopback>,
}

impl FullNode {
    fn new(seed: u8) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("node");
        let catalog =
            Arc::new(OikomeCatalog::open(FileIO::new_with_fs(), root.to_str().unwrap()).unwrap());
        let identity = Arc::new(NodeIdentity::from_seed(&[seed; 32]));
        let engines = Arc::new(
            EngineRegistry::new(
                catalog.clone(),
                NodeConfig {
                    identity: Some(identity.clone()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let owner = OikomeSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
        let dtls = DtlsIdentity::load_or_create(&dir.path().join("dtls.json")).unwrap();
        let addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
        let record = dtls
            .access_record(&identity, addr, Duration::from_secs(3600))
            .unwrap();
        let transport = Arc::new(Loopback {
            addr: addr.to_string(),
            fingerprint: dtls.fingerprint().to_string(),
            service: OikomeFlightService::new(catalog, engines).unwrap(),
        });
        Self {
            _dir: dir,
            owner,
            record,
            transport,
        }
    }

    async fn sql(&self, sql: &str) -> QueryResult {
        self.owner
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// `ISSUE UCAN ...` → (id, token).
    async fn issue(&self, sql: &str) -> (String, String) {
        let result = self.sql(sql).await;
        let column = |i: usize| {
            result.batches()[0]
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        (column(0), column(3))
    }
}

fn browser(full: &FullNode, seed: u8) -> BrowserNode {
    let engine: Arc<dyn QueryEngine> =
        Arc::new(DuckDbEngine::open_in_memory(Default::default()).unwrap());
    BrowserNode::open(NodeIdentity::from_seed(&[seed; 32]), engine, None)
        .unwrap()
        .with_transport(full.transport.clone())
}

async fn rows(node: &BrowserNode, sql: &str) -> Vec<RecordBatch> {
    match node.sql(sql).await {
        Ok(Output::Rows { batches, .. }) => batches,
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn ints(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

fn strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A Full Node with `blog.posts`, readable by `grantee` except for drafts
/// and the `draft` column.
async fn seed_blog(full: &FullNode, grantee: &str) {
    for sql in [
        "CREATE SPACE blog".to_string(),
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR, draft BOOLEAN)".to_string(),
        "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello', false), (2, 'bob', 'World', false), \
         (3, 'alice', 'Again', false), (4, 'alice', 'Unfinished', true)"
            .to_string(),
        "CREATE TABLE blog.secrets (k VARCHAR)".to_string(),
        format!("GRANT SELECT (id, author, title) ON blog.posts TO \"{grantee}\""),
        format!("CREATE POLICY published ON blog.posts TO \"{grantee}\" USING (NOT draft)"),
    ] {
        full.sql(&sql).await;
    }
}

const JOIN: &str = "SELECT o.id, o.customer, p.title \
     FROM orders.orders o JOIN pub.posts p ON p.author = o.customer \
     ORDER BY o.id, p.title";

#[tokio::test]
async fn a_local_table_joins_a_full_nodes_table_until_the_token_is_revoked() {
    let full = FullNode::new(31);
    let b = browser(&full, 32);
    let me = b.identity().did().to_string();
    seed_blog(&full, &me).await;
    for sql in [
        "CREATE SPACE orders",
        "CREATE TABLE orders.orders (id BIGINT, customer VARCHAR)",
        "INSERT INTO orders.orders VALUES (10, 'alice'), (11, 'carol'), (12, 'bob')",
    ] {
        b.sql(sql).await.unwrap();
    }
    let (id, token) = full
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{me}'"))
        .await;
    b.sql(&format!(
        "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{token}'",
        full.record
    ))
    .await
    .unwrap();

    let joined = rows(&b, JOIN).await;
    assert_eq!(ints(&joined, 0), vec![10, 10, 12]);
    assert_eq!(strings(&joined, 1), vec!["alice", "alice", "bob"]);
    // The draft post is filtered on the Full Node by its row policy.
    assert_eq!(strings(&joined, 2), vec!["Again", "Hello", "World"]);

    // Only the granted columns and relations are visible.
    let all = rows(&b, "SELECT * FROM pub.posts ORDER BY id").await;
    let columns: Vec<String> = all[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(columns, vec!["id", "author", "title"]);
    let err = b.sql("SELECT * FROM pub.secrets").await.unwrap_err();
    assert!(err.to_string().contains("not found"), "{err}");

    // Aggregates and unions run in the federated plan.
    let counts = rows(
        &b,
        "SELECT author, count(*) AS n FROM pub.posts GROUP BY author ORDER BY author",
    )
    .await;
    assert_eq!(strings(&counts, 0), vec!["alice", "bob"]);
    assert_eq!(ints(&counts, 1), vec![2, 1]);
    let names = rows(
        &b,
        "SELECT customer AS name FROM orders.orders UNION SELECT author FROM pub.posts ORDER BY name",
    )
    .await;
    assert_eq!(strings(&names, 0), vec!["alice", "bob", "carol"]);

    // Remote Spaces are read-only.
    let err = b
        .sql("INSERT INTO orders.orders SELECT id, author FROM pub.posts")
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::Unsupported(_)), "{err}");

    // The mount survives a restart, without printing the token.
    let registry = b.registry_bytes().unwrap();
    assert!(!format!("{b:?}").contains(&token));
    let reopened = BrowserNode::open(
        NodeIdentity::from_seed(&[32; 32]),
        Arc::new(DuckDbEngine::open_in_memory(Default::default()).unwrap()),
        Some(&registry),
    )
    .unwrap()
    .with_transport(full.transport.clone());
    assert_eq!(
        ints(
            &rows(&reopened, "SELECT id FROM pub.posts ORDER BY id").await,
            0
        ),
        vec![1, 2, 3]
    );

    // Revocation on the Full Node takes effect on the next query.
    full.sql(&format!("REVOKE UCAN '{id}'")).await;
    let err = b.sql(JOIN).await.unwrap_err();
    assert!(err.to_string().contains("permission denied"), "{err}");
}

#[tokio::test]
async fn records_and_tokens_are_checked_when_mounting() {
    let full = FullNode::new(33);
    let b = browser(&full, 34);
    let me = b.identity().did().to_string();
    seed_blog(&full, &me).await;
    let (_, token) = full
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{me}'"))
        .await;
    let mount = |record: &str, token: &str| {
        format!("CREATE SPACE pub WITH PEER = '{record}', REMOTE = 'blog', TOKEN = '{token}'")
    };

    // A record pointed at another endpoint no longer matches its signature.
    let (body, signature) = full.record.rsplit_once('.').unwrap();
    let json = URL_SAFE_NO_PAD
        .decode(body.strip_prefix(RECORD_PREFIX).unwrap())
        .unwrap();
    let mut fields: AccessRecord = serde_json::from_slice(&json).unwrap();
    fields.addr = "127.0.0.1:9".to_string();
    let redirected = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&fields).unwrap());
    let tampered = format!("{RECORD_PREFIX}{redirected}.{signature}");
    let err = b.sql(&mount(&tampered, &token)).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "PEER: access record signature does not match its DID"
    );

    // A record signed by another node does not match the token's issuer.
    let other = FullNode::new(35);
    let err = b.sql(&mount(&other.record, &token)).await.unwrap_err();
    assert!(err.to_string().contains("issued by"), "{err}");

    // A token issued to another node, or for another Space, is refused.
    let (_, theirs) = full
        .issue(&format!(
            "ISSUE UCAN ON SPACE blog TO '{}'",
            NodeIdentity::from_seed(&[36; 32]).did()
        ))
        .await;
    let err = b.sql(&mount(&full.record, &theirs)).await.unwrap_err();
    assert!(err.to_string().starts_with("TOKEN:"), "{err}");
    let err = b
        .sql(&format!(
            "CREATE SPACE pub WITH PEER = '{}', REMOTE = 'other', TOKEN = '{token}'",
            full.record
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().starts_with("TOKEN:"), "{err}");

    // Only WebRTC peers, queried in place.
    let err = b
        .sql(&mount("https://127.0.0.1:50051", &token))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::Unsupported(_)), "{err}");
    let err = b
        .sql(&format!(
            "{}, MODE = 'subscribe'",
            mount(&full.record, &token)
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::Unsupported(_)), "{err}");

    b.sql(&mount(&full.record, &token)).await.unwrap();
    assert_eq!(
        ints(&rows(&b, "SELECT count(*) FROM pub.posts").await, 0),
        vec![3]
    );
}
