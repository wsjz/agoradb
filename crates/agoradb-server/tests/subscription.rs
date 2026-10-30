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

//! Subscription (3.1b): copying another node's Space with `space/read` and
//! querying the copy locally.

mod common;

use common::{ints, strings, Node};

/// Node A's `blog` Space, as the DID `grantee` sees it: `posts` in full,
/// `notes` with one column granted, `drafts` under a row policy.
async fn seed_blog(a: &Node, grantee: &str) {
    for sql in [
        "CREATE SPACE blog".to_string(),
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)".to_string(),
        "INSERT INTO blog.posts VALUES (1, 'Hello'), (2, 'World')".to_string(),
        "CREATE TABLE blog.notes (id BIGINT NOT NULL, body VARCHAR)".to_string(),
        "INSERT INTO blog.notes VALUES (1, 'private')".to_string(),
        "CREATE TABLE blog.drafts (id BIGINT NOT NULL, done BOOLEAN)".to_string(),
        "INSERT INTO blog.drafts VALUES (1, true), (2, false)".to_string(),
        format!("GRANT SELECT ON blog.posts TO \"{grantee}\""),
        format!("GRANT SELECT (id) ON blog.notes TO \"{grantee}\""),
        format!("GRANT SELECT ON blog.drafts TO \"{grantee}\""),
        format!("CREATE POLICY finished ON blog.drafts TO \"{grantee}\" USING (done)"),
    ] {
        a.sql(&sql).await;
    }
}

/// A token from `a` letting `b` read (and so copy) `space`.
async fn read_token(a: &Node, b: &Node, space: &str) -> (String, String) {
    a.issue(&format!(
        "ISSUE UCAN ON SPACE {space} TO '{}' CAN 'space/read'",
        b.did()
    ))
    .await
}

fn subscribe(a: &Node, name: &str, remote: &str, token: &str) -> String {
    format!(
        "CREATE SPACE {name} WITH PEER = '{}', REMOTE = '{remote}', TOKEN = '{token}', MODE = 'subscribe'",
        a.url()
    )
}

/// Files held by `b`'s copy `name`.
fn replica_files(b: &Node, name: &str) -> usize {
    std::fs::read_dir(b.catalog.replica_dir(name).join("files"))
        .map(|d| d.count())
        .unwrap_or(0)
}

#[tokio::test]
async fn subscription_copies_full_tables_and_survives_the_owner_going_offline() {
    let mut a = Node::new(21).serve().await;
    let b = Node::new(22);
    seed_blog(&a, &b.did()).await;
    let (_, token) = read_token(&a, &b, "blog").await;

    let report = b.sql(&subscribe(&a, "copy", "blog", &token)).await;
    assert_eq!(strings(&report, 0), vec!["posts", "drafts", "notes"]);
    assert_eq!(
        strings(&report, 4),
        vec!["copied", "restricted", "restricted"]
    );
    assert_eq!(ints(&report, 2)[0], 1, "one data file for posts");
    assert_eq!(replica_files(&b, "copy"), 1);

    let posts = b.sql("SELECT id, title FROM copy.posts ORDER BY id").await;
    assert_eq!(ints(&posts, 0), vec![1, 2]);
    assert_eq!(strings(&posts, 1), vec!["Hello", "World"]);
    // Restricted tables are not copied...
    let err = b.sql_err("SELECT * FROM copy.notes").await;
    assert!(err.contains("not found"), "{err}");
    // ...but the same token still queries them remotely, under A's rules.
    b.sql(&format!(
        "CREATE SPACE live WITH PEER = '{}', REMOTE = 'blog', TOKEN = '{token}'",
        a.url()
    ))
    .await;
    let notes = b.sql("SELECT * FROM live.notes").await;
    assert_eq!(notes.schema().unwrap().fields().len(), 1);
    assert_eq!(ints(&b.sql("SELECT id FROM live.drafts").await, 0), vec![1]);

    // A copy is read-only.
    let err = b.sql_err("INSERT INTO copy.posts VALUES (9, 'x')").await;
    assert!(err.contains("read-only"), "{err}");

    // With A offline the copy still answers; refreshing it does not.
    a.stop().await;
    let count = b.sql("SELECT count(*) FROM copy.posts").await;
    assert_eq!(ints(&count, 0), vec![2]);
    let err = b.sql_err("REFRESH SPACE copy").await;
    assert!(err.contains("cannot reach"), "{err}");
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM copy.posts").await, 0),
        vec![2]
    );
}

#[tokio::test]
async fn refresh_fetches_only_new_files() {
    let a = Node::new(23).serve().await;
    let b = Node::new(24);
    seed_blog(&a, &b.did()).await;
    let (_, token) = read_token(&a, &b, "blog").await;
    b.sql(&subscribe(&a, "copy", "blog", &token)).await;

    a.sql("INSERT INTO blog.posts VALUES (3, 'Again')").await;
    let report = b.sql("REFRESH SPACE copy").await;
    assert_eq!(strings(&report, 0)[0], "posts");
    assert_eq!(ints(&report, 2)[0], 1, "only the new file is fetched");
    assert!(ints(&report, 3)[0] > 0);
    let posts = b.sql("SELECT id FROM copy.posts ORDER BY id").await;
    assert_eq!(ints(&posts, 0), vec![1, 2, 3]);

    // Nothing changed: nothing fetched.
    let report = b.sql("REFRESH SPACE copy").await;
    assert_eq!(ints(&report, 2)[0], 0);

    // A replaced table (DROP + CREATE) is followed; files of the copy before
    // the previous one are removed.
    a.sql("DROP TABLE blog.posts").await;
    a.sql("CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)")
        .await;
    a.sql("INSERT INTO blog.posts VALUES (7, 'Fresh')").await;
    b.sql("REFRESH SPACE copy").await;
    b.sql("REFRESH SPACE copy").await;
    let posts = b.sql("SELECT id, title FROM copy.posts").await;
    assert_eq!(ints(&posts, 0), vec![7]);
    assert_eq!(replica_files(&b, "copy"), 1);
}

#[tokio::test]
async fn subscribing_needs_space_read_and_revocation_stops_refreshes() {
    let a = Node::new(25).serve().await;
    let b = Node::new(26);
    seed_blog(&a, &b.did()).await;

    // A query-only token mounts the Space but cannot copy it.
    let (_, query_only) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;
    let err = b.sql_err(&subscribe(&a, "copy", "blog", &query_only)).await;
    assert!(err.contains("subscribe to 'blog'"), "{err}");
    assert!(b
        .sql_err("SELECT * FROM copy.posts")
        .await
        .contains("not found"));

    let (id, token) = read_token(&a, &b, "blog").await;
    b.sql(&subscribe(&a, "copy", "blog", &token)).await;
    a.sql(&format!("REVOKE UCAN '{id}'")).await;
    a.sql("INSERT INTO blog.posts VALUES (3, 'Again')").await;

    // Revocation stops further copies; what was copied stays readable.
    let err = b.sql_err("REFRESH SPACE copy").await;
    assert!(err.contains("permission denied"), "{err}");
    let count = b.sql("SELECT count(*) FROM copy.posts").await;
    assert_eq!(ints(&count, 0), vec![2]);

    // Dropping the Space removes the copy.
    assert!(b.catalog.replica_dir("copy").exists());
    b.sql("DROP SPACE copy").await;
    assert!(!b.catalog.replica_dir("copy").exists());
}

#[tokio::test]
async fn transactional_spaces_are_copied_through_their_publication() {
    let a = Node::new(27).serve().await;
    let b = Node::new(28);
    for sql in [
        "CREATE SPACE shop WITH KIND = 'transactional'".to_string(),
        "CREATE TABLE shop.items (id INTEGER PRIMARY KEY, name TEXT)".to_string(),
        "INSERT INTO shop.items VALUES (1, 'pen'), (2, 'ink')".to_string(),
        format!("GRANT SELECT ON shop.items TO \"{}\"", b.did()),
    ] {
        a.sql(&sql).await;
    }
    let (_, token) = read_token(&a, &b, "shop").await;
    let err = b.sql_err(&subscribe(&a, "copy", "shop", &token)).await;
    assert!(err.contains("PUBLISH"), "{err}");
    // Nothing was registered or left behind.
    assert!(b.sql_err("REFRESH SPACE copy").await.contains("not found"));
    assert!(!b.catalog.replica_dir("copy").exists());

    a.sql("PUBLISH SPACE shop").await;
    a.sql(&format!(
        "GRANT SELECT ON shop_published.items TO \"{}\"",
        b.did()
    ))
    .await;
    let (_, token) = read_token(&a, &b, "shop_published").await;
    b.sql(&subscribe(&a, "copy", "shop_published", &token))
        .await;
    let items = b.sql("SELECT name FROM copy.items ORDER BY id").await;
    assert_eq!(strings(&items, 0), vec!["pen", "ink"]);
}

#[tokio::test]
async fn copies_are_never_served_onwards() {
    let a = Node::new(29).serve().await;
    let b = Node::new(30).serve().await;
    let c = Node::new(31);
    seed_blog(&a, &b.did()).await;
    let (_, token) = read_token(&a, &b, "blog").await;
    b.sql(&subscribe(&a, "copy", "blog", &token)).await;

    // B cannot grant A's data...
    let err = b
        .sql_err(&format!("ISSUE UCAN ON SPACE copy TO '{}'", c.did()))
        .await;
    assert!(err.contains("another node"), "{err}");
    // ...and a token for all of B's Spaces does not reach the copy either.
    let (_, every) = b
        .issue(&format!(
            "ISSUE UCAN ON SPACE * TO '{}' CAN 'space/read'",
            c.did()
        ))
        .await;
    let err = c.sql_err(&subscribe(&b, "copy2", "copy", &every)).await;
    assert!(err.contains("permission denied"), "{err}");
    c.sql(&format!(
        "CREATE SPACE mounted WITH PEER = '{}', REMOTE = 'copy', TOKEN = '{every}'",
        b.url()
    ))
    .await;
    let err = c.sql_err("SELECT * FROM mounted.posts").await;
    assert!(err.contains("permission denied"), "{err}");
}
