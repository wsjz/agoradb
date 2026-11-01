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

//! Transport between nodes (3.1c): TLS pinned to the owner's DID, and
//! plaintext only where a node allows it.

mod common;

use common::{ints, Node};

/// `a` owns `blog.posts` and grants it to `grantee`.
async fn seed(a: &Node, grantee: &Node) -> String {
    for sql in [
        "CREATE SPACE blog".to_string(),
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)".to_string(),
        "INSERT INTO blog.posts VALUES (1, 'Hello'), (2, 'World')".to_string(),
        format!("GRANT SELECT ON blog.posts TO \"{}\"", grantee.did()),
    ] {
        a.sql(&sql).await;
    }
    a.issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", grantee.did()))
        .await
        .1
}

fn mount(name: &str, peer: &str, token: &str) -> String {
    format!("CREATE SPACE {name} WITH PEER = '{peer}', REMOTE = 'blog', TOKEN = '{token}'")
}

#[tokio::test]
async fn peers_are_reached_over_tls_pinned_to_the_owners_did() {
    let a = Node::new(41).serve().await;
    let b = Node::new(42);
    let token = seed(&a, &b).await;
    assert!(a.url().starts_with("https://"));
    b.sql(&mount("pub", a.url(), &token)).await;
    let count = b.sql("SELECT count(*) FROM pub.posts").await;
    assert_eq!(ints(&count, 0), vec![2]);

    // Another node answering at the address B was given cannot pose as A:
    // its certificate carries its own key, not A's.
    let c = Node::new(43).serve().await;
    b.sql(&mount("fake", c.url(), &token)).await;
    let err = b.sql_err("SELECT count(*) FROM fake.posts").await;
    assert!(err.contains("certificate"), "{err}");
}

#[tokio::test]
async fn plaintext_peers_need_allow_http() {
    let a = Node::new(44).serve_plaintext().await;
    assert!(a.url().starts_with("http://"));

    // B only speaks TLS: the address is refused when the Space is created.
    let b = Node::new(45);
    let token = seed(&a, &b).await;
    let err = b.sql_err(&mount("pub", a.url(), &token)).await;
    assert!(
        err.contains("plaintext") && err.contains("--allow-http"),
        "{err}"
    );

    // A node configured to allow it reaches the plaintext peer.
    let c = Node::allowing_http(46);
    let token = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", c.did()))
        .await
        .1;
    a.sql(&format!("GRANT SELECT ON blog.posts TO \"{}\"", c.did()))
        .await;
    c.sql(&mount("pub", a.url(), &token)).await;
    let count = c.sql("SELECT count(*) FROM pub.posts").await;
    assert_eq!(ints(&count, 0), vec![2]);
}
