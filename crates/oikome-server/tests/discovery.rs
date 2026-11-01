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

//! Discovery (3.1c): nodes find each other's Flight endpoints by DID, and
//! remote Spaces work without a PEER address.

mod common;

use std::time::Duration;

use common::{ints, strings, Node};
use oikome_discovery::{Discovery, DiscoveryConfig, EndpointResponse, Multiaddr};

/// Run discovery for a serving node, announcing its Flight endpoint.
async fn discover(node: &Node, listen: &str, mdns: bool) -> Discovery {
    let (scheme, rest) = node.url().split_once("://").unwrap();
    let port = rest.rsplit_once(':').unwrap().1.parse().unwrap();
    Discovery::start(
        &node.identity,
        node.catalog.clone(),
        DiscoveryConfig {
            listen: listen.parse().unwrap(),
            flight: EndpointResponse {
                scheme: scheme.to_string(),
                port,
            },
            mdns,
        },
    )
    .await
    .unwrap()
}

/// Wait until `node` has recorded `peer` at its current Flight endpoint.
async fn wait_for(node: &Node, peer: &Node) {
    for _ in 0..200 {
        if node
            .catalog
            .get_peer(&peer.did())
            .is_some_and(|p| p.endpoint == peer.url())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{} was not discovered at {}", peer.did(), peer.url());
}

fn first_addr(d: &Discovery) -> Multiaddr {
    d.listen_addrs()[0].clone()
}

async fn seed_blog(a: &Node, grantee: &Node) {
    for sql in [
        "CREATE SPACE blog".to_string(),
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)".to_string(),
        "INSERT INTO blog.posts VALUES (1, 'Hello'), (2, 'World')".to_string(),
        format!("GRANT SELECT ON blog.posts TO \"{}\"", grantee.did()),
    ] {
        a.sql(&sql).await;
    }
}

#[tokio::test]
async fn discovered_owners_are_reached_without_a_peer_address() {
    let a = Node::new(51).serve().await;
    let b = Node::new(52).serve().await;
    seed_blog(&a, &b).await;
    let (_, query) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;

    // Not discovered yet: an address is needed.
    let err = b
        .sql_err(&format!(
            "CREATE SPACE pub WITH TOKEN = '{query}', REMOTE = 'blog'"
        ))
        .await;
    assert!(err.contains("has not been discovered"), "{err}");

    let da = discover(&a, "/ip4/127.0.0.1/tcp/0", false).await;
    let db = discover(&b, "/ip4/127.0.0.1/tcp/0", false).await;
    db.dial(first_addr(&da)).await.unwrap();
    // Both sides learn each other's endpoint from one connection.
    wait_for(&b, &a).await;
    wait_for(&a, &b).await;
    let peers = b.sql("SHOW PEERS").await;
    assert_eq!(strings(&peers, 0), vec![a.did()]);
    assert_eq!(strings(&peers, 1), vec![a.url().to_string()]);

    b.sql(&format!(
        "CREATE SPACE pub WITH TOKEN = '{query}', REMOTE = 'blog'"
    ))
    .await;
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM pub.posts").await, 0),
        vec![2]
    );

    let (_, read) = a
        .issue(&format!(
            "ISSUE UCAN ON SPACE blog TO '{}' CAN 'space/read'",
            b.did()
        ))
        .await;
    b.sql(&format!(
        "CREATE SPACE copy WITH TOKEN = '{read}', REMOTE = 'blog', MODE = 'subscribe'"
    ))
    .await;
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM copy.posts").await, 0),
        vec![2]
    );
}

#[tokio::test]
async fn an_owner_that_moved_is_found_at_its_new_address() {
    let mut a = Node::new(53).serve().await;
    let b = Node::new(54).serve().await;
    seed_blog(&a, &b).await;
    let (_, token) = a
        .issue(&format!("ISSUE UCAN ON SPACE blog TO '{}'", b.did()))
        .await;
    let da = discover(&a, "/ip4/127.0.0.1/tcp/0", false).await;
    let db = discover(&b, "/ip4/127.0.0.1/tcp/0", false).await;
    db.dial(first_addr(&da)).await.unwrap();
    wait_for(&b, &a).await;
    b.sql(&format!(
        "CREATE SPACE pub WITH TOKEN = '{token}', REMOTE = 'blog'"
    ))
    .await;
    b.sql("SELECT count(*) FROM pub.posts").await;

    // A restarts on another port and is found again.
    let old = a.url().to_string();
    a.stop().await;
    drop(da);
    let a = a.serve().await;
    assert_ne!(a.url(), old);
    let da = discover(&a, "/ip4/127.0.0.1/tcp/0", false).await;
    db.dial(first_addr(&da)).await.unwrap();
    wait_for(&b, &a).await;
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM pub.posts").await, 0),
        vec![2]
    );
}

/// mDNS needs multicast on the host's network, which CI containers often
/// lack, and a host firewall that lets the test binary receive it (the
/// macOS application firewall blocks new unsigned binaries). Run by hand:
/// `cargo test -p oikome-server --test discovery -- --ignored`.
#[tokio::test]
#[ignore]
async fn nodes_on_the_same_network_find_each_other_with_mdns() {
    let a = Node::new(55).serve().await;
    let b = Node::new(56).serve().await;
    let _da = discover(&a, "/ip4/0.0.0.0/tcp/0", true).await;
    let _db = discover(&b, "/ip4/0.0.0.0/tcp/0", true).await;
    for _ in 0..300 {
        if b.catalog.get_peer(&a.did()).is_some() && a.catalog.get_peer(&b.did()).is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("mDNS did not find the other node");
}
