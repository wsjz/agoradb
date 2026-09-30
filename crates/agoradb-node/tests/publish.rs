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

//! `PUBLISH SPACE` (3.0-D exit criteria): a transactional Space published as
//! Iceberg snapshots, queried by DuckDB, equal to the SQLite source.

use std::sync::Arc;
use std::time::Duration;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::{CatalogError, SpaceKind};
use agoradb_node::{
    spawn_publisher, AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig,
    SessionError,
};
use agoradb_semantic::PublishRequest;
use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use futures::TryStreamExt;
use iceberg::io::FileIO;
use iceberg::{Catalog, TableIdent};

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<AgoraCatalog>,
    engines: Arc<EngineRegistry>,
    owner: Arc<AgoraSession>,
}

impl Node {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(
            AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap(),
        );
        let engines =
            Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
        let owner = Arc::new(AgoraSession::new(
            catalog.clone(),
            engines.clone(),
            SessionConfig::default(),
        ));
        Self {
            _dir: dir,
            catalog,
            engines,
            owner,
        }
    }

    fn session(&self) -> AgoraSession {
        AgoraSession::new(
            self.catalog.clone(),
            self.engines.clone(),
            SessionConfig::default(),
        )
    }

    async fn run(&self, sqls: &[&str]) {
        for sql in sqls {
            self.owner
                .sql(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    async fn scalar(&self, sql: &str) -> i64 {
        ints(&self.owner.sql(sql).await.unwrap(), 0)[0]
    }
}

fn ints(result: &QueryResult, column: usize) -> Vec<i64> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            b.column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn strings(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn seed(n: &Node) {
    n.run(&[
        "CREATE SPACE orders WITH KIND = 'transactional'",
        "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount REAL, paid BOOLEAN)",
        "INSERT INTO orders.orders VALUES (1, 'alice', 9.5, 1), (2, 'bob', 20.0, 0), (3, 'alice', NULL, 1)",
        "CREATE TABLE orders.items (order_id INTEGER, sku TEXT, blob BLOB)",
        "INSERT INTO orders.items VALUES (1, 'a', x'00ff'), (1, 'b', NULL), (2, 'c', x'01')",
    ])
    .await;
}

/// Row count of `space.table` at a given Iceberg snapshot, read through Iceberg.
async fn rows_at(catalog: &AgoraCatalog, space: &str, table: &str, snapshot: i64) -> usize {
    let ident = TableIdent::from_strs([space, table]).unwrap();
    let table = catalog.load_table(&ident).await.unwrap();
    let batches: Vec<_> = table
        .scan()
        .snapshot_id(snapshot)
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn publish_creates_an_analytical_copy() {
    let n = Node::new();
    seed(&n).await;

    let report = n.owner.sql("PUBLISH SPACE orders").await.unwrap();
    assert_eq!(strings(&report, 0), vec!["items", "orders"]);
    assert_eq!(ints(&report, 1), vec![3, 3]);

    let published = n.catalog.get_space("orders_published").unwrap();
    assert_eq!(published.kind, SpaceKind::Analytical);
    assert_eq!(published.published_from.as_deref(), Some("orders"));

    // Same content, now served by DuckDB from Parquet.
    let source = n
        .owner
        .sql("SELECT id, customer, amount FROM orders.orders ORDER BY id")
        .await
        .unwrap();
    let copy = n
        .owner
        .sql("SELECT id, customer, amount FROM orders_published.orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&source, 0), ints(&copy, 0));
    assert_eq!(strings(&source, 1), strings(&copy, 1));
    let amounts = copy.batches()[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(amounts.value(0), 9.5);
    assert!(amounts.is_null(2));
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.orders WHERE paid")
            .await,
        2
    );
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.items WHERE blob IS NOT NULL")
            .await,
        2
    );

    // The published copy federates with live data like any analytical Space.
    let joined = n
        .owner
        .sql(
            "SELECT count(*) FROM orders_published.orders p \
             JOIN orders.items i ON i.order_id = p.id",
        )
        .await
        .unwrap();
    assert_eq!(ints(&joined, 0), vec![3]);
}

#[tokio::test]
async fn republish_replaces_data_and_keeps_history() {
    let n = Node::new();
    seed(&n).await;
    let first = n.owner.sql("PUBLISH SPACE orders").await.unwrap();
    let first_snapshot = ints(&first, 2)[1];

    n.run(&[
        "INSERT INTO orders.orders VALUES (4, 'carol', 5.0, 0)",
        "UPDATE orders.orders SET amount = 1.0 WHERE id = 2",
        "DELETE FROM orders.orders WHERE id = 1",
    ])
    .await;
    let second = n.owner.sql("PUBLISH SPACE orders").await.unwrap();
    let second_snapshot = ints(&second, 2)[1];
    assert_ne!(first_snapshot, second_snapshot);

    // Exact copy of the source, not an append.
    let copy = n
        .owner
        .sql("SELECT id FROM orders_published.orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&copy, 0), vec![2, 3, 4]);
    assert_eq!(
        n.scalar("SELECT CAST(amount AS BIGINT) FROM orders_published.orders WHERE id = 2")
            .await,
        1
    );

    // The previous publication is still readable at its snapshot.
    assert_eq!(
        rows_at(&n.catalog, "orders_published", "orders", first_snapshot).await,
        3
    );
    assert_eq!(
        rows_at(&n.catalog, "orders_published", "orders", second_snapshot).await,
        3
    );

    // Publishing an emptied table yields an empty snapshot.
    n.run(&["DELETE FROM orders.items"]).await;
    n.run(&["PUBLISH SPACE orders"]).await;
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.items")
            .await,
        0
    );
}

#[tokio::test]
async fn subset_target_schema_change_and_dropped_tables() {
    let n = Node::new();
    seed(&n).await;

    let report = n
        .owner
        .sql("PUBLISH SPACE orders TABLES (items) TO snap")
        .await
        .unwrap();
    assert_eq!(strings(&report, 0), vec!["items"]);
    assert_eq!(
        n.catalog
            .space_tables(&n.catalog.get_space("snap").unwrap())
            .await
            .unwrap(),
        vec!["items"]
    );

    // A column added in SQLite shows up in the next publication.
    n.run(&[
        "ALTER TABLE orders.orders ADD COLUMN note TEXT",
        "UPDATE orders.orders SET note = 'vip' WHERE id = 2",
        "PUBLISH SPACE orders TO snap",
    ])
    .await;
    let notes = n
        .owner
        .sql("SELECT note FROM snap.orders WHERE note IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(strings(&notes, 0), vec!["vip"]);

    // A full publish drops tables the source no longer has.
    n.run(&["DROP TABLE orders.items", "PUBLISH SPACE orders TO snap"])
        .await;
    let snap = n.catalog.get_space("snap").unwrap();
    assert_eq!(n.catalog.space_tables(&snap).await.unwrap(), vec!["orders"]);
    assert!(n.owner.sql("SELECT * FROM snap.items").await.is_err());
}

#[tokio::test]
async fn published_space_is_read_only() {
    let n = Node::new();
    seed(&n).await;
    n.run(&["PUBLISH SPACE orders"]).await;
    for sql in [
        "INSERT INTO orders_published.orders VALUES (9, 'x', 1.0, 0)",
        "CREATE TABLE orders_published.extra (id BIGINT)",
        "DROP TABLE orders_published.orders",
    ] {
        assert!(
            matches!(
                n.owner.sql(sql).await,
                Err(SessionError::PublishedSpace { ref origin, .. }) if origin == "orders"
            ),
            "{sql} must be rejected"
        );
    }
    // Views and grants on a published Space are fine.
    n.run(&[
        "CREATE VIEW orders_published.paid AS SELECT id FROM orders_published.orders WHERE paid",
        "GRANT SELECT ON orders_published.paid TO alice",
    ])
    .await;
    let alice = AgoraSession::new(
        n.catalog.clone(),
        n.engines.clone(),
        SessionConfig {
            principal: Some("alice".into()),
            ..Default::default()
        },
    );
    let paid = alice
        .sql("SELECT id FROM orders_published.paid ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&paid, 0), vec![1, 3]);
    assert!(matches!(
        alice.sql("PUBLISH SPACE orders").await,
        Err(SessionError::PermissionDenied(_))
    ));
}

#[tokio::test]
async fn publish_validates_its_request() {
    let n = Node::new();
    seed(&n).await;
    n.run(&["CREATE SPACE blog"]).await;
    assert!(matches!(
        n.owner.sql("PUBLISH SPACE blog").await,
        Err(SessionError::Unsupported(_))
    ));
    assert!(matches!(
        n.owner.sql("PUBLISH SPACE orders TO blog").await,
        Err(SessionError::AlreadyExists(_))
    ));
    assert!(matches!(
        n.owner.sql("PUBLISH SPACE orders TABLES (nope)").await,
        Err(SessionError::Catalog(CatalogError::TableNotFound(_)))
    ));
    assert!(matches!(
        n.owner.sql("PUBLISH SPACE ghost").await,
        Err(SessionError::Catalog(CatalogError::SpaceNotFound(_)))
    ));
}

#[tokio::test]
async fn uncommitted_writes_are_not_published() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "SET SPACE orders",
        "BEGIN",
        "INSERT INTO orders VALUES (7, 'dave', 1.0, 0)",
    ])
    .await;

    // Another session publishes while the transaction is open: it sees the
    // last committed state and does not disturb the transaction.
    let other = n.session();
    other.sql("PUBLISH SPACE orders").await.unwrap();
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.orders")
            .await,
        3
    );

    n.run(&["COMMIT"]).await;
    assert_eq!(n.scalar("SELECT count(*) FROM orders.orders").await, 4);
    other.sql("PUBLISH SPACE orders").await.unwrap();
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.orders")
            .await,
        4
    );
}

#[tokio::test]
async fn dropped_publication_can_be_recreated() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "PUBLISH SPACE orders",
        "DROP SPACE orders_published",
        "PUBLISH SPACE orders",
    ])
    .await;
    assert_eq!(
        n.scalar("SELECT count(*) FROM orders_published.orders")
            .await,
        3
    );
    assert_eq!(
        n.catalog
            .get_space("orders_published")
            .unwrap()
            .published_from
            .as_deref(),
        Some("orders")
    );
}

#[tokio::test]
async fn scheduled_publisher_keeps_the_copy_fresh() {
    let n = Node::new();
    seed(&n).await;
    let handle = spawn_publisher(
        n.owner.clone(),
        PublishRequest {
            space: "orders".into(),
            tables: Some(vec!["orders".into()]),
            target: None,
        },
        Duration::from_millis(50),
    );
    n.run(&["INSERT INTO orders.orders VALUES (5, 'erin', 2.0, 1)"])
        .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let count = n
            .owner
            .sql("SELECT count(*) FROM orders_published.orders")
            .await
            .ok()
            .map(|r| ints(&r, 0)[0]);
        if count == Some(4) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publisher never caught up (last count {count:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle.abort();
}
