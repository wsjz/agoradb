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

//! Cross-Space queries through the federation coordinator (3.0-B exit criteria).

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_node::{
    AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig, SessionError,
};
use arrow_array::{Array, Int64Array, StringArray};
use iceberg::io::FileIO;

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<AgoraCatalog>,
    engines: Arc<EngineRegistry>,
    session: AgoraSession,
}

fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
    let session = AgoraSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
    Node {
        _dir: dir,
        catalog,
        engines,
        session,
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

/// blog (analytical, Parquet) + orders (transactional, SQLite).
async fn seed(n: &Node) {
    for sql in [
        "CREATE SPACE blog",
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR)",
        "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello'), (2, 'bob', 'World'), (3, 'alice', 'Again')",
        "CREATE SPACE orders WITH KIND = 'transactional'",
        "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount REAL)",
        "INSERT INTO orders.orders VALUES (10, 'alice', 9.5), (11, 'carol', 20.0), (12, 'alice', 0.5)",
    ] {
        n.session.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test]
async fn join_parquet_space_with_sqlite_space_returns_correct_rows() {
    let n = node();
    seed(&n).await;

    let result = n
        .session
        .sql(
            "SELECT o.customer, count(DISTINCT p.id) AS posts, sum(o.amount) AS spent \
             FROM orders.orders o JOIN blog.posts p ON p.author = o.customer \
             GROUP BY o.customer ORDER BY o.customer",
        )
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["alice"]);
    assert_eq!(ints(&result, 1), vec![2]);
    let spent = result.batches()[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    // alice has 2 posts and 2 orders (9.5 + 0.5): the join yields 4 rows.
    assert!((spent.value(0) - 20.0).abs() < 1e-9, "{}", spent.value(0));

    // Plain cross-space filter + projection.
    let result = n
        .session
        .sql(
            "SELECT p.title, o.id FROM blog.posts p, orders.orders o \
             WHERE p.author = o.customer AND o.amount > 1 ORDER BY p.id",
        )
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["Hello", "Again"]);
    assert_eq!(ints(&result, 1), vec![10, 10]);
}

#[tokio::test]
async fn explain_shows_two_pushed_down_leaves() {
    let n = node();
    seed(&n).await;
    let plan = n
        .session
        .explain(
            "SELECT p.title FROM blog.posts p JOIN orders.orders o ON p.author = o.customer WHERE o.amount > 1",
        )
        .await
        .unwrap();
    let leaves = plan.matches("VirtualExecutionPlan").count();
    assert_eq!(
        leaves, 2,
        "expected one pushed-down leaf per engine:\n{plan}"
    );
    assert!(plan.contains("name=blog"), "{plan}");
    assert!(plan.contains("name=orders"), "{plan}");
    // The SQLite side receives the filter.
    assert!(plan.contains("amount"), "{plan}");
}

#[tokio::test]
async fn two_analytical_spaces_join_pushes_single_duckdb_sql() {
    let n = node();
    for sql in [
        "CREATE SPACE a",
        "CREATE SPACE b",
        "CREATE TABLE a.t (id BIGINT NOT NULL, v VARCHAR)",
        "CREATE TABLE b.t (id BIGINT NOT NULL, w VARCHAR)",
        "INSERT INTO a.t VALUES (1, 'x'), (2, 'y')",
        "INSERT INTO b.t VALUES (2, 'yy'), (3, 'zz')",
    ] {
        n.session.sql(sql).await.unwrap();
    }
    let plan = n
        .session
        .explain("SELECT x.v, y.w FROM a.t x JOIN b.t y ON x.id = y.id")
        .await
        .unwrap();
    assert_eq!(
        plan.matches("VirtualExecutionPlan").count(),
        1,
        "both analytical spaces share one DuckDB, so the join is one leaf:\n{plan}"
    );
    let result = n
        .session
        .sql("SELECT x.v, y.w FROM a.t x JOIN b.t y ON x.id = y.id")
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["y"]);
    assert_eq!(strings(&result, 1), vec!["yy"]);
}

#[tokio::test]
async fn federated_query_inside_sqlite_tx_sees_uncommitted_rows_of_this_session() {
    let n = node();
    seed(&n).await;
    n.session.sql("SET SPACE orders").await.unwrap();
    n.session.sql("BEGIN").await.unwrap();
    n.session
        .sql("INSERT INTO orders (id, customer, amount) VALUES (13, 'bob', 1.0)")
        .await
        .unwrap();
    // The session's SQLite engine is the same connection that holds the
    // transaction, so a federated query through it observes the new row.
    let result = n
        .session
        .sql(
            "SELECT o.customer FROM orders.orders o JOIN blog.posts p ON p.author = o.customer \
             GROUP BY o.customer ORDER BY o.customer",
        )
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["alice", "bob"]);
    n.session.sql("ROLLBACK").await.unwrap();
    let result = n
        .session
        .sql(
            "SELECT o.customer FROM orders.orders o JOIN blog.posts p ON p.author = o.customer \
             GROUP BY o.customer ORDER BY o.customer",
        )
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["alice"]);
}

#[tokio::test]
async fn federated_query_pins_current_snapshots() {
    let n = node();
    seed(&n).await;
    let space = n.catalog.get_space("blog").unwrap();
    let before = n.catalog.current_snapshot_ids(&space).await.unwrap();

    // A second session sharing the node appends a new snapshot.
    let other = AgoraSession::new(
        n.catalog.clone(),
        n.engines.clone(),
        SessionConfig::default(),
    );
    other
        .sql("INSERT INTO blog.posts VALUES (4, 'carol', 'Late')")
        .await
        .unwrap();
    let after = n.catalog.current_snapshot_ids(&space).await.unwrap();
    assert_ne!(before, after);

    // A federated query issued now binds the newest snapshot.
    let result = n
        .session
        .sql("SELECT count(*) FROM blog.posts p JOIN orders.orders o ON p.author = o.customer")
        .await
        .unwrap();
    assert_eq!(
        ints(&result, 0),
        vec![5],
        "alice: 2 posts x 2 orders + carol: 1 x 1"
    );
}

#[tokio::test]
async fn federation_error_from_engine_propagates() {
    let n = node();
    seed(&n).await;
    // A column that does not exist is caught at planning time.
    let err = n
        .session
        .sql("SELECT p.nope FROM blog.posts p JOIN orders.orders o ON p.author = o.customer")
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::Federation(_)), "{err}");
    // A missing table is a catalog error before federation starts.
    let err = n
        .session
        .sql("SELECT * FROM blog.missing m JOIN orders.orders o ON m.a = o.customer")
        .await
        .unwrap_err();
    assert!(
        matches!(err, SessionError::Federation(_) | SessionError::Catalog(_)),
        "{err}"
    );
}
