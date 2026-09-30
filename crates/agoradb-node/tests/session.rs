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

//! Single-Space routing through [`AgoraSession`] (3.0-A exit criteria).

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::{AccessMode, CatalogError, EngineKind, SpaceKind};
use agoradb_node::{
    AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig, SessionError,
};
use agoradb_semantic::SemanticError;
use arrow_array::{Array, Int64Array, StringArray};
use arrow_schema::DataType;
use iceberg::io::FileIO;

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<AgoraCatalog>,
    session: AgoraSession,
}

fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
    let session = AgoraSession::new(catalog.clone(), engines, SessionConfig::default());
    Node {
        _dir: dir,
        catalog,
        session,
    }
}

fn int_column(result: &QueryResult, column: usize) -> Vec<i64> {
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

fn string_column(result: &QueryResult, column: usize) -> Vec<String> {
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

#[tokio::test]
async fn create_space_persists_kind_and_location() {
    let n = node();
    n.session.sql("CREATE SPACE blog").await.unwrap();
    n.session
        .sql("CREATE SPACE orders WITH KIND = 'transactional'")
        .await
        .unwrap();
    n.session
        .sql("CREATE SPACE orders_ro WITH LOCATION = 'orders', ACCESS = 'readonly'")
        .await
        .unwrap();

    let blog = n.catalog.get_space("blog").unwrap();
    assert_eq!(
        (blog.kind, blog.engine),
        (SpaceKind::Analytical, EngineKind::DuckDb)
    );
    let orders = n.catalog.get_space("orders").unwrap();
    assert_eq!(
        (orders.kind, orders.engine),
        (SpaceKind::Transactional, EngineKind::Sqlite)
    );
    let ro = n.catalog.get_space("orders_ro").unwrap();
    assert_eq!(
        (ro.location.as_str(), ro.access),
        ("orders", AccessMode::ReadOnly)
    );

    assert!(matches!(
        n.session.sql("CREATE SPACE blog").await,
        Err(SessionError::Catalog(CatalogError::SpaceExists(_)))
    ));
    n.session.sql("DROP SPACE orders_ro").await.unwrap();
    assert!(matches!(
        n.session.sql("SET SPACE orders_ro").await,
        Err(SessionError::Catalog(CatalogError::SpaceNotFound(_)))
    ));
}

#[tokio::test]
async fn create_table_analytical_commits_iceberg_schema() {
    let n = node();
    n.session.sql("CREATE SPACE blog").await.unwrap();
    n.session
        .sql("CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR, score DOUBLE, day DATE)")
        .await
        .unwrap();
    n.session
        .sql("CREATE TABLE IF NOT EXISTS blog.posts (id BIGINT)")
        .await
        .unwrap();

    let space = n.catalog.get_space("blog").unwrap();
    let resolved = n
        .catalog
        .resolve_table(&space, "posts", None)
        .await
        .unwrap();
    let fields = resolved.schema.fields();
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].data_type(), &DataType::Int64);
    assert!(!fields[0].is_nullable());
    assert_eq!(fields[1].data_type(), &DataType::Utf8);
    assert!(fields[1].is_nullable());
    assert_eq!(fields[3].data_type(), &DataType::Date32);

    // Empty table is queryable with its declared schema.
    let result = n
        .session
        .sql("SELECT id, title FROM blog.posts")
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 0);
    assert_eq!(
        result.schema().unwrap().field(0).data_type(),
        &DataType::Int64
    );

    n.session.sql("DROP TABLE blog.posts").await.unwrap();
    assert!(matches!(
        n.session.sql("DROP TABLE blog.posts").await,
        Err(SessionError::Catalog(CatalogError::TableNotFound(_)))
    ));
    n.session
        .sql("DROP TABLE IF EXISTS blog.posts")
        .await
        .unwrap();
}

#[tokio::test]
async fn insert_values_analytical_writes_parquet_then_select() {
    let n = node();
    n.session.sql("CREATE SPACE blog").await.unwrap();
    n.session
        .sql("CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)")
        .await
        .unwrap();

    let inserted = n
        .session
        .sql("INSERT INTO blog.posts VALUES (1, 'hello'), (2, 'world')")
        .await
        .unwrap();
    assert_eq!(inserted.rows_affected(), Some(2));
    let inserted = n
        .session
        .sql("INSERT INTO blog.posts (title, id) VALUES ('third', 3)")
        .await
        .unwrap();
    assert_eq!(inserted.rows_affected(), Some(1));

    let result = n
        .session
        .sql("SELECT id, title FROM blog.posts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![1, 2, 3]);
    assert_eq!(string_column(&result, 1), vec!["hello", "world", "third"]);

    // Two flushes => two snapshots; the newest is bound.
    let space = n.catalog.get_space("blog").unwrap();
    let resolved = n
        .catalog
        .resolve_table(&space, "posts", None)
        .await
        .unwrap();
    assert_eq!(resolved.files.len(), 2);

    assert!(matches!(
        n.session
            .sql("INSERT INTO blog.posts (title) VALUES ('no id')")
            .await,
        Err(SessionError::Unsupported(_))
    ));
    assert!(matches!(
        n.session.sql("DELETE FROM blog.posts WHERE id = 1").await,
        Err(SessionError::Unsupported(_))
    ));
}

#[tokio::test]
async fn insert_select_analytical() {
    let n = node();
    n.session.sql("CREATE SPACE blog").await.unwrap();
    n.session
        .sql("CREATE TABLE blog.posts (id BIGINT NOT NULL, title VARCHAR)")
        .await
        .unwrap();
    n.session
        .sql("CREATE TABLE blog.archive (id BIGINT NOT NULL, title VARCHAR)")
        .await
        .unwrap();
    n.session
        .sql("INSERT INTO blog.posts VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();

    let copied = n
        .session
        .sql("INSERT INTO blog.archive SELECT id, title FROM blog.posts WHERE id > 1")
        .await
        .unwrap();
    assert_eq!(copied.rows_affected(), Some(2));
    let result = n
        .session
        .sql("SELECT id FROM blog.archive ORDER BY id")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![2, 3]);

    // Joins inside one Space are pushed to DuckDB in full.
    let result = n
        .session
        .sql("SELECT count(*) FROM blog.posts p JOIN blog.archive a ON a.id = p.id")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![2]);
}

#[tokio::test]
async fn oltp_smoke() {
    let n = node();
    n.session
        .sql("CREATE SPACE orders WITH KIND = 'transactional'")
        .await
        .unwrap();
    n.session.sql("SET SPACE orders").await.unwrap();
    assert_eq!(n.session.default_space().as_deref(), Some("orders"));

    n.session
        .sql("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount REAL)")
        .await
        .unwrap();
    n.session.sql("BEGIN").await.unwrap();
    let r = n
        .session
        .sql("INSERT INTO orders (id, customer, amount) VALUES (1, 'alice', 9.5)")
        .await
        .unwrap();
    assert_eq!(r.rows_affected(), Some(1));

    // Visible inside the transaction (same connection).
    let result = n
        .session
        .sql("SELECT id, customer FROM orders")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![1]);
    assert_eq!(string_column(&result, 1), vec!["alice"]);

    assert!(matches!(
        n.session.sql("BEGIN").await,
        Err(SessionError::TransactionOpen(_))
    ));
    n.session.sql("COMMIT").await.unwrap();
    assert!(matches!(
        n.session.sql("COMMIT").await,
        Err(SessionError::NoTransaction)
    ));

    let result = n
        .session
        .sql("SELECT count(*) AS n FROM orders.orders")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![1]);

    let updated = n
        .session
        .sql("UPDATE orders SET amount = 10 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), Some(1));
}

#[tokio::test]
async fn rollback_hides_rows() {
    let n = node();
    n.session
        .sql("CREATE SPACE orders WITH KIND = 'transactional'")
        .await
        .unwrap();
    n.session.sql("SET SPACE orders").await.unwrap();
    n.session.sql("CREATE TABLE t (id INTEGER)").await.unwrap();

    n.session.sql("BEGIN").await.unwrap();
    n.session
        .sql("INSERT INTO t VALUES (1), (2)")
        .await
        .unwrap();
    assert_eq!(
        n.session.sql("SELECT id FROM t").await.unwrap().num_rows(),
        2
    );
    n.session.sql("ROLLBACK").await.unwrap();
    assert_eq!(
        n.session.sql("SELECT id FROM t").await.unwrap().num_rows(),
        0
    );

    // BEGIN needs a transactional default Space.
    n.session.sql("CREATE SPACE blog").await.unwrap();
    n.session.sql("SET SPACE blog").await.unwrap();
    assert!(matches!(
        n.session.sql("BEGIN").await,
        Err(SessionError::Unsupported(_))
    ));
    n.session.set_default_space(None);
    assert!(matches!(
        n.session.sql("BEGIN").await,
        Err(SessionError::NoDefaultSpace)
    ));
}

#[tokio::test]
async fn query_without_space_qualifier_errors() {
    let n = node();
    n.session.sql("CREATE SPACE blog").await.unwrap();
    assert!(matches!(
        n.session.sql("SELECT * FROM posts").await,
        Err(SessionError::Semantic(SemanticError::UnqualifiedTable(_)))
    ));
    assert!(matches!(
        n.session.sql("SELECT * FROM agora.blog.posts").await,
        Err(SessionError::Semantic(
            SemanticError::InvalidTableReference(_)
        ))
    ));
    assert!(matches!(
        n.session.sql("SELECT * FROM nope.posts").await,
        Err(SessionError::Catalog(CatalogError::SpaceNotFound(_)))
    ));
    assert!(matches!(
        n.session.sql("SELECT 1; SELECT 2").await,
        Err(SessionError::Semantic(SemanticError::MultipleStatements(2)))
    ));
    // A constant query needs no Space at all.
    let result = n.session.sql("SELECT 40 + 2 AS answer").await.unwrap();
    assert_eq!(result.num_rows(), 1);
}

/// Needs the DuckDB `sqlite` extension (network on first use).
#[tokio::test]
#[ignore]
async fn readonly_analytical_over_sqlite_location() {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    let config = NodeConfig {
        duckdb: agoradb_engine_duckdb::DuckDbConfig {
            allow_extension_install: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), config).unwrap());
    let session = AgoraSession::new(catalog, engines, SessionConfig::default());

    session
        .sql("CREATE SPACE orders WITH KIND = 'transactional'")
        .await
        .unwrap();
    session
        .sql("CREATE TABLE orders.orders (id INTEGER, customer TEXT)")
        .await
        .unwrap();
    session
        .sql("INSERT INTO orders.orders VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap();
    session
        .sql("CREATE SPACE orders_ro WITH LOCATION = 'orders', ACCESS = 'readonly'")
        .await
        .unwrap();
    let result = session
        .sql("SELECT count(*) FROM orders_ro.orders")
        .await
        .unwrap();
    assert_eq!(int_column(&result, 0), vec![2]);
}
