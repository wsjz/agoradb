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

//! End-to-end OLTP behaviour of [`SqliteEngine`] through the [`QueryEngine`] contract.

use std::sync::Arc;

use agoradb_engine::{
    Capabilities, EngineError, EngineKind, QualifiedName, QueryEngine, TableSource, Value,
};
use agoradb_engine_sqlite::SqliteEngine;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;

async fn collect(engine: &SqliteEngine, sql: &str) -> Vec<RecordBatch> {
    let (_, stream) = engine.query(sql, &[]).await.unwrap();
    stream.map(|b| b.unwrap()).collect().await
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn oltp_transaction_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("orders.sqlite");
    let engine = SqliteEngine::open("orders", &path).unwrap();

    assert_eq!(engine.kind(), EngineKind::Sqlite);
    assert!(engine.capabilities().contains(Capabilities::TRANSACTIONS));
    assert!(!engine.capabilities().contains(Capabilities::READ_PARQUET));

    let created = engine
        .execute(
            "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount REAL)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(created, 0, "DDL reports zero affected rows");

    // Rows inserted inside a transaction are visible to the same connection...
    let tx = engine.begin().await.unwrap();
    let n = engine
        .execute(
            "INSERT INTO orders.orders (id, customer, amount) VALUES (?1, ?2, ?3), (?4, ?5, ?6)",
            &[
                Value::Int64(1),
                Value::from("alice"),
                Value::Float64(9.5),
                Value::Int64(2),
                Value::from("bob"),
                Value::Null,
            ],
        )
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        ids(&collect(&engine, "SELECT id FROM orders.orders ORDER BY id").await),
        vec![1, 2]
    );

    // ...and disappear on rollback.
    engine.rollback(tx).await.unwrap();
    assert!(ids(&collect(&engine, "SELECT id FROM orders.orders").await).is_empty());

    // A committed transaction persists across a fresh engine on the same file.
    let tx = engine.begin().await.unwrap();
    engine
        .execute(
            "INSERT INTO orders.orders (id, customer) VALUES (3, 'carol')",
            &[],
        )
        .await
        .unwrap();
    engine.commit(tx).await.unwrap();
    drop(engine);

    let reopened = SqliteEngine::open("orders", &path).unwrap();
    let batches = collect(&reopened, "SELECT id, customer, amount FROM orders.orders").await;
    assert_eq!(ids(&batches), vec![3]);
    let customer = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(customer.value(0), "carol");
    assert!(batches[0].column(2).is_null(0));
}

#[tokio::test]
async fn transaction_handle_bookkeeping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = SqliteEngine::open("s", &dir.path().join("s.sqlite")).unwrap();

    let tx = engine.begin().await.unwrap();
    let err = engine.begin().await.unwrap_err();
    assert!(matches!(err, EngineError::Transaction(_)));

    let bogus = agoradb_engine::TxHandle::new(tx.id() + 100);
    assert!(matches!(
        engine.commit(bogus).await,
        Err(EngineError::Transaction(_))
    ));

    engine.commit(tx).await.unwrap();
    let err = engine
        .rollback(agoradb_engine::TxHandle::new(1))
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Transaction(_)));
}

#[tokio::test]
async fn table_names_and_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = SqliteEngine::open("inv", &dir.path().join("inv.sqlite")).unwrap();
    engine
        .execute(
            "CREATE TABLE inv.items (sku TEXT NOT NULL, qty INTEGER, price DECIMAL(10,2), active BOOLEAN)",
            &[],
        )
        .await
        .unwrap();
    engine
        .execute("CREATE TABLE inv.bins (id INTEGER PRIMARY KEY)", &[])
        .await
        .unwrap();

    assert_eq!(
        engine.table_names("inv").await.unwrap(),
        vec!["bins", "items"]
    );
    assert!(engine.table_names("other").await.unwrap().is_empty());

    let schema = engine
        .table_schema(&QualifiedName::new("inv", "items"))
        .await
        .unwrap();
    let expected = Schema::new(vec![
        Field::new("sku", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    assert_eq!(schema.as_ref(), &expected);

    let err = engine
        .table_schema(&QualifiedName::new("inv", "missing"))
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::TableNotFound(_)));
}

#[tokio::test]
async fn expression_columns_are_typed_from_values() {
    let dir = tempfile::tempdir().unwrap();
    let engine = SqliteEngine::open("s", &dir.path().join("s.sqlite")).unwrap();
    let (schema, stream) = engine
        .query(
            "SELECT 1 + 1 AS two, 'x' || 'y' AS xy, NULL AS empty_col",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    let batches: Vec<_> = stream.map(|b| b.unwrap()).collect().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn large_result_streams_in_batches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = SqliteEngine::open("s", &dir.path().join("s.sqlite")).unwrap();
    engine
        .execute("CREATE TABLE s.t (id INTEGER)", &[])
        .await
        .unwrap();
    let tx = engine.begin().await.unwrap();
    for i in 0..3000 {
        engine
            .execute("INSERT INTO s.t VALUES (?1)", &[Value::Int64(i)])
            .await
            .unwrap();
    }
    engine.commit(tx).await.unwrap();

    let batches = collect(&engine, "SELECT id FROM s.t ORDER BY id").await;
    assert_eq!(batches.len(), 3, "3000 rows => 1024 + 1024 + 952");
    let all = ids(&batches);
    assert_eq!(all.len(), 3000);
    assert_eq!(all[2999], 2999);
}

#[tokio::test]
async fn attach_parquet_is_unsupported_and_own_file_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sqlite");
    let engine = SqliteEngine::open("s", &path).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let err = engine
        .attach(
            &QualifiedName::new("s", "p"),
            TableSource::ParquetFiles {
                files: vec![],
                schema,
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        EngineError::Unsupported(EngineKind::Sqlite, _)
    ));

    engine
        .attach(
            &QualifiedName::new("s", "t"),
            TableSource::SqliteFile {
                path: path.clone(),
                table: "t".to_string(),
            },
        )
        .await
        .unwrap();

    let err = engine
        .attach(
            &QualifiedName::new("s", "t"),
            TableSource::SqliteFile {
                path: dir.path().join("other.sqlite"),
                table: "t".to_string(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(..)));

    let Err(err) = engine.query("SELECT * FROM s.does_not_exist", &[]).await else {
        panic!("querying a missing table must fail");
    };
    assert!(matches!(err, EngineError::Sql(_)));
}
