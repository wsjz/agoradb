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

//! [`DuckDbEngine`] through the [`QueryEngine`] contract.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use agoradb_engine::{
    Capabilities, EngineError, EngineKind, QualifiedName, QueryEngine, TableSource, Value,
};
use agoradb_engine_duckdb::{DuckDbConfig, DuckDbEngine};
use arrow_array::{Array, Date32Array, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;

fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn simple_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        simple_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn write_parquet(dir: &Path, file: &str, batch: &RecordBatch) -> PathBuf {
    let path = dir.join(file);
    let f = std::fs::File::create(&path).unwrap();
    let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
    path
}

fn engine(dir: &Path) -> DuckDbEngine {
    DuckDbEngine::open_in_memory(DuckDbConfig {
        threads: Some(2),
        extension_dir: Some(dir.join("ext")),
        ..Default::default()
    })
    .unwrap()
}

async fn collect(engine: &DuckDbEngine, sql: &str) -> Vec<RecordBatch> {
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
async fn read_parquet_works() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_parquet(
        dir.path(),
        "a.parquet",
        &simple_batch(vec![1, 2, 3], vec!["a", "b", "c"]),
    );
    let engine = engine(dir.path());
    assert_eq!(engine.kind(), EngineKind::DuckDb);
    assert!(engine.capabilities().contains(Capabilities::READ_PARQUET));

    let sql = format!(
        "SELECT count(*) AS n FROM read_parquet('{}')",
        file.display()
    );
    let batches = collect(&engine, &sql).await;
    assert_eq!(ids(&batches), vec![3]);
}

#[tokio::test]
async fn attach_parquet_files_creates_view_and_query_streams() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = write_parquet(
        dir.path(),
        "1.parquet",
        &simple_batch(vec![1, 2, 3], vec!["a", "b", "c"]),
    );
    let f2 = write_parquet(
        dir.path(),
        "2.parquet",
        &simple_batch(vec![4, 5, 6], vec!["d", "e", "f"]),
    );
    let engine = engine(dir.path());

    engine
        .attach(
            &QualifiedName::new("blog", "posts"),
            TableSource::ParquetFiles {
                files: vec![f1, f2],
                schema: simple_schema(),
            },
        )
        .await
        .unwrap();

    assert_eq!(engine.table_names("blog").await.unwrap(), vec!["posts"]);
    let batches = collect(
        &engine,
        "SELECT id, name FROM blog.posts WHERE id > 2 ORDER BY id",
    )
    .await;
    assert_eq!(ids(&batches), vec![3, 4, 5, 6]);

    // Parameters are bound positionally.
    let (_, stream) = engine
        .query(
            "SELECT id FROM blog.posts WHERE name = ? ORDER BY id",
            &[Value::from("e")],
        )
        .await
        .unwrap();
    let batches: Vec<_> = stream.map(|b| b.unwrap()).collect().await;
    assert_eq!(ids(&batches), vec![5]);
}

#[tokio::test]
async fn attach_empty_file_list_yields_typed_empty_view() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("when", DataType::Date32, true),
        Field::new("amount", DataType::Decimal128(15, 2), true),
    ]));
    engine
        .attach(
            &QualifiedName::new("s", "empty"),
            TableSource::ParquetFiles {
                files: vec![],
                schema: schema.clone(),
            },
        )
        .await
        .unwrap();

    let got = engine
        .table_schema(&QualifiedName::new("s", "empty"))
        .await
        .unwrap();
    let types: Vec<_> = got.fields().iter().map(|f| f.data_type().clone()).collect();
    assert_eq!(
        types,
        vec![
            DataType::Int64,
            DataType::Date32,
            DataType::Decimal128(15, 2)
        ]
    );
    let batches = collect(&engine, "SELECT * FROM s.empty").await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn reattach_same_files_is_noop_and_new_files_replace() {
    let dir = tempfile::tempdir().unwrap();
    let f1 = write_parquet(dir.path(), "1.parquet", &simple_batch(vec![1], vec!["a"]));
    let f2 = write_parquet(dir.path(), "2.parquet", &simple_batch(vec![2], vec!["b"]));
    let engine = engine(dir.path());
    let name = QualifiedName::new("s", "t");

    for _ in 0..2 {
        engine
            .attach(
                &name,
                TableSource::ParquetFiles {
                    files: vec![f1.clone()],
                    schema: simple_schema(),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(ids(&collect(&engine, "SELECT id FROM s.t").await), vec![1]);

    // A new snapshot (different file list) replaces the view.
    engine
        .attach(
            &name,
            TableSource::ParquetFiles {
                files: vec![f1.clone(), f2.clone()],
                schema: simple_schema(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&collect(&engine, "SELECT id FROM s.t ORDER BY id").await),
        vec![1, 2]
    );

    engine.detach(&name).await.unwrap();
    assert!(engine.table_names("s").await.unwrap().is_empty());
}

#[tokio::test]
async fn query_arrow_schema_matches_declared_schema() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("amount", DataType::Decimal128(15, 2), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Date32Array::from(vec![19_000, 19_001])),
            Arc::new(
                Decimal128Array::from(vec![12345, 67890])
                    .with_precision_and_scale(15, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();
    let file = write_parquet(dir.path(), "typed.parquet", &batch);
    let engine = engine(dir.path());
    let name = QualifiedName::new("s", "typed");
    engine
        .attach(
            &name,
            TableSource::ParquetFiles {
                files: vec![file],
                schema: schema.clone(),
            },
        )
        .await
        .unwrap();

    let (got, stream) = engine
        .query("SELECT id, name, day, amount FROM s.typed ORDER BY id", &[])
        .await
        .unwrap();
    let got_types: Vec<_> = got.fields().iter().map(|f| f.data_type().clone()).collect();
    let want_types: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    assert_eq!(got_types, want_types);

    let batches: Vec<_> = stream.map(|b| b.unwrap()).collect().await;
    assert_eq!(batches.len(), 1);
    let amounts = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.value(1), 67890);
    assert_eq!(engine.table_schema(&name).await.unwrap().fields().len(), 4);
}

#[tokio::test]
async fn execute_reports_affected_rows_and_begin_is_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_eq!(
        engine
            .execute("CREATE TABLE scratch (v INTEGER)", &[])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        engine
            .execute(
                "INSERT INTO scratch VALUES (?), (?)",
                &[Value::Int64(1), Value::Int64(2)]
            )
            .await
            .unwrap(),
        2
    );
    assert!(matches!(
        engine.begin().await,
        Err(EngineError::Unsupported(EngineKind::DuckDb, _))
    ));
    let Err(err) = engine.query("SELECT * FROM nope.nothing_here", &[]).await else {
        panic!("querying a missing table must fail");
    };
    assert!(matches!(err, EngineError::Sql(_)));
    assert!(matches!(
        engine
            .table_schema(&QualifiedName::new("nope", "nothing_here"))
            .await,
        Err(EngineError::TableNotFound(_))
    ));
}

#[tokio::test]
async fn attach_arrow_batches_creates_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    engine
        .attach(
            &QualifiedName::new("remote", "peers"),
            TableSource::ArrowBatches {
                schema: simple_schema(),
                batches: vec![
                    simple_batch(vec![1, 2], vec!["a", "b"]),
                    simple_batch(vec![3], vec!["c"]),
                ],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&collect(&engine, "SELECT id FROM remote.peers ORDER BY id DESC").await),
        vec![3, 2, 1]
    );
}

#[tokio::test]
async fn attach_sqlite_file_requires_extension() {
    let dir = tempfile::tempdir().unwrap();
    // A private, empty extension directory and installs disabled: LOAD must fail cleanly.
    let engine = engine(dir.path());
    assert!(!engine.capabilities().contains(Capabilities::READ_SQLITE));
    let err = engine
        .attach(
            &QualifiedName::new("orders_ro", "orders"),
            TableSource::SqliteFile {
                path: dir.path().join("orders.sqlite"),
                table: "orders".to_string(),
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::ExtensionUnavailable(ref ext, _) if ext == "sqlite"),
        "unexpected error {err:?}"
    );
}

/// Needs the DuckDB `sqlite` extension, which is downloaded on first use.
/// Run with: `cargo test -p agoradb-engine-duckdb --features sqlite-scanner-tests -- --ignored`
#[tokio::test]
#[ignore]
#[cfg(feature = "sqlite-scanner-tests")]
async fn attach_sqlite_file_reads_rows() {
    let dir = tempfile::tempdir().unwrap();
    let sqlite_path = dir.path().join("orders.sqlite");
    {
        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT);\
             INSERT INTO orders VALUES (1, 'alice'), (2, 'bob');",
        )
        .unwrap();
    }
    let engine = DuckDbEngine::open_in_memory(DuckDbConfig {
        allow_extension_install: true,
        ..Default::default()
    })
    .unwrap();
    engine
        .attach(
            &QualifiedName::new("orders_ro", "orders"),
            TableSource::SqliteFile {
                path: sqlite_path,
                table: "orders".to_string(),
            },
        )
        .await
        .unwrap();
    assert!(engine.capabilities().contains(Capabilities::READ_SQLITE));
    assert_eq!(
        ids(&collect(&engine, "SELECT id FROM orders_ro.orders ORDER BY id").await),
        vec![1, 2]
    );
}
