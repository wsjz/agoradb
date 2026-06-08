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

//! Integration tests: DataFusion queries against Iceberg tables via the catalog bridge.

use std::collections::HashMap;
use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, AgoraCatalogProvider};
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::TableProvider;
use datafusion::prelude::*;
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog as _, NamespaceIdent, TableCreation, TableIdent};

// ------------------------------------------------------------------
// Helper: create a catalog with namespace and one table, write data
// ------------------------------------------------------------------

async fn setup_catalog_with_table(
    table_name: &str,
    arrow_schema: Arc<Schema>,
    batch: RecordBatch,
) -> (Arc<AgoraCatalog>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));

    let ns = NamespaceIdent::new("default".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    let iceberg_schema = arrow_to_iceberg_schema(&arrow_schema);
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .build();
    catalog.create_table(&ns, creation).await.unwrap();

    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema,
        temp_dir.path().to_path_buf(),
        table_name.to_string(),
    );
    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    (catalog, temp_dir)
}

fn arrow_to_iceberg_schema(arrow_schema: &Schema) -> IcebergSchema {
    let fields: Vec<iceberg::spec::NestedFieldRef> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let primitive = match field.data_type() {
                DataType::Int64 => PrimitiveType::Long,
                DataType::Utf8 => PrimitiveType::String,
                _ => panic!("Unsupported type for test helper: {:?}", field.data_type()),
            };
            NestedField::required((idx + 1) as i32, field.name(), Type::Primitive(primitive)).into()
        })
        .collect();
    IcebergSchema::builder().with_fields(fields).build().unwrap()
}

fn make_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_test_batch(schema: Arc<Schema>, count: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((1..=count as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie", "diana", "eve"].into_iter().cycle().take(count).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Int64Array::from((10..=10 + count as i64 - 1).collect::<Vec<_>>())) as ArrayRef,
        ],
    )
    .unwrap()
}

// ------------------------------------------------------------------
// Test: empty table schema (original)
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_reads_iceberg_table_schema() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));

    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .unwrap();

    let schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();

    let creation = TableCreation::builder()
        .name("test_table".to_string())
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.unwrap();

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT id, name FROM agora.default.test_table")
        .await;

    assert!(df.is_ok(), "Query should succeed: {:?}", df.err());
    let batches = df.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 0, "Empty table should return 0 batches");

    let schema = ctx
        .catalog("agora")
        .unwrap()
        .schema("default")
        .unwrap()
        .table("test_table")
        .await
        .unwrap()
        .unwrap()
        .schema();

    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

// ------------------------------------------------------------------
// Test: SELECT * from table with data
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_select_all_with_data() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT * FROM agora.default.users")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Should return all 5 rows");

    // Verify schema
    let first = &batches[0];
    assert_eq!(first.schema().fields().len(), 3);
    assert_eq!(first.schema().field(0).name(), "id");
    assert_eq!(first.schema().field(1).name(), "name");
    assert_eq!(first.schema().field(2).name(), "value");
}

// ------------------------------------------------------------------
// Test: projection pushdown via SQL
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_projection_pushdown() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT name FROM agora.default.users")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // Should only have one column: name
    let first = &batches[0];
    assert_eq!(first.schema().fields().len(), 1);
    assert_eq!(first.schema().field(0).name(), "name");
    assert_eq!(first.schema().field(0).data_type(), &DataType::Utf8);
}

// ------------------------------------------------------------------
// Test: LIMIT
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_limit() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT * FROM agora.default.users LIMIT 3")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "LIMIT 3 should return exactly 3 rows");
}

// ------------------------------------------------------------------
// Test: WHERE filtering
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_where_filter() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT id, name FROM agora.default.users WHERE id > 2")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "WHERE id > 2 should return 3 rows");

    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

// ------------------------------------------------------------------
// Test: ORDER BY
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_order_by() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT id FROM agora.default.users ORDER BY id DESC")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![5, 4, 3, 2, 1]);
}

// ------------------------------------------------------------------
// Test: COUNT(*) aggregation
// ------------------------------------------------------------------

#[tokio::test]
async fn test_datafusion_count_star() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    let df = ctx
        .sql("SELECT COUNT(*) AS cnt FROM agora.default.users")
        .await
        .expect("Query should succeed");

    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let counts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 5);
}

// ------------------------------------------------------------------
// Test: IcebergTableProvider::scan directly
// ------------------------------------------------------------------

#[tokio::test]
async fn test_iceberg_table_provider_scan_with_data() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema.clone(), batch).await;

    // Load the table and create provider
    let table_ident = TableIdent::from_strs(["default", "users"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    let provider = agoradb_catalog::IcebergTableProvider::new(table).unwrap();

    // Verify schema
    assert_eq!(provider.schema().fields().len(), 3);

    // Call scan directly
    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan should succeed");

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "scan should return all 5 rows");
}

// ------------------------------------------------------------------
// Test: IcebergTableProvider::scan with projection
// ------------------------------------------------------------------

#[tokio::test]
async fn test_iceberg_table_provider_scan_projection() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let table_ident = TableIdent::from_strs(["default", "users"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    let provider = agoradb_catalog::IcebergTableProvider::new(table).unwrap();

    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = provider
        .scan(&state, Some(&vec![2, 0]), &[], None) // project value, id
        .await
        .expect("scan should succeed");

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();

    let first = &batches[0];
    assert_eq!(first.schema().fields().len(), 2);
    assert_eq!(first.schema().field(0).name(), "value"); // projected order
    assert_eq!(first.schema().field(1).name(), "id");
}

// ------------------------------------------------------------------
// Test: IcebergTableProvider::scan with limit
// ------------------------------------------------------------------

#[tokio::test]
async fn test_iceberg_table_provider_scan_limit() {
    let schema = make_test_schema();
    let batch = make_test_batch(schema.clone(), 5);
    let (catalog, _temp) = setup_catalog_with_table("users", schema, batch).await;

    let table_ident = TableIdent::from_strs(["default", "users"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    let provider = agoradb_catalog::IcebergTableProvider::new(table).unwrap();

    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], Some(2))
        .await
        .expect("scan should succeed");

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "scan with limit=2 should return 2 rows");
}
