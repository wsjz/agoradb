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

//! Integration test: DataFusion can query Iceberg tables via the catalog bridge.

use std::collections::HashMap;
use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, AgoraCatalogProvider};
use datafusion::prelude::*;
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog as _, NamespaceIdent, TableCreation};

#[tokio::test]
async fn test_datafusion_reads_iceberg_table_schema() {
    // Setup: create a test catalog with one table
    let temp_dir = tempfile::tempdir().unwrap();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, temp_dir.path().to_str().unwrap()));

    let ns = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .unwrap();

    let schema = Schema::builder()
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

    // Create DataFusion context with our catalog provider
    let ctx = SessionContext::new();
    let catalog_provider = Arc::new(AgoraCatalogProvider::new(catalog));
    ctx.register_catalog("agora", catalog_provider);

    // Query the table — for an empty table, should return empty result with correct schema
    let df = ctx
        .sql("SELECT id, name FROM agora.default.test_table")
        .await;

    assert!(df.is_ok(), "Query should succeed: {:?}", df.err());
    let batches = df.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 0, "Empty table should return 0 batches");

    // Verify schema
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
