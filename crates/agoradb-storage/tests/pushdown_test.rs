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

use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::expr::Reference;
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

#[tokio::test]
async fn test_scan_with_predicate_pushdown() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("pushdown_test".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "pushdown_test".to_string(),
    );

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
        ],
    )
    .unwrap();

    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    let table_ident = TableIdent::from_strs(["default", "pushdown_test"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    let snapshot = table.metadata().current_snapshot().unwrap();
    let snapshot_id = snapshot.snapshot_id();

    let space = agoradb_core::SpaceUri::parse("space://test/pushdown_test").unwrap();

    // 1. Scan WITHOUT filter — should return all 5 rows
    let stream = catalog.scan_table(&space, snapshot_id, None).await.unwrap();
    let batches: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Unfiltered scan should return all 5 rows");

    // 2. Scan WITH filter id > 2 — should return rows with id 3, 4, 5
    let predicate = Reference::new("id").greater_than(Datum::long(2));
    let stream = catalog
        .scan_table(&space, snapshot_id, Some(predicate))
        .await
        .unwrap();
    let batches: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Filtered scan (id > 2) should return 3 rows");

    let all_ids: Vec<i64> = batches
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
    assert_eq!(all_ids, vec![3, 4, 5]);

    // 3. Scan WITH filter value >= 40 — should return rows with value 40, 50
    let predicate = Reference::new("value").greater_than_or_equal_to(Datum::long(40));
    let stream = catalog
        .scan_table(&space, snapshot_id, Some(predicate))
        .await
        .unwrap();
    let batches: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "Filtered scan (value >= 40) should return 2 rows"
    );

    let all_values: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(all_values, vec![40, 50]);
}

#[tokio::test]
async fn test_scan_with_predicate_no_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, &root_path));

    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("empty_filter_test".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "empty_filter_test".to_string(),
    );

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();

    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    let table_ident = TableIdent::from_strs(["default", "empty_filter_test"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    let space = agoradb_core::SpaceUri::parse("space://test/empty_filter_test").unwrap();

    let predicate = Reference::new("id").greater_than(Datum::long(100));
    let stream = catalog
        .scan_table(&space, snapshot_id, Some(predicate))
        .await
        .unwrap();
    let batches: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Filter with no matches should return 0 rows");
}
