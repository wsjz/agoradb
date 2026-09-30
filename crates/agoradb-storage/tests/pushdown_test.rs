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

//! Verifies that Iceberg predicate pushdown works on data written by
//! [`StorageEngine`] (row-group statistics + row selection).

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

/// Scan `table` at `snapshot_id`, optionally applying `filter`, and return all batches.
async fn scan_with_filter(
    table: &Table,
    snapshot_id: i64,
    filter: Option<Predicate>,
) -> Vec<RecordBatch> {
    let mut builder = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_row_selection_enabled(true);
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    let stream = builder.build().unwrap().to_arrow().await.unwrap();
    stream.map(|r| r.unwrap()).collect().await
}

fn column_values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
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

#[tokio::test]
async fn test_scan_with_predicate_pushdown() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

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

    let table_ident = TableIdent::from_strs(["default", "pushdown_test"]).unwrap();
    let mut engine = StorageEngine::new_with_catalog(
        catalog.clone(),
        file_io.clone(),
        table_ident.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
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

    let table = catalog.load_table(&table_ident).await.unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // 1. Scan WITHOUT filter — should return all 5 rows
    let batches = scan_with_filter(&table, snapshot_id, None).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Unfiltered scan should return all 5 rows");

    // 2. Scan WITH filter id > 2 — should return rows with id 3, 4, 5
    let predicate = Reference::new("id").greater_than(Datum::long(2));
    let batches = scan_with_filter(&table, snapshot_id, Some(predicate)).await;
    assert_eq!(column_values(&batches, 0), vec![3, 4, 5]);

    // 3. Scan WITH filter value >= 40 — should return rows with value 40, 50
    let predicate = Reference::new("value").greater_than_or_equal_to(Datum::long(40));
    let batches = scan_with_filter(&table, snapshot_id, Some(predicate)).await;
    assert_eq!(column_values(&batches, 1), vec![40, 50]);
}

#[tokio::test]
async fn test_scan_with_predicate_no_matches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = iceberg::io::FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

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
    let table_ident = TableIdent::from_strs(["default", "empty_filter_test"]).unwrap();
    let mut engine = StorageEngine::new_with_catalog(
        catalog.clone(),
        file_io.clone(),
        table_ident.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
    );

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();

    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    let table = catalog.load_table(&table_ident).await.unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    let predicate = Reference::new("id").greater_than(Datum::long(100));
    let batches = scan_with_filter(&table, snapshot_id, Some(predicate)).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Filter with no matches should return 0 rows");
}
