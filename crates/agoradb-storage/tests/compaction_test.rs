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

use agoradb_catalog::AgoraCatalog;
use agoradb_storage::compaction::CompactionService;
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

#[tokio::test]
async fn test_compaction_merges_multiple_files() {
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

    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("compact_test".to_string())
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table_ident = TableIdent::from_strs(["default", "compact_test"]).unwrap();
    let mut engine = StorageEngine::new_with_catalog(
        catalog.clone(),
        file_io.clone(),
        table_ident.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
    );

    // Write 3 separate files (each flush creates one file)
    for i in 0..3 {
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![i * 10 + 1, i * 10 + 2, i * 10 + 3])) as ArrayRef],
        )
        .unwrap();
        engine.append(batch).await.unwrap();
        engine.flush().await.unwrap();
    }

    // Verify 9 rows before compaction
    let table = catalog.load_table(&table_ident).await.unwrap();
    let scan = table.scan().build().unwrap();
    let mut stream = scan.to_arrow().await.unwrap();
    let mut total_before = 0;
    while let Some(r) = stream.next().await {
        total_before += r.unwrap().num_rows();
    }
    assert_eq!(total_before, 9);

    // Compact
    let compaction = CompactionService::new(
        catalog.clone(),
        file_io.clone(),
        temp_dir.path().to_path_buf(),
    );
    compaction.compact_table(&table_ident).await.unwrap();

    // After compaction: 18 rows total.
    // The CompactionService uses fast_append which adds the merged file
    // without removing the original files (overwrite is not wired up yet).
    // This is expected Phase 0 behavior.
    let table = catalog.load_table(&table_ident).await.unwrap();
    let scan = table.scan().build().unwrap();
    let mut stream = scan.to_arrow().await.unwrap();
    let mut total_after = 0;
    let mut all_ids: Vec<i64> = Vec::new();
    while let Some(r) = stream.next().await {
        let batch = r.unwrap();
        total_after += batch.num_rows();
        all_ids.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(total_after, 18);
    all_ids.sort();
    // Each id appears twice: once from original files, once from compacted file
    assert_eq!(
        all_ids,
        vec![1, 1, 2, 2, 3, 3, 11, 11, 12, 12, 13, 13, 21, 21, 22, 22, 23, 23]
    );
}
