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
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

#[tokio::test]
async fn test_end_to_end_write_read_pipeline() {
    // 1. Create a temporary directory.
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();

    // 2. Create FileIO backed by local filesystem.
    let file_io = iceberg::io::FileIO::new_with_fs();

    // 3. Create AgoraCatalog with FileIO.
    let catalog = Arc::new(AgoraCatalog::new(file_io.clone(), &root_path));

    // 4. Create a namespace "default".
    let namespace = NamespaceIdent::new("default".to_string());
    catalog
        .create_namespace(&namespace, std::collections::HashMap::new())
        .await
        .unwrap();

    // 5. Create a table "test_table" with a simple schema (two int64 columns: "id", "value").
    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("test_table".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap();

    // Verify table exists and can be loaded.
    let table_ident = TableIdent::from_strs(["default", "test_table"]).unwrap();
    assert!(catalog.table_exists(&table_ident).await.unwrap());
    let _loaded = catalog.load_table(&table_ident).await.unwrap();

    // 6. Create a StorageEngine and write a RecordBatch with 3 rows.
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

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
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();

    engine.append(batch).await.unwrap();

    // 7. Call flush() which commits via Iceberg Transaction.
    engine.flush().await.unwrap();

    // 8. Load the table from catalog.
    let table = catalog.load_table(&table_ident).await.unwrap();

    // 9. Data files must live under the table's own data directory, not the catalog root.
    let expected_data_dir = format!("{}/default/test_table/data/", root_path);
    let scan = table.scan().build().unwrap();
    let mut files = scan.plan_files().await.unwrap();
    let mut file_count = 0;
    while let Some(task) = files.next().await {
        let task = task.unwrap();
        assert!(
            task.data_file_path().starts_with(&expected_data_dir),
            "data file {} is not under {}",
            task.data_file_path(),
            expected_data_dir
        );
        file_count += 1;
    }
    assert_eq!(file_count, 1);

    // 10. Create a TableScan and read data back.
    let scan = table.scan().build().unwrap();
    let mut stream = scan.to_arrow().await.unwrap();
    let read_batch = stream.next().await.unwrap().unwrap();

    // 11. Verify the data matches what was written.
    assert_eq!(read_batch.num_rows(), 3);
    let id_col = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.values(), &[1, 2, 3]);

    let value_col = read_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col.values(), &[10, 20, 30]);
}
