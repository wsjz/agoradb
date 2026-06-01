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

use agoradb_catalog::AgoraCatalog;
use agoradb_core::{ExecutionError, SpaceUri};
use agoradb_execution::chunk::DataChunk;
use agoradb_execution::filter::{FilterOperator, PredicateFn};
use agoradb_execution::operator::Operator;
use agoradb_execution::project::ProjectOperator;
use agoradb_execution::scan::ScanOperator;
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use iceberg::io::FileIO;
use iceberg::{Catalog, NamespaceIdent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Shared sink that collects DataChunks via Arc<Mutex<Vec>>.
struct SharedSink {
    results: Arc<Mutex<Vec<DataChunk>>>,
}

impl Operator for SharedSink {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.results.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn set_output(&mut self, _output: Box<dyn Operator>) {
        // SharedSink is a terminal sink; no downstream operator.
    }
}

#[tokio::test]
async fn test_end_to_end_select_where() {
    // 1. Setup catalog + create namespace + write data
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, &root_path));

    catalog
        .create_namespace(&NamespaceIdent::new("default".to_string()), HashMap::new())
        .await
        .unwrap();

    // Create table
    let iceberg_schema = iceberg::spec::Schema::builder()
        .with_fields(vec![iceberg::spec::NestedField::required(
            1,
            "id",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
        )
        .into()])
        .build()
        .unwrap();

    let table_creation = iceberg::TableCreation::builder()
        .name("test_table".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&NamespaceIdent::new("default".to_string()), table_creation)
        .await
        .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        false,
    )]));
    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "test_table".to_string(),
    );

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();

    // 2. Get current snapshot ID
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs(["default", "test_table"]).unwrap())
        .await
        .unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // 3. Build operator chain: Scan → Filter(id > 1) → Project([id]) → Collect
    let results = Arc::new(Mutex::new(Vec::new()));

    let mut project = ProjectOperator::new(vec![0]);
    project.set_output(Box::new(SharedSink {
        results: results.clone(),
    }));

    let predicate: PredicateFn = Box::new(|chunk, row| chunk.columns[0].as_i64_slice()[row] > 1);
    let mut filter = FilterOperator::new(predicate);
    filter.set_output(Box::new(project));

    let space = SpaceUri::parse("space://did:agora:test/test_table").unwrap();
    let mut scan = ScanOperator::new(catalog.clone(), space, snapshot_id);
    scan.set_output(Box::new(filter));

    // 4. Execute
    scan.execute().await.unwrap();

    // 4. Verify
    let chunks = results.lock().unwrap();
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows (id > 1)");
    assert_eq!(chunks[0].columns[0].as_i64_slice(), &[2, 3]);
}
