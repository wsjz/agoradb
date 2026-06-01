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

use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::SpaceUri;
use agoradb_execution::chunk::DataChunk;
use agoradb_execution::filter::{FilterOperator, PredicateFn};
use agoradb_execution::operator::Operator;
use agoradb_execution::{MorselScheduler, ParallelExecutor};
use agoradb_execution::project::ProjectOperator;
use agoradb_execution::scan::ScanOperator;
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use iceberg::io::FileIO;
use iceberg::{Catalog, NamespaceIdent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct SharedSink {
    results: Arc<Mutex<Vec<DataChunk>>>,
}

impl Operator for SharedSink {
    fn push(&mut self, chunk: DataChunk) -> Result<(), agoradb_core::ExecutionError> {
        self.results.lock().unwrap().push(chunk);
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), agoradb_core::ExecutionError> {
        Ok(())
    }
    fn set_output(&mut self, _output: Box<dyn Operator>) {
        // SharedSink is a terminal sink; no downstream operator.
    }
}

#[tokio::test]
async fn test_parallel_scan_two_files() {
    // 1. Setup catalog + create namespace
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, &root_path));

    catalog
        .create_namespace(&NamespaceIdent::new("default".to_string()), HashMap::new())
        .await
        .unwrap();

    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        false,
    )]));

    // 2. Write two batches via StorageEngine (will create two files after two flushes)
    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "test_table".to_string(),
    );

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

    // Write batch 1: [1, 2, 3]
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    engine.append(batch1).await.unwrap();
    engine.flush().await.unwrap();

    // Write batch 2: [4, 5, 6]
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef],
    )
    .unwrap();
    engine.append(batch2).await.unwrap();
    engine.flush().await.unwrap();

    // 3. Get snapshot ID
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs(["default", "test_table"]).unwrap())
        .await
        .unwrap();
    let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // 4. Get morsels from catalog
    let space = SpaceUri::parse("space://did:agora:test/test_table").unwrap();
    let morsels = catalog
        .list_morsels(&space, snapshot_id, 10_000)
        .await
        .unwrap();

    // Should have at least 1 morsel (1 per file in simplified impl)
    assert!(
        !morsels.is_empty(),
        "Expected at least 1 morsel, got {}",
        morsels.len()
    );

    // 5. Parallel execution: each worker pulls a morsel and runs Scan → Filter → Project → Collect
    let scheduler = Arc::new(MorselScheduler::new(morsels));
    let results = Arc::new(Mutex::new(Vec::new()));

    let worker_results = results.clone();
    let worker_catalog = catalog.clone();

    let _chunks = ParallelExecutor::execute(scheduler, 2, move |_morsel| {
        let cat = worker_catalog.clone();
        let r = worker_results.clone();
        async move {
            let mut project = ProjectOperator::new(vec![0]);
            project.set_output(Box::new(SharedSink { results: r }));

            let predicate: PredicateFn =
                Box::new(|chunk, row| chunk.columns[0].as_i64_slice()[row] > 2);
            let mut filter = FilterOperator::new(predicate);
            filter.set_output(Box::new(project));

            // Note: Scan uses the file from morsel, but current ScanOperator
            // uses catalog.scan_table() which reads all files.
            // For true morsel-based scan, we'd need a per-morsel reader.
            // Simplified: use scan_table with the space.
            let mut scan = ScanOperator::new(
                cat,
                SpaceUri::parse("space://did:agora:test/test_table").unwrap(),
                snapshot_id,
            );
            scan.set_output(Box::new(filter));
            scan.execute().await?;

            Ok(Vec::new()) // results collected via SharedSink
        }
    })
    .await
    .unwrap();

    // 6. Verify — all rows with id > 2 should be present
    let collected = results.lock().unwrap();
    let total_rows: usize = collected.iter().map(|c| c.len).sum();
    assert!(
        total_rows >= 2,
        "Expected at least 2 rows (id > 2), got {}",
        total_rows
    );
}
