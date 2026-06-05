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
use agoradb_core::SpaceUri;
use agoradb_core::{AggFunction, ExecutionPlan, JoinType, PredicateDef, Stage, StagePlan};
use agoradb_execution::executor::Executor;
use agoradb_query::{BinaryOp, PhysicalExpr, PhysicalPlan, StageBuilder};
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use iceberg::io::FileIO;
use iceberg::{Catalog, NamespaceIdent};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Helpers
// ============================================================================

async fn setup_catalog() -> (Arc<AgoraCatalog>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let root_path = temp_dir.path().to_str().unwrap().to_string();
    let file_io = FileIO::new_with_fs();
    let catalog = Arc::new(AgoraCatalog::new(file_io, &root_path));

    catalog
        .create_namespace(&NamespaceIdent::new("default".to_string()), HashMap::new())
        .await
        .unwrap();

    (catalog, temp_dir)
}

// ============================================================================
// Test 1: Pipeline stage — Scan → Filter → Project
// ============================================================================

#[tokio::test]
async fn test_executor_pipeline_scan_filter_project() {
    let (catalog, temp_dir) = setup_catalog().await;

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

    // Write data: [1, 2, 3]
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

    // Build PhysicalPlan: Project([0]) → Filter(id > 1) → Scan(test_table)
    let space = SpaceUri::parse("space://did:agora:test/test_table").unwrap();
    let physical_plan = PhysicalPlan::Project {
        expressions: vec![PhysicalExpr::Column(0)],
        input: Box::new(PhysicalPlan::Filter {
            predicate: PhysicalExpr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(PhysicalExpr::Column(0)),
                right: Box::new(PhysicalExpr::Literal(agoradb_query::LiteralValue::Int64(1))),
            },
            input: Box::new(PhysicalPlan::Scan {
                space,
                projection: None,
                filter: None,
            }),
        }),
    };

    // Build ExecutionPlan via StageBuilder
    let builder = StageBuilder::new();
    let exec_plan = builder.build(&physical_plan).unwrap();
    assert_eq!(exec_plan.stages.len(), 1);
    assert!(
        matches!(exec_plan.stages[0].plan, StagePlan::Project { .. }),
        "Expected Project at root"
    );

    // Execute via Executor
    let executor = Executor::new();
    let chunks = executor.execute(&exec_plan, &catalog).await.unwrap();

    // Verify: 2 rows (id=2, id=3)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows (id > 1)");
    assert_eq!(chunks[0].columns[0].as_i64_slice(), &[2, 3]);
}

// ============================================================================
// Test 2: HashJoin — build + probe stages
// ============================================================================

#[tokio::test]
async fn test_executor_hash_join() {
    let (catalog, temp_dir) = setup_catalog().await;

    // Create "orders" table (id: Int64, cid: Int64)
    let orders_iceberg_schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )
            .into(),
            iceberg::spec::NestedField::required(
                2,
                "cid",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )
            .into(),
        ])
        .build()
        .unwrap();

    catalog
        .create_table(
            &NamespaceIdent::new("default".to_string()),
            iceberg::TableCreation::builder()
                .name("orders".to_string())
                .schema(orders_iceberg_schema)
                .build(),
        )
        .await
        .unwrap();

    // Create "customers" table (id: Int64, name: Utf8)
    let customers_iceberg_schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )
            .into(),
            iceberg::spec::NestedField::required(
                2,
                "name",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
            .into(),
        ])
        .build()
        .unwrap();

    catalog
        .create_table(
            &NamespaceIdent::new("default".to_string()),
            iceberg::TableCreation::builder()
                .name("customers".to_string())
                .schema(customers_iceberg_schema)
                .build(),
        )
        .await
        .unwrap();

    // Write data to orders: [(1, 10), (2, 20), (3, 10)]
    let orders_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("cid", ArrowDataType::Int64, false),
    ]));
    let mut orders_engine = StorageEngine::new(
        catalog.clone(),
        orders_arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "orders".to_string(),
    );

    let orders_batch = RecordBatch::try_new(
        orders_arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 10])) as ArrayRef,
        ],
    )
    .unwrap();
    orders_engine.append(orders_batch).await.unwrap();
    orders_engine.flush().await.unwrap();

    // Write data to customers: [(10, "Alice"), (20, "Bob")]
    let customers_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("name", ArrowDataType::Utf8, false),
    ]));
    let mut customers_engine = StorageEngine::new(
        catalog.clone(),
        customers_arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "customers".to_string(),
    );

    let customers_batch = RecordBatch::try_new(
        customers_arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
        ],
    )
    .unwrap();
    customers_engine.append(customers_batch).await.unwrap();
    customers_engine.flush().await.unwrap();

    // Build PhysicalPlan: HashJoin
    let orders_space = SpaceUri::parse("space://did:agora:test/orders").unwrap();
    let customers_space = SpaceUri::parse("space://did:agora:test/customers").unwrap();

    let physical_plan = PhysicalPlan::HashJoin {
        left: Box::new(PhysicalPlan::Scan {
            space: orders_space,
            projection: None,
            filter: None,
        }),
        right: Box::new(PhysicalPlan::Scan {
            space: customers_space,
            projection: None,
            filter: None,
        }),
        left_key: 1,
        right_key: 0,
        join_type: JoinType::Inner,
    };

    // Build ExecutionPlan
    let builder = StageBuilder::new();
    let exec_plan = builder.build(&physical_plan).unwrap();
    assert_eq!(exec_plan.stages.len(), 1);
    assert!(
        matches!(exec_plan.stages[0].plan, StagePlan::HashJoin { .. }),
        "Expected HashJoin"
    );
    assert_eq!(exec_plan.stages[0].dependencies.len(), 0);

    // Execute
    let executor = Executor::new();
    let chunks = executor.execute(&exec_plan, &catalog).await.unwrap();

    // Verify: 3 joined rows
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 3, "Expected 3 joined rows");

    // Collect all rows
    let mut all_ids = Vec::new();
    let mut all_cids = Vec::new();
    let mut all_customer_ids = Vec::new();
    let mut all_names = Vec::new();

    for chunk in &chunks {
        for row in 0..chunk.len {
            all_ids.push(chunk.columns[0].as_i64_slice()[row]);
            all_cids.push(chunk.columns[1].as_i64_slice()[row]);
            all_customer_ids.push(chunk.columns[2].as_i64_slice()[row]);
            all_names.push(chunk.columns[3].as_utf8_slice()[row].to_string());
        }
    }

    // Verify all 3 expected joined rows
    assert!(all_ids.contains(&1));
    assert!(all_ids.contains(&2));
    assert!(all_ids.contains(&3));

    let idx_1 = all_ids.iter().position(|&id| id == 1).unwrap();
    assert_eq!(all_cids[idx_1], 10);
    assert_eq!(all_customer_ids[idx_1], 10);
    assert_eq!(all_names[idx_1], "Alice");

    let idx_2 = all_ids.iter().position(|&id| id == 2).unwrap();
    assert_eq!(all_cids[idx_2], 20);
    assert_eq!(all_customer_ids[idx_2], 20);
    assert_eq!(all_names[idx_2], "Bob");

    let idx_3 = all_ids.iter().position(|&id| id == 3).unwrap();
    assert_eq!(all_cids[idx_3], 10);
    assert_eq!(all_customer_ids[idx_3], 10);
    assert_eq!(all_names[idx_3], "Alice");
}

// ============================================================================
// Test 3: HashAggregate — accumulate + emit stages
// ============================================================================

#[tokio::test]
async fn test_executor_hash_aggregate() {
    let (catalog, temp_dir) = setup_catalog().await;

    // Create "sales" table (region: Utf8, amount: Int64)
    let sales_iceberg_schema = iceberg::spec::Schema::builder()
        .with_fields(vec![
            iceberg::spec::NestedField::required(
                1,
                "region",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
            )
            .into(),
            iceberg::spec::NestedField::required(
                2,
                "amount",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            )
            .into(),
        ])
        .build()
        .unwrap();

    catalog
        .create_table(
            &NamespaceIdent::new("default".to_string()),
            iceberg::TableCreation::builder()
                .name("sales".to_string())
                .schema(sales_iceberg_schema)
                .build(),
        )
        .await
        .unwrap();

    // Write data: [("US", 100), ("US", 200), ("EU", 150)]
    let sales_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("region", ArrowDataType::Utf8, false),
        Field::new("amount", ArrowDataType::Int64, false),
    ]));
    let mut sales_engine = StorageEngine::new(
        catalog.clone(),
        sales_arrow_schema.clone(),
        temp_dir.path().to_path_buf(),
        "sales".to_string(),
    );

    let sales_batch = RecordBatch::try_new(
        sales_arrow_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["US", "US", "EU"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100, 200, 150])) as ArrayRef,
        ],
    )
    .unwrap();
    sales_engine.append(sales_batch).await.unwrap();
    sales_engine.flush().await.unwrap();

    // Build PhysicalPlan: HashAggregate
    let sales_space = SpaceUri::parse("space://did:agora:test/sales").unwrap();

    let physical_plan = PhysicalPlan::HashAggregate {
        input: Box::new(PhysicalPlan::Scan {
            space: sales_space,
            projection: None,
            filter: None,
        }),
        group_exprs: vec![PhysicalExpr::Column(0)],
        agg_exprs: vec![(PhysicalExpr::Column(1), AggFunction::Sum)],
    };

    // Build ExecutionPlan
    let builder = StageBuilder::new();
    let exec_plan = builder.build(&physical_plan).unwrap();
    assert_eq!(exec_plan.stages.len(), 1);
    assert!(
        matches!(exec_plan.stages[0].plan, StagePlan::HashAggregate { .. }),
        "Expected HashAggregate"
    );
    assert_eq!(exec_plan.stages[0].dependencies.len(), 0);

    // Execute
    let executor = Executor::new();
    let chunks = executor.execute(&exec_plan, &catalog).await.unwrap();

    // Verify: 2 groups (US: 300, EU: 150)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 2, "Expected 2 groups");

    let mut all_regions = Vec::new();
    let mut all_sums = Vec::new();

    for chunk in &chunks {
        for row in 0..chunk.len {
            all_regions.push(chunk.columns[0].as_utf8_slice()[row].to_string());
            all_sums.push(chunk.columns[1].as_i64_slice()[row]);
        }
    }

    assert!(all_regions.contains(&"US".to_string()));
    assert!(all_regions.contains(&"EU".to_string()));

    let us_idx = all_regions.iter().position(|r| r == "US").unwrap();
    let eu_idx = all_regions.iter().position(|r| r == "EU").unwrap();
    assert_eq!(all_sums[us_idx], 300, "US sum should be 100 + 200 = 300");
    assert_eq!(all_sums[eu_idx], 150, "EU sum should be 150");
}

// ============================================================================
// Test 4: Parallel pipeline — Scan → Filter with parallelism > 1
// ============================================================================

#[tokio::test]
async fn test_executor_parallel_pipeline() {
    let (catalog, _temp_dir) = setup_catalog().await;

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
        .name("parallel_table".to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&NamespaceIdent::new("default".to_string()), table_creation)
        .await
        .unwrap();

    // Write two batches via two flushes (creates two parquet files)
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        false,
    )]));
    let mut engine = StorageEngine::new(
        catalog.clone(),
        arrow_schema.clone(),
        _temp_dir.path().to_path_buf(),
        "parallel_table".to_string(),
    );

    // Batch 1: [1, 2, 3]
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    engine.append(batch1).await.unwrap();
    engine.flush().await.unwrap();

    // Batch 2: [4, 5, 6]
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef],
    )
    .unwrap();
    engine.append(batch2).await.unwrap();
    engine.flush().await.unwrap();

    // Build an ExecutionPlan with forced parallelism = 2
    let space = SpaceUri::parse("space://did:agora:test/parallel_table").unwrap();
    let exec_plan = ExecutionPlan {
        stages: vec![Stage {
            id: 0,
            label: "parallel_pipeline".to_string(),
            dependencies: vec![],
            parallelism: 2, // Force parallel execution
            plan: StagePlan::Filter {
                predicate: PredicateDef::Gt {
                    column: 0,
                    value: 2,
                },
                input: Box::new(StagePlan::Scan {
                    space: space.clone(),
                    projection: None,
                    filter: None,
                }),
            },
            output: None,
        }],
    };

    // Execute via Executor
    let executor = Executor::new();
    let chunks = executor.execute(&exec_plan, &catalog).await.unwrap();

    // Verify: 4 rows (id = 3, 4, 5, 6)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows (id > 2)");

    // Collect all ids
    let mut all_ids: Vec<i64> = Vec::new();
    for chunk in &chunks {
        for row in 0..chunk.len {
            all_ids.push(chunk.columns[0].as_i64_slice()[row]);
        }
    }
    all_ids.sort();
    assert_eq!(all_ids, vec![3, 4, 5, 6]);
}
