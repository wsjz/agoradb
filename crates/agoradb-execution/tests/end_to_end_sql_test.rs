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
use agoradb_execution::runner::run_physical_plan;
use agoradb_logical::{Analyzer, DataType, SchemaProvider};
use agoradb_physical::PhysicalPlanner;
use agoradb_sql::SqlParser;
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use iceberg::io::FileIO;
use iceberg::{Catalog, NamespaceIdent};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// TestSchemaProvider
// ============================================================================

struct TestSchemaProvider {
    schemas: HashMap<String, HashMap<String, DataType>>,
}

impl SchemaProvider for TestSchemaProvider {
    fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<HashMap<String, DataType>, agoradb_core::ExecutionError> {
        self.schemas.get(table).cloned().ok_or_else(|| {
            agoradb_core::ExecutionError::OperatorError(format!("Table not found: {}", table))
        })
    }
}

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

fn create_test_schema_provider(
    table_schemas: Vec<(&str, Vec<(&str, DataType)>)>,
) -> TestSchemaProvider {
    let mut schemas = HashMap::new();
    for (table_name, columns) in table_schemas {
        let mut schema = HashMap::new();
        for (col_name, dtype) in columns {
            schema.insert(col_name.to_string(), dtype);
        }
        schemas.insert(table_name.to_string(), schema);
    }
    TestSchemaProvider { schemas }
}

/// Run the full pipeline: SQL -> Parser -> Analyzer -> Planner -> Runner
async fn run_sql_pipeline(
    sql: &str,
    catalog: &Arc<AgoraCatalog>,
    schema_provider: &TestSchemaProvider,
    schema_map: &HashMap<String, usize>,
) -> Result<Vec<agoradb_execution::chunk::DataChunk>, agoradb_core::ExecutionError> {
    // 1. Parse SQL -> LogicalPlan
    let parser = SqlParser::new();
    let mut logical_plan = parser.parse(sql)?;

    // 2. Analyze -> validated LogicalPlan
    let analyzer = Analyzer::new();
    analyzer.analyze(&mut logical_plan, schema_provider)?;

    // 3. Plan -> PhysicalPlan
    let planner = PhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan, schema_map)?;

    // 4. Execute -> DataChunks
    run_physical_plan(&physical_plan, catalog, schema_map).await
}

// ============================================================================
// Test 1: SELECT + WHERE
// ============================================================================

#[tokio::test]
async fn test_e2e_select_where() {
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

    // Schema provider
    let schema_provider =
        create_test_schema_provider(vec![("test_table", vec![("id", DataType::Int64)])]);

    // Schema map: column name -> index
    let mut schema_map = HashMap::new();
    schema_map.insert("id".to_string(), 0);

    // Run full pipeline
    let sql = "SELECT id FROM test_table WHERE id > 1";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    // Verify: 2 rows (id=2, id=3)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows (id > 1)");
    assert_eq!(chunks[0].columns[0].as_i64_slice(), &[2, 3]);
}

// ============================================================================
// Test 2: JOIN
// ============================================================================

#[tokio::test]
async fn test_e2e_join() {
    let (catalog, temp_dir) = setup_catalog().await;

    // Create "orders" table (id, cid)
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

    // Create "customers" table (id, name)
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

    // Schema provider with qualified names for alias support
    let schema_provider = create_test_schema_provider(vec![
        (
            "orders",
            vec![
                ("id", DataType::Int64),
                ("cid", DataType::Int64),
                ("o.id", DataType::Int64),
                ("o.cid", DataType::Int64),
            ],
        ),
        (
            "customers",
            vec![
                ("id", DataType::Int64),
                ("name", DataType::Utf8),
                ("c.id", DataType::Int64),
                ("c.name", DataType::Utf8),
            ],
        ),
    ]);

    // Schema map:
    // Combined join output: [orders.id(0), orders.cid(1), customers.id(2), customers.name(3)]
    // For join keys, "c.id" must map to customers.id's LOCAL index (0) since the runner
    // uses these as table-local indices. For projection, "c.name" maps to combined index 3.
    let mut schema_map = HashMap::new();
    schema_map.insert("o.id".to_string(), 0);
    schema_map.insert("o.cid".to_string(), 1);
    schema_map.insert("c.id".to_string(), 0); // local index in customers table
    schema_map.insert("c.name".to_string(), 3); // combined index

    // Run full pipeline
    let sql = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    // Verify: 3 rows
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 3, "Expected 3 joined rows");

    // Collect all rows for verification
    let mut all_ids = Vec::new();
    let mut all_names = Vec::new();

    for chunk in &chunks {
        for row in 0..chunk.len {
            all_ids.push(chunk.columns[0].as_i64_slice()[row]);
            all_names.push(chunk.columns[1].as_utf8_slice()[row].to_string());
        }
    }

    // Verify: order ids 1, 2, 3 all present
    assert!(all_ids.contains(&1));
    assert!(all_ids.contains(&2));
    assert!(all_ids.contains(&3));

    // Verify names: orders 1 and 3 -> Alice, order 2 -> Bob
    let idx_1 = all_ids.iter().position(|&id| id == 1).unwrap();
    let idx_2 = all_ids.iter().position(|&id| id == 2).unwrap();
    let idx_3 = all_ids.iter().position(|&id| id == 3).unwrap();

    assert_eq!(all_names[idx_1], "Alice");
    assert_eq!(all_names[idx_2], "Bob");
    assert_eq!(all_names[idx_3], "Alice");
}

// ============================================================================
// Test 3: AGGREGATE
// ============================================================================

#[tokio::test]
async fn test_e2e_aggregate() {
    let (catalog, temp_dir) = setup_catalog().await;

    // Create "sales" table (region, amount)
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

    // Schema provider
    let schema_provider = create_test_schema_provider(vec![(
        "sales",
        vec![("region", DataType::Utf8), ("amount", DataType::Int64)],
    )]);

    // Schema map
    let mut schema_map = HashMap::new();
    schema_map.insert("region".to_string(), 0);
    schema_map.insert("amount".to_string(), 1);

    // Run full pipeline
    let sql = "SELECT region, SUM(amount) FROM sales GROUP BY region";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

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
// Test 4: JOIN + AGGREGATE
// ============================================================================

#[tokio::test]
async fn test_e2e_join_aggregate() {
    let (catalog, temp_dir) = setup_catalog().await;

    // Create "orders" table (id, cid, amount)
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
            iceberg::spec::NestedField::required(
                3,
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
                .name("orders".to_string())
                .schema(orders_iceberg_schema)
                .build(),
        )
        .await
        .unwrap();

    // Create "customers" table (id, name)
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

    // Write data to orders: [(1, 10, 100), (2, 20, 200), (3, 10, 300)]
    let orders_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("cid", ArrowDataType::Int64, false),
        Field::new("amount", ArrowDataType::Int64, false),
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
            Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef,
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

    // Schema provider with qualified names for alias support
    let schema_provider = create_test_schema_provider(vec![
        (
            "orders",
            vec![
                ("id", DataType::Int64),
                ("cid", DataType::Int64),
                ("amount", DataType::Int64),
                ("o.id", DataType::Int64),
                ("o.cid", DataType::Int64),
                ("o.amount", DataType::Int64),
            ],
        ),
        (
            "customers",
            vec![
                ("id", DataType::Int64),
                ("name", DataType::Utf8),
                ("c.id", DataType::Int64),
                ("c.name", DataType::Utf8),
            ],
        ),
    ]);

    // Schema map:
    // Combined join output: [orders.id(0), orders.cid(1), orders.amount(2), customers.id(3), customers.name(4)]
    // For join keys: "o.cid" -> 1 (local), "c.id" -> 0 (local in customers)
    // For aggregate: "c.name" -> 4 (combined), "o.amount" -> 2 (combined)
    let mut schema_map = HashMap::new();
    schema_map.insert("o.id".to_string(), 0);
    schema_map.insert("o.cid".to_string(), 1);
    schema_map.insert("o.amount".to_string(), 2);
    schema_map.insert("c.id".to_string(), 0); // local index in customers
    schema_map.insert("c.name".to_string(), 4); // combined index

    // Run full pipeline
    let sql = "SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON o.cid = c.id GROUP BY c.name";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    // Verify: 2 groups (Alice: 400, Bob: 200)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 2, "Expected 2 groups");

    let mut all_names = Vec::new();
    let mut all_sums = Vec::new();

    for chunk in &chunks {
        for row in 0..chunk.len {
            all_names.push(chunk.columns[0].as_utf8_slice()[row].to_string());
            all_sums.push(chunk.columns[1].as_i64_slice()[row]);
        }
    }

    assert!(all_names.contains(&"Alice".to_string()));
    assert!(all_names.contains(&"Bob".to_string()));

    let alice_idx = all_names.iter().position(|n| n == "Alice").unwrap();
    let bob_idx = all_names.iter().position(|n| n == "Bob").unwrap();
    assert_eq!(
        all_sums[alice_idx], 400,
        "Alice sum should be 100 + 300 = 400"
    );
    assert_eq!(all_sums[bob_idx], 200, "Bob sum should be 200");
}
