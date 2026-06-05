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

//! End-to-end SQL pipeline integration tests.
//!
//! These tests exercise the full query pipeline against pre-generated
//! TPC-H test data stored in `tests/agora-local/`.
//!
//! Generate data first:
//!   cargo run -p test-data-gen --bin generate-test-data -- --force

use agoradb_tests::framework::catalog::setup_catalog;
use agoradb_tests::framework::runner::run_sql_pipeline;
use agoradb_tests::framework::schema::TpchSchemaProvider;
use std::collections::HashMap;

// ============================================================================
// Test 1: SELECT + WHERE on customer table
// ============================================================================

#[tokio::test]
async fn test_select_where() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // customer parquet: [c_custkey(0), c_name(1), c_address(2), c_nationkey(3), ...]
    let mut schema_map = HashMap::new();
    schema_map.insert("c_custkey".to_string(), 0);
    schema_map.insert("c_name".to_string(), 1);
    schema_map.insert("c_nationkey".to_string(), 3);

    // nationkey 0 = ALGERIA (Africa), should match ~60 customers out of 1500
    let sql = "SELECT c_custkey, c_name FROM customer WHERE c_nationkey = 0";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert!(total_rows > 0, "Should find customers from Algeria");
}

// ============================================================================
// Test 2: JOIN orders JOIN customer
// ============================================================================

#[tokio::test]
async fn test_join() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // orders parquet:   [o_orderkey(0), o_custkey(1), o_orderstatus(2), o_totalprice(3), o_orderdate(4), o_orderpriority(5), o_clerk(6), o_shippriority(7), o_comment(8)]  (9 cols)
    // customer parquet: [c_custkey(0), c_name(1), c_address(2), c_nationkey(3), c_phone(4), c_acctbal(5), c_mktsegment(6), c_comment(7)]  (8 cols)
    // Join output (left + right): orders(0..9) + customer(9..17)
    let mut schema_map = HashMap::new();
    schema_map.insert("o.orderkey".to_string(), 0);
    schema_map.insert("o.custkey".to_string(), 1); // local index for join key
    schema_map.insert("o.orderstatus".to_string(), 2);
    schema_map.insert("o.totalprice".to_string(), 3);
    schema_map.insert("c.custkey".to_string(), 0); // local index for join key
    schema_map.insert("c.name".to_string(), 10); // global: 9 + 1

    let sql = "SELECT o.orderkey, c.name FROM orders o JOIN customer c ON o.custkey = c.custkey";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(
        total_rows, 15000,
        "Should return all orders (each has a valid customer)"
    );
}

// ============================================================================
// Test 3: AGGREGATE - GROUP BY on orders
// ============================================================================

#[tokio::test]
async fn test_aggregate() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // orders parquet: [o_orderkey(0), o_custkey(1), o_orderstatus(2), o_totalprice(3), ...]
    let mut schema_map = HashMap::new();
    schema_map.insert("o_orderstatus".to_string(), 2);
    schema_map.insert("o_totalprice".to_string(), 3);
    schema_map.insert("o_orderkey".to_string(), 0);

    let sql = "SELECT o_orderstatus, COUNT(o_orderkey), SUM(o_totalprice) FROM orders GROUP BY o_orderstatus";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 3, "Should have 3 groups: O, F, P");

    // Verify each group has count > 0
    for chunk in &chunks {
        for row in 0..chunk.len {
            let count = chunk.columns[1].as_i64_slice()[row];
            assert!(count > 0, "Each group should have at least 1 order");
        }
    }
}

// ============================================================================
// Test 4: JOIN + AGGREGATE
// ============================================================================

#[tokio::test]
async fn test_join_aggregate() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // orders: 9 cols, customer: 8 cols. Join output: orders(0..9) + customer(9..17)
    let mut schema_map = HashMap::new();
    schema_map.insert("o.custkey".to_string(), 1);
    schema_map.insert("c.custkey".to_string(), 0);
    schema_map.insert("c.name".to_string(), 10); // global: 9 + 1
    schema_map.insert("o.totalprice".to_string(), 3);

    let sql = concat!(
        "SELECT c.name, SUM(o.totalprice) ",
        "FROM orders o JOIN customer c ON o.custkey = c.custkey ",
        "GROUP BY c.name"
    );
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    // orders table has only 375 unique customers (o_custkey range 1-1497)
    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(
        total_rows, 375,
        "Should have one group per distinct customer in orders"
    );

    // Verify each customer has positive total (SUM returns Float64)
    for chunk in &chunks {
        let totals = chunk.columns[1].as_f64_slice();
        for &total in totals {
            assert!(
                total > 0.0,
                "Each customer should have positive order total"
            );
        }
    }
}

// ============================================================================
// Test 5: ORDER BY
// ============================================================================

#[tokio::test]
async fn test_order_by() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // orders parquet: [o_orderkey(0), o_custkey(1), o_orderstatus(2), o_totalprice(3), ...]
    let mut schema_map = HashMap::new();
    schema_map.insert("o_orderkey".to_string(), 0);
    schema_map.insert("o_totalprice".to_string(), 3);

    // Test ORDER BY ASC
    let sql = "SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_totalprice";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert_eq!(total_rows, 15000, "Should return all orders");

    // Verify ascending order (o_totalprice is Float64 from Decimal128 parquet)
    let mut prev_price = f64::NEG_INFINITY;
    for chunk in &chunks {
        let prices = chunk.columns[1].as_f64_slice();
        for &price in prices {
            assert!(price >= prev_price, "Prices should be in ascending order");
            prev_price = price;
        }
    }

    // Test ORDER BY DESC
    let sql = "SELECT o_orderkey, o_totalprice FROM orders ORDER BY o_totalprice DESC";
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let mut prev_price = f64::INFINITY;
    for chunk in &chunks {
        let prices = chunk.columns[1].as_f64_slice();
        for &price in prices {
            assert!(price <= prev_price, "Prices should be in descending order");
            prev_price = price;
        }
    }
}

// ============================================================================
// Test 6: Max complexity - JOIN + WHERE + GROUP BY + LIMIT
// ============================================================================

#[tokio::test]
async fn test_max_complexity() {
    let catalog = setup_catalog().await;
    let schema_provider = TpchSchemaProvider::new();

    // orders: 9 cols, customer: 8 cols. Join output: orders(0..9) + customer(9..17)
    let mut schema_map = HashMap::new();
    schema_map.insert("o.orderkey".to_string(), 0);
    schema_map.insert("o.custkey".to_string(), 1);
    schema_map.insert("o.orderstatus".to_string(), 2);
    schema_map.insert("o.totalprice".to_string(), 3);
    schema_map.insert("c.custkey".to_string(), 0);
    schema_map.insert("c.name".to_string(), 10); // global: 9 + 1
    schema_map.insert("c.nationkey".to_string(), 12); // global: 9 + 3

    let sql = concat!(
        "SELECT c.name, o.orderstatus, COUNT(o.orderkey), SUM(o.totalprice) ",
        "FROM orders o JOIN customer c ON o.custkey = c.custkey ",
        "WHERE o.totalprice > 100000 ",
        "GROUP BY c.name, o.orderstatus ",
        "LIMIT 100"
    );
    let chunks = run_sql_pipeline(sql, &catalog, &schema_provider, &schema_map)
        .await
        .unwrap();

    let total_rows: usize = chunks.iter().map(|c| c.len).sum();
    assert!(
        total_rows <= 100,
        "LIMIT 100 should return at most 100 rows, got {}",
        total_rows
    );
    assert!(total_rows > 0, "Should have some matching rows");
}
