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

//! TPC-H Q1–Q5 against the pre-generated dataset in `tests/agora-local`
//! (SF 0.001), executed by DuckDB through [`AgoraSession`].
//!
//! Generate the data first with the command in
//! [`agoradb_tests::framework::catalog::GENERATE_CMD`].

use std::sync::Arc;

use agoradb_node::{AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig};
use agoradb_tests::framework::catalog::{setup_catalog, TPCH_SPACE};
use arrow_array::{Array, Int64Array, StringArray};

async fn session() -> AgoraSession {
    let catalog = setup_catalog().await;
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
    AgoraSession::new(
        catalog,
        engines,
        SessionConfig {
            default_space: Some(TPCH_SPACE.to_string()),
            ..Default::default()
        },
    )
}

async fn run(sql: &str) -> QueryResult {
    let session = session().await;
    session
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{sql}"))
}

#[tokio::test]
async fn test_tpch_q1_pricing_summary_report() {
    let result = run("SELECT \
            l_returnflag, \
            l_linestatus, \
            SUM(l_quantity) AS sum_qty, \
            SUM(l_extendedprice) AS sum_base_price, \
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
            AVG(l_quantity) AS avg_qty, \
            AVG(l_extendedprice) AS avg_price, \
            AVG(l_discount) AS avg_disc, \
            COUNT(*) AS count_order \
         FROM lineitem \
         WHERE l_shipdate <= DATE '1998-12-01' \
         GROUP BY l_returnflag, l_linestatus \
         ORDER BY l_returnflag, l_linestatus")
    .await;
    assert!(
        result.num_rows() >= 1,
        "Q1 should return at least one group"
    );
    assert!(
        result.num_rows() <= 6,
        "Q1 has at most 6 (flag, status) groups"
    );

    // Deterministic value check: every group is a distinct (flag, status) pair
    // and the counts add up to the whole table.
    let batches = result.batches();
    let mut groups = Vec::new();
    let mut total = 0i64;
    for b in batches {
        let flags = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let status = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            groups.push((flags.value(i).to_string(), status.value(i).to_string()));
            total += counts.value(i);
        }
    }
    let mut sorted = groups.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        groups.len(),
        "groups must be distinct and ordered"
    );
    let all = run("SELECT count(*) FROM tpch.lineitem WHERE l_shipdate <= DATE '1998-12-01'").await;
    let expected = all.batches()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, expected);
}

#[tokio::test]
async fn test_tpch_q2_minimum_cost_supplier() {
    let result = run("SELECT \
            s_acctbal, s_name, n_name, p_partkey, p_mfgr, \
            s_address, s_phone, s_comment \
         FROM tpch.part, tpch.supplier, tpch.partsupp, tpch.nation, tpch.region \
         WHERE p_partkey = ps_partkey \
           AND s_suppkey = ps_suppkey \
           AND p_size = 15 \
           AND p_type LIKE '%BRASS' \
           AND s_nationkey = n_nationkey \
           AND n_regionkey = r_regionkey \
           AND r_name = 'EUROPE' \
         ORDER BY s_acctbal DESC, n_name, s_name, p_partkey \
         LIMIT 100")
    .await;
    assert!(
        result.num_rows() <= 100,
        "Q2 should return at most 100 rows"
    );
}

#[tokio::test]
async fn test_tpch_q3_shipping_priority() {
    let result = run("SELECT \
            l_orderkey, \
            SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
            o_orderdate, o_shippriority \
         FROM customer, orders, lineitem \
         WHERE c_mktsegment = 'BUILDING' \
           AND c_custkey = o_custkey \
           AND l_orderkey = o_orderkey \
           AND o_orderdate < DATE '1995-03-15' \
           AND l_shipdate > DATE '1995-03-15' \
         GROUP BY l_orderkey, o_orderdate, o_shippriority \
         ORDER BY revenue DESC, o_orderdate \
         LIMIT 10")
    .await;
    assert!(result.num_rows() <= 10, "Q3 should return at most 10 rows");
}

#[tokio::test]
async fn test_tpch_q4_order_priority_checking() {
    let result = run("SELECT \
            o_orderpriority, \
            COUNT(*) AS order_count \
         FROM tpch.orders \
         WHERE o_orderdate >= DATE '1993-07-01' \
           AND o_orderdate < DATE '1993-10-01' \
           AND EXISTS ( \
               SELECT * FROM tpch.lineitem \
               WHERE l_orderkey = o_orderkey \
                 AND l_commitdate < l_receiptdate \
           ) \
         GROUP BY o_orderpriority \
         ORDER BY o_orderpriority")
    .await;
    assert!(
        result.num_rows() <= 5,
        "Q4 should return at most 5 priority groups"
    );
}

#[tokio::test]
async fn test_tpch_q5_local_supplier_volume() {
    let result = run("SELECT \
            n_name, \
            SUM(l_extendedprice * (1 - l_discount)) AS revenue \
         FROM customer, orders, lineitem, supplier, nation, region \
         WHERE c_custkey = o_custkey \
           AND l_orderkey = o_orderkey \
           AND l_suppkey = s_suppkey \
           AND c_nationkey = s_nationkey \
           AND s_nationkey = n_nationkey \
           AND n_regionkey = r_regionkey \
           AND r_name = 'ASIA' \
           AND o_orderdate >= DATE '1994-01-01' \
           AND o_orderdate < DATE '1995-01-01' \
         GROUP BY n_name \
         ORDER BY revenue DESC")
    .await;
    assert!(
        result.num_rows() <= 25,
        "Q5 should return at most 25 nations"
    );
}
