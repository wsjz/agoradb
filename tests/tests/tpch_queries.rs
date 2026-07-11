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

//! TPC-H Q1-Q5 integration tests against pre-generated SF0.001 data.

use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, AgoraCatalogProvider};
use agoradb_query::AgoraSessionContext;
use iceberg::io::FileIO;

/// Load the pre-generated TPC-H dataset from `tests/agora-local`.
async fn load_tpch_catalog() -> Arc<AgoraCatalog> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let data_dir = std::path::PathBuf::from(manifest_dir).join("agora-local");
    let root_path = data_dir.to_str().unwrap().to_string();

    let file_io = FileIO::new_with_fs();
    Arc::new(AgoraCatalog::new(file_io, &root_path))
}

fn make_context(catalog: Arc<AgoraCatalog>) -> AgoraSessionContext {
    let provider = Arc::new(AgoraCatalogProvider::new(catalog));
    AgoraSessionContext::new(provider)
}

#[tokio::test]
async fn test_tpch_q1_pricing_summary_report() {
    let catalog = load_tpch_catalog().await;
    let ctx = make_context(catalog);

    let df = ctx.sql(
        "SELECT \
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
         FROM agora.default.lineitem \
         WHERE l_shipdate <= DATE '1998-12-01' \
         GROUP BY l_returnflag, l_linestatus \
         ORDER BY l_returnflag, l_linestatus"
    ).await;
    assert!(df.is_ok(), "Q1 should succeed: {:?}", df.err());

    let batches = df.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "Q1 should return at least one group");
}

#[tokio::test]
async fn test_tpch_q2_minimum_cost_supplier() {
    let catalog = load_tpch_catalog().await;
    let ctx = make_context(catalog);

    let df = ctx.sql(
        "SELECT \
            s_acctbal, s_name, n_name, p_partkey, p_mfgr, \
            s_address, s_phone, s_comment \
         FROM agora.default.part, agora.default.supplier, agora.default.partsupp, agora.default.nation, agora.default.region \
         WHERE p_partkey = ps_partkey \
           AND s_suppkey = ps_suppkey \
           AND p_size = 15 \
           AND p_type LIKE '%BRASS' \
           AND s_nationkey = n_nationkey \
           AND n_regionkey = r_regionkey \
           AND r_name = 'EUROPE' \
         ORDER BY s_acctbal DESC, n_name, s_name, p_partkey \
         LIMIT 100"
    ).await;
    assert!(df.is_ok(), "Q2 should succeed: {:?}", df.err());

    let batches = df.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows <= 100, "Q2 should return at most 100 rows");
}

#[tokio::test]
async fn test_tpch_q3_shipping_priority() {
    let catalog = load_tpch_catalog().await;
    let ctx = make_context(catalog);

    let df = ctx.sql(
        "SELECT \
            l_orderkey, \
            SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
            o_orderdate, o_shippriority \
         FROM agora.default.customer, agora.default.orders, agora.default.lineitem \
         WHERE c_mktsegment = 'BUILDING' \
           AND c_custkey = o_custkey \
           AND l_orderkey = o_orderkey \
           AND o_orderdate < DATE '1995-03-15' \
           AND l_shipdate > DATE '1995-03-15' \
         GROUP BY l_orderkey, o_orderdate, o_shippriority \
         ORDER BY revenue DESC, o_orderdate \
         LIMIT 10"
    ).await;
    assert!(df.is_ok(), "Q3 should succeed: {:?}", df.err());

    let batches = df.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows <= 10, "Q3 should return at most 10 rows");
}

#[tokio::test]
async fn test_tpch_q4_order_priority_checking() {
    let catalog = load_tpch_catalog().await;
    let ctx = make_context(catalog);

    let df = ctx.sql(
        "SELECT \
            o_orderpriority, \
            COUNT(*) AS order_count \
         FROM agora.default.orders \
         WHERE o_orderdate >= DATE '1993-07-01' \
           AND o_orderdate < DATE '1993-10-01' \
           AND EXISTS ( \
               SELECT * FROM agora.default.lineitem \
               WHERE l_orderkey = o_orderkey \
                 AND l_commitdate < l_receiptdate \
           ) \
         GROUP BY o_orderpriority \
         ORDER BY o_orderpriority"
    ).await;
    assert!(df.is_ok(), "Q4 should succeed: {:?}", df.err());

    let batches = df.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows <= 5, "Q4 should return at most 5 priority groups");
}

#[tokio::test]
async fn test_tpch_q5_local_supplier_volume() {
    let catalog = load_tpch_catalog().await;
    let ctx = make_context(catalog);

    let df = ctx.sql(
        "SELECT \
            n_name, \
            SUM(l_extendedprice * (1 - l_discount)) AS revenue \
         FROM agora.default.customer, agora.default.orders, agora.default.lineitem, agora.default.supplier, agora.default.nation, agora.default.region \
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
         ORDER BY revenue DESC"
    ).await;
    assert!(df.is_ok(), "Q5 should succeed: {:?}", df.err());

    let batches = df.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows <= 25, "Q5 should return at most 25 nations");
}
