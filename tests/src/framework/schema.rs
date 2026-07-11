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

//! TPC-H schema definitions for integration tests.
//!
//! Provides analyzer `DataType` mappings for all TPC-H tables used in
//! end-to-end tests. Note that `agoradb_core::DataType` currently only
//! supports Int64, Float64, Boolean, and Utf8; columns that are
//! Decimal128 or Date32 in Parquet are mapped to Int64 here to match
//! the conventions used by existing tests.

use agoradb_core::{DataType, SchemaProvider};
use std::collections::HashMap;

/// Schema provider that resolves TPC-H table schemas.
pub struct TpchSchemaProvider {
    schemas: HashMap<String, HashMap<String, DataType>>,
}

impl TpchSchemaProvider {
    /// Create a new provider with all TPC-H tables.
    pub fn new() -> Self {
        let mut schemas = HashMap::new();
        schemas.insert("region".to_string(), region_schema());
        schemas.insert("nation".to_string(), nation_schema());
        schemas.insert("customer".to_string(), customer_schema());
        schemas.insert("orders".to_string(), orders_schema());
        schemas.insert("lineitem".to_string(), lineitem_schema());
        Self { schemas }
    }

    /// Get the schema for a specific table.
    pub fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<HashMap<String, DataType>, agoradb_core::ExecutionError> {
        self.schemas.get(table).cloned().ok_or_else(|| {
            agoradb_core::ExecutionError::OperatorError(format!("Table not found: {}", table))
        })
    }
}

impl SchemaProvider for TpchSchemaProvider {
    fn get_table_schema(
        &self,
        table: &str,
    ) -> Result<HashMap<String, DataType>, agoradb_core::ExecutionError> {
        self.schemas.get(table).cloned().ok_or_else(|| {
            agoradb_core::ExecutionError::OperatorError(format!("Table not found: {}", table))
        })
    }
}

impl Default for TpchSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn region_schema() -> HashMap<String, DataType> {
    let mut s = HashMap::new();
    s.insert("r_regionkey".to_string(), DataType::Int64);
    s.insert("r_name".to_string(), DataType::Utf8);
    s.insert("r_comment".to_string(), DataType::Utf8);
    s
}

fn nation_schema() -> HashMap<String, DataType> {
    let mut s = HashMap::new();
    s.insert("n_nationkey".to_string(), DataType::Int64);
    s.insert("n_name".to_string(), DataType::Utf8);
    s.insert("n_regionkey".to_string(), DataType::Int64);
    s.insert("n_comment".to_string(), DataType::Utf8);
    s
}

fn customer_schema() -> HashMap<String, DataType> {
    let mut s = HashMap::new();
    s.insert("c_custkey".to_string(), DataType::Int64);
    s.insert("c_name".to_string(), DataType::Utf8);
    s.insert("c_address".to_string(), DataType::Utf8);
    s.insert("c_nationkey".to_string(), DataType::Int64);
    s.insert("c_phone".to_string(), DataType::Utf8);
    s.insert("c_acctbal".to_string(), DataType::Int64);
    s.insert("c_mktsegment".to_string(), DataType::Utf8);
    s.insert("c_comment".to_string(), DataType::Utf8);
    s
}

fn orders_schema() -> HashMap<String, DataType> {
    let mut s = HashMap::new();
    s.insert("o_orderkey".to_string(), DataType::Int64);
    s.insert("o_custkey".to_string(), DataType::Int64);
    s.insert("o_orderstatus".to_string(), DataType::Utf8);
    s.insert("o_totalprice".to_string(), DataType::Int64);
    s.insert("o_orderdate".to_string(), DataType::Int64);
    s.insert("o_orderpriority".to_string(), DataType::Utf8);
    s.insert("o_clerk".to_string(), DataType::Utf8);
    s.insert("o_shippriority".to_string(), DataType::Int64);
    s.insert("o_comment".to_string(), DataType::Utf8);
    s
}

fn lineitem_schema() -> HashMap<String, DataType> {
    let mut s = HashMap::new();
    s.insert("l_orderkey".to_string(), DataType::Int64);
    s.insert("l_partkey".to_string(), DataType::Int64);
    s.insert("l_suppkey".to_string(), DataType::Int64);
    s.insert("l_linenumber".to_string(), DataType::Int64);
    s.insert("l_quantity".to_string(), DataType::Int64);
    s.insert("l_extendedprice".to_string(), DataType::Int64);
    s.insert("l_discount".to_string(), DataType::Int64);
    s.insert("l_tax".to_string(), DataType::Int64);
    s.insert("l_returnflag".to_string(), DataType::Utf8);
    s.insert("l_linestatus".to_string(), DataType::Utf8);
    s.insert("l_shipdate".to_string(), DataType::Int64);
    s.insert("l_commitdate".to_string(), DataType::Int64);
    s.insert("l_receiptdate".to_string(), DataType::Int64);
    s.insert("l_shipinstruct".to_string(), DataType::Utf8);
    s.insert("l_shipmode".to_string(), DataType::Utf8);
    s.insert("l_comment".to_string(), DataType::Utf8);
    s
}
