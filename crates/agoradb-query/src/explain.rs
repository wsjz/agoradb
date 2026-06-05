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

//! EXPLAIN — return the execution plan for a SQL query as human-readable text.

use crate::logical::analyzer::Analyzer;
use crate::parser::SqlParser;
use crate::physical::planner::PhysicalPlanner;
use crate::StageBuilder;
use agoradb_core::{ExecutionError, SchemaProvider};
use std::collections::HashMap;

/// Parse, analyze and plan a SQL query, returning the execution plan tree
/// without actually running it.
pub fn explain(
    sql: &str,
    schema_provider: &dyn SchemaProvider,
    schema_map: &HashMap<String, usize>,
) -> Result<String, ExecutionError> {
    let parser = SqlParser::new();
    let mut logical_plan = parser.parse(sql)?;

    let analyzer = Analyzer::new();
    analyzer.analyze(&mut logical_plan, schema_provider)?;

    let planner = PhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan, schema_map)?;

    let stage_builder = StageBuilder::new();
    let stage_plan = stage_builder.build(&physical_plan)?;

    Ok(format!(
        "============ Physical Plan ============\n{:?}\n\n============ Stage Plan ============\n{:?}",
        physical_plan, stage_plan
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::plan::DataType;
    use std::collections::HashMap;

    struct TestSchemaProvider {
        schemas: HashMap<String, HashMap<String, DataType>>,
    }

    impl SchemaProvider for TestSchemaProvider {
        fn get_table_schema(
            &self,
            table: &str,
        ) -> Result<HashMap<String, DataType>, ExecutionError> {
            self.schemas
                .get(table)
                .cloned()
                .ok_or_else(|| ExecutionError::OperatorError(format!("Table not found: {}", table)))
        }
    }

    fn make_provider() -> TestSchemaProvider {
        let mut schemas = HashMap::new();
        let mut orders = HashMap::new();
        orders.insert("o_orderkey".to_string(), DataType::Int64);
        orders.insert("o_custkey".to_string(), DataType::Int64);
        orders.insert("o_totalprice".to_string(), DataType::Float64);
        orders.insert("o_orderstatus".to_string(), DataType::Utf8);
        schemas.insert("orders".to_string(), orders);
        TestSchemaProvider { schemas }
    }

    #[test]
    fn test_explain_select_star() {
        let provider = make_provider();
        let mut schema_map = HashMap::new();
        schema_map.insert("o_orderkey".to_string(), 0);
        schema_map.insert("o_custkey".to_string(), 1);
        schema_map.insert("o_totalprice".to_string(), 2);
        schema_map.insert("o_orderstatus".to_string(), 3);

        let plan = explain(
            "SELECT o_orderkey, o_custkey FROM orders",
            &provider,
            &schema_map,
        )
        .unwrap();
        assert!(plan.contains("Physical Plan"));
        assert!(plan.contains("Stage Plan"));
        assert!(plan.contains("Scan"));
    }

    #[test]
    fn test_explain_select_where() {
        let provider = make_provider();
        let mut schema_map = HashMap::new();
        schema_map.insert("o_orderkey".to_string(), 0);
        schema_map.insert("o_custkey".to_string(), 1);
        schema_map.insert("o_totalprice".to_string(), 2);
        schema_map.insert("o_orderstatus".to_string(), 3);

        let plan = explain(
            "SELECT o_orderkey FROM orders WHERE o_totalprice > 100000",
            &provider,
            &schema_map,
        )
        .unwrap();
        assert!(plan.contains("Filter"));
        assert!(plan.contains("Project"));
        assert!(plan.contains("Scan"));
    }

    #[test]
    fn test_explain_aggregate() {
        let provider = make_provider();
        let mut schema_map = HashMap::new();
        schema_map.insert("o_orderstatus".to_string(), 3);
        schema_map.insert("o_totalprice".to_string(), 2);

        let plan = explain(
            "SELECT o_orderstatus, COUNT(*), SUM(o_totalprice) FROM orders GROUP BY o_orderstatus",
            &provider,
            &schema_map,
        )
        .unwrap();
        assert!(plan.contains("HashAggregate"));
    }

    #[test]
    fn test_explain_keyword_lower_case() {
        let provider = make_provider();
        let mut schema_map = HashMap::new();
        schema_map.insert("o_orderkey".to_string(), 0);

        let plan = explain(
            "explain select o_orderkey from orders",
            &provider,
            &schema_map,
        )
        .unwrap();
        assert!(plan.contains("Physical Plan"));
    }
}
