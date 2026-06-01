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

use agoradb_query::logical::analyzer::{Analyzer, SchemaProvider};
use agoradb_query::logical::plan::{DataType, LogicalExpr, LogicalPlan};
use std::collections::HashMap;

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

#[test]
fn test_analyzer_valid_columns() {
    let mut schemas = HashMap::new();
    let mut users_schema = HashMap::new();
    users_schema.insert("id".to_string(), DataType::Int64);
    users_schema.insert("name".to_string(), DataType::Utf8);
    schemas.insert("users".to_string(), users_schema);

    let provider = TestSchemaProvider { schemas };
    let analyzer = Analyzer::new();

    let mut plan = LogicalPlan::Scan {
        table: "users".to_string(),
        schema: Vec::new(),
    };

    let result = analyzer.analyze(&mut plan, &provider);
    assert!(result.is_ok());

    // After analysis, Scan schema should be populated
    match plan {
        LogicalPlan::Scan { schema, .. } => {
            assert_eq!(schema.len(), 2);
        }
        _ => panic!("Expected Scan"),
    }
}

#[test]
fn test_analyzer_invalid_column() {
    let mut schemas = HashMap::new();
    let mut users_schema = HashMap::new();
    users_schema.insert("id".to_string(), DataType::Int64);
    schemas.insert("users".to_string(), users_schema);

    let provider = TestSchemaProvider { schemas };
    let analyzer = Analyzer::new();

    let mut plan = LogicalPlan::Project {
        expressions: vec![(
            "nonexistent".to_string(),
            LogicalExpr::Column("nonexistent".to_string()),
        )],
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            schema: Vec::new(),
        }),
    };

    let result = analyzer.analyze(&mut plan, &provider);
    assert!(result.is_err());
}

#[test]
fn test_analyzer_table_not_found() {
    let provider = TestSchemaProvider {
        schemas: HashMap::new(),
    };
    let analyzer = Analyzer::new();

    let mut plan = LogicalPlan::Scan {
        table: "nonexistent".to_string(),
        schema: Vec::new(),
    };

    let result = analyzer.analyze(&mut plan, &provider);
    assert!(result.is_err());
}
