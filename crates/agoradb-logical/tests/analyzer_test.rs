use agoradb_logical::{Analyzer, DataType, LogicalExpr, LogicalPlan, SchemaProvider};
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
