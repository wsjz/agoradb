// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use agoradb_core::ExecutionError;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// AgoraDB SQL statement.
pub enum AgoraStatement {
    /// Standard SQL statement (delegated to DataFusion).
    Sql(Vec<datafusion::sql::sqlparser::ast::Statement>),
    /// Agora extension: CREATE SPACE.
    CreateSpace {
        name: String,
        properties: std::collections::HashMap<String, String>,
    },
}

/// Wraps DataFusion/sqlparser with Agora-specific syntax hooks.
pub struct AgoraSQLParser;

impl AgoraSQLParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(
        &self,
        sql: &str,
    ) -> Result<Vec<AgoraStatement>, ExecutionError> {
        let trimmed = sql.trim_start();
        let upper = trimmed.to_uppercase();

        // Detect Agora-specific syntax: CREATE SPACE
        if upper.starts_with("CREATE SPACE") {
            return Ok(vec![self.parse_create_space(trimmed)?]);
        }

        // Everything else: delegate to standard sqlparser
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| {
            ExecutionError::OperatorError(format!("SQL parse error: {e}"))
        })?;

        Ok(vec![AgoraStatement::Sql(statements)])
    }

    fn parse_create_space(
        &self,
        sql: &str,
    ) -> Result<AgoraStatement, ExecutionError> {
        let mut tokens = sql.split_whitespace();

        let create = tokens.next().ok_or_else(|| {
            ExecutionError::OperatorError("Expected CREATE".to_string())
        })?;
        let space = tokens.next().ok_or_else(|| {
            ExecutionError::OperatorError("Expected SPACE".to_string())
        })?;

        if create.to_uppercase() != "CREATE" || space.to_uppercase() != "SPACE" {
            return Err(ExecutionError::OperatorError(
                "Expected CREATE SPACE".to_string(),
            ));
        }

        let name = tokens.next().ok_or_else(|| {
            ExecutionError::OperatorError("Expected space name".to_string())
        })?;

        let mut properties = std::collections::HashMap::new();

        if let Some(with) = tokens.next() {
            if with.to_uppercase() == "WITH" {
                let rest: String = tokens.collect::<Vec<_>>().join(" ");
                let mut i = 0;
                while i < rest.len() {
                    // Find the '=' for this property
                    let eq_pos = match rest[i..].find('=') {
                        Some(pos) => i + pos,
                        None => break,
                    };
                    let key = rest[i..eq_pos].trim().to_uppercase();

                    // Find the comma that separates properties, but skip commas inside quotes
                    let mut value_end = eq_pos + 1;
                    let mut in_quote: Option<char> = None;
                    while value_end < rest.len() {
                        let c = rest.as_bytes()[value_end] as char;
                        match (in_quote, c) {
                            (None, '\'' | '"') => in_quote = Some(c),
                            (Some(q), c) if q == c => in_quote = None,
                            (None, ',') => break,
                            _ => {}
                        }
                        value_end += 1;
                    }

                    let value = rest[eq_pos + 1..value_end]
                        .trim()
                        .trim_matches('\'')
                        .trim_matches('"');
                    properties.insert(key, value.to_string());

                    // Move past the comma (if present)
                    i = value_end + 1;
                }
            }
        }

        Ok(AgoraStatement::CreateSpace {
            name: name.to_string(),
            properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_space_basic() {
        let parser = AgoraSQLParser::new();
        let stmts = parser.parse("CREATE SPACE blog").unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            AgoraStatement::CreateSpace { name, properties } => {
                assert_eq!(name, "blog");
                assert!(properties.is_empty());
            }
            _ => panic!("Expected CreateSpace"),
        }
    }

    #[test]
    fn test_parse_create_space_with_properties() {
        let parser = AgoraSQLParser::new();
        let stmts = parser
            .parse(
                "CREATE SPACE blog WITH STORAGE = 'disk,s3', MODES = 'TABLE,VECTOR'",
            )
            .unwrap();
        match &stmts[0] {
            AgoraStatement::CreateSpace { name, properties } => {
                assert_eq!(name, "blog");
                assert_eq!(
                    properties.get("STORAGE"),
                    Some(&"disk,s3".to_string())
                );
                assert_eq!(
                    properties.get("MODES"),
                    Some(&"TABLE,VECTOR".to_string())
                );
            }
            _ => panic!("Expected CreateSpace"),
        }
    }

    #[test]
    fn test_parse_standard_sql() {
        let parser = AgoraSQLParser::new();
        let stmts = parser.parse("SELECT id FROM users").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], AgoraStatement::Sql(_)));
    }
}
