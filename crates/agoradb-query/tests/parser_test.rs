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

use agoradb_query::logical::plan::{AggFunction, LogicalExpr, LogicalPlan};
use agoradb_query::parser::SqlParser;

#[test]
fn test_parse_count_star() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT status, COUNT(*) FROM users GROUP BY status").unwrap();

    // Should produce: Aggregate([status, COUNT(1)] → Scan(users))
    match plan {
        LogicalPlan::Aggregate { aggregates, group_by, .. } => {
            assert_eq!(group_by.len(), 1);
            assert_eq!(aggregates.len(), 1);
            let (name, func, arg) = &aggregates[0];
            assert_eq!(name, "COUNT");
            assert!(matches!(func, AggFunction::Count));
            assert!(matches!(arg, LogicalExpr::Literal(agoradb_query::LiteralValue::Int64(1))));
        }
        _ => panic!("Expected Aggregate, got {:?}", plan),
    }
}

#[test]
fn test_parse_having() {
    let parser = SqlParser::new();
    let plan = parser.parse(
        "SELECT status, COUNT(*) FROM users GROUP BY status HAVING status = 'A'"
    ).unwrap();

    // Should produce: Filter(status = 'A') → Aggregate(...)
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            match input.as_ref() {
                LogicalPlan::Aggregate { .. } => {}
                _ => panic!("Expected Aggregate inside Filter, got {:?}", input),
            }
            // Verify predicate is a binary comparison
            assert!(matches!(predicate, LogicalExpr::BinaryOp { .. }));
        }
        _ => panic!("Expected Filter → Aggregate, got {:?}", plan),
    }
}

#[test]
fn test_parse_distinct() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT DISTINCT id, name FROM users").unwrap();

    // Should produce: Project([id, name] → Aggregate(group_by=[id, name], aggregates=[]))
    match plan {
        LogicalPlan::Project { expressions, input } => {
            assert_eq!(expressions.len(), 2);
            match input.as_ref() {
                LogicalPlan::Aggregate { group_by, aggregates, .. } => {
                    assert_eq!(group_by.len(), 2);
                    assert!(aggregates.is_empty(), "DISTINCT uses no aggregates");
                }
                _ => panic!("Expected Aggregate inside Project, got {:?}", input),
            }
        }
        _ => panic!("Expected Project → Aggregate, got {:?}", plan),
    }
}

#[test]
fn test_parse_select_from() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT id, name FROM users").unwrap();

    // Should produce: Project([id, name] → Scan(users))
    match plan {
        agoradb_query::logical::plan::LogicalPlan::Project { expressions, input } => {
            assert_eq!(expressions.len(), 2);
            assert_eq!(expressions[0].0, "id");
            assert_eq!(expressions[1].0, "name");
            match input.as_ref() {
                agoradb_query::logical::plan::LogicalPlan::Scan { table, .. } => {
                    assert_eq!(table, "users");
                }
                _ => panic!("Expected Scan, got {:?}", input),
            }
        }
        _ => panic!("Expected Project, got {:?}", plan),
    }
}

#[test]
fn test_parse_select_where() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT id FROM users WHERE id > 10").unwrap();

    // Should produce: Project([id] → Filter(id > 10) → Scan(users))
    match plan {
        agoradb_query::logical::plan::LogicalPlan::Project { input, .. } => match input.as_ref() {
            agoradb_query::logical::plan::LogicalPlan::Filter { predicate, input } => {
                match predicate {
                    agoradb_query::logical::plan::LogicalExpr::BinaryOp { op, .. } => {
                        assert!(matches!(op, agoradb_query::BinaryOp::Gt));
                    }
                    _ => panic!("Expected binary op, got {:?}", predicate),
                }
                match input.as_ref() {
                    agoradb_query::logical::plan::LogicalPlan::Scan { table, .. } => {
                        assert_eq!(table, "users");
                    }
                    _ => panic!("Expected Scan, got {:?}", input),
                }
            }
            _ => panic!("Expected Filter, got {:?}", input),
        },
        _ => panic!("Expected Project, got {:?}", plan),
    }
}
