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

use agoradb_query::logical::plan::{
    AggFunction as LogicalAgg, JoinType as LogicalJoinType, LogicalExpr, LogicalPlan,
};
use agoradb_query::physical::planner::PhysicalPlanner;
use agoradb_query::BinaryOp as LogicalBinOp;
use std::collections::HashMap;

#[test]
fn test_plan_join() {
    let planner = PhysicalPlanner::new();
    let mut schema = HashMap::new();
    schema.insert("a_id".to_string(), 0);
    schema.insert("b_id".to_string(), 0);

    let logical = LogicalPlan::Join {
        left: Box::new(LogicalPlan::Scan {
            table: "a".to_string(),
            alias: None,
            schema: vec![],
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "b".to_string(),
            alias: None,
            schema: vec![],
        }),
        join_type: LogicalJoinType::Inner,
        condition: LogicalExpr::BinaryOp {
            op: LogicalBinOp::Eq,
            left: Box::new(LogicalExpr::Column("a_id".to_string())),
            right: Box::new(LogicalExpr::Column("b_id".to_string())),
        },
    };

    let physical = planner.plan(&logical, &schema).unwrap();
    match physical {
        agoradb_query::PhysicalPlan::HashJoin {
            left_key,
            right_key,
            join_type,
            ..
        } => {
            assert_eq!(left_key, 0);
            assert_eq!(right_key, 0);
            assert!(matches!(join_type, agoradb_core::JoinType::Inner));
        }
        other => panic!("Expected HashJoin, got {:?}", other),
    }
}

#[test]
fn test_plan_aggregate() {
    let planner = PhysicalPlanner::new();
    let mut schema = HashMap::new();
    schema.insert("region".to_string(), 0);
    schema.insert("price".to_string(), 1);

    let logical = LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Scan {
            table: "orders".to_string(),
            alias: None,
            schema: vec![],
        }),
        group_by: vec![LogicalExpr::Column("region".to_string())],
        aggregates: vec![(
            "sum_price".to_string(),
            LogicalAgg::Sum,
            LogicalExpr::Column("price".to_string()),
        )],
    };

    let physical = planner.plan(&logical, &schema).unwrap();
    match physical {
        agoradb_query::PhysicalPlan::HashAggregate {
            group_exprs,
            agg_exprs,
            ..
        } => {
            assert_eq!(group_exprs.len(), 1);
            assert_eq!(agg_exprs.len(), 1);
        }
        other => panic!("Expected HashAggregate, got {:?}", other),
    }
}
