use agoradb_logical::plan::{
    AggFunction as LogicalAgg, BinaryOp as LogicalBinOp, JoinType as LogicalJoinType, LogicalExpr,
    LogicalPlan,
};
use agoradb_physical::planner::PhysicalPlanner;
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
            schema: vec![],
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "b".to_string(),
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
        agoradb_core::plan::PhysicalPlan::HashJoin {
            left_key,
            right_key,
            join_type,
            ..
        } => {
            assert_eq!(left_key, 0);
            assert_eq!(right_key, 0);
            assert!(matches!(join_type, agoradb_core::plan::JoinType::Inner));
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
        agoradb_core::plan::PhysicalPlan::HashAggregate {
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
