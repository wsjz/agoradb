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

use crate::physical::plan::{PhysicalExpr, PhysicalPlan};
use crate::BinaryOp;
use agoradb_core::{AggFunction, ExecutionError, ExecutionPlan, SortDirection, Stage, StagePlan};

/// Builds an [`ExecutionPlan`] from a [`PhysicalPlan`].
///
/// In the new design, each top-level PhysicalPlan node becomes one Stage
/// with a complete operator tree. Breaker splitting is done by PipelineBuilder
/// in the execution layer.
pub struct StageBuilder;

impl StageBuilder {
    pub fn new() -> Self {
        Self
    }

    /// Convert a PhysicalPlan into an ExecutionPlan.
    ///
    /// In the new design, each top-level PhysicalPlan node becomes one Stage
    /// with a complete operator tree. Breaker splitting is done by PipelineBuilder
    /// in the execution layer.
    pub fn build(&self, plan: &PhysicalPlan) -> Result<ExecutionPlan, ExecutionError> {
        let mut stages = Vec::new();
        self.build_inner(plan, &mut stages)?;
        Ok(ExecutionPlan { stages })
    }

    fn build_inner(
        &self,
        plan: &PhysicalPlan,
        stages: &mut Vec<Stage>,
    ) -> Result<(), ExecutionError> {
        let id = stages.len();
        let stage_plan = self.physical_to_stage_plan(plan)?;
        // Sort must be single-threaded: global ordering requires all data
        // in one place. Parallel sort would produce N sorted runs, not a
        // single globally sorted output.
        let parallelism = if matches!(plan, PhysicalPlan::Sort { .. }) {
            1
        } else {
            Self::default_parallelism()
        };
        stages.push(Stage {
            id,
            label: self.label_for_plan(plan),
            dependencies: vec![], // v1: no cross-stage Exchange within single-node
            parallelism,
            plan: stage_plan,
            output: None,
        });
        Ok(())
    }

    fn label_for_plan(&self, plan: &PhysicalPlan) -> String {
        match plan {
            PhysicalPlan::Scan { .. } => "scan".to_string(),
            PhysicalPlan::Filter { .. } => "filter".to_string(),
            PhysicalPlan::Project { .. } => "project".to_string(),
            PhysicalPlan::Limit { .. } => "limit".to_string(),
            PhysicalPlan::HashJoin { .. } => "hash_join".to_string(),
            PhysicalPlan::HashAggregate { .. } => "hash_aggregate".to_string(),
            PhysicalPlan::Sort { .. } => "sort".to_string(),
        }
    }

    /// Convert a PhysicalPlan subtree into a StagePlan.
    ///
    /// This recursively processes the entire tree, including breakers.
    /// The resulting StagePlan is a complete operator tree.
    fn physical_to_stage_plan(&self, plan: &PhysicalPlan) -> Result<StagePlan, ExecutionError> {
        match plan {
            PhysicalPlan::Scan {
                space,
                projection,
                filter,
            } => {
                // Attempt predicate pushdown to storage. If expr_to_predicate fails
                // (e.g. non-Int64 literal), silently ignore — the Filter node above
                // still handles the predicate at execution time.
                let pushed_filter = filter
                    .as_ref()
                    .map(Self::expr_to_predicate)
                    .and_then(Result::ok);
                Ok(StagePlan::Scan {
                    space: space.clone(),
                    projection: projection.clone(),
                    filter: pushed_filter,
                })
            }

            PhysicalPlan::Filter { predicate, input } => Ok(StagePlan::Filter {
                predicate: Self::expr_to_predicate(predicate)?,
                input: Box::new(self.physical_to_stage_plan(input)?),
            }),

            PhysicalPlan::Project { expressions, input } => Ok(StagePlan::Project {
                columns: Self::extract_columns(expressions)?,
                input: Box::new(self.physical_to_stage_plan(input)?),
            }),

            PhysicalPlan::Limit { skip, fetch, input } => Ok(StagePlan::Limit {
                skip: *skip,
                fetch: *fetch,
                input: Box::new(self.physical_to_stage_plan(input)?),
            }),

            PhysicalPlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => Ok(StagePlan::HashJoin {
                left: Box::new(self.physical_to_stage_plan(left)?),
                right: Box::new(self.physical_to_stage_plan(right)?),
                left_key: *left_key,
                right_key: *right_key,
                join_type: join_type.clone(),
            }),

            PhysicalPlan::HashAggregate {
                input,
                group_exprs,
                agg_exprs,
            } => Ok(StagePlan::HashAggregate {
                input: Box::new(self.physical_to_stage_plan(input)?),
                group_columns: Self::extract_columns(group_exprs)?,
                agg_columns: Self::extract_agg_columns(agg_exprs)?,
            }),

            PhysicalPlan::Sort { expressions, input } => {
                let (limit, remaining_exprs) = Self::extract_limit_from_sort(expressions);

                // Remap sort column indices if the input is a Project.
                // PhysicalPlanner resolves sort expressions against the *original*
                // table schema, but Sort operates on the Project's output which
                // may have fewer columns. We need to find each sort column's
                // position in the Project's output.
                let sort_columns = if let PhysicalPlan::Project {
                    expressions: proj_exprs,
                    ..
                } = input.as_ref()
                {
                    remaining_exprs
                        .iter()
                        .map(|(expr, _)| match expr {
                            PhysicalExpr::Column(orig_idx) => proj_exprs
                                .iter()
                                .position(|e| matches!(e, PhysicalExpr::Column(i) if i == orig_idx))
                                .ok_or_else(|| {
                                    ExecutionError::OperatorError(format!(
                                        "Sort column {} not found in Project output",
                                        orig_idx
                                    ))
                                }),
                            _ => Err(ExecutionError::OperatorError(
                                "Non-column sort expression in StagePlan".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    Self::extract_columns_from_sort(&remaining_exprs)?
                };

                Ok(StagePlan::Sort {
                    input: Box::new(self.physical_to_stage_plan(input)?),
                    sort_columns,
                    directions: remaining_exprs.iter().map(|(_, d)| *d).collect(),
                    limit,
                })
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Extract a LIMIT(fetch) value from ORDER BY if the query is
    /// `ORDER BY ... LIMIT n` with no OFFSET. Returns (Some(n), exprs).
    fn extract_limit_from_sort(
        expressions: &[(PhysicalExpr, SortDirection)],
    ) -> (Option<usize>, Vec<(PhysicalExpr, SortDirection)>) {
        // For now, no limit extraction from sort expressions.
        // This would need access to the parent Limit node.
        (None, expressions.to_vec())
    }

    fn expr_to_predicate(
        expr: &PhysicalExpr,
    ) -> Result<agoradb_core::PredicateDef, ExecutionError> {
        use crate::LiteralValue;
        match expr {
            PhysicalExpr::BinaryOp { op, left, right } => {
                // Try (Column, Literal) pattern first
                match (left.as_ref(), right.as_ref()) {
                    (
                        PhysicalExpr::Column(col),
                        PhysicalExpr::Literal(LiteralValue::Int64(val)),
                    ) => {
                        return Self::make_predicate(*op, *col, *val);
                    }
                    (
                        PhysicalExpr::Literal(LiteralValue::Int64(val)),
                        PhysicalExpr::Column(col),
                    ) => {
                        // Flip comparison: col op val  ->  val op col
                        let flipped = Self::flip_op(*op)?;
                        return Self::make_predicate(flipped, *col, *val);
                    }
                    (PhysicalExpr::Column(_), PhysicalExpr::Literal(other)) => {
                        return Err(ExecutionError::OperatorError(format!(
                            "Predicate pushdown only supports Int64 literals, got {:?}",
                            other
                        )));
                    }
                    (PhysicalExpr::Literal(other), PhysicalExpr::Column(_)) => {
                        return Err(ExecutionError::OperatorError(format!(
                            "Predicate pushdown only supports Int64 literals, got {:?}",
                            other
                        )));
                    }
                    _ => {}
                }
                // Recursive: And / Or
                match op {
                    BinaryOp::And | BinaryOp::Or => {
                        let left_pred = Self::expr_to_predicate(left)?;
                        let right_pred = Self::expr_to_predicate(right)?;
                        Ok(match op {
                            BinaryOp::And => agoradb_core::PredicateDef::And {
                                left: Box::new(left_pred),
                                right: Box::new(right_pred),
                            },
                            BinaryOp::Or => agoradb_core::PredicateDef::Or {
                                left: Box::new(left_pred),
                                right: Box::new(right_pred),
                            },
                            _ => unreachable!(),
                        })
                    }
                    _ => Err(ExecutionError::OperatorError(format!(
                        "Unsupported predicate pattern: {:?}",
                        expr
                    ))),
                }
            }
            _ => Err(ExecutionError::OperatorError(format!(
                "Expected binary op in predicate, got: {:?}",
                expr
            ))),
        }
    }

    fn make_predicate(
        op: BinaryOp,
        col: usize,
        val: i64,
    ) -> Result<agoradb_core::PredicateDef, ExecutionError> {
        Ok(match op {
            BinaryOp::Eq => agoradb_core::PredicateDef::Eq {
                column: col,
                value: val,
            },
            BinaryOp::Neq => agoradb_core::PredicateDef::Neq {
                column: col,
                value: val,
            },
            BinaryOp::Lt => agoradb_core::PredicateDef::Lt {
                column: col,
                value: val,
            },
            BinaryOp::LtEq => agoradb_core::PredicateDef::LtEq {
                column: col,
                value: val,
            },
            BinaryOp::Gt => agoradb_core::PredicateDef::Gt {
                column: col,
                value: val,
            },
            BinaryOp::GtEq => agoradb_core::PredicateDef::GtEq {
                column: col,
                value: val,
            },
            other => {
                return Err(ExecutionError::OperatorError(format!(
                    "Unsupported comparison op in predicate: {:?}",
                    other
                )))
            }
        })
    }

    fn flip_op(op: BinaryOp) -> Result<BinaryOp, ExecutionError> {
        Ok(match op {
            BinaryOp::Eq => BinaryOp::Eq,
            BinaryOp::Neq => BinaryOp::Neq,
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::LtEq => BinaryOp::GtEq,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::GtEq => BinaryOp::LtEq,
            other => {
                return Err(ExecutionError::OperatorError(format!(
                    "Cannot flip non-comparison op: {:?}",
                    other
                )))
            }
        })
    }

    fn extract_columns_from_sort(
        expressions: &[(PhysicalExpr, SortDirection)],
    ) -> Result<Vec<usize>, ExecutionError> {
        expressions
            .iter()
            .map(|(expr, _)| match expr {
                PhysicalExpr::Column(idx) => Ok(*idx),
                _ => Err(ExecutionError::OperatorError(
                    "Only column references supported in ORDER BY".to_string(),
                )),
            })
            .collect()
    }

    fn extract_columns(expressions: &[PhysicalExpr]) -> Result<Vec<usize>, ExecutionError> {
        expressions
            .iter()
            .map(|expr| match expr {
                PhysicalExpr::Column(idx) => Ok(*idx),
                _ => Err(ExecutionError::OperatorError(
                    "Only column references supported in projection".to_string(),
                )),
            })
            .collect()
    }

    fn extract_agg_columns(
        expressions: &[(PhysicalExpr, AggFunction)],
    ) -> Result<Vec<(usize, AggFunction)>, ExecutionError> {
        expressions
            .iter()
            .map(|(expr, agg)| {
                // COUNT counts rows regardless of argument (COUNT(*) == COUNT(1) == COUNT(col))
                // so we allow any expression here. Use column index when available, else 0.
                let idx = match expr {
                    PhysicalExpr::Column(i) => *i,
                    _ if matches!(agg, AggFunction::Count) => 0,
                    _ => {
                        return Err(ExecutionError::OperatorError(
                            "Only column references supported in aggregate".to_string(),
                        ))
                    }
                };
                Ok((idx, agg.clone()))
            })
            .collect()
    }

    fn default_parallelism() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}

impl Default for StageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::{JoinType, PredicateDef, SpaceUri};

    #[test]
    fn test_single_scan_stage() {
        let plan = PhysicalPlan::Scan {
            space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
            projection: None,
            filter: None,
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        assert!(matches!(exec_plan.stages[0].plan, StagePlan::Scan { .. }));
        assert_eq!(exec_plan.stages[0].dependencies.len(), 0);
    }

    #[test]
    fn test_filter_scan_stage() {
        use crate::LiteralValue;
        let plan = PhysicalPlan::Filter {
            predicate: PhysicalExpr::BinaryOp {
                op: crate::BinaryOp::Gt,
                left: Box::new(PhysicalExpr::Column(0)),
                right: Box::new(PhysicalExpr::Literal(LiteralValue::Int64(1))),
            },
            input: Box::new(PhysicalPlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
                projection: None,
                filter: None,
            }),
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        match &exec_plan.stages[0].plan {
            StagePlan::Filter { predicate, input } => {
                assert!(matches!(
                    predicate,
                    PredicateDef::Gt {
                        column: 0,
                        value: 1
                    }
                ));
                assert!(matches!(input.as_ref(), StagePlan::Scan { .. }));
            }
            other => panic!("Expected Filter -> Scan, got {:?}", other),
        }
    }

    #[test]
    fn test_predicate_pushdown_to_scan() {
        use crate::LiteralValue;
        // Scan with a filter attached (simulating predicate pushdown from planner)
        let plan = PhysicalPlan::Scan {
            space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
            projection: None,
            filter: Some(PhysicalExpr::BinaryOp {
                op: crate::BinaryOp::Eq,
                left: Box::new(PhysicalExpr::Column(0)),
                right: Box::new(PhysicalExpr::Literal(LiteralValue::Int64(42))),
            }),
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        match &exec_plan.stages[0].plan {
            StagePlan::Scan { filter, .. } => {
                assert!(matches!(
                    filter,
                    Some(PredicateDef::Eq {
                        column: 0,
                        value: 42
                    })
                ));
            }
            other => panic!("Expected Scan with pushed filter, got {:?}", other),
        }
    }

    #[test]
    fn test_predicate_pushdown_non_int64_ignored() {
        use crate::LiteralValue;
        // String literal predicate cannot be converted to PredicateDef (Int64-only),
        // but should not error — it is silently ignored and the Filter node handles it.
        let plan = PhysicalPlan::Scan {
            space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
            projection: None,
            filter: Some(PhysicalExpr::BinaryOp {
                op: crate::BinaryOp::Eq,
                left: Box::new(PhysicalExpr::Column(0)),
                right: Box::new(PhysicalExpr::Literal(LiteralValue::String("hello".to_string()))),
            }),
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        match &exec_plan.stages[0].plan {
            StagePlan::Scan { filter, .. } => {
                assert!(filter.is_none(), "Non-Int64 predicate should be ignored");
            }
            other => panic!("Expected Scan, got {:?}", other),
        }
    }

    #[test]
    fn test_hash_join_single_stage() {
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/a").unwrap(),
                projection: None,
                filter: None,
            }),
            right: Box::new(PhysicalPlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/b").unwrap(),
                projection: None,
                filter: None,
            }),
            left_key: 0,
            right_key: 0,
            join_type: JoinType::Inner,
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        match &exec_plan.stages[0].plan {
            StagePlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                assert!(matches!(left.as_ref(), StagePlan::Scan { .. }));
                assert!(matches!(right.as_ref(), StagePlan::Scan { .. }));
                assert_eq!(*left_key, 0);
                assert_eq!(*right_key, 0);
                assert!(matches!(join_type, JoinType::Inner));
            }
            other => panic!("Expected HashJoin, got {:?}", other),
        }
        assert_eq!(exec_plan.stages[0].dependencies.len(), 0);
    }

    #[test]
    fn test_hash_join_with_downstream_project() {
        let plan = PhysicalPlan::Project {
            expressions: vec![PhysicalExpr::Column(0)],
            input: Box::new(PhysicalPlan::HashJoin {
                left: Box::new(PhysicalPlan::Scan {
                    space: SpaceUri::parse("space://did:agora:test/a").unwrap(),
                    projection: None,
                    filter: None,
                }),
                right: Box::new(PhysicalPlan::Scan {
                    space: SpaceUri::parse("space://did:agora:test/b").unwrap(),
                    projection: None,
                    filter: None,
                }),
                left_key: 0,
                right_key: 0,
                join_type: JoinType::Inner,
            }),
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        match &exec_plan.stages[0].plan {
            StagePlan::Project { columns, input } => {
                assert_eq!(columns, &vec![0]);
                match input.as_ref() {
                    StagePlan::HashJoin { .. } => {}
                    other => panic!("Expected HashJoin inside Project, got {:?}", other),
                }
            }
            other => panic!("Expected Project, got {:?}", other),
        }
    }

    #[test]
    fn test_hash_aggregate_single_stage() {
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
                projection: None,
                filter: None,
            }),
            group_exprs: vec![PhysicalExpr::Column(0)],
            agg_exprs: vec![(PhysicalExpr::Column(1), AggFunction::Sum)],
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        assert!(
            matches!(exec_plan.stages[0].plan, StagePlan::HashAggregate { .. }),
            "Expected HashAggregate, got {:?}",
            exec_plan.stages[0].plan
        );
    }

    #[test]
    fn test_sort_single_stage() {
        let plan = PhysicalPlan::Sort {
            expressions: vec![(PhysicalExpr::Column(0), SortDirection::Asc)],
            input: Box::new(PhysicalPlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
                projection: None,
                filter: None,
            }),
        };

        let builder = StageBuilder::new();
        let exec_plan = builder.build(&plan).unwrap();

        assert_eq!(exec_plan.stages.len(), 1);
        assert!(
            matches!(exec_plan.stages[0].plan, StagePlan::Sort { .. }),
            "Expected Sort, got {:?}",
            exec_plan.stages[0].plan
        );
    }
}
