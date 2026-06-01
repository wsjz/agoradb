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
use agoradb_core::{
    AggFunction, BinaryOp, OperatorDef, PredicateDef, Stage, StagePlan, StageTask,
};
use agoradb_core::ExecutionError;

/// Builds a [`StagePlan`] from a [`PhysicalPlan`] by partitioning
/// the physical operator tree into stages separated by pipeline breakers
/// (HashJoin, HashAggregate).
///
/// Breaker downstream operators (e.g. Project, Limit) are merged into the
/// probe / emit stage so that the joined / aggregated results flow directly
/// through them without an extra data-shuffle stage.
pub struct StageBuilder;

impl StageBuilder {
    pub fn new() -> Self {
        Self
    }

    /// Convert a PhysicalPlan into a StagePlan with explicit stages.
    pub fn build(&self, plan: &PhysicalPlan) -> Result<StagePlan, ExecutionError> {
        let mut stages = Vec::new();
        self.build_inner(plan, &mut stages, Vec::new())?;
        Ok(StagePlan { stages })
    }

    /// Recursive inner build.
    ///
    /// `downstream` collects operators that sit *above* the current node in the
    /// tree (Project, Filter, Limit).  When we hit a leaf (Scan) or a breaker
    /// we flush the collected operators into the appropriate stage.
    fn build_inner(
        &self,
        plan: &PhysicalPlan,
        stages: &mut Vec<Stage>,
        downstream: Vec<OperatorDef>,
    ) -> Result<Vec<usize>, ExecutionError> {
        match plan {
            PhysicalPlan::Scan {
                space,
                projection,
                filter,
            } => {
                let mut ops = vec![OperatorDef::Scan {
                    space: space.clone(),
                    projection: projection.clone(),
                    filter: filter
                        .as_ref()
                        .map(Self::expr_to_predicate)
                        .transpose()?,
                }];
                ops.extend(downstream);
                let id = stages.len();
                stages.push(Stage {
                    id,
                    label: format!("pipeline_{}", id),
                    dependencies: vec![],
                    parallelism: Self::default_parallelism(),
                    task: StageTask::Pipeline { operators: ops },
                });
                Ok(vec![id])
            }

            PhysicalPlan::Filter { predicate, input } => {
                let pred = Self::expr_to_predicate(predicate)?;
                let mut new_down = vec![OperatorDef::Filter { predicate: pred }];
                new_down.extend(downstream);
                self.build_inner(input, stages, new_down)
            }

            PhysicalPlan::Project { expressions, input } => {
                let columns = Self::extract_columns(expressions)?;
                let mut new_down = vec![OperatorDef::Project { columns }];
                new_down.extend(downstream);
                self.build_inner(input, stages, new_down)
            }

            PhysicalPlan::Limit { skip, fetch, input } => {
                let mut new_down = vec![OperatorDef::Limit {
                    skip: *skip,
                    fetch: *fetch,
                }];
                new_down.extend(downstream);
                self.build_inner(input, stages, new_down)
            }

            PhysicalPlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                let join_id = stages.len();

                // Build stage: execute left subtree to populate hash table
                let build_id = stages.len();
                stages.push(Stage {
                    id: build_id,
                    label: format!("hj_{}_build", join_id),
                    dependencies: vec![],
                    parallelism: 1,
                    task: StageTask::HashJoinBuild {
                        join_id,
                        operators: Self::physical_to_operators(left)?,
                        left_key: *left_key,
                        right_key: *right_key,
                        join_type: join_type.clone(),
                    },
                });

                // Probe stage: execute right subtree + downstream ops
                let probe_id = stages.len();
                let probe_ops = Self::physical_to_operators(right)?;
                stages.push(Stage {
                    id: probe_id,
                    label: format!("hj_{}_probe", join_id),
                    dependencies: vec![build_id],
                    parallelism: Self::default_parallelism(),
                    task: StageTask::HashJoinProbe {
                        join_id,
                        operators: probe_ops,
                        post_operators: downstream,
                    },
                });
                Ok(vec![build_id, probe_id])
            }

            PhysicalPlan::HashAggregate {
                input,
                group_exprs,
                agg_exprs,
            } => {
                let agg_id = stages.len();

                // Only recurse into input if it contains a nested breaker.
                let (accum_deps, accum_ops) = if Self::contains_breaker(input) {
                    let input_ids = self.build_inner(input, stages, Vec::new())?;
                    (vec![*input_ids.last().unwrap()], vec![])
                } else {
                    (vec![], Self::physical_to_operators(input)?)
                };

                // Accumulate stage
                let accum_id = stages.len();
                stages.push(Stage {
                    id: accum_id,
                    label: format!("agg_{}_accum", agg_id),
                    dependencies: accum_deps,
                    parallelism: 1,
                    task: StageTask::AggregateAccumulate {
                        agg_id,
                        operators: accum_ops,
                        group_columns: Self::extract_columns(group_exprs)?,
                        agg_columns: Self::extract_agg_columns(agg_exprs)?,
                    },
                });

                // Emit stage: produce final aggregate results + downstream ops
                let emit_id = stages.len();
                stages.push(Stage {
                    id: emit_id,
                    label: format!("agg_{}_emit", emit_id),
                    dependencies: vec![accum_id],
                    parallelism: 1,
                    task: StageTask::AggregateEmit {
                        agg_id,
                        post_operators: downstream,
                    },
                });
                Ok(vec![accum_id, emit_id])
            }
        }
    }

    // ------------------------------------------------------------------
    // PhysicalPlan → Vec<OperatorDef> (for the subtree below a breaker)
    // ------------------------------------------------------------------

    fn physical_to_operators(plan: &PhysicalPlan) -> Result<Vec<OperatorDef>, ExecutionError> {
        let mut ops = Vec::new();
        Self::collect_operators(plan, &mut ops)?;
        Ok(ops)
    }

    fn collect_operators(
        plan: &PhysicalPlan,
        ops: &mut Vec<OperatorDef>,
    ) -> Result<(), ExecutionError> {
        match plan {
            PhysicalPlan::Scan {
                space,
                projection,
                filter,
            } => {
                ops.push(OperatorDef::Scan {
                    space: space.clone(),
                    projection: projection.clone(),
                    filter: filter
                        .as_ref()
                        .map(Self::expr_to_predicate)
                        .transpose()?,
                });
                Ok(())
            }
            PhysicalPlan::Filter { predicate, input } => {
                Self::collect_operators(input, ops)?;
                ops.push(OperatorDef::Filter {
                    predicate: Self::expr_to_predicate(predicate)?,
                });
                Ok(())
            }
            PhysicalPlan::Project { expressions, input } => {
                Self::collect_operators(input, ops)?;
                ops.push(OperatorDef::Project {
                    columns: Self::extract_columns(expressions)?,
                });
                Ok(())
            }
            PhysicalPlan::Limit { skip, fetch, input } => {
                Self::collect_operators(input, ops)?;
                ops.push(OperatorDef::Limit {
                    skip: *skip,
                    fetch: *fetch,
                });
                Ok(())
            }
            PhysicalPlan::HashJoin { .. } | PhysicalPlan::HashAggregate { .. } => Err(
                ExecutionError::OperatorError(
                    "Nested breaker found inside pipeline — not yet supported".to_string(),
                ),
            ),
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn expr_to_predicate(expr: &PhysicalExpr) -> Result<PredicateDef, ExecutionError> {
        use crate::physical::plan::LiteralValue;
        match expr {
            PhysicalExpr::BinaryOp { op, left, right } => {
                // Try (Column, Literal) pattern first
                match (left.as_ref(), right.as_ref()) {
                    (PhysicalExpr::Column(col), PhysicalExpr::Literal(LiteralValue::Int64(val))) => {
                        return Self::make_predicate(*op, *col, *val);
                    }
                    (PhysicalExpr::Literal(LiteralValue::Int64(val)), PhysicalExpr::Column(col)) => {
                        // Flip comparison: col op val  →  val op col
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
                            BinaryOp::And => PredicateDef::And {
                                left: Box::new(left_pred),
                                right: Box::new(right_pred),
                            },
                            BinaryOp::Or => PredicateDef::Or {
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
    ) -> Result<PredicateDef, ExecutionError> {
        Ok(match op {
            BinaryOp::Eq => PredicateDef::Eq { column: col, value: val },
            BinaryOp::Neq => PredicateDef::Neq { column: col, value: val },
            BinaryOp::Lt => PredicateDef::Lt { column: col, value: val },
            BinaryOp::LtEq => PredicateDef::LtEq { column: col, value: val },
            BinaryOp::Gt => PredicateDef::Gt { column: col, value: val },
            BinaryOp::GtEq => PredicateDef::GtEq { column: col, value: val },
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
            .map(|(expr, agg)| match expr {
                PhysicalExpr::Column(idx) => Ok((*idx, agg.clone())),
                _ => Err(ExecutionError::OperatorError(
                    "Only column references supported in aggregate".to_string(),
                )),
            })
            .collect()
    }

    fn default_parallelism() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }

    /// Returns true if the plan tree contains a pipeline breaker (HashJoin or
    /// HashAggregate) anywhere below the root.
    fn contains_breaker(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::HashJoin { .. } | PhysicalPlan::HashAggregate { .. } => true,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Limit { input, .. } => Self::contains_breaker(input),
            PhysicalPlan::Scan { .. } => false,
        }
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
    use agoradb_core::{JoinType as PhysicalJoinType, PredicateDef};
    use agoradb_core::SpaceUri;

    #[test]
    fn test_pipeline_single_stage() {
        let plan = PhysicalPlan::Scan {
            space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
            projection: None,
            filter: None,
        };

        let builder = StageBuilder::new();
        let stage_plan = builder.build(&plan).unwrap();

        assert_eq!(stage_plan.stages.len(), 1);
        assert!(
            matches!(stage_plan.stages[0].task, StageTask::Pipeline { .. }),
            "Expected Pipeline"
        );
        assert_eq!(stage_plan.stages[0].dependencies.len(), 0);
    }

    #[test]
    fn test_hash_join_two_stages() {
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
            join_type: PhysicalJoinType::Inner,
        };

        let builder = StageBuilder::new();
        let stage_plan = builder.build(&plan).unwrap();

        assert_eq!(stage_plan.stages.len(), 2);
        assert!(
            matches!(stage_plan.stages[0].task, StageTask::HashJoinBuild { .. }),
            "Expected HashJoinBuild"
        );
        assert!(
            matches!(stage_plan.stages[1].task, StageTask::HashJoinProbe { .. }),
            "Expected HashJoinProbe"
        );
        assert_eq!(stage_plan.stages[1].dependencies, vec![0]);
    }

    #[test]
    fn test_hash_aggregate_two_stages() {
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
        let stage_plan = builder.build(&plan).unwrap();

        assert_eq!(stage_plan.stages.len(), 2);
        assert!(
            matches!(
                stage_plan.stages[0].task,
                StageTask::AggregateAccumulate { .. }
            ),
            "Expected AggregateAccumulate"
        );
        assert!(
            matches!(stage_plan.stages[1].task, StageTask::AggregateEmit { .. }),
            "Expected AggregateEmit"
        );
        assert_eq!(stage_plan.stages[1].dependencies, vec![0]);
    }

    #[test]
    fn test_join_with_downstream_project() {
        // Project([0]) -> HashJoin(left=Scan(a), right=Scan(b))
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
                join_type: PhysicalJoinType::Inner,
            }),
        };

        let builder = StageBuilder::new();
        let stage_plan = builder.build(&plan).unwrap();

        assert_eq!(stage_plan.stages.len(), 2);
        // Probe stage should carry the Project in post_operators
        match &stage_plan.stages[1].task {
            StageTask::HashJoinProbe { post_operators, .. } => {
                assert_eq!(post_operators.len(), 1);
                assert!(matches!(post_operators[0], OperatorDef::Project { .. }));
            }
            other => panic!("Expected HashJoinProbe, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_predicate_conversion() {
        use crate::physical::plan::LiteralValue;
        let plan = PhysicalPlan::Filter {
            predicate: PhysicalExpr::BinaryOp {
                op: BinaryOp::Gt,
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
        let stage_plan = builder.build(&plan).unwrap();

        assert_eq!(stage_plan.stages.len(), 1);
        match &stage_plan.stages[0].task {
            StageTask::Pipeline { operators } => {
                assert_eq!(operators.len(), 2);
                assert!(matches!(operators[0], OperatorDef::Scan { .. }));
                assert!(
                    matches!(operators[1], OperatorDef::Filter { ref predicate, .. } if matches!(predicate, PredicateDef::Gt { column: 0, value: 1 })),
                    "Expected Gt predicate"
                );
            }
            other => panic!("Expected Pipeline, got {:?}", other),
        }
    }
}
