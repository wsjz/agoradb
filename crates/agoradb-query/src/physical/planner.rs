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

use crate::logical::plan::{
    AggFunction as LogicalAggFunction, BinaryOp as LogicalBinOp, JoinType as LogicalJoinType,
    LogicalExpr, LogicalPlan,
};
use crate::physical::plan::{PhysicalExpr, PhysicalPlan};
use agoradb_core::{AggFunction, BinaryOp as PhysicalBinOp, JoinType};
use agoradb_core::{ExecutionError, SpaceUri};
use std::collections::HashMap;

pub struct PhysicalPlanner;

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Convert a LogicalPlan to a PhysicalPlan.
    ///
    /// `schema_map` maps column names to their indices in the source table.
    pub fn plan(
        &self,
        logical: &LogicalPlan,
        schema_map: &HashMap<String, usize>,
    ) -> Result<PhysicalPlan, ExecutionError> {
        self.plan_inner(logical, schema_map)
    }

    fn plan_inner(
        &self,
        logical: &LogicalPlan,
        schema_map: &HashMap<String, usize>,
    ) -> Result<PhysicalPlan, ExecutionError> {
        match logical {
            LogicalPlan::Scan { table, .. } => {
                let space = SpaceUri::parse(&format!("space://did:agora:test/{}", table))
                    .map_err(|e| ExecutionError::OperatorError(e.to_string()))?;
                Ok(PhysicalPlan::Scan {
                    space,
                    projection: None,
                    filter: None,
                })
            }
            LogicalPlan::Filter { predicate, input } => {
                let physical_input = self.plan_inner(input, schema_map)?;
                let physical_pred = self.expr_to_physical(predicate, schema_map)?;
                Ok(PhysicalPlan::Filter {
                    predicate: physical_pred,
                    input: Box::new(physical_input),
                })
            }
            LogicalPlan::Project { expressions, input } => {
                let physical_input = self.plan_inner(input, schema_map)?;
                let physical_exprs: Result<Vec<PhysicalExpr>, _> = expressions
                    .iter()
                    .map(|(_, expr)| self.expr_to_physical(expr, schema_map))
                    .collect();
                Ok(PhysicalPlan::Project {
                    expressions: physical_exprs?,
                    input: Box::new(physical_input),
                })
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let physical_left = self.plan_inner(left, schema_map)?;
                let physical_right = self.plan_inner(right, schema_map)?;

                let (left_key, right_key) = self
                    .extract_join_keys(condition, schema_map)?
                    .ok_or_else(|| {
                        ExecutionError::OperatorError(
                            "Only equi-join conditions supported".to_string(),
                        )
                    })?;

                let physical_join_type = match join_type {
                    LogicalJoinType::Inner => JoinType::Inner,
                    LogicalJoinType::Left => JoinType::Left,
                    LogicalJoinType::Right => JoinType::Right,
                    LogicalJoinType::Full => JoinType::Full,
                };

                Ok(PhysicalPlan::HashJoin {
                    left: Box::new(physical_left),
                    right: Box::new(physical_right),
                    left_key,
                    right_key,
                    join_type: physical_join_type,
                })
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let physical_input = self.plan_inner(input, schema_map)?;

                let group_exprs: Result<Vec<PhysicalExpr>, _> = group_by
                    .iter()
                    .map(|e| self.expr_to_physical(e, schema_map))
                    .collect();

                let agg_exprs: Result<Vec<(PhysicalExpr, AggFunction)>, ExecutionError> =
                    aggregates
                        .iter()
                        .map(|(_, agg_func, expr)| {
                            let physical_expr = self.expr_to_physical(expr, schema_map)?;
                            let physical_agg = match agg_func {
                                LogicalAggFunction::Count => AggFunction::Count,
                                LogicalAggFunction::Sum => AggFunction::Sum,
                                LogicalAggFunction::Avg => AggFunction::Avg,
                                LogicalAggFunction::Min => AggFunction::Min,
                                LogicalAggFunction::Max => AggFunction::Max,
                            };
                            Ok::<_, ExecutionError>((physical_expr, physical_agg))
                        })
                        .collect();

                Ok(PhysicalPlan::HashAggregate {
                    input: Box::new(physical_input),
                    group_exprs: group_exprs?,
                    agg_exprs: agg_exprs?,
                })
            }
            LogicalPlan::Limit { skip, fetch, input } => {
                let physical_input = self.plan_inner(input, schema_map)?;
                Ok(PhysicalPlan::Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Box::new(physical_input),
                })
            }
        }
    }

    fn extract_join_keys(
        &self,
        condition: &LogicalExpr,
        schema_map: &HashMap<String, usize>,
    ) -> Result<Option<(usize, usize)>, ExecutionError> {
        match condition {
            LogicalExpr::BinaryOp {
                op: LogicalBinOp::Eq,
                left,
                right,
            } => {
                let left_col = self.extract_column_index(left, schema_map)?;
                let right_col = self.extract_column_index(right, schema_map)?;
                match (left_col, right_col) {
                    (Some(l), Some(r)) => Ok(Some((l, r))),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    fn extract_column_index(
        &self,
        expr: &LogicalExpr,
        schema_map: &HashMap<String, usize>,
    ) -> Result<Option<usize>, ExecutionError> {
        match expr {
            LogicalExpr::Column(name) => Ok(schema_map.get(name).copied()),
            _ => Ok(None),
        }
    }

    fn expr_to_physical(
        &self,
        expr: &LogicalExpr,
        schema_map: &HashMap<String, usize>,
    ) -> Result<PhysicalExpr, ExecutionError> {
        match expr {
            LogicalExpr::Column(name) => {
                if name == "*" {
                    Err(ExecutionError::OperatorError(
                        "Wildcard '*' should be resolved before physical planning".to_string(),
                    ))
                } else {
                    let idx = schema_map.get(name).copied().ok_or_else(|| {
                        ExecutionError::OperatorError(format!(
                            "Column '{}' not found in schema",
                            name
                        ))
                    })?;
                    Ok(PhysicalExpr::Column(idx))
                }
            }
            LogicalExpr::Literal(val) => Ok(PhysicalExpr::Literal(val.clone())),
            LogicalExpr::BinaryOp { op, left, right } => {
                let physical_op = match op {
                    LogicalBinOp::Eq => PhysicalBinOp::Eq,
                    LogicalBinOp::Neq => PhysicalBinOp::Neq,
                    LogicalBinOp::Lt => PhysicalBinOp::Lt,
                    LogicalBinOp::LtEq => PhysicalBinOp::LtEq,
                    LogicalBinOp::Gt => PhysicalBinOp::Gt,
                    LogicalBinOp::GtEq => PhysicalBinOp::GtEq,
                    LogicalBinOp::And => PhysicalBinOp::And,
                    LogicalBinOp::Or => PhysicalBinOp::Or,
                    _ => {
                        return Err(ExecutionError::OperatorError(format!(
                            "Unsupported binary op: {:?}",
                            op
                        )))
                    }
                };
                Ok(PhysicalExpr::BinaryOp {
                    op: physical_op,
                    left: Box::new(self.expr_to_physical(left, schema_map)?),
                    right: Box::new(self.expr_to_physical(right, schema_map)?),
                })
            }
            LogicalExpr::Function { .. } => Err(ExecutionError::OperatorError(
                "Function expressions not yet supported".to_string(),
            )),
        }
    }
}
