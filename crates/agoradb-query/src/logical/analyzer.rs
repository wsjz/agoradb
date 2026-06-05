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

use crate::logical::plan::{LogicalExpr, LogicalPlan};
use agoradb_core::{DataType, ExecutionError, SchemaProvider};
use std::collections::HashMap;

/// Validates and resolves a logical plan.
///
/// Checks:
/// - Table existence
/// - Column name validity
/// - Populates Scan schema from the provider
pub struct Analyzer;

impl Default for Analyzer {
    fn default() -> Self {
        Self
    }
}

impl Analyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyze a logical plan, validating all references and populating schemas.
    ///
    /// Returns the resolved plan (with schemas filled in) and the final output
    /// column map (name → index).
    pub fn analyze(
        &self,
        plan: &mut LogicalPlan,
        provider: &dyn SchemaProvider,
    ) -> Result<HashMap<String, DataType>, ExecutionError> {
        self.analyze_plan(plan, provider)
    }

    fn analyze_plan(
        &self,
        plan: &mut LogicalPlan,
        provider: &dyn SchemaProvider,
    ) -> Result<HashMap<String, DataType>, ExecutionError> {
        match plan {
            LogicalPlan::Scan {
                table,
                alias,
                schema,
            } => {
                let table_schema = provider.get_table_schema(table).map_err(|_| {
                    ExecutionError::OperatorError(format!("Table not found: {}", table))
                })?;
                let resolved_schema = if let Some(ref a) = alias {
                    // With alias: map column names to alias.column format.
                    // E.g. table prefix "o_" removed, alias "o" added: "o_custkey" → "o.custkey"
                    table_schema
                        .iter()
                        .map(|(name, dt)| {
                            let base = if let Some(prefix) = name.split('_').next() {
                                if prefix == a {
                                    name.trim_start_matches(&format!("{}_", a))
                                } else {
                                    name.as_str()
                                }
                            } else {
                                name.as_str()
                            };
                            let key = format!("{}.{}", a, base);
                            (key, dt.clone())
                        })
                        .collect()
                } else {
                    table_schema
                };
                *schema = resolved_schema
                    .iter()
                    .map(|(name, dt)| (name.clone(), dt.clone()))
                    .collect();
                Ok(resolved_schema)
            }
            LogicalPlan::Filter { predicate, input } => {
                let input_schema = self.analyze_plan(input, provider)?;
                self.validate_expr(predicate, &input_schema)?;
                Ok(input_schema)
            }
            LogicalPlan::Project { expressions, input } => {
                let input_schema = self.analyze_plan(input, provider)?;
                let mut output_schema = HashMap::new();
                for (name, expr) in expressions {
                    self.validate_expr(expr, &input_schema)?;
                    let expr_type = self.infer_type(expr, &input_schema)?;
                    output_schema.insert(name.clone(), expr_type);
                }
                Ok(output_schema)
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                let left_schema = self.analyze_plan(left, provider)?;
                let right_schema = self.analyze_plan(right, provider)?;
                let mut combined = left_schema;
                combined.extend(right_schema);
                self.validate_expr(condition, &combined)?;
                Ok(combined)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_schema = self.analyze_plan(input, provider)?;
                for expr in &*group_by {
                    self.validate_expr(expr, &input_schema)?;
                }
                let mut output_schema = HashMap::new();
                // GROUP BY columns are part of the output schema
                for expr in &*group_by {
                    if let LogicalExpr::Column(name) = expr {
                        if let Some(dt) = input_schema.get(name) {
                            output_schema.insert(name.clone(), dt.clone());
                        }
                    }
                }
                // Aggregate columns
                for (name, _func, expr) in aggregates {
                    self.validate_expr(expr, &input_schema)?;
                    output_schema.insert(name.clone(), DataType::Int64); // simplified
                }
                Ok(output_schema)
            }
            LogicalPlan::Limit { input, .. } => {
                let input_schema = self.analyze_plan(input, provider)?;
                Ok(input_schema)
            }
            LogicalPlan::Sort { expressions, input } => {
                let input_schema = self.analyze_plan(input, provider)?;
                for (expr, _) in expressions {
                    self.validate_expr(expr, &input_schema)?;
                }
                Ok(input_schema)
            }
        }
    }

    fn validate_expr(
        &self,
        expr: &LogicalExpr,
        schema: &HashMap<String, DataType>,
    ) -> Result<(), ExecutionError> {
        match expr {
            LogicalExpr::Column(name) => {
                if name != "*" && !schema.contains_key(name) {
                    return Err(ExecutionError::ColumnNotFound(name.clone()));
                }
                Ok(())
            }
            LogicalExpr::Literal(_) => Ok(()),
            LogicalExpr::BinaryOp { left, right, .. } => {
                self.validate_expr(left, schema)?;
                self.validate_expr(right, schema)?;
                Ok(())
            }
            LogicalExpr::Function { name: _, args } => {
                for arg in args {
                    self.validate_expr(arg, schema)?;
                }
                Ok(())
            }
        }
    }

    fn infer_type(
        &self,
        expr: &LogicalExpr,
        schema: &HashMap<String, DataType>,
    ) -> Result<DataType, ExecutionError> {
        match expr {
            LogicalExpr::Column(name) => schema
                .get(name)
                .cloned()
                .ok_or_else(|| ExecutionError::ColumnNotFound(name.clone())),
            LogicalExpr::Literal(val) => match val {
                crate::LiteralValue::Int64(_) => Ok(DataType::Int64),
                crate::LiteralValue::Float64(_) => Ok(DataType::Float64),
                crate::LiteralValue::Boolean(_) => Ok(DataType::Boolean),
                crate::LiteralValue::String(_) => Ok(DataType::Utf8),
                crate::LiteralValue::Null => Ok(DataType::Int64), // simplified
            },
            LogicalExpr::BinaryOp { .. } => Ok(DataType::Boolean),
            LogicalExpr::Function { .. } => Ok(DataType::Int64),
        }
    }
}
