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

use crate::logical::plan::{AggFunction, JoinType, LogicalExpr, LogicalPlan};
use crate::{BinaryOp, LiteralValue};
use agoradb_core::ExecutionError;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct SqlParser;

impl Default for SqlParser {
    fn default() -> Self {
        Self
    }
}

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, sql: &str) -> Result<LogicalPlan, ExecutionError> {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| ExecutionError::OperatorError(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(ExecutionError::OperatorError(
                "Only single statements supported".to_string(),
            ));
        }

        self.statement_to_plan(&statements[0])
    }

    fn statement_to_plan(
        &self,
        stmt: &sqlparser::ast::Statement,
    ) -> Result<LogicalPlan, ExecutionError> {
        match stmt {
            sqlparser::ast::Statement::Query(query) => self.query_to_plan(query),
            _ => Err(ExecutionError::OperatorError(
                "Only SELECT queries supported".to_string(),
            )),
        }
    }

    fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<LogicalPlan, ExecutionError> {
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Err(ExecutionError::OperatorError(
                "Only SELECT is supported".to_string(),
            ));
        };

        // FROM clause (supports simple JOINs)
        let mut plan = self.build_from(&select.from)?;

        // WHERE clause → Filter
        if let Some(selection) = &select.selection {
            plan = LogicalPlan::Filter {
                predicate: self.expr_to_logical(selection)?,
                input: Box::new(plan),
            };
        }

        // GROUP BY + aggregates → Aggregate
        let group_by_exprs = match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
                let parsed: Result<Vec<LogicalExpr>, _> =
                    exprs.iter().map(|e| self.expr_to_logical(e)).collect();
                Some(parsed?)
            }
            _ => None,
        };

        if let Some(group_by) = &group_by_exprs {
            let aggregates = self.extract_aggregates(&select.projection)?;

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_by.clone(),
                aggregates,
            };
        }

        // SELECT clause → Project (non-aggregate expressions)
        if group_by_exprs.is_none() || !self.has_non_aggregate_projection(&select.projection) {
            let projections = self.build_projections(&select.projection)?;
            if !projections.is_empty() {
                plan = LogicalPlan::Project {
                    expressions: projections,
                    input: Box::new(plan),
                };
            }
        }

        // ORDER BY → Sort (before Limit)
        if let Some(order_by) = query.order_by.as_ref() {
            let mut sort_exprs = Vec::new();
            for ob in &order_by.exprs {
                let expr = self.expr_to_logical(&ob.expr)?;
                let direction = match ob.asc {
                    Some(true) | None => crate::logical::plan::SortDirection::Asc,
                    Some(false) => crate::logical::plan::SortDirection::Desc,
                };
                sort_exprs.push((expr, direction));
            }
            plan = LogicalPlan::Sort {
                expressions: sort_exprs,
                input: Box::new(plan),
            };
        }

        // LIMIT → Limit
        if let Some(limit) = &query.limit {
            let fetch = match limit {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                    n.parse().unwrap_or(usize::MAX)
                }
                _ => usize::MAX,
            };
            plan = LogicalPlan::Limit {
                skip: 0,
                fetch,
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    /// Build the FROM clause: single table or JOIN.
    fn build_from(
        &self,
        from_list: &[sqlparser::ast::TableWithJoins],
    ) -> Result<LogicalPlan, ExecutionError> {
        if from_list.is_empty() {
            return Err(ExecutionError::OperatorError(
                "SELECT without FROM not supported".to_string(),
            ));
        }

        let first = &from_list[0];
        let mut plan = self.build_table_factor(&first.relation)?;

        for join in &first.joins {
            let right = self.build_table_factor(&join.relation)?;
            let join_type = match join.join_operator {
                sqlparser::ast::JoinOperator::Inner(..) => JoinType::Inner,
                sqlparser::ast::JoinOperator::LeftOuter(..) => JoinType::Left,
                sqlparser::ast::JoinOperator::RightOuter(..) => JoinType::Right,
                sqlparser::ast::JoinOperator::FullOuter(..) => JoinType::Full,
                _ => JoinType::Inner,
            };

            let condition = match &join.join_operator {
                sqlparser::ast::JoinOperator::Inner(constraint)
                | sqlparser::ast::JoinOperator::LeftOuter(constraint)
                | sqlparser::ast::JoinOperator::RightOuter(constraint)
                | sqlparser::ast::JoinOperator::FullOuter(constraint) => match constraint {
                    sqlparser::ast::JoinConstraint::On(expr) => self.expr_to_logical(expr)?,
                    _ => LogicalExpr::Literal(LiteralValue::Boolean(true)),
                },
                _ => LogicalExpr::Literal(LiteralValue::Boolean(true)),
            };

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type,
                condition,
            };
        }

        Ok(plan)
    }

    fn build_table_factor(
        &self,
        factor: &sqlparser::ast::TableFactor,
    ) -> Result<LogicalPlan, ExecutionError> {
        match factor {
            sqlparser::ast::TableFactor::Table { name, .. } => Ok(LogicalPlan::Scan {
                table: name.to_string(),
                schema: Vec::new(),
            }),
            _ => Err(ExecutionError::OperatorError(
                "Only simple table references supported".to_string(),
            )),
        }
    }

    fn build_projections(
        &self,
        projection: &[sqlparser::ast::SelectItem],
    ) -> Result<Vec<(String, LogicalExpr)>, ExecutionError> {
        projection
            .iter()
            .map(|item| match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                    let name = expr_to_name(expr);
                    let logical_expr = self.expr_to_logical(expr)?;
                    Ok((name, logical_expr))
                }
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let logical_expr = self.expr_to_logical(expr)?;
                    Ok((alias.value.clone(), logical_expr))
                }
                sqlparser::ast::SelectItem::Wildcard(_) => {
                    Ok(("*".to_string(), LogicalExpr::Column("*".to_string())))
                }
                _ => Err(ExecutionError::OperatorError(
                    "Unsupported SELECT item".to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn extract_aggregates(
        &self,
        projection: &[sqlparser::ast::SelectItem],
    ) -> Result<Vec<(String, AggFunction, LogicalExpr)>, ExecutionError> {
        let mut aggregates = Vec::new();
        for item in projection {
            let (name, expr) = match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => (expr_to_name(expr), expr),
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                    (alias.value.clone(), expr)
                }
                _ => continue,
            };

            if let sqlparser::ast::Expr::Function(func) = expr {
                let agg_func = match func.name.to_string().to_uppercase().as_str() {
                    "COUNT" => AggFunction::Count,
                    "SUM" => AggFunction::Sum,
                    "AVG" => AggFunction::Avg,
                    "MIN" => AggFunction::Min,
                    "MAX" => AggFunction::Max,
                    _ => continue,
                };
                let arg = match &func.args {
                    sqlparser::ast::FunctionArguments::List(list) => {
                        list.args.first().and_then(|arg| {
                            let arg_expr = match arg {
                                sqlparser::ast::FunctionArg::Unnamed(expr) => Some(expr),
                                sqlparser::ast::FunctionArg::Named { arg, .. } => Some(arg),
                                sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => Some(arg),
                            };
                            arg_expr.and_then(|expr| match expr {
                                sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                    self.expr_to_logical(e).ok()
                                }
                                _ => None,
                            })
                        })
                    }
                    _ => None,
                };
                if let Some(arg_expr) = arg {
                    aggregates.push((name, agg_func, arg_expr));
                }
            }
        }
        Ok(aggregates)
    }

    fn has_non_aggregate_projection(&self, projection: &[sqlparser::ast::SelectItem]) -> bool {
        projection.iter().any(|item| match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                !matches!(expr, sqlparser::ast::Expr::Function(_))
            }
            _ => true,
        })
    }

    fn expr_to_logical(&self, expr: &sqlparser::ast::Expr) -> Result<LogicalExpr, ExecutionError> {
        match expr {
            sqlparser::ast::Expr::Identifier(ident) => Ok(LogicalExpr::Column(ident.value.clone())),
            sqlparser::ast::Expr::CompoundIdentifier(parts) => {
                let name = parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                Ok(LogicalExpr::Column(name))
            }
            sqlparser::ast::Expr::Value(val) => match val {
                sqlparser::ast::Value::Number(n, _) => {
                    if n.contains('.') {
                        Ok(LogicalExpr::Literal(LiteralValue::Float64(
                            n.parse().unwrap_or(0.0),
                        )))
                    } else {
                        Ok(LogicalExpr::Literal(LiteralValue::Int64(
                            n.parse().unwrap_or(0),
                        )))
                    }
                }
                sqlparser::ast::Value::Boolean(b) => {
                    Ok(LogicalExpr::Literal(LiteralValue::Boolean(*b)))
                }
                sqlparser::ast::Value::SingleQuotedString(s) => {
                    Ok(LogicalExpr::Literal(LiteralValue::String(s.clone())))
                }
                _ => Err(ExecutionError::OperatorError(
                    "Unsupported literal type".to_string(),
                )),
            },
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let binary_op = match op {
                    sqlparser::ast::BinaryOperator::Eq => BinaryOp::Eq,
                    sqlparser::ast::BinaryOperator::NotEq => BinaryOp::Neq,
                    sqlparser::ast::BinaryOperator::Lt => BinaryOp::Lt,
                    sqlparser::ast::BinaryOperator::LtEq => BinaryOp::LtEq,
                    sqlparser::ast::BinaryOperator::Gt => BinaryOp::Gt,
                    sqlparser::ast::BinaryOperator::GtEq => BinaryOp::GtEq,
                    sqlparser::ast::BinaryOperator::And => BinaryOp::And,
                    sqlparser::ast::BinaryOperator::Or => BinaryOp::Or,
                    sqlparser::ast::BinaryOperator::Plus => BinaryOp::Add,
                    sqlparser::ast::BinaryOperator::Minus => BinaryOp::Sub,
                    sqlparser::ast::BinaryOperator::Multiply => BinaryOp::Mul,
                    sqlparser::ast::BinaryOperator::Divide => BinaryOp::Div,
                    _ => {
                        return Err(ExecutionError::OperatorError(format!(
                            "Unsupported binary operator: {:?}",
                            op
                        )))
                    }
                };
                Ok(LogicalExpr::BinaryOp {
                    op: binary_op,
                    left: Box::new(self.expr_to_logical(left)?),
                    right: Box::new(self.expr_to_logical(right)?),
                })
            }
            _ => Err(ExecutionError::OperatorError(format!(
                "Unsupported expression: {:?}",
                expr
            ))),
        }
    }
}

fn expr_to_name(expr: &sqlparser::ast::Expr) -> String {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => ident.value.clone(),
        sqlparser::ast::Expr::Function(func) => func.name.to_string(),
        _ => "?column?".to_string(),
    }
}
