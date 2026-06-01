use crate::plan::{DataType, LogicalExpr, LogicalPlan};
use agoradb_core::ExecutionError;
use std::collections::HashMap;

/// Provides table schema information for the analyzer.
pub trait SchemaProvider {
    /// Return the schema (column name → DataType) for a given table.
    fn get_table_schema(&self, table: &str) -> Result<HashMap<String, DataType>, ExecutionError>;
}

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
            LogicalPlan::Scan { table, schema } => {
                let table_schema = provider.get_table_schema(table).map_err(|_| {
                    ExecutionError::OperatorError(format!("Table not found: {}", table))
                })?;
                *schema = table_schema
                    .iter()
                    .map(|(name, dt)| (name.clone(), dt.clone()))
                    .collect();
                Ok(table_schema)
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
                for expr in group_by {
                    self.validate_expr(expr, &input_schema)?;
                }
                let mut output_schema = HashMap::new();
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
                crate::plan::LiteralValue::Int64(_) => Ok(DataType::Int64),
                crate::plan::LiteralValue::Float64(_) => Ok(DataType::Float64),
                crate::plan::LiteralValue::Boolean(_) => Ok(DataType::Boolean),
                crate::plan::LiteralValue::String(_) => Ok(DataType::Utf8),
                crate::plan::LiteralValue::Null => Ok(DataType::Int64), // simplified
            },
            LogicalExpr::BinaryOp { .. } => Ok(DataType::Boolean),
            LogicalExpr::Function { .. } => Ok(DataType::Int64),
        }
    }
}
