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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use agoradb_catalog::AgoraCatalog;
use agoradb_core::plan::{
    AggFunction, BinaryOp as PhysicalBinOp, JoinType as PhysicalJoinType, PhysicalExpr,
    PhysicalPlan,
};
use agoradb_core::{ExecutionError, SpaceUri};
use iceberg::{Catalog, TableIdent};

use crate::chunk::DataChunk;
use crate::filter::{FilterOperator, PredicateFn};
use crate::hash_aggregate::{AggFunc, HashAggregateOperator};
use crate::hash_join::{HashJoinOperator, JoinType as ExecJoinType};
use crate::limit::LimitOperator;
use crate::operator::Operator;
use crate::project::ProjectOperator;
use crate::scan::ScanOperator;

// ========== Adapters for shared operators ==========

struct JoinAdapter(Arc<Mutex<HashJoinOperator>>);

impl Operator for JoinAdapter {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().push(chunk)
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().finalize()
    }
    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.0.lock().unwrap().set_output(output);
    }
}

struct AggregateAdapter(Arc<Mutex<HashAggregateOperator>>);

impl Operator for AggregateAdapter {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().push(chunk)
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().finalize()
    }
    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.0.lock().unwrap().set_output(output);
    }
}

// ========== Collecting sink ==========

struct CollectSink {
    results: Arc<Mutex<Vec<DataChunk>>>,
}

impl CollectSink {
    fn new(results: Arc<Mutex<Vec<DataChunk>>>) -> Self {
        Self { results }
    }
}

impl Operator for CollectSink {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.results.lock().unwrap().push(chunk);
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
    fn set_output(&mut self, _output: Box<dyn Operator>) {
        // Sink has no output
    }
}

// ========== Public API ==========

/// Execute a PhysicalPlan and return all resulting DataChunks.
pub async fn run_physical_plan(
    plan: &PhysicalPlan,
    catalog: &Arc<AgoraCatalog>,
    schema_map: &HashMap<String, usize>,
) -> Result<Vec<DataChunk>, ExecutionError> {
    let results = Arc::new(Mutex::new(Vec::new()));
    let sink = CollectSink::new(results.clone());
    run_inner(plan, catalog, schema_map, Box::new(sink)).await?;
    let locked = results.lock().unwrap();
    Ok(locked.iter().map(|c| c.deep_clone()).collect())
}

// ========== Recursive execution ==========

/// Boxed future type alias for recursive async execution.
type RunInnerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ExecutionError>> + Send + 'a>>;

fn run_inner<'a>(
    plan: &'a PhysicalPlan,
    catalog: &'a Arc<AgoraCatalog>,
    schema_map: &'a HashMap<String, usize>,
    mut sink: Box<dyn Operator>,
) -> RunInnerFuture<'a> {
    Box::pin(async move {
        match plan {
            PhysicalPlan::Scan { space, .. } => {
                let snapshot_id = get_snapshot_id(catalog, space).await?;
                let mut scan = ScanOperator::new(catalog.clone(), space.clone(), snapshot_id);
                scan.set_output(sink);
                scan.execute().await
            }
            PhysicalPlan::Filter { predicate, input } => {
                let pred = build_predicate(predicate, schema_map)?;
                let mut filter = FilterOperator::new(pred);
                filter.set_output(sink);
                run_inner(input, catalog, schema_map, Box::new(filter)).await
            }
            PhysicalPlan::Project { expressions, input } => {
                let indices = build_projections(expressions, schema_map)?;
                let mut project = ProjectOperator::new(indices);
                project.set_output(sink);
                run_inner(input, catalog, schema_map, Box::new(project)).await
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                let exec_join_type = convert_join_type(join_type);
                let join = Arc::new(Mutex::new(HashJoinOperator::new(
                    *left_key,
                    *right_key,
                    exec_join_type,
                )));

                // Build phase: execute left side, feeding into join build
                let join_build = JoinAdapter(join.clone());
                run_inner(left, catalog, schema_map, Box::new(join_build)).await?;
                join.lock().unwrap().start_probe();

                // Probe phase: execute right side, feeding into join probe
                // Set the output sink on the join so probed results flow downstream
                join.lock().unwrap().set_output(sink);
                let join_probe = JoinAdapter(join.clone());
                run_inner(right, catalog, schema_map, Box::new(join_probe)).await?;
                let result = join.lock().unwrap().finalize();
                result
            }
            PhysicalPlan::HashAggregate {
                input,
                group_exprs,
                agg_exprs,
            } => {
                let group_indices = build_group_indices(group_exprs, schema_map)?;
                let agg_indices = build_agg_indices(agg_exprs, schema_map)?;
                let agg = Arc::new(Mutex::new(HashAggregateOperator::new(
                    group_indices,
                    agg_indices,
                )));

                // Accumulate phase
                let agg_accum = AggregateAdapter(agg.clone());
                run_inner(input, catalog, schema_map, Box::new(agg_accum)).await?;

                // Emit phase
                let chunk = agg.lock().unwrap().emit_results()?;
                sink.push(chunk)?;
                sink.finalize()
            }
            PhysicalPlan::Limit { skip, fetch, input } => {
                let mut limit = LimitOperator::new(*skip, *fetch);
                limit.set_output(sink);
                run_inner(input, catalog, schema_map, Box::new(limit)).await
            }
        }
    })
}

// ========== Helper functions ==========

fn convert_join_type(join_type: &PhysicalJoinType) -> ExecJoinType {
    match join_type {
        PhysicalJoinType::Inner => ExecJoinType::Inner,
        PhysicalJoinType::Left => ExecJoinType::Left,
        PhysicalJoinType::Right => ExecJoinType::Left, // Not fully supported yet
        PhysicalJoinType::Full => ExecJoinType::Inner, // Not fully supported yet
    }
}

async fn get_snapshot_id(
    catalog: &Arc<AgoraCatalog>,
    space: &SpaceUri,
) -> Result<i64, ExecutionError> {
    let table_ident = TableIdent::from_strs(["default", &space.name])
        .map_err(|e| ExecutionError::OperatorError(format!("Invalid table ident: {e}")))?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| ExecutionError::Catalog(agoradb_core::CatalogError::Iceberg(e.to_string())))?;
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .ok_or_else(|| ExecutionError::OperatorError("No current snapshot".to_string()))?
        .snapshot_id();
    Ok(snapshot_id)
}

fn build_predicate(
    expr: &PhysicalExpr,
    _schema_map: &HashMap<String, usize>,
) -> Result<PredicateFn, ExecutionError> {
    match expr {
        PhysicalExpr::BinaryOp { op, left, right } => {
            let op = op.clone();
            let left = left.clone();
            let right = right.clone();
            Ok(Box::new(move |chunk, row| {
                evaluate_binary_op(&op, &left, &right, chunk, row)
            }))
        }
        _ => Err(ExecutionError::OperatorError(
            "Only binary ops supported in filter predicates".to_string(),
        )),
    }
}

fn evaluate_binary_op(
    op: &PhysicalBinOp,
    left: &PhysicalExpr,
    right: &PhysicalExpr,
    chunk: &DataChunk,
    row: usize,
) -> bool {
    match (left, right) {
        (PhysicalExpr::Column(idx), PhysicalExpr::Literal(val)) => {
            let col_val = chunk.columns[*idx].as_i64_slice()[row];
            match op {
                PhysicalBinOp::Eq => col_val == *val,
                PhysicalBinOp::Neq => col_val != *val,
                PhysicalBinOp::Lt => col_val < *val,
                PhysicalBinOp::LtEq => col_val <= *val,
                PhysicalBinOp::Gt => col_val > *val,
                PhysicalBinOp::GtEq => col_val >= *val,
                _ => false,
            }
        }
        _ => false,
    }
}

fn build_projections(
    expressions: &[PhysicalExpr],
    _schema_map: &HashMap<String, usize>,
) -> Result<Vec<usize>, ExecutionError> {
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

fn build_group_indices(
    expressions: &[PhysicalExpr],
    _schema_map: &HashMap<String, usize>,
) -> Result<Vec<usize>, ExecutionError> {
    expressions
        .iter()
        .map(|expr| match expr {
            PhysicalExpr::Column(idx) => Ok(*idx),
            _ => Err(ExecutionError::OperatorError(
                "Only column references supported in group-by".to_string(),
            )),
        })
        .collect()
}

fn build_agg_indices(
    expressions: &[(PhysicalExpr, AggFunction)],
    _schema_map: &HashMap<String, usize>,
) -> Result<Vec<(usize, AggFunc)>, ExecutionError> {
    expressions
        .iter()
        .map(|(expr, agg)| {
            let col_idx = match expr {
                PhysicalExpr::Column(idx) => *idx,
                _ => {
                    return Err(ExecutionError::OperatorError(
                        "Only column references supported in aggregate".to_string(),
                    ))
                }
            };
            let agg_func = match agg {
                AggFunction::Count => AggFunc::Count,
                AggFunction::Sum => AggFunc::Sum,
                AggFunction::Avg => AggFunc::Avg,
                AggFunction::Min => AggFunc::Min,
                AggFunction::Max => AggFunc::Max,
            };
            Ok((col_idx, agg_func))
        })
        .collect()
}
