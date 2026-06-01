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

use crate::filter::FilterOperator;
use crate::limit::LimitOperator;
use crate::operator::Operator;
use crate::predicate_builder::build_predicate_fn;
use crate::project::ProjectOperator;
use crate::scan::ScanOperator;
use agoradb_catalog::AgoraCatalog;
use agoradb_core::{ExecutionError, OperatorDef, SpaceUri};
use std::sync::Arc;

/// Build an operator pipeline from a slice of definitions, wiring each
/// operator's output to the next one.  The last operator's output is
/// wired to `sink`.
///
/// Returns the **head** of the pipeline (the first operator), which the
/// caller must drive (usually a ScanOperator).
pub async fn build_pipeline_from_sink(
    operators: &[OperatorDef],
    catalog: &Arc<AgoraCatalog>,
    sink: Box<dyn Operator>,
) -> Result<Box<dyn Operator>, ExecutionError> {
    let mut current: Box<dyn Operator> = sink;
    for op_def in operators.iter().rev() {
        let mut op = build_operator(op_def, catalog).await?;
        op.set_output(current);
        current = op;
    }
    Ok(current)
}

pub async fn build_operator(
    op_def: &OperatorDef,
    _catalog: &Arc<AgoraCatalog>,
) -> Result<Box<dyn Operator>, ExecutionError> {
    match op_def {
        OperatorDef::Filter { predicate } => {
            let pred_fn = build_predicate_fn(predicate)?;
            Ok(Box::new(FilterOperator::new(pred_fn)))
        }
        OperatorDef::Project { columns } => Ok(Box::new(ProjectOperator::new(columns.clone()))),
        OperatorDef::Limit { skip, fetch } => Ok(Box::new(LimitOperator::new(*skip, *fetch))),
        OperatorDef::Scan { .. } => Err(ExecutionError::OperatorError(
            "Scan should not appear in build_pipeline_from_sink — use run_scan_driver instead"
                .to_string(),
        )),
    }
}

/// The first operator in every pipeline is a Scan.  This helper drives
/// it by calling `execute()`.
pub async fn run_scan_driver(
    scan_def: &OperatorDef,
    catalog: &Arc<AgoraCatalog>,
    output: Box<dyn Operator>,
) -> Result<(), ExecutionError> {
    match scan_def {
        OperatorDef::Scan { space, .. } => {
            let snapshot_id = get_snapshot_id(catalog, space).await?;
            let mut scan = ScanOperator::new(catalog.clone(), space.clone(), snapshot_id);
            scan.set_output(output);
            scan.execute().await
        }
        other => Err(ExecutionError::OperatorError(format!(
            "Pipeline must start with Scan, got: {:?}",
            other
        ))),
    }
}

/// Resolve the current snapshot id for a space via the catalog.
pub async fn get_snapshot_id(
    catalog: &Arc<AgoraCatalog>,
    space: &SpaceUri,
) -> Result<i64, ExecutionError> {
    use iceberg::Catalog;
    let table_ident = iceberg::TableIdent::from_strs(["default", &space.name])
        .map_err(|e| ExecutionError::OperatorError(format!("Invalid table ident: {e}")))?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| {
            ExecutionError::Catalog(agoradb_core::CatalogError::Iceberg(e.to_string()))
        })?;
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .ok_or_else(|| ExecutionError::OperatorError("No current snapshot".to_string()))?
        .snapshot_id();
    Ok(snapshot_id)
}
