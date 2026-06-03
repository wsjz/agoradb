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

use crate::chunk::DataChunk;
use crate::filter::FilterOperator;
use crate::limit::LimitOperator;
use crate::operator::Operator;
use crate::pipeline::{Pipeline, Sink};
use crate::predicate_builder::build_predicate_fn;
use crate::project::ProjectOperator;
use crate::scan::ScanOperator;
use crate::source::{EmptySource, TableScanSource};
use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::{ExecutionError, OperatorSpec, SpaceUri, StageId, StagePlan};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// ------------------------------------------------------------------
// Legacy helpers (retained for backward compatibility with executor)
// ------------------------------------------------------------------

/// Build an operator pipeline from a slice of definitions, wiring each
/// operator's output to the next one.  The last operator's output is
/// wired to `sink`.
///
/// Returns the **head** of the pipeline (the first operator), which the
/// caller must drive (usually a ScanOperator).
pub async fn build_pipeline_from_sink(
    operators: &[OperatorSpec],
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
    op_def: &OperatorSpec,
    _catalog: &Arc<AgoraCatalog>,
) -> Result<Box<dyn Operator>, ExecutionError> {
    match op_def {
        OperatorSpec::Filter { predicate } => {
            let pred_fn = build_predicate_fn(predicate)?;
            Ok(Box::new(FilterOperator::new(pred_fn)))
        }
        OperatorSpec::Project { columns } => Ok(Box::new(ProjectOperator::new(columns.clone()))),
        OperatorSpec::Limit { skip, fetch } => Ok(Box::new(LimitOperator::new(*skip, *fetch))),
        OperatorSpec::Scan { .. } => Err(ExecutionError::OperatorError(
            "Scan should not appear in build_pipeline_from_sink — use run_scan_driver instead"
                .to_string(),
        )),
    }
}

/// The first operator in every pipeline is a Scan.  This helper drives
/// it by calling `execute()`.
pub async fn run_scan_driver(
    scan_def: &OperatorSpec,
    catalog: &Arc<AgoraCatalog>,
    output: Box<dyn Operator>,
) -> Result<(), ExecutionError> {
    match scan_def {
        OperatorSpec::Scan { space, .. } => {
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

// ------------------------------------------------------------------
// PipelineBuilder — converts a StagePlan into a Vec<Pipeline>
// ------------------------------------------------------------------

/// Converts a `StagePlan` into a `Vec<Pipeline>` by splitting at breaker boundaries.
///
/// A Stage with a HashJoin becomes two Pipelines:
///   Pipeline 0 (Build): left subtree → HashJoinBuildSink
///   Pipeline 1 (Probe): right subtree → HashJoinProbeOperator → [downstream ops] → CollectSink
pub struct PipelineBuilder {
    stage_id: StageId,
    next_pipeline_id: usize,
    next_join_id: usize,
    next_agg_id: usize,
    next_sort_id: usize,
    catalog: Arc<AgoraCatalog>,
    default_parallelism: usize,
}

impl PipelineBuilder {
    /// Legacy constructor (no args) — builds a linear Vec<OperatorSpec>.
    pub fn new() -> Self {
        use iceberg::io::FileIO;
        let file_io = FileIO::new_with_memory();
        Self {
            stage_id: 0,
            next_pipeline_id: 0,
            next_join_id: 0,
            next_agg_id: 0,
            next_sort_id: 0,
            catalog: Arc::new(AgoraCatalog::new(file_io, "/tmp")),
            default_parallelism: 1,
        }
    }

    /// New constructor for Pipeline-based building.
    pub fn new_with_catalog(stage_id: StageId, catalog: Arc<AgoraCatalog>, default_parallelism: usize) -> Self {
        Self {
            stage_id,
            next_pipeline_id: 0,
            next_join_id: 0,
            next_agg_id: 0,
            next_sort_id: 0,
            catalog,
            default_parallelism,
        }
    }

    /// Legacy build: returns a linear Vec<OperatorSpec> for the old executor.
    pub fn build(&self, plan: &StagePlan) -> Result<Vec<OperatorSpec>, ExecutionError> {
        let mut ops = Vec::new();
        self.collect_operator_specs(plan, &mut ops)?;
        Ok(ops)
    }

    /// New build: returns a Vec<Pipeline> with breaker splitting.
    pub async fn build_pipelines(
        &mut self,
        plan: &StagePlan,
    ) -> Result<Vec<Pipeline>, ExecutionError> {
        let mut pipelines = Vec::new();
        self.build_inner(plan, &mut pipelines, None).await?;
        Ok(pipelines)
    }

    // ------------------------------------------------------------------
    // Legacy: collect OperatorSpecs
    // ------------------------------------------------------------------

    fn collect_operator_specs(
        &self,
        plan: &StagePlan,
        ops: &mut Vec<OperatorSpec>,
    ) -> Result<(), ExecutionError> {
        match plan {
            StagePlan::Scan { space, projection, filter } => {
                ops.push(OperatorSpec::Scan {
                    space: space.clone(),
                    projection: projection.clone(),
                    filter: filter.clone(),
                });
                Ok(())
            }
            StagePlan::Filter { predicate, input } => {
                self.collect_operator_specs(input, ops)?;
                ops.push(OperatorSpec::Filter {
                    predicate: predicate.clone(),
                });
                Ok(())
            }
            StagePlan::Project { columns, input } => {
                self.collect_operator_specs(input, ops)?;
                ops.push(OperatorSpec::Project {
                    columns: columns.clone(),
                });
                Ok(())
            }
            StagePlan::Limit { skip, fetch, input } => {
                self.collect_operator_specs(input, ops)?;
                ops.push(OperatorSpec::Limit {
                    skip: *skip,
                    fetch: *fetch,
                });
                Ok(())
            }
            StagePlan::HashJoinBuild { input, .. } => {
                self.collect_operator_specs(input, ops)?;
                Ok(())
            }
            StagePlan::HashJoinProbe { input, .. } => {
                self.collect_operator_specs(input, ops)?;
                Ok(())
            }
            StagePlan::HashAggregateAccumulate { input, .. } => {
                self.collect_operator_specs(input, ops)?;
                Ok(())
            }
            StagePlan::HashAggregateEmit { .. } => Ok(()),
            StagePlan::SortCollect { input, .. } => {
                self.collect_operator_specs(input, ops)?;
                Ok(())
            }
            StagePlan::SortEmit { .. } => Ok(()),
            StagePlan::ExchangeSource => Ok(()),
            // New unified breakers — not supported by legacy build
            StagePlan::HashJoin { .. }
            | StagePlan::HashAggregate { .. }
            | StagePlan::Sort { .. } => {
                Err(ExecutionError::OperatorError(
                    "New unified breaker StagePlan variants require build_pipelines()".to_string(),
                ))
            }
        }
    }

    // ------------------------------------------------------------------
    // New: build Pipeline DAG
    // ------------------------------------------------------------------

    fn build_inner<'a>(
        &'a mut self,
        plan: &'a StagePlan,
        pipelines: &'a mut Vec<Pipeline>,
        parent_sink: Option<Box<dyn Sink>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionError>> + 'a>> {
        Box::pin(async move {
        match plan {
            // ========== Breaker: HashJoin ==========
            StagePlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
                join_type,
            } => {
                let join_id = self.next_join_id;
                self.next_join_id += 1;

                // Build Pipeline: left subtree → HashJoinBuildSink
                let build_pipeline_id = self.next_pipeline_id();
                let build_source = self.build_source(left).await?;
                let build_ops = self.build_operators(left)?;
                let build_sink = Box::new(crate::hash_join::HashJoinBuildSink::new(join_id, *left_key));

                pipelines.push(Pipeline {
                    id: build_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: build_source,
                    operators: build_ops,
                    sink: build_sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });

                // Probe Pipeline: right subtree → HashJoinProbeOperator → downstream → sink
                let probe_pipeline_id = self.next_pipeline_id();
                let probe_source = self.build_source(right).await?;
                let mut probe_ops = self.build_operators(right)?;
                probe_ops.push(Box::new(crate::hash_join::HashJoinProbeOperator::new(
                    *left_key,
                    *right_key,
                    join_type.clone(),
                    join_id,
                )));

                let probe_sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: probe_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: probe_source,
                    operators: probe_ops,
                    sink: probe_sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![build_pipeline_id],
                });

                Ok(())
            }

            // ========== Breaker: HashAggregate ==========
            StagePlan::HashAggregate {
                input,
                group_columns,
                agg_columns,
            } => {
                let agg_id = self.next_agg_id;
                self.next_agg_id += 1;

                // Accumulate Pipeline
                let accum_pipeline_id = self.next_pipeline_id();
                let accum_source = self.build_source(input).await?;
                let accum_ops = self.build_operators(input)?;
                let accum_sink = Box::new(crate::hash_aggregate::HashAggregateAccumulateSink::new(
                    agg_id,
                    group_columns.clone(),
                    agg_columns.clone(),
                ));

                pipelines.push(Pipeline {
                    id: accum_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: accum_source,
                    operators: accum_ops,
                    sink: accum_sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });

                // Emit Pipeline
                let emit_pipeline_id = self.next_pipeline_id();
                let emit_sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: emit_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: Box::new(|_| Box::new(EmptySource)),
                    operators: vec![Box::new(crate::hash_aggregate::HashAggregateEmitOperator::new(
                        agg_id,
                        group_columns.clone(),
                        agg_columns.clone(),
                    ))],
                    sink: emit_sink,
                    parallelism: 1,
                    dependencies: vec![accum_pipeline_id],
                });

                Ok(())
            }

            // ========== Breaker: Sort ==========
            StagePlan::Sort {
                input,
                sort_columns,
                directions,
                limit,
            } => {
                let sort_id = self.next_sort_id;
                self.next_sort_id += 1;

                // Collect Pipeline
                let collect_pipeline_id = self.next_pipeline_id();
                let collect_source = self.build_source(input).await?;
                let collect_ops = self.build_operators(input)?;
                let collect_sink = Box::new(crate::sort::SortCollectSink::new(
                    sort_id,
                    sort_columns.clone(),
                    directions.clone(),
                    *limit,
                ));

                pipelines.push(Pipeline {
                    id: collect_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: collect_source,
                    operators: collect_ops,
                    sink: collect_sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });

                // Emit Pipeline
                let emit_pipeline_id = self.next_pipeline_id();
                let emit_sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: emit_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: Box::new(|_| Box::new(EmptySource)),
                    operators: vec![Box::new(crate::sort::SortEmitOperator::new(
                        sort_id,
                        sort_columns.clone(),
                        directions.clone(),
                        *limit,
                    ))],
                    sink: emit_sink,
                    parallelism: 1,
                    dependencies: vec![collect_pipeline_id],
                });

                Ok(())
            }

            // ========== Linear operators ==========
            StagePlan::Filter { predicate, input } => {
                let mut ops = self.build_operators(input)?;
                ops.push(Box::new(crate::filter::FilterPipelineOperator::new(
                    build_predicate_fn(predicate)?,
                )));
                self.build_sink_pipeline(input, ops, parent_sink, pipelines).await
            }

            StagePlan::Project { columns, input } => {
                let mut ops = self.build_operators(input)?;
                ops.push(Box::new(crate::project::ProjectPipelineOperator::new(columns.clone())));
                self.build_sink_pipeline(input, ops, parent_sink, pipelines).await
            }

            StagePlan::Limit { skip, fetch, input } => {
                let mut ops = self.build_operators(input)?;
                ops.push(Box::new(crate::limit::LimitPipelineOperator::new(*skip, *fetch)));
                self.build_sink_pipeline(input, ops, parent_sink, pipelines).await
            }

            // ========== Leaf nodes ==========
            StagePlan::Scan {
                space,
                projection: _,
                filter: _,
            } => {
                let source = self.build_scan_source(space.clone()).await?;
                let sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: self.next_pipeline_id(),
                    stage_id: self.stage_id,
                    source_factory: source,
                    operators: vec![],
                    sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });
                Ok(())
            }

            StagePlan::ExchangeSource => {
                let sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));
                pipelines.push(Pipeline {
                    id: self.next_pipeline_id(),
                    stage_id: self.stage_id,
                    source_factory: Box::new(|_| Box::new(EmptySource)),
                    operators: vec![],
                    sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });
                Ok(())
            }

            // Old breaker variants — should not appear in new StagePlans
            StagePlan::HashJoinBuild { .. }
            | StagePlan::HashJoinProbe { .. }
            | StagePlan::HashAggregateAccumulate { .. }
            | StagePlan::HashAggregateEmit { .. }
            | StagePlan::SortCollect { .. }
            | StagePlan::SortEmit { .. } => {
                Err(ExecutionError::OperatorError(
                    "Old breaker StagePlan variants are not supported by new PipelineBuilder".to_string(),
                ))
            }
        }
        })
    }

    /// Build operators for non-breaker, non-leaf parts of the tree.
    fn build_operators(
        &self,
        plan: &StagePlan,
    ) -> Result<Vec<Box<dyn crate::pipeline::PipelineOperator>>, ExecutionError> {
        let mut ops = Vec::new();
        self.collect_operators(plan, &mut ops)?;
        Ok(ops)
    }

    fn collect_operators(
        &self,
        plan: &StagePlan,
        ops: &mut Vec<Box<dyn crate::pipeline::PipelineOperator>>,
    ) -> Result<(), ExecutionError> {
        match plan {
            StagePlan::Filter { predicate, input } => {
                self.collect_operators(input, ops)?;
                ops.push(Box::new(crate::filter::FilterPipelineOperator::new(
                    build_predicate_fn(predicate)?,
                )));
                Ok(())
            }
            StagePlan::Project { columns, input } => {
                self.collect_operators(input, ops)?;
                ops.push(Box::new(crate::project::ProjectPipelineOperator::new(columns.clone())));
                Ok(())
            }
            StagePlan::Limit { skip, fetch, input } => {
                self.collect_operators(input, ops)?;
                ops.push(Box::new(crate::limit::LimitPipelineOperator::new(*skip, *fetch)));
                Ok(())
            }
            // Stop at leaves and breakers
            StagePlan::Scan { .. }
            | StagePlan::ExchangeSource { .. }
            | StagePlan::HashJoin { .. }
            | StagePlan::HashAggregate { .. }
            | StagePlan::Sort { .. }
            | StagePlan::HashJoinBuild { .. }
            | StagePlan::HashJoinProbe { .. }
            | StagePlan::HashAggregateAccumulate { .. }
            | StagePlan::HashAggregateEmit { .. }
            | StagePlan::SortCollect { .. }
            | StagePlan::SortEmit { .. } => Ok(()),
        }
    }

    /// Build a source factory for a plan subtree.
    async fn build_source(
        &self,
        plan: &StagePlan,
    ) -> Result<Box<dyn Fn(usize) -> Box<dyn crate::source::Source> + Send + Sync>, ExecutionError> {
        match plan {
            StagePlan::Scan { space, .. } => {
                self.build_scan_source(space.clone()).await
            }
            StagePlan::ExchangeSource => {
                Ok(Box::new(|_| Box::new(EmptySource)))
            }
            _ => {
                // For non-leaf plans, the source is handled by recursive build_inner
                Ok(Box::new(|_| Box::new(EmptySource)))
            }
        }
    }

    async fn build_scan_source(
        &self,
        space: agoradb_core::SpaceUri,
    ) -> Result<Box<dyn Fn(usize) -> Box<dyn crate::source::Source> + Send + Sync>, ExecutionError> {
        let catalog = self.catalog.clone();
        let snapshot_id = self.get_snapshot_id(&space).await?;
        let morsels = catalog
            .list_morsels(&space, snapshot_id, 10_000)
            .await
            .map_err(ExecutionError::Catalog)?;

        Ok(Box::new(move |_task_id: usize| {
            Box::new(TableScanSource::new(
                catalog.clone(),
                space.clone(),
                snapshot_id,
                morsels.clone(),
            ))
        }))
    }

    async fn get_snapshot_id(
        &self,
        space: &agoradb_core::SpaceUri,
    ) -> Result<i64, ExecutionError> {
        use iceberg::Catalog;
        let table_ident = iceberg::TableIdent::from_strs(["default", &space.name])
            .map_err(|e| ExecutionError::OperatorError(format!("Invalid table ident: {e}")))?;
        let table = self
            .catalog
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

    /// Helper: for linear plans, build a single pipeline with source + ops + sink.
    fn build_sink_pipeline<'a>(
        &'a mut self,
        input: &'a StagePlan,
        ops: Vec<Box<dyn crate::pipeline::PipelineOperator>>,
        parent_sink: Option<Box<dyn Sink>>,
        pipelines: &'a mut Vec<Pipeline>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionError>> + 'a>> {
        Box::pin(async move {
        match input {
            StagePlan::Scan { .. } | StagePlan::ExchangeSource => {
                let source = self.build_source(input).await?;
                let sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));
                pipelines.push(Pipeline {
                    id: self.next_pipeline_id(),
                    stage_id: self.stage_id,
                    source_factory: source,
                    operators: ops,
                    sink,
                    parallelism: self.default_parallelism,
                    dependencies: vec![],
                });
                Ok(())
            }
            _ => self.build_inner(input, pipelines, parent_sink).await,
        }
        })
    }

    fn next_pipeline_id(&mut self) -> usize {
        let id = self.next_pipeline_id;
        self.next_pipeline_id += 1;
        id
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------
// CollectSink — collects DataChunks into a Vec, implements Sink
// ------------------------------------------------------------------

use std::sync::Mutex;

pub struct CollectSink {
    results: Arc<Mutex<Vec<DataChunk>>>,
}

impl CollectSink {
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn into_results(self) -> Vec<DataChunk> {
        match Arc::try_unwrap(self.results) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn get_results_arc(&self) -> Arc<Mutex<Vec<DataChunk>>> {
        self.results.clone()
    }
}

impl Sink for CollectSink {
    fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.results.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl crate::pipeline::CloneSink for CollectSink {
    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(Self {
            results: Arc::new(Mutex::new(Vec::new())),
        })
    }
}
