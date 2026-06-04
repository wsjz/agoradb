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

use crate::adapters::CollectSink;
use crate::chunk::DataChunk;
use crate::filter::FilterOperator;
use crate::limit::LimitOperator;
use crate::operator::Operator;
use crate::pipeline::{Pipeline, Sink};
use crate::predicate_builder::build_predicate_fn;
use crate::project::ProjectOperator;
use crate::scan::ScanOperator;
use crate::source::{EmptySource, EmitSource, TableScanSource};
use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::{ExecutionError, Morsel, OperatorSpec, SpaceUri, StageId, StagePlan};
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
        Self::new_with_pipeline_offset(stage_id, catalog, default_parallelism, 0)
    }

    /// New constructor with a global pipeline ID offset.
    /// Ensures globally unique pipeline IDs when multiple stages share one scheduler.
    pub fn new_with_pipeline_offset(
        stage_id: StageId,
        catalog: Arc<AgoraCatalog>,
        default_parallelism: usize,
        pipeline_id_offset: usize,
    ) -> Self {
        Self {
            stage_id,
            next_pipeline_id: pipeline_id_offset,
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

                // Shared global state between build and probe
                let hj_global = Arc::new(crate::hash_join::HashJoinGlobalState::new());
                hj_global.set_expected_tasks(self.default_parallelism);

                // Build Pipeline: left subtree → HashJoinBuildSink
                let build_pipeline_id = self.next_pipeline_id();
                let build_source = self.build_source(left).await?;
                let build_ops = self.build_operators(left)?;
                let build_sink = Box::new(crate::hash_join::HashJoinBuildSink::new(
                    join_id, *left_key, hj_global.clone(),
                ));

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
                    hj_global.clone(),
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

                // Shared global state between accumulate and emit
                let agg_global = Arc::new(crate::hash_aggregate::HashAggregateGlobalState::new(
                    group_columns.clone(),
                    agg_columns.clone(),
                ));
                agg_global.set_expected_tasks(self.default_parallelism);

                // Build input with accumulate sink as parent.
                // If input is a nested breaker (e.g. HashJoin), the inner breaker's
                // result pipeline will use the accumulate sink directly.
                let accum_sink = Box::new(crate::hash_aggregate::HashAggregateAccumulateSink::new(
                    agg_id,
                    group_columns.clone(),
                    agg_columns.clone(),
                    agg_global.clone(),
                ));
                self.build_inner(input, pipelines, Some(accum_sink)).await?;
                let accum_pipeline_id = pipelines.last().map(|p| p.id).unwrap_or(0);

                // Emit Pipeline
                let emit_pipeline_id = self.next_pipeline_id();
                let emit_sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: emit_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: Box::new(|_| Box::new(EmitSource::new())),
                    operators: vec![Box::new(crate::hash_aggregate::HashAggregateEmitOperator::new(
                        agg_id,
                        group_columns.clone(),
                        agg_columns.clone(),
                        agg_global.clone(),
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

                // Shared sort state between collect and emit
                let sort_state = Arc::new(crate::sort::SortState::new(
                    sort_columns.clone(),
                    directions.clone(),
                    *limit,
                ));

                // Build input with collect sink as parent.
                // If input is a nested breaker, the inner breaker's result
                // pipeline will use the collect sink directly.
                let collect_sink = Box::new(crate::sort::SortCollectSink::new(
                    sort_id,
                    sort_state.clone(),
                ));
                self.build_inner(input, pipelines, Some(collect_sink)).await?;
                let collect_pipeline_id = pipelines.last().map(|p| p.id).unwrap_or(0);

                // Emit Pipeline
                let emit_pipeline_id = self.next_pipeline_id();
                let emit_sink = parent_sink.unwrap_or_else(|| Box::new(CollectSink::new()));

                pipelines.push(Pipeline {
                    id: emit_pipeline_id,
                    stage_id: self.stage_id,
                    source_factory: Box::new(|_| Box::new(EmitSource::new())),
                    operators: vec![Box::new(crate::sort::SortEmitOperator::new(
                        sort_id,
                        sort_state.clone(),
                    ))],
                    sink: emit_sink,
                    parallelism: 1,
                    dependencies: vec![collect_pipeline_id],
                });

                Ok(())
            }

            // ========== Linear operators ==========
            // ========== Linear operators ==========
            // build_operators(plan) collects all operators from this node down to the leaf.
            // build_sink_pipeline then finds the actual leaf (Scan/ExchangeSource).
            StagePlan::Filter { input, .. } => {
                let ops = self.build_operators(plan)?;
                self.build_sink_pipeline(input, ops, parent_sink, pipelines).await
            }

            StagePlan::Project { input, .. } => {
                let ops = self.build_operators(plan)?;
                self.build_sink_pipeline(input, ops, parent_sink, pipelines).await
            }

            StagePlan::Limit { input, .. } => {
                let ops = self.build_operators(plan)?;
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
                    predicate.clone(),
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
        // Walk down linear operators to find the actual leaf (Scan or ExchangeSource).
        let mut current = plan;
        loop {
            match current {
                StagePlan::Scan { space, .. } => {
                    return self.build_scan_source(space.clone()).await;
                }
                StagePlan::ExchangeSource => {
                    return Ok(Box::new(|_| Box::new(EmptySource)));
                }
                StagePlan::Filter { input, .. }
                | StagePlan::Project { input, .. }
                | StagePlan::Limit { input, .. } => {
                    current = input;
                }
                _ => {
                    // Breaker plans should not reach here — their source is built separately.
                    return Ok(Box::new(|_| Box::new(EmptySource)));
                }
            }
        }
    }

    async fn build_scan_source(
        &self,
        space: agoradb_core::SpaceUri,
    ) -> Result<Box<dyn Fn(usize) -> Box<dyn crate::source::Source> + Send + Sync>, ExecutionError> {
        let catalog = self.catalog.clone();
        let snapshot_id = self.get_snapshot_id(&space).await?;
        let all_morsels = catalog
            .list_morsels(&space, snapshot_id, 10_000)
            .await
            .map_err(ExecutionError::Catalog)?;

        // Partition morsels round-robin across tasks so each worker reads a distinct subset.
        let parallelism = self.default_parallelism.max(1);
        let mut partitioned: Vec<Vec<Morsel>> = (0..parallelism).map(|_| Vec::new()).collect();
        for (i, morsel) in all_morsels.into_iter().enumerate() {
            partitioned[i % parallelism].push(morsel);
        }

        Ok(Box::new(move |task_id: usize| {
            let task_morsels = partitioned
                .get(task_id)
                .cloned()
                .unwrap_or_default();
            Box::new(TableScanSource::new(
                catalog.clone(),
                space.clone(),
                snapshot_id,
                task_morsels,
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
            _ => {
                // Input is not a leaf — follow linear operators down to the actual leaf.
                let mut current = input;
                loop {
                    match current {
                        StagePlan::Scan { .. } | StagePlan::ExchangeSource => {
                            let source = self.build_source(current).await?;
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
                            return Ok(());
                        }
                        StagePlan::Filter { input: next, .. }
                        | StagePlan::Project { input: next, .. }
                        | StagePlan::Limit { input: next, .. } => {
                            current = next;
                        }
                        _ => {
                            let before_count = pipelines.len();
                            self.build_inner(current, pipelines, parent_sink).await?;
                            let after_count = pipelines.len();
                            // Attach any downstream linear operators collected above
                            // the breaker to the breaker's result pipeline.
                            if after_count > before_count && !ops.is_empty() {
                                let last_idx = after_count - 1;
                                pipelines[last_idx].operators.extend(ops);
                            }
                            return Ok(());
                        }
                    }
                }
            }
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

// CollectSink moved to adapters.rs (implements new Sink trait)
