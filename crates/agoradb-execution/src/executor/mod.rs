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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::{
    AggFunction, JoinType as PhysicalJoinType, OperatorDef, Stage, StagePlan, StageTask,
};
use agoradb_core::ExecutionError;

use crate::chunk::DataChunk;
use crate::adapters::{AggregateAdapter, CollectSink, JoinAdapter};
use crate::pipeline_builder;
use crate::hash_aggregate::HashAggregateOperator;
use crate::hash_join::HashJoinOperator;

use crate::operator::Operator;
use crate::morsel_scheduler::MorselScheduler;
use crate::parallel_executor::ParallelExecutor;



/// An executor that runs a [`StagePlan`] by scheduling stages in
/// topological order and managing shared breaker state between stages.
///
/// When a stage's `parallelism > 1`, the executor spawns multiple
/// independent pipelines (one per worker) that each process a slice
/// (morsel) of the input data in parallel.
pub struct Executor;

impl Executor {
    /// Execute a [`StagePlan`] and return the final result chunks.
    ///
    /// Stages are executed in topological order (respecting `dependencies`).
    /// Breaker operators (HashJoin, HashAggregate) are created during the
    /// build/accumulate stage and shared with the subsequent probe/emit stage.
    pub async fn execute(
        &self,
        plan: &StagePlan,
        catalog: &Arc<AgoraCatalog>,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        let order = Self::topo_sort(&plan.stages)?;

        // Shared breaker state passed between dependent stages.
        let mut hash_joins: HashMap<usize, Arc<Mutex<HashJoinOperator>>> = HashMap::new();
        let mut aggregates: HashMap<usize, Arc<Mutex<HashAggregateOperator>>> = HashMap::new();
        let mut stage_results: HashMap<usize, Vec<DataChunk>> = HashMap::new();

        for &stage_id in &order {
            let stage = &plan.stages[stage_id];
            let results = Self::run_stage(
                stage,
                catalog,
                &mut hash_joins,
                &mut aggregates,
                &stage_results,
            )
            .await?;
            stage_results.insert(stage_id, results);
        }

        // Return results from the last stage in topological order.
        let last_stage = order.last().copied().unwrap_or(0);
        Ok(stage_results.remove(&last_stage).unwrap_or_default())
    }

    // ------------------------------------------------------------------
    // Stage execution dispatcher
    // ------------------------------------------------------------------

    async fn run_stage(
        stage: &Stage,
        catalog: &Arc<AgoraCatalog>,
        hash_joins: &mut HashMap<usize, Arc<Mutex<HashJoinOperator>>>,
        aggregates: &mut HashMap<usize, Arc<Mutex<HashAggregateOperator>>>,
        stage_results: &HashMap<usize, Vec<DataChunk>>,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        match &stage.task {
            StageTask::Pipeline { operators } => {
                Self::run_pipeline(operators, catalog, stage.parallelism).await
            }

            StageTask::HashJoinBuild {
                join_id,
                operators,
                left_key,
                right_key,
                join_type,
            } => {
                let exec_join_type = match join_type {
                    PhysicalJoinType::Inner => PhysicalJoinType::Inner,
                    PhysicalJoinType::Left => PhysicalJoinType::Left,
                    PhysicalJoinType::Right => PhysicalJoinType::Left, // Not fully supported yet
                    PhysicalJoinType::Full => PhysicalJoinType::Inner, // Not fully supported yet
                };
                let join = Arc::new(Mutex::new(HashJoinOperator::new(
                    *left_key,
                    *right_key,
                    exec_join_type,
                )));
                let adapter = JoinAdapter(join.clone());
                Self::run_operators_into_sink(operators, catalog, Box::new(adapter)).await?;
                hash_joins.insert(*join_id, join);
                Ok(Vec::new())
            }

            StageTask::HashJoinProbe {
                join_id,
                operators,
                post_operators,
            } => {
                let join = hash_joins.get(join_id).cloned().ok_or_else(|| {
                    ExecutionError::OperatorError(format!(
                        "HashJoin {} not found — build stage must run first",
                        join_id
                    ))
                })?;
                join.lock().unwrap().start_probe();

                // Set up a sink to collect probed results.
                let results = Arc::new(Mutex::new(Vec::new()));
                let sink = CollectSink::new(results.clone());

                // If there are post_operators (e.g. Project, Limit), wire them
                // between the join output and the final sink.
                if !post_operators.is_empty() {
                    let post_head =
                        pipeline_builder::build_pipeline_from_sink(post_operators, catalog, Box::new(sink))
                            .await?;
                    join.lock().unwrap().set_output(post_head);
                } else {
                    join.lock().unwrap().set_output(Box::new(sink));
                }

                let adapter = JoinAdapter(join.clone());
                Self::run_operators_into_sink(operators, catalog, Box::new(adapter)).await?;
                join.lock().unwrap().finalize()?;

                let locked = results.lock().unwrap();
                Ok(locked.iter().map(|c| c.deep_clone()).collect())
            }

            StageTask::AggregateAccumulate {
                agg_id,
                operators,
                group_columns,
                agg_columns,
            } => {
                let exec_agg_cols: Vec<(usize, AggFunction)> = agg_columns
                    .iter()
                    .map(|(col, func)| (*col, func.clone()))
                    .collect();
                let agg = Arc::new(Mutex::new(HashAggregateOperator::new(
                    group_columns.clone(),
                    exec_agg_cols,
                )));

                if operators.is_empty() {
                    // Input comes from the previous stage's output.
                    let dep_id = stage.dependencies.last().copied().ok_or_else(|| {
                        ExecutionError::OperatorError(
                            "AggregateAccumulate with empty operators needs a dependency"
                                .to_string(),
                        )
                    })?;
                    let mut adapter = AggregateAdapter(agg.clone());
                    if let Some(input_chunks) = stage_results.get(&dep_id) {
                        for chunk in input_chunks {
                            adapter.push(chunk.deep_clone())?;
                        }
                    }
                } else {
                    let adapter = AggregateAdapter(agg.clone());
                    Self::run_operators_into_sink(operators, catalog, Box::new(adapter)).await?;
                }

                aggregates.insert(*agg_id, agg);
                Ok(Vec::new())
            }

            StageTask::AggregateEmit {
                agg_id,
                post_operators,
            } => {
                let agg = aggregates.get(agg_id).cloned().ok_or_else(|| {
                    ExecutionError::OperatorError(format!(
                        "Aggregate {} not found — accumulate stage must run first",
                        agg_id
                    ))
                })?;
                let chunk = agg.lock().unwrap().emit_results()?;

                if !post_operators.is_empty() {
                    Self::run_operators_on_chunk(post_operators, catalog, chunk).await
                } else {
                    Ok(vec![chunk])
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Pipeline execution (single- or multi-threaded)
    // ------------------------------------------------------------------

    async fn run_pipeline(
        operators: &[OperatorDef],
        catalog: &Arc<AgoraCatalog>,
        parallelism: usize,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        if operators.is_empty() {
            return Ok(Vec::new());
        }

        if parallelism <= 1 {
            Self::run_pipeline_single(operators, catalog).await
        } else {
            Self::run_pipeline_parallel(operators, catalog, parallelism).await
        }
    }

    async fn run_pipeline_parallel(
        operators: &[OperatorDef],
        catalog: &Arc<AgoraCatalog>,
        parallelism: usize,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        let (space, snapshot_id) = match &operators[0] {
            OperatorDef::Scan { space, .. } => {
                let snapshot_id = pipeline_builder::get_snapshot_id(catalog, space).await?;
                (space.clone(), snapshot_id)
            }
            other => {
                return Err(ExecutionError::OperatorError(format!(
                    "Parallel pipeline must start with Scan, got: {:?}",
                    other
                )))
            }
        };

        let morsels = catalog
            .list_morsels(&space, snapshot_id, 10_000)
            .await
            .map_err(ExecutionError::Catalog)?;

        if morsels.len() <= 1 {
            return Self::run_pipeline_single(operators, catalog).await;
        }

        let scheduler = Arc::new(MorselScheduler::new(morsels));
        let num_workers = parallelism.min(scheduler.total());
        let remaining_ops: Arc<Vec<OperatorDef>> = Arc::new(operators[1..].to_vec());
        let catalog = catalog.clone();

        ParallelExecutor::execute(scheduler, num_workers, move |morsel| {
            let ops = remaining_ops.clone();
            let cat = catalog.clone();
            async move {
                let batches = cat
                    .read_morsel(&morsel)
                    .await
                    .map_err(ExecutionError::Catalog)?;

                if batches.is_empty() {
                    return Ok(Vec::new());
                }

                if ops.is_empty() {
                    let mut results = Vec::with_capacity(batches.len());
                    for batch in batches {
                        results.push(DataChunk::from_record_batch(&batch)?);
                    }
                    return Ok(results);
                }

                let chunk_results = Arc::new(Mutex::new(Vec::new()));
                let sink = CollectSink::new(chunk_results.clone());

                let mut head =
                    pipeline_builder::build_pipeline_from_sink(&ops, &cat, Box::new(sink)).await?;

                for batch in batches {
                    let chunk = DataChunk::from_record_batch(&batch)?;
                    head.push(chunk)?;
                }
                head.finalize()?;

                let locked = chunk_results.lock().unwrap();
                Ok(locked.iter().map(|c| c.deep_clone()).collect())
            }
        })
        .await
    }

    async fn run_pipeline_single(
        operators: &[OperatorDef],
        catalog: &Arc<AgoraCatalog>,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        let results = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectSink::new(results.clone());

        let head = pipeline_builder::build_pipeline_from_sink(
            &operators[1..],
            catalog,
            Box::new(sink),
        )
        .await?;

        pipeline_builder::run_scan_driver(&operators[0],
            catalog,
            head,
        )
        .await?;

        let locked = results.lock().unwrap();
        Ok(locked.iter().map(|c| c.deep_clone()).collect())
    }

    /// Execute a list of operators, feeding the final output into `sink`.
    async fn run_operators_into_sink(
        operators: &[OperatorDef],
        catalog: &Arc<AgoraCatalog>,
        sink: Box<dyn Operator>,
    ) -> Result<(), ExecutionError> {
        if operators.is_empty() {
            return Ok(());
        }

        let head =
            pipeline_builder::build_pipeline_from_sink(&operators[1..], catalog, sink).await?;

        pipeline_builder::run_scan_driver(&operators[0],
            catalog,
            head,
        )
        .await
    }

    /// Run a single DataChunk through a pipeline of operators.
    async fn run_operators_on_chunk(
        operators: &[OperatorDef],
        catalog: &Arc<AgoraCatalog>,
        chunk: DataChunk,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        if operators.is_empty() {
            return Ok(vec![chunk]);
        }

        let results = Arc::new(Mutex::new(Vec::new()));
        let sink = CollectSink::new(results.clone());

        let mut head =
            pipeline_builder::build_pipeline_from_sink(operators, catalog, Box::new(sink)).await?;

        head.push(chunk)?;
        head.finalize()?;

        let locked = results.lock().unwrap();
        Ok(locked.iter().map(|c| c.deep_clone()).collect())
    }

    // ------------------------------------------------------------------
    // Topological sort
    // ------------------------------------------------------------------

    fn topo_sort(stages: &[Stage]) -> Result<Vec<usize>, ExecutionError> {
        let n = stages.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let mut in_degree = vec![0; n];
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];

        for stage in stages {
            for &dep in &stage.dependencies {
                if dep >= n {
                    return Err(ExecutionError::OperatorError(format!(
                        "Invalid dependency {} for stage {} (only {} stages)",
                        dep, stage.id, n
                    )));
                }
                adj[dep].push(stage.id);
                in_degree[stage.id] += 1;
            }
        }

        let mut queue = VecDeque::new();
        for (i, degree) in in_degree.iter().enumerate().take(n) {
            if *degree == 0 {
                queue.push_back(i);
            }
        }

        let mut result = Vec::with_capacity(n);
        while let Some(u) = queue.pop_front() {
            result.push(u);
            for &v in &adj[u] {
                in_degree[v] -= 1;
                if in_degree[v] == 0 {
                    queue.push_back(v);
                }
            }
        }

        if result.len() != n {
            return Err(ExecutionError::OperatorError(
                "Cycle detected in stage dependencies".to_string(),
            ));
        }

        Ok(result)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::JoinType;
    use agoradb_core::SpaceUri;

    fn make_plan(stages: Vec<Stage>) -> StagePlan {
        StagePlan { stages }
    }

    #[test]
    fn test_topo_sort_empty() {
        let plan = make_plan(vec![]);
        let order = Executor::topo_sort(&plan.stages).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_topo_sort_single_stage() {
        let plan = make_plan(vec![Stage {
            id: 0,
            label: "scan".to_string(),
            dependencies: vec![],
            parallelism: 1,
            task: StageTask::Pipeline {
                operators: vec![OperatorDef::Scan {
                    space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
                    projection: None,
                    filter: None,
                }],
            },
        }]);
        let order = Executor::topo_sort(&plan.stages).unwrap();
        assert_eq!(order, vec![0]);
    }

    #[test]
    fn test_topo_sort_build_before_probe() {
        let plan = make_plan(vec![
            Stage {
                id: 0,
                label: "build".to_string(),
                dependencies: vec![],
                parallelism: 1,
                task: StageTask::HashJoinBuild {
                    join_id: 0,
                    operators: vec![],
                    left_key: 0,
                    right_key: 0,
                    join_type: JoinType::Inner,
                },
            },
            Stage {
                id: 1,
                label: "probe".to_string(),
                dependencies: vec![0],
                parallelism: 4,
                task: StageTask::HashJoinProbe {
                    join_id: 0,
                    operators: vec![],
                    post_operators: vec![],
                },
            },
        ]);
        let order = Executor::topo_sort(&plan.stages).unwrap();
        assert_eq!(order, vec![0, 1]);
    }

    #[test]
    fn test_topo_sort_cycle_detection() {
        let plan = make_plan(vec![
            Stage {
                id: 0,
                label: "a".to_string(),
                dependencies: vec![1],
                parallelism: 1,
                task: StageTask::Pipeline { operators: vec![] },
            },
            Stage {
                id: 1,
                label: "b".to_string(),
                dependencies: vec![0],
                parallelism: 1,
                task: StageTask::Pipeline { operators: vec![] },
            },
        ]);
        let result = Executor::topo_sort(&plan.stages);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cycle"));
    }
}
