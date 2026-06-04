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

use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::{ExecutionError, ExecutionPlan, Stage};

use crate::chunk::DataChunk;
use crate::pipeline::PipelineState;
use crate::adapters::CollectSink;
use crate::pipeline::Pipeline;
use crate::pipeline_builder::PipelineBuilder;
use crate::scheduler::{spawn_workers, TaskScheduler};

/// An executor that runs an ExecutionPlan using yield-based pipeline scheduling.
///
/// Each Stage is converted into a DAG of Pipelines. Pipelines with dependencies
/// (e.g., Probe depends on Build) are started only after their upstream completes.
/// Worker threads pull tasks from work-stealing queues.
pub struct Executor {
    num_workers: usize,
    /// Global pipeline ID counter to ensure uniqueness across concurrent stages.
    next_pipeline_id: AtomicUsize,
}

impl Executor {
    pub fn new() -> Self {
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            num_workers,
            next_pipeline_id: AtomicUsize::new(0),
        }
    }

    pub fn with_workers(num_workers: usize) -> Self {
        Self {
            num_workers,
            next_pipeline_id: AtomicUsize::new(0),
        }
    }

    /// Execute an ExecutionPlan and return the final result chunks.
    pub async fn execute(
        &self,
        plan: &ExecutionPlan,
        catalog: &Arc<AgoraCatalog>,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        if plan.stages.is_empty() {
            return Ok(Vec::new());
        }

        // Validate stage dependencies with topo sort
        let _order = Self::topo_sort(&plan.stages)?;

        // Create scheduler and spawn workers
        let scheduler = TaskScheduler::new(self.num_workers);
        let worker_handles = spawn_workers(scheduler.clone(), self.num_workers);

        // Execute stages with dependencies respected.
        let mut completed_stages: HashMap<usize, Vec<DataChunk>> = HashMap::new();
        let mut remaining_stages: Vec<_> = plan.stages.iter().collect();

        while !remaining_stages.is_empty() {
            // Find stages whose dependencies are all satisfied
            let ready_indices: Vec<usize> = remaining_stages
                .iter()
                .enumerate()
                .filter(|(_, stage)| {
                    stage.dependencies.iter().all(|dep| completed_stages.contains_key(dep))
                })
                .map(|(idx, _)| idx)
                .collect();

            if ready_indices.is_empty() && !remaining_stages.is_empty() {
                scheduler.shutdown();
                for handle in worker_handles {
                    let _ = handle.join();
                }
                return Err(ExecutionError::OperatorError(
                    "Deadlock: no stage can proceed".to_string(),
                ));
            }

            // Remove ready stages from remaining (in reverse order to preserve indices)
            let mut ready_stages = Vec::new();
            for &idx in ready_indices.iter().rev() {
                ready_stages.push(remaining_stages.remove(idx));
            }

            // Build pipelines for all ready stages (fast, no I/O).
            // Reassign globally unique pipeline IDs to avoid conflicts when
            // multiple stages share the same scheduler.
            let mut stage_pipeline_list: Vec<(usize, Vec<Pipeline>)> = Vec::new();
            for stage in ready_stages {
                let pipeline_offset = self
                    .next_pipeline_id
                    .fetch_add(1000, Ordering::SeqCst);
                let mut builder = PipelineBuilder::new_with_pipeline_offset(
                    stage.id,
                    catalog.clone(),
                    stage.parallelism.max(1),
                    pipeline_offset,
                );
                let mut pipelines = builder.build_pipelines(&stage.plan).await?;

                // Reassign globally unique pipeline IDs and update dependencies.
                let mut id_map: HashMap<usize, usize> = HashMap::new();
                for pipeline in &mut pipelines {
                    let old_id = pipeline.id;
                    let new_id = self.next_pipeline_id.fetch_add(1, Ordering::SeqCst);
                    pipeline.id = new_id;
                    id_map.insert(old_id, new_id);
                }
                for pipeline in &mut pipelines {
                    for dep in &mut pipeline.dependencies {
                        if let Some(&new_id) = id_map.get(dep) {
                            *dep = new_id;
                        }
                    }
                }

                stage_pipeline_list.push((stage.id, pipelines));
            }

            // Execute ready stages concurrently using FuturesUnordered.
            let mut futures = futures::stream::FuturesUnordered::new();
            for (stage_id, pipelines) in stage_pipeline_list {
                let catalog = catalog.clone();
                let scheduler = scheduler.clone();
                futures.push(async move {
                    let result = Self::run_pipelines(pipelines, &catalog, scheduler).await?;
                    Ok::<(usize, Vec<DataChunk>), ExecutionError>((stage_id, result))
                });
            }

            while let Some(result) = futures.next().await {
                let (stage_id, chunks) = result?;
                completed_stages.insert(stage_id, chunks);
            }
        }

        // Shutdown workers
        scheduler.shutdown();
        for handle in worker_handles {
            let _ = handle.join();
        }

        // Return results from the last stage (highest id)
        let last_stage_id = plan.stages.iter().map(|s| s.id).max().unwrap_or(0);
        Ok(completed_stages.remove(&last_stage_id).unwrap_or_default())
    }

    /// Run a set of pipelines to completion and return collected results.
    /// Pipelines must already have globally unique IDs.
    async fn run_pipelines(
        pipelines: Vec<Pipeline>,
        _catalog: &Arc<AgoraCatalog>,
        scheduler: Arc<TaskScheduler>,
    ) -> Result<Vec<DataChunk>, ExecutionError> {
        if pipelines.is_empty() {
            return Ok(Vec::new());
        }

        // Register pipeline states and find the "result" pipeline (last one with CollectSink)
        let mut result_sink: Option<Arc<std::sync::Mutex<Vec<DataChunk>>>> = None;

        for pipeline in &pipelines {
            scheduler.register_pipeline(pipeline.id, PipelineState::new());

            // Check if this pipeline has a CollectSink as its sink
            if let Some(collect) = pipeline.sink.as_any().downcast_ref::<CollectSink>() {
                result_sink = Some(collect.get_results_arc());
            }
        }

        // Start pipelines with no dependencies
        for pipeline in &pipelines {
            if pipeline.dependencies.is_empty() {
                Self::start_pipeline(pipeline, scheduler.clone());
            }
        }

        // Wait for all pipelines to complete
        loop {
            let all_complete = pipelines.iter().all(|p| scheduler.is_pipeline_completed(p.id));
            if all_complete {
                break;
            }

            // Check if any dependency-satisfied pipeline can start
            for pipeline in &pipelines {
                let state = scheduler.pipeline_states.lock().unwrap();
                let can_start = pipeline.dependencies.iter().all(|dep| {
                    state.get(dep).map(|s| s.is_completed()).unwrap_or(false)
                });
                drop(state);

                if can_start && scheduler.start_pipeline(pipeline.id) {
                    Self::start_pipeline(pipeline, scheduler.clone());
                }
            }

            // Yield to tokio runtime so background tasks (e.g. TableScanSource I/O)
            // can make progress. Using tokio::time::sleep instead of thread::sleep
            // is critical when running on single-thread tokio runtimes.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // Extract results
        if let Some(sink) = result_sink {
            let results = sink.lock().unwrap();
            Ok(results.iter().map(|c| c.deep_clone()).collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn start_pipeline(
        pipeline: &crate::pipeline::Pipeline,
        scheduler: Arc<TaskScheduler>,
    ) {
        // Ensure pipeline state transitions to RUNNING before submitting tasks.
        // This is required so that complete_pipeline() can later CAS to COMPLETED.
        let _ = scheduler.start_pipeline(pipeline.id);
        let parallelism = pipeline.parallelism;
        // Register the expected number of tasks so scheduler can detect completion
        scheduler.register_pipeline_tasks(pipeline.id, parallelism);
        for task_id in 0..parallelism {
            let task = pipeline.create_task_with_scheduler(task_id, scheduler.clone());
            scheduler.submit_task(task);
        }
    }

    // ------------------------------------------------------------------
    // Topological sort (kept for dependency validation)
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

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::SpaceUri;

    fn make_plan(stages: Vec<Stage>) -> ExecutionPlan {
        ExecutionPlan { stages }
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
            plan: agoradb_core::StagePlan::Scan {
                space: SpaceUri::parse("space://did:agora:test/t").unwrap(),
                projection: None,
                filter: None,
            },
            output: None,
        }]);
        let order = Executor::topo_sort(&plan.stages).unwrap();
        assert_eq!(order, vec![0]);
    }

    #[test]
    fn test_topo_sort_cycle_detection() {
        let plan = make_plan(vec![
            Stage {
                id: 0,
                label: "a".to_string(),
                dependencies: vec![1],
                parallelism: 1,
                plan: agoradb_core::StagePlan::ExchangeSource,
                output: None,
            },
            Stage {
                id: 1,
                label: "b".to_string(),
                dependencies: vec![0],
                parallelism: 1,
                plan: agoradb_core::StagePlan::ExchangeSource,
                output: None,
            },
        ]);
        let result = Executor::topo_sort(&plan.stages);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cycle"));
    }
}
