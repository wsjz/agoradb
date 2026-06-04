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

use crate::pipeline::{PipelineState, PipelineTask};
use crate::worker_pool::WorkerPool;
use agoradb_core::PipelineId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ------------------------------------------------------------------
// ActiveTaskTracker
// ------------------------------------------------------------------

/// Tracks how many tasks are still running for each pipeline.
/// When the count reaches zero, the pipeline is marked completed.
pub struct ActiveTaskTracker {
    counts: Mutex<HashMap<PipelineId, AtomicUsize>>,
}

impl ActiveTaskTracker {
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
        }
    }

    /// Register that `count` tasks will be submitted for this pipeline.
    pub fn register(&self, pipeline_id: PipelineId, count: usize) {
        let mut map = self.counts.lock().unwrap();
        map.insert(pipeline_id, AtomicUsize::new(count));
    }

    /// Atomically decrement the count for a pipeline.
    /// Returns true if the count reached zero (caller should mark pipeline completed).
    pub fn finish_one(&self, pipeline_id: PipelineId) -> bool {
        let map = self.counts.lock().unwrap();
        if let Some(counter) = map.get(&pipeline_id) {
            let remaining = counter.fetch_sub(1, Ordering::SeqCst);
            remaining == 1 // was 1 before decrement, now 0
        } else {
            false
        }
    }
}


// ------------------------------------------------------------------
// TaskWaker — per-task precise wake (StarRocks/Doris style)
// ------------------------------------------------------------------

/// A waker that wakes exactly one (pipeline_id, task_id) pair.
/// Prevents duplicate wakeups via an atomic flag.
#[derive(Clone)]
pub struct TaskWaker {
    scheduler: Arc<TaskScheduler>,
    pipeline_id: PipelineId,
    task_id: usize,
}

impl TaskWaker {
    pub fn new(scheduler: Arc<TaskScheduler>, pipeline_id: PipelineId, task_id: usize) -> Self {
        Self {
            scheduler,
            pipeline_id,
            task_id,
        }
    }

    /// Wake the task. If it is already running or not in the blocked
    /// registry, wake_task() is a no-op, so duplicate calls are safe.
    pub fn wake(&self) {
        self.scheduler.wake_task(self.pipeline_id, self.task_id);
    }
}

// ------------------------------------------------------------------
// SchedulerMetrics
// ------------------------------------------------------------------

/// Real-time scheduler statistics (all atomic, lock-free reads).
#[derive(Default)]
pub struct SchedulerMetrics {
    pub ready_tasks: AtomicUsize,
    pub running_tasks: AtomicUsize,
    pub blocked_tasks: AtomicUsize,
    pub finished_tasks: AtomicUsize,
}

// ------------------------------------------------------------------
// TaskScheduler
// ------------------------------------------------------------------

/// Query-level task scheduler that manages pipeline state and coordinates
/// with the global `WorkerPool` for task execution.
///
/// One `TaskScheduler` instance is created per query. It tracks:
/// - Pipeline lifecycle (NotStarted → Running → Completed)
/// - Active task counts per pipeline
/// - Blocked tasks waiting for Source data
/// - Completion events for event-driven DAG scheduling
pub struct TaskScheduler {
    /// Pipeline states — tracks NotStarted/Running/Completed.
    pub pipeline_states: Mutex<HashMap<PipelineId, PipelineState>>,
    /// Per-pipeline active task tracker.
    pub active_tasks: ActiveTaskTracker,
    /// Global metrics.
    pub metrics: SchedulerMetrics,
    /// Blocked tasks waiting for Source data (event-driven wake).
    blocked_registry: Mutex<HashMap<(PipelineId, usize), PipelineTask>>,
    /// Reference to the global worker pool for task submission.
    worker_pool: Arc<WorkerPool>,
    /// Completion event sender — notified when a pipeline finishes.
    /// Used by `run_pipelines()` for event-driven downstream scheduling.
    completion_tx: Option<tokio::sync::mpsc::Sender<PipelineId>>,
}

impl TaskScheduler {
    pub fn new(
        worker_pool: Arc<WorkerPool>,
        completion_tx: Option<tokio::sync::mpsc::Sender<PipelineId>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pipeline_states: Mutex::new(HashMap::new()),
            active_tasks: ActiveTaskTracker::new(),
            metrics: SchedulerMetrics::default(),
            blocked_registry: Mutex::new(HashMap::new()),
            worker_pool,
            completion_tx,
        })
    }

    /// Submit a task to the worker pool.
    pub fn submit_task(&self, task: PipelineTask) {
        self.metrics.ready_tasks.fetch_add(1, Ordering::Relaxed);
        self.worker_pool.submit(task);
    }

    /// Submit multiple tasks.
    pub fn submit_tasks(&self, tasks: Vec<PipelineTask>) {
        for task in tasks {
            self.submit_task(task);
        }
    }

    /// Register a pipeline's state.
    pub fn register_pipeline(&self, pipeline_id: PipelineId, state: PipelineState) {
        self.pipeline_states.lock().unwrap().insert(pipeline_id, state);
    }

    /// Register the expected number of tasks for a pipeline.
    /// Call this before submitting any tasks for the pipeline.
    pub fn register_pipeline_tasks(&self, pipeline_id: PipelineId, count: usize) {
        self.active_tasks.register(pipeline_id, count);
    }

    /// Atomically start a pipeline (CAS NotStarted → Running).
    /// Returns true if this caller is responsible for creating tasks.
    pub fn start_pipeline(&self, pipeline_id: PipelineId) -> bool {
        let states = self.pipeline_states.lock().unwrap();
        if let Some(state) = states.get(&pipeline_id) {
            state.start()
        } else {
            false
        }
    }

    /// Mark a pipeline as completed and notify the completion channel
    /// so that event-driven DAG scheduling can start downstream pipelines.
    pub fn complete_pipeline(&self, pipeline_id: PipelineId) {
        {
            let states = self.pipeline_states.lock().unwrap();
            if let Some(state) = states.get(&pipeline_id) {
                state.complete();
            }
        }
        // Notify event-driven scheduler
        if let Some(ref tx) = self.completion_tx {
            let _ = tx.blocking_send(pipeline_id);
        }
    }

    /// Check if a pipeline is completed.
    pub fn is_pipeline_completed(&self, pipeline_id: PipelineId) -> bool {
        let states = self.pipeline_states.lock().unwrap();
        states.get(&pipeline_id).map(|s| s.is_completed()).unwrap_or(false)
    }

    /// Register a task that yielded because its Source returned `NotReady`.
    pub fn register_blocked_task(&self, task: PipelineTask) {
        let key = (task.pipeline_id, task.task_id);
        self.blocked_registry.lock().unwrap().insert(key, task);
        self.metrics.blocked_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// Wake exactly one blocked task by (pipeline_id, task_id).
    pub fn wake_task(&self, pipeline_id: PipelineId, task_id: usize) {
        let key = (pipeline_id, task_id);
        let task = self.blocked_registry.lock().unwrap().remove(&key);
        if let Some(task) = task {
            self.metrics.blocked_tasks.fetch_sub(1, Ordering::Relaxed);
            self.submit_task(task);
        }
    }

    /// Wake all remaining blocked tasks.
    /// Used as a fallback when a pipeline completes or for bulk wakeup.
    pub fn wake_all_blocked_tasks(&self) {
        let tasks: Vec<PipelineTask> = {
            let mut blocked = self.blocked_registry.lock().unwrap();
            if blocked.is_empty() {
                return;
            }
            blocked.drain().map(|(_, task)| task).collect()
        };
        let count = tasks.len();
        for task in tasks {
            self.submit_task(task);
        }
        self.metrics.blocked_tasks.fetch_sub(count, Ordering::Relaxed);
    }
}
