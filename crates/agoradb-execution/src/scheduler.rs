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

use crate::pipeline::{PipelineState, PipelineTask, TaskStatus};
use agoradb_core::PipelineId;
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

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

/// Global task scheduler with work-stealing.
pub struct TaskScheduler {
    /// Global injector queue — any thread can push.
    pub global_queue: Injector<PipelineTask>,
    /// Stealers for each worker.
    pub stealers: Vec<Stealer<PipelineTask>>,
    /// Pipeline states — tracks NotStarted/Running/Completed.
    pub pipeline_states: Mutex<HashMap<PipelineId, PipelineState>>,
    /// Global metrics.
    pub metrics: SchedulerMetrics,
    /// Shutdown flag.
    shutdown: AtomicUsize,
}

impl TaskScheduler {
    pub fn new(num_workers: usize) -> Arc<Self> {
        let global_queue = Injector::new();
        let mut stealers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let worker = DequeWorker::new_fifo();
            stealers.push(worker.stealer());
        }

        Arc::new(Self {
            global_queue,
            stealers,
            pipeline_states: Mutex::new(HashMap::new()),
            metrics: SchedulerMetrics::default(),
            shutdown: AtomicUsize::new(0),
        })
    }

    /// Submit a task to the global queue.
    pub fn submit_task(&self, task: PipelineTask) {
        self.metrics.ready_tasks.fetch_add(1, Ordering::Relaxed);
        self.global_queue.push(task);
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

    /// Mark a pipeline as completed.
    pub fn complete_pipeline(&self, pipeline_id: PipelineId) {
        let states = self.pipeline_states.lock().unwrap();
        if let Some(state) = states.get(&pipeline_id) {
            state.complete();
        }
    }

    /// Check if a pipeline is completed.
    pub fn is_pipeline_completed(&self, pipeline_id: PipelineId) -> bool {
        let states = self.pipeline_states.lock().unwrap();
        states.get(&pipeline_id).map(|s| s.is_completed()).unwrap_or(false)
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::SeqCst);
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst) != 0
    }

    /// Steal a task from another worker.
    pub fn steal_from_others(&self, my_id: usize) -> Option<PipelineTask> {
        let n = self.stealers.len();
        for i in 0..n {
            let idx = (my_id + 1 + i) % n;
            if idx == my_id {
                continue;
            }
            if let Some(task) = self.stealers[idx].steal().success() {
                return Some(task);
            }
        }
        None
    }
}

// ------------------------------------------------------------------
// Worker thread
// ------------------------------------------------------------------

/// Spawn `num_workers` worker threads. Returns a vec of thread handles.
pub fn spawn_workers(
    scheduler: Arc<TaskScheduler>,
    num_workers: usize,
) -> Vec<thread::JoinHandle<()>> {
    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let sched = scheduler.clone();
        let handle = thread::spawn(move || {
            let local_queue = DequeWorker::<PipelineTask>::new_fifo();

            loop {
                if sched.is_shutdown() {
                    break;
                }

                // 1. Try local queue (LIFO for cache locality)
                let task = if let Some(task) = local_queue.pop() {
                    task
                } else {
                    // 2. Try global queue (FIFO)
                    if let Some(task) = sched.global_queue.steal().success() {
                        task
                    } else {
                        // 3. Steal from other workers
                        if let Some(task) = sched.steal_from_others(worker_id) {
                            task
                        } else {
                            // 4. Nothing to do — yield briefly
                            thread::yield_now();
                            continue;
                        }
                    }
                };

                // Execute task
                sched.metrics.ready_tasks.fetch_sub(1, Ordering::Relaxed);
                sched.metrics.running_tasks.fetch_add(1, Ordering::Relaxed);

                match task.run() {
                    TaskStatus::Finished => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        sched.metrics.finished_tasks.fetch_add(1, Ordering::Relaxed);
                    }
                    TaskStatus::Yielded(task) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        sched.metrics.blocked_tasks.fetch_add(1, Ordering::Relaxed);
                        // v1: re-submit immediately. v2: proper blocked registry.
                        sched.submit_task(task);
                        sched.metrics.blocked_tasks.fetch_sub(1, Ordering::Relaxed);
                    }
                    TaskStatus::Error(e) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        eprintln!("PipelineTask error: {:?}", e);
                    }
                }
            }
        });
        handles.push(handle);
    }

    handles
}
