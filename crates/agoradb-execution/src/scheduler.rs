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
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

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
    /// Per-pipeline active task tracker.
    pub active_tasks: ActiveTaskTracker,
    /// Global metrics.
    pub metrics: SchedulerMetrics,
    /// Blocked tasks waiting for Source data (event-driven wake).
    blocked_registry: Mutex<Vec<PipelineTask>>,
    /// Condvar for event-driven worker wake-up (broadcast).
    condvar: Condvar,
    /// Flag paired with condvar — set to true when work is available.
    has_work: Mutex<bool>,
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
            active_tasks: ActiveTaskTracker::new(),
            metrics: SchedulerMetrics::default(),
            blocked_registry: Mutex::new(Vec::new()),
            condvar: Condvar::new(),
            has_work: Mutex::new(false),
            shutdown: AtomicUsize::new(0),
        })
    }

    /// Submit a task to the global queue and wake one worker.
    pub fn submit_task(&self, task: PipelineTask) {
        self.metrics.ready_tasks.fetch_add(1, Ordering::Relaxed);
        self.global_queue.push(task);
        *self.has_work.lock().unwrap() = true;
        self.condvar.notify_one();
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

    /// Signal shutdown and wake all workers so they can exit.
    pub fn shutdown(&self) {
        self.shutdown.store(1, Ordering::SeqCst);
        *self.has_work.lock().unwrap() = true;
        self.condvar.notify_all();
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst) != 0
    }

    /// Register a task that yielded because its Source returned `NotReady`.
    pub fn register_blocked_task(&self, task: PipelineTask) {
        self.blocked_registry.lock().unwrap().push(task);
        self.metrics.blocked_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// Wake all blocked tasks by re-submitting them to the global queue,
    /// then unpark one worker thread so it can pick up the new work.
    /// Called by Sources when new data arrives.
    pub fn wake_blocked_tasks(&self) {
        let tasks: Vec<PipelineTask> = {
            let mut blocked = self.blocked_registry.lock().unwrap();
            if blocked.is_empty() {
                return;
            }
            std::mem::take(&mut *blocked)
        };
        let count = tasks.len();
        for task in tasks {
            self.submit_task(task);
        }
        self.metrics.blocked_tasks.fetch_sub(count, Ordering::Relaxed);
        // Broadcast wake to ALL workers — Condvar::notify_all correctly wakes
        // every waiting thread, unlike thread::unpark which races.
        *self.has_work.lock().unwrap() = true;
        self.condvar.notify_all();
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
                            // 4. Nothing to do — wait on condvar until woken by
                            //    submit_task or wake_blocked_tasks().
                            let mut has_work = sched.has_work.lock().unwrap();
                            while !*has_work && !sched.is_shutdown() {
                                has_work = sched.condvar.wait(has_work).unwrap();
                            }
                            *has_work = false;
                            continue;
                        }
                    }
                };

                // Execute task
                sched.metrics.ready_tasks.fetch_sub(1, Ordering::Relaxed);
                sched.metrics.running_tasks.fetch_add(1, Ordering::Relaxed);

                let pipeline_id = task.pipeline_id;
                // Catch panics so a single failing task doesn't crash the worker thread.
                // This prevents deadlocks where a crashed worker leaves pipelines
                // permanently uncompleted.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || task.run()));
                match result {
                    Ok(TaskStatus::Finished) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        sched.metrics.finished_tasks.fetch_add(1, Ordering::Relaxed);
                        // Track per-pipeline completion
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                    Ok(TaskStatus::Yielded(task)) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        // Register in the global blocked registry.
                        // The Source will wake this task via wake_blocked_tasks()
                        // when new data arrives.
                        sched.register_blocked_task(task);
                    }
                    Ok(TaskStatus::Error(e)) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        eprintln!("PipelineTask error: {:?}", e);
                        // Even on error, track completion so executor doesn't hang
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                    Err(_) => {
                        sched.metrics.running_tasks.fetch_sub(1, Ordering::Relaxed);
                        eprintln!("PipelineTask panicked in pipeline {}", pipeline_id);
                        // Mark task as finished so pipeline can complete and executor
                        // doesn't hang forever waiting for a dead task.
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    handles
}
