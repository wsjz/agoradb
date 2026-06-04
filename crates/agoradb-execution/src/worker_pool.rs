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

use crate::pipeline::{PipelineTask, TaskStatus};
use agoradb_core::PipelineId;
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::thread;

// ------------------------------------------------------------------
// WorkerPool
// ------------------------------------------------------------------

/// A pool of long-running worker threads that execute `PipelineTask`s.
///
/// Workers are created when the pool is constructed and live until the
/// pool is dropped. Tasks are submitted via `submit()` and distributed
/// through a global injector queue with work-stealing fallback.
///
/// The pool is intentionally **decoupled** from query-level state:
/// it does not know about `TaskScheduler`, `PipelineState`, etc.
/// Completion notification is handled by the task itself via the
/// `task.scheduler` field.
pub struct WorkerPool {
    inner: Arc<WorkerPoolInner>,
}

struct WorkerPoolInner {
    num_workers: usize,
    global_queue: Injector<PipelineTask>,
    stealers: Vec<Stealer<PipelineTask>>,
    condvar: Condvar,
    has_work: Mutex<bool>,
    shutdown: AtomicUsize,
    handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl WorkerPool {
    /// Create a new worker pool with `num_workers` threads.
    /// Threads are spawned immediately and wait on a condvar until
    /// work arrives.
    pub fn new(num_workers: usize) -> Arc<Self> {
        let global_queue = Injector::new();
        let mut stealers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let worker = DequeWorker::new_fifo();
            stealers.push(worker.stealer());
        }

        let inner = Arc::new(WorkerPoolInner {
            num_workers,
            global_queue,
            stealers,
            condvar: Condvar::new(),
            has_work: Mutex::new(false),
            shutdown: AtomicUsize::new(0),
            handles: Mutex::new(Vec::new()),
        });

        // Spawn worker threads
        let mut handles = inner.handles.lock().unwrap();
        for worker_id in 0..num_workers {
            let pool_inner = inner.clone();
            let handle = thread::spawn(move || {
                Self::worker_loop(worker_id, pool_inner);
            });
            handles.push(handle);
        }
        drop(handles);

        Arc::new(Self { inner })
    }

    /// Submit a task to the global queue and wake one worker.
    pub fn submit(&self, task: PipelineTask) {
        self.inner.global_queue.push(task);
        *self.inner.has_work.lock().unwrap() = true;
        self.inner.condvar.notify_one();
    }

    /// Signal all workers to exit on next loop iteration.
    fn shutdown(&self) {
        self.inner.shutdown.store(1, Ordering::SeqCst);
        *self.inner.has_work.lock().unwrap() = true;
        self.inner.condvar.notify_all();
    }

    /// Worker thread main loop.
    fn worker_loop(worker_id: usize, pool: Arc<WorkerPoolInner>) {
        let local_queue = DequeWorker::<PipelineTask>::new_fifo();

        loop {
            if pool.shutdown.load(Ordering::SeqCst) != 0 {
                break;
            }

            // 1. Try local queue (LIFO for cache locality)
            let task = if let Some(task) = local_queue.pop() {
                task
            } else {
                // 2. Try global queue (FIFO)
                if let Some(task) = pool.global_queue.steal().success() {
                    task
                } else {
                    // 3. Steal from other workers
                    if let Some(task) = Self::steal_from_others(&pool.stealers, worker_id) {
                        task
                    } else {
                        // 4. Nothing to do — wait on condvar
                        let mut has_work = pool.has_work.lock().unwrap();
                        while !*has_work && pool.shutdown.load(Ordering::SeqCst) == 0 {
                            has_work = pool.condvar.wait(has_work).unwrap();
                        }
                        *has_work = false;
                        continue;
                    }
                }
            };

            let pipeline_id = task.pipeline_id;
            let scheduler = task.scheduler.clone();

            // Execute task with panic safety
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || task.run()));

            match result {
                Ok(TaskStatus::Finished) => {
                    if let Some(ref sched) = scheduler {
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                }
                Ok(TaskStatus::Yielded(task)) => {
                    if let Some(ref sched) = scheduler {
                        sched.register_blocked_task(task);
                    }
                }
                Ok(TaskStatus::Error(_)) => {
                    if let Some(ref sched) = scheduler {
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                }
                Err(_) => {
                    eprintln!("PipelineTask panicked in pipeline {}", pipeline_id);
                    if let Some(ref sched) = scheduler {
                        if sched.active_tasks.finish_one(pipeline_id) {
                            sched.complete_pipeline(pipeline_id);
                        }
                    }
                }
            }
        }
    }

    fn steal_from_others(stealers: &[Stealer<PipelineTask>], my_id: usize) -> Option<PipelineTask> {
        let n = stealers.len();
        for i in 0..n {
            let idx = (my_id + 1 + i) % n;
            if idx == my_id {
                continue;
            }
            if let Some(task) = stealers[idx].steal().success() {
                return Some(task);
            }
        }
        None
    }
}

// ------------------------------------------------------------------
// Global shared worker pools
// ------------------------------------------------------------------

/// Global cache of `WorkerPool`s keyed by `num_workers`.
/// Pools are created lazily on first access and live for the program
/// lifetime. This ensures that concurrent queries share the same
/// worker threads instead of spawning N × num_workers threads.
static GLOBAL_POOLS: LazyLock<Mutex<HashMap<usize, Arc<WorkerPool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a `WorkerPool` with the given number of workers.
/// Multiple `Executor` instances (from concurrent queries) that request
/// the same `num_workers` will share the same underlying pool.
pub fn get_or_create_pool(num_workers: usize) -> Arc<WorkerPool> {
    let mut pools = GLOBAL_POOLS.lock().unwrap();
    pools
        .entry(num_workers)
        .or_insert_with(|| WorkerPool::new(num_workers))
        .clone()
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
        let handles = std::mem::take(&mut *self.inner.handles.lock().unwrap());
        for handle in handles {
            let _ = handle.join();
        }
    }
}
