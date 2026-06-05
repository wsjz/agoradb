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
use crate::local_exchange::LocalExchangeSource;
use crate::morsel_scheduler::MorselScheduler;
use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::ExecutionError;
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};

// ------------------------------------------------------------------
// Global I/O Runtime
// ------------------------------------------------------------------

/// Shared multi-threaded tokio runtime for all storage I/O.
///
/// Instead of spawning a new thread (or a new runtime) per
/// `TableScanSource`, every source submits its async read work to this
/// single runtime.  This caps the total I/O threads at a fixed number
/// (4) regardless of how many concurrent queries or sources exist.
static IO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_io_runtime() -> &'static tokio::runtime::Runtime {
    IO_RUNTIME.get_or_init(|| {
        let num_io_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_io_threads)
            .thread_name("agoradb-io")
            .build()
            .expect("Failed to create global I/O runtime")
    })
}

/// Result of a non-blocking pull from a Source.
pub enum SourceResult {
    /// Data chunk is ready.
    Ready(DataChunk),
    /// End of stream.
    Done,
    /// No data available now — caller should yield.
    NotReady,
}

/// A non-blocking data source for PipelineTask.
pub trait Source: Send {
    /// Attempt to get the next chunk without blocking.
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError>;
    /// Optional: inject scheduler reference for event-driven wake.
    /// Called once when the PipelineTask is created.
    fn set_scheduler(&mut self, _scheduler: Arc<crate::scheduler::TaskScheduler>) {}
    /// Optional: inject a per-task waker for precise wakeups.
    /// Called once when the PipelineTask is created.
    fn set_waker(&mut self, _waker: crate::scheduler::TaskWaker) {}
}

// ------------------------------------------------------------------
// TableScanSource
// ------------------------------------------------------------------

/// Reads morsels from storage using a central `MorselScheduler`.
///
/// All tasks for the same table share one `MorselScheduler`, which
/// dynamically assigns morsels via an atomic counter.  This means:
/// - Fast workers process more morsels; slow workers process fewer.
/// - No task is stuck with a large morsel while others idle.
/// - If a task panics or hangs, its unprocessed morsels are picked up
///   by the remaining tasks.
///
/// A background tokio task loads data into a channel; `try_next`
/// pulls from that channel.  The background task is started lazily
/// on first `try_next` so that `set_scheduler` has already been called.
pub struct TableScanSource {
    rx: mpsc::Receiver<DataChunk>,
    tx: Option<mpsc::SyncSender<DataChunk>>,
    #[allow(dead_code)]
    total_morsels: usize,
    /// Scheduler handle set by `create_task_with_scheduler`.
    scheduler: Option<std::sync::Arc<crate::scheduler::TaskScheduler>>,
    /// Per-task waker for precise wakeups (StarRocks/Doris style).
    waker: Option<crate::scheduler::TaskWaker>,
    /// Deferred start state — moved into the background task on first `try_next`.
    catalog: Option<Arc<AgoraCatalog>>,
    /// Central morsel scheduler shared across all tasks of the same scan.
    morsel_scheduler: Option<Arc<MorselScheduler>>,
}

impl TableScanSource {
    pub fn new_with_scheduler(
        catalog: Arc<AgoraCatalog>,
        morsel_scheduler: Arc<MorselScheduler>,
    ) -> Self {
        let total_morsels = morsel_scheduler.total();
        let (tx, rx) = mpsc::sync_channel::<DataChunk>(4);
        Self {
            rx,
            tx: Some(tx),
            total_morsels,
            scheduler: None,
            waker: None,
            catalog: Some(catalog),
            morsel_scheduler: Some(morsel_scheduler),
        }
    }

    /// Start the background I/O task lazily.
    /// Called from `try_next` so that `set_scheduler` has already run.
    fn ensure_started(&mut self) {
        if self.morsel_scheduler.is_none() {
            return; // Already started
        }
        let catalog = self.catalog.take().unwrap();
        let morsel_scheduler = self.morsel_scheduler.take().unwrap();
        let tx = self.tx.take().unwrap();
        let scheduler = self.scheduler.clone();
        let waker = self.waker.clone();

        // All I/O is dispatched to the shared global runtime.
        get_io_runtime().spawn(async move {
            Self::read_morsels(catalog, morsel_scheduler, tx, scheduler, waker).await;
        });
    }

    async fn read_morsels(
        catalog: Arc<AgoraCatalog>,
        morsel_scheduler: Arc<MorselScheduler>,
        tx: mpsc::SyncSender<DataChunk>,
        scheduler: Option<std::sync::Arc<crate::scheduler::TaskScheduler>>,
        waker: Option<crate::scheduler::TaskWaker>,
    ) {
        let mut morsel_count = 0;
        let mut batch_count = 0;
        let mut chunk_count = 0;
        // Dynamic morsel allocation: all tasks for this table compete
        // for the next morsel via a central atomic counter.
        while let Some(morsel) = morsel_scheduler.next() {
            morsel_count += 1;
            match catalog.read_morsel(&morsel).await {
                Ok(batches) => {
                    for batch in batches {
                        batch_count += 1;
                        match DataChunk::from_record_batch(&batch) {
                            Ok(chunk) => {
                                chunk_count += 1;
                                if tx.send(chunk).is_err() {
                                    eprintln!(
                                        "[DEBUG-IO] rx dropped after {} morsels, {} batches, {} chunks",
                                        morsel_count, batch_count, chunk_count
                                    );
                                    return;
                                }
                                // Data arrived — wake this task precisely.
                                if let Some(ref w) = waker {
                                    w.wake();
                                }
                            }
                            Err(e) => {
                                eprintln!("[DEBUG-IO] from_record_batch failed: {:?}", e);
                                continue;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[DEBUG-IO] read_morsel failed: {:?}", e);
                    break;
                }
            }
        }
        eprintln!(
            "[DEBUG-IO] read_morsels done: {} morsels, {} batches, {} chunks",
            morsel_count, batch_count, chunk_count
        );
        // After all data sent, wake this task precisely...
        if let Some(ref w) = waker {
            w.wake();
        }
        // ...and broadcast-wake all blocked tasks as a safety net.
        // This prevents lost tasks when wake() races with register_blocked_task().
        if let Some(ref sched) = scheduler {
            sched.wake_all_blocked_tasks();
        }
    }
}

impl Source for TableScanSource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        self.ensure_started();
        match self.rx.try_recv() {
            Ok(chunk) => Ok(SourceResult::Ready(chunk)),
            Err(mpsc::TryRecvError::Empty) => Ok(SourceResult::NotReady),
            Err(mpsc::TryRecvError::Disconnected) => Ok(SourceResult::Done),
        }
    }

    fn set_scheduler(&mut self, scheduler: std::sync::Arc<crate::scheduler::TaskScheduler>) {
        self.scheduler = Some(scheduler);
    }

    fn set_waker(&mut self, waker: crate::scheduler::TaskWaker) {
        self.waker = Some(waker);
    }
}

// ------------------------------------------------------------------
// ExchangeSource
// ------------------------------------------------------------------

/// Reads from a LocalExchangeBuffer partition.
pub struct ExchangeSource {
    inner: LocalExchangeSource,
    finished: bool,
}

impl ExchangeSource {
    pub fn new(inner: LocalExchangeSource) -> Self {
        Self {
            inner,
            finished: false,
        }
    }
}

impl Source for ExchangeSource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        if self.finished {
            return Ok(SourceResult::Done);
        }
        match self.inner.pull() {
            Some(chunk) => Ok(SourceResult::Ready(chunk)),
            None => {
                if self.inner.is_finished() {
                    self.finished = true;
                    Ok(SourceResult::Done)
                } else {
                    Ok(SourceResult::NotReady)
                }
            }
        }
    }
}

// ------------------------------------------------------------------
// EmptySource
// ------------------------------------------------------------------

/// A source that immediately returns Done.
pub struct EmptySource;

impl Source for EmptySource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        Ok(SourceResult::Done)
    }
}

// ------------------------------------------------------------------
// EmitSource
// ------------------------------------------------------------------

/// A source that returns one empty `DataChunk` then `Done`.
/// Used for emit pipelines (e.g. `SortEmit`, `HashAggregateEmit`) so that
/// the operator gets a chance to run even though there is no upstream data.
pub struct EmitSource {
    emitted: bool,
}

impl EmitSource {
    pub fn new() -> Self {
        Self { emitted: false }
    }
}

impl Source for EmitSource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        if !self.emitted {
            self.emitted = true;
            Ok(SourceResult::Ready(DataChunk::new(vec![])))
        } else {
            Ok(SourceResult::Done)
        }
    }
}

// ------------------------------------------------------------------
// InMemorySource
// ------------------------------------------------------------------

/// A source that serves from a pre-loaded Vec<DataChunk>.
pub struct InMemorySource {
    chunks: Vec<DataChunk>,
    idx: usize,
}

impl InMemorySource {
    pub fn new(chunks: Vec<DataChunk>) -> Self {
        Self { chunks, idx: 0 }
    }
}

impl Source for InMemorySource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        if self.idx < self.chunks.len() {
            let chunk = self.chunks[self.idx].deep_clone();
            self.idx += 1;
            Ok(SourceResult::Ready(chunk))
        } else {
            Ok(SourceResult::Done)
        }
    }
}

// ------------------------------------------------------------------
// SingleChunkSource
// ------------------------------------------------------------------

/// A source that returns exactly one chunk then Done.
pub struct SingleChunkSource {
    chunk: Option<DataChunk>,
}

impl SingleChunkSource {
    pub fn new(chunk: DataChunk) -> Self {
        Self { chunk: Some(chunk) }
    }
}

impl Source for SingleChunkSource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        match self.chunk.take() {
            Some(chunk) => Ok(SourceResult::Ready(chunk)),
            None => Ok(SourceResult::Done),
        }
    }
}
