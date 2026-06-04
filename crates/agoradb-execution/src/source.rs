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
use agoradb_catalog::{AgoraCatalog, StorageScanProvider};
use agoradb_core::{ExecutionError, Morsel, SpaceUri};
use std::sync::mpsc;
use std::sync::Arc;

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
}

// ------------------------------------------------------------------
// TableScanSource
// ------------------------------------------------------------------

/// Reads morsels from storage. A background tokio task loads
/// data into a channel; `try_next` pulls from that channel.
/// Background task is started lazily on first `try_next` so that
/// `set_scheduler` has already been called and wake events work.
pub struct TableScanSource {
    rx: mpsc::Receiver<DataChunk>,
    tx: Option<mpsc::Sender<DataChunk>>,
    #[allow(dead_code)]
    total_morsels: usize,
    /// Scheduler handle set by `create_task_with_scheduler`.
    scheduler: Option<std::sync::Arc<crate::scheduler::TaskScheduler>>,
    /// Deferred start state — moved into the background task on first `try_next`.
    catalog: Option<Arc<AgoraCatalog>>,
    morsels: Option<Vec<Morsel>>,
}

impl TableScanSource {
    pub fn new(catalog: Arc<AgoraCatalog>, _space: SpaceUri, _snapshot_id: i64, morsels: Vec<Morsel>) -> Self {
        let total_morsels = morsels.len();
        let (tx, rx) = mpsc::channel::<DataChunk>();
        Self {
            rx,
            tx: Some(tx),
            total_morsels,
            scheduler: None,
            catalog: Some(catalog),
            morsels: Some(morsels),
        }
    }

    /// Start the background I/O task lazily.
    /// Called from `try_next` so that `set_scheduler` has already run.
    fn ensure_started(&mut self) {
        if self.morsels.is_none() {
            return; // Already started
        }
        let catalog = self.catalog.take().unwrap();
        let morsels = self.morsels.take().unwrap();
        let tx = self.tx.take().unwrap();
        let scheduler = self.scheduler.clone();

        // Worker threads are plain OS threads (not tokio runtime threads).
        // We cannot call tokio::spawn() here — it would panic with
        // "there is no reactor running".
        //
        // Strategy:
        //   - If we're inside a tokio runtime (e.g. test calling directly),
        //     use Handle::try_current() + spawn.
        //   - Otherwise (worker thread), spawn a new std::thread with its
        //     own tokio::runtime::Runtime and block_on the async I/O.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    Self::read_morsels(catalog, morsels, tx, scheduler).await;
                });
            }
            Err(_) => {
                std::thread::spawn(move || {
                    let rt = match tokio::runtime::Runtime::new() {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("TableScanSource: failed to create tokio runtime: {}", e);
                            return;
                        }
                    };
                    rt.block_on(Self::read_morsels(catalog, morsels, tx, scheduler));
                });
            }
        }
    }

    async fn read_morsels(
        catalog: Arc<AgoraCatalog>,
        morsels: Vec<Morsel>,
        tx: mpsc::Sender<DataChunk>,
        scheduler: Option<std::sync::Arc<crate::scheduler::TaskScheduler>>,
    ) {
        for morsel in morsels {
            match catalog.read_morsel(&morsel).await {
                Ok(batches) => {
                    for batch in batches {
                        match DataChunk::from_record_batch(&batch) {
                            Ok(chunk) => {
                                if tx.send(chunk).is_err() {
                                    return;
                                }
                                // Data arrived — wake blocked workers
                                // so they can retry their sources.
                                if let Some(ref sched) = scheduler {
                                    sched.wake_blocked_tasks();
                                }
                            }
                            Err(_) => continue,
                        }
                    }
                }
                Err(_) => break,
            }
        }
        // After all data sent, wake any remaining blocked tasks
        // so they see Done instead of waiting forever.
        if let Some(ref sched) = scheduler {
            sched.wake_blocked_tasks();
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

    fn set_scheduler(
        &mut self,
        scheduler: std::sync::Arc<crate::scheduler::TaskScheduler>,
    ) {
        self.scheduler = Some(scheduler);
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
        Self { inner, finished: false }
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
