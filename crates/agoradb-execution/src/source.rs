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
}

// ------------------------------------------------------------------
// TableScanSource
// ------------------------------------------------------------------

/// Reads morsels from storage. A background tokio task loads
/// data into a channel; `try_next` pulls from that channel.
pub struct TableScanSource {
    rx: mpsc::Receiver<DataChunk>,
    #[allow(dead_code)]
    total_morsels: usize,
}

impl TableScanSource {
    pub fn new(catalog: Arc<AgoraCatalog>, _space: SpaceUri, _snapshot_id: i64, morsels: Vec<Morsel>) -> Self {
        let total_morsels = morsels.len();
        let (tx, rx) = mpsc::channel::<DataChunk>();

        tokio::spawn(async move {
            for morsel in morsels {
                match catalog.read_morsel(&morsel).await {
                    Ok(batches) => {
                        for batch in batches {
                            match DataChunk::from_record_batch(&batch) {
                                Ok(chunk) => {
                                    if tx.send(chunk).is_err() {
                                        return;
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Self { rx, total_morsels }
    }
}

impl Source for TableScanSource {
    fn try_next(&mut self) -> Result<SourceResult, ExecutionError> {
        match self.rx.try_recv() {
            Ok(chunk) => Ok(SourceResult::Ready(chunk)),
            Err(mpsc::TryRecvError::Empty) => Ok(SourceResult::NotReady),
            Err(mpsc::TryRecvError::Disconnected) => Ok(SourceResult::Done),
        }
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
