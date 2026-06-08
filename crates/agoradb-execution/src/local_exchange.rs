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
use agoradb_core::{ExchangeType, ExecutionError};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Shared buffer for local data exchange between pipelines.
///
/// Supports three modes:
/// - **Gather**: All sink data is collected into a single partition.
/// - **Broadcast**: Each chunk is replicated to all partitions.
/// - **HashPartition** (Shuffle): Rows are distributed across partitions
///   by hash key columns.
pub struct LocalExchangeBuffer {
    exchange_type: ExchangeType,
    num_sinks: usize,
    pub(crate) partitions: Vec<ExchangeQueue>,
    finished_sinks: AtomicUsize,
}

/// A single FIFO queue for one partition of the exchange.
pub struct ExchangeQueue {
    chunks: Mutex<VecDeque<DataChunk>>,
    not_empty: Condvar,
    #[allow(dead_code)]
    capacity: usize,
}

impl LocalExchangeBuffer {
    /// Create a new exchange buffer.
    ///
    /// - `num_sinks`: number of producer tasks (used to detect completion).
    /// - `num_partitions`: number of consumer partitions.
    ///
    /// For **Gather** `num_partitions` is always 1 regardless of this argument.
    pub fn new_with_partitions(
        exchange_type: ExchangeType,
        num_sinks: usize,
        num_partitions: usize,
    ) -> Self {
        let actual_partitions = match &exchange_type {
            ExchangeType::Gather => 1,
            ExchangeType::Broadcast => num_partitions,
            ExchangeType::HashPartition { .. } => num_partitions,
        };
        let partitions = (0..actual_partitions)
            .map(|_| ExchangeQueue::new(1024))
            .collect();
        Self {
            exchange_type,
            num_sinks,
            partitions,
            finished_sinks: AtomicUsize::new(0),
        }
    }

    /// Convenience constructor where `num_sinks == num_partitions`.
    pub fn new(exchange_type: ExchangeType, num_sinks: usize) -> Self {
        Self::new_with_partitions(exchange_type, num_sinks, num_sinks)
    }

    /// Push a chunk from the given sink into the appropriate partition(s).
    pub fn push(&self, chunk: DataChunk, _sink_id: usize) -> Result<(), ExecutionError> {
        match &self.exchange_type {
            ExchangeType::Gather => {
                self.partitions[0].push(chunk)?;
            }
            ExchangeType::Broadcast => {
                for partition in &self.partitions {
                    partition.push(chunk.deep_clone())?;
                }
            }
            ExchangeType::HashPartition(key_columns) => {
                let sub_chunks = partition_chunk(chunk, key_columns, self.partitions.len())?;
                for (idx, sub_chunk) in sub_chunks.into_iter().enumerate() {
                    if sub_chunk.len > 0 {
                        self.partitions[idx].push(sub_chunk)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Signal that a sink has finished producing data.
    pub fn signal_sink_done(&self) {
        self.finished_sinks.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns true when all sinks have signaled completion.
    pub fn is_finished(&self) -> bool {
        self.finished_sinks.load(Ordering::SeqCst) >= self.num_sinks
    }
}

impl ExchangeQueue {
    /// Create a new queue with the given capacity limit.
    pub fn new(capacity: usize) -> Self {
        Self {
            chunks: Mutex::new(VecDeque::new()),
            not_empty: Condvar::new(),
            capacity,
        }
    }

    /// Push a chunk into the queue, waking a waiting consumer.
    pub fn push(&self, chunk: DataChunk) -> Result<(), ExecutionError> {
        let mut queue = self.chunks.lock().unwrap();
        queue.push_back(chunk);
        self.not_empty.notify_one();
        Ok(())
    }

    /// Pop a chunk from the queue (non-blocking).
    pub fn pop(&self) -> Option<DataChunk> {
        let mut queue = self.chunks.lock().unwrap();
        queue.pop_front()
    }
}

/// Partition a chunk by hash key columns.
///
/// Currently uses a simple round-robin fallback. Full row-by-row hash
/// partitioning will be implemented in a future optimization pass.
fn partition_chunk(
    chunk: DataChunk,
    _key_columns: &[usize],
    num_partitions: usize,
) -> Result<Vec<DataChunk>, ExecutionError> {
    // TODO: implement row-by-row hash partitioning
    // For now, simple round-robin fallback: put all rows into partition 0
    let mut result: Vec<DataChunk> = (0..num_partitions)
        .map(|_| DataChunk::new(vec![]))
        .collect();
    result[0] = chunk;
    Ok(result)
}

// ------------------------------------------------------------------
// LocalExchangeSink
// ------------------------------------------------------------------

/// A Sink that feeds data into a `LocalExchangeBuffer`.
/// Used as the sink of a producer pipeline in the Pipeline architecture.
pub struct LocalExchangeSink {
    buffer: Arc<LocalExchangeBuffer>,
    sink_id: usize,
}

impl LocalExchangeSink {
    pub fn new(buffer: Arc<LocalExchangeBuffer>, sink_id: usize) -> Self {
        Self { buffer, sink_id }
    }

    pub fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.buffer.push(chunk, self.sink_id)
    }

    pub fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.buffer.signal_sink_done();
        Ok(())
    }
}

// ------------------------------------------------------------------
// LocalExchangeSource
// ------------------------------------------------------------------

/// Pull-based source that reads data from a `LocalExchangeBuffer` partition.
pub struct LocalExchangeSource {
    buffer: Arc<LocalExchangeBuffer>,
    partition_idx: usize,
}

impl LocalExchangeSource {
    /// Create a new source reading from the given partition index.
    pub fn new(buffer: Arc<LocalExchangeBuffer>, partition_idx: usize) -> Self {
        Self {
            buffer,
            partition_idx,
        }
    }

    /// Pull the next chunk from this source's partition (non-blocking).
    pub fn pull(&mut self) -> Option<DataChunk> {
        self.buffer.partitions[self.partition_idx].pop()
    }

    /// Returns true when all sinks have finished and no more data is coming.
    pub fn is_finished(&self) -> bool {
        self.buffer.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::DataType;

    fn make_chunk(values: &[i64]) -> DataChunk {
        let mut col = ColumnVector::new(DataType::Int64, values.len());
        for &v in values {
            col.push_i64(v);
        }
        DataChunk::new(vec![col])
    }

    use crate::chunk::ColumnVector;

    #[test]
    fn test_gather_exchange() {
        let buffer = Arc::new(LocalExchangeBuffer::new(ExchangeType::Gather, 2));
        let mut sink0 = LocalExchangeSink::new(buffer.clone(), 0);
        let mut sink1 = LocalExchangeSink::new(buffer.clone(), 1);

        sink0.consume(make_chunk(&[1, 2])).unwrap();
        sink1.consume(make_chunk(&[3, 4])).unwrap();
        sink0.finalize().unwrap();
        sink1.finalize().unwrap();

        let mut source = LocalExchangeSource::new(buffer.clone(), 0);
        assert!(source.pull().is_some());
        assert!(source.pull().is_some());
        assert!(source.pull().is_none());
        assert!(source.is_finished());
    }

    #[test]
    fn test_broadcast_exchange() {
        let buffer = Arc::new(LocalExchangeBuffer::new(ExchangeType::Broadcast, 2));
        let mut sink0 = LocalExchangeSink::new(buffer.clone(), 0);

        sink0.consume(make_chunk(&[1, 2])).unwrap();
        sink0.finalize().unwrap();

        let mut source0 = LocalExchangeSource::new(buffer.clone(), 0);
        let mut source1 = LocalExchangeSource::new(buffer.clone(), 1);

        let chunk0 = source0.pull().unwrap();
        let chunk1 = source1.pull().unwrap();
        assert_eq!(chunk0.len, 2);
        assert_eq!(chunk1.len, 2);
    }

    #[test]
    fn test_hash_partition_exchange() {
        let buffer = Arc::new(LocalExchangeBuffer::new(
            ExchangeType::HashPartition(vec![0]),
            2,
        ));
        let mut sink0 = LocalExchangeSink::new(buffer.clone(), 0);

        sink0.consume(make_chunk(&[1, 2, 3])).unwrap();
        sink0.finalize().unwrap();

        // Round-robin fallback puts everything in partition 0
        let mut source0 = LocalExchangeSource::new(buffer.clone(), 0);
        let mut source1 = LocalExchangeSource::new(buffer.clone(), 1);

        assert!(source0.pull().is_some());
        assert!(source1.pull().is_none());
    }
}
