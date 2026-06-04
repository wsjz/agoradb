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

use crate::chunk::{ColumnVector, DataChunk};
use agoradb_core::DataType;
use crate::operator::Operator;
use agoradb_core::{ExecutionError, JoinType};
use std::collections::HashMap;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use crate::pipeline::{CloneOperator, CloneSink, PipelineOperator, Sink};
use std::any::Any;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinKey {
    Int64(i64),
    Float64(u64), // bit-pattern for deterministic hashing
    Boolean(bool),
    Utf8(String),
}

#[derive(Clone, Copy)]
enum JoinState {
    Building,
    Probing,
    Done,
}

/// Global state shared across parallel HashJoin build workers.
///
/// Uses pre-allocated `AtomicPtr` slots so each worker can publish
/// its local hash table and chunks with **zero contention**.
/// The last worker to arrive triggers the merge into a single
/// global hash table.
pub struct HashJoinGlobalState {
    expected_tasks: AtomicUsize,
    completed_tasks: AtomicUsize,
    slots: OnceLock<Vec<AtomicPtr<(HashMap<JoinKey, Vec<usize>>, Vec<DataChunk>)>>>,
    global_data: OnceLock<Arc<(HashMap<JoinKey, Vec<usize>>, Vec<DataChunk>)>>,
}

// Safety: All mutable state is accessed through atomic operations
// or OnceLock (which provides its own synchronization). The only
// unsafe code is the Box::into_raw / Box::from_raw pair, where
// ownership is transferred atomically via AtomicPtr.
unsafe impl Sync for HashJoinGlobalState {}

impl HashJoinGlobalState {
    pub fn new() -> Self {
        Self {
            expected_tasks: AtomicUsize::new(0),
            completed_tasks: AtomicUsize::new(0),
            slots: OnceLock::new(),
            global_data: OnceLock::new(),
        }
    }

    /// Set the expected number of worker tasks and pre-allocate
    /// `AtomicPtr` slots — one per worker.
    pub fn set_expected_tasks(&self, n: usize) {
        self.expected_tasks.store(n, Ordering::SeqCst);
        let slots: Vec<_> = (0..n)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        let _ = self.slots.set(slots);
    }

    /// Register a worker's local hash table and chunks.
    ///
    /// Each worker gets a unique index via `fetch_add`, writes its
    /// data into the corresponding slot with `Release` ordering,
    /// and the last arrival triggers the merge.
    pub fn register_local_table(
        &self,
        table: HashMap<JoinKey, Vec<usize>>,
        chunks: Vec<DataChunk>,
    ) {
        let idx = self.completed_tasks.fetch_add(1, Ordering::SeqCst);
        let slots = self.slots.get().expect("expected_tasks not set");

        let ptr = Box::into_raw(Box::new((table, chunks)));
        slots[idx].store(ptr, Ordering::Release);

        let completed = idx + 1;
        let expected = self.expected_tasks.load(Ordering::SeqCst);

        if completed >= expected && self.global_data.get().is_none() {
            self.merge_all(slots, expected);
        }
    }

    /// Returns `true` if all workers have registered and the global
    /// data has been merged.
    pub fn is_ready(&self) -> bool {
        self.global_data.get().is_some()
    }

    /// Get the merged global hash table and chunks.
    pub fn get_global_data(&self) -> Option<Arc<(HashMap<JoinKey, Vec<usize>>, Vec<DataChunk>)>> {
        self.global_data.get().cloned()
    }

    /// Merge all worker-local slots into a single global hash table.
    ///
    /// Called exactly once by the last worker to arrive. Uses
    /// `swap(Acquire)` to take ownership of each slot's data,
    /// forming an acquire-release pair with the worker's `store`.
    fn merge_all(
        &self,
        slots: &[AtomicPtr<(HashMap<JoinKey, Vec<usize>>, Vec<DataChunk>)>],
        n: usize,
    ) {
        let mut global_table: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        let mut global_chunks: Vec<DataChunk> = Vec::new();

        for i in 0..n {
            let ptr = slots[i].swap(std::ptr::null_mut(), Ordering::Acquire);
            // ptr should never be null here — every slot was written
            // by its worker before any merge could begin.
            let (local_table, local_chunks) = unsafe { *Box::from_raw(ptr) };

            // Merge chunks first (needed for correct row offset mapping)
            let base_idx: usize = global_chunks.iter().map(|c| c.len).sum();
            global_chunks.extend(local_chunks);

            // Merge hash table with offset-adjusted indices
            for (key, mut indices) in local_table {
                for idx in &mut indices {
                    *idx += base_idx;
                }
                global_table
                    .entry(key)
                    .or_default()
                    .extend(indices);
            }
        }

        let _ = self.global_data.set(Arc::new((global_table, global_chunks)));
    }
}

// ------------------------------------------------------------------
// HashJoinBuildSink
// ------------------------------------------------------------------

/// Sink for the HashJoin build pipeline.
/// Each build task builds a local hash table from its input chunks.
/// On finalize, it registers the local table with the global state.
pub struct HashJoinBuildSink {
    join_id: usize,
    left_key: usize,
    local_table: HashMap<JoinKey, Vec<usize>>,
    local_chunks: Vec<DataChunk>,
    global_state: Arc<HashJoinGlobalState>,
}

impl HashJoinBuildSink {
    pub fn new(join_id: usize, left_key: usize, global_state: Arc<HashJoinGlobalState>) -> Self {
        Self {
            join_id,
            left_key,
            local_table: HashMap::new(),
            local_chunks: Vec::new(),
            global_state,
        }
    }

    fn build(&mut self, chunk: &DataChunk) {
        let key_col = &chunk.columns[self.left_key];
        let base_idx: usize = self.local_chunks.iter().map(|c| c.len).sum();
        match key_col.data_type {
            DataType::Int64 => {
                let keys = key_col.as_i64_slice();
                for (i, &key) in keys.iter().enumerate() {
                    self.local_table
                        .entry(JoinKey::Int64(key))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(key_col.data.as_ptr() as *const f64, key_col.len)
                };
                for (i, &key) in slice.iter().enumerate() {
                    self.local_table
                        .entry(JoinKey::Float64(key.to_bits()))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Boolean => {
                for (i, &key) in key_col.data[..key_col.len].iter().enumerate() {
                    self.local_table
                        .entry(JoinKey::Boolean(key != 0))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Utf8 => {
                let keys = key_col.as_utf8_slice();
                for (i, key) in keys.iter().enumerate() {
                    self.local_table
                        .entry(JoinKey::Utf8(key.to_string()))
                        .or_default()
                        .push(base_idx + i);
                }
            }
        }
        self.local_chunks.push(chunk.deep_clone());
    }
}

impl Sink for HashJoinBuildSink {
    fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.build(&chunk);
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.global_state.register_local_table(
            std::mem::take(&mut self.local_table),
            std::mem::take(&mut self.local_chunks),
        );
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloneSink for HashJoinBuildSink {
    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(Self {
            join_id: self.join_id,
            left_key: self.left_key,
            local_table: HashMap::new(),
            local_chunks: Vec::new(),
            global_state: self.global_state.clone(),
        })
    }
}

// ------------------------------------------------------------------
// HashJoinProbeOperator
// ------------------------------------------------------------------

/// PipelineOperator that probes the global hash table.
pub struct HashJoinProbeOperator {
    left_key: usize,
    right_key: usize,
    join_type: JoinType,
    join_id: usize,
    global_state: Arc<HashJoinGlobalState>,
}

impl HashJoinProbeOperator {
    pub fn new(left_key: usize, right_key: usize, join_type: JoinType, join_id: usize, global_state: Arc<HashJoinGlobalState>) -> Self {
        Self {
            left_key,
            right_key,
            join_type,
            join_id,
            global_state,
        }
    }

    fn probe(&self, chunk: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        // Global state must be ready by the time probe pipeline starts
        // (the executor ensures build completes before launching probe).
        if !self.global_state.is_ready() {
            return Err(ExecutionError::OperatorError(
                format!("HashJoin {} probe started before build completed", self.join_id)
            ));
        }

        let global_data = self.global_state.get_global_data()
            .ok_or_else(|| ExecutionError::OperatorError(
                format!("HashJoin {} global data not available", self.join_id)
            ))?;

        let build_table = &global_data.0;
        let build_chunks = &global_data.1;

        let right_key_col = &chunk.columns[self.right_key];

        // Collect all joined chunks first, then merge into a single output chunk
        // with sufficient capacity. This avoids the capacity=1 problem from
        // build_joined_chunk when append_chunk is called multiple times.
        let mut joined_chunks: Vec<DataChunk> = Vec::new();

        for row in 0..chunk.len {
            let key = match right_key_col.data_type {
                DataType::Int64 => JoinKey::Int64(right_key_col.as_i64_slice()[row]),
                DataType::Float64 => {
                    let slice = unsafe {
                        std::slice::from_raw_parts(right_key_col.data.as_ptr() as *const f64, right_key_col.len)
                    };
                    JoinKey::Float64(slice[row].to_bits())
                }
                DataType::Boolean => JoinKey::Boolean(right_key_col.data[row] != 0),
                DataType::Utf8 => JoinKey::Utf8(right_key_col.as_utf8_slice()[row].to_string()),
            };

            if let Some(left_rows) = build_table.get(&key) {
                for &left_row in left_rows {
                    joined_chunks.push(Self::build_joined_chunk(left_row, row, build_chunks, chunk)?);
                }
            } else if matches!(self.join_type, JoinType::Left) {
                joined_chunks.push(Self::build_left_join_chunk(row, chunk, build_chunks)?);
            }
        }

        if joined_chunks.is_empty() {
            return Ok(());
        }

        // Merge all joined chunks into a single output with pre-allocated capacity
        let total_rows: usize = joined_chunks.iter().map(|c| c.len).sum();
        let schema: Vec<DataType> = joined_chunks[0].columns.iter().map(|c| c.data_type.clone()).collect();
        let mut merged = DataChunk::with_capacity(schema, total_rows);
        for joined in joined_chunks {
            merged.append_chunk(joined)?;
        }
        *output = merged;

        Ok(())
    }

    fn build_joined_chunk(
        left_row: usize,
        right_row: usize,
        build_chunks: &[DataChunk],
        right_chunk: &DataChunk,
    ) -> Result<DataChunk, ExecutionError> {
        let (chunk_idx, offset) = Self::find_chunk_for_row(left_row, build_chunks);
        let left_chunk = &build_chunks[chunk_idx];
        let local_left_row = left_row - offset;

        let mut columns = Vec::new();
        for col in &left_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, local_left_row, &mut new_col)?;
            columns.push(new_col);
        }
        for col in &right_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, right_row, &mut new_col)?;
            columns.push(new_col);
        }
        Ok(DataChunk::new(columns))
    }

    fn build_left_join_chunk(
        right_row: usize,
        right_chunk: &DataChunk,
        build_chunks: &[DataChunk],
    ) -> Result<DataChunk, ExecutionError> {
        let mut columns = Vec::new();
        if let Some(first_build_chunk) = build_chunks.first() {
            for col in &first_build_chunk.columns {
                let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
                Self::fill_default(col, &mut new_col)?;
                columns.push(new_col);
            }
        }
        for col in &right_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, right_row, &mut new_col)?;
            columns.push(new_col);
        }
        Ok(DataChunk::new(columns))
    }

    fn find_chunk_for_row(row: usize, chunks: &[DataChunk]) -> (usize, usize) {
        let mut offset = 0;
        for (i, chunk) in chunks.iter().enumerate() {
            if row < offset + chunk.len {
                return (i, offset);
            }
            offset += chunk.len;
        }
        panic!("Row {} not found in build chunks", row);
    }

    fn copy_row(
        src: &ColumnVector,
        src_row: usize,
        dst: &mut ColumnVector,
    ) -> Result<(), ExecutionError> {
        match src.data_type {
            DataType::Int64 => dst.push_i64(src.as_i64_slice()[src_row]),
            DataType::Float64 => {
                let slice = unsafe { std::slice::from_raw_parts(src.data.as_ptr() as *const f64, src.len) };
                dst.push_f64(slice[src_row]);
            }
            DataType::Boolean => dst.push_bool(src.data[src_row] != 0),
            DataType::Utf8 => dst.push_utf8(src.as_utf8_slice()[src_row]),
        }
        dst.validity[dst.len - 1] = src.validity[src_row];
        Ok(())
    }

    fn fill_default(src: &ColumnVector, dst: &mut ColumnVector) -> Result<(), ExecutionError> {
        match src.data_type {
            DataType::Int64 => dst.push_i64(0),
            DataType::Float64 => dst.push_f64(0.0),
            DataType::Boolean => dst.push_bool(false),
            DataType::Utf8 => dst.push_utf8(""),
        }
        dst.validity[dst.len - 1] = false; // NULL
        Ok(())
    }
}

impl PipelineOperator for HashJoinProbeOperator {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        self.probe(input, output)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloneOperator for HashJoinProbeOperator {
    fn clone_box(&self) -> Box<dyn PipelineOperator> {
        Box::new(Self {
            left_key: self.left_key,
            right_key: self.right_key,
            join_type: self.join_type.clone(),
            join_id: self.join_id,
            global_state: self.global_state.clone(),
        })
    }
}

pub struct HashJoinOperator {
    left_key_idx: usize,
    right_key_idx: usize,
    join_type: JoinType,
    build_table: HashMap<JoinKey, Vec<usize>>,
    build_chunks: Vec<DataChunk>,
    output: Option<Box<dyn Operator>>,
    state: JoinState,
}

impl HashJoinOperator {
    pub fn new(left_key_idx: usize, right_key_idx: usize, join_type: JoinType) -> Self {
        Self {
            left_key_idx,
            right_key_idx,
            join_type,
            build_table: HashMap::new(),
            build_chunks: Vec::new(),
            output: None,
            state: JoinState::Building,
        }
    }

    pub fn start_probe(&mut self) {
        self.state = JoinState::Probing;
    }

    fn build(&mut self, chunk: DataChunk) {
        let key_col = &chunk.columns[self.left_key_idx];
        let base_idx: usize = self.build_chunks.iter().map(|c| c.len).sum();
        match key_col.data_type {
            DataType::Int64 => {
                let keys = key_col.as_i64_slice();
                for (i, &key) in keys.iter().enumerate() {
                    self.build_table
                        .entry(JoinKey::Int64(key))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(key_col.data.as_ptr() as *const f64, key_col.len)
                };
                for (i, &key) in slice.iter().enumerate() {
                    self.build_table
                        .entry(JoinKey::Float64(key.to_bits()))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Boolean => {
                for (i, &key) in key_col.data[..key_col.len].iter().enumerate() {
                    self.build_table
                        .entry(JoinKey::Boolean(key != 0))
                        .or_default()
                        .push(base_idx + i);
                }
            }
            DataType::Utf8 => {
                let keys = key_col.as_utf8_slice();
                for (i, key) in keys.iter().enumerate() {
                    self.build_table
                        .entry(JoinKey::Utf8(key.to_string()))
                        .or_default()
                        .push(base_idx + i);
                }
            }
        }
        self.build_chunks.push(chunk);
    }

    fn probe(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        let right_key_col = &chunk.columns[self.right_key_idx];
        match right_key_col.data_type {
            DataType::Int64 => {
                let keys = right_key_col.as_i64_slice();
                for (row, &key) in keys.iter().enumerate() {
                    self.probe_key(JoinKey::Int64(key), row, &chunk)?;
                }
            }
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(right_key_col.data.as_ptr() as *const f64, right_key_col.len)
                };
                for (row, &key) in slice.iter().enumerate() {
                    self.probe_key(JoinKey::Float64(key.to_bits()), row, &chunk)?;
                }
            }
            DataType::Boolean => {
                for (row, &key) in right_key_col.data[..right_key_col.len].iter().enumerate() {
                    self.probe_key(JoinKey::Boolean(key != 0), row, &chunk)?;
                }
            }
            DataType::Utf8 => {
                let keys = right_key_col.as_utf8_slice();
                for (row, key) in keys.iter().enumerate() {
                    self.probe_key(JoinKey::Utf8(key.to_string()), row, &chunk)?;
                }
            }
        }
        Ok(())
    }

    fn probe_key(
        &mut self,
        key: JoinKey,
        right_row: usize,
        chunk: &DataChunk,
    ) -> Result<(), ExecutionError> {
        let matched = if let Some(left_rows) = self.build_table.get(&key) {
            for &left_row in left_rows {
                let joined = self.build_joined_chunk(left_row, right_row, chunk)?;
                if let Some(ref mut output) = self.output {
                    output.push(joined)?;
                }
            }
            true
        } else {
            false
        };

        if !matched && matches!(self.join_type, JoinType::Left) {
            let joined = self.build_left_join_chunk(right_row, chunk)?;
            if let Some(ref mut output) = self.output {
                output.push(joined)?;
            }
        }

        Ok(())
    }

    fn build_joined_chunk(
        &self,
        left_row: usize,
        right_row: usize,
        right_chunk: &DataChunk,
    ) -> Result<DataChunk, ExecutionError> {
        let mut columns = Vec::new();
        let (chunk_idx, offset) = self.find_chunk_for_row(left_row);
        let left_chunk = &self.build_chunks[chunk_idx];
        let local_left_row = left_row - offset;
        for col in &left_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, local_left_row, &mut new_col)?;
            columns.push(new_col);
        }
        for col in &right_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, right_row, &mut new_col)?;
            columns.push(new_col);
        }
        Ok(DataChunk::new(columns))
    }

    fn build_left_join_chunk(
        &self,
        right_row: usize,
        right_chunk: &DataChunk,
    ) -> Result<DataChunk, ExecutionError> {
        let mut columns = Vec::new();

        // For LEFT JOIN without match: emit NULL/default values for build (left) side
        // and the actual probe (right) row values.
        if let Some(first_build_chunk) = self.build_chunks.first() {
            for col in &first_build_chunk.columns {
                let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
                Self::fill_default(col, &mut new_col)?;
                columns.push(new_col);
            }
        }
        for col in &right_chunk.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), 1);
            Self::copy_row(col, right_row, &mut new_col)?;
            columns.push(new_col);
        }

        Ok(DataChunk::new(columns))
    }

    fn find_chunk_for_row(&self, row: usize) -> (usize, usize) {
        let mut offset = 0;
        for (i, chunk) in self.build_chunks.iter().enumerate() {
            if row < offset + chunk.len {
                return (i, offset);
            }
            offset += chunk.len;
        }
        panic!("Row {} not found in build chunks", row);
    }

    fn copy_row(
        src: &ColumnVector,
        src_row: usize,
        dst: &mut ColumnVector,
    ) -> Result<(), ExecutionError> {
        match src.data_type {
            DataType::Int64 => dst.push_i64(src.as_i64_slice()[src_row]),
            DataType::Float64 => {
                let slice =
                    unsafe { std::slice::from_raw_parts(src.data.as_ptr() as *const f64, src.len) };
                dst.push_f64(slice[src_row]);
            }
            DataType::Boolean => dst.push_bool(src.data[src_row] != 0),
            DataType::Utf8 => {
                let slice = src.as_utf8_slice();
                dst.push_utf8(slice[src_row]);
            }
        }
        dst.validity[dst.len - 1] = src.validity[src_row];
        Ok(())
    }

    fn fill_default(src: &ColumnVector, dst: &mut ColumnVector) -> Result<(), ExecutionError> {
        match src.data_type {
            DataType::Int64 => dst.push_i64(0),
            DataType::Float64 => dst.push_f64(0.0),
            DataType::Boolean => dst.push_bool(false),
            DataType::Utf8 => dst.push_utf8(""),
        }
        dst.validity[dst.len - 1] = false; // NULL
        Ok(())
    }
}

impl Operator for HashJoinOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        match self.state {
            JoinState::Building => {
                self.build(chunk);
                Ok(())
            }
            JoinState::Probing => self.probe(chunk),
            JoinState::Done => Ok(()),
        }
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.state = JoinState::Done;
        if let Some(ref mut output) = self.output {
            output.finalize()?;
        }
        Ok(())
    }

    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.output = Some(output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSink {
        chunks: Vec<DataChunk>,
    }

    impl Operator for TestSink {
        fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
            self.chunks.push(chunk);
            Ok(())
        }
        fn finalize(&mut self) -> Result<(), ExecutionError> {
            Ok(())
        }
        fn set_output(&mut self, _output: Box<dyn Operator>) {}
    }

    #[test]
    fn test_hash_join_inner_i64() {
        let sink = TestSink { chunks: Vec::new() };

        // Build side: left table with columns [key, value]
        let mut left_key = ColumnVector::new(DataType::Int64, 4);
        left_key.push_i64(1);
        left_key.push_i64(2);
        left_key.push_i64(3);
        left_key.push_i64(2);

        let mut left_val = ColumnVector::new(DataType::Int64, 4);
        left_val.push_i64(10);
        left_val.push_i64(20);
        left_val.push_i64(30);
        left_val.push_i64(25);

        let build_chunk = DataChunk::new(vec![left_key, left_val]);

        // Probe side: right table with columns [key, rvalue]
        let mut right_key = ColumnVector::new(DataType::Int64, 2);
        right_key.push_i64(2);
        right_key.push_i64(4);

        let mut right_val = ColumnVector::new(DataType::Int64, 2);
        right_val.push_i64(200);
        right_val.push_i64(400);

        let probe_chunk = DataChunk::new(vec![right_key, right_val]);

        let mut join = HashJoinOperator::new(0, 0, JoinType::Inner);
        join.set_output(Box::new(sink));

        join.push(build_chunk).unwrap();
        join.start_probe();
        join.push(probe_chunk).unwrap();
        join.finalize().unwrap();
    }

    #[test]
    fn test_hash_join_inner_utf8() {
        let sink = TestSink { chunks: Vec::new() };

        // Build side with Utf8 keys
        let mut left_key = ColumnVector::new(DataType::Utf8, 3);
        left_key.push_utf8("alice");
        left_key.push_utf8("bob");
        left_key.push_utf8("charlie");

        let mut left_val = ColumnVector::new(DataType::Int64, 3);
        left_val.push_i64(100);
        left_val.push_i64(200);
        left_val.push_i64(300);

        let build_chunk = DataChunk::new(vec![left_key, left_val]);

        // Probe side with Utf8 keys
        let mut right_key = ColumnVector::new(DataType::Utf8, 2);
        right_key.push_utf8("bob");
        right_key.push_utf8("dave");

        let mut right_val = ColumnVector::new(DataType::Int64, 2);
        right_val.push_i64(20);
        right_val.push_i64(40);

        let probe_chunk = DataChunk::new(vec![right_key, right_val]);

        let mut join = HashJoinOperator::new(0, 0, JoinType::Inner);
        join.set_output(Box::new(sink));

        join.push(build_chunk).unwrap();
        join.start_probe();
        join.push(probe_chunk).unwrap();
        join.finalize().unwrap();
    }

    #[test]
    fn test_hash_join_left_no_match() {
        let sink = TestSink { chunks: Vec::new() };

        // Build side
        let mut left_key = ColumnVector::new(DataType::Int64, 2);
        left_key.push_i64(1);
        left_key.push_i64(2);

        let mut left_val = ColumnVector::new(DataType::Int64, 2);
        left_val.push_i64(10);
        left_val.push_i64(20);

        let build_chunk = DataChunk::new(vec![left_key, left_val]);

        // Probe side with a key that has no match
        let mut right_key = ColumnVector::new(DataType::Int64, 1);
        right_key.push_i64(99);

        let mut right_val = ColumnVector::new(DataType::Int64, 1);
        right_val.push_i64(990);

        let probe_chunk = DataChunk::new(vec![right_key, right_val]);

        let mut join = HashJoinOperator::new(0, 0, JoinType::Left);
        join.set_output(Box::new(sink));

        join.push(build_chunk).unwrap();
        join.start_probe();
        join.push(probe_chunk).unwrap();
        join.finalize().unwrap();
    }

    // ------------------------------------------------------------------
    // HashJoinGlobalState tests
    // ------------------------------------------------------------------

    #[test]
    fn test_hash_join_global_state_single_worker() {
        let global = HashJoinGlobalState::new();
        global.set_expected_tasks(1);

        let mut table: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        table.insert(JoinKey::Int64(1), vec![0]);
        table.insert(JoinKey::Int64(2), vec![1]);

        let mut col = ColumnVector::new(DataType::Int64, 2);
        col.push_i64(10);
        col.push_i64(20);
        let chunk = DataChunk::new(vec![col]);

        global.register_local_table(table, vec![chunk]);

        assert!(global.is_ready());
        let data = global.get_global_data().unwrap();
        assert_eq!(data.0.len(), 2);
        assert_eq!(data.0.get(&JoinKey::Int64(1)).unwrap(), &vec![0]);
        assert_eq!(data.0.get(&JoinKey::Int64(2)).unwrap(), &vec![1]);
        assert_eq!(data.1.len(), 1);
        assert_eq!(data.1[0].len, 2);
    }

    #[test]
    fn test_hash_join_global_state_multiple_workers() {
        let global = HashJoinGlobalState::new();
        global.set_expected_tasks(3);

        // Worker 0: keys 1, 2
        let mut table0: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        table0.insert(JoinKey::Int64(1), vec![0]);
        table0.insert(JoinKey::Int64(2), vec![1]);
        let mut col0 = ColumnVector::new(DataType::Int64, 2);
        col0.push_i64(10);
        col0.push_i64(20);
        let chunk0 = DataChunk::new(vec![col0]);

        // Worker 1: keys 3, 4
        let mut table1: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        table1.insert(JoinKey::Int64(3), vec![0]);
        table1.insert(JoinKey::Int64(4), vec![1]);
        let mut col1 = ColumnVector::new(DataType::Int64, 2);
        col1.push_i64(30);
        col1.push_i64(40);
        let chunk1 = DataChunk::new(vec![col1]);

        // Worker 2: key 2 (duplicate key across workers)
        let mut table2: HashMap<JoinKey, Vec<usize>> = HashMap::new();
        table2.insert(JoinKey::Int64(2), vec![0]);
        let mut col2 = ColumnVector::new(DataType::Int64, 1);
        col2.push_i64(25);
        let chunk2 = DataChunk::new(vec![col2]);

        global.register_local_table(table0, vec![chunk0]);
        assert!(!global.is_ready());

        global.register_local_table(table1, vec![chunk1]);
        assert!(!global.is_ready());

        global.register_local_table(table2, vec![chunk2]);
        assert!(global.is_ready());

        let data = global.get_global_data().unwrap();
        // key 1 -> row 0 in global
        assert_eq!(data.0.get(&JoinKey::Int64(1)).unwrap(), &vec![0]);
        // key 2 -> row 1 (from worker 0) and row 4 (from worker 2, offset by 3)
        let key2_rows = data.0.get(&JoinKey::Int64(2)).unwrap();
        assert_eq!(key2_rows.len(), 2);
        assert!(key2_rows.contains(&1));
        assert!(key2_rows.contains(&4));
        // key 3 -> row 2 (offset by 2 from worker 1)
        assert_eq!(data.0.get(&JoinKey::Int64(3)).unwrap(), &vec![2]);
        // key 4 -> row 3 (offset by 2 from worker 1)
        assert_eq!(data.0.get(&JoinKey::Int64(4)).unwrap(), &vec![3]);

        // Total chunks: 3, total rows: 2 + 2 + 1 = 5
        assert_eq!(data.1.len(), 3);
        let total_rows: usize = data.1.iter().map(|c| c.len).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_hash_join_global_state_parallel_build() {
        use std::thread;

        let global = Arc::new(HashJoinGlobalState::new());
        global.set_expected_tasks(4);

        let mut handles = Vec::new();
        for worker_id in 0..4 {
            let g = global.clone();
            let handle = thread::spawn(move || {
                let mut table: HashMap<JoinKey, Vec<usize>> = HashMap::new();
                table.insert(JoinKey::Int64(worker_id as i64), vec![0]);
                table.insert(JoinKey::Int64(100), vec![1]); // shared key

                let mut col = ColumnVector::new(DataType::Int64, 2);
                col.push_i64(worker_id as i64 * 10);
                col.push_i64(1000);
                let chunk = DataChunk::new(vec![col]);

                g.register_local_table(table, vec![chunk]);
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(global.is_ready());
        let data = global.get_global_data().unwrap();

        // Each worker contributed a unique key + the shared key 100
        // Shared key 100 should have 4 entries, one per worker
        let shared_rows = data.0.get(&JoinKey::Int64(100)).unwrap();
        assert_eq!(shared_rows.len(), 4);

        // Unique keys
        for worker_id in 0..4 {
            let key = JoinKey::Int64(worker_id as i64);
            assert!(data.0.contains_key(&key));
        }

        // Total rows: 4 workers * 2 rows = 8
        let total_rows: usize = data.1.iter().map(|c| c.len).sum();
        assert_eq!(total_rows, 8);
    }
}
