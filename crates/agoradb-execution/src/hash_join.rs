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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum JoinKey {
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
}
