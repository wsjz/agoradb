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
use agoradb_core::ExecutionError;

/// Project operator.
pub struct ProjectOperator {
    column_indices: Vec<usize>,
}

impl ProjectOperator {
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self { column_indices }
    }

    pub fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        let mut columns = Vec::with_capacity(self.column_indices.len());
        for &idx in &self.column_indices {
            if idx >= input.columns.len() {
                return Err(ExecutionError::ColumnNotFound(format!(
                    "Column index {} out of bounds",
                    idx
                )));
            }
            columns.push(ColumnVector {
                data_type: input.columns[idx].data_type.clone(),
                validity: input.columns[idx].validity.clone(),
                data: input.columns[idx].data.clone(),
                strings: input.columns[idx].strings.clone(),
                len: input.columns[idx].len,
                capacity: input.columns[idx].capacity,
            });
        }
        *output = DataChunk::new(columns);
        Ok(())
    }
}
