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
use crate::operator::Operator;
use crate::pipeline::{CloneOperator, PipelineOperator};
use agoradb_core::ExecutionError;

pub struct ProjectOperator {
    column_indices: Vec<usize>,
    output: Option<Box<dyn Operator>>,
}

impl ProjectOperator {
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self {
            column_indices,
            output: None,
        }
    }
}

/// Pipeline-based project operator (pull-based).
pub struct ProjectPipelineOperator {
    column_indices: Vec<usize>,
}

impl ProjectPipelineOperator {
    pub fn new(column_indices: Vec<usize>) -> Self {
        Self { column_indices }
    }
}

impl PipelineOperator for ProjectPipelineOperator {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl CloneOperator for ProjectPipelineOperator {
    fn clone_box(&self) -> Box<dyn PipelineOperator> {
        Box::new(Self {
            column_indices: self.column_indices.clone(),
        })
    }
}

impl Operator for ProjectOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        let mut columns = Vec::with_capacity(self.column_indices.len());
        for &idx in &self.column_indices {
            if idx >= chunk.columns.len() {
                return Err(ExecutionError::ColumnNotFound(format!(
                    "Column index {} out of bounds",
                    idx
                )));
            }
            columns.push(ColumnVector {
                data_type: chunk.columns[idx].data_type.clone(),
                validity: chunk.columns[idx].validity.clone(),
                data: chunk.columns[idx].data.clone(),
                strings: chunk.columns[idx].strings.clone(),
                len: chunk.columns[idx].len,
                capacity: chunk.columns[idx].capacity,
            });
        }
        if let Some(ref mut output) = self.output {
            output.push(DataChunk::new(columns))?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        if let Some(ref mut output) = self.output {
            output.finalize()?;
        }
        Ok(())
    }

    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.output = Some(output);
    }
}
