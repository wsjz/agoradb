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
use crate::pipeline::{CloneOperator, PipelineOperator};
use crate::predicate_builder::build_predicate_fn;
use agoradb_core::DataType;
use agoradb_core::{ExecutionError, PredicateDef};

pub type PredicateFn = Box<dyn Fn(&DataChunk, usize) -> bool + Send>;

/// Pipeline filter operator.
/// Stores the `PredicateDef` (cloneable) and builds the closure on each `execute` call.
pub struct FilterOperator {
    predicate_def: PredicateDef,
}

impl FilterOperator {
    pub fn new(predicate_def: PredicateDef) -> Self {
        Self { predicate_def }
    }
}

impl PipelineOperator for FilterOperator {
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        let predicate = build_predicate_fn(&self.predicate_def)?;
        let selected: Vec<usize> = (0..input.len).filter(|&i| predicate(input, i)).collect();

        if selected.is_empty() {
            return Ok(());
        }

        let mut output_columns = Vec::with_capacity(input.columns.len());
        for col in &input.columns {
            let mut new_col = ColumnVector::new(col.data_type.clone(), selected.len());
            for &idx in &selected {
                match col.data_type {
                    DataType::Int64 => new_col.push_i64(col.as_i64_slice()[idx]),
                    DataType::Float64 => {
                        let slice = unsafe {
                            std::slice::from_raw_parts(col.data.as_ptr() as *const f64, col.len)
                        };
                        new_col.push_f64(slice[idx]);
                    }
                    DataType::Boolean => new_col.push_bool(col.data[idx] != 0),
                    DataType::Utf8 => new_col.push_utf8(col.as_utf8_slice()[idx]),
                }
                new_col.validity[new_col.len - 1] = col.validity[idx];
            }
            output_columns.push(new_col);
        }

        *output = DataChunk::new(output_columns);
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl CloneOperator for FilterOperator {
    fn clone_box(&self) -> Box<dyn PipelineOperator> {
        Box::new(Self {
            predicate_def: self.predicate_def.clone(),
        })
    }
}
