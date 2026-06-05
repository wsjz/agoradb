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
use crate::filter::PredicateFn;
use agoradb_core::{DataType, ExecutionError, PredicateDef};

/// Compare a column cell against an i64 value, handling both Int64 and Float64 types.
fn compare_cell(chunk: &DataChunk, col: usize, row: usize, val: i64) -> (f64, f64) {
    match chunk.columns[col].data_type {
        DataType::Int64 => {
            let v = chunk.columns[col].as_i64_slice()[row];
            (v as f64, val as f64)
        }
        DataType::Float64 => {
            let slice = unsafe {
                std::slice::from_raw_parts(
                    chunk.columns[col].data.as_ptr() as *const f64,
                    chunk.columns[col].len,
                )
            };
            (slice[row], val as f64)
        }
        _ => (0.0, val as f64),
    }
}

/// Build a runtime predicate closure from a [`PredicateDef`].
pub fn build_predicate_fn(pred: &PredicateDef) -> Result<PredicateFn, ExecutionError> {
    match pred {
        PredicateDef::Eq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                (a - b).abs() < f64::EPSILON
            }))
        }
        PredicateDef::Neq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                (a - b).abs() >= f64::EPSILON
            }))
        }
        PredicateDef::Lt { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                a < b
            }))
        }
        PredicateDef::LtEq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                a <= b
            }))
        }
        PredicateDef::Gt { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                a > b
            }))
        }
        PredicateDef::GtEq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                let (a, b) = compare_cell(chunk, col, row, val);
                a >= b
            }))
        }
        PredicateDef::And { left, right } => {
            let left_fn = build_predicate_fn(left)?;
            let right_fn = build_predicate_fn(right)?;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                left_fn(chunk, row) && right_fn(chunk, row)
            }))
        }
        PredicateDef::Or { left, right } => {
            let left_fn = build_predicate_fn(left)?;
            let right_fn = build_predicate_fn(right)?;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                left_fn(chunk, row) || right_fn(chunk, row)
            }))
        }
    }
}
