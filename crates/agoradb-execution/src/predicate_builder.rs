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
use agoradb_core::{ExecutionError, PredicateDef};

/// Build a runtime predicate closure from a [`PredicateDef`].
pub fn build_predicate_fn(pred: &PredicateDef) -> Result<PredicateFn, ExecutionError> {
    match pred {
        PredicateDef::Eq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] == val
            }))
        }
        PredicateDef::Neq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] != val
            }))
        }
        PredicateDef::Lt { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] < val
            }))
        }
        PredicateDef::LtEq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] <= val
            }))
        }
        PredicateDef::Gt { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] > val
            }))
        }
        PredicateDef::GtEq { column, value } => {
            let col = *column;
            let val = *value;
            Ok(Box::new(move |chunk: &DataChunk, row: usize| {
                chunk.columns[col].as_i64_slice()[row] >= val
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
