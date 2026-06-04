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
use crate::hash_aggregate::HashAggregateOperator;
use crate::hash_join::HashJoinOperator;
use crate::operator::Operator;
use crate::pipeline::{CloneSink, Sink};
use crate::sort::SortState;
use agoradb_core::ExecutionError;
use std::any::Any;
use std::sync::{Arc, Mutex};

/// Operator adapter that forwards push() calls to a shared [`HashJoinOperator`].
pub struct JoinAdapter(pub Arc<Mutex<HashJoinOperator>>);

impl Operator for JoinAdapter {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().push(chunk)
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().finalize()
    }
    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.0.lock().unwrap().set_output(output);
    }
}

/// Operator adapter that forwards push() calls to a shared [`HashAggregateOperator`].
pub struct AggregateAdapter(pub Arc<Mutex<HashAggregateOperator>>);

impl Operator for AggregateAdapter {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().push(chunk)
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        self.0.lock().unwrap().finalize()
    }
    fn set_output(&mut self, output: Box<dyn Operator>) {
        self.0.lock().unwrap().set_output(output);
    }
}

/// Operator adapter that forwards push() calls to a shared [`SortState`].
pub struct SortAdapter(pub Arc<SortState>);

impl Operator for SortAdapter {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.0.push(chunk);
        Ok(())
    }
    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
    fn set_output(&mut self, _output: Box<dyn Operator>) {}
}

/// A collecting sink — stores all consumed chunks in a Vec, implements `Sink`.
pub struct CollectSink {
    results: Arc<Mutex<Vec<DataChunk>>>,
}

impl CollectSink {
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn into_results(self) -> Vec<DataChunk> {
        match Arc::try_unwrap(self.results) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn get_results_arc(&self) -> Arc<Mutex<Vec<DataChunk>>> {
        self.results.clone()
    }
}

impl Sink for CollectSink {
    fn consume(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        self.results.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CloneSink for CollectSink {
    fn clone_box(&self) -> Box<dyn Sink> {
        Box::new(Self {
            results: self.results.clone(),
        })
    }
}
