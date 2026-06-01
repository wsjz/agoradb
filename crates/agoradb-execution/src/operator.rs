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
use agoradb_core::ExecutionError;

/// Push-based operator trait.
/// Data flows: upstream operator → push() → this operator → push() → downstream operator
pub trait Operator: Send {
    /// Push a DataChunk into this operator for processing.
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError>;

    /// Signal end of input (called after all chunks are pushed).
    fn finalize(&mut self) -> Result<(), ExecutionError>;

    /// Set the downstream operator to receive output from this operator.
    fn set_output(&mut self, output: Box<dyn Operator>);
}

