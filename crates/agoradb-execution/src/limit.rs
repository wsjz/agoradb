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

/// Limit operator.
pub struct LimitOperator {
    skip: usize,
    fetch: usize,
    seen: usize,
    emitted: usize,
}

impl LimitOperator {
    pub fn new(skip: usize, fetch: usize) -> Self {
        Self {
            skip,
            fetch,
            seen: 0,
            emitted: 0,
        }
    }

    pub fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<(), ExecutionError> {
        if self.emitted >= self.fetch {
            return Ok(());
        }

        let chunk_len = input.len;
        let start = self.seen.saturating_sub(self.skip);
        let end = ((self.seen + chunk_len).saturating_sub(self.skip)).min(self.fetch);

        if end > start {
            let sliced = input.slice_rows(start, end);
            *output = sliced;
            self.emitted += end - start;
        }

        self.seen += chunk_len;
        Ok(())
    }
}
