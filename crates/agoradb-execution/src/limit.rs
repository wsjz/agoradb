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
use crate::operator::Operator;
use agoradb_core::ExecutionError;

pub struct LimitOperator {
    skip: usize,
    fetch: usize,
    seen: usize,
    emitted: usize,
    output: Option<Box<dyn Operator>>,
}

impl LimitOperator {
    pub fn new(skip: usize, fetch: usize) -> Self {
        Self {
            skip,
            fetch,
            seen: 0,
            emitted: 0,
            output: None,
        }
    }
}

impl Operator for LimitOperator {
    fn push(&mut self, chunk: DataChunk) -> Result<(), ExecutionError> {
        if self.emitted >= self.fetch {
            return Ok(());
        }

        let chunk_len = chunk.len;
        let start = self.seen.saturating_sub(self.skip);
        let end = ((self.seen + chunk_len).saturating_sub(self.skip)).min(self.fetch);

        if end > start {
            if let Some(ref mut output) = self.output {
                let sliced = chunk.slice_rows(start, end);
                output.push(sliced)?;
            }
            self.emitted += end - start;
        }

        self.seen += chunk_len;
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
