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

use agoradb_core::APPEND_BUFFER_FLUSH_THRESHOLD_ROWS;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// In-memory buffer for accumulating records before flushing to Parquet.
pub struct AppendBuffer {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
}

impl AppendBuffer {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            row_count: 0,
        }
    }

    /// Append a batch to the buffer.
    pub fn push(&mut self, batch: RecordBatch) {
        self.row_count += batch.num_rows();
        self.batches.push(batch);
    }

    /// Check if the buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        self.row_count >= APPEND_BUFFER_FLUSH_THRESHOLD_ROWS
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.row_count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_buffer_accumulation() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut buffer = AppendBuffer::new(schema.clone());

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        buffer.push(batch);
        assert_eq!(buffer.row_count, 3);
        assert!(!buffer.should_flush());
    }

    #[test]
    fn test_buffer_should_flush() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut buffer = AppendBuffer::new(schema.clone());

        for i in 0..1000 {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap();
            buffer.push(batch);
        }

        assert!(!buffer.should_flush()); // 1000 < 10_000

        for i in 0..9001 {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .unwrap();
            buffer.push(batch);
        }

        assert!(buffer.should_flush()); // 10_001 >= 10_000
    }
}
