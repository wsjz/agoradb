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

use agoradb_core::{DataType, ExecutionError};
use arrow_array::Array;

/// A column of data — the core computational unit.
pub struct ColumnVector {
    pub data_type: DataType,
    pub validity: Vec<bool>,  // true = valid (not null)
    pub data: Vec<u8>,        // type-punned storage for fixed-width types
    pub strings: Vec<String>, // for Utf8 type
    pub len: usize,
    pub capacity: usize,
}

impl ColumnVector {
    pub fn new(data_type: DataType, capacity: usize) -> Self {
        let data_size = match data_type {
            DataType::Int64 => 8,
            DataType::Float64 => 8,
            DataType::Boolean => 1,
            DataType::Utf8 => 0, // variable length — stored in strings vec
        };
        Self {
            data_type,
            validity: vec![true; capacity],
            data: vec![0; capacity * data_size],
            strings: Vec::new(),
            len: 0,
            capacity,
        }
    }

    /// Get a slice of i64 values (panics if wrong type).
    pub fn as_i64_slice(&self) -> &[i64] {
        assert!(matches!(self.data_type, DataType::Int64));
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const i64, self.len) }
    }

    /// Get a mutable slice of i64 values.
    pub fn as_i64_slice_mut(&mut self) -> &mut [i64] {
        assert!(matches!(self.data_type, DataType::Int64));
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr() as *mut i64, self.len) }
    }

    /// Append a single i64 value.
    pub fn push_i64(&mut self, value: i64) {
        assert!(matches!(self.data_type, DataType::Int64));
        assert!(self.len < self.capacity);
        unsafe {
            let ptr = self.data.as_mut_ptr() as *mut i64;
            ptr.add(self.len).write(value);
        }
        self.len += 1;
    }

    /// Append a single f64 value.
    pub fn push_f64(&mut self, value: f64) {
        assert!(matches!(self.data_type, DataType::Float64));
        assert!(self.len < self.capacity);
        unsafe {
            let ptr = self.data.as_mut_ptr() as *mut f64;
            ptr.add(self.len).write(value);
        }
        self.len += 1;
    }

    /// Append a single bool value.
    pub fn push_bool(&mut self, value: bool) {
        assert!(matches!(self.data_type, DataType::Boolean));
        assert!(self.len < self.capacity);
        self.data[self.len] = if value { 1 } else { 0 };
        self.len += 1;
    }

    /// Append a single Utf8 value.
    pub fn push_utf8(&mut self, value: &str) {
        assert!(matches!(self.data_type, DataType::Utf8));
        assert!(self.len < self.capacity);
        self.strings.push(value.to_string());
        self.len += 1;
    }

    /// Get a slice of string references (panics if wrong type).
    pub fn as_utf8_slice(&self) -> Vec<&str> {
        assert!(matches!(self.data_type, DataType::Utf8));
        self.strings.iter().map(|s| s.as_str()).collect()
    }

    /// Deep clone this ColumnVector (copies all data).
    pub fn deep_clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            validity: self.validity.clone(),
            data: self.data.clone(),
            strings: self.strings.clone(),
            len: self.len,
            capacity: self.capacity,
        }
    }

    /// Create a new ColumnVector containing only rows in [start, end).
    pub fn slice_rows(&self, start: usize, end: usize) -> Self {
        let count = end - start;
        let mut new = Self::new(self.data_type.clone(), count);
        match self.data_type {
            DataType::Int64 => {
                let slice = self.as_i64_slice();
                for i in start..end {
                    new.push_i64(slice[i]);
                    new.validity[new.len - 1] = self.validity[i];
                }
            }
            DataType::Float64 => {
                let slice = unsafe {
                    std::slice::from_raw_parts(self.data.as_ptr() as *const f64, self.len)
                };
                for i in start..end {
                    new.push_f64(slice[i]);
                    new.validity[new.len - 1] = self.validity[i];
                }
            }
            DataType::Boolean => {
                for i in start..end {
                    new.push_bool(self.data[i] != 0);
                    new.validity[new.len - 1] = self.validity[i];
                }
            }
            DataType::Utf8 => {
                for i in start..end {
                    new.push_utf8(&self.strings[i]);
                    new.validity[new.len - 1] = self.validity[i];
                }
            }
        }
        new
    }
}

/// A batch of rows — passed between operators in the pipeline.
pub struct DataChunk {
    pub columns: Vec<ColumnVector>,
    pub len: usize,
}

impl DataChunk {
    pub fn new(columns: Vec<ColumnVector>) -> Self {
        let len = columns.first().map(|c| c.len).unwrap_or(0);
        Self { columns, len }
    }

    pub fn with_capacity(schema: Vec<DataType>, capacity: usize) -> Self {
        let columns = schema
            .into_iter()
            .map(|dt| ColumnVector::new(dt, capacity))
            .collect();
        Self { columns, len: 0 }
    }

    /// Deep clone this DataChunk (copies all column data).
    pub fn deep_clone(&self) -> Self {
        let columns = self.columns.iter().map(|c| c.deep_clone()).collect();
        Self {
            columns,
            len: self.len,
        }
    }

    /// Create a new DataChunk containing only rows in [start, end).
    pub fn slice_rows(&self, start: usize, end: usize) -> Self {
        let columns = self.columns.iter().map(|c| c.slice_rows(start, end)).collect();
        Self::new(columns)
    }

    /// Convert from Arrow RecordBatch (used at IO boundary).
    pub fn from_record_batch(batch: &arrow_array::RecordBatch) -> Result<Self, ExecutionError> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for array in batch.columns() {
            let col = if let Some(arr) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                let mut col = ColumnVector::new(DataType::Int64, arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        col.validity[i] = false;
                        col.push_i64(0);
                    } else {
                        col.push_i64(arr.value(i));
                    }
                }
                col
            } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                let mut col = ColumnVector::new(DataType::Float64, arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        col.validity[i] = false;
                        col.push_f64(0.0);
                    } else {
                        col.push_f64(arr.value(i));
                    }
                }
                col
            } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                let mut col = ColumnVector::new(DataType::Boolean, arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        col.validity[i] = false;
                        col.push_bool(false);
                    } else {
                        col.push_bool(arr.value(i));
                    }
                }
                col
            } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
                let mut col = ColumnVector::new(DataType::Utf8, arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        col.validity[i] = false;
                        col.push_utf8("");
                    } else {
                        col.push_utf8(arr.value(i));
                    }
                }
                col
            } else {
                return Err(ExecutionError::OperatorError(format!(
                    "Unsupported Arrow array type: {:?}",
                    array.data_type()
                )));
            };
            columns.push(col);
        }
        Ok(Self::new(columns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_vector_i64() {
        let mut col = ColumnVector::new(DataType::Int64, 1024);
        col.push_i64(1);
        col.push_i64(2);
        col.push_i64(3);
        assert_eq!(col.len, 3);
        assert_eq!(col.as_i64_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_column_vector_utf8() {
        let mut col = ColumnVector::new(DataType::Utf8, 1024);
        col.push_utf8("hello");
        col.push_utf8("world");
        assert_eq!(col.len, 2);
        assert_eq!(col.as_utf8_slice(), vec!["hello", "world"]);
    }

    #[test]
    fn test_data_chunk_from_record_batch() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType as ArrowDataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let chunk = DataChunk::from_record_batch(&batch).unwrap();
        assert_eq!(chunk.len, 3);
        assert_eq!(chunk.columns[0].as_i64_slice(), &[1, 2, 3]);
    }
}
