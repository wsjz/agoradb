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

//! SQLite rows → Arrow batches.
//!
//! SQLite is dynamically typed, so a column's Arrow type is decided up front
//! from its declared type (affinity rules) or, failing that, from the first
//! non-null value observed. Values that do not fit the decided type are an
//! error rather than a silent coercion.

use std::sync::Arc;

use agoradb_engine::EngineError;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rusqlite::types::ValueRef;

/// The Arrow-side type chosen for a SQLite column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    Binary,
    Boolean,
}

impl ColumnType {
    /// The Arrow data type this column is materialised as.
    pub fn arrow_type(self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Utf8 => DataType::Utf8,
            ColumnType::Binary => DataType::Binary,
            ColumnType::Boolean => DataType::Boolean,
        }
    }
}

/// Map a declared SQLite column type to a [`ColumnType`] following the
/// affinity rules of <https://www.sqlite.org/datatype3.html#affname>, with
/// `BOOL` and date/time names special-cased.
pub fn affinity_type(declared: &str) -> ColumnType {
    let upper = declared.to_ascii_uppercase();
    if upper.contains("BOOL") {
        ColumnType::Boolean
    } else if upper.contains("INT") {
        ColumnType::Int64
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        ColumnType::Utf8
    } else if upper.contains("DATE") || upper.contains("TIME") {
        // SQLite stores these as text; keep them textual so nothing is lost.
        ColumnType::Utf8
    } else if upper.is_empty() || upper.contains("BLOB") {
        ColumnType::Binary
    } else if upper.contains("REAL")
        || upper.contains("FLOA")
        || upper.contains("DOUB")
        || upper.contains("NUMERIC")
        || upper.contains("DECIMAL")
    {
        ColumnType::Float64
    } else {
        // NUMERIC affinity for anything else; values are usually numbers.
        ColumnType::Float64
    }
}

/// Infer a [`ColumnType`] from an observed value; `None` for SQL NULL.
pub fn infer_from_value(value: ValueRef<'_>) -> Option<ColumnType> {
    match value {
        ValueRef::Null => None,
        ValueRef::Integer(_) => Some(ColumnType::Int64),
        ValueRef::Real(_) => Some(ColumnType::Float64),
        ValueRef::Text(_) => Some(ColumnType::Utf8),
        ValueRef::Blob(_) => Some(ColumnType::Binary),
    }
}

/// Build the Arrow schema for `names` / `types` (all columns nullable, as
/// SQLite result sets carry no nullability information).
pub fn arrow_schema(names: &[String], types: &[ColumnType]) -> SchemaRef {
    let fields = names
        .iter()
        .zip(types)
        .map(|(name, ty)| Field::new(name, ty.arrow_type(), true))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

enum ColumnBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Boolean(BooleanBuilder),
}

impl ColumnBuilder {
    fn new(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
            ColumnType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
            ColumnType::Utf8 => ColumnBuilder::Utf8(StringBuilder::new()),
            ColumnType::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            ColumnType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
        }
    }
}

fn mismatch(column: &str, expected: ColumnType, value: ValueRef<'_>) -> EngineError {
    let actual = match value {
        ValueRef::Null => "NULL",
        ValueRef::Integer(_) => "INTEGER",
        ValueRef::Real(_) => "REAL",
        ValueRef::Text(_) => "TEXT",
        ValueRef::Blob(_) => "BLOB",
    };
    EngineError::TypeMismatch {
        column: column.to_string(),
        expected: format!("{expected:?}"),
        actual: actual.to_string(),
    }
}

fn text_of<'a>(
    column: &str,
    expected: ColumnType,
    value: ValueRef<'a>,
) -> Result<&'a str, EngineError> {
    match value {
        ValueRef::Text(bytes) => {
            std::str::from_utf8(bytes).map_err(|_| mismatch(column, expected, value))
        }
        other => Err(mismatch(column, expected, other)),
    }
}

/// Accumulates SQLite rows into Arrow batches of a fixed schema.
pub struct RowBatcher {
    schema: SchemaRef,
    names: Vec<String>,
    types: Vec<ColumnType>,
    builders: Vec<ColumnBuilder>,
    rows: usize,
}

impl RowBatcher {
    /// Create a batcher for the given column names and types.
    pub fn new(names: Vec<String>, types: Vec<ColumnType>) -> Self {
        let schema = arrow_schema(&names, &types);
        let builders = types.iter().map(|t| ColumnBuilder::new(*t)).collect();
        Self {
            schema,
            names,
            types,
            builders,
            rows: 0,
        }
    }

    /// The schema every produced batch has.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Rows accumulated since the last [`Self::flush`].
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether no rows are pending.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one row given as column values.
    pub fn push_row(&mut self, values: &[ValueRef<'_>]) -> Result<(), EngineError> {
        if values.len() != self.builders.len() {
            return Err(EngineError::Sql(format!(
                "row has {} columns, expected {}",
                values.len(),
                self.builders.len()
            )));
        }
        for (i, value) in values.iter().enumerate() {
            let column = &self.names[i];
            let expected = self.types[i];
            match (&mut self.builders[i], *value) {
                (_, ValueRef::Null) => append_null(&mut self.builders[i]),
                (ColumnBuilder::Int64(b), ValueRef::Integer(v)) => b.append_value(v),
                (ColumnBuilder::Int64(b), v @ ValueRef::Text(_)) => {
                    let parsed = text_of(column, expected, v)?
                        .trim()
                        .parse::<i64>()
                        .map_err(|_| mismatch(column, expected, v))?;
                    b.append_value(parsed)
                }
                (ColumnBuilder::Float64(b), ValueRef::Real(v)) => b.append_value(v),
                (ColumnBuilder::Float64(b), ValueRef::Integer(v)) => b.append_value(v as f64),
                (ColumnBuilder::Float64(b), v @ ValueRef::Text(_)) => {
                    let parsed = text_of(column, expected, v)?
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| mismatch(column, expected, v))?;
                    b.append_value(parsed)
                }
                (ColumnBuilder::Utf8(b), v @ ValueRef::Text(_)) => {
                    b.append_value(text_of(column, expected, v)?)
                }
                (ColumnBuilder::Utf8(b), ValueRef::Integer(v)) => b.append_value(v.to_string()),
                (ColumnBuilder::Utf8(b), ValueRef::Real(v)) => b.append_value(v.to_string()),
                (ColumnBuilder::Binary(b), ValueRef::Blob(v)) => b.append_value(v),
                (ColumnBuilder::Binary(b), ValueRef::Text(v)) => b.append_value(v),
                (ColumnBuilder::Boolean(b), ValueRef::Integer(v)) => b.append_value(v != 0),
                (_, v) => return Err(mismatch(column, expected, v)),
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Produce a batch from the pending rows and reset.
    pub fn flush(&mut self) -> Result<RecordBatch, EngineError> {
        let columns = self
            .builders
            .iter_mut()
            .map(|b| b.finish())
            .collect::<Vec<_>>();
        self.rows = 0;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

fn append_null(builder: &mut ColumnBuilder) {
    match builder {
        ColumnBuilder::Int64(b) => b.append_null(),
        ColumnBuilder::Float64(b) => b.append_null(),
        ColumnBuilder::Utf8(b) => b.append_null(),
        ColumnBuilder::Binary(b) => b.append_null(),
        ColumnBuilder::Boolean(b) => b.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_core::ENGINE_BATCH_ROWS;
    use arrow_array::{Array, Int64Array, StringArray};

    #[test]
    fn affinity_mapping_table() {
        let cases = [
            ("INTEGER", ColumnType::Int64),
            ("int", ColumnType::Int64),
            ("BIGINT", ColumnType::Int64),
            ("UNSIGNED BIG INT", ColumnType::Int64),
            ("VARCHAR(255)", ColumnType::Utf8),
            ("TEXT", ColumnType::Utf8),
            ("CLOB", ColumnType::Utf8),
            ("CHARACTER(20)", ColumnType::Utf8),
            ("BLOB", ColumnType::Binary),
            ("", ColumnType::Binary),
            ("REAL", ColumnType::Float64),
            ("DOUBLE PRECISION", ColumnType::Float64),
            ("FLOAT", ColumnType::Float64),
            ("NUMERIC", ColumnType::Float64),
            ("DECIMAL(10,5)", ColumnType::Float64),
            ("BOOLEAN", ColumnType::Boolean),
            ("DATE", ColumnType::Utf8),
            ("DATETIME", ColumnType::Utf8),
            ("TIMESTAMP", ColumnType::Utf8),
        ];
        for (decl, expected) in cases {
            assert_eq!(affinity_type(decl), expected, "declared type {decl:?}");
        }
    }

    #[test]
    fn row_batcher_flushes_at_batch_size() {
        let mut batcher = RowBatcher::new(vec!["id".into()], vec![ColumnType::Int64]);
        for i in 0..ENGINE_BATCH_ROWS as i64 {
            batcher.push_row(&[ValueRef::Integer(i)]).unwrap();
        }
        assert_eq!(batcher.len(), ENGINE_BATCH_ROWS);
        let batch = batcher.flush().unwrap();
        assert_eq!(batch.num_rows(), ENGINE_BATCH_ROWS);
        assert!(batcher.is_empty());
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert_eq!(
            ids.value(ENGINE_BATCH_ROWS - 1),
            ENGINE_BATCH_ROWS as i64 - 1
        );
    }

    #[test]
    fn dynamic_text_in_int_column_is_type_mismatch() {
        let mut batcher = RowBatcher::new(vec!["n".into()], vec![ColumnType::Int64]);
        // Numeric text is accepted (SQLite would have stored it as INTEGER anyway).
        batcher.push_row(&[ValueRef::Text(b"42")]).unwrap();
        let err = batcher
            .push_row(&[ValueRef::Text(b"forty-two")])
            .unwrap_err();
        match err {
            EngineError::TypeMismatch {
                column,
                expected,
                actual,
            } => {
                assert_eq!(column, "n");
                assert_eq!(expected, "Int64");
                assert_eq!(actual, "TEXT");
            }
            other => panic!("unexpected error {other:?}"),
        }
        // A REAL in an INTEGER column is not silently truncated.
        assert!(matches!(
            batcher.push_row(&[ValueRef::Real(1.5)]),
            Err(EngineError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn null_only_column_defaults_utf8_and_nulls_survive() {
        assert_eq!(infer_from_value(ValueRef::Null), None);
        assert_eq!(
            infer_from_value(ValueRef::Integer(1)),
            Some(ColumnType::Int64)
        );

        let mut batcher = RowBatcher::new(
            vec!["a".into(), "b".into()],
            vec![ColumnType::Utf8, ColumnType::Int64],
        );
        batcher
            .push_row(&[ValueRef::Text(b"x"), ValueRef::Null])
            .unwrap();
        batcher
            .push_row(&[ValueRef::Null, ValueRef::Integer(7)])
            .unwrap();
        let batch = batcher.flush().unwrap();
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), "x");
        assert!(a.is_null(1));
        assert!(b.is_null(0));
        assert_eq!(b.value(1), 7);
        assert!(batch.schema().field(0).is_nullable());
    }
}
