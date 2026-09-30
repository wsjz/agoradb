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

//! Arrow ↔ DuckDB type names.

use agoradb_core::EngineKind;
use agoradb_engine::EngineError;
use arrow_schema::{DataType, TimeUnit};

/// The DuckDB type name to use in DDL for an Arrow [`DataType`].
pub fn arrow_to_duckdb_type(dt: &DataType) -> Result<String, EngineError> {
    let name = match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "UTINYINT".to_string(),
        DataType::UInt16 => "USMALLINT".to_string(),
        DataType::UInt32 => "UINTEGER".to_string(),
        DataType::UInt64 => "UBIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BLOB".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time64(TimeUnit::Microsecond) => "TIME".to_string(),
        DataType::Timestamp(unit, tz) => {
            let base = match unit {
                TimeUnit::Second => "TIMESTAMP_S",
                TimeUnit::Millisecond => "TIMESTAMP_MS",
                TimeUnit::Microsecond => "TIMESTAMP",
                TimeUnit::Nanosecond => "TIMESTAMP_NS",
            };
            if tz.is_some() {
                "TIMESTAMPTZ".to_string()
            } else {
                base.to_string()
            }
        }
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        other => {
            return Err(EngineError::Unsupported(
                EngineKind::DuckDb,
                format!("arrow type {other} has no DuckDB DDL equivalent"),
            ))
        }
    };
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrow_to_duckdb_type_table() {
        let cases = [
            (DataType::Int64, "BIGINT"),
            (DataType::Int32, "INTEGER"),
            (DataType::Float64, "DOUBLE"),
            (DataType::Float32, "FLOAT"),
            (DataType::Utf8, "VARCHAR"),
            (DataType::Boolean, "BOOLEAN"),
            (DataType::Date32, "DATE"),
            (DataType::Binary, "BLOB"),
            (DataType::Decimal128(15, 2), "DECIMAL(15,2)"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "TIMESTAMP",
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "TIMESTAMPTZ",
            ),
        ];
        for (dt, expected) in cases {
            assert_eq!(arrow_to_duckdb_type(&dt).unwrap(), expected, "{dt}");
        }
        assert!(matches!(
            arrow_to_duckdb_type(&DataType::Null),
            Err(EngineError::Unsupported(EngineKind::DuckDb, _))
        ));
    }
}
