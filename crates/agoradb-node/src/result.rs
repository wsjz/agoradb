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

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// Outcome of [`AgoraSession::sql`](crate::AgoraSession::sql).
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// A result set.
    Batches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    /// A DML statement and the number of rows it touched.
    RowsAffected(u64),
    /// A statement without a result (DDL, TCL, `SET SPACE`).
    Empty,
}

impl QueryResult {
    /// Total number of result rows (0 for non-queries).
    pub fn num_rows(&self) -> usize {
        match self {
            QueryResult::Batches { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
            _ => 0,
        }
    }

    /// The result batches, empty for non-queries.
    pub fn batches(&self) -> &[RecordBatch] {
        match self {
            QueryResult::Batches { batches, .. } => batches,
            _ => &[],
        }
    }

    /// The result schema, if this is a result set.
    pub fn schema(&self) -> Option<SchemaRef> {
        match self {
            QueryResult::Batches { schema, .. } => Some(schema.clone()),
            _ => None,
        }
    }

    /// Rows affected by a DML statement, `None` otherwise.
    pub fn rows_affected(&self) -> Option<u64> {
        match self {
            QueryResult::RowsAffected(n) => Some(*n),
            _ => None,
        }
    }
}
