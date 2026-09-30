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

use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// A physical source an engine can be asked to expose as a table.
#[derive(Debug, Clone)]
pub enum TableSource {
    /// A set of Parquet files, typically the data files of one Iceberg snapshot.
    ///
    /// `files` may be empty for a table without a snapshot; the engine must
    /// then expose an empty table with `schema`.
    ParquetFiles {
        files: Vec<PathBuf>,
        schema: SchemaRef,
    },
    /// One table inside a SQLite database file.
    SqliteFile { path: PathBuf, table: String },
    /// In-memory Arrow batches (remote results, temporary tables).
    ArrowBatches {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
}

impl TableSource {
    /// Short human-readable kind, for error messages.
    pub fn kind_name(&self) -> &'static str {
        match self {
            TableSource::ParquetFiles { .. } => "parquet files",
            TableSource::SqliteFile { .. } => "sqlite file",
            TableSource::ArrowBatches { .. } => "arrow batches",
        }
    }
}
