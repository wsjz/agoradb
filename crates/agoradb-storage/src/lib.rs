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

use agoradb_core::StorageError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg::io::FileIO;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::{Catalog, TableIdent};
use std::path::PathBuf;
use std::sync::Arc;

pub mod buffer;
pub mod compaction;
pub mod datafile;

use buffer::AppendBuffer;
pub use datafile::write_data_file;

/// Append-only Parquet writer for one Iceberg table.
///
/// Batches are buffered in memory, flushed to a Parquet file under the
/// table's own `data/` directory and committed to the table as a new
/// snapshot via an Iceberg fast-append transaction.
pub struct StorageEngine {
    catalog: Arc<dyn Catalog>,
    file_io: FileIO,
    table: TableIdent,
    buffer: AppendBuffer,
    temp_dir: PathBuf,
}

impl StorageEngine {
    /// Create a new [`StorageEngine`] from a concrete catalog implementation.
    pub fn new_with_catalog<C: Catalog + 'static>(
        catalog: Arc<C>,
        file_io: FileIO,
        table: TableIdent,
        schema: SchemaRef,
        temp_dir: PathBuf,
    ) -> Self {
        Self::new(
            catalog as Arc<dyn Catalog>,
            file_io,
            table,
            schema,
            temp_dir,
        )
    }

    /// Create a new [`StorageEngine`] writing to `table`.
    ///
    /// `temp_dir` must be a local directory; Parquet files are staged there
    /// before being uploaded through `file_io`.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        file_io: FileIO,
        table: TableIdent,
        schema: SchemaRef,
        temp_dir: PathBuf,
    ) -> Self {
        Self {
            catalog,
            file_io,
            table,
            buffer: AppendBuffer::new(schema),
            temp_dir,
        }
    }

    /// The table this engine writes to.
    pub fn table(&self) -> &TableIdent {
        &self.table
    }

    /// Append a RecordBatch to the buffer.
    pub async fn append(&mut self, batch: RecordBatch) -> Result<(), StorageError> {
        self.buffer.push(batch);
        if self.buffer.should_flush() {
            self.flush().await?;
        }
        Ok(())
    }

    /// Force flush the buffer to storage.
    pub async fn flush(&mut self) -> Result<(), StorageError> {
        if self.buffer.batches.is_empty() {
            return Ok(());
        }

        // 1. Merge all batches.
        let merged =
            arrow_select::concat::concat_batches(&self.buffer.schema, &self.buffer.batches)
                .map_err(|e| StorageError::FlushFailed(e.to_string()))?;

        // 2. Write the Parquet file under the table's own data directory.
        let table = self
            .catalog
            .load_table(&self.table)
            .await
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        let data_file = write_data_file(
            &self.file_io,
            table.metadata().location(),
            &self.buffer.schema,
            &merged,
            &self.temp_dir,
        )
        .await?;

        // 3. Iceberg Transaction Commit.
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file])
            .apply(tx)
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        action
            .commit(self.catalog.as_ref())
            .await
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;

        // 4. Clear the buffer.
        self.buffer.clear();

        Ok(())
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }
}
