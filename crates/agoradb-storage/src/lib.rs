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
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::{Catalog, TableIdent};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;

pub mod buffer;
pub mod compaction;

use buffer::AppendBuffer;

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

        // 2. Write to a local temporary Parquet file.
        let temp_file_path = self
            .temp_dir
            .join(format!("{}.parquet", uuid::Uuid::new_v4()));
        let temp_file = std::fs::File::create(&temp_file_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
            ))
            .build();

        let mut writer = ArrowWriter::try_new(temp_file, self.buffer.schema.clone(), Some(props))
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        writer
            .write(&merged)
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        let _file_metadata = writer
            .close()
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        let file_size = std::fs::metadata(&temp_file_path)?.len() as u64;

        // 3. Load the table so the data file lands under its own location.
        let table = self
            .catalog
            .load_table(&self.table)
            .await
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        let data_dir = format!("{}/data", table.metadata().location());
        let target_path = format!("{}/{}.parquet", data_dir, uuid::Uuid::new_v4());

        let output = self
            .file_io
            .new_output(&target_path)
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        let data = tokio::fs::read(&temp_file_path).await?;
        output
            .write(Bytes::from(data))
            .await
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;
        tokio::fs::remove_file(&temp_file_path).await?;

        // 4. Build DataFile.
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(target_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(merged.num_rows() as u64)
            .file_size_in_bytes(file_size)
            .build()
            .map_err(|e| StorageError::FlushFailed(e.to_string()))?;

        // 5. Iceberg Transaction Commit.
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

        // 6. Clear the buffer.
        self.buffer.clear();

        Ok(())
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }
}
