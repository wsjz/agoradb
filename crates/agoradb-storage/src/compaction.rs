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

use agoradb_catalog::AgoraCatalog;
use agoradb_core::CompactionError;
use arrow_array::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;

/// Service for compacting small Parquet files into larger ones.
pub struct CompactionService {
    catalog: Arc<AgoraCatalog>,
    #[allow(dead_code)]
    target_file_size: usize,
    temp_dir: PathBuf,
}

impl CompactionService {
    /// Default target file size: 128 MB.
    pub const DEFAULT_TARGET_FILE_SIZE: usize = 128 * 1024 * 1024;

    /// Create a new [`CompactionService`].
    pub fn new(catalog: Arc<AgoraCatalog>, temp_dir: PathBuf) -> Self {
        Self {
            catalog,
            target_file_size: Self::DEFAULT_TARGET_FILE_SIZE,
            temp_dir,
        }
    }

    /// Compact all data files in a table into a single Parquet file.
    pub async fn compact_table(&self, table_ident: &TableIdent) -> Result<(), CompactionError> {
        // 1. Load table.
        let table = self
            .catalog
            .load_table(table_ident)
            .await
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        let snapshot = table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| CompactionError::CompactionFailed("No snapshots".to_string()))?;
        let snapshot_id = snapshot.snapshot_id();

        // 2. Scan all data files via TableScan.
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_row_selection_enabled(true)
            .build()
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        let mut stream = scan
            .to_arrow()
            .await
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(CompactionError::NoDataFiles);
        }

        if batches.len() == 1 && batches[0].num_rows() == 0 {
            return Err(CompactionError::NoDataFiles);
        }

        // 3. Concatenate all batches.
        let schema = batches[0].schema();
        let merged = arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        // 4. Write temp Parquet.
        let temp_path = self
            .temp_dir
            .join(format!("compact_{}.parquet", uuid::Uuid::new_v4()));
        let temp_file = std::fs::File::create(&temp_path)?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
            ))
            .build();
        let mut writer = ArrowWriter::try_new(temp_file, schema.clone(), Some(props))
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        writer
            .write(&merged)
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        let _ = writer
            .close()
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        let file_size = std::fs::metadata(&temp_path)?.len() as u64;

        // 5. Upload via file_io.
        let space_data_dir = format!("{}/data", self.catalog.root_path());
        let target_filename = format!("{}.parquet", uuid::Uuid::new_v4());
        let target_path = format!("{}/{}", space_data_dir, target_filename);

        let output = self
            .catalog
            .file_io()
            .new_output(&target_path)
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        let data = tokio::fs::read(&temp_path).await?;
        output
            .write(Bytes::from(data))
            .await
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        tokio::fs::remove_file(&temp_path).await?;

        // 6. Build DataFile.
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(target_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(merged.num_rows() as u64)
            .file_size_in_bytes(file_size)
            .build()
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        // 7. Iceberg Transaction.
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file])
            .apply(tx)
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;
        action
            .commit(self.catalog.as_ref())
            .await
            .map_err(|e| CompactionError::CompactionFailed(e.to_string()))?;

        Ok(())
    }
}
