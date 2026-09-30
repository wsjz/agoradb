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
use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::CompactionError;
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::{Catalog, TableIdent};

use crate::datafile::write_data_file;

/// Rewrites a table's data files into one file.
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
    ///
    /// `temp_dir` must be a local directory used to stage merged Parquet files.
    pub fn new(catalog: Arc<AgoraCatalog>, temp_dir: PathBuf) -> Self {
        Self {
            catalog,
            target_file_size: Self::DEFAULT_TARGET_FILE_SIZE,
            temp_dir,
        }
    }

    /// Merge every data file of the current snapshot into one file and commit
    /// it as a replacement, so the row count is unchanged. Older snapshots
    /// still reference the original files.
    pub async fn compact_table(&self, table_ident: &TableIdent) -> Result<(), CompactionError> {
        let failed = |e: &dyn std::fmt::Display| CompactionError::CompactionFailed(e.to_string());

        let table = self
            .catalog
            .load_table(table_ident)
            .await
            .map_err(|e| failed(&e))?;
        let snapshot_id = table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| CompactionError::CompactionFailed("No snapshots".to_string()))?;

        let batches: Vec<RecordBatch> = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_row_selection_enabled(true)
            .build()
            .map_err(|e| failed(&e))?
            .to_arrow()
            .await
            .map_err(|e| failed(&e))?
            .try_collect()
            .await
            .map_err(|e| failed(&e))?;
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Err(CompactionError::NoDataFiles);
        }

        let schema = batches[0].schema();
        let merged =
            arrow_select::concat::concat_batches(&schema, &batches).map_err(|e| failed(&e))?;
        let data_file = write_data_file(
            self.catalog.file_io(),
            table.metadata().location(),
            &schema,
            &merged,
            &self.temp_dir,
        )
        .await
        .map_err(|e| failed(&e))?;

        self.catalog
            .replace_data_files(table_ident, vec![data_file])
            .await?;
        Ok(())
    }
}
