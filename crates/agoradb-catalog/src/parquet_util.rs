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

use agoradb_core::{CatalogError, Morsel};

/// Read a single morsel (Parquet file, or row range within it).
///
/// Returns a vector of RecordBatches — one per row group in the file.
/// If the morsel specifies `row_count > 0`, only rows within the
/// `[row_start, row_start + row_count)` range are returned.
pub async fn read_morsel(
    morsel: &Morsel,
) -> std::result::Result<Vec<arrow_array::RecordBatch>, CatalogError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(&morsel.file_path).map_err(|e| {
        CatalogError::Iceberg(format!(
            "Failed to open parquet file {}: {e}",
            morsel.file_path
        ))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        CatalogError::Iceberg(format!("Failed to read parquet {}: {e}", morsel.file_path))
    })?;

    let mut reader = builder.build().map_err(|e| {
        CatalogError::Iceberg(format!(
            "Failed to build parquet reader for {}: {e}",
            morsel.file_path
        ))
    })?;

    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|e| CatalogError::Iceberg(format!("Parquet read error: {e}")))?;
        batches.push(batch);
    }

    // If morsel specifies a row range, slice the batches accordingly.
    if morsel.row_count > 0 {
        batches = slice_batches(batches, morsel.row_start, morsel.row_count)?;
    }

    Ok(batches)
}

/// Read the Parquet file footer and return the total number of rows.
pub fn parquet_row_count(path: &str) -> std::result::Result<usize, CatalogError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)
        .map_err(|e| CatalogError::Iceberg(format!("Failed to open {path}: {e}")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        CatalogError::Iceberg(format!("Failed to read parquet footer for {path}: {e}"))
    })?;
    let metadata = builder.metadata();
    Ok(metadata.file_metadata().num_rows() as usize)
}

/// Slice a vector of batches to include only rows in [start, start + count).
pub fn slice_batches(
    batches: Vec<arrow_array::RecordBatch>,
    start: usize,
    count: usize,
) -> std::result::Result<Vec<arrow_array::RecordBatch>, CatalogError> {
    let mut result = Vec::new();
    let mut current_row = 0usize;
    let end = start + count;

    for batch in batches {
        let batch_len = batch.num_rows();
        let batch_start = current_row;
        let batch_end = current_row + batch_len;

        if batch_end <= start || batch_start >= end {
            // This batch is entirely outside the requested range.
            current_row += batch_len;
            continue;
        }

        let slice_start = start.saturating_sub(batch_start);
        let slice_end = (end - batch_start).min(batch_len);
        let slice_len = slice_end - slice_start;

        if slice_len == batch_len {
            result.push(batch);
        } else {
            let sliced = batch.slice(slice_start, slice_len);
            result.push(sliced);
        }

        current_row += batch_len;

        if current_row >= end {
            break;
        }
    }

    Ok(result)
}
