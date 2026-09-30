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

//! Writing one Parquet data file for an Iceberg table.

use std::path::Path;

use agoradb_core::StorageError;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn failed(e: impl std::fmt::Display) -> StorageError {
    StorageError::FlushFailed(e.to_string())
}

/// Write `batch` as a ZSTD-compressed Parquet file under
/// `<table_location>/data/` and describe it as an Iceberg [`DataFile`].
///
/// The file is staged in the local `temp_dir` and then uploaded through
/// `file_io`. It is not committed: pass the result to a fast-append
/// transaction or to `AgoraCatalog::replace_data_files`.
pub async fn write_data_file(
    file_io: &FileIO,
    table_location: &str,
    schema: &SchemaRef,
    batch: &RecordBatch,
    temp_dir: &Path,
) -> Result<DataFile, StorageError> {
    let temp_path = temp_dir.join(format!("{}.parquet", uuid::Uuid::new_v4()));
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
        ))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&temp_path)?,
        schema.clone(),
        Some(props),
    )
    .map_err(failed)?;
    writer.write(batch).map_err(failed)?;
    writer.close().map_err(failed)?;
    let file_size = std::fs::metadata(&temp_path)?.len();

    let target_path = format!("{table_location}/data/{}.parquet", uuid::Uuid::new_v4());
    let data = tokio::fs::read(&temp_path).await?;
    file_io
        .new_output(&target_path)
        .map_err(failed)?
        .write(Bytes::from(data))
        .await
        .map_err(failed)?;
    tokio::fs::remove_file(&temp_path).await?;

    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(target_path)
        .file_format(DataFileFormat::Parquet)
        .record_count(batch.num_rows() as u64)
        .file_size_in_bytes(file_size)
        .build()
        .map_err(failed)
}
