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

//! Atomic replacement of a table's live data (an Iceberg `overwrite`
//! snapshot). iceberg-rust 0.10 only exposes `fast_append`; since
//! [`AgoraCatalog`] is the table's catalog, it can produce the snapshot itself.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use agoradb_core::CatalogError;
use iceberg::spec::{
    DataFile, FormatVersion, ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot,
    Summary, TableMetadataBuilder, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, TableIdent};

use crate::catalog::AgoraCatalog;

/// Commit attempts before giving up on concurrent writers.
const MAX_COMMIT_ATTEMPTS: usize = 5;

fn iceberg_err(e: iceberg::Error) -> CatalogError {
    CatalogError::Iceberg(e.to_string())
}

fn unique_snapshot_id(table: &Table) -> i64 {
    loop {
        let (hi, lo) = uuid::Uuid::new_v4().as_u64_pair();
        let id = ((hi ^ lo) >> 1) as i64;
        if !table.metadata().snapshots().any(|s| s.snapshot_id() == id) {
            return id;
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl AgoraCatalog {
    /// Make `files` the complete live data of `ident` in one new snapshot.
    ///
    /// The previous data files are no longer part of the current snapshot but
    /// stay referenced by older snapshots, so readers pinned to an older
    /// snapshot (and time travel) are unaffected. `files` may be empty, which
    /// empties the table. Concurrent commits are retried.
    pub async fn replace_data_files(
        &self,
        ident: &TableIdent,
        files: Vec<DataFile>,
    ) -> Result<Table, CatalogError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.try_replace_data_files(ident, files.clone()).await {
                Err(e)
                    if e.kind() == ErrorKind::CatalogCommitConflicts
                        && attempt < MAX_COMMIT_ATTEMPTS =>
                {
                    tracing::debug!(table = %ident, attempt, "replace_data_files conflict, retrying");
                }
                other => return other.map_err(iceberg_err),
            }
        }
    }

    async fn try_replace_data_files(
        &self,
        ident: &TableIdent,
        files: Vec<DataFile>,
    ) -> iceberg::Result<Table> {
        let table = self.load_table(ident).await?;
        let metadata = table.metadata();
        if metadata.format_version() != FormatVersion::V2 {
            return Err(iceberg::Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "replace_data_files supports format v2 tables only, {ident} is {:?}",
                    metadata.format_version()
                ),
            ));
        }

        let snapshot_id = unique_snapshot_id(&table);
        let parent = metadata.current_snapshot_id();
        let sequence_number = metadata.next_sequence_number();
        let commit_id = uuid::Uuid::new_v4();
        let metadata_dir = format!("{}/metadata", metadata.location());

        let added_files = files.len();
        let added_records: u64 = files.iter().map(|f| f.record_count()).sum();
        let added_bytes: u64 = files.iter().map(|f| f.file_size_in_bytes()).sum();

        let mut manifests = Vec::new();
        if !files.is_empty() {
            let output = self
                .file_io()
                .new_output(format!("{metadata_dir}/{commit_id}-m0.avro"))?;
            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(snapshot_id),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            for file in files {
                writer.add_file(file, sequence_number)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }

        let manifest_list = format!("{metadata_dir}/snap-{snapshot_id}-0-{commit_id}.avro");
        let mut list_writer = ManifestListWriter::v2(
            self.file_io()
                .new_output(manifest_list.clone())?
                .writer()
                .await?,
            snapshot_id,
            parent,
            sequence_number,
        );
        list_writer.add_manifests(manifests.into_iter())?;
        list_writer.close().await?;

        let summary = Summary {
            operation: Operation::Overwrite,
            additional_properties: HashMap::from([
                ("added-data-files".to_string(), added_files.to_string()),
                ("added-records".to_string(), added_records.to_string()),
                ("added-files-size".to_string(), added_bytes.to_string()),
                ("total-data-files".to_string(), added_files.to_string()),
                ("total-records".to_string(), added_records.to_string()),
                ("replace-partitions".to_string(), "true".to_string()),
            ]),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list)
            .with_summary(summary)
            .with_schema_id(metadata.current_schema_id())
            .build();

        let current_location = table.metadata_location_result()?.to_string();
        let next = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            Some(current_location.clone()),
        )
        .set_branch_snapshot(snapshot, MAIN_BRANCH)?
        .build()?
        .metadata;
        self.write_next_metadata(ident, &current_location, next)
            .await
    }
}
