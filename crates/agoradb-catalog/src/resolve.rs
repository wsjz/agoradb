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

//! Resolve an analytical table to the Parquet files of one Iceberg snapshot,
//! which is what an engine needs to expose it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use agoradb_core::CatalogError;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use iceberg::arrow::{schema_to_arrow_schema, strip_metadata_from_schema};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::catalog::AgoraCatalog;
use crate::space::Space;

/// An analytical table pinned to one snapshot.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub ident: TableIdent,
    /// `None` for a table that has no snapshot yet (no files).
    pub snapshot_id: Option<i64>,
    /// Absolute local paths of the snapshot's data files.
    pub files: Vec<PathBuf>,
    /// Arrow schema of the snapshot (field metadata stripped).
    pub schema: SchemaRef,
}

fn iceberg_err(e: iceberg::Error) -> CatalogError {
    CatalogError::Iceberg(e.to_string())
}

fn local_path(file_path: &str) -> PathBuf {
    PathBuf::from(file_path.strip_prefix("file://").unwrap_or(file_path))
}

impl AgoraCatalog {
    /// Names of the tables in an analytical Space, sorted.
    pub async fn space_tables(&self, space: &Space) -> Result<Vec<String>, CatalogError> {
        let ns = self.space_namespace(space)?;
        let mut names: Vec<String> = self
            .list_tables(&ns)
            .await
            .map_err(iceberg_err)?
            .into_iter()
            .map(|t| t.name().to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    /// Load a table of an analytical Space, mapping a missing table to
    /// [`CatalogError::TableNotFound`].
    pub async fn load_space_table(
        &self,
        space: &Space,
        table: &str,
    ) -> Result<Table, CatalogError> {
        let ident = TableIdent::new(self.space_namespace(space)?, table.to_string());
        if !self.table_exists(&ident).await.map_err(iceberg_err)? {
            return Err(CatalogError::TableNotFound(format!(
                "{}.{}",
                space.name, table
            )));
        }
        self.load_table(&ident).await.map_err(iceberg_err)
    }

    /// Current snapshot id of every table in `space`, for pinning a federated query.
    pub async fn current_snapshot_ids(
        &self,
        space: &Space,
    ) -> Result<BTreeMap<String, Option<i64>>, CatalogError> {
        let mut out = BTreeMap::new();
        for name in self.space_tables(space).await? {
            let table = self.load_space_table(space, &name).await?;
            out.insert(name, table.metadata().current_snapshot_id());
        }
        Ok(out)
    }

    /// Resolve `space.table` at `pin` (or its current snapshot) to data files + schema.
    pub async fn resolve_table(
        &self,
        space: &Space,
        table: &str,
        pin: Option<i64>,
    ) -> Result<ResolvedTable, CatalogError> {
        let tbl = self.load_space_table(space, table).await?;
        let metadata = tbl.metadata();
        let snapshot_id = pin.or_else(|| metadata.current_snapshot_id());

        let (schema, files) = match snapshot_id {
            None => (metadata.current_schema().clone(), Vec::new()),
            Some(id) => {
                let snapshot = metadata.snapshot_by_id(id).ok_or_else(|| {
                    CatalogError::Iceberg(format!(
                        "snapshot {id} not found in table {}.{}",
                        space.name, table
                    ))
                })?;
                let schema = snapshot.schema(metadata).map_err(iceberg_err)?;
                let scan = tbl.scan().snapshot_id(id).build().map_err(iceberg_err)?;
                let tasks: Vec<_> = scan
                    .plan_files()
                    .await
                    .map_err(iceberg_err)?
                    .try_collect()
                    .await
                    .map_err(iceberg_err)?;
                let mut files: Vec<PathBuf> = tasks
                    .iter()
                    .map(|t| local_path(t.data_file_path()))
                    .collect();
                files.sort();
                files.dedup();
                (schema, files)
            }
        };

        let arrow = schema_to_arrow_schema(&schema).map_err(iceberg_err)?;
        let arrow = strip_metadata_from_schema(&arrow).map_err(iceberg_err)?;
        Ok(ResolvedTable {
            ident: tbl.identifier().clone(),
            snapshot_id,
            files,
            schema: Arc::new(arrow),
        })
    }
}
