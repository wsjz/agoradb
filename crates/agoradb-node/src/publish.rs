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

//! `PUBLISH SPACE`: snapshot a transactional (SQLite) Space into an
//! analytical (Iceberg + Parquet) Space (architecture v3 §7.3).
//!
//! Every table is read inside one SQLite read transaction on a private
//! connection, so all tables of a publication reflect the same committed
//! state: uncommitted writes are never published, open transactions do not
//! block the publish, and the node's shared connection is left untouched.
//! Each table then gets one new Iceberg snapshot that *replaces* its
//! data, so the published Space is always an exact copy as of the publish and
//! older publications stay readable through their snapshots. Commits are
//! atomic per table, not across tables.

use std::sync::Arc;
use std::time::Duration;

use agoradb_catalog::Space;
use agoradb_core::{AccessMode, CatalogError, CreateSpaceRequest, SpaceKind};
use agoradb_engine::{QualifiedName, QueryEngine};
use agoradb_semantic::rewrite::quote_ident;
use agoradb_semantic::PublishRequest;
use agoradb_storage::write_data_file;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::spec::{DataFile, Schema as IcebergSchema};
use iceberg::{Catalog, TableCreation, TableIdent};
use tokio::task::JoinHandle;

use crate::error::SessionError;
use crate::result::QueryResult;
use crate::session::AgoraSession;

/// Rows per Parquet file written by a publish.
pub const PUBLISH_FILE_ROWS: usize = 128 * 1024;

/// Outcome of publishing one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTable {
    pub table: String,
    pub rows: u64,
    /// The Iceberg snapshot holding this publication of the table.
    pub snapshot_id: Option<i64>,
}

/// Outcome of a `PUBLISH SPACE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReport {
    pub source: String,
    pub target: String,
    pub tables: Vec<PublishedTable>,
}

impl PublishReport {
    /// The report as a result set: `(table, rows, snapshot_id)`.
    pub fn into_result(self) -> Result<QueryResult, SessionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("snapshot_id", DataType::Int64, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                self.tables.iter().map(|t| t.table.clone()),
            )),
            Arc::new(Int64Array::from_iter_values(
                self.tables.iter().map(|t| t.rows as i64),
            )),
            Arc::new(Int64Array::from_iter(
                self.tables.iter().map(|t| t.snapshot_id),
            )),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        Ok(QueryResult::Batches {
            schema,
            batches: vec![batch],
        })
    }
}

/// Field names, types and nullability, ignoring field ids.
fn same_shape(a: &IcebergSchema, b: &IcebergSchema) -> bool {
    let shape = |s: &IcebergSchema| {
        s.as_struct()
            .fields()
            .iter()
            .map(|f| (f.name.clone(), (*f.field_type).clone(), f.required))
            .collect::<Vec<_>>()
    };
    shape(a) == shape(b)
}

/// Cast `batch` column by column to `target` (e.g. `Binary` → `LargeBinary`).
fn conform(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, SessionError> {
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(col, field)| arrow_cast::cast(col, field.data_type()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}

/// One table staged for publication.
struct Staged {
    table: String,
    ident: TableIdent,
    location: String,
    schema: SchemaRef,
}

impl AgoraSession {
    /// Execute a `PUBLISH SPACE` request.
    pub async fn publish(&self, request: &PublishRequest) -> Result<PublishReport, SessionError> {
        if let Some(p) = self.principal() {
            return Err(SessionError::PermissionDenied(format!(
                "principal '{p}' is read-only and cannot run PUBLISH SPACE"
            )));
        }
        let source = self.space(&request.space)?;
        if source.kind != SpaceKind::Transactional {
            return Err(SessionError::Unsupported(format!(
                "PUBLISH SPACE {}: only transactional spaces are published; \
                 analytical spaces already are Iceberg snapshots",
                source.name
            )));
        }
        let target_name = request
            .target
            .clone()
            .unwrap_or_else(|| format!("{}_published", source.name));
        let target = self.publication_target(&source, &target_name).await?;
        let namespace = self.catalog.space_namespace(&target)?;

        let engine = self.engines().sqlite_private_for(&source)?;
        let all_tables = engine.table_names(&source.name).await?;
        let selected = match &request.tables {
            None => all_tables.clone(),
            Some(tables) => {
                for t in tables {
                    if !all_tables.contains(t) {
                        return Err(
                            CatalogError::TableNotFound(format!("{}.{t}", source.name)).into()
                        );
                    }
                }
                tables.clone()
            }
        };

        // Make sure every target table exists with the source's current schema.
        let mut staged = Vec::with_capacity(selected.len());
        for table in &selected {
            let arrow = engine
                .table_schema(&QualifiedName::new(&source.name, table))
                .await?;
            staged.push(self.stage_table(&target, &namespace, table, &arrow).await?);
        }

        // Read everything inside one read transaction, writing Parquet files
        // as batches arrive. The transaction is always ended, even on error.
        let tx = engine.begin().await?;
        let written = self.write_tables(&engine, &source.name, &staged).await;
        engine.rollback(tx).await?;
        let written = written?;

        let mut report = PublishReport {
            source: source.name.clone(),
            target: target.name.clone(),
            tables: Vec::with_capacity(staged.len()),
        };
        for (stage, (files, rows)) in staged.iter().zip(written) {
            let table = self.catalog.replace_data_files(&stage.ident, files).await?;
            report.tables.push(PublishedTable {
                table: stage.table.clone(),
                rows,
                snapshot_id: table.metadata().current_snapshot_id(),
            });
        }

        // A full publish mirrors the source: drop tables it no longer has.
        if request.tables.is_none() {
            for existing in self.catalog.space_tables(&target).await? {
                if !all_tables.contains(&existing) {
                    self.catalog
                        .drop_table(&TableIdent::new(namespace.clone(), existing))
                        .await
                        .map_err(|e| SessionError::Iceberg(e.to_string()))?;
                }
            }
        }
        // Rebind so queries on this node see the new snapshots right away.
        self.bind(&target).await?;
        tracing::info!(source = %report.source, target = %report.target, tables = report.tables.len(), "published");
        Ok(report)
    }

    /// Find or create the analytical Space `name` that `source` publishes into.
    async fn publication_target(&self, source: &Space, name: &str) -> Result<Space, SessionError> {
        match self.catalog.get_space(name) {
            Ok(space) if space.published_from.as_deref() == Some(source.name.as_str()) => Ok(space),
            Ok(_) => Err(SessionError::AlreadyExists(format!(
                "space '{name}' exists and is not published from '{}'",
                source.name
            ))),
            Err(CatalogError::SpaceNotFound(_)) => {
                let mut request = CreateSpaceRequest::new(name);
                request.kind = Some(SpaceKind::Analytical);
                // Re-publishing after DROP SPACE rebinds the kept Location.
                if self.catalog.get_location(name).is_ok() {
                    request.location = Some(name.to_string());
                    request.access = Some(AccessMode::Writable);
                }
                self.catalog.create_space(request).await?;
                Ok(self.catalog.mark_published(name, &source.name)?)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Create (or recreate, if the source schema changed) one target table.
    async fn stage_table(
        &self,
        target: &Space,
        namespace: &iceberg::NamespaceIdent,
        table: &str,
        source_schema: &SchemaRef,
    ) -> Result<Staged, SessionError> {
        let iceberg_err = |e: iceberg::Error| SessionError::Iceberg(e.to_string());
        let desired = arrow_schema_to_schema_auto_assign_ids(source_schema).map_err(iceberg_err)?;
        let ident = TableIdent::new(namespace.clone(), table.to_string());
        if self
            .catalog
            .table_exists(&ident)
            .await
            .map_err(iceberg_err)?
        {
            let existing = self.catalog.load_table(&ident).await.map_err(iceberg_err)?;
            if !same_shape(existing.metadata().current_schema(), &desired) {
                tracing::info!(table = %ident, "source schema changed, recreating published table");
                self.catalog.drop_table(&ident).await.map_err(iceberg_err)?;
            }
        }
        if !self
            .catalog
            .table_exists(&ident)
            .await
            .map_err(iceberg_err)?
        {
            self.catalog
                .create_table(
                    namespace,
                    TableCreation::builder()
                        .name(table.to_string())
                        .schema(desired)
                        .build(),
                )
                .await
                .map_err(iceberg_err)?;
        }
        let resolved = self.catalog.resolve_table(target, table, None).await?;
        let location = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(iceberg_err)?
            .metadata()
            .location()
            .to_string();
        Ok(Staged {
            table: table.to_string(),
            ident,
            location,
            schema: resolved.schema,
        })
    }

    /// Stream every staged table out of the source engine into Parquet files.
    async fn write_tables(
        &self,
        engine: &Arc<dyn QueryEngine>,
        source: &str,
        staged: &[Staged],
    ) -> Result<Vec<(Vec<DataFile>, u64)>, SessionError> {
        let temp_dir = self.temp_dir();
        let mut out = Vec::with_capacity(staged.len());
        for stage in staged {
            let sql = format!(
                "SELECT * FROM {}.{}",
                quote_ident(source),
                quote_ident(&stage.table)
            );
            let (_, mut stream) = engine.query(&sql, &[]).await?;
            let mut files = Vec::new();
            let mut rows = 0u64;
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows = 0usize;
            loop {
                let next = stream.next().await.transpose()?;
                if let Some(batch) = &next {
                    pending_rows += batch.num_rows();
                    pending.push(conform(batch, &stage.schema)?);
                }
                let done = next.is_none();
                if (done && pending_rows > 0) || pending_rows >= PUBLISH_FILE_ROWS {
                    let merged = arrow_select::concat::concat_batches(&stage.schema, &pending)?;
                    files.push(
                        write_data_file(
                            self.catalog.file_io(),
                            &stage.location,
                            &stage.schema,
                            &merged,
                            temp_dir,
                        )
                        .await?,
                    );
                    rows += pending_rows as u64;
                    pending.clear();
                    pending_rows = 0;
                }
                if done {
                    break;
                }
            }
            out.push((files, rows));
        }
        Ok(out)
    }
}

/// Publish `request` every `every` on a background task, starting now.
///
/// Failures (for example a transaction open on the source Space at that
/// moment) are logged and retried on the next tick. Abort the returned
/// handle to stop.
pub fn spawn_publisher(
    session: Arc<AgoraSession>,
    request: PublishRequest,
    every: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(e) = session.publish(&request).await {
                tracing::warn!(space = %request.space, error = %e, "scheduled publish failed");
            }
        }
    })
}
