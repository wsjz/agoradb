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

//! `INSERT` into an analytical Space: evaluate the source on DuckDB, cast the
//! batches to the table schema and append them through the Parquet write path.

use std::path::Path;
use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, Space};
use agoradb_engine::QueryEngine;
use agoradb_storage::StorageEngine;
use arrow_array::{new_null_array, ArrayRef, RecordBatch};
use arrow_cast::cast;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use sqlparser::ast::{Insert, TableObject};

use crate::ddl::table_name;
use crate::error::SessionError;

/// Rearrange and cast `batch` (columns named by `insert_columns`, or in
/// table order when empty) to `target`.
fn conform_batch(
    batch: &RecordBatch,
    insert_columns: &[String],
    target: &SchemaRef,
) -> Result<RecordBatch, SessionError> {
    let columns = target
        .fields()
        .iter()
        .map(|field| {
            let source_index = if insert_columns.is_empty() {
                let idx = target.index_of(field.name())?;
                (idx < batch.num_columns()).then_some(idx)
            } else {
                insert_columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(field.name()))
            };
            let array: ArrayRef = match source_index {
                Some(i) => cast(batch.column(i), field.data_type())?,
                None if field.is_nullable() => new_null_array(field.data_type(), batch.num_rows()),
                None => {
                    return Err(SessionError::Unsupported(format!(
                        "INSERT does not provide a value for non-null column '{}'",
                        field.name()
                    )))
                }
            };
            if !field.is_nullable() && array.null_count() > 0 {
                return Err(SessionError::Unsupported(format!(
                    "NULL value for non-null column '{}'",
                    field.name()
                )));
            }
            Ok(array)
        })
        .collect::<Result<Vec<_>, SessionError>>()?;
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}

/// Run an `INSERT` whose target is an analytical table.
///
/// `source_engine` must already have every table referenced by the source
/// query attached.
pub async fn insert_analytical(
    catalog: &AgoraCatalog,
    space: &Space,
    insert: &Insert,
    source_engine: &Arc<dyn QueryEngine>,
    temp_dir: &Path,
) -> Result<u64, SessionError> {
    let TableObject::TableName(target_name) = &insert.table else {
        return Err(SessionError::Unsupported(format!(
            "INSERT INTO {}",
            insert.table
        )));
    };
    let table = table_name(target_name);
    let source = insert.source.as_ref().ok_or_else(|| {
        SessionError::Unsupported("INSERT without a VALUES or SELECT source".to_string())
    })?;
    let insert_columns: Vec<String> = insert.columns.iter().map(table_name).collect();

    let resolved = catalog.resolve_table(space, &table, None).await?;
    let target_schema = resolved.schema.clone();
    if insert_columns.is_empty() {
        // Column count must match when no column list is given.
        let (_, first) = source_engine.query(&source.to_string(), &[]).await?;
        let batches: Vec<RecordBatch> = first.try_collect().await?;
        return write_batches(
            catalog,
            &resolved.ident,
            &target_schema,
            &insert_columns,
            batches,
            temp_dir,
        )
        .await;
    }

    let (_, stream) = source_engine.query(&source.to_string(), &[]).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    write_batches(
        catalog,
        &resolved.ident,
        &target_schema,
        &insert_columns,
        batches,
        temp_dir,
    )
    .await
}

async fn write_batches(
    catalog: &AgoraCatalog,
    ident: &iceberg::TableIdent,
    target_schema: &SchemaRef,
    insert_columns: &[String],
    batches: Vec<RecordBatch>,
    temp_dir: &Path,
) -> Result<u64, SessionError> {
    let mut rows = 0u64;
    let mut writer = StorageEngine::new(
        Arc::new(catalog.clone()),
        catalog.file_io().clone(),
        ident.clone(),
        target_schema.clone(),
        temp_dir.to_path_buf(),
    );
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        if insert_columns.is_empty() && batch.num_columns() != target_schema.fields().len() {
            return Err(SessionError::Unsupported(format!(
                "INSERT provides {} columns but the table has {}",
                batch.num_columns(),
                target_schema.fields().len()
            )));
        }
        let conformed = conform_batch(batch, insert_columns, target_schema)?;
        rows += conformed.num_rows() as u64;
        writer.append(conformed).await?;
    }
    writer.flush().await?;
    Ok(rows)
}
