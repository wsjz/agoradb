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

//! [`SQLExecutor`] over any [`QueryEngine`]: the single bridge between
//! `datafusion-federation` and AgoraDB engines.

use std::sync::Arc;

use agoradb_engine::{QualifiedName, QueryEngine, SqlDialect};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{conjunction_opt, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect, DuckDBDialect, SqliteDialect};
use datafusion_federation::sql::SQLExecutor;
use futures::TryStreamExt;

use crate::error::engine_to_df;

/// Runs SQL pushed down by DataFusion on one Space's engine.
///
/// Two executors with the same `compute_context` (engine instance id) are
/// treated by `datafusion-federation` as the same database, so tables of
/// every analytical Space served by the shared DuckDB instance can be joined
/// in one pushed-down statement.
pub struct EngineSqlExecutor {
    space: String,
    engine: Arc<dyn QueryEngine>,
}

impl EngineSqlExecutor {
    /// Create an executor for `space` backed by `engine`.
    pub fn new(space: impl Into<String>, engine: Arc<dyn QueryEngine>) -> Self {
        Self {
            space: space.into(),
            engine,
        }
    }

    /// The Space this executor serves.
    pub fn space(&self) -> &str {
        &self.space
    }
}

/// Cast `batch` column-by-column to `schema` (engines may return e.g. a
/// different timestamp unit or nullability than the Iceberg-derived schema).
pub fn conform(batch: RecordBatch, schema: &SchemaRef) -> DfResult<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "engine returned {} columns, federation expected {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    if batch.schema().as_ref() == schema.as_ref() {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(col, field)| -> DfResult<ArrayRef> {
            if col.data_type() == field.data_type() {
                Ok(col.clone())
            } else {
                Ok(arrow_cast::cast(col, field.data_type())?)
            }
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Keep only the rows of `batch` for which `predicate` is true.
fn apply_predicate(batch: &RecordBatch, predicate: &dyn PhysicalExpr) -> DfResult<RecordBatch> {
    let mask = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
    let mask = mask.as_boolean_opt().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "filter predicate evaluated to {} instead of boolean",
            mask.data_type()
        ))
    })?;
    Ok(filter_record_batch(batch, mask)?)
}

#[async_trait]
impl SQLExecutor for EngineSqlExecutor {
    fn name(&self) -> &str {
        &self.space
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.engine.instance_id().to_string())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        match self.engine.dialect() {
            SqlDialect::DuckDb => Arc::new(DuckDBDialect::new()),
            SqlDialect::Sqlite => Arc::new(SqliteDialect {}),
            SqlDialect::Generic => Arc::new(DefaultDialect {}),
        }
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> DfResult<SendableRecordBatchStream> {
        let engine = Arc::clone(&self.engine);
        let sql = query.to_string();
        let target = schema.clone();
        // DataFusion's physical filter pushdown may hand us predicates that
        // are not part of `query` (`VirtualExecutionPlan` accepts them all).
        // They must be applied here or rows would silently leak through.
        let predicate = conjunction_opt(filters.iter().cloned());
        tracing::debug!(space = %self.space, %sql, residual_filters = filters.len(), "federation pushdown");
        let stream = futures::stream::once(async move {
            engine
                .query(&sql, &[])
                .await
                .map(|(_, stream)| stream.map_err(engine_to_df))
                .map_err(engine_to_df)
        })
        .try_flatten()
        .and_then(move |batch| {
            let target = target.clone();
            let predicate = predicate.clone();
            async move {
                let batch = conform(batch, &target)?;
                match predicate {
                    Some(p) => apply_predicate(&batch, p.as_ref()),
                    None => Ok(batch),
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DfResult<Vec<String>> {
        self.engine
            .table_names(&self.space)
            .await
            .map_err(engine_to_df)
    }

    async fn get_table_schema(&self, table_name: &str) -> DfResult<SchemaRef> {
        // `table_name` arrives quoted (`"space"."table"`); the last segment is the table.
        let table = table_name
            .rsplit('.')
            .next()
            .unwrap_or(table_name)
            .trim_matches('"')
            .to_string();
        self.engine
            .table_schema(&QualifiedName::new(&self.space, table))
            .await
            .map_err(engine_to_df)
    }
}
