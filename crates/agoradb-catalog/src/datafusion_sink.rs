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

//! DataFusion `DataSink` implementation for writing to Iceberg tables.

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::Result as DFResult;
use datafusion_datasource::sink::DataSink;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::catalog::AgoraCatalog;
use agoradb_core::CatalogError;

/// A DataFusion sink that writes Arrow batches into an Iceberg table.
#[derive(Debug)]
pub struct IcebergDataSink {
    catalog: Arc<AgoraCatalog>,
    table_name: String,
    schema: SchemaRef,
}

impl IcebergDataSink {
    /// Create a new sink for the given Iceberg table.
    pub fn new(catalog: Arc<AgoraCatalog>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            catalog,
            table_name,
            schema,
        }
    }
}

impl DisplayAs for IcebergDataSink {
    fn fmt_as(
        &self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "IcebergDataSink(table={}.default.{})",
            self.catalog.root_path(),
            self.table_name
        )
    }
}

#[async_trait]
impl DataSink for IcebergDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let temp_dir =
            std::path::PathBuf::from(self.catalog.root_path()).join("_insert_tmp");
        tokio::fs::create_dir_all(&temp_dir).await.map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(CatalogError::Iceberg(
                format!("Failed to create insert temp dir: {e}"),
            )))
        })?;

        let engine = agoradb_storage::StorageEngine::new_with_catalog(
            Arc::clone(&self.catalog),
            self.catalog.file_io().clone(),
            self.catalog.root_path(),
            Arc::clone(&self.schema),
            temp_dir,
            self.table_name.clone(),
        );

        // StorageEngine::append takes &mut self, so protect with a mutex.
        let engine = Mutex::new(engine);
        let mut total_rows: u64 = 0;

        while let Some(batch_result) = data.next().await {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(CatalogError::Iceberg(
                    format!("Failed to read input batch: {e}"),
                )))
            })?;
            total_rows += batch.num_rows() as u64;

            let mut guard = engine.lock().await;
            guard.append(batch).await.map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(CatalogError::Iceberg(
                    format!("StorageEngine append failed: {e}"),
                )))
            })?;
        }

        // Flush remaining buffered data.
        let mut guard = engine.lock().await;
        guard.flush().await.map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(CatalogError::Iceberg(
                format!("StorageEngine flush failed: {e}"),
            )))
        })?;

        Ok(total_rows)
    }
}
