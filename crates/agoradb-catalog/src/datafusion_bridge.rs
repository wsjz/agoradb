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

//! Bridge Iceberg catalog to DataFusion catalog traits.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;

use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};

use futures::StreamExt;
use iceberg::spec::{PrimitiveType, Type};
use iceberg::Catalog;

use crate::catalog::AgoraCatalog;
use agoradb_core::CatalogError;

// === AgoraCatalogProvider ===

/// Bridges [`AgoraCatalog`] (Iceberg-backed) to DataFusion's [`CatalogProvider`] trait.
///
/// Currently exposes a single schema named `"default"`.
#[derive(Debug)]
pub struct AgoraCatalogProvider {
    inner: Arc<AgoraCatalog>,
    schemas: std::sync::RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl AgoraCatalogProvider {
    /// Create a new [`AgoraCatalogProvider`] wrapping the given [`AgoraCatalog`].
    pub fn new(catalog: Arc<AgoraCatalog>) -> Self {
        Self {
            inner: catalog,
            schemas: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get a clone of the inner [`AgoraCatalog`] Arc.
    pub fn inner_catalog(&self) -> Arc<AgoraCatalog> {
        Arc::clone(&self.inner)
    }
}

impl CatalogProvider for AgoraCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["default".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name != "default" {
            return None;
        }
        {
            let cache = self.schemas.read().unwrap();
            if let Some(provider) = cache.get(name) {
                return Some(Arc::clone(provider));
            }
        }
        let provider: Arc<dyn SchemaProvider> = Arc::new(AgoraSchemaProvider {
            catalog: Arc::clone(&self.inner),
        });
        let mut cache = self.schemas.write().unwrap();
        cache.insert(name.to_string(), Arc::clone(&provider));
        Some(provider)
    }
}

// === AgoraSchemaProvider ===

/// Bridges an Iceberg namespace to DataFusion's [`SchemaProvider`] trait.
#[derive(Debug)]
struct AgoraSchemaProvider {
    catalog: Arc<AgoraCatalog>,
}

#[async_trait]
impl SchemaProvider for AgoraSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => return vec![],
        };
        handle.block_on(async {
            let ns = iceberg::NamespaceIdent::new("default".to_string());
            match self.catalog.list_tables(&ns).await {
                Ok(tables) => tables.into_iter().map(|t| t.name().to_string()).collect(),
                Err(_) => vec![],
            }
        })
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let ns = iceberg::NamespaceIdent::new("default".to_string());
        let ident = iceberg::TableIdent::new(ns, name.to_string());
        match self.catalog.load_table(&ident).await {
            Ok(table) => {
                let provider = Arc::new(IcebergTableProvider::new(table)?);
                Ok(Some(provider))
            }
            Err(_) => Ok(None),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }
}

// === IcebergTableProvider ===

/// Bridges an Iceberg [`Table`] to DataFusion's [`TableProvider`] trait.
///
/// The [`scan`](Self::scan) method is currently a stub; full implementation
/// is planned for Task A3.
pub struct IcebergTableProvider {
    table: iceberg::table::Table,
    arrow_schema: SchemaRef,
}

impl Debug for IcebergTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTableProvider")
            .field("table", &self.table.identifier().to_string())
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

impl IcebergTableProvider {
    /// Create a new [`IcebergTableProvider`] from an Iceberg [`Table`].
    pub fn new(table: iceberg::table::Table) -> DFResult<Self> {
        let schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(iceberg_schema_to_arrow(schema.as_ref())?);
        Ok(Self {
            table,
            arrow_schema,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. Build Iceberg scan
        let scan = self
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // 2. Get file paths from scan plan
        let mut file_stream = scan
            .plan_files()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut batches = Vec::new();
        while let Some(result) = file_stream.next().await {
            let task = result.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let path = task.data_file_path();

            // 3. Read Parquet file into RecordBatches (all columns, projection applied later)
            let file_batches = read_parquet_file(path, None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            batches.extend(file_batches);
        }

        // 4. Apply projection and reorder columns to match projection order
        let exec_schema = if let Some(proj) = projection {
            let projected_fields: Vec<_> =
                proj.iter().map(|&i| self.arrow_schema.field(i).clone()).collect();
            let schema = Arc::new(ArrowSchema::new(projected_fields));
            if proj.is_empty() {
                // COUNT(*) optimization: project zero columns but preserve row count.
                let empty = batches
                    .into_iter()
                    .map(|batch| {
                        let opts = arrow::record_batch::RecordBatchOptions::new()
                            .with_row_count(Some(batch.num_rows()));
                        arrow::record_batch::RecordBatch::try_new_with_options(
                            Arc::clone(&schema),
                            vec![],
                            &opts,
                        )
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                batches = empty;
            } else {
                let projected: DFResult<Vec<_>> = batches
                    .into_iter()
                    .map(|batch| {
                        let arrays: Vec<arrow::array::ArrayRef> = proj
                            .iter()
                            .map(|&idx| batch.column(idx).clone())
                            .collect();
                        arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), arrays)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    })
                    .collect();
                batches = projected?;
            }
            schema
        } else {
            Arc::clone(&self.arrow_schema)
        };

        // 5. Apply limit (row-based truncation within and across batches)
        if let Some(limit_val) = limit {
            let mut total = 0;
            let mut truncated = Vec::new();
            for batch in batches {
                let batch_rows = batch.num_rows();
                if total >= limit_val {
                    break;
                }
                if total + batch_rows <= limit_val {
                    total += batch_rows;
                    truncated.push(batch);
                } else {
                    let take = limit_val - total;
                    total = limit_val;
                    let arrays: Vec<arrow::array::ArrayRef> = (0..batch.num_columns())
                        .map(|i| batch.column(i).slice(0, take))
                        .collect();
                    let sliced = arrow::record_batch::RecordBatch::try_new(
                        batch.schema(),
                        arrays,
                    )?;
                    truncated.push(sliced);
                }
            }
            batches = truncated;
        }

        // 6. Return MemorySourceConfig-backed execution plan
        let partitions: Vec<Vec<arrow::record_batch::RecordBatch>> = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };

        Ok(MemorySourceConfig::try_new_exec(
            &partitions,
            exec_schema,
            None, // projection already applied above
        )?)
    }
}

// ------------------------------------------------------------------
// Helpers: Convert Iceberg schema to Arrow schema
// ------------------------------------------------------------------

/// Convert an Iceberg [`Schema`] to an Arrow [`Schema`].
fn iceberg_schema_to_arrow(
    schema: &iceberg::spec::Schema,
) -> DFResult<arrow::datatypes::Schema> {
    let fields: Vec<arrow::datatypes::Field> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| {
            let dt = iceberg_type_to_arrow(&f.field_type)
                .unwrap_or(arrow::datatypes::DataType::Null);
            arrow::datatypes::Field::new(&f.name, dt, !f.required)
        })
        .collect();
    Ok(arrow::datatypes::Schema::new(fields))
}

/// Convert an Iceberg [`Type`] to an Arrow [`DataType`].
fn iceberg_type_to_arrow(ty: &Type) -> Option<arrow::datatypes::DataType> {
    match ty {
        Type::Primitive(p) => match p {
            PrimitiveType::Long | PrimitiveType::Int => {
                Some(arrow::datatypes::DataType::Int64)
            }
            PrimitiveType::Double | PrimitiveType::Float => {
                Some(arrow::datatypes::DataType::Float64)
            }
            PrimitiveType::Boolean => Some(arrow::datatypes::DataType::Boolean),
            PrimitiveType::String => Some(arrow::datatypes::DataType::Utf8),
            _ => None,
        },
        _ => None,
    }
}

// ------------------------------------------------------------------
// Helpers: Read Parquet file into RecordBatches
// ------------------------------------------------------------------

/// Read a Parquet file into Arrow [`RecordBatch`]es, with optional projection.
async fn read_parquet_file(
    path: &str,
    projection: Option<&Vec<usize>>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, CatalogError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path).map_err(|e| {
        CatalogError::Iceberg(format!("Failed to open parquet file {path}: {e}"))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        CatalogError::Iceberg(format!("Failed to read parquet {path}: {e}"))
    })?;

    let reader = match projection {
        Some(proj) => {
            let proj_vec: Vec<usize> = proj.iter().copied().collect();
            let parquet_schema = builder.parquet_schema().clone();
            builder
                .with_projection(parquet::arrow::ProjectionMask::roots(
                    &parquet_schema,
                    proj_vec,
                ))
                .build()
                .map_err(|e| {
                    CatalogError::Iceberg(format!(
                        "Failed to build parquet reader for {path}: {e}"
                    ))
                })?
        }
        None => builder.build().map_err(|e| {
            CatalogError::Iceberg(format!(
                "Failed to build parquet reader for {path}: {e}"
            ))
        })?,
    };

    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| {
            CatalogError::Iceberg(format!("Parquet read error: {e}"))
        })?;
        batches.push(batch);
    }
    Ok(batches)
}
