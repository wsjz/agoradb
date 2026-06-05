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

use std::collections::HashMap;
use std::str::FromStr;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::scan::ArrowRecordBatchStream;
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};

/// AgoraDB Catalog implementation backed by [`FileIO`].
///
/// Stores namespaces as directories and tables as subdirectories
/// containing Iceberg metadata files.
#[derive(Debug, Clone)]
pub struct AgoraCatalog {
    file_io: FileIO,
    root_path: String,
}

impl AgoraCatalog {
    /// Create a new [`AgoraCatalog`] with the given [`FileIO`] and root path.
    pub fn new(file_io: FileIO, root_path: impl Into<String>) -> Self {
        Self {
            file_io,
            root_path: root_path.into(),
        }
    }

    /// Return the root path of this catalog.
    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    /// Return a reference to the [`FileIO`] instance.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Compute the filesystem path for a namespace directory.
    fn namespace_path(&self, namespace: &NamespaceIdent) -> String {
        format!("{}/{}", self.root_path, namespace.as_ref().join("/"))
    }

    /// Compute the filesystem path for a table directory.
    fn table_path(&self, namespace: &NamespaceIdent, name: &str) -> String {
        format!("{}/{}", self.namespace_path(namespace), name)
    }

    /// Compute the metadata directory path for a table.
    fn metadata_dir(&self, namespace: &NamespaceIdent, name: &str) -> String {
        format!("{}/metadata", self.table_path(namespace, name))
    }

    /// Compute the data directory path for a table.
    fn data_dir(&self, namespace: &NamespaceIdent, name: &str) -> String {
        format!("{}/data", self.table_path(namespace, name))
    }

    /// Path to the namespace properties file.
    fn namespace_properties_path(&self, namespace: &NamespaceIdent) -> String {
        format!("{}/.namespace.properties", self.namespace_path(namespace))
    }

    /// Find the latest metadata file for a table.
    ///
    /// Scans the metadata directory for `*.metadata.json` files and returns
    /// the one with the highest version number.
    async fn latest_metadata_file(
        &self,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> Result<Option<String>> {
        let meta_dir = self.metadata_dir(namespace, name);
        let mut entries = match tokio::fs::read_dir(&meta_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to read metadata directory {meta_dir}: {e}"),
                ))
            }
        };

        let mut files: Vec<String> = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to read dir: {e}")))?
        {
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            if name_str.ends_with(".metadata.json") {
                files.push(format!("{}/{}", meta_dir, name_str));
            }
        }

        files.sort();
        Ok(files.pop())
    }

    /// Read namespace properties from the `.namespace.properties` file.
    async fn read_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<HashMap<String, String>> {
        let path = self.namespace_properties_path(namespace);
        match tokio::fs::read_to_string(&path).await {
            Ok(content) => serde_json::from_str(&content).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to parse namespace properties: {e}"),
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to read namespace properties: {e}"),
            )),
        }
    }

    /// Write namespace properties to the `.namespace.properties` file.
    async fn write_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        properties: &HashMap<String, String>,
    ) -> Result<()> {
        let path = self.namespace_properties_path(namespace);
        let content = serde_json::to_string_pretty(properties).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to serialize namespace properties: {e}"),
            )
        })?;
        tokio::fs::write(&path, content).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to write namespace properties: {e}"),
            )
        })
    }
}

#[async_trait]
impl Catalog for AgoraCatalog {
    /// List namespaces inside the catalog.
    ///
    /// If `parent` is `None`, lists top-level namespaces (directories under root_path).
    /// If `parent` is provided, lists namespaces one level under the parent.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let dir_path = match parent {
            Some(ns) => self.namespace_path(ns),
            None => self.root_path.clone(),
        };

        let mut entries = match tokio::fs::read_dir(&dir_path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to read directory {dir_path}: {e}"),
                ))
            }
        };

        let mut namespaces = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to read dir: {e}")))?
        {
            let file_type = entry.file_type().await.map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to get file type: {e}"),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip hidden directories
            if name_str.starts_with('.') {
                continue;
            }

            let mut parts = match parent {
                Some(ns) => ns.as_ref().clone(),
                None => Vec::new(),
            };
            parts.push(name_str.to_string());
            namespaces.push(NamespaceIdent::from_vec(parts)?);
        }

        Ok(namespaces)
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let path = self.namespace_path(namespace);

        tokio::fs::create_dir_all(&path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to create namespace directory {path}: {e}"),
            )
        })?;

        self.write_namespace_properties(namespace, &properties)
            .await?;

        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let path = self.namespace_path(namespace);

        match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace path exists but is not a directory: {path}"),
                ))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace does not exist: {}", namespace),
                ))
            }
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to check namespace {path}: {e}"),
                ))
            }
        }

        let properties = self.read_namespace_properties(namespace).await?;
        Ok(Namespace::with_properties(namespace.clone(), properties))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let path = self.namespace_path(namespace);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(meta.is_dir()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to check namespace existence {path}: {e}"),
            )),
        }
    }

    /// Update a namespace inside the catalog.
    ///
    /// The properties must be the full set of namespace properties.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        // Verify namespace exists
        if !self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace does not exist: {}", namespace),
            ));
        }

        self.write_namespace_properties(namespace, &properties)
            .await
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let path = self.namespace_path(namespace);
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace does not exist: {}", namespace),
            )),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to drop namespace {path}: {e}"),
            )),
        }
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // Verify namespace exists
        if !self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace does not exist: {}", namespace),
            ));
        }

        let dir_path = self.namespace_path(namespace);
        let mut entries = match tokio::fs::read_dir(&dir_path).await {
            Ok(entries) => entries,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to read directory {dir_path}: {e}"),
                ))
            }
        };

        let mut tables = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to read dir: {e}")))?
        {
            let file_type = entry.file_type().await.map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to get file type: {e}"),
                )
            })?;
            if !file_type.is_dir() {
                continue;
            }

            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip hidden directories
            if name_str.starts_with('.') {
                continue;
            }

            // Only include directories that have a metadata subdirectory
            let table_meta_dir = format!("{}/{}/metadata", dir_path, name_str);
            if tokio::fs::metadata(&table_meta_dir).await.is_ok() {
                tables.push(TableIdent::new(namespace.clone(), name_str.to_string()));
            }
        }

        Ok(tables)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        // Verify namespace exists
        if !self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace does not exist: {}", namespace),
            ));
        }

        let table_name = creation.name.clone();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

        let table_path = self.table_path(namespace, &table_name);
        let metadata_dir = self.metadata_dir(namespace, &table_name);
        let data_dir = self.data_dir(namespace, &table_name);

        // Create metadata and data directories
        tokio::fs::create_dir_all(&metadata_dir)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to create metadata directory: {e}"),
                )
            })?;
        tokio::fs::create_dir_all(&data_dir).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to create data directory: {e}"),
            )
        })?;

        // Determine table location
        let location = creation
            .location
            .clone()
            .unwrap_or_else(|| table_path.clone());

        // Build table creation with location
        let table_creation = TableCreation {
            location: Some(location.clone()),
            ..creation
        };

        // Build metadata
        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;

        // Write metadata file
        let metadata_location = MetadataLocation::new_with_table_location(&location);
        let metadata_file_path = metadata_location.to_string();

        metadata
            .write_to(&self.file_io, &metadata_file_path)
            .await?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .identifier(table_ident)
            .metadata_location(metadata_file_path)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let namespace = table.namespace();
        let name = table.name();

        // Verify namespace exists
        if !self.namespace_exists(namespace).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace does not exist: {}", namespace),
            ));
        }

        let metadata_file = match self.latest_metadata_file(namespace, name).await? {
            Some(file) => file,
            None => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Table does not exist: {}", table),
                ))
            }
        };

        let metadata =
            iceberg::spec::TableMetadata::read_from(&self.file_io, &metadata_file).await?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .identifier(table.clone())
            .metadata_location(metadata_file)
            .build()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        let table_dir = self.table_path(table.namespace(), table.name());
        match tokio::fs::remove_dir_all(&table_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Table does not exist: {}", table),
            )),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to drop table {table_dir}: {e}"),
            )),
        }
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        match self
            .latest_metadata_file(table.namespace(), table.name())
            .await
        {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        let src_path = self.table_path(src.namespace(), src.name());
        let dest_path = self.table_path(dest.namespace(), dest.name());

        // Verify source exists
        if !self.table_exists(src).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Source table does not exist: {}", src),
            ));
        }

        // Verify destination namespace exists
        if !self.namespace_exists(dest.namespace()).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Destination namespace does not exist: {}", dest.namespace()),
            ));
        }

        // Verify destination doesn't already exist
        if self.table_exists(dest).await? {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Destination table already exists: {}", dest),
            ));
        }

        tokio::fs::rename(&src_path, &dest_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to rename table from {src_path} to {dest_path}: {e}"),
            )
        })
    }

    /// Register an existing table to the catalog.
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "register_table is not supported by AgoraCatalog",
        ))
    }

    /// Update a table in the catalog.
    ///
    /// Uses optimistic concurrency control: no locks are held during read/apply.
    /// After writing the new metadata file we re-read the latest metadata to
    /// verify our write won the race.  If another commit raced ahead we delete
    /// our stale file and return a retryable error so the caller (typically
    /// [`Transaction::commit`]) can re-try with the updated state.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();

        // 1. Optimistic read — no lock.
        let current_table = self.load_table(&table_ident).await?;

        // 2. Apply commit (validates requirements, e.g. snapshot-id match).
        let staged_table = commit.apply(current_table)?;

        // 3. Build the new metadata file path.
        let metadata_location = staged_table.metadata_location_result()?;
        let new_metadata_location = MetadataLocation::from_str(metadata_location)?
            .with_next_version()
            .to_string();

        // 4. Write the new metadata file.
        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &new_metadata_location)
            .await?;

        // 5. Verify: did our write win the race?
        let latest_table = self.load_table(&table_ident).await?;
        let latest_location = latest_table
            .metadata_location()
            .map(|s| s.to_string())
            .unwrap_or_default();

        if latest_location != new_metadata_location {
            // We lost the race — another commit wrote a newer metadata file.
            // Clean up our stale file and signal the caller to retry.
            let _ = self
                .file_io
                .delete(&new_metadata_location)
                .await
                .map_err(|e| {
                    eprintln!(
                        "Warning: failed to delete stale metadata file {}: {}",
                        new_metadata_location, e
                    );
                });
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!(
                    "Concurrent modification detected on table {}: expected latest metadata to be {}, but found {}",
                    table_ident, new_metadata_location, latest_location
                ),
            )
            .with_retryable(true));
        }

        // 6. Our write won — return the updated table.
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata(staged_table.metadata().clone())
            .identifier(table_ident)
            .metadata_location(new_metadata_location)
            .build()
    }
}

use crate::scan_provider::StorageScanProvider;
use agoradb_core::{CatalogError, DataType, ExecutionError, Morsel, SchemaProvider, SpaceUri};
use futures::StreamExt;
use iceberg::expr::Predicate;
use iceberg::spec::{PrimitiveType, Type};

#[async_trait]
impl StorageScanProvider for AgoraCatalog {
    async fn scan_table(
        &self,
        space: &SpaceUri,
        snapshot_id: i64,
        filter: Option<Predicate>,
    ) -> std::result::Result<ArrowRecordBatchStream, CatalogError> {
        let table_ident = TableIdent::from_strs(["default", &space.name])
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let table = self
            .load_table(&table_ident)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let mut scan_builder = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_row_selection_enabled(true);

        if let Some(predicate) = filter {
            scan_builder = scan_builder.with_filter(predicate);
        }

        let scan = scan_builder
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        scan.to_arrow()
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))
    }

    async fn list_morsels(
        &self,
        space: &SpaceUri,
        snapshot_id: i64,
        morsel_size: usize,
    ) -> std::result::Result<Vec<Morsel>, CatalogError> {
        let table_ident = TableIdent::from_strs(["default", &space.name])
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let table = self
            .load_table(&table_ident)
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .build()
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let mut task_stream = scan
            .plan_files()
            .await
            .map_err(|e| CatalogError::Iceberg(e.to_string()))?;

        let chunk_size = if morsel_size == 0 {
            10_000
        } else {
            morsel_size
        };
        let mut morsels = Vec::new();

        while let Some(result) = task_stream.next().await {
            let task = result.map_err(|e| CatalogError::Iceberg(e.to_string()))?;
            let path = task.data_file_path().to_string();

            // record_count comes from Iceberg metadata; fall back to reading
            // the Parquet footer when the metadata field is absent.
            let total_rows = match task.record_count {
                Some(n) => n as usize,
                None => crate::parquet_util::parquet_row_count(&path).map_err(|e| {
                    CatalogError::Iceberg(format!(
                        "missing record_count and failed to read parquet footer for {path}: {e}"
                    ))
                })?,
            };

            if total_rows == 0 {
                continue;
            }

            let mut row_start = 0usize;
            while row_start < total_rows {
                let row_count = chunk_size.min(total_rows - row_start);
                morsels.push(Morsel {
                    file_path: path.clone(),
                    row_start,
                    row_count,
                });
                row_start += row_count;
            }
        }

        Ok(morsels)
    }

    async fn read_morsel(
        &self,
        morsel: &Morsel,
    ) -> std::result::Result<Vec<arrow_array::RecordBatch>, CatalogError> {
        crate::parquet_util::read_morsel(morsel).await
    }
}

// ------------------------------------------------------------------
// SchemaProvider — bridges catalog metadata to the query analyzer
// ------------------------------------------------------------------

impl SchemaProvider for AgoraCatalog {
    fn get_table_schema(
        &self,
        table: &str,
    ) -> std::result::Result<HashMap<String, DataType>, ExecutionError> {
        futures::executor::block_on(async {
            // Search all namespaces for a table with the given name.
            let namespaces = self.list_namespaces(None).await.map_err(|e| {
                ExecutionError::OperatorError(format!("Failed to list namespaces: {}", e))
            })?;

            let mut found_table = None;
            for ns in &namespaces {
                let tables = self.list_tables(ns).await.map_err(|e| {
                    ExecutionError::OperatorError(format!(
                        "Failed to list tables in namespace '{}': {}",
                        ns, e
                    ))
                })?;
                if let Some(ident) = tables.iter().find(|t| t.name() == table) {
                    found_table = Some(ident.clone());
                    break;
                }
            }

            let table_ident = found_table.ok_or_else(|| {
                ExecutionError::OperatorError(format!("Table not found: {}", table))
            })?;

            let table = self.load_table(&table_ident).await.map_err(|e| {
                ExecutionError::OperatorError(format!(
                    "Failed to load table '{}': {}",
                    table, e
                ))
            })?;

            let schema = table.metadata().current_schema();
            let mut result = HashMap::new();
            for field in schema.as_struct().fields() {
                let dt = iceberg_type_to_data_type(&field.field_type).ok_or_else(|| {
                    ExecutionError::OperatorError(format!(
                        "Unsupported Iceberg type for column '{}': {:?}",
                        field.name, field.field_type
                    ))
                })?;
                result.insert(field.name.clone(), dt);
            }
            Ok(result)
        })
    }
}

/// Convert an Iceberg [`Type`] to an AgoraDB [`DataType`].
fn iceberg_type_to_data_type(ty: &Type) -> Option<DataType> {
    match ty {
        Type::Primitive(p) => match p {
            PrimitiveType::Long => Some(DataType::Int64),
            PrimitiveType::Int => Some(DataType::Int64),
            PrimitiveType::Double => Some(DataType::Float64),
            PrimitiveType::Float => Some(DataType::Float64),
            PrimitiveType::Boolean => Some(DataType::Boolean),
            PrimitiveType::String => Some(DataType::Utf8),
            PrimitiveType::Date
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs
            | PrimitiveType::Decimal { .. }
            | PrimitiveType::Uuid
            | PrimitiveType::Binary
            | PrimitiveType::Fixed(_) => None,
        },
        Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use tempfile::TempDir;

    async fn new_test_catalog() -> (TempDir, AgoraCatalog) {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();
        let catalog = AgoraCatalog::new(file_io, root_path);
        (temp_dir, catalog)
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_create_and_get_namespace() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());

        let namespace = catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        assert_eq!(namespace.name(), &ns);

        let got = catalog.get_namespace(&ns).await.unwrap();
        assert_eq!(got.name(), &ns);
    }

    #[tokio::test]
    async fn test_namespace_exists() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());

        assert!(!catalog.namespace_exists(&ns).await.unwrap());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        assert!(catalog.namespace_exists(&ns).await.unwrap());
    }

    #[tokio::test]
    async fn test_list_namespaces() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns1 = NamespaceIdent::new("ns1".to_string());
        let ns2 = NamespaceIdent::new("ns2".to_string());

        catalog
            .create_namespace(&ns1, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_namespace(&ns2, HashMap::new())
            .await
            .unwrap();

        let namespaces = catalog.list_namespaces(None).await.unwrap();
        assert_eq!(namespaces.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());

        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        assert!(catalog.namespace_exists(&ns).await.unwrap());

        catalog.drop_namespace(&ns).await.unwrap();
        assert!(!catalog.namespace_exists(&ns).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_and_load_table() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(test_schema())
            .build();

        let table = catalog.create_table(&ns, creation).await.unwrap();
        assert_eq!(table.identifier().name(), "test_table");

        let ident = TableIdent::new(ns.clone(), "test_table".to_string());
        let loaded = catalog.load_table(&ident).await.unwrap();
        assert_eq!(loaded.identifier().name(), "test_table");
    }

    #[tokio::test]
    async fn test_list_tables() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let creation = TableCreation::builder()
            .name("table1".to_string())
            .schema(test_schema())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();

        let tables = catalog.list_tables(&ns).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name(), "table1");
    }

    #[tokio::test]
    async fn test_table_exists_and_drop() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let ident = TableIdent::new(ns.clone(), "test_table".to_string());
        assert!(!catalog.table_exists(&ident).await.unwrap());

        let creation = TableCreation::builder()
            .name("test_table".to_string())
            .schema(test_schema())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();

        assert!(catalog.table_exists(&ident).await.unwrap());

        catalog.drop_table(&ident).await.unwrap();
        assert!(!catalog.table_exists(&ident).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_table() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let creation = TableCreation::builder()
            .name("old_name".to_string())
            .schema(test_schema())
            .build();
        catalog.create_table(&ns, creation).await.unwrap();

        let src = TableIdent::new(ns.clone(), "old_name".to_string());
        let dest = TableIdent::new(ns.clone(), "new_name".to_string());

        catalog.rename_table(&src, &dest).await.unwrap();

        assert!(!catalog.table_exists(&src).await.unwrap());
        assert!(catalog.table_exists(&dest).await.unwrap());
    }

    #[tokio::test]
    async fn test_register_table_unsupported() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let ident = TableIdent::new(ns.clone(), "test".to_string());
        let result = catalog.register_table(&ident, "/path".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_namespace_properties() {
        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());

        let mut props = HashMap::new();
        props.insert("owner".to_string(), "team".to_string());

        catalog.create_namespace(&ns, props.clone()).await.unwrap();

        let got = catalog.get_namespace(&ns).await.unwrap();
        assert_eq!(got.properties().get("owner"), Some(&"team".to_string()));

        let mut new_props = HashMap::new();
        new_props.insert("owner".to_string(), "new_team".to_string());
        catalog
            .update_namespace(&ns, new_props.clone())
            .await
            .unwrap();

        let updated = catalog.get_namespace(&ns).await.unwrap();
        assert_eq!(
            updated.properties().get("owner"),
            Some(&"new_team".to_string())
        );
    }

    #[tokio::test]
    async fn test_schema_provider() {
        use agoradb_core::{DataType, SchemaProvider};
        use iceberg::spec::{NestedField, Schema, Type};

        let (_dir, catalog) = new_test_catalog().await;
        let ns = NamespaceIdent::new("test_ns".to_string());
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "price", Type::Primitive(PrimitiveType::Double)).into(),
                NestedField::required(3, "active", Type::Primitive(PrimitiveType::Boolean)).into(),
                NestedField::required(4, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        let creation = TableCreation::builder()
            .name("products".to_string())
            .schema(schema)
            .build();
        catalog.create_table(&ns, creation).await.unwrap();

        let table_schema = catalog.get_table_schema("products").unwrap();
        assert_eq!(table_schema.get("id"), Some(&DataType::Int64));
        assert_eq!(table_schema.get("price"), Some(&DataType::Float64));
        assert_eq!(table_schema.get("active"), Some(&DataType::Boolean));
        assert_eq!(table_schema.get("name"), Some(&DataType::Utf8));
    }
}
