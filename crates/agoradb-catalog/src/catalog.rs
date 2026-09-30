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
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use agoradb_core::CatalogError;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadataBuilder;
use iceberg::table::Table;
use iceberg::Runtime;
use iceberg::{
    Catalog, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};

use crate::registry::Registry;

/// AgoraDB Catalog implementation backed by [`FileIO`].
///
/// Stores namespaces as directories and tables as subdirectories
/// containing Iceberg metadata files. Space and Location registries live in
/// the hidden `.agora/` directory under the root (see [`crate::registry`]).
#[derive(Debug, Clone)]
pub struct AgoraCatalog {
    file_io: FileIO,
    root_path: String,
    pub(crate) registry: Arc<RwLock<Registry>>,
}

impl AgoraCatalog {
    /// Open the catalog at `root_path`, loading the Space/Location registries.
    pub fn open(
        file_io: FileIO,
        root_path: impl Into<String>,
    ) -> std::result::Result<Self, CatalogError> {
        let root_path = root_path.into();
        let registry = Registry::load(Path::new(&root_path))?;
        Ok(Self {
            file_io,
            root_path,
            registry: Arc::new(RwLock::new(registry)),
        })
    }

    /// Create a new [`AgoraCatalog`] with the given [`FileIO`] and root path.
    ///
    /// Like [`Self::open`], but an unreadable registry is logged and treated
    /// as empty instead of failing.
    pub fn new(file_io: FileIO, root_path: impl Into<String>) -> Self {
        let root_path = root_path.into();
        let registry = Registry::load(Path::new(&root_path)).unwrap_or_else(|e| {
            tracing::warn!(root = %root_path, error = %e, "could not load space registry");
            Registry::default()
        });
        Self {
            file_io,
            root_path,
            registry: Arc::new(RwLock::new(registry)),
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

    /// Write `metadata` as the version after `current_location` and verify
    /// it became the latest one (optimistic concurrency).
    ///
    /// If another commit raced ahead, our file is deleted and a retryable
    /// [`ErrorKind::CatalogCommitConflicts`] error is returned.
    pub(crate) async fn write_next_metadata(
        &self,
        table_ident: &TableIdent,
        current_location: &str,
        metadata: iceberg::spec::TableMetadata,
    ) -> Result<Table> {
        let next_metadata_location =
            MetadataLocation::from_str(current_location)?.with_next_version();
        let new_metadata_location = next_metadata_location.to_string();

        metadata
            .write_to(&self.file_io, &next_metadata_location)
            .await?;

        let latest_table = self.load_table(table_ident).await?;
        let latest_location = latest_table
            .metadata_location()
            .map(|s| s.to_string())
            .unwrap_or_default();

        if latest_location != new_metadata_location {
            let _ = self
                .file_io
                .delete(&new_metadata_location)
                .await
                .map_err(|e| {
                    tracing::warn!(
                        path = %new_metadata_location,
                        error = %e,
                        "failed to delete stale metadata file after commit conflict"
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

        Table::builder()
            .runtime(Runtime::try_current()?)
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .identifier(table_ident.clone())
            .metadata_location(new_metadata_location)
            .build()
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
        let metadata_location = MetadataLocation::new_with_metadata(&location, &metadata);
        metadata.write_to(&self.file_io, &metadata_location).await?;

        Table::builder()
            .runtime(Runtime::try_current()?)
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .identifier(table_ident)
            .metadata_location(metadata_location.to_string())
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
            .runtime(Runtime::try_current()?)
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

    /// Drop a table and delete its data.
    ///
    /// Tables own their `metadata/` and `data/` directories under the table
    /// path, so removing the table directory (what [`Self::drop_table`] does)
    /// already purges every file that belongs to it.
    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.drop_table(table).await
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
    /// [`iceberg::transaction::Transaction::commit`]) can re-try with the updated state.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let table_ident = commit.identifier().clone();

        // 1. Optimistic read — no lock.
        let current_table = self.load_table(&table_ident).await?;

        // 2. Apply commit (validates requirements, e.g. snapshot-id match).
        let staged_table = commit.apply(current_table)?;

        // 3-6. Write the next metadata version and verify it won the race.
        let current_location = staged_table.metadata_location_result()?.to_string();
        self.write_next_metadata(
            &table_ident,
            &current_location,
            staged_table.metadata().clone(),
        )
        .await
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
}
