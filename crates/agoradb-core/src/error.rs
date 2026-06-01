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

use thiserror::Error;

/// Top-level error type for AgoraDB operations.
#[derive(Error, Debug)]
pub enum AgoraError {
    #[error("VFS error: {0}")]
    Vfs(#[from] VfsError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] CompactionError),

    #[error("Execution error: {0}")]
    Execution(#[from] ExecutionError),

    #[error("Invalid space URI: {0}")]
    InvalidSpaceUri(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors originating from the query execution engine.
#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Operator error: {0}")]
    OperatorError(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors originating from the virtual file system layer.
#[derive(Error, Debug)]
pub enum VfsError {
    #[error("Backend not supported: {0}")]
    BackendNotSupported(String),

    #[error("Memory mapping not supported on this platform")]
    MmapNotSupported,

    #[error("OpenDAL error: {0}")]
    OpenDal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors originating from the catalog/metadata layer.
#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Iceberg error: {0}")]
    Iceberg(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Invalid mode: {0}")]
    InvalidMode(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors originating from the compaction process.
#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Compaction failed: {0}")]
    CompactionFailed(String),
    #[error("No data files to compact")]
    NoDataFiles,
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors originating from the storage engine.
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Buffer overflow")]
    BufferOverflow,

    #[error("Flush failed: {0}")]
    FlushFailed(String),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("VFS error: {0}")]
    Vfs(#[from] VfsError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] CompactionError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
