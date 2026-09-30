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

/// Convenience type alias for fallible AgoraDB operations.
pub type Result<T> = std::result::Result<T, AgoraError>;

/// Top-level error type for AgoraDB operations.
#[derive(Error, Debug)]
pub enum AgoraError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] CompactionError),

    #[error("Invalid space URI: {0}")]
    InvalidSpaceUri(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

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

    #[error("Space already exists: {0}")]
    SpaceExists(String),

    #[error("Space not found: {0}")]
    SpaceNotFound(String),

    #[error("Location not found: {0}")]
    LocationNotFound(String),

    #[error("Engine {engine} cannot read a {format} location")]
    IncompatibleFormat { engine: String, format: String },

    #[error("Location {location} already has writer Space {writer}")]
    LocationHasWriter { location: String, writer: String },

    #[error("Space kind {kind} is incompatible with a {format} location")]
    KindFormatMismatch { kind: String, format: String },

    #[error("Invalid property: {0}")]
    InvalidProperty(String),

    #[error("Registry error: {0}")]
    Registry(String),

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
    #[error("Flush failed: {0}")]
    FlushFailed(String),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Compaction error: {0}")]
    Compaction(#[from] CompactionError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
