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

use agoradb_core::{CatalogError, EngineKind, StorageError};
use agoradb_engine::EngineError;
use agoradb_semantic::SemanticError;
use thiserror::Error;

/// Errors raised by [`AgoraSession`](crate::AgoraSession).
#[derive(Error, Debug)]
pub enum SessionError {
    #[error(transparent)]
    Semantic(#[from] SemanticError),

    #[error(transparent)]
    Catalog(#[from] CatalogError),

    #[error(transparent)]
    Engine(#[from] EngineError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Federation(#[from] agoradb_federation::FederationError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("iceberg error: {0}")]
    Iceberg(String),

    #[error("engine {0} is not registered on this node")]
    EngineNotRegistered(EngineKind),

    #[error("no default space is set; run SET SPACE <name> or qualify table names")]
    NoDefaultSpace,

    #[error("a transaction is already open on space '{0}'")]
    TransactionOpen(String),

    #[error("no transaction is open")]
    NoTransaction,

    #[error("DML spanning several spaces is not supported: {}", .0.join(", "))]
    MultiSpaceNotSupported(Vec<String>),

    #[error("unsupported: {0}")]
    Unsupported(String),
}
