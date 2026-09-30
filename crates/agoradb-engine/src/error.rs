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

use agoradb_core::EngineKind;
use thiserror::Error;

/// Errors raised by a [`QueryEngine`](crate::QueryEngine).
#[derive(Error, Debug)]
pub enum EngineError {
    /// The engine cannot perform the requested operation.
    #[error("engine {0} does not support: {1}")]
    Unsupported(EngineKind, String),

    /// The engine rejected or failed to run a statement.
    #[error("SQL error: {0}")]
    Sql(String),

    /// The referenced table is not attached to the engine.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// A value could not be represented in the declared column type.
    #[error("type mismatch in column '{column}': expected {expected}, got {actual}")]
    TypeMismatch {
        column: String,
        expected: String,
        actual: String,
    },

    /// A required engine extension is not installed / could not be loaded.
    #[error("extension '{0}' unavailable: {1}")]
    ExtensionUnavailable(String, String),

    /// Transaction bookkeeping failed (double begin, wrong handle, ...).
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Arrow conversion failed.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Underlying I/O failed.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The blocking worker running the statement was cancelled or panicked.
    #[error("engine worker cancelled: {0}")]
    Cancelled(String),
}
