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

//! The engine abstraction layer of AgoraDB.
//!
//! AgoraDB never computes a query itself. Every Space is served by a
//! [`QueryEngine`] — DuckDB for analytical Spaces, SQLite for transactional
//! ones, or an engine provided by the embedding host — and the federation
//! layer only merges the Arrow streams those engines produce.
//!
//! The contract is deliberately small: an engine can be told to make a
//! physical source visible under a `<space>.<table>` name ([`QueryEngine::attach`]),
//! run SQL that returns Arrow batches ([`QueryEngine::query`]), run SQL that
//! returns a row count ([`QueryEngine::execute`]) and, for transactional
//! engines, manage a transaction.

pub mod capabilities;
pub mod error;
pub mod name;
pub mod source;
pub mod stream;
pub mod value;

use std::fmt;

use arrow_schema::SchemaRef;
use async_trait::async_trait;

pub use agoradb_core::EngineKind;
pub use capabilities::Capabilities;
pub use error::EngineError;
pub use name::QualifiedName;
pub use source::TableSource;
pub use stream::{BatchSink, BlockingWorker, RecordBatchStream};
pub use value::Value;

/// The SQL dialect an engine speaks; the federation layer uses it to unparse
/// pushed-down plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlDialect {
    /// DuckDB dialect.
    DuckDb,
    /// SQLite dialect.
    Sqlite,
    /// Generic ANSI-ish SQL.
    Generic,
}

/// Handle of an open transaction, returned by [`QueryEngine::begin`].
///
/// The handle must be given back to [`QueryEngine::commit`] or
/// [`QueryEngine::rollback`] on the same engine. Dropping it without doing so
/// leaves the transaction open on the engine's connection.
#[derive(Debug, PartialEq, Eq)]
pub struct TxHandle {
    id: u64,
}

impl TxHandle {
    /// Create a handle; only engines should call this.
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    /// Engine-local identifier of the transaction.
    pub fn id(&self) -> u64 {
        self.id
    }
}

/// A query engine that computes over one or more Spaces.
///
/// Implementations are shared between sessions, hence `Send + Sync`.
/// Blocking drivers should wrap their connection in a [`BlockingWorker`].
#[async_trait]
pub trait QueryEngine: Send + Sync + fmt::Debug {
    /// Which engine this is.
    fn kind(&self) -> EngineKind;

    /// The SQL dialect the engine expects in [`Self::query`] / [`Self::execute`].
    fn dialect(&self) -> SqlDialect;

    /// What the engine can do.
    fn capabilities(&self) -> Capabilities;

    /// Stable identifier of this engine *instance*.
    ///
    /// Two tables attached to engines with the same instance id live in the
    /// same process-local database, so a join between them can be pushed
    /// down as a single SQL statement.
    fn instance_id(&self) -> &str;

    /// Make `source` visible to the engine under `name`.
    ///
    /// Attaching the same source under the same name again is a no-op;
    /// attaching a different source under an existing name replaces it.
    async fn attach(&self, name: &QualifiedName, source: TableSource) -> Result<(), EngineError>;

    /// Remove a previously attached table. Unknown names are ignored.
    async fn detach(&self, name: &QualifiedName) -> Result<(), EngineError>;

    /// Names of the tables currently visible in `space`.
    async fn table_names(&self, space: &str) -> Result<Vec<String>, EngineError>;

    /// The Arrow schema of an attached table.
    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError>;

    /// Run a statement that produces rows.
    ///
    /// The schema is available as soon as the statement is prepared; batches
    /// stream afterwards. An error raised mid-way arrives as the last stream item.
    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError>;

    /// Run a statement without a result set and return the affected row count
    /// (0 for DDL).
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, EngineError>;

    /// Open a transaction. Analytical engines return [`EngineError::Unsupported`].
    async fn begin(&self) -> Result<TxHandle, EngineError>;

    /// Commit the transaction identified by `tx`.
    async fn commit(&self, tx: TxHandle) -> Result<(), EngineError>;

    /// Roll back the transaction identified by `tx`.
    async fn rollback(&self, tx: TxHandle) -> Result<(), EngineError>;
}
