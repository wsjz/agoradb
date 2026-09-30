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

//! Node-wide engine instances.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use agoradb_catalog::{AgoraCatalog, Space};
use agoradb_core::EngineKind;
use agoradb_engine::QueryEngine;

use crate::error::SessionError;

/// Node-level configuration.
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
    /// DuckDB tunables (ignored when the `engine-duckdb` feature is off).
    #[cfg(feature = "engine-duckdb")]
    pub duckdb: agoradb_engine_duckdb::DuckDbConfig,
    /// Local scratch directory for staging Parquet files; defaults to
    /// `<catalog root>/.agora/tmp`.
    pub temp_dir: Option<PathBuf>,
}

/// The engines available on this node.
///
/// One DuckDB instance is shared by every analytical Space (so cross-Space
/// joins between them push down as a single statement); each transactional
/// Space gets its own SQLite engine, opened lazily.
pub struct EngineRegistry {
    catalog: Arc<AgoraCatalog>,
    duckdb: Option<Arc<dyn QueryEngine>>,
    sqlite: RwLock<HashMap<String, Arc<dyn QueryEngine>>>,
    temp_dir: PathBuf,
}

impl std::fmt::Debug for EngineRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineRegistry")
            .field("duckdb", &self.duckdb.is_some())
            .field("temp_dir", &self.temp_dir)
            .finish()
    }
}

impl EngineRegistry {
    /// Create the registry, opening the engines compiled into this node.
    pub fn new(catalog: Arc<AgoraCatalog>, config: NodeConfig) -> Result<Self, SessionError> {
        let temp_dir = config
            .temp_dir
            .clone()
            .unwrap_or_else(|| catalog.agora_dir().join("tmp"));
        std::fs::create_dir_all(&temp_dir).map_err(agoradb_core::CatalogError::Io)?;

        #[cfg(feature = "engine-duckdb")]
        let duckdb: Option<Arc<dyn QueryEngine>> = Some(Arc::new(
            agoradb_engine_duckdb::DuckDbEngine::open_in_memory(config.duckdb.clone())?,
        ));
        #[cfg(not(feature = "engine-duckdb"))]
        let duckdb: Option<Arc<dyn QueryEngine>> = None;

        Ok(Self {
            catalog,
            duckdb,
            sqlite: RwLock::new(HashMap::new()),
            temp_dir,
        })
    }

    /// Directory for staging files before they are committed to a table.
    pub fn temp_dir(&self) -> &PathBuf {
        &self.temp_dir
    }

    /// The shared analytical engine.
    pub fn duckdb(&self) -> Result<Arc<dyn QueryEngine>, SessionError> {
        self.duckdb
            .clone()
            .ok_or(SessionError::EngineNotRegistered(EngineKind::DuckDb))
    }

    /// The SQLite engine serving `space`'s Location, opened on first use.
    #[cfg(feature = "engine-sqlite")]
    pub fn sqlite_for(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        if let Some(engine) = self
            .sqlite
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&space.name)
        {
            return Ok(engine.clone());
        }
        let location = self.catalog.get_location(&space.location)?;
        let path = self.catalog.sqlite_path(&location)?;
        let engine: Arc<dyn QueryEngine> = Arc::new(agoradb_engine_sqlite::SqliteEngine::open(
            &space.name,
            &path,
        )?);
        self.sqlite
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(space.name.clone(), engine.clone());
        Ok(engine)
    }

    #[cfg(not(feature = "engine-sqlite"))]
    pub fn sqlite_for(&self, _space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        Err(SessionError::EngineNotRegistered(EngineKind::Sqlite))
    }

    /// The engine a Space is declared to use.
    pub fn engine_for(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        match space.engine {
            EngineKind::DuckDb => self.duckdb(),
            EngineKind::Sqlite => self.sqlite_for(space),
        }
    }
}
