// Copyright 2025 The Oikome Authors
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
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

use oikome_catalog::{is_plaintext_peer, LocationFormat, OikomeCatalog, Space};
use oikome_core::{CatalogError, EngineKind};
use oikome_engine::QueryEngine;
use oikome_identity::{load_node_identity, Did, NodeIdentity};
use oikome_network::{FlightEngine, RemoteSpaceConfig};

use crate::error::SessionError;

/// Node-level configuration.
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
    /// DuckDB tunables (ignored when the `engine-duckdb` feature is off).
    #[cfg(feature = "engine-duckdb")]
    pub duckdb: oikome_engine_duckdb::DuckDbConfig,
    /// Local scratch directory for staging Parquet files; defaults to
    /// `<catalog root>/.oikome/tmp`.
    pub temp_dir: Option<PathBuf>,
    /// The node's signing identity. When unset it is loaded on first use
    /// from `OIKOME_NODE_KEY` or `<catalog root>/.oikome/identity/`.
    pub identity: Option<Arc<NodeIdentity>>,
    /// Accept plaintext (`http://`) peers. Off by default: peers are
    /// reached over TLS with their certificate pinned to their DID.
    pub allow_http: bool,
}

/// A remote Space's engine and the binding (peer, Space, owner, token id)
/// it was built for.
type RemoteEngine = (String, Arc<dyn QueryEngine>);

/// The engines available on this node.
///
/// One DuckDB instance is shared by every analytical Space (so cross-Space
/// joins between them push down as a single statement); each transactional
/// Space gets its own SQLite engine, opened lazily.
pub struct EngineRegistry {
    catalog: Arc<OikomeCatalog>,
    duckdb: Option<Arc<dyn QueryEngine>>,
    sqlite: RwLock<HashMap<String, Arc<dyn QueryEngine>>>,
    /// Flight engines of remote Spaces, keyed by Space name, with the
    /// binding they were built for (so a re-created Space gets a new one).
    remote: RwLock<HashMap<String, RemoteEngine>>,
    identity: OnceLock<Arc<NodeIdentity>>,
    temp_dir: PathBuf,
    allow_http: bool,
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
    pub fn new(catalog: Arc<OikomeCatalog>, config: NodeConfig) -> Result<Self, SessionError> {
        let temp_dir = config
            .temp_dir
            .clone()
            .unwrap_or_else(|| catalog.oikome_dir().join("tmp"));
        std::fs::create_dir_all(&temp_dir).map_err(oikome_core::CatalogError::Io)?;

        #[cfg(feature = "engine-duckdb")]
        let duckdb: Option<Arc<dyn QueryEngine>> = Some(Arc::new(
            oikome_engine_duckdb::DuckDbEngine::open_in_memory(config.duckdb.clone())?,
        ));
        #[cfg(not(feature = "engine-duckdb"))]
        let duckdb: Option<Arc<dyn QueryEngine>> = None;

        let identity = OnceLock::new();
        if let Some(id) = config.identity {
            let _ = identity.set(id);
        }
        Ok(Self {
            catalog,
            duckdb,
            sqlite: RwLock::new(HashMap::new()),
            remote: RwLock::new(HashMap::new()),
            identity,
            temp_dir,
            allow_http: config.allow_http,
        })
    }

    /// Whether this node accepts plaintext (`http://`) peers.
    pub fn allow_http(&self) -> bool {
        self.allow_http
    }

    /// The Flight endpoint of the node `owner`: `peer` when the Space names
    /// one, otherwise where discovery last saw `owner`. Plaintext endpoints
    /// are refused unless [`NodeConfig::allow_http`] is set.
    pub fn peer_endpoint(&self, peer: Option<&str>, owner: &str) -> Result<String, SessionError> {
        let endpoint = match peer {
            Some(peer) => peer.to_string(),
            None => {
                // Discovery records peers from the serving process.
                self.catalog.refresh()?;
                self.catalog
                    .get_peer(owner)
                    .map(|p| p.endpoint)
                    .ok_or_else(|| {
                        CatalogError::InvalidProperty(format!(
                            "node {owner} has not been discovered: check that it is serving on \
                             this network, or give its address with PEER = 'https://host:port'"
                        ))
                    })?
            }
        };
        if is_plaintext_peer(&endpoint) && !self.allow_http {
            return Err(CatalogError::InvalidProperty(format!(
                "{endpoint} is plaintext; this node only reaches peers over https \
                 (start it with --allow-http to accept http)"
            ))
            .into());
        }
        Ok(endpoint)
    }

    /// The node's identity, loaded on first use if it was not configured.
    pub fn identity(&self) -> Result<Arc<NodeIdentity>, SessionError> {
        if let Some(id) = self.identity.get() {
            return Ok(id.clone());
        }
        let loaded = Arc::new(load_node_identity(Path::new(self.catalog.root_path()))?);
        Ok(self.identity.get_or_init(|| loaded).clone())
    }

    /// The Flight engine querying the remote Space `space` on its owner.
    pub fn remote_for(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        let location = self.catalog.get_location(&space.location)?;
        let LocationFormat::Remote {
            peer,
            space: remote_space,
            owner,
        } = &location.format
        else {
            return Err(CatalogError::KindFormatMismatch {
                kind: space.kind.to_string(),
                format: location.format.name().to_string(),
            }
            .into());
        };
        let token = self.catalog.remote_token(&space.name)?;
        // Resolved on every bind: a discovered owner may have moved.
        let peer = self.peer_endpoint(peer.as_deref(), owner)?;
        let binding = format!(
            "{peer}|{remote_space}|{owner}|{}",
            oikome_identity::ucan::token_id(token.expose())
        );
        if let Some((key, engine)) = self
            .remote
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&space.name)
        {
            if *key == binding {
                return Ok(engine.clone());
            }
        }
        let engine: Arc<dyn QueryEngine> = Arc::new(FlightEngine::new(
            RemoteSpaceConfig {
                peer: peer.clone(),
                remote_space: remote_space.clone(),
                owner: Did::parse(owner)?,
                token: token.expose().to_string(),
                allow_http: self.allow_http,
            },
            self.identity()?,
        ));
        self.remote
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(space.name.clone(), (binding, engine.clone()));
        Ok(engine)
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
        let engine: Arc<dyn QueryEngine> = Arc::new(oikome_engine_sqlite::SqliteEngine::open(
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

    /// A new, private SQLite connection to `space`'s database.
    ///
    /// Anything that needs a transaction of its own uses one: a session's
    /// `BEGIN … COMMIT`, or a publish snapshot. The shared connection from
    /// [`Self::sqlite_for`] must never hold a transaction, or statements of
    /// other sessions would run inside it. WAL mode lets private readers see
    /// the last committed state while writers keep going.
    #[cfg(feature = "engine-sqlite")]
    pub fn sqlite_private_for(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        let location = self.catalog.get_location(&space.location)?;
        let path = self.catalog.sqlite_path(&location)?;
        Ok(Arc::new(oikome_engine_sqlite::SqliteEngine::open(
            &space.name,
            &path,
        )?))
    }

    #[cfg(not(feature = "engine-sqlite"))]
    pub fn sqlite_private_for(&self, _space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        Err(SessionError::EngineNotRegistered(EngineKind::Sqlite))
    }

    /// The engine a Space is declared to use.
    pub fn engine_for(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        match space.engine {
            EngineKind::DuckDb => self.duckdb(),
            EngineKind::Sqlite => self.sqlite_for(space),
            EngineKind::Flight => self.remote_for(space),
            EngineKind::Host => Err(SessionError::EngineNotRegistered(EngineKind::Host)),
        }
    }
}
