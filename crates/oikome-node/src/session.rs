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

//! The session: parse → classify → route (architecture v3 §4.4).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use oikome_catalog::{LocationFormat, OikomeCatalog, Space};
use oikome_core::{AccessMode, EngineKind, RemoteMode, SpaceKind};
use oikome_engine::name::quote_ident;
use oikome_engine::{Capabilities, QualifiedName, QueryEngine, RecordBatchStream, TxHandle};
use oikome_federation::{BoundSpace, BoundTable, Coordinator};
use oikome_semantic::{
    apply_access_control, bind_search_tables, classify, collect_spaces, inline_views, parse_single,
    search_tables, uses_match, DmlKind, OikomeStatement, StatementClass, TclKind,
    VirtualTableRequest,
};
use sqlparser::ast::{ObjectType, Statement};

use crate::acl::{CatalogAccess, CatalogViews};
use crate::bind::bind_space;
use crate::error::SessionError;
use crate::lance::{dataset_path, index_sql, lance_root};
use crate::registry::EngineRegistry;
use crate::result::QueryResult;
use crate::{ddl, dml};

/// Per-session settings.
#[derive(Debug, Clone, Default)]
pub struct SessionConfig {
    /// Space used to qualify bare table names.
    pub default_space: Option<String>,
    /// Who the session acts for. `None` is the node owner (full access);
    /// `Some` is a peer principal (a DID in 3.1): read-only, and it only sees
    /// what `GRANT` / `CREATE POLICY` give it.
    pub principal: Option<String>,
}

/// A session's open transaction, on a private connection to its Space.
struct OpenTx {
    space: String,
    handle: TxHandle,
    engine: Arc<dyn QueryEngine>,
}

/// A client session on a node.
pub struct OikomeSession {
    pub(crate) catalog: Arc<OikomeCatalog>,
    engines: Arc<EngineRegistry>,
    principal: Option<String>,
    default_space: RwLock<Option<String>>,
    tx: Mutex<Option<OpenTx>>,
}

impl std::fmt::Debug for OikomeSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OikomeSession")
            .field("principal", &self.principal)
            .field("default_space", &self.default_space())
            .finish()
    }
}

/// What a classified statement should be sent to an engine as.
pub(crate) struct Routed {
    pub(crate) class: StatementClass,
    pub(crate) stmt: Statement,
    /// The SQL text to hand to an engine: the user's text when qualification
    /// changed nothing, otherwise the re-rendered, fully qualified statement.
    text: String,
    /// What the engine must be able to do for the statement's searches:
    /// Lance search functions (§7.6) or `MATCH` on FTS5 / vec0 tables (§8).
    needs: Capabilities,
}

impl OikomeSession {
    /// Create a session over `catalog` using the node's `engines`.
    pub fn new(
        catalog: Arc<OikomeCatalog>,
        engines: Arc<EngineRegistry>,
        config: SessionConfig,
    ) -> Self {
        Self {
            catalog,
            engines,
            principal: config.principal,
            default_space: RwLock::new(config.default_space),
            tx: Mutex::new(None),
        }
    }

    /// The principal this session acts for; `None` is the node owner.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    pub(crate) fn deny_principal(&self, what: &str) -> Result<(), SessionError> {
        match &self.principal {
            Some(p) => Err(SessionError::PermissionDenied(format!(
                "principal '{p}' is read-only and cannot run {what}"
            ))),
            None => Ok(()),
        }
    }

    /// The Space bare table names resolve against.
    pub fn default_space(&self) -> Option<String> {
        self.default_space
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Set (or clear) the default Space.
    pub fn set_default_space(&self, space: Option<String>) {
        *self
            .default_space
            .write()
            .unwrap_or_else(|p| p.into_inner()) = space;
    }

    /// Execute one statement and collect its result.
    pub async fn sql(&self, sql: &str) -> Result<QueryResult, SessionError> {
        match parse_single(sql)? {
            OikomeStatement::CreateSpace(request) => {
                self.deny_principal("CREATE SPACE")?;
                if request.kind == Some(SpaceKind::Remote) {
                    return match request.mode.unwrap_or_default() {
                        RemoteMode::Query => self.create_remote_space(request),
                        RemoteMode::Subscribe => self.create_subscribed_space(request).await,
                    };
                }
                self.catalog.create_space(request).await?;
                Ok(QueryResult::Empty)
            }
            OikomeStatement::ShowIdentity => self.show_identity(),
            OikomeStatement::IssueUcan(request) => {
                self.deny_principal("ISSUE UCAN")?;
                self.issue_ucan(&request)
            }
            OikomeStatement::RevokeUcan(id) => {
                self.deny_principal("REVOKE UCAN")?;
                self.revoke_ucan(&id)
            }
            OikomeStatement::ShowUcans => {
                self.deny_principal("SHOW UCANS")?;
                self.show_ucans()
            }
            OikomeStatement::ShowWebrtc => {
                self.deny_principal("SHOW WEBRTC")?;
                self.show_webrtc()
            }
            OikomeStatement::ShowPeers => {
                self.deny_principal("SHOW PEERS")?;
                self.show_peers()
            }
            OikomeStatement::CreateVirtualTable(request) => {
                self.deny_principal("CREATE VIRTUAL TABLE")?;
                self.create_virtual_table(request).await
            }
            OikomeStatement::CreatePropertyGraph(request) => {
                self.deny_principal("CREATE PROPERTY GRAPH")?;
                self.create_property_graph(request).await
            }
            OikomeStatement::DropPropertyGraph(request) => {
                self.deny_principal("DROP PROPERTY GRAPH")?;
                self.drop_property_graph(request).await
            }
            OikomeStatement::GraphQuery(query) => self.graph_query(query).await,
            OikomeStatement::DropSpace(name) => {
                self.deny_principal("DROP SPACE")?;
                let space = self.space(&name)?;
                let format = self.catalog.get_location(&space.location).map(|l| l.format);
                let replica = matches!(format, Ok(LocationFormat::Replica { .. }));
                if matches!(format, Ok(LocationFormat::Lance { .. })) || replica {
                    // A Lance Space (or a copy of one) is attached whole.
                    self.engines.duckdb()?.detach_space(&name).await?;
                }
                self.catalog.drop_space(&name)?;
                if self.default_space().as_deref() == Some(name.as_str()) {
                    self.set_default_space(None);
                }
                if replica {
                    self.remove_replica(&space.location).await?;
                }
                Ok(QueryResult::Empty)
            }
            OikomeStatement::PublishSpace(request) => self.publish(&request).await?.into_result(),
            OikomeStatement::RefreshSpace(name) => self.refresh_space(&name).await?.into_result(),
            OikomeStatement::SetSpace(name) => {
                // A principal cannot tell a Space it has no grant in from one
                // that does not exist.
                if let Some(p) = &self.principal {
                    if !self.catalog.has_grants_in(p, &name) {
                        return Err(oikome_core::CatalogError::SpaceNotFound(name).into());
                    }
                }
                self.catalog.get_space(&name)?;
                self.set_default_space(Some(name));
                Ok(QueryResult::Empty)
            }
            OikomeStatement::Sql(stmt) => {
                let routed = self.route(*stmt, sql)?;
                match routed.class.clone() {
                    StatementClass::Tcl(kind) => self.run_tcl(kind).await,
                    StatementClass::TableDdl { space } => self.run_table_ddl(&space, routed).await,
                    StatementClass::Dml {
                        space,
                        kind,
                        spaces,
                    } => self.run_dml(&space, kind, &spaces, routed).await,
                    StatementClass::Query { spaces } => {
                        self.check_search(&routed, &spaces).await?;
                        let (schema, stream) = self.run_query(&spaces, &routed.text).await?;
                        let batches = stream.try_collect().await?;
                        Ok(QueryResult::Batches { schema, batches })
                    }
                    StatementClass::ViewDdl => self.run_view_ddl(routed).await,
                    StatementClass::Acl => self.run_acl(routed).await,
                }
            }
        }
    }

    /// Execute a query and stream its result.
    pub async fn sql_stream(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, RecordBatchStream), SessionError> {
        match parse_single(sql)? {
            OikomeStatement::Sql(stmt) => {
                let routed = self.route(*stmt, sql)?;
                match &routed.class {
                    StatementClass::Query { spaces } => {
                        self.check_search(&routed, spaces).await?;
                        self.run_query(spaces, &routed.text).await
                    }
                    _ => Err(SessionError::Unsupported(
                        "sql_stream only accepts queries".to_string(),
                    )),
                }
            }
            _ => Err(SessionError::Unsupported(
                "sql_stream only accepts queries".to_string(),
            )),
        }
    }

    /// Classify, then apply the semantic layer: access control for a
    /// principal, then view inlining. `spaces` is recomputed afterwards so it
    /// names the Spaces of the base tables actually read.
    fn route(&self, mut stmt: Statement, original: &str) -> Result<Routed, SessionError> {
        let before = stmt.to_string();
        let default = self.default_space();
        let mut class = classify(&mut stmt, default.as_deref())?;
        let searches = !search_tables(&stmt).is_empty();
        let matches = uses_match(&stmt);
        if let Some(principal) = &self.principal {
            if !matches!(class, StatementClass::Query { .. }) {
                return Err(SessionError::PermissionDenied(format!(
                    "principal '{principal}' is read-only"
                )));
            }
            if searches || matches {
                // A search reads the dataset itself, past column grants and
                // row policies.
                return Err(SessionError::PermissionDenied(format!(
                    "principal '{principal}' cannot call search functions; \
                     subscribe to the space to search a copy of it"
                )));
            }
            apply_access_control(&mut stmt, principal, &CatalogAccess(&self.catalog))?;
        }
        if let StatementClass::Query { spaces }
        | StatementClass::Dml {
            kind: DmlKind::Insert,
            spaces,
            ..
        } = &mut class
        {
            inline_views(&mut stmt, &CatalogViews(&self.catalog))?;
            *spaces = collect_spaces(&stmt);
        }
        if searches {
            self.bind_search_calls(&mut stmt, &class)?;
        }
        if matches {
            self.check_match_spaces(&class)?;
        }
        let needs = match (searches, matches) {
            (true, _) => Capabilities::READ_LANCE,
            (false, true) => Capabilities::EXT_FTS | Capabilities::EXT_VECTOR,
            (false, false) => Capabilities::empty(),
        };
        let after = stmt.to_string();
        let text = if before == after {
            original.trim().trim_end_matches(';').to_string()
        } else {
            after
        };
        Ok(Routed {
            class,
            stmt,
            text,
            needs,
        })
    }

    /// `MATCH` searches FTS5 and vec0 tables, which live in SQLite: a
    /// statement using it runs on one transactional Space.
    fn check_match_spaces(&self, class: &StatementClass) -> Result<(), SessionError> {
        let spaces = match class {
            StatementClass::Query { spaces } | StatementClass::Dml { spaces, .. } => spaces,
            _ => return Ok(()),
        };
        let mut iter = spaces.iter();
        match (iter.next(), iter.next()) {
            (Some(name), None) if self.space(name)?.kind == SpaceKind::Transactional => Ok(()),
            _ => Err(SessionError::Unsupported(
                "MATCH searches the FTS5 and vec0 tables of one transactional space".to_string(),
            )),
        }
    }

    /// `CREATE VIRTUAL TABLE ... USING fts5(...) | vec0(...)` in a
    /// transactional Space; the module arguments go to SQLite as written.
    async fn create_virtual_table(
        &self,
        request: VirtualTableRequest,
    ) -> Result<QueryResult, SessionError> {
        let name = match request.space {
            Some(space) => space,
            None => self.default_space().ok_or(SessionError::NoDefaultSpace)?,
        };
        let space = self.space(&name)?;
        if space.kind != SpaceKind::Transactional {
            return Err(SessionError::Unsupported(format!(
                "virtual tables (FTS5, vec0) belong to transactional spaces; '{name}' is {}",
                space.kind
            )));
        }
        self.require_writable(&space)?;
        let sql = format!(
            "CREATE VIRTUAL TABLE {}{}.{} USING {}({})",
            if request.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            quote_ident(&name),
            quote_ident(&request.table),
            request.module,
            request.args
        );
        self.bind(&space).await?.execute(&sql, &[]).await?;
        Ok(QueryResult::Empty)
    }

    /// Replace each search call's table with its dataset path. A search runs
    /// in a query over one Lance Space (or a copy of one), on DuckDB.
    fn bind_search_calls(
        &self,
        stmt: &mut Statement,
        class: &StatementClass,
    ) -> Result<(), SessionError> {
        let StatementClass::Query { spaces } = class else {
            return Err(SessionError::Unsupported(
                "search functions outside a query".to_string(),
            ));
        };
        if spaces.len() != 1 {
            return Err(SessionError::Unsupported(format!(
                "a search across several spaces ({}); search one space per query",
                spaces.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
        bind_search_tables(stmt, |space, table| {
            let space = self.space(space)?;
            let root = lance_root(&self.catalog, &space)?.ok_or_else(|| {
                SessionError::Unsupported(format!(
                    "search functions read Lance spaces; '{}' is not stored as Lance",
                    space.name
                ))
            })?;
            Ok(dataset_path(&root, &space.name, table)?
                .to_string_lossy()
                .into_owned())
        })
    }

    /// A search needs its extension on the Space's engine: `lance` for the
    /// search functions (binding the Space loads it, or fails with how to
    /// install it), FTS5 and sqlite-vec for `MATCH`.
    async fn check_search(
        &self,
        routed: &Routed,
        spaces: &BTreeSet<String>,
    ) -> Result<(), SessionError> {
        if routed.needs.is_empty() {
            return Ok(());
        }
        for name in spaces {
            let engine = self.bind(&self.space(name)?).await?;
            if !engine.capabilities().contains(routed.needs) {
                return Err(SessionError::Unsupported(format!(
                    "this search needs {:?}, which engine {} lacks",
                    routed.needs,
                    engine.kind()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn space(&self, name: &str) -> Result<Space, SessionError> {
        Ok(self.catalog.get_space(name)?)
    }

    /// Local directory for staging Parquet files.
    pub(crate) fn temp_dir(&self) -> &std::path::Path {
        self.engines.temp_dir()
    }

    /// The node's engines.
    pub(crate) fn engines(&self) -> &EngineRegistry {
        &self.engines
    }

    fn remote_is_read_only(space: &Space) -> SessionError {
        SessionError::Unsupported(format!(
            "space '{}' is a remote space and read-only",
            space.name
        ))
    }

    /// Published Spaces are written only by `PUBLISH SPACE <source>`, and a
    /// read-only binding of a Location never writes to it.
    fn require_writable(&self, space: &Space) -> Result<(), SessionError> {
        if space.access == AccessMode::ReadOnly {
            return Err(SessionError::ReadOnlySpace(space.name.clone()));
        }
        match &space.published_from {
            Some(origin) => Err(SessionError::PublishedSpace {
                space: space.name.clone(),
                origin: origin.clone(),
            }),
            None => Ok(()),
        }
    }

    /// The engine for `space`: this session's transaction connection while a
    /// transaction is open on it, the node's shared engine otherwise.
    pub(crate) async fn bind(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        if let Some(engine) = self.tx_engine(&space.name) {
            return Ok(engine);
        }
        bind_space(&self.catalog, &self.engines, space, None).await
    }

    fn tx_engine(&self, space: &str) -> Option<Arc<dyn QueryEngine>> {
        self.tx
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .filter(|open| open.space == space)
            .map(|open| open.engine.clone())
    }

    pub(crate) async fn run_query(
        &self,
        spaces: &BTreeSet<String>,
        text: &str,
    ) -> Result<(SchemaRef, RecordBatchStream), SessionError> {
        let (mut remote, mut shipped) = (false, false);
        for name in spaces {
            let space = self.space(name)?;
            remote |= space.kind == SpaceKind::Remote;
            // A subscribed copy is local data on DuckDB; only queries on
            // a Space queried over Flight are shipped to its owner.
            shipped |= space.kind == SpaceKind::Remote && space.engine == EngineKind::Flight;
        }
        if remote {
            // A node never forwards a peer's query to a third node, nor
            // serves another node's data onwards: the capability it was
            // presented covers this node's Spaces only.
            if let Some(p) = &self.principal {
                return Err(SessionError::PermissionDenied(format!(
                    "principal '{p}' cannot query remote spaces through this node"
                )));
            }
        }
        if shipped {
            // Always federate, so the SQL each owner receives names its
            // tables by their names on the owner.
            let coordinator = self.federate(spaces).await?;
            return Ok(coordinator.run(text).await?);
        }
        let engine = match spaces.len() {
            // `SELECT 1`: any engine will do; prefer the analytical one.
            0 => self.engines.duckdb()?,
            1 => {
                let space = self.space(spaces.iter().next().map(String::as_str).unwrap_or(""))?;
                self.bind(&space).await?
            }
            _ => {
                let coordinator = self.federate(spaces).await?;
                return Ok(coordinator.run(text).await?);
            }
        };
        Ok(engine.query(text, &[]).await?)
    }

    /// Bind every Space (analytical ones pinned to their current snapshots)
    /// and build a federation coordinator over them.
    async fn federate(&self, spaces: &BTreeSet<String>) -> Result<Coordinator, SessionError> {
        let mut bound = Vec::with_capacity(spaces.len());
        for name in spaces {
            let space = self.space(name)?;
            let iceberg = self
                .catalog
                .get_location(&space.location)
                .is_ok_and(|l| matches!(l.format, LocationFormat::IcebergParquet { .. }));
            let (engine, pins) = match space.kind {
                // Iceberg tables are pinned to their current snapshots; Lance
                // and SQLite tables are read at their latest state (§5.4).
                SpaceKind::Analytical if iceberg => {
                    let pins = self.catalog.current_snapshot_ids(&space).await?;
                    let engine =
                        bind_space(&self.catalog, &self.engines, &space, Some(&pins)).await?;
                    (engine, pins)
                }
                SpaceKind::Analytical => (self.bind(&space).await?, Default::default()),
                SpaceKind::Transactional => (self.bind(&space).await?, Default::default()),
                SpaceKind::Remote => {
                    if self.principal.is_some() {
                        return Err(SessionError::PermissionDenied(
                            "remote spaces are not served to peers".to_string(),
                        ));
                    }
                    // A subscribed copy is attached to the local DuckDB.
                    (self.bind(&space).await?, Default::default())
                }
            };
            // Queries shipped to an owner name its tables as the owner does.
            let engine_space = match &space.uri {
                Some(uri) if space.engine == EngineKind::Flight => uri.name.clone(),
                _ => name.clone(),
            };
            let mut tables = Vec::new();
            for table in engine.table_names(name).await? {
                let schema = engine
                    .table_schema(&QualifiedName::new(name, &table))
                    .await?;
                let snapshot_id = pins.get(&table).copied().flatten();
                tables.push(BoundTable {
                    name: table,
                    schema,
                    snapshot_id,
                });
            }
            bound.push(BoundSpace {
                space: name.clone(),
                engine_space,
                engine,
                tables,
            });
        }
        Ok(Coordinator::new(&bound)?)
    }

    /// The federated physical plan for a query, showing which subtrees are
    /// pushed down to which engine. Works for single-Space queries too.
    pub async fn explain(&self, sql: &str) -> Result<String, SessionError> {
        let OikomeStatement::Sql(stmt) = parse_single(sql)? else {
            return Err(SessionError::Unsupported(
                "explain only accepts queries".to_string(),
            ));
        };
        let routed = self.route(*stmt, sql)?;
        let StatementClass::Query { spaces } = &routed.class else {
            return Err(SessionError::Unsupported(
                "explain only accepts queries".to_string(),
            ));
        };
        let coordinator = self.federate(spaces).await?;
        Ok(coordinator.explain(&routed.text).await?)
    }

    async fn run_table_ddl(
        &self,
        space_name: &str,
        routed: Routed,
    ) -> Result<QueryResult, SessionError> {
        let space = self.space(space_name)?;
        if space.kind == SpaceKind::Remote {
            return Err(Self::remote_is_read_only(&space));
        }
        self.require_writable(&space)?;
        let index = matches!(
            routed.stmt,
            Statement::CreateIndex(_)
                | Statement::Drop {
                    object_type: ObjectType::Index,
                    ..
                }
        );
        if let Some(root) = lance_root(&self.catalog, &space)? {
            return self.run_lance_ddl(&space, &root, index, routed).await;
        }
        if index {
            return Err(SessionError::Unsupported(format!(
                "indexes on {} space '{}'; indexes are for Lance spaces (FORMAT = 'lance')",
                space.kind, space.name
            )));
        }
        match space.kind {
            SpaceKind::Remote => return Err(Self::remote_is_read_only(&space)),
            SpaceKind::Transactional => {
                let engine = self.bind(&space).await?;
                engine.execute(&routed.text, &[]).await?;
            }
            SpaceKind::Analytical => match &routed.stmt {
                Statement::CreateTable(create) => {
                    let name = ddl::table_name(&create.name);
                    if self.catalog.get_view(space_name, &name).is_some() {
                        return Err(SessionError::AlreadyExists(format!(
                            "a view named {space_name}.{name} already exists"
                        )));
                    }
                    ddl::create_analytical_table(&self.catalog, &space, create).await?
                }
                Statement::Drop {
                    object_type: ObjectType::Table,
                    names,
                    if_exists,
                    ..
                } => ddl::drop_analytical_tables(&self.catalog, &space, names, *if_exists).await?,
                other => {
                    return Err(SessionError::Unsupported(format!(
                        "{} on an analytical space",
                        other
                            .to_string()
                            .split_whitespace()
                            .take(2)
                            .collect::<Vec<_>>()
                            .join(" ")
                    )))
                }
            },
        }
        Ok(QueryResult::Empty)
    }

    async fn run_dml(
        &self,
        space_name: &str,
        kind: DmlKind,
        spaces: &BTreeSet<String>,
        routed: Routed,
    ) -> Result<QueryResult, SessionError> {
        let space = self.space(space_name)?;
        if space.kind == SpaceKind::Remote {
            return Err(Self::remote_is_read_only(&space));
        }
        self.require_writable(&space)?;
        if lance_root(&self.catalog, &space)?.is_some() {
            // Each statement commits one Lance version.
            self.bind_duckdb_sources(space_name, spaces).await?;
            let engine = self.bind(&space).await?;
            return Ok(QueryResult::RowsAffected(
                engine.execute(&routed.text, &[]).await?,
            ));
        }
        match space.kind {
            SpaceKind::Remote => Err(Self::remote_is_read_only(&space)),
            SpaceKind::Transactional if kind == DmlKind::Merge => Err(SessionError::Unsupported(
                format!("MERGE on transactional space '{space_name}'"),
            )),
            SpaceKind::Transactional => {
                if spaces.iter().any(|s| s != space_name) {
                    return Err(SessionError::MultiSpaceNotSupported(
                        spaces.iter().cloned().collect(),
                    ));
                }
                let engine = self.bind(&space).await?;
                Ok(QueryResult::RowsAffected(
                    engine.execute(&routed.text, &[]).await?,
                ))
            }
            SpaceKind::Analytical => match (kind, &routed.stmt) {
                (DmlKind::Insert, Statement::Insert(insert)) => {
                    // Bind every analytical Space the source reads from; the
                    // target table itself is attached by bind() as well.
                    let mut source_engine = self.bind(&space).await?;
                    for other in spaces.iter().filter(|s| *s != space_name) {
                        let other_space = self.space(other)?;
                        if other_space.kind != SpaceKind::Analytical {
                            return Err(SessionError::MultiSpaceNotSupported(
                                spaces.iter().cloned().collect(),
                            ));
                        }
                        source_engine = self.bind(&other_space).await?;
                    }
                    let rows = dml::insert_analytical(
                        &self.catalog,
                        &space,
                        insert,
                        &source_engine,
                        self.engines.temp_dir(),
                    )
                    .await?;
                    // Make the new snapshot visible to subsequent queries.
                    self.bind(&space).await?;
                    Ok(QueryResult::RowsAffected(rows))
                }
                _ => Err(SessionError::Unsupported(format!(
                    "{kind:?} on an analytical space (append-only in 3.0)"
                ))),
            },
        }
    }

    /// DDL on a Lance Space: tables through DuckDB's attached namespace,
    /// indexes on the dataset they cover.
    async fn run_lance_ddl(
        &self,
        space: &Space,
        root: &std::path::Path,
        index: bool,
        routed: Routed,
    ) -> Result<QueryResult, SessionError> {
        let engine = self.bind(space).await?;
        if index {
            let table = match &routed.stmt {
                Statement::CreateIndex(create) => ddl::table_name(&create.table_name),
                Statement::Drop {
                    table: Some(table), ..
                } => ddl::table_name(table),
                _ => unreachable!("an index statement names its table"),
            };
            let sql = index_sql(&routed.stmt, &dataset_path(root, &space.name, &table)?)?;
            engine.execute(&sql, &[]).await?;
            return Ok(QueryResult::Empty);
        }
        if let Statement::CreateTable(create) = &routed.stmt {
            let name = ddl::table_name(&create.name);
            if self.catalog.get_view(&space.name, &name).is_some() {
                return Err(SessionError::AlreadyExists(format!(
                    "a view named {}.{name} already exists",
                    space.name
                )));
            }
        }
        // CREATE TABLE ... AS SELECT may read other Spaces on DuckDB.
        self.bind_duckdb_sources(&space.name, &collect_spaces(&routed.stmt))
            .await?;
        engine.execute(&routed.text, &[]).await?;
        Ok(QueryResult::Empty)
    }

    /// Bind every Space other than `target` that a statement reads, all of
    /// which must live on DuckDB.
    async fn bind_duckdb_sources(
        &self,
        target: &str,
        spaces: &BTreeSet<String>,
    ) -> Result<(), SessionError> {
        for name in spaces.iter().filter(|s| *s != target) {
            let space = self.space(name)?;
            if space.engine != EngineKind::DuckDb {
                return Err(SessionError::MultiSpaceNotSupported(
                    spaces.iter().cloned().collect(),
                ));
            }
            self.bind(&space).await?;
        }
        Ok(())
    }

    async fn run_tcl(&self, kind: TclKind) -> Result<QueryResult, SessionError> {
        match kind {
            TclKind::Begin => {
                let name = self.default_space().ok_or(SessionError::NoDefaultSpace)?;
                let space = self.space(&name)?;
                if space.kind != SpaceKind::Transactional {
                    return Err(SessionError::Unsupported(format!(
                        "BEGIN on {} space '{name}'",
                        space.kind
                    )));
                }
                {
                    let tx = self.tx.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(open) = tx.as_ref() {
                        return Err(SessionError::TransactionOpen(open.space.clone()));
                    }
                }
                let engine = self.engines.sqlite_private_for(&space)?;
                let handle = engine.begin().await?;
                *self.tx.lock().unwrap_or_else(|p| p.into_inner()) = Some(OpenTx {
                    space: name,
                    handle,
                    engine,
                });
            }
            TclKind::Commit | TclKind::Rollback => {
                let open = self
                    .tx
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .ok_or(SessionError::NoTransaction)?;
                if kind == TclKind::Commit {
                    open.engine.commit(open.handle).await?;
                } else {
                    open.engine.rollback(open.handle).await?;
                }
            }
        }
        Ok(QueryResult::Empty)
    }
}
