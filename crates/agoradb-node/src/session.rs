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

//! The session: parse → classify → route (architecture v3 §4.4).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use agoradb_catalog::{AgoraCatalog, Space};
use agoradb_core::SpaceKind;
use agoradb_engine::{QualifiedName, QueryEngine, RecordBatchStream, TxHandle};
use agoradb_federation::{BoundSpace, BoundTable, Coordinator};
use agoradb_semantic::{classify, parse_single, AgoraStatement, DmlKind, StatementClass, TclKind};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use sqlparser::ast::{ObjectType, Statement};

use crate::bind::bind_space;
use crate::error::SessionError;
use crate::registry::EngineRegistry;
use crate::result::QueryResult;
use crate::{ddl, dml};

/// Per-session settings.
#[derive(Debug, Clone, Default)]
pub struct SessionConfig {
    /// Space used to qualify bare table names.
    pub default_space: Option<String>,
}

struct OpenTx {
    space: String,
    handle: TxHandle,
}

/// A client session on a node.
pub struct AgoraSession {
    catalog: Arc<AgoraCatalog>,
    engines: Arc<EngineRegistry>,
    default_space: RwLock<Option<String>>,
    tx: Mutex<Option<OpenTx>>,
}

impl std::fmt::Debug for AgoraSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgoraSession")
            .field("default_space", &self.default_space())
            .finish()
    }
}

/// What a classified statement should be sent to an engine as.
struct Routed {
    class: StatementClass,
    stmt: Statement,
    /// The SQL text to hand to an engine: the user's text when qualification
    /// changed nothing, otherwise the re-rendered, fully qualified statement.
    text: String,
}

impl AgoraSession {
    /// Create a session over `catalog` using the node's `engines`.
    pub fn new(
        catalog: Arc<AgoraCatalog>,
        engines: Arc<EngineRegistry>,
        config: SessionConfig,
    ) -> Self {
        Self {
            catalog,
            engines,
            default_space: RwLock::new(config.default_space),
            tx: Mutex::new(None),
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
            AgoraStatement::CreateSpace(request) => {
                self.catalog.create_space(request).await?;
                Ok(QueryResult::Empty)
            }
            AgoraStatement::DropSpace(name) => {
                self.catalog.drop_space(&name)?;
                if self.default_space().as_deref() == Some(name.as_str()) {
                    self.set_default_space(None);
                }
                Ok(QueryResult::Empty)
            }
            AgoraStatement::SetSpace(name) => {
                self.catalog.get_space(&name)?;
                self.set_default_space(Some(name));
                Ok(QueryResult::Empty)
            }
            AgoraStatement::Sql(stmt) => {
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
                        let (schema, stream) = self.run_query(&spaces, &routed.text).await?;
                        let batches = stream.try_collect().await?;
                        Ok(QueryResult::Batches { schema, batches })
                    }
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
            AgoraStatement::Sql(stmt) => {
                let routed = self.route(*stmt, sql)?;
                match &routed.class {
                    StatementClass::Query { spaces } => self.run_query(spaces, &routed.text).await,
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

    fn route(&self, mut stmt: Statement, original: &str) -> Result<Routed, SessionError> {
        let before = stmt.to_string();
        let default = self.default_space();
        let class = classify(&mut stmt, default.as_deref())?;
        let after = stmt.to_string();
        let text = if before == after {
            original.trim().trim_end_matches(';').to_string()
        } else {
            after
        };
        Ok(Routed { class, stmt, text })
    }

    fn space(&self, name: &str) -> Result<Space, SessionError> {
        Ok(self.catalog.get_space(name)?)
    }

    async fn bind(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        bind_space(&self.catalog, &self.engines, space, None).await
    }

    async fn run_query(
        &self,
        spaces: &BTreeSet<String>,
        text: &str,
    ) -> Result<(SchemaRef, RecordBatchStream), SessionError> {
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
            let (engine, pins) = match space.kind {
                SpaceKind::Analytical => {
                    let pins = self.catalog.current_snapshot_ids(&space).await?;
                    let engine =
                        bind_space(&self.catalog, &self.engines, &space, Some(&pins)).await?;
                    (engine, pins)
                }
                SpaceKind::Transactional => (self.bind(&space).await?, Default::default()),
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
                engine,
                tables,
            });
        }
        Ok(Coordinator::new(&bound)?)
    }

    /// The federated physical plan for a query, showing which subtrees are
    /// pushed down to which engine. Works for single-Space queries too.
    pub async fn explain(&self, sql: &str) -> Result<String, SessionError> {
        let AgoraStatement::Sql(stmt) = parse_single(sql)? else {
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
        match space.kind {
            SpaceKind::Transactional => {
                let engine = self.bind(&space).await?;
                engine.execute(&routed.text, &[]).await?;
            }
            SpaceKind::Analytical => match &routed.stmt {
                Statement::CreateTable(create) => {
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
        match space.kind {
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

    async fn run_tcl(&self, kind: TclKind) -> Result<QueryResult, SessionError> {
        match kind {
            TclKind::Begin => {
                let name = self.default_space().ok_or(SessionError::NoDefaultSpace)?;
                let space = self.space(&name)?;
                if space.kind != SpaceKind::Transactional {
                    return Err(SessionError::Unsupported(format!(
                        "BEGIN on analytical space '{name}'"
                    )));
                }
                {
                    let tx = self.tx.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(open) = tx.as_ref() {
                        return Err(SessionError::TransactionOpen(open.space.clone()));
                    }
                }
                let engine = self.bind(&space).await?;
                let handle = engine.begin().await?;
                *self.tx.lock().unwrap_or_else(|p| p.into_inner()) = Some(OpenTx {
                    space: name,
                    handle,
                });
            }
            TclKind::Commit | TclKind::Rollback => {
                let open = self
                    .tx
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .ok_or(SessionError::NoTransaction)?;
                let space = self.space(&open.space)?;
                let engine = self.bind(&space).await?;
                if kind == TclKind::Commit {
                    engine.commit(open.handle).await?;
                } else {
                    engine.rollback(open.handle).await?;
                }
            }
        }
        Ok(QueryResult::Empty)
    }
}
