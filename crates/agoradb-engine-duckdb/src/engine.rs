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
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use agoradb_core::EngineKind;
use agoradb_engine::name::{quote_ident, quote_literal};
use agoradb_engine::stream::DEFAULT_STREAM_CAPACITY;
use agoradb_engine::{
    BlockingWorker, Capabilities, EngineError, QualifiedName, QueryEngine, RecordBatchStream,
    SqlDialect, TableSource, TxHandle, Value,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use duckdb::types::Value as DuckValue;
use duckdb::{Connection, ToSql};

use crate::types::arrow_to_duckdb_type;

/// Tunables for a [`DuckDbEngine`].
#[derive(Debug, Clone, Default)]
pub struct DuckDbConfig {
    /// DuckDB worker threads (`SET threads`). `None` keeps DuckDB's default.
    pub threads: Option<usize>,
    /// Memory limit such as `"4GB"` (`SET memory_limit`).
    pub memory_limit: Option<String>,
    /// Allow `INSTALL <extension>` (which downloads from the network) when an
    /// extension is not present locally. Off by default.
    pub allow_extension_install: bool,
    /// Directory DuckDB loads / installs extensions from.
    pub extension_dir: Option<PathBuf>,
}

/// What is currently attached under a name; used to make re-attaching a no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Attached {
    Parquet(Vec<PathBuf>),
    Sqlite(PathBuf, String),
    Arrow,
}

/// In-memory DuckDB instance shared by all analytical Spaces of a node.
#[derive(Debug)]
pub struct DuckDbEngine {
    worker: BlockingWorker<Connection>,
    instance_id: String,
    attached: Mutex<HashMap<QualifiedName, Attached>>,
    sqlite_ext_loaded: AtomicBool,
    cfg: DuckDbConfig,
}

fn sql_err(e: duckdb::Error) -> EngineError {
    EngineError::Sql(e.to_string())
}

fn to_duck(v: &Value) -> DuckValue {
    match v {
        Value::Null => DuckValue::Null,
        Value::Bool(b) => DuckValue::Boolean(*b),
        Value::Int64(i) => DuckValue::BigInt(*i),
        Value::Float64(f) => DuckValue::Double(*f),
        Value::Text(s) => DuckValue::Text(s.clone()),
        Value::Blob(b) => DuckValue::Blob(b.clone()),
    }
}

fn path_literal(path: &std::path::Path) -> String {
    quote_literal(&path.to_string_lossy())
}

impl DuckDbEngine {
    /// Open a new in-memory DuckDB database.
    pub fn open_in_memory(cfg: DuckDbConfig) -> Result<Self, EngineError> {
        let conn = Connection::open_in_memory().map_err(sql_err)?;
        let mut settings = Vec::new();
        if let Some(threads) = cfg.threads {
            settings.push(format!("SET threads = {threads};"));
        }
        if let Some(limit) = &cfg.memory_limit {
            settings.push(format!("SET memory_limit = {};", quote_literal(limit)));
        }
        if let Some(dir) = &cfg.extension_dir {
            std::fs::create_dir_all(dir)?;
            settings.push(format!("SET extension_directory = {};", path_literal(dir)));
        }
        if !cfg.allow_extension_install {
            // Never touch the network unless explicitly allowed.
            settings.push("SET autoinstall_known_extensions = false;".to_string());
            settings.push("SET autoload_known_extensions = false;".to_string());
        }
        if !settings.is_empty() {
            conn.execute_batch(&settings.join("\n")).map_err(sql_err)?;
        }
        Ok(Self {
            worker: BlockingWorker::new(conn),
            instance_id: format!("duckdb:{}", uuid::Uuid::new_v4()),
            attached: Mutex::new(HashMap::new()),
            sqlite_ext_loaded: AtomicBool::new(false),
            cfg,
        })
    }

    fn already_attached(&self, name: &QualifiedName, what: &Attached) -> bool {
        self.attached
            .lock()
            .map(|m| m.get(name) == Some(what))
            .unwrap_or(false)
    }

    fn remember(&self, name: QualifiedName, what: Attached) {
        if let Ok(mut m) = self.attached.lock() {
            m.insert(name, what);
        }
    }

    async fn run_batch(&self, sql: String) -> Result<(), EngineError> {
        self.worker
            .run(move |conn| conn.execute_batch(&sql).map_err(sql_err))
            .await
    }

    /// Load the `sqlite` extension, installing it first only when allowed.
    async fn ensure_sqlite_extension(&self) -> Result<(), EngineError> {
        if self.sqlite_ext_loaded.load(Ordering::SeqCst) {
            return Ok(());
        }
        let allow_install = self.cfg.allow_extension_install;
        let result = self
            .worker
            .run(move |conn| {
                if let Err(load_err) = conn.execute_batch("LOAD sqlite;") {
                    if !allow_install {
                        return Err(EngineError::ExtensionUnavailable(
                            "sqlite".to_string(),
                            format!("{load_err} (extension install is disabled)"),
                        ));
                    }
                    conn.execute_batch("INSTALL sqlite; LOAD sqlite;")
                        .map_err(|e| {
                            EngineError::ExtensionUnavailable("sqlite".to_string(), e.to_string())
                        })?;
                }
                Ok(())
            })
            .await;
        if result.is_ok() {
            self.sqlite_ext_loaded.store(true, Ordering::SeqCst);
        }
        result
    }

    fn view_over_parquet(
        name: &QualifiedName,
        files: &[PathBuf],
        schema: &SchemaRef,
    ) -> Result<String, EngineError> {
        let mut sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {};\n",
            quote_ident(&name.space)
        );
        if files.is_empty() {
            // A typed, empty relation so the table is still queryable.
            let columns = schema
                .fields()
                .iter()
                .map(|f| {
                    Ok(format!(
                        "CAST(NULL AS {}) AS {}",
                        arrow_to_duckdb_type(f.data_type())?,
                        quote_ident(f.name())
                    ))
                })
                .collect::<Result<Vec<_>, EngineError>>()?;
            sql.push_str(&format!(
                "CREATE OR REPLACE VIEW {} AS SELECT {} WHERE false;",
                name.quoted(),
                columns.join(", ")
            ));
        } else {
            let list = files
                .iter()
                .map(|p| path_literal(p))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet([{}]);",
                name.quoted(),
                list
            ));
        }
        Ok(sql)
    }
}

#[async_trait]
impl QueryEngine for DuckDbEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::DuckDb
    }

    fn dialect(&self) -> SqlDialect {
        SqlDialect::DuckDb
    }

    fn capabilities(&self) -> Capabilities {
        let mut caps = Capabilities::OLAP
            | Capabilities::READ_PARQUET
            | Capabilities::ARROW_OUT
            | Capabilities::PUSHDOWN_JOIN
            | Capabilities::PUSHDOWN_AGG;
        if self.sqlite_ext_loaded.load(Ordering::SeqCst) {
            caps |= Capabilities::READ_SQLITE;
        }
        caps
    }

    fn instance_id(&self) -> &str {
        &self.instance_id
    }

    async fn attach(&self, name: &QualifiedName, source: TableSource) -> Result<(), EngineError> {
        match source {
            TableSource::ParquetFiles { files, schema } => {
                let what = Attached::Parquet(files.clone());
                if self.already_attached(name, &what) {
                    return Ok(());
                }
                let sql = Self::view_over_parquet(name, &files, &schema)?;
                self.run_batch(sql).await?;
                self.remember(name.clone(), what);
                Ok(())
            }
            TableSource::SqliteFile { path, table } => {
                let what = Attached::Sqlite(path.clone(), table.clone());
                if self.already_attached(name, &what) {
                    return Ok(());
                }
                self.ensure_sqlite_extension().await?;
                let alias = format!("{}__sqlite", name.space);
                let sql = format!(
                    "ATTACH IF NOT EXISTS {} AS {} (TYPE sqlite, READ_ONLY);\n\
                     CREATE SCHEMA IF NOT EXISTS {};\n\
                     CREATE OR REPLACE VIEW {} AS SELECT * FROM {}.{};",
                    path_literal(&path),
                    quote_ident(&alias),
                    quote_ident(&name.space),
                    name.quoted(),
                    quote_ident(&alias),
                    quote_ident(&table)
                );
                self.run_batch(sql).await?;
                self.remember(name.clone(), what);
                Ok(())
            }
            TableSource::ArrowBatches { schema, batches } => {
                let columns = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        Ok(format!(
                            "{} {}",
                            quote_ident(f.name()),
                            arrow_to_duckdb_type(f.data_type())?
                        ))
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?;
                let ddl = format!(
                    "CREATE SCHEMA IF NOT EXISTS {};\nCREATE OR REPLACE TABLE {} ({});",
                    quote_ident(&name.space),
                    name.quoted(),
                    columns.join(", ")
                );
                let (space, table) = (name.space.clone(), name.table.clone());
                self.worker
                    .run(move |conn| {
                        conn.execute_batch(&ddl).map_err(sql_err)?;
                        let mut appender = conn.appender_to_db(&table, &space).map_err(sql_err)?;
                        for batch in &batches {
                            appender
                                .append_record_batch(batch.clone())
                                .map_err(sql_err)?;
                        }
                        appender.flush().map_err(sql_err)?;
                        Ok(())
                    })
                    .await?;
                self.remember(name.clone(), Attached::Arrow);
                Ok(())
            }
        }
    }

    async fn detach(&self, name: &QualifiedName) -> Result<(), EngineError> {
        let sql = format!(
            "DROP VIEW IF EXISTS {n}; DROP TABLE IF EXISTS {n};",
            n = name.quoted()
        );
        self.run_batch(sql).await?;
        if let Ok(mut m) = self.attached.lock() {
            m.remove(name);
        }
        Ok(())
    }

    async fn table_names(&self, space: &str) -> Result<Vec<String>, EngineError> {
        let space = space.to_string();
        self.worker
            .run(move |conn| {
                let mut stmt = conn
                    .prepare(
                        "SELECT table_name FROM information_schema.tables \
                         WHERE table_schema = ? ORDER BY table_name",
                    )
                    .map_err(sql_err)?;
                let names = stmt
                    .query_map([space], |row| row.get::<_, String>(0))
                    .map_err(sql_err)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_err)?;
                Ok(names)
            })
            .await
    }

    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        let sql = format!("SELECT * FROM {} LIMIT 0", name.quoted());
        let display = name.to_string();
        self.worker
            .run(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("does not exist") {
                        EngineError::TableNotFound(display.clone())
                    } else {
                        EngineError::Sql(msg)
                    }
                })?;
                let arrow = stmt.query_arrow([]).map_err(sql_err)?;
                Ok(arrow.get_schema())
            })
            .await
    }

    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        let sql = sql.to_string();
        let params: Vec<DuckValue> = params.iter().map(to_duck).collect();
        self.worker
            .query_stream(DEFAULT_STREAM_CAPACITY, move |conn, sink| {
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let bound: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
                let arrow = stmt.query_arrow(bound.as_slice()).map_err(sql_err)?;
                sink.start(arrow.get_schema())?;
                for batch in arrow {
                    sink.send(batch)?;
                }
                Ok(())
            })
            .await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, EngineError> {
        let sql = sql.to_string();
        let params: Vec<DuckValue> = params.iter().map(to_duck).collect();
        self.worker
            .run(move |conn| {
                let bound: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
                let n = conn.execute(&sql, bound.as_slice()).map_err(sql_err)?;
                Ok(n as u64)
            })
            .await
    }

    async fn begin(&self) -> Result<TxHandle, EngineError> {
        Err(EngineError::Unsupported(
            EngineKind::DuckDb,
            "transactions are reserved for transactional Spaces".to_string(),
        ))
    }

    async fn commit(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(EngineError::Unsupported(
            EngineKind::DuckDb,
            "transactions are reserved for transactional Spaces".to_string(),
        ))
    }

    async fn rollback(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(EngineError::Unsupported(
            EngineKind::DuckDb,
            "transactions are reserved for transactional Spaces".to_string(),
        ))
    }
}
