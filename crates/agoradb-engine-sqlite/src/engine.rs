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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use agoradb_core::{EngineKind, ENGINE_BATCH_ROWS};
use agoradb_engine::name::quote_ident;
use agoradb_engine::stream::DEFAULT_STREAM_CAPACITY;
use agoradb_engine::{
    BlockingWorker, Capabilities, EngineError, QualifiedName, QueryEngine, RecordBatchStream,
    SqlDialect, TableSource, TxHandle, Value,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rusqlite::types::{ToSqlOutput, Value as SqlValue, ValueRef};
use rusqlite::{Connection, OpenFlags, ToSql};

use crate::convert::{affinity_type, infer_from_value, ColumnType, RowBatcher};

/// How long a statement waits for another connection's write lock.
pub const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// SQLite engine serving exactly one transactional Space.
///
/// The connection's `main` database is an empty in-memory database; the
/// Space's file is attached under the Space name so that `<space>.<table>`
/// resolves natively. The file is put into WAL mode on first open.
#[derive(Debug)]
pub struct SqliteEngine {
    space: String,
    path: PathBuf,
    worker: BlockingWorker<Connection>,
    instance_id: String,
    in_tx: AtomicBool,
    next_tx_id: AtomicU64,
}

/// Wrapper so [`Value`] can be bound as a SQLite parameter.
struct Param<'a>(&'a Value);

impl ToSql for Param<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self.0 {
            Value::Null => ToSqlOutput::Owned(SqlValue::Null),
            Value::Bool(b) => ToSqlOutput::Owned(SqlValue::Integer(i64::from(*b))),
            Value::Int64(i) => ToSqlOutput::Owned(SqlValue::Integer(*i)),
            Value::Float64(f) => ToSqlOutput::Owned(SqlValue::Real(*f)),
            Value::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            Value::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
        })
    }
}

fn sql_err(e: rusqlite::Error) -> EngineError {
    EngineError::Sql(e.to_string())
}

fn owned_value(v: ValueRef<'_>) -> SqlValue {
    match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(i) => SqlValue::Integer(i),
        ValueRef::Real(r) => SqlValue::Real(r),
        ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

impl SqliteEngine {
    /// Open (creating if necessary) the database file at `path` for `space`.
    pub fn open(space: &str, path: &Path) -> Result<Self, EngineError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open_in_memory_with_flags(
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sql_err)?;
        let path_str = path.to_string_lossy().into_owned();
        conn.execute(
            &format!("ATTACH DATABASE ?1 AS {}", quote_ident(space)),
            [&path_str],
        )
        .map_err(sql_err)?;
        conn.execute_batch(&format!(
            "PRAGMA {}.journal_mode=WAL; PRAGMA foreign_keys=ON;",
            quote_ident(space)
        ))
        .map_err(sql_err)?;
        // Several connections may write the same file (one per open
        // transaction); wait for the write lock instead of failing at once.
        conn.busy_timeout(BUSY_TIMEOUT).map_err(sql_err)?;

        Ok(Self {
            space: space.to_string(),
            path: path.to_path_buf(),
            worker: BlockingWorker::new(conn),
            instance_id: format!("sqlite:{}:{}", space, uuid::Uuid::new_v4()),
            in_tx: AtomicBool::new(false),
            next_tx_id: AtomicU64::new(1),
        })
    }

    /// The Space this engine serves.
    pub fn space(&self) -> &str {
        &self.space
    }

    /// The database file this engine serves.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn check_space(&self, name: &QualifiedName) -> Result<(), EngineError> {
        if name.space == self.space {
            Ok(())
        } else {
            Err(EngineError::TableNotFound(name.to_string()))
        }
    }

    fn take_tx(&self, tx: TxHandle) -> Result<(), EngineError> {
        if !self.in_tx.load(Ordering::SeqCst) {
            return Err(EngineError::Transaction(
                "no transaction is open".to_string(),
            ));
        }
        let expected = self.next_tx_id.load(Ordering::SeqCst) - 1;
        if tx.id() != expected {
            return Err(EngineError::Transaction(format!(
                "handle {} does not match the open transaction {}",
                tx.id(),
                expected
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl QueryEngine for SqliteEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Sqlite
    }

    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::OLTP
            | Capabilities::TRANSACTIONS
            | Capabilities::READ_SQLITE
            | Capabilities::ARROW_OUT
            | Capabilities::PUSHDOWN_JOIN
            | Capabilities::PUSHDOWN_AGG
    }

    fn instance_id(&self) -> &str {
        &self.instance_id
    }

    async fn attach(&self, name: &QualifiedName, source: TableSource) -> Result<(), EngineError> {
        match source {
            TableSource::SqliteFile { path, .. }
                if name.space == self.space && path == self.path =>
            {
                // The Space's own file is attached at open time; nothing to do.
                Ok(())
            }
            TableSource::SqliteFile { path, .. } => Err(EngineError::Unsupported(
                EngineKind::Sqlite,
                format!(
                    "attaching {} for {}: this engine serves only {}",
                    path.display(),
                    name,
                    self.path.display()
                ),
            )),
            other => Err(EngineError::Unsupported(
                EngineKind::Sqlite,
                format!("attaching {} for {}", other.kind_name(), name),
            )),
        }
    }

    async fn detach(&self, _name: &QualifiedName) -> Result<(), EngineError> {
        Ok(())
    }

    async fn table_names(&self, space: &str) -> Result<Vec<String>, EngineError> {
        if space != self.space {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT name FROM {}.sqlite_master WHERE type = 'table' ORDER BY name",
            quote_ident(space)
        );
        self.worker
            .run(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let names = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(sql_err)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_err)?;
                Ok(names)
            })
            .await
    }

    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        self.check_space(name)?;
        let sql = format!(
            "PRAGMA {}.table_info({})",
            quote_ident(&name.space),
            quote_ident(&name.table)
        );
        let display = name.to_string();
        self.worker
            .run(move |conn| {
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let mut rows = stmt.query([]).map_err(sql_err)?;
                let mut names = Vec::new();
                let mut types = Vec::new();
                while let Some(row) = rows.next().map_err(sql_err)? {
                    let col: String = row.get(1).map_err(sql_err)?;
                    let decl: String = row.get(2).map_err(sql_err)?;
                    names.push(col);
                    types.push(affinity_type(&decl));
                }
                if names.is_empty() {
                    return Err(EngineError::TableNotFound(display));
                }
                Ok(crate::convert::arrow_schema(&names, &types))
            })
            .await
    }

    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        let sql = sql.to_string();
        let params = params.to_vec();
        self.worker
            .query_stream(DEFAULT_STREAM_CAPACITY, move |conn, sink| {
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let names: Vec<String> = stmt
                    .column_names()
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                let declared: Vec<Option<ColumnType>> = stmt
                    .columns()
                    .iter()
                    .map(|c| c.decl_type().map(affinity_type))
                    .collect();
                let bound: Vec<Param<'_>> = params.iter().map(Param).collect();
                let mut rows = stmt
                    .query(rusqlite::params_from_iter(bound.iter()))
                    .map_err(sql_err)?;

                // The first batch is buffered so undeclared (expression) columns
                // can be typed from observed values before the schema is announced.
                let ncols = names.len();
                let mut pending: Vec<Vec<SqlValue>> = Vec::new();
                let mut types: Vec<Option<ColumnType>> = declared;
                while pending.len() < ENGINE_BATCH_ROWS {
                    let Some(row) = rows.next().map_err(sql_err)? else {
                        break;
                    };
                    let mut values = Vec::with_capacity(ncols);
                    for (i, slot) in types.iter_mut().enumerate() {
                        let v = row.get_ref(i).map_err(sql_err)?;
                        if slot.is_none() {
                            *slot = infer_from_value(v);
                        }
                        values.push(owned_value(v));
                    }
                    pending.push(values);
                }
                let types: Vec<ColumnType> = types
                    .into_iter()
                    .map(|t| t.unwrap_or(ColumnType::Utf8))
                    .collect();
                let mut batcher = RowBatcher::new(names, types);
                sink.start(batcher.schema())?;

                for values in &pending {
                    let refs: Vec<ValueRef<'_>> = values.iter().map(ValueRef::from).collect();
                    batcher.push_row(&refs)?;
                }
                if !batcher.is_empty() {
                    sink.send(batcher.flush()?)?;
                }

                while let Some(row) = rows.next().map_err(sql_err)? {
                    let mut refs = Vec::with_capacity(ncols);
                    for i in 0..ncols {
                        refs.push(row.get_ref(i).map_err(sql_err)?);
                    }
                    batcher.push_row(&refs)?;
                    if batcher.len() >= ENGINE_BATCH_ROWS {
                        sink.send(batcher.flush()?)?;
                    }
                }
                if !batcher.is_empty() {
                    sink.send(batcher.flush()?)?;
                }
                Ok(())
            })
            .await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, EngineError> {
        let sql = sql.to_string();
        let params = params.to_vec();
        self.worker
            .run(move |conn| {
                let bound: Vec<Param<'_>> = params.iter().map(Param).collect();
                let changed = conn
                    .execute(&sql, rusqlite::params_from_iter(bound.iter()))
                    .map_err(sql_err)?;
                Ok(changed as u64)
            })
            .await
    }

    async fn begin(&self) -> Result<TxHandle, EngineError> {
        if self
            .in_tx
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(EngineError::Transaction(
                "a transaction is already open".to_string(),
            ));
        }
        let result = self
            .worker
            .run(|conn| conn.execute_batch("BEGIN").map_err(sql_err))
            .await;
        if let Err(e) = result {
            self.in_tx.store(false, Ordering::SeqCst);
            return Err(e);
        }
        let id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        Ok(TxHandle::new(id))
    }

    async fn commit(&self, tx: TxHandle) -> Result<(), EngineError> {
        self.take_tx(tx)?;
        let result = self
            .worker
            .run(|conn| conn.execute_batch("COMMIT").map_err(sql_err))
            .await;
        self.in_tx.store(false, Ordering::SeqCst);
        result
    }

    async fn rollback(&self, tx: TxHandle) -> Result<(), EngineError> {
        self.take_tx(tx)?;
        let result = self
            .worker
            .run(|conn| conn.execute_batch("ROLLBACK").map_err(sql_err))
            .await;
        self.in_tx.store(false, Ordering::SeqCst);
        result
    }
}
