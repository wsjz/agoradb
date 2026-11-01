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

//! [`HostEngine`]: the engine the JavaScript host provides (duckdb-wasm, see
//! `packages/node/src/host.js`) as a [`QueryEngine`] (architecture v3 §10).
//!
//! `QueryEngine` and DataFusion require `Send`, but JavaScript objects are
//! not. So the engine holds only an id: the host object lives in a
//! thread-local table, each call is started with `spawn_local`, and its
//! result comes back over a oneshot channel, which is `Send`. Results cross
//! the boundary as Arrow IPC streams.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::channel::oneshot;
use js_sys::{Array, Promise, Uint8Array};
use oikome_core::EngineKind;
use oikome_engine::{
    Capabilities, EngineError, QualifiedName, QueryEngine, RecordBatchStream, SqlDialect,
    TableSource, TxHandle, Value,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use crate::ipc;

#[wasm_bindgen]
extern "C" {
    /// The host's engine. Every method returns a Promise:
    /// `query(sql)` and `tableSchema(space, table)` resolve to an Arrow IPC
    /// stream (`Uint8Array`), `execute(sql)` to the number of rows changed,
    /// `tableNames(space)` to an array of table names.
    pub type HostEngineAdapter;

    #[wasm_bindgen(method, catch)]
    fn query(this: &HostEngineAdapter, sql: &str) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch)]
    fn execute(this: &HostEngineAdapter, sql: &str) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = tableNames)]
    fn table_names(this: &HostEngineAdapter, space: &str) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = tableSchema)]
    fn table_schema(this: &HostEngineAdapter, space: &str, table: &str)
        -> Result<Promise, JsValue>;
}

thread_local! {
    static ADAPTERS: RefCell<HashMap<u32, HostEngineAdapter>> = RefCell::new(HashMap::new());
}

static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// The message of a JavaScript error (or the value itself).
pub(crate) fn js_message(value: &JsValue) -> String {
    value
        .dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.message()))
        .or_else(|| value.as_string())
        .unwrap_or_else(|| format!("{value:?}"))
}

fn host_err(value: JsValue) -> EngineError {
    EngineError::Sql(js_message(&value))
}

/// Call the host object `id`, await its Promise on the JavaScript side and
/// convert the result there; only the converted value crosses back.
async fn call<T: Send + 'static>(
    id: u32,
    start: impl FnOnce(&HostEngineAdapter) -> Result<Promise, JsValue> + Send + 'static,
    convert: impl FnOnce(JsValue) -> Result<T, EngineError> + Send + 'static,
) -> Result<T, EngineError> {
    let (tx, rx) = oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let result = async {
            let promise = ADAPTERS
                .with(|adapters| adapters.borrow().get(&id).map(start))
                .ok_or_else(|| EngineError::Cancelled("the host engine was closed".into()))?
                .map_err(host_err)?;
            let value = JsFuture::from(promise).await.map_err(host_err)?;
            convert(value)
        }
        .await;
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| EngineError::Cancelled("the host call was dropped".into()))?
}

fn bytes(value: JsValue) -> Result<Vec<u8>, EngineError> {
    if !value.is_instance_of::<Uint8Array>() {
        return Err(EngineError::Sql(
            "the host engine must return an Arrow IPC stream as a Uint8Array".into(),
        ));
    }
    Ok(Uint8Array::new(&value).to_vec())
}

/// The host's engine as a [`QueryEngine`]. Dropping it releases the host
/// object.
#[derive(Debug)]
pub struct HostEngine {
    id: u32,
    instance_id: String,
}

impl HostEngine {
    pub fn new(adapter: HostEngineAdapter) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        ADAPTERS.with(|adapters| adapters.borrow_mut().insert(id, adapter));
        Self {
            id,
            instance_id: format!("host:{id}"),
        }
    }

    fn unsupported(what: &str) -> EngineError {
        EngineError::Unsupported(EngineKind::Host, what.to_string())
    }
}

impl Drop for HostEngine {
    fn drop(&mut self) {
        ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&self.id));
    }
}

#[async_trait]
impl QueryEngine for HostEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Host
    }

    fn dialect(&self) -> SqlDialect {
        SqlDialect::DuckDb
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::OLAP
            | Capabilities::ARROW_OUT
            | Capabilities::PUSHDOWN_JOIN
            | Capabilities::PUSHDOWN_AGG
    }

    fn instance_id(&self) -> &str {
        &self.instance_id
    }

    async fn attach(&self, _name: &QualifiedName, _source: TableSource) -> Result<(), EngineError> {
        Err(Self::unsupported(
            "attach: browser tables live in duckdb-wasm",
        ))
    }

    async fn detach(&self, _name: &QualifiedName) -> Result<(), EngineError> {
        Err(Self::unsupported(
            "detach: browser tables live in duckdb-wasm",
        ))
    }

    async fn table_names(&self, space: &str) -> Result<Vec<String>, EngineError> {
        let space = space.to_string();
        call(
            self.id,
            move |host| host.table_names(&space),
            |value| {
                Array::from(&value)
                    .iter()
                    .map(|name| {
                        name.as_string()
                            .ok_or_else(|| EngineError::Sql("table names must be strings".into()))
                    })
                    .collect()
            },
        )
        .await
    }

    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        let (space, table) = (name.space.clone(), name.table.clone());
        let stream = call(
            self.id,
            move |host| host.table_schema(&space, &table),
            bytes,
        )
        .await?;
        Ok(ipc::decode(stream)?.0)
    }

    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        if !params.is_empty() {
            return Err(Self::unsupported("query parameters"));
        }
        let sql = sql.to_string();
        let stream = call(self.id, move |host| host.query(&sql), bytes).await?;
        let (schema, batches) = ipc::decode(stream)?;
        Ok((
            schema,
            Box::pin(futures::stream::iter(batches.into_iter().map(Ok))),
        ))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, EngineError> {
        if !params.is_empty() {
            return Err(Self::unsupported("statement parameters"));
        }
        let sql = sql.to_string();
        call(
            self.id,
            move |host| host.execute(&sql),
            |value| {
                value
                    .as_f64()
                    .filter(|n| *n >= 0.0)
                    .map(|n| n as u64)
                    .ok_or_else(|| EngineError::Sql("execute must resolve to a row count".into()))
            },
        )
        .await
    }

    async fn begin(&self) -> Result<TxHandle, EngineError> {
        Err(Self::unsupported("transactions"))
    }

    async fn commit(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(Self::unsupported("transactions"))
    }

    async fn rollback(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(Self::unsupported("transactions"))
    }
}
