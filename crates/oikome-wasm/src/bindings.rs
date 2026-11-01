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

//! The JavaScript API of the browser node:
//!
//! ```js
//! const node = await OikomeNode.open(engine, store, transport);
//! node.did();
//! const { ipc, rowsAffected } = await node.sql("SELECT ...");
//! ```
//!
//! `engine` is a [`HostEngineAdapter`]; `transport` a
//! [`HostTransportAdapter`], which reaches the Full Nodes of remote Spaces
//! over WebRTC; `store` keeps bytes by key
//! (`get(key)` resolving to a `Uint8Array` or `undefined`, `put(key, bytes)`),
//! for the node's seed and registry. The seed is stored as bytes, readable by
//! scripts of the same origin (architecture v3 §10).

use std::rc::Rc;
use std::sync::Arc;

use js_sys::{Object, Promise, Reflect, Uint8Array};
use oikome_identity::NodeIdentity;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{future_to_promise, JsFuture};

use crate::host::{HostEngine, HostEngineAdapter};
use crate::ipc;
use crate::node::{BrowserNode, Output};
use crate::transport::{HostTransport, HostTransportAdapter};

const SEED_KEY: &str = "oikome.seed";
const REGISTRY_KEY: &str = "oikome.registry";

#[wasm_bindgen]
extern "C" {
    /// Byte storage of the host (IndexedDB in `web/src/store.js`).
    pub type HostStore;

    #[wasm_bindgen(method, catch)]
    fn get(this: &HostStore, key: &str) -> Result<Promise, JsValue>;

    #[wasm_bindgen(method, catch)]
    fn put(this: &HostStore, key: &str, value: Uint8Array) -> Result<Promise, JsValue>;
}

fn error(message: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&message.to_string()).into()
}

async fn load(store: &HostStore, key: &str) -> Result<Option<Vec<u8>>, JsValue> {
    let value = JsFuture::from(store.get(key)?).await?;
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    if !value.is_instance_of::<Uint8Array>() {
        return Err(error(format!("store value '{key}' is not a Uint8Array")));
    }
    Ok(Some(Uint8Array::new(&value).to_vec()))
}

async fn save(store: &HostStore, key: &str, bytes: &[u8]) -> Result<(), JsValue> {
    JsFuture::from(store.put(key, Uint8Array::from(bytes))?).await?;
    Ok(())
}

/// A browser node.
#[wasm_bindgen]
pub struct OikomeNode {
    node: Rc<BrowserNode>,
    store: Rc<HostStore>,
}

#[wasm_bindgen]
impl OikomeNode {
    /// Open the node over the host's `engine`, `store` and `transport`. The
    /// node's key is created on first use and kept in `store`, as is its
    /// registry.
    pub async fn open(
        engine: HostEngineAdapter,
        store: HostStore,
        transport: HostTransportAdapter,
    ) -> Result<OikomeNode, JsValue> {
        let identity = match load(&store, SEED_KEY).await? {
            Some(seed) => {
                let seed: [u8; 32] = seed
                    .try_into()
                    .map_err(|_| error("the stored seed is not 32 bytes"))?;
                NodeIdentity::from_seed(&seed)
            }
            None => {
                let identity = NodeIdentity::generate().map_err(error)?;
                let seed = identity.with_seed(|seed| seed.to_vec());
                save(&store, SEED_KEY, &seed).await?;
                identity
            }
        };
        let registry = load(&store, REGISTRY_KEY).await?;
        let node = BrowserNode::open(
            identity,
            Arc::new(HostEngine::new(engine)),
            registry.as_deref(),
        )
        .map_err(error)?
        .with_transport(Arc::new(HostTransport::new(transport)));
        Ok(OikomeNode {
            node: Rc::new(node),
            store: Rc::new(store),
        })
    }

    /// This node's DID.
    pub fn did(&self) -> String {
        self.node.identity().did().to_string()
    }

    /// Run one statement. Resolves to `{ ipc }` (an Arrow IPC stream) for a
    /// query, `{ rowsAffected }` for DML, `{}` otherwise.
    pub fn sql(&self, text: String) -> Promise {
        let (node, store) = (self.node.clone(), self.store.clone());
        future_to_promise(async move {
            let before = node.registry_bytes().map_err(error)?;
            let output = node.sql(&text).await.map_err(error)?;
            let after = node.registry_bytes().map_err(error)?;
            if after != before {
                save(&store, REGISTRY_KEY, &after).await?;
            }
            let result = Object::new();
            match output {
                Output::Rows { schema, batches } => {
                    let bytes = ipc::encode(&schema, &batches).map_err(error)?;
                    Reflect::set(&result, &"ipc".into(), &Uint8Array::from(&bytes[..]))?;
                }
                Output::RowsAffected(n) => {
                    Reflect::set(
                        &result,
                        &"rowsAffected".into(),
                        &JsValue::from_f64(n as f64),
                    )?;
                }
                Output::Done => {}
            }
            Ok(result.into())
        })
    }
}

/// Keep the panic message in the browser console instead of `unreachable`.
#[wasm_bindgen(start)]
pub fn start() {
    std::panic::set_hook(Box::new(|info| {
        // The JavaScript stack names the wasm functions when the module
        // keeps its name section (any profile but `wasm`).
        let stack = Reflect::get(&js_sys::Error::new(""), &"stack".into())
            .ok()
            .and_then(|s| s.as_string())
            .unwrap_or_default();
        web_sys_console_error(&format!("{info}\n{stack}"));
    }));
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn web_sys_console_error(message: &str);
}
