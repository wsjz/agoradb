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

//! [`HostTransport`]: the host's WebRTC transport (`web/src/transport.js`)
//! as a [`Transport`], bridged like [`HostEngine`](crate::host::HostEngine):
//! the JavaScript object stays in a thread-local table and each call comes
//! back over a oneshot channel.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use futures::channel::oneshot;
use js_sys::{Promise, Uint8Array};
use oikome_engine::EngineError;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

use crate::host::js_message;
use crate::remote::Transport;

#[wasm_bindgen]
extern "C" {
    /// The host's transport. `call(addr, fingerprint, message)` sends one
    /// message to the node at `addr` (`ip:port`) over a data channel whose
    /// DTLS certificate must have `fingerprint`, and resolves to the reply
    /// (`Uint8Array`).
    pub type HostTransportAdapter;

    #[wasm_bindgen(method, catch)]
    fn call(
        this: &HostTransportAdapter,
        addr: &str,
        fingerprint: &str,
        message: Uint8Array,
    ) -> Result<Promise, JsValue>;
}

thread_local! {
    static ADAPTERS: RefCell<HashMap<u32, HostTransportAdapter>> = RefCell::new(HashMap::new());
}

static NEXT_ID: AtomicU32 = AtomicU32::new(1);

fn remote_err(value: JsValue) -> EngineError {
    EngineError::Remote(js_message(&value))
}

/// The host's transport as a [`Transport`]. Dropping it releases the host
/// object.
#[derive(Debug)]
pub struct HostTransport {
    id: u32,
}

impl HostTransport {
    pub fn new(adapter: HostTransportAdapter) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        ADAPTERS.with(|adapters| adapters.borrow_mut().insert(id, adapter));
        Self { id }
    }
}

impl Drop for HostTransport {
    fn drop(&mut self) {
        ADAPTERS.with(|adapters| adapters.borrow_mut().remove(&self.id));
    }
}

#[async_trait]
impl Transport for HostTransport {
    async fn call(
        &self,
        addr: &str,
        fingerprint: &str,
        message: Vec<u8>,
    ) -> Result<Vec<u8>, EngineError> {
        let (id, addr, fingerprint) = (self.id, addr.to_string(), fingerprint.to_string());
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = async {
                let promise = ADAPTERS
                    .with(|adapters| {
                        adapters.borrow().get(&id).map(|transport| {
                            transport.call(&addr, &fingerprint, Uint8Array::from(&message[..]))
                        })
                    })
                    .ok_or_else(|| EngineError::Cancelled("the transport was closed".into()))?
                    .map_err(remote_err)?;
                let reply = JsFuture::from(promise).await.map_err(remote_err)?;
                if !reply.is_instance_of::<Uint8Array>() {
                    return Err(EngineError::Remote(
                        "the transport must resolve to a Uint8Array".into(),
                    ));
                }
                Ok(Uint8Array::new(&reply).to_vec())
            }
            .await;
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| EngineError::Cancelled("the transport call was dropped".into()))?
    }
}
