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

//! The Oikome browser node (architecture v3 §10).
//!
//! The Rust core — identity, statement parsing and the semantic layer — is
//! compiled to WebAssembly; the engine is provided by the JavaScript host
//! (duckdb-wasm, data in OPFS). See `web/` for the host and the build.
//!
//! * [`node`]: statement handling, tested natively against DuckDB.
//! * `host` (wasm32 only): the host's engine as a `QueryEngine`.
//! * `bindings` (wasm32 only): the `OikomeNode` JavaScript API.

pub mod ipc;
pub mod node;

#[cfg(target_arch = "wasm32")]
mod bindings;
#[cfg(target_arch = "wasm32")]
pub mod host;

pub use node::{BrowserNode, NodeError, Output};
