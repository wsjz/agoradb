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

//! Peer networking for AgoraDB (architecture v3 §9).
//!
//! * [`protocol`]: the calls, payloads and headers nodes exchange over
//!   Arrow Flight.
//! * [`PeerClient`]: signed calls to one peer, shared by query shipping and
//!   subscription (`agora.snapshot`, `agora.fetch_file`).
//! * [`FlightEngine`]: the client side — a remote Space exposed as a
//!   [`QueryEngine`](agoradb_engine::QueryEngine), so the federation layer
//!   pushes subqueries to the owner node exactly as it does to a local
//!   engine.
//!
//! The server side lives in `agoradb-server`.

pub mod client;
pub mod engine;
pub mod protocol;
pub mod tls;

pub use client::{ChunkStream, PeerClient};
pub use engine::{FlightEngine, RemoteSpaceConfig};
pub use tls::{install_crypto_provider, DidCertVerifier};
