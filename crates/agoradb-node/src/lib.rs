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

//! The AgoraDB node runtime.
//!
//! [`AgoraSession`] is the entry point: it parses a statement, classifies it
//! (see `agoradb-semantic`), and routes it to the catalog, to one Space's
//! engine, or — for statements spanning several Spaces — to the federation
//! coordinator. Engines are shared per node through [`EngineRegistry`].

mod bind;
mod ddl;
mod dml;
pub mod error;
pub mod registry;
pub mod result;
pub mod session;

pub use error::SessionError;
pub use registry::{EngineRegistry, NodeConfig};
pub use result::QueryResult;
pub use session::{AgoraSession, SessionConfig};
