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

//! The Oikome node runtime.
//!
//! [`OikomeSession`] is the entry point: it parses a statement, classifies it
//! (see `oikome-semantic`), applies views and the session principal's
//! grants and row policies, and routes it to the catalog, to one Space's
//! engine, or — for statements spanning several Spaces — to the federation
//! coordinator. Engines are shared per node through [`EngineRegistry`].
//!
//! Remote Spaces (mounted with `CREATE SPACE ... WITH PEER, TOKEN`) are
//! queried through the federation coordinator too, their subqueries shipped
//! to the owner node over Arrow Flight; `ISSUE UCAN` / `REVOKE UCAN` manage
//! the capabilities other nodes use to query this one ([`capability`]).

mod acl;
mod bind;
pub mod capability;
mod ddl;
mod dml;
pub mod error;
mod lance;
pub mod publish;
pub mod registry;
pub mod result;
pub mod session;
pub mod subscribe;

pub use capability::query_spaces;
pub use error::SessionError;
pub use publish::{spawn_publisher, PublishReport, PublishedTable};
pub use registry::{EngineRegistry, NodeConfig};
pub use result::QueryResult;
pub use session::{OikomeSession, SessionConfig};
pub use subscribe::{spawn_refresher, RefreshReport, RefreshedTable};
