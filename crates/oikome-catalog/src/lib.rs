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

//! Oikome catalog: an [`iceberg::Catalog`] implementation over a local
//! directory tree, plus the Space/Location registries that make up the
//! node-level namespace and the snapshot resolution engines rely on.

pub mod acl;
pub mod catalog;
pub mod graph;
pub mod lance;
pub mod peer;
pub mod registry;
pub mod replace;
pub mod resolve;
pub mod space;
pub mod ucan;

pub use acl::{Grant, RowPolicy, ViewDef};
pub use catalog::OikomeCatalog;
pub use graph::GraphDef;
pub use peer::Peer;
pub use resolve::ResolvedTable;
pub use space::{Location, LocationFormat, LocationId, Space};
pub use ucan::{
    is_plaintext_peer, validate_peer_url, HeldToken, IssuedUcan, Revocation, UcanCapability,
};
