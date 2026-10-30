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

//! The Peers table (architecture v3 §3.4): nodes found by discovery, with
//! the Flight endpoint each announced.
//!
//! Discovery runs in the serving process; recording peers in the catalog
//! lets every process of the node (`agoradb sql` included) resolve a remote
//! Space's owner by DID. An entry only says where a DID was last seen: the
//! connection itself is authenticated against the DID (TLS certificate and
//! signed requests), so a wrong entry can fail a connection but never
//! impersonate a peer.

use agoradb_core::CatalogError;
use serde::{Deserialize, Serialize};

use crate::catalog::AgoraCatalog;
use crate::registry::now_ms;
use crate::ucan::validate_peer_url;

/// Re-recording an unchanged endpoint more often than this is skipped, so
/// discovery does not rewrite the registry on every announcement.
const PEER_REFRESH_MS: u64 = 60_000;

/// A node found by discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// The node's DID (`did:key:z…`).
    pub did: String,
    /// Its Flight endpoint, `https://host:port` (`http://` for a plaintext
    /// node).
    pub endpoint: String,
    pub last_seen_ms: u64,
}

impl AgoraCatalog {
    /// Record that `did` was seen serving Flight at `endpoint`.
    pub fn record_peer(&self, did: &str, endpoint: &str) -> Result<(), CatalogError> {
        if !did.starts_with("did:") {
            return Err(CatalogError::InvalidProperty(format!(
                "peer '{did}' is not a DID"
            )));
        }
        let endpoint = validate_peer_url(endpoint)?;
        let now = now_ms();
        let fresh = self.with_registry(|reg| {
            reg.peers.get(did).is_some_and(|p| {
                p.endpoint == endpoint && now.saturating_sub(p.last_seen_ms) < PEER_REFRESH_MS
            })
        });
        if fresh {
            return Ok(());
        }
        self.mutate_registry(|reg| {
            reg.peers.insert(
                did.to_string(),
                Peer {
                    did: did.to_string(),
                    endpoint,
                    last_seen_ms: now,
                },
            );
            Ok(())
        })
    }

    /// Where `did` was last seen, if discovery found it.
    pub fn get_peer(&self, did: &str) -> Option<Peer> {
        self.with_registry(|reg| reg.peers.get(did).cloned())
    }

    /// Every discovered peer, ordered by DID.
    pub fn list_peers(&self) -> Vec<Peer> {
        self.with_registry(|reg| reg.peers.values().cloned().collect())
    }
}
