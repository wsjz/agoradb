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

//! Capability records (architecture v3 §9): the UCANs this node issued, the
//! token ids it revoked, and the UCANs it holds for remote Spaces.
//!
//! Issued UCANs are recorded by id and claims only; the encoded token is
//! handed to the caller once and never stored. Held tokens are stored in a
//! separate file under `.agora/identity/` that only the owner can read.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;

use agoradb_core::{AccessMode, CatalogError, EngineKind, RemoteMode, SpaceKind, SpaceUri};
use serde::{Deserialize, Serialize};

use crate::catalog::AgoraCatalog;
use crate::registry::now_ms;
use crate::space::{Location, LocationFormat, Space, RESIDENCY_REMOTE};

/// Directory under `.agora/` holding subscribed copies, one per Location.
const REPLICA_DIR: &str = "replicas";

/// One capability of an issued UCAN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UcanCapability {
    /// Resource, e.g. `space://did:key:z…/blog`.
    pub with: String,
    /// Ability, e.g. `space/query`.
    pub can: String,
}

/// A UCAN this node issued (claims only; the token itself is not kept).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedUcan {
    /// Token id (see `agoradb_identity::ucan::token_id`).
    pub id: String,
    /// DID the token was issued to.
    pub audience: String,
    pub capabilities: Vec<UcanCapability>,
    pub issued_at_ms: u64,
    /// Expiry, seconds since the Unix epoch.
    pub expires_at: u64,
}

/// A revoked token id. Revocation also invalidates every token delegated
/// from the revoked one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revocation {
    pub id: String,
    pub revoked_at_ms: u64,
}

/// A UCAN held by this node. `Debug` never prints it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HeldToken(String);

impl HeldToken {
    /// Wrap an encoded token.
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The encoded token, for sending to its issuer's node.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HeldToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HeldToken(<redacted>)")
    }
}

/// The UCAN held for one remote Space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RemoteToken {
    pub(crate) space: String,
    pub(crate) token: HeldToken,
}

/// Whether `id` looks like a token id: 32 lowercase hex digits.
pub fn is_token_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Validate a peer's Flight endpoint and return it without a trailing `/`.
///
/// Only `http://host[:port]` is accepted: 3.1a speaks plaintext gRPC (every
/// request is signed, see architecture v3 §9), and anything beyond scheme,
/// host and port — credentials, paths, queries — is rejected.
pub fn validate_peer_url(peer: &str) -> Result<String, CatalogError> {
    let invalid = |why: &str| CatalogError::InvalidProperty(format!("PEER '{peer}': {why}"));
    let rest = if let Some(rest) = peer.strip_prefix("http://") {
        rest
    } else if peer.starts_with("https://") {
        return Err(invalid("https is not supported yet; use http://host:port"));
    } else {
        return Err(invalid("expected http://host:port"));
    };
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty() {
        return Err(invalid("missing host"));
    }
    if authority
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '@' | '/' | '?' | '#' | '\\'))
    {
        return Err(invalid("only a host and an optional port are allowed"));
    }
    let (host, port) = match authority.strip_prefix('[') {
        // IPv6 literal: [::1]:50051
        Some(v6) => {
            let (addr, after) = v6
                .split_once(']')
                .ok_or_else(|| invalid("unterminated IPv6 address"))?;
            if addr.is_empty() || !addr.chars().all(|c| c.is_ascii_hexdigit() || c == ':') {
                return Err(invalid("invalid IPv6 address"));
            }
            match after {
                "" => (addr, None),
                p => (
                    addr,
                    Some(
                        p.strip_prefix(':')
                            .ok_or_else(|| invalid("expected :port"))?,
                    ),
                ),
            }
        }
        None => match authority.rsplit_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        },
    };
    if host.is_empty()
        || !host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '_'))
    {
        return Err(invalid("invalid host"));
    }
    if let Some(port) = port {
        if port.parse::<u16>().map_or(true, |p| p == 0) {
            return Err(invalid("invalid port"));
        }
    }
    Ok(format!("http://{authority}"))
}

impl AgoraCatalog {
    /// Record a UCAN this node issued.
    pub fn record_issued_ucan(&self, issued: IssuedUcan) -> Result<(), CatalogError> {
        self.mutate_registry(|reg| {
            reg.issued.insert(issued.id.clone(), issued);
            Ok(())
        })
    }

    /// UCANs this node issued, ordered by id.
    pub fn list_issued_ucans(&self) -> Vec<IssuedUcan> {
        self.with_registry(|reg| reg.issued.values().cloned().collect())
    }

    /// Revoke a token id. Returns `false` if it was already revoked.
    ///
    /// Any id can be revoked, not only ids this node issued: revoking a
    /// token someone else delegated from one of ours cuts that branch only.
    pub fn revoke_ucan(&self, id: &str) -> Result<bool, CatalogError> {
        if !is_token_id(id) {
            return Err(CatalogError::InvalidProperty(format!(
                "'{id}' is not a UCAN id (32 lowercase hex digits)"
            )));
        }
        self.mutate_registry(|reg| {
            if reg.revoked.contains_key(id) {
                return Ok(false);
            }
            reg.revoked.insert(
                id.to_string(),
                Revocation {
                    id: id.to_string(),
                    revoked_at_ms: now_ms(),
                },
            );
            Ok(true)
        })
    }

    /// Every revoked token id.
    pub fn revoked_ucans(&self) -> HashSet<String> {
        self.with_registry(|reg| reg.revoked.keys().cloned().collect())
    }

    /// Revocations, ordered by id.
    pub fn list_revocations(&self) -> Vec<Revocation> {
        self.with_registry(|reg| reg.revoked.values().cloned().collect())
    }

    /// Register a remote Space `name` backed by `space` on the node `owner`
    /// reachable at `peer`, holding `token` to query it.
    ///
    /// The caller is expected to have checked the token (audience, issuer,
    /// capability, expiry); the catalog only stores it.
    pub fn create_remote_space(
        &self,
        name: &str,
        peer: &str,
        space: &str,
        owner: &str,
        token: HeldToken,
    ) -> Result<Space, CatalogError> {
        self.create_peer_space(name, peer, space, owner, token, RemoteMode::Query)
    }

    /// Register `name` as a local copy of `space` on the node `owner`
    /// reachable at `peer`, holding `token` (with `space/read`) to refresh
    /// it. The copy lives in [`replica_dir`](Self::replica_dir) and is read
    /// by DuckDB; it is still a remote Space: read-only, never granted or
    /// served onwards.
    pub fn create_replica_space(
        &self,
        name: &str,
        peer: &str,
        space: &str,
        owner: &str,
        token: HeldToken,
    ) -> Result<Space, CatalogError> {
        self.create_peer_space(name, peer, space, owner, token, RemoteMode::Subscribe)
    }

    /// Directory holding the files of the replica Location `location`.
    pub fn replica_dir(&self, location: &str) -> PathBuf {
        self.agora_dir().join(REPLICA_DIR).join(location)
    }

    fn create_peer_space(
        &self,
        name: &str,
        peer: &str,
        space: &str,
        owner: &str,
        token: HeldToken,
        mode: RemoteMode,
    ) -> Result<Space, CatalogError> {
        if name.is_empty() || space.is_empty() || owner.is_empty() {
            return Err(CatalogError::InvalidProperty(
                "remote space, name and owner must not be empty".to_string(),
            ));
        }
        let peer = validate_peer_url(peer)?;
        let (engine, format) = match mode {
            RemoteMode::Query => (
                EngineKind::Flight,
                LocationFormat::Remote {
                    peer,
                    space: space.to_string(),
                    owner: owner.to_string(),
                },
            ),
            RemoteMode::Subscribe => (
                EngineKind::DuckDb,
                LocationFormat::Replica {
                    peer,
                    space: space.to_string(),
                    owner: owner.to_string(),
                },
            ),
        };
        let created = Space {
            name: name.to_string(),
            uri: Some(SpaceUri {
                owner_did: owner.to_string(),
                name: space.to_string(),
            }),
            kind: SpaceKind::Remote,
            engine,
            location: name.to_string(),
            access: AccessMode::ReadOnly,
            created_at_ms: now_ms(),
            published_from: None,
        };
        let location = Location {
            id: name.to_string(),
            format,
            residency: vec![RESIDENCY_REMOTE.to_string()],
            writer: None,
        };
        self.mutate_registry(|reg| {
            if reg.spaces.contains_key(name) {
                return Err(CatalogError::SpaceExists(name.to_string()));
            }
            if reg.locations.contains_key(name) {
                return Err(CatalogError::Registry(format!(
                    "a location named '{name}' already exists"
                )));
            }
            reg.spaces.insert(name.to_string(), created.clone());
            reg.locations.insert(name.to_string(), location);
            reg.tokens.insert(
                name.to_string(),
                RemoteToken {
                    space: name.to_string(),
                    token,
                },
            );
            Ok(())
        })?;
        Ok(created)
    }

    /// The UCAN held for the remote Space `space`.
    pub fn remote_token(&self, space: &str) -> Result<HeldToken, CatalogError> {
        self.with_registry(|reg| {
            reg.tokens
                .get(space)
                .map(|t| t.token.clone())
                .ok_or_else(|| CatalogError::SpaceNotFound(space.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_ids_are_32_lowercase_hex() {
        assert!(is_token_id("0123456789abcdef0123456789abcdef"));
        assert!(!is_token_id("0123456789ABCDEF0123456789abcdef"));
        assert!(!is_token_id("0123"));
        assert!(!is_token_id("0123456789abcdef0123456789abcdeg"));
    }

    #[test]
    fn peer_urls_are_restricted_to_http_host_port() {
        assert_eq!(
            validate_peer_url("http://node-a:50051/").unwrap(),
            "http://node-a:50051"
        );
        assert_eq!(
            validate_peer_url("http://127.0.0.1:1").unwrap(),
            "http://127.0.0.1:1"
        );
        assert_eq!(
            validate_peer_url("http://[::1]:50051").unwrap(),
            "http://[::1]:50051"
        );
        for bad in [
            "node-a:50051",
            "https://node-a:50051",
            "ftp://node-a",
            "http://",
            "http://user:pw@node-a:50051",
            "http://node-a:50051/path",
            "http://node-a:50051?x=1",
            "http://node-a:0",
            "http://node-a:99999",
            "http://node a:1",
            "http://[zz]:1",
        ] {
            assert!(validate_peer_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn held_token_debug_is_redacted() {
        let token = HeldToken::new("header.payload.signature");
        assert_eq!(format!("{token:?}"), "HeldToken(<redacted>)");
        assert_eq!(token.expose(), "header.payload.signature");
        assert_eq!(
            serde_json::to_string(&token).unwrap(),
            "\"header.payload.signature\""
        );
    }
}
