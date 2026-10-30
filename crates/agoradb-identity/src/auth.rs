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

//! Peer request authentication.
//!
//! Every request a node sends to a peer carries an [`AuthRequest`]: the
//! caller's signature over the server's DID, the caller's DID, a timestamp,
//! a random nonce and a digest of the request itself (method, payload and
//! the UCAN presented). So a captured request cannot be
//!
//! * sent to another node (the server DID is signed),
//! * altered — different SQL or a different UCAN (the digest is signed),
//! * replayed later (the timestamp is checked against a skew window), or
//!   within the window (the server remembers nonces it has seen).
//!
//! No bearer secret is ever sent, so this holds even over plaintext gRPC.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::did::Did;
use crate::error::IdentityError;
use crate::keys::NodeIdentity;

const CONTEXT: &str = "agoradb-request/v1";

/// Largest accepted difference between the request time and the server clock.
pub const MAX_AUTH_SKEW_MS: u64 = 60_000;

/// Largest accepted encoded [`AuthRequest`] header.
pub const MAX_AUTH_HEADER_BYTES: usize = 2048;

const NONCE_BYTES: usize = 16;

/// A signed request header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthRequest {
    pub did: Did,
    pub server_did: Did,
    pub timestamp_ms: u64,
    /// base64url random nonce.
    pub nonce: String,
    /// base64url Ed25519 signature.
    pub signature: String,
}

/// Digest of a request: what is called, with which payload and UCAN.
pub fn request_digest(method: &str, payload: &[u8], ucan: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for part in [method.as_bytes(), payload, ucan.as_bytes()] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

fn message(
    server: &Did,
    caller: &Did,
    timestamp_ms: u64,
    nonce: &str,
    digest: &[u8; 32],
) -> Vec<u8> {
    let digest: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("{CONTEXT}\n{server}\n{caller}\n{timestamp_ms}\n{nonce}\n{digest}").into_bytes()
}

impl AuthRequest {
    /// Sign a request with `digest` (see [`request_digest`]) from `identity`
    /// to the node `server_did`.
    pub fn sign(
        identity: &NodeIdentity,
        server_did: &Did,
        timestamp_ms: u64,
        digest: &[u8; 32],
    ) -> Result<Self, IdentityError> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).map_err(|e| IdentityError::Random(e.to_string()))?;
        let nonce = URL_SAFE_NO_PAD.encode(nonce);
        let signature = identity.sign(&message(
            server_did,
            identity.did(),
            timestamp_ms,
            &nonce,
            digest,
        ));
        Ok(Self {
            did: identity.did().clone(),
            server_did: server_did.clone(),
            timestamp_ms,
            nonce,
            signature: URL_SAFE_NO_PAD.encode(signature),
        })
    }

    /// Verify the request on the server `server` for a request with
    /// `digest`; returns the caller's DID. Replay within the skew window is
    /// the server's to check, by remembering `(did, nonce)`.
    pub fn verify(
        &self,
        server: &Did,
        now_ms: u64,
        digest: &[u8; 32],
    ) -> Result<Did, IdentityError> {
        if &self.server_did != server {
            return Err(IdentityError::Auth(
                "request is for another node".to_string(),
            ));
        }
        if self.timestamp_ms.abs_diff(now_ms) > MAX_AUTH_SKEW_MS {
            return Err(IdentityError::Auth(
                "request timestamp out of range".to_string(),
            ));
        }
        let nonce_ok = URL_SAFE_NO_PAD
            .decode(&self.nonce)
            .is_ok_and(|n| n.len() == NONCE_BYTES);
        if !nonce_ok {
            return Err(IdentityError::Auth("malformed nonce".to_string()));
        }
        let signature = URL_SAFE_NO_PAD
            .decode(&self.signature)
            .map_err(|_| IdentityError::InvalidSignature)?;
        self.did.verify(
            &message(server, &self.did, self.timestamp_ms, &self.nonce, digest),
            &signature,
        )?;
        Ok(self.did.clone())
    }

    /// Encode for a request header (base64url JSON).
    pub fn to_header(&self) -> Result<String, IdentityError> {
        let json = serde_json::to_vec(self).map_err(|e| IdentityError::Auth(e.to_string()))?;
        Ok(URL_SAFE_NO_PAD.encode(json))
    }

    /// Decode a request header.
    pub fn from_header(value: &str) -> Result<Self, IdentityError> {
        if value.len() > MAX_AUTH_HEADER_BYTES {
            return Err(IdentityError::Auth("auth header too large".to_string()));
        }
        let json = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| IdentityError::Auth("auth header is not base64url".to_string()))?;
        serde_json::from_slice(&json).map_err(|e| IdentityError::Auth(format!("auth header: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_request_verifies_only_for_its_server_time_and_content() {
        let server = NodeIdentity::from_seed(&[7; 32]);
        let other = NodeIdentity::from_seed(&[8; 32]);
        let caller = NodeIdentity::from_seed(&[9; 32]);
        let now = 1_800_000_000_000;
        let digest = request_digest("do_get", b"SELECT 1", "ucan");

        let req = AuthRequest::sign(&caller, server.did(), now, &digest).unwrap();
        assert_eq!(
            req.verify(server.did(), now + 1000, &digest).unwrap(),
            *caller.did()
        );
        assert!(req.verify(other.did(), now, &digest).is_err());
        assert!(req
            .verify(server.did(), now + MAX_AUTH_SKEW_MS + 1, &digest)
            .is_err());

        // Different SQL or a swapped UCAN changes the digest.
        for changed in [
            request_digest("do_get", b"SELECT 2", "ucan"),
            request_digest("do_get", b"SELECT 1", "other"),
            request_digest("get_schema", b"SELECT 1", "ucan"),
        ] {
            assert!(req.verify(server.did(), now, &changed).is_err());
        }

        // Claiming another DID, or editing the timestamp or nonce, fails.
        let mut spoofed = req.clone();
        spoofed.did = other.did().clone();
        assert!(spoofed.verify(server.did(), now, &digest).is_err());
        let mut shifted = req.clone();
        shifted.timestamp_ms += 1;
        assert!(shifted.verify(server.did(), now, &digest).is_err());
        let again = AuthRequest::sign(&caller, server.did(), now, &digest).unwrap();
        assert_ne!(again.nonce, req.nonce, "every request gets a fresh nonce");
        let mut renonced = req.clone();
        renonced.nonce = again.nonce;
        assert!(renonced.verify(server.did(), now, &digest).is_err());
    }

    #[test]
    fn header_roundtrip_and_limits() {
        let server = NodeIdentity::from_seed(&[7; 32]);
        let caller = NodeIdentity::from_seed(&[9; 32]);
        let digest = request_digest("m", b"", "");
        let req = AuthRequest::sign(&caller, server.did(), 5, &digest).unwrap();
        let header = req.to_header().unwrap();
        assert!(header.len() < MAX_AUTH_HEADER_BYTES);
        assert_eq!(AuthRequest::from_header(&header).unwrap(), req);
        assert!(AuthRequest::from_header("not base64!").is_err());
        assert!(AuthRequest::from_header(&"A".repeat(MAX_AUTH_HEADER_BYTES + 1)).is_err());
        let bad_did = URL_SAFE_NO_PAD.encode(
            br#"{"did":"did:key:zbad","server_did":"did:key:zbad","timestamp_ms":1,"nonce":"","signature":""}"#,
        );
        assert!(AuthRequest::from_header(&bad_did).is_err());
    }
}
