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

//! [`PeerClient`]: signed Arrow Flight calls to the owner of a remote Space.
//!
//! Both ways of reading another node's Space go through it: query shipping
//! ([`FlightEngine`](crate::FlightEngine)) and subscription
//! ([`PeerClient::snapshot`], [`PeerClient::fetch_file`]).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::error::FlightError;
use arrow_flight::{Action, FlightClient};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use oikome_engine::EngineError;
use oikome_identity::{unix_now_ms, AuthRequest, Did, NodeIdentity};
use tokio::sync::OnceCell;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Code;

use crate::protocol::{
    digest, FetchFileRequest, SnapshotRequest, SnapshotResponse, ACTION_FETCH_FILE,
    ACTION_SNAPSHOT, HEADER_AUTH, HEADER_UCAN, METHOD_FETCH_FILE, METHOD_SNAPSHOT,
};
use crate::tls::{install_crypto_provider, DidCertVerifier};

/// How long to wait for a TCP connection to a peer.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// TLS server name sent to every peer; peers are identified by DID instead.
const PEER_SERVER_NAME: &str = "oikome.peer";

/// `e` and its sources, so a TLS failure says why the peer was refused.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// A stream of file chunks from [`PeerClient::fetch_file`].
pub type ChunkStream = BoxStream<'static, Result<Bytes, EngineError>>;

/// Calls one peer on behalf of this node, presenting one UCAN. Connects on
/// first use.
pub struct PeerClient {
    peer: String,
    owner: Did,
    token: String,
    identity: Arc<NodeIdentity>,
    allow_http: bool,
    channel: OnceCell<Channel>,
}

impl fmt::Debug for PeerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerClient")
            .field("peer", &self.peer)
            .field("owner", &self.owner)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

pub(crate) fn status_to_engine(status: &tonic::Status) -> EngineError {
    match status.code() {
        Code::PermissionDenied | Code::Unauthenticated => {
            EngineError::PermissionDenied(status.message().to_string())
        }
        Code::NotFound => EngineError::TableNotFound(status.message().to_string()),
        code => EngineError::Remote(format!("{code:?}: {}", status.message())),
    }
}

pub(crate) fn flight_to_engine(e: FlightError) -> EngineError {
    match e {
        FlightError::Tonic(status) => status_to_engine(&status),
        FlightError::Arrow(e) => EngineError::Arrow(e),
        other => EngineError::Remote(other.to_string()),
    }
}

impl PeerClient {
    /// A client for the node `owner` at `peer` (`https://host:port`),
    /// signing as `identity` and presenting `token`. Plaintext peers
    /// (`http://`) are refused unless [`allow_http`](Self::allow_http) is set.
    pub fn new(peer: String, owner: Did, token: String, identity: Arc<NodeIdentity>) -> Self {
        Self {
            peer,
            owner,
            token,
            identity,
            allow_http: false,
            channel: OnceCell::new(),
        }
    }

    /// Accept a plaintext (`http://`) peer.
    pub fn allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }

    /// The peer's Flight endpoint.
    pub fn peer(&self) -> &str {
        &self.peer
    }

    async fn channel(&self) -> Result<Channel, EngineError> {
        self.channel
            .get_or_try_init(|| async {
                if self.peer.starts_with("http://") && !self.allow_http {
                    return Err(EngineError::PermissionDenied(format!(
                        "{} is plaintext and this node does not allow http peers",
                        self.peer
                    )));
                }
                let mut endpoint = Endpoint::from_shared(self.peer.clone())
                    .map_err(|e| EngineError::Remote(format!("invalid peer address: {e}")))?
                    .connect_timeout(CONNECT_TIMEOUT);
                if self.peer.starts_with("https://") {
                    install_crypto_provider();
                    // The name is not checked (the certificate is pinned to
                    // the owner's DID); a fixed one avoids IPv6 literals.
                    let tls = ClientTlsConfig::new().domain_name(PEER_SERVER_NAME);
                    let pinned = DidCertVerifier::new(self.owner.public_key().to_bytes());
                    endpoint = endpoint
                        .tls_config_with_verifier(tls, pinned)
                        .map_err(|e| EngineError::Remote(format!("TLS setup: {e}")))?;
                }
                endpoint.connect().await.map_err(|e| {
                    EngineError::Remote(format!("cannot reach {}: {}", self.peer, error_chain(&e)))
                })
            })
            .await
            .cloned()
    }

    /// A Flight client whose headers authorize exactly one call: `method`
    /// with `payload`.
    pub async fn signed(&self, method: &str, payload: &[u8]) -> Result<FlightClient, EngineError> {
        let mut client = FlightClient::new(self.channel().await?);
        let digest = digest(method, payload, &self.token);
        let auth = AuthRequest::sign(&self.identity, &self.owner, unix_now_ms(), &digest)
            .and_then(|a| a.to_header())
            .map_err(|e| EngineError::Remote(e.to_string()))?;
        client
            .add_header(HEADER_AUTH, &auth)
            .map_err(flight_to_engine)?;
        client
            .add_header(HEADER_UCAN, &self.token)
            .map_err(flight_to_engine)?;
        Ok(client)
    }

    async fn action(
        &self,
        method: &str,
        action: &'static str,
        body: Vec<u8>,
    ) -> Result<ChunkStream, EngineError> {
        let mut client = self.signed(method, &body).await?;
        let results = client
            .do_action(Action::new(action, body))
            .await
            .map_err(flight_to_engine)?;
        Ok(results.map_err(flight_to_engine).boxed())
    }

    /// The current snapshot of the tables of `space` (in the owner's
    /// namespace) this node may copy. Needs `space/read`.
    pub async fn snapshot(&self, space: &str) -> Result<SnapshotResponse, EngineError> {
        let body = serde_json::to_vec(&SnapshotRequest {
            space: space.to_string(),
        })
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        let mut results = self.action(METHOD_SNAPSHOT, ACTION_SNAPSHOT, body).await?;
        let first = results
            .try_next()
            .await?
            .ok_or_else(|| EngineError::Remote("empty snapshot response".to_string()))?;
        serde_json::from_slice(&first)
            .map_err(|e| EngineError::Remote(format!("snapshot response: {e}")))
    }

    /// Stream one data file of a snapshot. The caller checks size and hash.
    pub async fn fetch_file(&self, request: &FetchFileRequest) -> Result<ChunkStream, EngineError> {
        let body = serde_json::to_vec(request).map_err(|e| EngineError::Remote(e.to_string()))?;
        self.action(METHOD_FETCH_FILE, ACTION_FETCH_FILE, body)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(peer: &str) -> PeerClient {
        PeerClient::new(
            peer.to_string(),
            NodeIdentity::from_seed(&[1; 32]).did().clone(),
            "header.payload.signature".to_string(),
            Arc::new(NodeIdentity::from_seed(&[2; 32])),
        )
    }

    #[tokio::test]
    async fn plaintext_peers_need_allow_http() {
        let refused = client("http://127.0.0.1:1").snapshot("blog").await;
        assert!(
            matches!(&refused, Err(EngineError::PermissionDenied(m)) if m.contains("plaintext")),
            "{refused:?}"
        );
        // Allowed: it gets as far as connecting (nothing listens there).
        let allowed = client("http://127.0.0.1:1")
            .allow_http(true)
            .snapshot("blog")
            .await;
        assert!(
            matches!(allowed, Err(EngineError::Remote(_))),
            "{allowed:?}"
        );
    }

    #[test]
    fn debug_never_prints_the_token() {
        let client = PeerClient::new(
            "http://127.0.0.1:1".to_string(),
            NodeIdentity::from_seed(&[1; 32]).did().clone(),
            "header.payload.signature".to_string(),
            Arc::new(NodeIdentity::from_seed(&[2; 32])),
        );
        let text = format!("{client:?}");
        assert!(text.contains("<redacted>"));
        assert!(!text.contains("payload"));
    }
}
