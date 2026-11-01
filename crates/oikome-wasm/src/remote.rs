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

//! [`RemoteEngine`]: a Space of a Full Node, reached over a WebRTC data
//! channel (architecture v3 §10).
//!
//! Each call is an [`Envelope`] with the same fields as an Arrow Flight
//! call — method, payload, the UCAN and a request signed with the browser's
//! DID for the owner's DID — so the owner authenticates and authorizes it
//! exactly as a native peer's. The [`Transport`] only carries bytes: the
//! owner is authenticated by the DTLS fingerprint its signed access record
//! pins.

use std::fmt;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use oikome_core::EngineKind;
use oikome_engine::{
    Capabilities, EngineError, QualifiedName, QueryEngine, RecordBatchStream, SqlDialect,
    TableSource, TxHandle, Value,
};
use oikome_identity::ucan::{self, Capability, Ucan, ABILITY_QUERY};
use oikome_identity::{unix_now, unix_now_ms, AuthRequest, Did, NodeIdentity, VerifyOptions};
use oikome_protocol::webrtc::{decode_reply, AccessRecord, Envelope};
use oikome_protocol::{
    decode_stream, digest, schema_payload, ListTablesRequest, ListTablesResponse, QueryTicket,
    METHOD_DO_GET, METHOD_GET_SCHEMA, METHOD_LIST_TABLES,
};
use serde::{Deserialize, Serialize};

/// Carries one call to a node and returns its reply. The host implements
/// it over an `RTCPeerConnection` (`packages/node/src/transport.js`): one data
/// channel per node, the browser accepting only a DTLS certificate with
/// `fingerprint`.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn call(
        &self,
        addr: &str,
        fingerprint: &str,
        message: Vec<u8>,
    ) -> Result<Vec<u8>, EngineError>;
}

/// A mounted remote Space, as kept in the browser node's registry.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSpace {
    /// The owner's access record (`webrtc:...`), as printed by `SHOW WEBRTC`.
    pub record: String,
    /// The Space's name on the owner.
    pub remote: String,
    /// UCAN issued (or delegated) to the browser node.
    pub token: String,
}

impl fmt::Debug for RemoteSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteSpace")
            .field("record", &self.record)
            .field("remote", &self.remote)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Why a remote Space cannot be mounted.
#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("PEER: {0}")]
    Record(String),
    #[error("TOKEN: {0}")]
    Token(String),
}

impl RemoteSpace {
    /// Check a `CREATE SPACE ... WITH PEER = 'webrtc:...', TOKEN = ...`
    /// request for the node `me`: the record is signed by the DID it names
    /// and has not expired, and the token was issued by that DID (or
    /// delegated from it) to `me` and grants `space/query` on `remote`.
    pub fn check(
        record: &str,
        remote: &str,
        token: &str,
        me: &Did,
    ) -> Result<(AccessRecord, Did), MountError> {
        let verified = AccessRecord::verify(record, unix_now())
            .map_err(|e| MountError::Record(e.to_string()))?;
        let owner = Did::parse(&verified.did).map_err(|e| MountError::Record(e.to_string()))?;
        let issuer = Ucan::decode(token)
            .and_then(|t| t.root_issuer())
            .map_err(|e| MountError::Token(e.to_string()))?;
        if issuer != owner {
            return Err(MountError::Token(format!(
                "issued by {issuer}, but the access record is for {owner}"
            )));
        }
        ucan::verify(
            token,
            &VerifyOptions {
                root: &owner,
                audience: me,
                required: &Capability::space(&owner, remote, ABILITY_QUERY),
                now: unix_now(),
                revoked: &Default::default(),
            },
        )
        .map_err(|e| MountError::Token(format!("does not let this node query '{remote}': {e}")))?;
        Ok((verified, owner))
    }
}

/// A remote Space as a read-only [`QueryEngine`].
pub struct RemoteEngine {
    record: AccessRecord,
    owner: Did,
    remote: String,
    token: String,
    identity: Arc<NodeIdentity>,
    transport: Arc<dyn Transport>,
    instance_id: String,
}

impl fmt::Debug for RemoteEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEngine")
            .field("instance_id", &self.instance_id)
            .field("addr", &self.record.addr)
            .finish_non_exhaustive()
    }
}

impl RemoteEngine {
    /// An engine for `space`, signing as `identity`. The record and token
    /// are checked again, so an expired record or token fails here.
    pub fn new(
        space: &RemoteSpace,
        identity: Arc<NodeIdentity>,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, EngineError> {
        let (record, owner) =
            RemoteSpace::check(&space.record, &space.remote, &space.token, identity.did())
                .map_err(|e| EngineError::PermissionDenied(e.to_string()))?;
        // One instance per remote Space, so a pushed-down statement never
        // spans two Spaces (each needs its own capability check on the owner).
        let instance_id = format!("webrtc:{owner}/{}", space.remote);
        Ok(Self {
            record,
            owner,
            remote: space.remote.clone(),
            token: space.token.clone(),
            identity,
            transport,
            instance_id,
        })
    }

    /// Call `method` on the owner; returns the reply body.
    async fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, EngineError> {
        let auth = AuthRequest::sign(
            &self.identity,
            &self.owner,
            unix_now_ms(),
            &digest(method, payload, &self.token),
        )
        .and_then(|a| a.to_header())
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        let envelope = Envelope::new(method, payload, &self.token, &auth);
        let message =
            serde_json::to_vec(&envelope).map_err(|e| EngineError::Remote(e.to_string()))?;
        let reply = self
            .transport
            .call(&self.record.addr, &self.record.fingerprint, message)
            .await?;
        let (header, body) =
            decode_reply(&reply).map_err(|e| EngineError::Remote(e.to_string()))?;
        match header.status.as_str() {
            "ok" => Ok(body.to_vec()),
            "permission_denied" | "unauthenticated" => {
                Err(EngineError::PermissionDenied(header.message))
            }
            "not_found" => Err(EngineError::TableNotFound(header.message)),
            status => Err(EngineError::Remote(format!("{status}: {}", header.message))),
        }
    }

    fn read_only(&self, what: &str) -> EngineError {
        EngineError::Unsupported(
            EngineKind::WebRtc,
            format!("{what}: remote space '{}' is read-only", self.remote),
        )
    }
}

#[async_trait]
impl QueryEngine for RemoteEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::WebRtc
    }

    fn dialect(&self) -> SqlDialect {
        // The owner parses with a generic dialect and re-emits for its engine.
        SqlDialect::Generic
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::ARROW_OUT | Capabilities::PUSHDOWN_JOIN | Capabilities::PUSHDOWN_AGG
    }

    fn instance_id(&self) -> &str {
        &self.instance_id
    }

    async fn attach(&self, _name: &QualifiedName, _source: TableSource) -> Result<(), EngineError> {
        Err(self.read_only("attach"))
    }

    async fn detach(&self, _name: &QualifiedName) -> Result<(), EngineError> {
        Err(self.read_only("detach"))
    }

    /// Relations of the remote Space this node may read.
    async fn table_names(&self, _space: &str) -> Result<Vec<String>, EngineError> {
        let body = serde_json::to_vec(&ListTablesRequest {
            space: self.remote.clone(),
        })
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        let reply = self.call(METHOD_LIST_TABLES, &body).await?;
        let response: ListTablesResponse = serde_json::from_slice(&reply)
            .map_err(|e| EngineError::Remote(format!("list_tables response: {e}")))?;
        Ok(response.tables)
    }

    /// Schema of a remote relation, restricted to the columns this node may read.
    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        let reply = self
            .call(
                METHOD_GET_SCHEMA,
                &schema_payload(&self.remote, &name.table),
            )
            .await?;
        Ok(decode_stream(reply)?.0)
    }

    /// Ship `sql` (in the owner's namespace) to the owner.
    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        if !params.is_empty() {
            return Err(EngineError::Unsupported(
                EngineKind::WebRtc,
                "parameters in remote queries".to_string(),
            ));
        }
        let ticket = serde_json::to_vec(&QueryTicket {
            sql: sql.to_string(),
        })
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        let reply = self.call(METHOD_DO_GET, &ticket).await?;
        let (schema, batches) = if reply.is_empty() {
            (Arc::new(Schema::empty()), Vec::new())
        } else {
            decode_stream(reply)?
        };
        Ok((
            schema,
            Box::pin(futures::stream::iter(batches.into_iter().map(Ok))),
        ))
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<u64, EngineError> {
        Err(self.read_only("execute"))
    }

    async fn begin(&self) -> Result<TxHandle, EngineError> {
        Err(self.read_only("begin"))
    }

    async fn commit(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(self.read_only("commit"))
    }

    async fn rollback(&self, _tx: TxHandle) -> Result<(), EngineError> {
        Err(self.read_only("rollback"))
    }
}
