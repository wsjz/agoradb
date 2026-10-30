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

//! The Arrow Flight service a node exposes to its peers.
//!
//! Every call goes through the same gate:
//!
//! 1. **Authenticate** — the `x-agora-auth` header must be a fresh, unseen
//!    request signed by the caller's DID for this node, over this call's
//!    method, payload and UCAN.
//! 2. **Authorize** — the `x-agora-ucan` token must chain back to this
//!    node, be issued to the caller, be unexpired and unrevoked (re-read
//!    from the registry on every call), and grant `space/query` on every
//!    Space the request names. Remote Spaces are never served.
//! 3. **Execute** as a read-only principal — the DID the node issued the
//!    root token to — through the node's own semantic layer, so its grants,
//!    column restrictions and row policies apply to whatever the peer sent.
//!
//! Authentication and capability failures all look the same to the caller.

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::{CatalogError, SpaceKind};
use agoradb_engine::EngineError;
use agoradb_identity::ucan::{self, Capability, VerifyOptions, ABILITY_QUERY, MAX_TOKEN_BYTES};
use agoradb_identity::{unix_now, unix_now_ms, AuthRequest, Did, NodeIdentity};
use agoradb_network::protocol::{
    digest, is_identifier, schema_payload, ListTablesRequest, ListTablesResponse, QueryTicket,
    ACTION_LIST_TABLES, HEADER_AUTH, HEADER_UCAN, MAX_SQL_BYTES, METHOD_DO_GET, METHOD_GET_SCHEMA,
    METHOD_LIST_TABLES,
};
use agoradb_node::{query_spaces, AgoraSession, EngineRegistry, SessionConfig, SessionError};
use agoradb_semantic::rewrite::quote_ident;
use agoradb_semantic::SemanticError;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::replay::{ReplayError, ReplayGuard};

const DENIED: &str = "permission denied";
const MAX_ACTION_BODY_BYTES: usize = 4096;

type FlightStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// An authenticated request: who sent it and the UCAN it presented.
struct Caller {
    did: Did,
    ucan: String,
}

/// The node's Flight endpoint.
pub struct AgoraFlightService {
    catalog: Arc<AgoraCatalog>,
    engines: Arc<EngineRegistry>,
    identity: Arc<NodeIdentity>,
    replay: ReplayGuard,
}

impl std::fmt::Debug for AgoraFlightService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgoraFlightService")
            .field("did", self.identity.did())
            .finish_non_exhaustive()
    }
}

/// Map a session error to a status a peer may see. Details of internal
/// failures (paths, storage errors) are logged, not returned.
fn session_status(e: SessionError) -> Status {
    match e {
        SessionError::PermissionDenied(msg) => Status::permission_denied(msg),
        SessionError::Semantic(SemanticError::TableNotFound(t)) => {
            Status::not_found(format!("Table not found: {t}"))
        }
        SessionError::Catalog(CatalogError::SpaceNotFound(s)) => {
            Status::not_found(format!("Space not found: {s}"))
        }
        SessionError::Catalog(CatalogError::TableNotFound(t)) => {
            Status::not_found(format!("Table not found: {t}"))
        }
        e @ (SessionError::Semantic(_)
        | SessionError::Unsupported(_)
        | SessionError::NoDefaultSpace
        | SessionError::Engine(
            EngineError::Sql(_) | EngineError::TableNotFound(_) | EngineError::TypeMismatch { .. },
        )) => Status::invalid_argument(e.to_string()),
        other => {
            tracing::warn!(error = %other, "peer request failed");
            Status::internal("query failed on the remote node")
        }
    }
}

impl AgoraFlightService {
    /// A service answering for the node whose engines are `engines`.
    pub fn new(
        catalog: Arc<AgoraCatalog>,
        engines: Arc<EngineRegistry>,
    ) -> Result<Self, SessionError> {
        let identity = engines.identity()?;
        Ok(Self {
            catalog,
            engines,
            identity,
            replay: ReplayGuard::default(),
        })
    }

    /// This node's DID.
    pub fn did(&self) -> &Did {
        self.identity.did()
    }

    fn header<'a, T>(request: &'a Request<T>, name: &str) -> Option<&'a str> {
        request.metadata().get(name).and_then(|v| v.to_str().ok())
    }

    /// Step 1: check the signed request header for `method` with `payload`.
    fn authenticate<T>(
        &self,
        request: &Request<T>,
        method: &str,
        payload: &[u8],
    ) -> Result<Caller, Status> {
        let fail = |reason: &str| {
            tracing::info!(%reason, method, "peer authentication failed");
            Status::unauthenticated("authentication failed")
        };
        let ucan = Self::header(request, HEADER_UCAN).ok_or_else(|| fail("missing UCAN"))?;
        if ucan.len() > MAX_TOKEN_BYTES {
            return Err(fail("UCAN too large"));
        }
        let auth = Self::header(request, HEADER_AUTH)
            .ok_or_else(|| fail("missing auth header"))
            .and_then(|h| AuthRequest::from_header(h).map_err(|e| fail(&e.to_string())))?;
        let now = unix_now_ms();
        let did = auth
            .verify(self.identity.did(), now, &digest(method, payload, ucan))
            .map_err(|e| fail(&e.to_string()))?;
        match self
            .replay
            .check(did.as_str(), &auth.nonce, auth.timestamp_ms, now)
        {
            Ok(()) => {}
            Err(ReplayError::Replayed) => return Err(fail("replayed request")),
            Err(ReplayError::Saturated) => {
                return Err(Status::resource_exhausted("too many requests"))
            }
        }
        Ok(Caller {
            did,
            ucan: ucan.to_string(),
        })
    }

    /// Step 2: check the caller's UCAN against every Space it wants to read
    /// and return the principal to run the request as.
    fn authorize(&self, caller: &Caller, spaces: &BTreeSet<String>) -> Result<String, Status> {
        let deny = |reason: String| {
            tracing::info!(caller = %caller.did, %reason, "peer request denied");
            Status::permission_denied(DENIED)
        };
        if spaces.is_empty() {
            return Err(deny("request names no space".to_string()));
        }
        // Pick up revocations and grants made by other processes.
        self.catalog.refresh().map_err(|e| {
            tracing::warn!(error = %e, "could not reload the registry");
            Status::internal("registry unavailable")
        })?;
        let revoked = self.catalog.revoked_ucans();
        let now = unix_now();
        let mut principal: Option<Did> = None;
        for space in spaces {
            let verified = ucan::verify(
                &caller.ucan,
                &VerifyOptions {
                    root: self.identity.did(),
                    audience: &caller.did,
                    required: &Capability::space(self.identity.did(), space, ABILITY_QUERY),
                    now,
                    revoked: &revoked,
                },
            )
            .map_err(|e| deny(format!("{space}: {e}")))?;
            if principal.as_ref().is_some_and(|p| *p != verified.principal) {
                return Err(deny("one request, several principals".to_string()));
            }
            principal = Some(verified.principal);
            if self
                .catalog
                .get_space(space)
                .is_ok_and(|s| s.kind == SpaceKind::Remote)
            {
                return Err(deny(format!("{space} is a remote space")));
            }
        }
        principal
            .map(|p| p.to_string())
            .ok_or_else(|| deny("no principal".to_string()))
    }

    /// Step 3: a read-only session acting for `principal`.
    fn session(&self, principal: String) -> AgoraSession {
        AgoraSession::new(
            self.catalog.clone(),
            self.engines.clone(),
            SessionConfig {
                default_space: None,
                principal: Some(principal),
            },
        )
    }

    async fn list_tables(&self, request: Request<Action>) -> Result<Vec<u8>, Status> {
        let body = request.get_ref().body.clone();
        if body.len() > MAX_ACTION_BODY_BYTES {
            return Err(Status::invalid_argument("request too large"));
        }
        let caller = self.authenticate(&request, METHOD_LIST_TABLES, &body)?;
        let req: ListTablesRequest = serde_json::from_slice(&body)
            .map_err(|e| Status::invalid_argument(format!("list_tables request: {e}")))?;
        if !is_identifier(&req.space) {
            return Err(Status::invalid_argument("invalid space name"));
        }
        let principal = self.authorize(&caller, &BTreeSet::from([req.space.clone()]))?;
        let tables = self
            .session(principal)
            .readable_relations(&req.space)
            .await
            .map_err(session_status)?;
        serde_json::to_vec(&ListTablesResponse { tables })
            .map_err(|e| Status::internal(e.to_string()))
    }
}

#[tonic::async_trait]
impl FlightService for AgoraFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoExchangeStream = FlightStream<FlightData>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "no handshake: every request is signed (x-agora-auth)",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("use the agora.list_tables action"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("send queries with DoGet"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("send queries with DoGet"))
    }

    /// Schema of `[space, table]` as the caller sees it (granted columns only).
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let path = request.get_ref().path.clone();
        let [space, table] = path.as_slice() else {
            return Err(Status::invalid_argument("expected the path [space, table]"));
        };
        if !is_identifier(space) || !is_identifier(table) {
            return Err(Status::invalid_argument("invalid space or table name"));
        }
        let caller =
            self.authenticate(&request, METHOD_GET_SCHEMA, &schema_payload(space, table))?;
        let principal = self.authorize(&caller, &BTreeSet::from([space.clone()]))?;
        // Both names are validated identifiers, and quoted on top.
        let sql = format!(
            "SELECT * FROM {}.{} LIMIT 0",
            quote_ident(space),
            quote_ident(table)
        );
        let (schema, _) = self
            .session(principal)
            .sql_stream(&sql)
            .await
            .map_err(session_status)?;
        let result: SchemaResult = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: arrow_schema::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(result))
    }

    /// Run a query in this node's namespace and stream the result.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.get_ref().ticket.clone();
        if ticket.len() > MAX_SQL_BYTES + 64 {
            return Err(Status::invalid_argument("query too large"));
        }
        let caller = self.authenticate(&request, METHOD_DO_GET, &ticket)?;
        let QueryTicket { sql } = serde_json::from_slice(&ticket)
            .map_err(|e| Status::invalid_argument(format!("ticket: {e}")))?;
        let spaces = query_spaces(&sql).map_err(session_status)?;
        let principal = self.authorize(&caller, &spaces)?;
        tracing::debug!(caller = %caller.did, %principal, ?spaces, "serving peer query");
        let (schema, batches) = self
            .session(principal)
            .sql_stream(&sql)
            .await
            .map_err(session_status)?;
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batches.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("peers are read-only"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("peers are read-only"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        if request.get_ref().r#type != ACTION_LIST_TABLES {
            return Err(Status::unimplemented("unknown action"));
        }
        let body = self.list_tables(request).await?;
        let out: BoxStream<'static, Result<arrow_flight::Result, Status>> =
            stream::once(async move { Ok(arrow_flight::Result { body: body.into() }) }).boxed();
        Ok(Response::new(out))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: ACTION_LIST_TABLES.to_string(),
            description: "Relations of a Space the caller may read".to_string(),
        })];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}
