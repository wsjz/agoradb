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

//! The Arrow Flight service a node exposes to its peers.
//!
//! Every call goes through the same gate:
//!
//! 1. **Authenticate** — the `x-oikome-auth` header must be a fresh, unseen
//!    request signed by the caller's DID for this node, over this call's
//!    method, payload and UCAN.
//! 2. **Authorize** — the `x-oikome-ucan` token must chain back to this
//!    node, be issued to the caller, be unexpired and unrevoked (re-read
//!    from the registry on every call), and grant the call's ability on
//!    every Space the request names: `space/query` to query,
//!    `space/read` to subscribe. Remote Spaces are never served.
//! 3. **Execute** as a read-only principal — the DID the node issued the
//!    root token to — through the node's own semantic layer, so its grants,
//!    column restrictions and row policies apply to whatever the peer sent.
//!    A subscription copies files, which no policy can filter, so it only
//!    covers tables the principal may read in full.
//!
//! Authentication and capability failures all look the same to the caller.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::SchemaRef;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use oikome_catalog::lance::{lance_tables, lance_version, DATASET_SUFFIX};
use oikome_catalog::{LocationFormat, OikomeCatalog, Space};
use oikome_core::{CatalogError, SpaceKind};
use oikome_engine::{EngineError, RecordBatchStream};
use oikome_identity::ucan::{
    self, Capability, VerifyOptions, ABILITY_QUERY, ABILITY_READ, MAX_TOKEN_BYTES,
};
use oikome_identity::{unix_now, unix_now_ms, AuthRequest, Did, NodeIdentity};
use oikome_network::protocol::webrtc::{encode_reply, Envelope, ReplyHeader};
use oikome_network::protocol::{
    digest, encode_schema, encode_stream, is_file_hash, is_identifier, parse_schema_payload,
    schema_payload, FetchFileRequest, FileEntry, ListTablesRequest, ListTablesResponse,
    QueryTicket, SnapshotRequest, SnapshotResponse, TableFormat, TableSnapshot, ACTION_FETCH_FILE,
    ACTION_LIST_TABLES, ACTION_SNAPSHOT, FILE_CHUNK_BYTES, HEADER_AUTH, HEADER_UCAN, MAX_SQL_BYTES,
    METHOD_DO_GET, METHOD_FETCH_FILE, METHOD_GET_SCHEMA, METHOD_LIST_TABLES, METHOD_SNAPSHOT,
};
use oikome_node::{query_spaces, EngineRegistry, OikomeSession, SessionConfig, SessionError};
use oikome_semantic::rewrite::quote_ident;
use oikome_semantic::SemanticError;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status, Streaming};

use crate::replay::{ReplayError, ReplayGuard};

const DENIED: &str = "permission denied";
const MAX_ACTION_BODY_BYTES: usize = 4096;
/// Entries kept by [`HashCache`] before it starts over.
const HASH_CACHE_ENTRIES: usize = 100_000;

/// BLAKE3 hashes of data files, keyed by path, size and modification time.
/// Data files are immutable, so an entry never goes stale.
#[derive(Default)]
struct HashCache(Mutex<HashMap<(PathBuf, u64, SystemTime), String>>);

impl HashCache {
    /// Hash and size of the file at `path`.
    async fn hash(&self, path: &Path) -> std::io::Result<(String, u64)> {
        let meta = tokio::fs::metadata(path).await?;
        let key = (path.to_path_buf(), meta.len(), meta.modified()?);
        if let Some(hash) = self.0.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
            return Ok((hash.clone(), meta.len()));
        }
        let owned = path.to_path_buf();
        let hash = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
            let mut hasher = blake3::Hasher::new();
            hasher.update_reader(std::fs::File::open(&owned)?)?;
            Ok(hasher.finalize().to_hex().to_string())
        })
        .await
        .map_err(std::io::Error::other)??;
        let mut map = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if map.len() >= HASH_CACHE_ENTRIES {
            map.clear();
        }
        map.insert(key, hash.clone());
        Ok((hash, meta.len()))
    }
}

/// Log a local I/O failure and return a status that does not reveal it.
fn io_status(e: std::io::Error) -> Status {
    tracing::warn!(error = %e, "serving a data file failed");
    Status::internal("reading data failed on the remote node")
}

type FlightStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// An authenticated request: who sent it and the UCAN it presented.
struct Caller {
    did: Did,
    ucan: String,
}

/// The node's Flight endpoint.
pub struct OikomeFlightService {
    catalog: Arc<OikomeCatalog>,
    engines: Arc<EngineRegistry>,
    identity: Arc<NodeIdentity>,
    replay: ReplayGuard,
    hashes: HashCache,
}

impl std::fmt::Debug for OikomeFlightService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OikomeFlightService")
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

impl OikomeFlightService {
    /// A service answering for the node whose engines are `engines`.
    pub fn new(
        catalog: Arc<OikomeCatalog>,
        engines: Arc<EngineRegistry>,
    ) -> Result<Self, SessionError> {
        let identity = engines.identity()?;
        Ok(Self {
            catalog,
            engines,
            identity,
            replay: ReplayGuard::default(),
            hashes: HashCache::default(),
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
        self.authenticate_parts(
            Self::header(request, HEADER_AUTH),
            Self::header(request, HEADER_UCAN),
            method,
            payload,
        )
    }

    /// Step 1 on the values of the two headers, whatever carried them.
    fn authenticate_parts(
        &self,
        auth: Option<&str>,
        ucan: Option<&str>,
        method: &str,
        payload: &[u8],
    ) -> Result<Caller, Status> {
        let fail = |reason: &str| {
            tracing::info!(%reason, method, "peer authentication failed");
            Status::unauthenticated("authentication failed")
        };
        let ucan = ucan.ok_or_else(|| fail("missing UCAN"))?;
        if ucan.len() > MAX_TOKEN_BYTES {
            return Err(fail("UCAN too large"));
        }
        let auth = auth
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

    /// Step 2: check that the caller's UCAN grants `ability` on every Space
    /// it names and return the principal to run the request as.
    fn authorize(
        &self,
        caller: &Caller,
        spaces: &BTreeSet<String>,
        ability: &str,
    ) -> Result<String, Status> {
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
                    required: &Capability::space(self.identity.did(), space, ability),
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
    fn session(&self, principal: String) -> OikomeSession {
        OikomeSession::new(
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
        self.list_tables_for(&caller, &body).await
    }

    async fn list_tables_for(&self, caller: &Caller, body: &[u8]) -> Result<Vec<u8>, Status> {
        let req: ListTablesRequest = serde_json::from_slice(body)
            .map_err(|e| Status::invalid_argument(format!("list_tables request: {e}")))?;
        if !is_identifier(&req.space) {
            return Err(Status::invalid_argument("invalid space name"));
        }
        let principal =
            self.authorize(caller, &BTreeSet::from([req.space.clone()]), ABILITY_QUERY)?;
        let tables = self
            .session(principal)
            .readable_relations(&req.space)
            .await
            .map_err(session_status)?;
        serde_json::to_vec(&ListTablesResponse { tables })
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// A Space a peer may subscribe to: analytical and stored as Iceberg
    /// tables or Lance datasets. Transactional Spaces are shared by
    /// publishing them. Returns the Space and, for Lance, its datasets'
    /// directory.
    fn subscribable(&self, name: &str) -> Result<(Space, Option<PathBuf>), Status> {
        let space = self
            .catalog
            .get_space(name)
            .map_err(|e| session_status(e.into()))?;
        match space.kind {
            SpaceKind::Analytical => {}
            SpaceKind::Transactional => {
                return Err(Status::failed_precondition(format!(
                    "space '{name}' is transactional: PUBLISH it and subscribe to the publication"
                )))
            }
            // authorize() already refuses these; never serve them onwards.
            SpaceKind::Remote => return Err(Status::permission_denied(DENIED)),
        }
        match self.catalog.get_location(&space.location) {
            Ok(l) if matches!(l.format, LocationFormat::IcebergParquet { .. }) => Ok((space, None)),
            Ok(l) if matches!(l.format, LocationFormat::Lance { .. }) => {
                let root = self
                    .catalog
                    .lance_root(&l)
                    .map_err(|e| session_status(e.into()))?;
                Ok((space, Some(root)))
            }
            _ => Err(Status::failed_precondition(format!(
                "space '{name}' is not stored as Iceberg tables or Lance datasets and cannot be subscribed"
            ))),
        }
    }

    /// Whether `principal` may read `space.table` in full: `None` if it has
    /// no grant on it at all, `Some(false)` under a column grant or a row
    /// policy.
    fn full_access(&self, principal: &str, space: &str, table: &str) -> Option<bool> {
        let grant = self.catalog.get_grant(principal, space, table)?;
        Some(grant.columns.is_none() && self.catalog.policies_on(space, table).is_empty())
    }

    /// `oikome.snapshot`: the current snapshot of every table of a Space the
    /// caller may copy, with its files named by hash.
    async fn snapshot(&self, request: Request<Action>) -> Result<Vec<u8>, Status> {
        let body = request.get_ref().body.clone();
        if body.len() > MAX_ACTION_BODY_BYTES {
            return Err(Status::invalid_argument("request too large"));
        }
        let caller = self.authenticate(&request, METHOD_SNAPSHOT, &body)?;
        let req: SnapshotRequest = serde_json::from_slice(&body)
            .map_err(|e| Status::invalid_argument(format!("snapshot request: {e}")))?;
        if !is_identifier(&req.space) {
            return Err(Status::invalid_argument("invalid space name"));
        }
        let principal =
            self.authorize(&caller, &BTreeSet::from([req.space.clone()]), ABILITY_READ)?;
        let (space, lance) = self.subscribable(&req.space)?;
        let mut response = SnapshotResponse::default();
        let tables = match &lance {
            Some(root) => lance_tables(root),
            None => self.catalog.space_tables(&space).await,
        }
        .map_err(|e| session_status(e.into()))?;
        for table in tables {
            match self.full_access(&principal, &space.name, &table) {
                None => continue,
                Some(false) => {
                    response.restricted.push(table);
                    continue;
                }
                Some(true) => {}
            }
            if let Some(root) = &lance {
                let dataset = root.join(format!("{table}{DATASET_SUFFIX}"));
                let version = lance_version(&dataset, None)
                    .await
                    .map_err(|e| session_status(e.into()))?;
                let mut files = Vec::with_capacity(version.files.len() + 1);
                for relative in version.files.iter().chain([&version.manifest]) {
                    let (hash, size) = self
                        .hashes
                        .hash(&dataset.join(relative))
                        .await
                        .map_err(io_status)?;
                    files.push(FileEntry {
                        hash,
                        size,
                        path: Some(relative.clone()),
                    });
                }
                response.tables.push(TableSnapshot {
                    name: table,
                    snapshot_id: Some(version.version as i64),
                    schema: encode_schema(&version.schema)
                        .map_err(|e| Status::internal(e.to_string()))?,
                    files,
                    format: TableFormat::Lance,
                });
                continue;
            }
            let resolved = self
                .catalog
                .resolve_table(&space, &table, None)
                .await
                .map_err(|e| session_status(e.into()))?;
            let mut files = Vec::with_capacity(resolved.files.len());
            for path in &resolved.files {
                let (hash, size) = self.hashes.hash(path).await.map_err(io_status)?;
                files.push(FileEntry {
                    hash,
                    size,
                    path: None,
                });
            }
            response.tables.push(TableSnapshot {
                name: table,
                snapshot_id: resolved.snapshot_id,
                schema: encode_schema(&resolved.schema)
                    .map_err(|e| Status::internal(e.to_string()))?,
                files,
                format: TableFormat::Iceberg,
            });
        }
        tracing::debug!(caller = %caller.did, %principal, space = %space.name, tables = response.tables.len(), "serving snapshot");
        serde_json::to_vec(&response).map_err(|e| Status::internal(e.to_string()))
    }

    /// `oikome.fetch_file`: stream one data file of a table's snapshot.
    async fn fetch_file(
        &self,
        request: Request<Action>,
    ) -> Result<FlightStream<arrow_flight::Result>, Status> {
        let body = request.get_ref().body.clone();
        if body.len() > MAX_ACTION_BODY_BYTES {
            return Err(Status::invalid_argument("request too large"));
        }
        let caller = self.authenticate(&request, METHOD_FETCH_FILE, &body)?;
        let req: FetchFileRequest = serde_json::from_slice(&body)
            .map_err(|e| Status::invalid_argument(format!("fetch_file request: {e}")))?;
        if !is_identifier(&req.space) || !is_identifier(&req.table) || !is_file_hash(&req.hash) {
            return Err(Status::invalid_argument("invalid space, table or file"));
        }
        let principal =
            self.authorize(&caller, &BTreeSet::from([req.space.clone()]), ABILITY_READ)?;
        let (space, lance) = self.subscribable(&req.space)?;
        if self.full_access(&principal, &space.name, &req.table) != Some(true) {
            tracing::info!(caller = %caller.did, table = %req.table, "file of a restricted table refused");
            return Err(Status::permission_denied(DENIED));
        }
        let not_found = || Status::not_found("file not found in this snapshot");
        let files = match &lance {
            Some(root) => {
                let dataset = root.join(format!("{}{DATASET_SUFFIX}", req.table));
                let version = u64::try_from(req.snapshot_id).map_err(|_| not_found())?;
                let version = lance_version(&dataset, Some(version)).await.map_err(|e| {
                    tracing::info!(error = %e, "fetch_file: version not resolved");
                    not_found()
                })?;
                version
                    .files
                    .iter()
                    .chain([&version.manifest])
                    .map(|relative| dataset.join(relative))
                    .collect()
            }
            None => {
                self.catalog
                    .resolve_table(&space, &req.table, Some(req.snapshot_id))
                    .await
                    .map_err(|e| {
                        tracing::info!(error = %e, "fetch_file: snapshot not resolved");
                        not_found()
                    })?
                    .files
            }
        };
        let mut found = None;
        for path in files {
            if self.hashes.hash(&path).await.map_err(io_status)?.0 == req.hash {
                found = Some(path);
                break;
            }
        }
        let file = tokio::fs::File::open(found.ok_or_else(not_found)?)
            .await
            .map_err(io_status)?;
        let chunks = stream::try_unfold(file, |mut file| async move {
            let mut buf = vec![0u8; FILE_CHUNK_BYTES];
            let n = file.read(&mut buf).await?;
            if n == 0 {
                return Ok(None);
            }
            buf.truncate(n);
            Ok(Some((arrow_flight::Result { body: buf.into() }, file)))
        })
        .map_err(io_status);
        Ok(Box::pin(chunks))
    }
}

impl OikomeFlightService {
    /// Schema of `space.table` as `caller`'s principal sees it.
    async fn schema_for(
        &self,
        caller: &Caller,
        space: &str,
        table: &str,
    ) -> Result<SchemaRef, Status> {
        let principal =
            self.authorize(caller, &BTreeSet::from([space.to_string()]), ABILITY_QUERY)?;
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
        Ok(schema)
    }

    /// Run the query of a ticket for `caller`'s principal.
    async fn query_for(
        &self,
        caller: &Caller,
        ticket: &[u8],
    ) -> Result<(SchemaRef, RecordBatchStream), Status> {
        let QueryTicket { sql } = serde_json::from_slice(ticket)
            .map_err(|e| Status::invalid_argument(format!("ticket: {e}")))?;
        let spaces = query_spaces(&sql).map_err(session_status)?;
        let principal = self.authorize(caller, &spaces, ABILITY_QUERY)?;
        tracing::debug!(caller = %caller.did, %principal, ?spaces, "serving peer query");
        self.session(principal)
            .sql_stream(&sql)
            .await
            .map_err(session_status)
    }

    /// Serve one call that did not come over Arrow Flight (a browser's
    /// WebRTC channel, §10): the same authentication, authorization and
    /// semantic layer, with the result as one message — an Arrow IPC stream
    /// for `do_get` and `get_schema`, JSON for `list_tables`.
    pub async fn serve_envelope(&self, envelope: &Envelope) -> Result<Vec<u8>, Status> {
        let payload = envelope
            .payload()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let caller = self.authenticate_parts(
            Some(&envelope.auth),
            Some(&envelope.ucan),
            &envelope.method,
            &payload,
        )?;
        let ipc = |schema: &SchemaRef, batches: &[RecordBatch]| {
            encode_stream(schema, batches).map_err(|e| Status::internal(e.to_string()))
        };
        match envelope.method.as_str() {
            METHOD_LIST_TABLES if payload.len() <= MAX_ACTION_BODY_BYTES => {
                self.list_tables_for(&caller, &payload).await
            }
            METHOD_GET_SCHEMA => {
                let (space, table) = parse_schema_payload(&payload)
                    .filter(|(s, t)| is_identifier(s) && is_identifier(t))
                    .ok_or_else(|| Status::invalid_argument("invalid space or table name"))?;
                ipc(&self.schema_for(&caller, &space, &table).await?, &[])
            }
            METHOD_DO_GET if payload.len() <= MAX_SQL_BYTES + 64 => {
                let (schema, stream) = self.query_for(&caller, &payload).await?;
                let batches: Vec<RecordBatch> = stream
                    .try_collect()
                    .await
                    .map_err(|e| session_status(e.into()))?;
                ipc(&schema, &batches)
            }
            METHOD_LIST_TABLES | METHOD_DO_GET => {
                Err(Status::invalid_argument("request too large"))
            }
            _ => Err(Status::unimplemented("unknown method")),
        }
    }
}

/// The reply status for a gRPC code, as sent over WebRTC.
fn status_name(code: tonic::Code) -> &'static str {
    use tonic::Code;
    match code {
        Code::Ok => "ok",
        Code::Unauthenticated => "unauthenticated",
        Code::PermissionDenied => "permission_denied",
        Code::NotFound => "not_found",
        Code::InvalidArgument => "invalid_argument",
        Code::FailedPrecondition => "failed_precondition",
        Code::ResourceExhausted => "resource_exhausted",
        Code::Unimplemented => "unimplemented",
        _ => "internal",
    }
}

#[async_trait::async_trait]
impl oikome_webrtc::Handler for OikomeFlightService {
    async fn call(&self, envelope: Envelope) -> Vec<u8> {
        let (header, body) = match self.serve_envelope(&envelope).await {
            Ok(body) => (
                ReplyHeader {
                    status: "ok".to_string(),
                    message: String::new(),
                },
                body,
            ),
            Err(status) => (
                ReplyHeader {
                    status: status_name(status.code()).to_string(),
                    message: status.message().to_string(),
                },
                Vec::new(),
            ),
        };
        encode_reply(&header, &body)
    }
}

fn single_result(body: Vec<u8>) -> FlightStream<arrow_flight::Result> {
    stream::once(async move { Ok(arrow_flight::Result { body: body.into() }) }).boxed()
}

#[tonic::async_trait]
impl FlightService for OikomeFlightService {
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
            "no handshake: every request is signed (x-oikome-auth)",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("use the oikome.list_tables action"))
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
        let schema = self.schema_for(&caller, space, table).await?;
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
        let (schema, batches) = self.query_for(&caller, &ticket).await?;
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
        let out = match request.get_ref().r#type.as_str() {
            ACTION_LIST_TABLES => single_result(self.list_tables(request).await?),
            ACTION_SNAPSHOT => single_result(self.snapshot(request).await?),
            ACTION_FETCH_FILE => self.fetch_file(request).await?,
            _ => return Err(Status::unimplemented("unknown action")),
        };
        Ok(Response::new(out))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let action = |name: &str, description: &str| {
            Ok(ActionType {
                r#type: name.to_string(),
                description: description.to_string(),
            })
        };
        let actions = vec![
            action(
                ACTION_LIST_TABLES,
                "Relations of a Space the caller may read",
            ),
            action(
                ACTION_SNAPSHOT,
                "Current snapshot of the tables the caller may copy (space/read)",
            ),
            action(
                ACTION_FETCH_FILE,
                "One data file of a snapshot, by hash (space/read)",
            ),
        ];
        Ok(Response::new(Box::pin(stream::iter(actions))))
    }
}
