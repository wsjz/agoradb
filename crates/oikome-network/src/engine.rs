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

//! [`FlightEngine`]: a remote Space seen as a [`QueryEngine`].
//!
//! The federation layer treats it like any other engine: it pushes the
//! largest subtree over the remote Space's tables down as SQL, which this
//! engine ships to the owner node with `DoGet` (query shipping, §5.3).

use std::fmt;
use std::sync::Arc;

use arrow_flight::{Action, FlightDescriptor, Ticket};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use oikome_engine::{
    Capabilities, EngineError, EngineKind, QualifiedName, QueryEngine, RecordBatchStream,
    SqlDialect, TableSource, TxHandle, Value,
};
use oikome_identity::{Did, NodeIdentity};

use crate::client::{flight_to_engine, PeerClient};
use crate::protocol::{
    schema_payload, ListTablesRequest, ListTablesResponse, QueryTicket, ACTION_LIST_TABLES,
    METHOD_DO_GET, METHOD_GET_SCHEMA, METHOD_LIST_TABLES,
};

/// Where a remote Space lives and the UCAN to query it with.
#[derive(Clone)]
pub struct RemoteSpaceConfig {
    /// Flight endpoint of the owner, `http://host:port`.
    pub peer: String,
    /// The Space's name on the owner node.
    pub remote_space: String,
    /// The owner's DID; requests are signed for it and fail on any other node.
    pub owner: Did,
    /// UCAN issued (or delegated) to this node.
    pub token: String,
    /// Accept `peer` even if it is plaintext (`http://`).
    pub allow_http: bool,
}

impl fmt::Debug for RemoteSpaceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteSpaceConfig")
            .field("peer", &self.peer)
            .field("remote_space", &self.remote_space)
            .field("owner", &self.owner)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Queries a Space on another node over Arrow Flight. Read-only.
pub struct FlightEngine {
    config: RemoteSpaceConfig,
    instance_id: String,
    client: PeerClient,
}

impl fmt::Debug for FlightEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlightEngine")
            .field("instance_id", &self.instance_id)
            .field("config", &self.config)
            .finish()
    }
}

impl FlightEngine {
    /// An engine for one remote Space, signing requests as `identity`.
    /// Connects on first use.
    pub fn new(config: RemoteSpaceConfig, identity: Arc<NodeIdentity>) -> Self {
        // Unique per remote Space, so a pushed-down statement never spans two
        // Spaces (each needs its own capability check on the owner).
        let instance_id = format!("flight:{}/{}", config.peer, config.remote_space);
        let client = PeerClient::new(
            config.peer.clone(),
            config.owner.clone(),
            config.token.clone(),
            identity,
        )
        .allow_http(config.allow_http);
        Self {
            config,
            instance_id,
            client,
        }
    }

    /// Where this engine sends its queries.
    pub fn config(&self) -> &RemoteSpaceConfig {
        &self.config
    }

    fn read_only(&self, what: &str) -> EngineError {
        EngineError::Unsupported(
            EngineKind::Flight,
            format!(
                "{what}: remote space '{}' is read-only",
                self.config.remote_space
            ),
        )
    }
}

#[async_trait]
impl QueryEngine for FlightEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Flight
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
            space: self.config.remote_space.clone(),
        })
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        let mut client = self.client.signed(METHOD_LIST_TABLES, &body).await?;
        let mut results = client
            .do_action(Action::new(ACTION_LIST_TABLES, body))
            .await
            .map_err(flight_to_engine)?;
        let first = results
            .try_next()
            .await
            .map_err(flight_to_engine)?
            .ok_or_else(|| EngineError::Remote("empty list_tables response".to_string()))?;
        let response: ListTablesResponse = serde_json::from_slice(&first)
            .map_err(|e| EngineError::Remote(format!("list_tables response: {e}")))?;
        Ok(response.tables)
    }

    /// Schema of a remote relation, restricted to the columns this node may read.
    async fn table_schema(&self, name: &QualifiedName) -> Result<SchemaRef, EngineError> {
        let space = &self.config.remote_space;
        let mut client = self
            .client
            .signed(METHOD_GET_SCHEMA, &schema_payload(space, &name.table))
            .await?;
        let schema = client
            .get_schema(FlightDescriptor::new_path(vec![
                space.clone(),
                name.table.clone(),
            ]))
            .await
            .map_err(flight_to_engine)?;
        Ok(Arc::new(schema))
    }

    /// Ship `sql` (in the owner's namespace) to the owner and stream the result.
    async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<(SchemaRef, RecordBatchStream), EngineError> {
        if !params.is_empty() {
            return Err(EngineError::Unsupported(
                EngineKind::Flight,
                "parameters in remote queries".to_string(),
            ));
        }
        let ticket = serde_json::to_vec(&QueryTicket {
            sql: sql.to_string(),
        })
        .map_err(|e| EngineError::Remote(e.to_string()))?;
        tracing::debug!(peer = %self.config.peer, space = %self.config.remote_space, %sql, "query shipping");
        let mut client = self.client.signed(METHOD_DO_GET, &ticket).await?;
        let mut stream = client
            .do_get(Ticket::new(ticket))
            .await
            .map_err(flight_to_engine)?;
        // The schema message precedes the first batch.
        let first = stream.next().await.transpose().map_err(flight_to_engine)?;
        let schema = match (stream.schema(), &first) {
            (Some(schema), _) => schema.clone(),
            (None, Some(batch)) => batch.schema(),
            (None, None) => Arc::new(Schema::empty()),
        };
        let rest = stream.map_err(flight_to_engine);
        let batches = futures::stream::iter(first.map(Ok)).chain(rest);
        Ok((schema, Box::pin(batches)))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> FlightEngine {
        let owner = NodeIdentity::from_seed(&[1; 32]);
        FlightEngine::new(
            RemoteSpaceConfig {
                peer: "http://127.0.0.1:1".to_string(),
                remote_space: "blog".to_string(),
                owner: owner.did().clone(),
                token: "header.payload.signature".to_string(),
                allow_http: true,
            },
            Arc::new(NodeIdentity::from_seed(&[2; 32])),
        )
    }

    #[test]
    fn debug_never_prints_the_token() {
        let text = format!("{:?}", engine());
        assert!(text.contains("<redacted>"));
        assert!(!text.contains("payload"));
    }

    #[tokio::test]
    async fn writes_are_rejected_and_unreachable_peers_error() {
        let e = engine();
        assert_eq!(e.instance_id(), "flight:http://127.0.0.1:1/blog");
        assert!(matches!(
            e.execute("DELETE FROM blog.posts", &[]).await,
            Err(EngineError::Unsupported(EngineKind::Flight, _))
        ));
        assert!(matches!(
            e.begin().await,
            Err(EngineError::Unsupported(EngineKind::Flight, _))
        ));
        assert!(matches!(
            e.query("SELECT 1", &[]).await,
            Err(EngineError::Remote(_))
        ));
    }
}
