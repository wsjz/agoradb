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

//! Test nodes for the multi-node tests: each has its own root directory and
//! identity, and can serve Arrow Flight on an ephemeral port, over TLS
//! unless a test asks for plaintext.

// Each test binary uses a different subset of these helpers.
#![allow(dead_code)]

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_identity::NodeIdentity;
use agoradb_node::{AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig};
use agoradb_server::{serve, AgoraFlightService};
use arrow_array::{Array, Int64Array, StringArray};
use iceberg::io::FileIO;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct Node {
    _dir: tempfile::TempDir,
    pub catalog: Arc<AgoraCatalog>,
    pub engines: Arc<EngineRegistry>,
    pub identity: Arc<NodeIdentity>,
    pub owner: AgoraSession,
    url: Option<String>,
    server: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl Node {
    /// A node that only reaches peers over TLS.
    pub fn new(seed: u8) -> Self {
        Self::with_config(seed, false)
    }

    /// A node that also accepts plaintext (`http://`) peers.
    pub fn allowing_http(seed: u8) -> Self {
        Self::with_config(seed, true)
    }

    fn with_config(seed: u8, allow_http: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(
            AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap(),
        );
        let identity = Arc::new(NodeIdentity::from_seed(&[seed; 32]));
        let engines = Arc::new(
            EngineRegistry::new(
                catalog.clone(),
                NodeConfig {
                    identity: Some(identity.clone()),
                    allow_http,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let owner = AgoraSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
        Self {
            _dir: dir,
            catalog,
            engines,
            identity,
            owner,
            url: None,
            server: None,
        }
    }

    /// Start serving Arrow Flight over TLS on an ephemeral port.
    pub async fn serve(self) -> Self {
        self.serve_with(true).await
    }

    /// Start serving plaintext Arrow Flight on an ephemeral port.
    pub async fn serve_plaintext(self) -> Self {
        self.serve_with(false).await
    }

    async fn serve_with(mut self, tls: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let scheme = if tls { "https" } else { "http" };
        let url = format!("{scheme}://{}", listener.local_addr().unwrap());
        let tls = tls.then(|| self.identity.tls_identity().unwrap());
        let service = AgoraFlightService::new(self.catalog.clone(), self.engines.clone()).unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            serve(listener, service, tls.as_ref(), async {
                let _ = rx.await;
            })
            .await
            .unwrap();
        });
        self.url = Some(url);
        self.server = Some((tx, handle));
        self
    }

    /// Stop serving and wait until the endpoint is closed.
    pub async fn stop(&mut self) {
        if let Some((tx, handle)) = self.server.take() {
            let _ = tx.send(());
            handle.await.unwrap();
        }
    }

    pub fn url(&self) -> &str {
        self.url.as_deref().unwrap()
    }

    pub fn did(&self) -> String {
        self.identity.did().to_string()
    }

    pub async fn sql(&self, sql: &str) -> QueryResult {
        self.owner
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    pub async fn sql_err(&self, sql: &str) -> String {
        match self.owner.sql(sql).await {
            Ok(_) => panic!("{sql}: expected an error"),
            Err(e) => e.to_string(),
        }
    }

    /// `ISSUE UCAN ...` → (id, token).
    pub async fn issue(&self, sql: &str) -> (String, String) {
        let result = self.sql(sql).await;
        (
            strings(&result, 0)[0].clone(),
            strings(&result, 3)[0].clone(),
        )
    }
}

pub fn strings(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let arr = arrow_cast::cast(b.column(column), &arrow_schema::DataType::Utf8).unwrap();
            let arr = arr.as_any().downcast_ref::<StringArray>().unwrap().clone();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

pub fn ints(result: &QueryResult, column: usize) -> Vec<i64> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let arr = arrow_cast::cast(b.column(column), &arrow_schema::DataType::Int64).unwrap();
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
