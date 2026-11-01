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

//! The Oikome node server (architecture v3 §9): the Arrow Flight endpoint
//! through which peers query this node's Spaces with UCANs, and the
//! `oikome` binary.

pub mod replay;
pub mod service;

use std::future::Future;

use arrow_flight::flight_service_server::FlightServiceServer;
use oikome_identity::TlsIdentity;
use oikome_network::install_crypto_provider;
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Identity, Server, ServerTlsConfig};

pub use service::OikomeFlightService;

/// Largest header block accepted: the UCAN, with its embedded proofs,
/// travels in a header (gRPC servers default to 16 KiB).
pub const MAX_HEADER_LIST_BYTES: u32 = 256 * 1024;

/// Serve `service` on `listener` until `shutdown` resolves: over TLS with
/// `tls` (the node's own certificate, see `NodeIdentity::tls_identity`), in
/// plaintext with `None`.
pub async fn serve(
    listener: TcpListener,
    service: OikomeFlightService,
    tls: Option<&TlsIdentity>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        install_crypto_provider();
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(&tls.cert_pem, tls.key_pem.as_bytes())),
        )?;
    }
    builder
        .http2_max_header_list_size(MAX_HEADER_LIST_BYTES)
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
        .await
}
