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

//! The AgoraDB node server (architecture v3 §9): the Arrow Flight endpoint
//! through which peers query this node's Spaces with UCANs, and the
//! `agoradb` binary.

pub mod replay;
pub mod service;

use std::future::Future;

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;

pub use service::AgoraFlightService;

/// Largest header block accepted: the UCAN, with its embedded proofs,
/// travels in a header (gRPC servers default to 16 KiB).
pub const MAX_HEADER_LIST_BYTES: u32 = 256 * 1024;

/// Serve `service` on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    service: AgoraFlightService,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .http2_max_header_list_size(MAX_HEADER_LIST_BYTES)
        .add_service(FlightServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown)
        .await
}
