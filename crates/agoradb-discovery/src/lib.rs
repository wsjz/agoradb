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

//! Peer discovery on the local network (architecture v3 §9).
//!
//! libp2p is used only to find nodes; data stays on Arrow Flight. The
//! node's libp2p key is its DID key, so a libp2p `PeerId` and a DID name
//! the same public key. Once connected (Noise authenticates the key), a
//! node asks the other for its Flight endpoint over `/agora/endpoint/1` and
//! records `DID → scheme://ip:port` in the catalog's Peers table, where
//! every process of the node can resolve remote Spaces by owner DID.
//!
//! mDNS finds nodes on the local network; [`Discovery::dial`] connects to a
//! known address (another network, or tests). Crossing NATs is out of
//! scope here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use agoradb_catalog::AgoraCatalog;
use agoradb_identity::{Did, NodeIdentity};
use futures::StreamExt;
use libp2p::core::ConnectedPoint;
use libp2p::identity::Keypair;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, json, ProtocolSupport};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{identify, mdns, noise, tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use zeroize::Zeroizing;

pub use libp2p::Multiaddr;

/// identify protocol version announced by AgoraDB nodes.
pub const IDENTIFY_PROTOCOL: &str = "/agora/id/1";
/// Request-response protocol asking a node for its Flight endpoint.
pub const ENDPOINT_PROTOCOL: &str = "/agora/endpoint/1";
/// Idle connections are kept this long, so identify and the endpoint
/// exchange can finish.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long [`Discovery::start`] waits for the listener to come up.
const LISTEN_TIMEOUT: Duration = Duration::from_secs(10);

/// `/agora/endpoint/1` request: "where do you serve Flight?"
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointRequest {}

/// `/agora/endpoint/1` response. The host is the address the connection
/// came from, so only the scheme and port are announced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointResponse {
    /// `https`, or `http` for a plaintext node.
    pub scheme: String,
    pub port: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("libp2p: {0}")]
    Libp2p(String),
    #[error("discovery has stopped")]
    Stopped,
}

fn libp2p_err(e: impl std::fmt::Display) -> DiscoveryError {
    DiscoveryError::Libp2p(e.to_string())
}

/// What a node runs discovery with.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// libp2p listen address; port 0 picks a free one.
    pub listen: Multiaddr,
    /// The Flight endpoint announced to peers.
    pub flight: EndpointResponse,
    /// Find peers on the local network with mDNS.
    pub mdns: bool,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    mdns: Toggle<mdns::tokio::Behaviour>,
    identify: identify::Behaviour,
    endpoint: json::Behaviour<EndpointRequest, EndpointResponse>,
}

enum Command {
    Dial(Multiaddr, oneshot::Sender<Result<(), DiscoveryError>>),
}

/// A running discovery task. Dropping it stops the task.
pub struct Discovery {
    peer_id: PeerId,
    listen_addrs: Vec<Multiaddr>,
    commands: mpsc::Sender<Command>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery")
            .field("peer_id", &self.peer_id)
            .field("listen_addrs", &self.listen_addrs)
            .finish_non_exhaustive()
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The libp2p identity of a node: its DID key.
pub fn libp2p_keypair(identity: &NodeIdentity) -> Result<Keypair, DiscoveryError> {
    identity.with_seed(|seed| {
        // Zeroed by libp2p on success and by `Zeroizing` in any case.
        let mut copy = Zeroizing::new(*seed);
        Keypair::ed25519_from_bytes(&mut *copy).map_err(libp2p_err)
    })
}

/// The DID of a libp2p public key, if it is an Ed25519 key.
fn did_of(key: libp2p::identity::PublicKey) -> Option<Did> {
    let key = key.try_into_ed25519().ok()?;
    Did::from_public_key_bytes(&key.to_bytes()).ok()
}

fn ip_of(addr: &Multiaddr) -> Option<IpAddr> {
    addr.iter().find_map(|p| match p {
        Protocol::Ip4(ip) => Some(IpAddr::V4(ip)),
        Protocol::Ip6(ip) => Some(IpAddr::V6(ip)),
        _ => None,
    })
}

/// `scheme://ip:port`, or `None` for an unknown scheme.
fn endpoint_url(scheme: &str, ip: IpAddr, port: u16) -> Option<String> {
    if !matches!(scheme, "https" | "http") {
        return None;
    }
    Some(match ip {
        IpAddr::V4(ip) => format!("{scheme}://{ip}:{port}"),
        IpAddr::V6(ip) => format!("{scheme}://[{ip}]:{port}"),
    })
}

impl Discovery {
    /// Start discovery for the node `identity`, recording peers in
    /// `catalog`. Returns once the libp2p listener is up.
    pub async fn start(
        identity: &NodeIdentity,
        catalog: Arc<AgoraCatalog>,
        config: DiscoveryConfig,
    ) -> Result<Self, DiscoveryError> {
        let use_mdns = config.mdns;
        let mut swarm = SwarmBuilder::with_existing_identity(libp2p_keypair(identity)?)
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .map_err(libp2p_err)?
            .with_behaviour(|key| {
                let peer_id = key.public().to_peer_id();
                let mdns = if use_mdns {
                    Some(mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        peer_id,
                    )?)
                } else {
                    None
                };
                Ok(Behaviour {
                    mdns: Toggle::from(mdns),
                    identify: identify::Behaviour::new(identify::Config::new(
                        IDENTIFY_PROTOCOL.to_string(),
                        key.public(),
                    )),
                    endpoint: json::Behaviour::new(
                        [(
                            StreamProtocol::new(ENDPOINT_PROTOCOL),
                            ProtocolSupport::Full,
                        )],
                        request_response::Config::default(),
                    ),
                })
            })
            .map_err(libp2p_err)?
            .with_swarm_config(|c| c.with_idle_connection_timeout(IDLE_TIMEOUT))
            .build();
        swarm.listen_on(config.listen.clone()).map_err(libp2p_err)?;

        let listen_addrs = tokio::time::timeout(LISTEN_TIMEOUT, async {
            loop {
                if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                    return address;
                }
            }
        })
        .await
        .map_err(|_| DiscoveryError::Libp2p("listener did not start".to_string()))?;

        let peer_id = *swarm.local_peer_id();
        let (commands, receiver) = mpsc::channel(16);
        let state = State {
            catalog,
            flight: config.flight,
            dids: HashMap::new(),
            ips: HashMap::new(),
        };
        let task = tokio::spawn(run(swarm, receiver, state));
        tracing::info!(%peer_id, addr = %listen_addrs, mdns = use_mdns, "discovery started");
        Ok(Self {
            peer_id,
            listen_addrs: vec![listen_addrs],
            commands,
            task,
        })
    }

    /// This node's libp2p peer id (its DID key).
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Where this node accepts libp2p connections.
    pub fn listen_addrs(&self) -> &[Multiaddr] {
        &self.listen_addrs
    }

    /// Connect to a node at a known libp2p address; it is recorded like
    /// one found by mDNS.
    pub async fn dial(&self, addr: Multiaddr) -> Result<(), DiscoveryError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Dial(addr, reply))
            .await
            .map_err(|_| DiscoveryError::Stopped)?;
        answer.await.map_err(|_| DiscoveryError::Stopped)?
    }
}

/// What the task knows about connected peers.
struct State {
    catalog: Arc<AgoraCatalog>,
    flight: EndpointResponse,
    /// DID of each peer, from its verified identify key.
    dids: HashMap<PeerId, Did>,
    /// Address each peer's connection came from.
    ips: HashMap<PeerId, IpAddr>,
}

impl State {
    fn handle(&mut self, swarm: &mut Swarm<Behaviour>, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(found))) => {
                for (peer, addr) in found {
                    if peer != *swarm.local_peer_id() && !swarm.is_connected(&peer) {
                        if let Err(e) = swarm.dial(addr.clone()) {
                            tracing::debug!(%peer, %addr, error = %e, "dialing a discovered peer failed");
                        }
                    }
                }
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                let addr = match &endpoint {
                    ConnectedPoint::Dialer { address, .. } => address,
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
                };
                if let Some(ip) = ip_of(addr) {
                    self.ips.insert(peer_id, ip);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                if info.public_key.to_peer_id() != peer_id {
                    tracing::warn!(%peer_id, "identify key does not match the peer id");
                    return;
                }
                let Some(did) = did_of(info.public_key) else {
                    return;
                };
                self.dids.insert(peer_id, did);
                swarm
                    .behaviour_mut()
                    .endpoint
                    .send_request(&peer_id, EndpointRequest::default());
            }
            SwarmEvent::Behaviour(BehaviourEvent::Endpoint(request_response::Event::Message {
                peer,
                message,
                ..
            })) => match message {
                request_response::Message::Request { channel, .. } => {
                    let _ = swarm
                        .behaviour_mut()
                        .endpoint
                        .send_response(channel, self.flight.clone());
                }
                request_response::Message::Response { response, .. } => {
                    self.record(peer, &response);
                }
            },
            _ => {}
        }
    }

    fn record(&self, peer: PeerId, response: &EndpointResponse) {
        let (Some(did), Some(ip)) = (self.dids.get(&peer), self.ips.get(&peer)) else {
            return;
        };
        let Some(url) = endpoint_url(&response.scheme, *ip, response.port) else {
            tracing::debug!(%peer, scheme = %response.scheme, "peer announced an unknown scheme");
            return;
        };
        match self.catalog.record_peer(did.as_str(), &url) {
            Ok(()) => tracing::info!(did = %did, endpoint = %url, "peer discovered"),
            Err(e) => {
                tracing::warn!(did = %did, endpoint = %url, error = %e, "could not record a peer")
            }
        }
    }
}

async fn run(mut swarm: Swarm<Behaviour>, mut commands: mpsc::Receiver<Command>, mut state: State) {
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Dial(addr, reply)) => {
                    let _ = reply.send(swarm.dial(addr).map_err(libp2p_err));
                }
                None => break,
            },
            event = swarm.select_next_some() => state.handle(&mut swarm, event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_libp2p_identity_is_the_did_key() {
        let identity = NodeIdentity::from_seed(&[9; 32]);
        let keypair = libp2p_keypair(&identity).unwrap();
        assert_eq!(did_of(keypair.public()).unwrap(), *identity.did());
    }

    #[test]
    fn endpoints_are_built_from_the_connection_address() {
        let v4: IpAddr = "10.0.0.5".parse().unwrap();
        let v6: IpAddr = "fe80::1".parse().unwrap();
        assert_eq!(
            endpoint_url("https", v4, 50051).as_deref(),
            Some("https://10.0.0.5:50051")
        );
        assert_eq!(
            endpoint_url("http", v6, 7).as_deref(),
            Some("http://[fe80::1]:7")
        );
        assert_eq!(endpoint_url("ftp", v4, 1), None);
        let addr: Multiaddr = "/ip4/192.168.1.2/tcp/4001".parse().unwrap();
        assert_eq!(ip_of(&addr), Some("192.168.1.2".parse().unwrap()));
    }
}
