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

//! The WebRTC endpoint of a Full Node (architecture v3 §10): how browsers,
//! which cannot pin a certificate to a DID, reach it with the same strength
//! of server authentication as native nodes.
//!
//! * The node keeps a P-256 DTLS certificate ([`DtlsIdentity`], stored with
//!   the node key) and publishes its fingerprint in a DID-signed
//!   [`AccessRecord`]. The browser verifies the record and pins the
//!   fingerprint, so only this node can complete the DTLS handshake.
//! * "WebRTC-direct": the browser builds the SDP itself from the record, so
//!   there is no signaling server. Its ICE username fragment, which doubles
//!   as the password, arrives in the first STUN request and names the
//!   session; the node runs ICE-lite and accepts any browser certificate —
//!   the browser is authenticated by its signed requests, as over Flight.
//! * One negotiated data channel (id 0) carries framed messages
//!   (`oikome_protocol::webrtc`): [`Envelope`] calls in, replies out. The
//!   [`Handler`] answers them; the server answers calls concurrently.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use oikome_identity::{unix_now, NodeIdentity};
use oikome_protocol::webrtc::{
    encode_frames, encode_reply, AccessRecord, Envelope, Reassembler, ReplyHeader, WebRtcError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use str0m::channel::{ChannelConfig, ChannelId, Reliability};
use str0m::crypto::Fingerprint;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceConnectionState, IceCreds, Input, Output, Rtc, RtcConfig};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// Sessions served at once; further browsers are ignored until one ends.
const MAX_SESSIONS: usize = 64;
/// A session without packets for this long is dropped.
const IDLE: Duration = Duration::from_secs(60);
/// Stop writing to a channel above this many buffered bytes.
const HIGH_WATER: usize = 1 << 20;
/// Resume writing below this many.
const LOW_WATER: usize = 256 << 10;

/// Answers the calls arriving over WebRTC with encoded replies
/// ([`encode_reply`]).
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn call(&self, envelope: Envelope) -> Vec<u8>;
}

/// The node's DTLS certificate (P-256), kept in a private file so its
/// fingerprint, and the records naming it, stay valid across restarts.
pub struct DtlsIdentity {
    cert: str0m::config::DtlsCert,
    fingerprint: String,
}

impl std::fmt::Debug for DtlsIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DtlsIdentity")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
struct StoredCert {
    certificate: String,
    private_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("DTLS certificate: {0}")]
    Certificate(String),
    #[error(transparent)]
    Record(#[from] WebRtcError),
}

impl DtlsIdentity {
    /// Load the certificate at `path`, creating it (readable by the owner
    /// only) on first use.
    pub fn load_or_create(path: &Path) -> Result<Self, ServerError> {
        let bad = |why: String| ServerError::Certificate(format!("{}: {why}", path.display()));
        let stored: StoredCert = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| bad(e.to_string()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let cert = str0m::crypto::from_feature_flags()
                    .dtls_provider
                    .generate_certificate()
                    .ok_or_else(|| bad("could not generate".to_string()))?;
                let stored = StoredCert {
                    certificate: URL_SAFE_NO_PAD.encode(&cert.certificate),
                    private_key: URL_SAFE_NO_PAD.encode(&cert.private_key),
                };
                write_private(
                    path,
                    &serde_json::to_vec(&stored).map_err(|e| bad(e.to_string()))?,
                )?;
                stored
            }
            Err(e) => return Err(e.into()),
        };
        let decode = |s: &str| URL_SAFE_NO_PAD.decode(s).map_err(|e| bad(e.to_string()));
        let cert = str0m::config::DtlsCert {
            certificate: decode(&stored.certificate)?,
            private_key: decode(&stored.private_key)?,
        };
        Ok(Self::new(cert))
    }

    fn new(cert: str0m::config::DtlsCert) -> Self {
        let digest = Sha256::digest(&cert.certificate);
        let hex: Vec<String> = digest.iter().map(|b| format!("{b:02X}")).collect();
        Self {
            fingerprint: format!("sha-256 {}", hex.join(":")),
            cert,
        }
    }

    /// `sha-256 AA:BB:…`, as in SDP and in access records.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// A signed access record for `identity` at `addr`, valid for `ttl`.
    pub fn access_record(
        &self,
        identity: &NodeIdentity,
        addr: SocketAddr,
        ttl: Duration,
    ) -> Result<String, ServerError> {
        Ok(AccessRecord {
            did: identity.did().to_string(),
            addr: addr.to_string(),
            fingerprint: self.fingerprint.clone(),
            expires_at: unix_now() + ttl.as_secs(),
        }
        .sign(identity)?)
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// USERNAME of a STUN binding request (`local:remote` from the browser's
/// side; both are the browser's fragment in WebRTC-direct).
fn stun_username(buf: &[u8]) -> Option<&str> {
    if buf.len() < 20 || u16::from_be_bytes([buf[0], buf[1]]) != 0x0001 {
        return None;
    }
    let end = (20 + u16::from_be_bytes([buf[2], buf[3]]) as usize).min(buf.len());
    let mut i = 20;
    while i + 4 <= end {
        let kind = u16::from_be_bytes([buf[i], buf[i + 1]]);
        let len = u16::from_be_bytes([buf[i + 2], buf[i + 3]]) as usize;
        let value = buf.get(i + 4..i + 4 + len)?;
        if kind == 0x0006 {
            return std::str::from_utf8(value).ok();
        }
        i += 4 + ((len + 3) & !3);
    }
    None
}

/// An ICE fragment we accept as a session name: 22–256 ICE characters
/// (it is also the password, which ICE wants at least 22 characters long).
fn valid_ufrag(ufrag: &str) -> bool {
    (22..=256).contains(&ufrag.len())
        && ufrag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

struct Session {
    rtc: Rtc,
    channel: Option<ChannelId>,
    reassembler: Reassembler,
    pending: VecDeque<Vec<u8>>,
    last_seen: Instant,
    next_timeout: Instant,
}

impl Session {
    fn new(base: &RtcConfig, ufrag: &str, local: SocketAddr, now: Instant) -> Option<Self> {
        let creds = IceCreds {
            ufrag: ufrag.to_string(),
            pass: ufrag.to_string(),
        };
        let mut rtc = base
            .clone()
            .set_local_ice_credentials(creds.clone())
            .build(now);
        rtc.add_local_candidate(Candidate::host(local, "udp").ok()?);
        let mut api = rtc.direct_api();
        api.set_remote_ice_credentials(creds);
        api.set_ice_controlling(false);
        // Verification is off: the browser authenticates each request with
        // its DID. DTLS still needs a value here.
        api.set_remote_fingerprint(Fingerprint {
            hash_func: "sha-256".to_string(),
            bytes: vec![0; 32],
        });
        api.start_dtls(false).ok()?;
        api.start_sctp(false);
        let channel = api.create_data_channel(ChannelConfig {
            label: String::new(),
            ordered: true,
            reliability: Reliability::Reliable,
            negotiated: Some(0),
            protocol: String::new(),
        });
        Some(Self {
            rtc,
            channel: Some(channel),
            reassembler: Reassembler::default(),
            pending: VecDeque::new(),
            last_seen: now,
            next_timeout: now,
        })
    }

    /// Write queued frames while the channel accepts them.
    fn flush(&mut self) {
        let Some(id) = self.channel else { return };
        let Some(mut channel) = self.rtc.channel(id) else {
            return;
        };
        while let Some(frame) = self.pending.front() {
            if channel.buffered_amount() > HIGH_WATER {
                break;
            }
            match channel.write(true, frame) {
                Ok(true) => {
                    self.pending.pop_front();
                }
                Ok(false) => break,
                Err(e) => {
                    tracing::debug!(error = %e, "writing to a data channel failed");
                    self.pending.clear();
                    break;
                }
            }
        }
    }
}

/// A reply ready to be written: session, request id, encoded reply.
type Ready = (String, u32, Vec<u8>);

fn error_reply(status: &str, message: impl Into<String>) -> Vec<u8> {
    encode_reply(
        &ReplyHeader {
            status: status.to_string(),
            message: message.into(),
        },
        &[],
    )
}

/// Serve WebRTC data channels on `socket` until `shutdown` resolves. The
/// socket's local address must be the one published in access records.
pub async fn serve(
    socket: UdpSocket,
    dtls: &DtlsIdentity,
    handler: Arc<dyn Handler>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ServerError> {
    let local = socket.local_addr()?;
    let base = RtcConfig::new()
        .set_ice_lite(true)
        .set_fingerprint_verification(false)
        .set_dtls_cert(dtls.cert.clone());
    let mut sessions: HashMap<String, Session> = HashMap::new();
    let mut by_addr: HashMap<SocketAddr, String> = HashMap::new();
    let (ready_tx, mut ready_rx) = mpsc::channel::<Ready>(256);
    let mut buf = vec![0u8; 2048];
    tokio::pin!(shutdown);

    loop {
        let wake = sessions
            .values()
            .map(|s| s.next_timeout)
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
        let mut touched: Vec<String> = Vec::new();
        tokio::select! {
            _ = &mut shutdown => break,
            received = socket.recv_from(&mut buf) => {
                let (n, source) = received?;
                let now = Instant::now();
                let data = &buf[..n];
                let name = match by_addr.get(&source) {
                    Some(name) if sessions.contains_key(name) => name.clone(),
                    _ => {
                        // A new browser: its first packet is a STUN request
                        // naming the session.
                        let Some(ufrag) = stun_username(data)
                            .and_then(|u| u.split(':').next())
                            .filter(|u| valid_ufrag(u))
                        else {
                            continue;
                        };
                        if !sessions.contains_key(ufrag) {
                            if sessions.len() >= MAX_SESSIONS {
                                tracing::warn!("too many WebRTC sessions; ignoring a new browser");
                                continue;
                            }
                            let Some(session) = Session::new(&base, ufrag, local, now) else {
                                continue;
                            };
                            tracing::debug!(%source, "new WebRTC session");
                            sessions.insert(ufrag.to_string(), session);
                        }
                        by_addr.insert(source, ufrag.to_string());
                        ufrag.to_string()
                    }
                };
                let Ok(contents) = data.try_into() else { continue };
                let session = sessions.get_mut(&name).expect("session exists");
                session.last_seen = now;
                let input = Input::Receive(now, Receive { proto: Protocol::Udp, source, destination: local, contents });
                if let Err(e) = session.rtc.handle_input(input) {
                    tracing::debug!(error = %e, "dropping a WebRTC session");
                    sessions.remove(&name);
                    continue;
                }
                touched.push(name);
            }
            Some((name, id, reply)) = ready_rx.recv() => {
                if let Some(session) = sessions.get_mut(&name) {
                    session.pending.extend(encode_frames(id, &reply));
                    session.flush();
                    touched.push(name);
                }
            }
            _ = tokio::time::sleep_until(wake.into()) => {
                let now = Instant::now();
                sessions.retain(|_, s| now.duration_since(s.last_seen) < IDLE && s.rtc.is_alive());
                for (name, session) in sessions.iter_mut() {
                    if session.next_timeout <= now {
                        let _ = session.rtc.handle_input(Input::Timeout(now));
                        touched.push(name.clone());
                    }
                }
            }
        }
        for name in touched {
            let Some(session) = sessions.get_mut(&name) else {
                continue;
            };
            if !drive(&name, session, &socket, &handler, &ready_tx).await {
                sessions.remove(&name);
            }
        }
        by_addr.retain(|_, name| sessions.contains_key(name));
    }
    Ok(())
}

/// Process a session's output; `false` once it has ended.
async fn drive(
    name: &str,
    session: &mut Session,
    socket: &UdpSocket,
    handler: &Arc<dyn Handler>,
    ready: &mpsc::Sender<Ready>,
) -> bool {
    loop {
        let output = match session.rtc.poll_output() {
            Ok(output) => output,
            Err(e) => {
                tracing::debug!(error = %e, "WebRTC session failed");
                return false;
            }
        };
        match output {
            Output::Transmit(t) => {
                if let Err(e) = socket.send_to(&t.contents, t.destination).await {
                    tracing::debug!(error = %e, "sending a WebRTC datagram failed");
                }
            }
            Output::Timeout(t) => {
                session.next_timeout = t;
                return true;
            }
            Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected)) => {
                return false;
            }
            Output::Event(Event::ChannelOpen(id, _)) => {
                session.channel = Some(id);
                if let Some(mut channel) = session.rtc.channel(id) {
                    channel.set_buffered_amount_low_threshold(LOW_WATER);
                }
                session.flush();
            }
            Output::Event(Event::ChannelBufferedAmountLow(_)) => session.flush(),
            Output::Event(Event::ChannelClose(_)) => return false,
            Output::Event(Event::ChannelData(data)) => match session.reassembler.push(&data.data) {
                Ok(None) => {}
                Ok(Some((id, message))) => {
                    let (handler, ready, name) = (handler.clone(), ready.clone(), name.to_string());
                    tokio::spawn(async move {
                        let reply = match serde_json::from_slice::<Envelope>(&message) {
                            Ok(envelope) => handler.call(envelope).await,
                            Err(e) => error_reply("invalid_argument", format!("envelope: {e}")),
                        };
                        let _ = ready.send((name, id, reply)).await;
                    });
                }
                Err(e) => {
                    tracing::debug!(error = %e, "bad frame; ending the session");
                    return false;
                }
            },
            Output::Event(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stun_usernames_name_sessions() {
        // A binding request with one USERNAME attribute ("abc:def", padded).
        let mut packet = vec![0x00, 0x01, 0x00, 0x0c];
        packet.extend_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
        packet.extend_from_slice(&[0; 12]);
        packet.extend_from_slice(&[0x00, 0x06, 0x00, 0x07]);
        packet.extend_from_slice(b"abc:def\0");
        assert_eq!(stun_username(&packet), Some("abc:def"));
        assert_eq!(stun_username(&packet[..10]), None);
        packet[1] = 0x02; // not a binding request
        assert_eq!(stun_username(&packet), None);

        assert!(valid_ufrag("oikome+v1/0123456789abcdef"));
        assert!(!valid_ufrag("short"));
        assert!(!valid_ufrag("oikome+v1/0123456789abcdef:x"));
    }

    #[test]
    fn certificates_persist_with_their_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity").join("webrtc_dtls.json");
        let first = DtlsIdentity::load_or_create(&path).unwrap();
        let again = DtlsIdentity::load_or_create(&path).unwrap();
        assert_eq!(first.fingerprint(), again.fingerprint());
        assert!(oikome_protocol::webrtc::is_fingerprint(first.fingerprint()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        let identity = NodeIdentity::from_seed(&[5; 32]);
        let addr: SocketAddr = "127.0.0.1:4433".parse().unwrap();
        let text = first
            .access_record(&identity, addr, Duration::from_secs(60))
            .unwrap();
        let record = AccessRecord::verify(&text, unix_now()).unwrap();
        assert_eq!(record.fingerprint, first.fingerprint());
        assert_eq!(record.addr, "127.0.0.1:4433");
    }
}
