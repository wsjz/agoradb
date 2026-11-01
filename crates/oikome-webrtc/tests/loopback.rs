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

//! A native str0m client dials the endpoint the way a browser does
//! (WebRTC-direct: its fragment names the session, the server's DTLS
//! fingerprint is pinned) and makes calls over the data channel.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use oikome_protocol::webrtc::{
    decode_reply, encode_frames, encode_reply, Envelope, Reassembler, ReplyHeader,
};
use oikome_webrtc::{serve, DtlsIdentity, Handler};
use str0m::channel::{ChannelConfig, Reliability};
use str0m::crypto::Fingerprint;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, IceCreds, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

/// Replies with the method and payload it was called with.
struct Echo;

#[async_trait]
impl Handler for Echo {
    async fn call(&self, envelope: Envelope) -> Vec<u8> {
        let payload = envelope.payload().unwrap();
        let mut body = format!("{}|", envelope.method).into_bytes();
        body.extend_from_slice(&payload);
        encode_reply(
            &ReplyHeader {
                status: "ok".to_string(),
                message: String::new(),
            },
            &body,
        )
    }
}

async fn start_server(dir: &std::path::Path) -> (SocketAddr, String, oneshot::Sender<()>) {
    let dtls = DtlsIdentity::load_or_create(&dir.join("webrtc_dtls.json")).unwrap();
    let fingerprint = dtls.fingerprint().to_string();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(async move {
        serve(socket, &dtls, Arc::new(Echo), async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (addr, fingerprint, stop)
}

/// Dial `server` pinning `fingerprint`, send `requests`; returns the replies
/// (request id → body), or `None` if no channel opened within the deadline.
async fn client(
    server: SocketAddr,
    fingerprint: &str,
    requests: Vec<(u32, Vec<u8>)>,
) -> Option<Vec<(u32, String, Vec<u8>)>> {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let local = socket.local_addr().unwrap();
    let ufrag = "oikome+v1/0123456789abcdefABCDEF".to_string();
    let creds = IceCreds {
        ufrag: ufrag.clone(),
        pass: ufrag,
    };
    let now = Instant::now();
    let mut rtc = Rtc::builder()
        .set_local_ice_credentials(creds.clone())
        .build(now);
    rtc.add_local_candidate(Candidate::host(local, "udp").unwrap());
    rtc.add_remote_candidate(Candidate::host(server, "udp").unwrap());
    let mut api = rtc.direct_api();
    api.set_remote_ice_credentials(creds);
    api.set_ice_controlling(true);
    api.set_remote_fingerprint(fingerprint.parse::<Fingerprint>().unwrap());
    api.start_dtls(true).unwrap();
    api.start_sctp(true);
    let channel = api.create_data_channel(ChannelConfig {
        label: String::new(),
        ordered: true,
        reliability: Reliability::Reliable,
        negotiated: Some(0),
        protocol: String::new(),
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut buf = vec![0u8; 2048];
    let mut reassembler = Reassembler::default();
    let mut replies = Vec::new();
    let mut sent = false;
    let mut wake;
    while Instant::now() < deadline {
        loop {
            match rtc.poll_output().ok()? {
                Output::Transmit(t) => {
                    socket.send_to(&t.contents, t.destination).await.unwrap();
                }
                Output::Timeout(t) => {
                    wake = t;
                    break;
                }
                Output::Event(Event::ChannelOpen(..)) if !sent => {
                    let mut ch = rtc.channel(channel).unwrap();
                    for (id, message) in &requests {
                        for frame in encode_frames(*id, message) {
                            assert!(ch.write(true, &frame).unwrap());
                        }
                    }
                    sent = true;
                }
                Output::Event(Event::ChannelData(d)) => {
                    if let Some((id, message)) = reassembler.push(&d.data).unwrap() {
                        let (header, body) = decode_reply(&message).unwrap();
                        replies.push((id, header.status, body.to_vec()));
                        if replies.len() == requests.len() {
                            replies.sort_by_key(|r| r.0);
                            return Some(replies);
                        }
                    }
                }
                Output::Event(_) => {}
            }
        }
        let wait = wake
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(50));
        if let Ok(Ok((n, source))) = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await {
            let contents = buf[..n].try_into().unwrap();
            let input = Input::Receive(
                Instant::now(),
                Receive {
                    proto: Protocol::Udp,
                    source,
                    destination: local,
                    contents,
                },
            );
            rtc.handle_input(input).ok()?;
        } else {
            rtc.handle_input(Input::Timeout(Instant::now())).ok()?;
        }
    }
    None
}

fn envelope(method: &str, payload: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&Envelope::new(method, payload, "ucan", "auth")).unwrap()
}

#[tokio::test]
async fn calls_travel_over_a_pinned_data_channel() {
    let dir = tempfile::tempdir().unwrap();
    let (server, fingerprint, _stop) = start_server(dir.path()).await;

    // Two calls in flight at once, one larger than a frame.
    let big = vec![b'x'; 40_000];
    let replies = client(
        server,
        &fingerprint,
        vec![
            (1, envelope("do_get", b"{\"sql\":\"SELECT 1\"}")),
            (2, envelope("get_schema", &big)),
            (3, b"not json".to_vec()),
        ],
    )
    .await
    .expect("the data channel opens");
    assert_eq!(replies[0].0, 1);
    assert_eq!(replies[0].1, "ok");
    assert_eq!(replies[0].2, b"do_get|{\"sql\":\"SELECT 1\"}");
    assert_eq!(replies[1].2.len(), "get_schema|".len() + big.len());
    assert_eq!(replies[2].1, "invalid_argument");
}

#[tokio::test]
async fn a_client_pinning_another_fingerprint_never_connects() {
    let dir = tempfile::tempdir().unwrap();
    let (server, fingerprint, _stop) = start_server(dir.path()).await;
    let other = if fingerprint.ends_with("00") {
        format!("{}11", &fingerprint[..fingerprint.len() - 2])
    } else {
        format!("{}00", &fingerprint[..fingerprint.len() - 2])
    };
    let replies = client(server, &other, vec![(1, envelope("do_get", b"{}"))]).await;
    assert!(replies.is_none());
}
