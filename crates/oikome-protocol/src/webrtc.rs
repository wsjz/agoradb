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

//! The browser transport (architecture v3 §10): a WebRTC data channel to a
//! Full Node, shared by the node (`oikome-webrtc`) and the browser
//! (`oikome-wasm`, `packages/node/src/transport.js`).
//!
//! * [`AccessRecord`]: where a node accepts WebRTC and the fingerprint of
//!   its DTLS certificate, signed with its DID. The browser pins that
//!   fingerprint, so the signature makes the channel as authenticated as
//!   the node's DID — browsers cannot pin a certificate to a DID themselves.
//! * Frames: messages are split into frames of at most [`MAX_FRAME_BYTES`]
//!   (the SCTP message size both sides announce): `[u32 BE request id]
//!   [u8 flags][bytes]`, flag [`FLAG_LAST`] on the last frame of a message.
//! * [`Envelope`] (request) and [`ReplyHeader`] (response): the same call,
//!   payload, UCAN and signed header as an Arrow Flight call, so the node
//!   applies the same authentication and authorization.

use std::collections::HashMap;
use std::net::SocketAddr;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use oikome_identity::{Did, NodeIdentity};
use serde::{Deserialize, Serialize};

/// Prefix of an access record in text form, as used in `PEER = '...'`.
pub const RECORD_PREFIX: &str = "webrtc:";
/// Domain separation for record signatures.
const RECORD_DOMAIN: &[u8] = b"oikome-webrtc-record-v1\n";
/// Largest data channel message either side sends.
pub const MAX_FRAME_BYTES: usize = 16 * 1024;
/// Bytes of frame header before the payload.
pub const FRAME_HEADER_BYTES: usize = 5;
/// Frame flag: the last frame of a message.
pub const FLAG_LAST: u8 = 1;
/// Largest reassembled message accepted.
pub const MAX_MESSAGE_BYTES: usize = 256 << 20;

/// Why an access record or a frame was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WebRtcError {
    #[error("invalid access record: {0}")]
    Record(String),
    #[error("access record has expired")]
    Expired,
    #[error("access record signature does not match its DID")]
    Signature,
    #[error("invalid frame: {0}")]
    Frame(String),
}

/// Where a node accepts WebRTC data channels, signed with its DID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRecord {
    /// The node's DID.
    pub did: String,
    /// UDP endpoint, `ip:port`.
    pub addr: String,
    /// SHA-256 fingerprint of the DTLS certificate, `sha-256 AA:BB:…`.
    pub fingerprint: String,
    /// Expiry, seconds since the Unix epoch.
    pub expires_at: u64,
}

fn record_err(why: impl Into<String>) -> WebRtcError {
    WebRtcError::Record(why.into())
}

/// Whether `fingerprint` is `sha-256` followed by 32 colon-separated hex bytes.
pub fn is_fingerprint(fingerprint: &str) -> bool {
    let Some(hex) = fingerprint.strip_prefix("sha-256 ") else {
        return false;
    };
    let bytes: Vec<&str> = hex.split(':').collect();
    bytes.len() == 32
        && bytes
            .iter()
            .all(|b| b.len() == 2 && b.bytes().all(|c| c.is_ascii_hexdigit()))
}

impl AccessRecord {
    /// The record in text form, signed by `identity` (whose DID it must name).
    pub fn sign(&self, identity: &NodeIdentity) -> Result<String, WebRtcError> {
        if self.did != identity.did().as_str() {
            return Err(record_err("the record names another DID"));
        }
        self.check_fields()?;
        let json = serde_json::to_vec(self).map_err(|e| record_err(e.to_string()))?;
        let signature = identity.sign(&[RECORD_DOMAIN, &json].concat());
        Ok(format!(
            "{RECORD_PREFIX}{}.{}",
            URL_SAFE_NO_PAD.encode(&json),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    /// Parse a record in text form and check its signature against the DID
    /// it names, and that it has not expired at `now` (Unix seconds).
    pub fn verify(text: &str, now: u64) -> Result<Self, WebRtcError> {
        let body = text
            .trim()
            .strip_prefix(RECORD_PREFIX)
            .ok_or_else(|| record_err(format!("expected {RECORD_PREFIX}<record>")))?;
        let (json, signature) = body
            .split_once('.')
            .ok_or_else(|| record_err("expected <record>.<signature>"))?;
        let json = URL_SAFE_NO_PAD
            .decode(json)
            .map_err(|e| record_err(e.to_string()))?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|e| record_err(e.to_string()))?;
        let record: AccessRecord =
            serde_json::from_slice(&json).map_err(|e| record_err(e.to_string()))?;
        let did = Did::parse(&record.did).map_err(|e| record_err(e.to_string()))?;
        did.verify(&[RECORD_DOMAIN, &json].concat(), &signature)
            .map_err(|_| WebRtcError::Signature)?;
        record.check_fields()?;
        if record.expires_at <= now {
            return Err(WebRtcError::Expired);
        }
        Ok(record)
    }

    fn check_fields(&self) -> Result<(), WebRtcError> {
        let addr: SocketAddr = self
            .addr
            .parse()
            .map_err(|_| record_err(format!("address '{}' is not ip:port", self.addr)))?;
        if addr.ip().is_unspecified() || addr.port() == 0 {
            return Err(record_err(format!("address '{addr}' is not reachable")));
        }
        if !is_fingerprint(&self.fingerprint) {
            return Err(record_err("fingerprint is not sha-256 with 32 hex bytes"));
        }
        Ok(())
    }
}

/// Split `message` into frames for request `id`.
pub fn encode_frames(id: u32, message: &[u8]) -> Vec<Vec<u8>> {
    let chunk = MAX_FRAME_BYTES - FRAME_HEADER_BYTES;
    let mut parts: Vec<&[u8]> = message.chunks(chunk).collect();
    if parts.is_empty() {
        parts.push(&[]);
    }
    let last = parts.len() - 1;
    parts
        .into_iter()
        .enumerate()
        .map(|(i, part)| {
            let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + part.len());
            frame.extend_from_slice(&id.to_be_bytes());
            frame.push(if i == last { FLAG_LAST } else { 0 });
            frame.extend_from_slice(part);
            frame
        })
        .collect()
}

/// Reassembles messages from frames of interleaved requests.
#[derive(Debug, Default)]
pub struct Reassembler {
    partial: HashMap<u32, Vec<u8>>,
}

impl Reassembler {
    /// Add one frame; returns `(id, message)` once a message is complete.
    pub fn push(&mut self, frame: &[u8]) -> Result<Option<(u32, Vec<u8>)>, WebRtcError> {
        if frame.len() < FRAME_HEADER_BYTES || frame.len() > MAX_FRAME_BYTES {
            return Err(WebRtcError::Frame(format!("{} bytes", frame.len())));
        }
        let id = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        let flags = frame[4];
        if flags & !FLAG_LAST != 0 {
            return Err(WebRtcError::Frame(format!("unknown flags {flags:#x}")));
        }
        let buf = self.partial.entry(id).or_default();
        if buf.len() + frame.len() - FRAME_HEADER_BYTES > MAX_MESSAGE_BYTES {
            self.partial.remove(&id);
            return Err(WebRtcError::Frame("message too large".to_string()));
        }
        buf.extend_from_slice(&frame[FRAME_HEADER_BYTES..]);
        if flags & FLAG_LAST == 0 {
            return Ok(None);
        }
        Ok(self.partial.remove(&id).map(|message| (id, message)))
    }
}

/// A call to the node: the same fields as an Arrow Flight call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// `do_get`, `get_schema` or `do_action:oikome.list_tables`.
    pub method: String,
    /// The call's payload, base64url: the ticket, the
    /// [`schema_payload`](crate::schema_payload) or the action body.
    pub payload: String,
    /// The UCAN (`x-oikome-ucan`).
    pub ucan: String,
    /// The signed request (`x-oikome-auth`), bound to method, payload and UCAN.
    pub auth: String,
}

impl Envelope {
    pub fn new(method: &str, payload: &[u8], ucan: &str, auth: &str) -> Self {
        Self {
            method: method.to_string(),
            payload: URL_SAFE_NO_PAD.encode(payload),
            ucan: ucan.to_string(),
            auth: auth.to_string(),
        }
    }

    pub fn payload(&self) -> Result<Vec<u8>, WebRtcError> {
        URL_SAFE_NO_PAD
            .decode(&self.payload)
            .map_err(|e| WebRtcError::Frame(format!("payload: {e}")))
    }
}

/// The first part of a reply; the body follows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyHeader {
    /// `ok`, or the error kind: `unauthenticated`, `permission_denied`,
    /// `not_found`, `invalid_argument`, `failed_precondition`,
    /// `resource_exhausted`, `unimplemented`, `internal`.
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// A reply message: `[u32 BE header length][header JSON][body]`. The body is
/// an Arrow IPC stream for `do_get` and `get_schema`, JSON for actions.
pub fn encode_reply(header: &ReplyHeader, body: &[u8]) -> Vec<u8> {
    let json = serde_json::to_vec(header).unwrap_or_default();
    let mut out = Vec::with_capacity(4 + json.len() + body.len());
    out.extend_from_slice(&(json.len() as u32).to_be_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(body);
    out
}

/// Split a reply message into header and body.
pub fn decode_reply(message: &[u8]) -> Result<(ReplyHeader, &[u8]), WebRtcError> {
    let len = message
        .get(..4)
        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize)
        .ok_or_else(|| WebRtcError::Frame("short reply".to_string()))?;
    let json = message
        .get(4..4 + len)
        .ok_or_else(|| WebRtcError::Frame("short reply header".to_string()))?;
    let header = serde_json::from_slice(json)
        .map_err(|e| WebRtcError::Frame(format!("reply header: {e}")))?;
    Ok((header, &message[4 + len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(identity: &NodeIdentity) -> AccessRecord {
        AccessRecord {
            did: identity.did().to_string(),
            addr: "192.168.1.20:4433".to_string(),
            fingerprint: format!("sha-256 {}", ["AB"; 32].join(":")),
            expires_at: 2_000,
        }
    }

    #[test]
    fn records_are_bound_to_their_did() {
        let a = NodeIdentity::from_seed(&[1; 32]);
        let text = record(&a).sign(&a).unwrap();
        assert!(text.starts_with(RECORD_PREFIX));
        assert_eq!(AccessRecord::verify(&text, 1_000).unwrap(), record(&a));
        assert_eq!(
            AccessRecord::verify(&text, 2_000),
            Err(WebRtcError::Expired)
        );

        // Any change to the signed bytes is caught.
        let (json, sig) = text[RECORD_PREFIX.len()..].split_once('.').unwrap();
        let mut forged = record(&a);
        forged.addr = "10.0.0.66:4433".to_string();
        let forged_json = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&forged).unwrap());
        assert_eq!(
            AccessRecord::verify(&format!("{RECORD_PREFIX}{forged_json}.{sig}"), 1_000),
            Err(WebRtcError::Signature)
        );
        // A node cannot sign for another DID.
        let b = NodeIdentity::from_seed(&[2; 32]);
        assert!(record(&a).sign(&b).is_err());
        for bad in [
            "",
            "webrtc:",
            "webrtc:abc",
            "other:x.y",
            &format!("webrtc:{json}"),
        ] {
            assert!(AccessRecord::verify(bad, 1_000).is_err(), "{bad}");
        }
        let mut unreachable = record(&a);
        unreachable.addr = "0.0.0.0:4433".to_string();
        assert!(unreachable.sign(&a).is_err());
        let mut bad_fp = record(&a);
        bad_fp.fingerprint = "sha-1 AB".to_string();
        assert!(bad_fp.sign(&a).is_err());
    }

    #[test]
    fn frames_split_and_reassemble_interleaved_messages() {
        let big: Vec<u8> = (0..(3 * MAX_FRAME_BYTES)).map(|i| i as u8).collect();
        let a = encode_frames(7, &big);
        assert_eq!(a.len(), 4);
        assert!(a.iter().all(|f| f.len() <= MAX_FRAME_BYTES));
        let b = encode_frames(8, b"small");
        let empty = encode_frames(9, b"");
        assert_eq!(empty.len(), 1);

        let mut r = Reassembler::default();
        assert_eq!(r.push(&a[0]).unwrap(), None);
        assert_eq!(r.push(&b[0]).unwrap(), Some((8, b"small".to_vec())));
        assert_eq!(r.push(&empty[0]).unwrap(), Some((9, Vec::new())));
        for frame in &a[1..3] {
            assert_eq!(r.push(frame).unwrap(), None);
        }
        assert_eq!(r.push(&a[3]).unwrap(), Some((7, big)));

        assert!(r.push(b"1234").is_err());
        assert!(r.push(&[0, 0, 0, 1, 0x80]).is_err());
    }

    #[test]
    fn replies_carry_a_header_and_a_body() {
        let header = ReplyHeader {
            status: "permission_denied".to_string(),
            message: "permission denied".to_string(),
        };
        let message = encode_reply(&header, b"body");
        let (decoded, body) = decode_reply(&message).unwrap();
        assert_eq!(decoded, header);
        assert_eq!(body, b"body");
        assert!(decode_reply(&[0, 0, 0, 9, b'{']).is_err());

        let envelope = Envelope::new("do_get", b"{\"sql\":\"SELECT 1\"}", "u", "a");
        assert_eq!(envelope.payload().unwrap(), b"{\"sql\":\"SELECT 1\"}");
    }
}
