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

//! Identity and authorization primitives of Oikome (architecture v3 §9).
//!
//! * [`Did`]: `did:key` identifiers for Ed25519 public keys.
//! * [`NodeIdentity`]: a node's signing key, loaded from the environment or
//!   a private file, never printed.
//! * [`ucan`]: UCAN-style JWT capability tokens — issuing, delegation
//!   chains, attenuation and revocation-aware verification.
//! * [`auth`]: the signed, timestamped, content-bound header a node attaches
//!   to every request it sends to a peer.

pub mod auth;
pub mod did;
pub mod error;
pub mod keys;
pub mod ucan;

pub use auth::{request_digest, AuthRequest};
pub use did::Did;
pub use error::IdentityError;
#[cfg(feature = "fs")]
pub use keys::load_node_identity;
pub use keys::{NodeIdentity, TlsIdentity, ENV_NODE_KEY};
pub use ucan::{Capability, Ucan, UcanBuilder, UcanError, Verified, VerifyOptions};

/// Seconds since the Unix epoch.
pub fn unix_now() -> u64 {
    unix_now_ms() / 1000
}

/// Milliseconds since the Unix epoch.
pub fn unix_now_ms() -> u64 {
    // `std::time::SystemTime::now` panics in the browser; web-time reads
    // the JavaScript clock there and is `std` elsewhere.
    #[cfg(not(target_arch = "wasm32"))]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(target_arch = "wasm32")]
    use web_time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
