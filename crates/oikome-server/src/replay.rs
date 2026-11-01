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

//! Replay protection: remember every `(caller, nonce)` until its request
//! timestamp falls out of the accepted skew window.

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

use oikome_identity::auth::MAX_AUTH_SKEW_MS;

/// Requests remembered at most; beyond this new requests are refused
/// rather than letting memory grow without bound.
pub const DEFAULT_CAPACITY: usize = 200_000;

/// Why a request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayError {
    /// The same request was seen before.
    Replayed,
    /// Too many requests within one skew window.
    Saturated,
}

#[derive(Default)]
struct Seen {
    keys: HashSet<(String, String)>,
    /// `(forget after, key)` in arrival order.
    queue: VecDeque<(u64, (String, String))>,
}

/// Remembers recently seen request nonces.
pub struct ReplayGuard {
    seen: Mutex<Seen>,
    capacity: usize,
}

impl Default for ReplayGuard {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl ReplayGuard {
    /// A guard remembering at most `capacity` requests.
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: Mutex::new(Seen::default()),
            capacity,
        }
    }

    /// Record a request; fails if it was already recorded.
    ///
    /// Call only after the request's signature verified, so that only
    /// authenticated peers can occupy the cache.
    pub fn check(
        &self,
        caller: &str,
        nonce: &str,
        timestamp_ms: u64,
        now_ms: u64,
    ) -> Result<(), ReplayError> {
        let mut seen = self.seen.lock().unwrap_or_else(|p| p.into_inner());
        while seen.queue.front().is_some_and(|(until, _)| *until < now_ms) {
            if let Some((_, key)) = seen.queue.pop_front() {
                seen.keys.remove(&key);
            }
        }
        let key = (caller.to_string(), nonce.to_string());
        if seen.keys.contains(&key) {
            return Err(ReplayError::Replayed);
        }
        if seen.keys.len() >= self.capacity {
            return Err(ReplayError::Saturated);
        }
        // Accepted while |timestamp - now| <= skew, so forget it after that.
        let until = timestamp_ms.saturating_add(MAX_AUTH_SKEW_MS);
        seen.keys.insert(key.clone());
        seen.queue.push_back((until, key));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicates_are_refused_until_the_window_passes() {
        let guard = ReplayGuard::new(10);
        let t = 1_000_000;
        guard.check("did:a", "n1", t, t).unwrap();
        assert_eq!(
            guard.check("did:a", "n1", t, t + 5),
            Err(ReplayError::Replayed)
        );
        // Same nonce from another caller is a different request.
        guard.check("did:b", "n1", t, t).unwrap();
        // Once the timestamp is outside the window the entry is forgotten
        // (the signature check would refuse such a request anyway).
        guard
            .check("did:a", "n1", t, t + MAX_AUTH_SKEW_MS + 1)
            .unwrap();
    }

    #[test]
    fn capacity_bounds_memory() {
        let guard = ReplayGuard::new(2);
        let t = 1_000_000;
        guard.check("did:a", "1", t, t).unwrap();
        guard.check("did:a", "2", t, t).unwrap();
        assert_eq!(guard.check("did:a", "3", t, t), Err(ReplayError::Saturated));
        guard
            .check(
                "did:a",
                "3",
                t + MAX_AUTH_SKEW_MS + 1,
                t + MAX_AUTH_SKEW_MS + 1,
            )
            .unwrap();
    }
}
