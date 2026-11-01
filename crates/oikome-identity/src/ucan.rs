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

//! UCAN-style capability tokens.
//!
//! A token is a JWT signed with EdDSA by its issuer's `did:key`, following
//! the UCAN 0.9 payload layout: `iss`, `aud`, `att` (capabilities as
//! `{with, can}`), `exp`, optional `nbf`, a nonce `nnc`, and `prf` (parent
//! tokens embedded as strings). A token delegated from another one must not
//! grant more than its parent, nor outlive it.
//!
//! Abilities: `space/query` (run queries; query shipping), `space/read`
//! (read snapshot files; implies `space/query`) and `space/*` (everything).
//! Resources: `space://<owner did>/<space>`, or `space://<owner did>/*`.

use std::collections::HashSet;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::did::Did;
use crate::keys::NodeIdentity;

/// Run queries against a Space.
pub const ABILITY_QUERY: &str = "space/query";
/// Read a Space's snapshot files (implies [`ABILITY_QUERY`]).
pub const ABILITY_READ: &str = "space/read";
/// Every ability on a Space.
pub const ABILITY_ALL: &str = "space/*";

/// Tolerated clock difference between nodes, in seconds.
pub const CLOCK_SKEW_SECS: u64 = 60;
/// Longest accepted delegation chain.
pub const MAX_CHAIN_DEPTH: usize = 8;
/// Largest accepted token, in bytes (proofs are embedded).
pub const MAX_TOKEN_BYTES: usize = 64 * 1024;
const MAX_PROOFS: usize = 16;
const UCAN_VERSION: &str = "0.9.0";

/// Why a token was rejected.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum UcanError {
    #[error("malformed token: {0}")]
    Malformed(String),
    #[error("unsupported token header: {0}")]
    UnsupportedHeader(String),
    #[error("bad signature")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("token not valid yet")]
    NotYetValid,
    #[error("token was issued to {actual}, not {expected}")]
    WrongAudience { expected: String, actual: String },
    #[error("token does not grant {0}")]
    NotCovered(String),
    #[error("delegated token outlives its proof")]
    OutlivesProof,
    #[error("token {0} has been revoked")]
    Revoked(String),
    #[error("no proof chain leads back to {0}")]
    UntrustedIssuer(String),
    #[error("delegation chain deeper than {MAX_CHAIN_DEPTH}")]
    ChainTooDeep,
}

/// One capability: an ability on a resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Capability {
    /// Resource, e.g. `space://did:key:z…/blog`.
    pub with: String,
    /// Ability, e.g. `space/query`.
    pub can: String,
}

impl Capability {
    /// `can` on `space://<owner>/<space>`.
    pub fn space(owner: &Did, space: &str, can: &str) -> Self {
        Self {
            with: format!("space://{owner}/{space}"),
            can: can.to_string(),
        }
    }

    /// Whether holding `self` implies holding `other`.
    pub fn covers(&self, other: &Capability) -> bool {
        let resource = self.with == other.with
            || self.with.strip_suffix('*').is_some_and(|prefix| {
                prefix.ends_with('/')
                    && other
                        .with
                        .strip_prefix(prefix)
                        .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'))
            });
        let ability = self.can == other.can
            || self.can == ABILITY_ALL
            || (self.can == ABILITY_READ && other.can == ABILITY_QUERY);
        resource && ability
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    alg: String,
    typ: String,
}

/// The claims of a token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    pub ucv: String,
    pub iss: Did,
    pub aud: Did,
    pub att: Vec<Capability>,
    /// Expiry, seconds since the Unix epoch. Always present.
    pub exp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    pub nnc: String,
    #[serde(default)]
    pub prf: Vec<String>,
}

/// A decoded (not yet verified) token.
#[derive(Debug, Clone)]
pub struct Ucan {
    raw: String,
    payload: Payload,
    signature: Vec<u8>,
    signed_part_len: usize,
}

/// Content identifier of a token: blake3 of its encoded form, 128 bits hex.
pub fn token_id(raw: &str) -> String {
    let hash = blake3::hash(raw.as_bytes());
    hash.as_bytes()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

fn unb64(part: &str, what: &str) -> Result<Vec<u8>, UcanError> {
    URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| UcanError::Malformed(format!("{what} is not base64url")))
}

impl Ucan {
    /// Decode a token without verifying it.
    pub fn decode(raw: &str) -> Result<Self, UcanError> {
        if raw.len() > MAX_TOKEN_BYTES {
            return Err(UcanError::Malformed("token too large".to_string()));
        }
        let parts: Vec<&str> = raw.split('.').collect();
        let [header, payload, signature] = parts.as_slice() else {
            return Err(UcanError::Malformed(
                "expected header.payload.signature".to_string(),
            ));
        };
        let header: Header = serde_json::from_slice(&unb64(header, "header")?)
            .map_err(|e| UcanError::Malformed(format!("header: {e}")))?;
        if header.alg != "EdDSA" || header.typ != "JWT" {
            return Err(UcanError::UnsupportedHeader(format!(
                "alg={} typ={}",
                header.alg, header.typ
            )));
        }
        let payload: Payload = serde_json::from_slice(&unb64(payload, "payload")?)
            .map_err(|e| UcanError::Malformed(format!("payload: {e}")))?;
        if payload.prf.len() > MAX_PROOFS {
            return Err(UcanError::Malformed("too many proofs".to_string()));
        }
        let signature = unb64(signature, "signature")?;
        let signed_part_len = raw.rfind('.').unwrap_or(0);
        Ok(Self {
            raw: raw.to_string(),
            payload,
            signature,
            signed_part_len,
        })
    }

    /// The claims.
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// The token's content identifier (used for revocation).
    pub fn id(&self) -> String {
        token_id(&self.raw)
    }

    /// The encoded token.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Check the issuer's signature over `header.payload`.
    pub fn verify_signature(&self) -> Result<(), UcanError> {
        self.payload
            .iss
            .verify(
                &self.raw.as_bytes()[..self.signed_part_len],
                &self.signature,
            )
            .map_err(|_| UcanError::BadSignature)
    }

    /// The issuer at the root of the first proof chain, i.e. the node that
    /// originally granted the capability. Does not verify anything.
    pub fn root_issuer(&self) -> Result<Did, UcanError> {
        let mut current = self.clone();
        for _ in 0..=MAX_CHAIN_DEPTH {
            match current.payload.prf.first() {
                None => return Ok(current.payload.iss.clone()),
                Some(parent) => current = Ucan::decode(parent)?,
            }
        }
        Err(UcanError::ChainTooDeep)
    }
}

/// Builds and signs a token.
#[derive(Debug, Clone)]
pub struct UcanBuilder {
    audience: Did,
    capabilities: Vec<Capability>,
    lifetime_secs: u64,
    not_before: Option<u64>,
    proofs: Vec<String>,
    now: u64,
}

impl UcanBuilder {
    /// A token for `audience`, valid for `lifetime_secs` from now.
    pub fn new(audience: Did, lifetime_secs: u64) -> Self {
        Self {
            audience,
            capabilities: Vec::new(),
            lifetime_secs,
            not_before: None,
            proofs: Vec::new(),
            now: crate::unix_now(),
        }
    }

    /// Grant a capability.
    pub fn capability(mut self, capability: Capability) -> Self {
        self.capabilities.push(capability);
        self
    }

    /// Embed a parent token this one is delegated from.
    pub fn proof(mut self, token: impl Into<String>) -> Self {
        self.proofs.push(token.into());
        self
    }

    /// Not valid before `nbf` (seconds since the epoch).
    pub fn not_before(mut self, nbf: u64) -> Self {
        self.not_before = Some(nbf);
        self
    }

    /// Override "now" (tests and replays).
    pub fn issued_at(mut self, now: u64) -> Self {
        self.now = now;
        self
    }

    /// Sign with `issuer` and encode.
    pub fn sign(self, issuer: &NodeIdentity) -> Result<String, UcanError> {
        let mut nonce = [0u8; 12];
        getrandom::fill(&mut nonce).map_err(|e| UcanError::Malformed(e.to_string()))?;
        let payload = Payload {
            ucv: UCAN_VERSION.to_string(),
            iss: issuer.did().clone(),
            aud: self.audience,
            att: self.capabilities,
            exp: self.now.saturating_add(self.lifetime_secs),
            nbf: self.not_before,
            nnc: b64(&nonce),
            prf: self.proofs,
        };
        let header = Header {
            alg: "EdDSA".to_string(),
            typ: "JWT".to_string(),
        };
        let signed = format!("{}.{}", b64(&to_json(&header)?), b64(&to_json(&payload)?));
        let signature = issuer.sign(signed.as_bytes());
        Ok(format!("{signed}.{}", b64(&signature)))
    }
}

fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, UcanError> {
    serde_json::to_vec(value).map_err(|e| UcanError::Malformed(e.to_string()))
}

/// What a verification must establish.
pub struct VerifyOptions<'a> {
    /// The node that owns the resource; every chain must start there.
    pub root: &'a Did,
    /// The authenticated caller; the presented token must be issued to it.
    pub audience: &'a Did,
    /// The capability being exercised.
    pub required: &'a Capability,
    /// Current time, seconds since the epoch.
    pub now: u64,
    /// Revoked token ids.
    pub revoked: &'a HashSet<String>,
}

/// A successfully verified token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// Id of the presented token.
    pub id: String,
    /// Ids of every token in the accepted chain, presented token first.
    pub chain: Vec<String>,
    /// When the presented token expires.
    pub expires_at: u64,
    /// The DID the root issuer granted the capability to (audience of the
    /// root token). A holder of a delegated token acts with its rights.
    pub principal: Did,
}

/// Verify that `token` lets `opts.audience` exercise `opts.required` on a
/// resource owned by `opts.root`.
///
/// Every link must be signed by its issuer, unrevoked, within its validity
/// window, issued to the next link's issuer (the last one to the caller),
/// cover the required capability, and expire no later than its proof. The
/// chain must end at a token issued by `opts.root`.
pub fn verify(token: &str, opts: &VerifyOptions<'_>) -> Result<Verified, UcanError> {
    let ucan = Ucan::decode(token)?;
    let mut chain = Vec::new();
    let principal = verify_link(&ucan, opts.audience, None, opts, 0, &mut chain)?;
    Ok(Verified {
        id: ucan.id(),
        chain,
        expires_at: ucan.payload.exp,
        principal,
    })
}

fn verify_link(
    ucan: &Ucan,
    audience: &Did,
    child_exp: Option<u64>,
    opts: &VerifyOptions<'_>,
    depth: usize,
    chain: &mut Vec<String>,
) -> Result<Did, UcanError> {
    if depth >= MAX_CHAIN_DEPTH {
        return Err(UcanError::ChainTooDeep);
    }
    ucan.verify_signature()?;
    let id = ucan.id();
    if opts.revoked.contains(&id) {
        return Err(UcanError::Revoked(id));
    }
    let p = &ucan.payload;
    if p.exp.saturating_add(CLOCK_SKEW_SECS) <= opts.now {
        return Err(UcanError::Expired);
    }
    if p.nbf
        .is_some_and(|nbf| nbf > opts.now.saturating_add(CLOCK_SKEW_SECS))
    {
        return Err(UcanError::NotYetValid);
    }
    if &p.aud != audience {
        return Err(UcanError::WrongAudience {
            expected: audience.to_string(),
            actual: p.aud.to_string(),
        });
    }
    if child_exp.is_some_and(|child| child > p.exp) {
        return Err(UcanError::OutlivesProof);
    }
    if !p.att.iter().any(|c| c.covers(opts.required)) {
        return Err(UcanError::NotCovered(format!(
            "{} on {}",
            opts.required.can, opts.required.with
        )));
    }
    chain.push(id);
    if &p.iss == opts.root {
        return Ok(p.aud.clone());
    }
    // Delegated: some proof must be a valid token issued to our issuer.
    let mut last_error = UcanError::UntrustedIssuer(opts.root.to_string());
    for raw in &p.prf {
        let parent = match Ucan::decode(raw) {
            Ok(parent) => parent,
            Err(e) => {
                last_error = e;
                continue;
            }
        };
        let mark = chain.len();
        match verify_link(&parent, &p.iss, Some(p.exp), opts, depth + 1, chain) {
            Ok(principal) => return Ok(principal),
            Err(e) => {
                chain.truncate(mark);
                last_error = e;
            }
        }
    }
    Err(last_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3600;

    struct World {
        owner: NodeIdentity,
        bob: NodeIdentity,
        carol: NodeIdentity,
        now: u64,
    }

    fn world() -> World {
        World {
            owner: NodeIdentity::from_seed(&[1; 32]),
            bob: NodeIdentity::from_seed(&[2; 32]),
            carol: NodeIdentity::from_seed(&[3; 32]),
            now: 1_800_000_000,
        }
    }

    fn query(w: &World, space: &str) -> Capability {
        Capability::space(w.owner.did(), space, ABILITY_QUERY)
    }

    fn check(
        w: &World,
        token: &str,
        caller: &Did,
        space: &str,
        revoked: &HashSet<String>,
    ) -> Result<Verified, UcanError> {
        verify(
            token,
            &VerifyOptions {
                root: w.owner.did(),
                audience: caller,
                required: &query(w, space),
                now: w.now,
                revoked,
            },
        )
    }

    fn grant(w: &World, to: &Did, cap: Capability, lifetime: u64) -> String {
        UcanBuilder::new(to.clone(), lifetime)
            .capability(cap)
            .issued_at(w.now)
            .sign(&w.owner)
            .unwrap()
    }

    #[test]
    fn root_grant_verifies() {
        let w = world();
        let token = grant(&w, w.bob.did(), query(&w, "blog"), HOUR);
        let ok = check(&w, &token, w.bob.did(), "blog", &HashSet::new()).unwrap();
        assert_eq!(ok.id, token_id(&token));
        assert_eq!(ok.chain, vec![ok.id.clone()]);
        assert_eq!(ok.expires_at, w.now + HOUR);
        assert_eq!(ok.principal, *w.bob.did());
        assert_eq!(
            Ucan::decode(&token).unwrap().root_issuer().unwrap(),
            *w.owner.did()
        );
    }

    #[test]
    fn wrong_audience_space_or_ability_is_rejected() {
        let w = world();
        let token = grant(&w, w.bob.did(), query(&w, "blog"), HOUR);
        assert!(matches!(
            check(&w, &token, w.carol.did(), "blog", &HashSet::new()),
            Err(UcanError::WrongAudience { .. })
        ));
        assert!(matches!(
            check(&w, &token, w.bob.did(), "orders", &HashSet::new()),
            Err(UcanError::NotCovered(_))
        ));
        // Granting `read` implies `query`, not the other way round.
        let read = grant(
            &w,
            w.bob.did(),
            Capability::space(w.owner.did(), "blog", ABILITY_READ),
            HOUR,
        );
        check(&w, &read, w.bob.did(), "blog", &HashSet::new()).unwrap();
        let wants_read = verify(
            &token,
            &VerifyOptions {
                root: w.owner.did(),
                audience: w.bob.did(),
                required: &Capability::space(w.owner.did(), "blog", ABILITY_READ),
                now: w.now,
                revoked: &HashSet::new(),
            },
        );
        assert!(matches!(wants_read, Err(UcanError::NotCovered(_))));
    }

    #[test]
    fn wildcard_resource_covers_one_level_only() {
        let w = world();
        let all = Capability {
            with: format!("space://{}/*", w.owner.did()),
            can: ABILITY_ALL.to_string(),
        };
        assert!(all.covers(&query(&w, "blog")));
        assert!(!all.covers(&Capability {
            with: format!("space://{}/blog/x", w.owner.did()),
            can: ABILITY_QUERY.into()
        }));
        assert!(!all.covers(&Capability::space(w.bob.did(), "blog", ABILITY_QUERY)));
        let token = grant(&w, w.bob.did(), all, HOUR);
        check(&w, &token, w.bob.did(), "anything", &HashSet::new()).unwrap();
    }

    #[test]
    fn expiry_and_not_before_are_enforced() {
        let w = world();
        let token = grant(&w, w.bob.did(), query(&w, "blog"), HOUR);
        let later = World {
            now: w.now + HOUR + CLOCK_SKEW_SECS,
            ..world()
        };
        assert_eq!(
            check(&later, &token, w.bob.did(), "blog", &HashSet::new()),
            Err(UcanError::Expired)
        );

        let future = UcanBuilder::new(w.bob.did().clone(), HOUR)
            .capability(query(&w, "blog"))
            .not_before(w.now + 10 * 60)
            .issued_at(w.now)
            .sign(&w.owner)
            .unwrap();
        assert_eq!(
            check(&w, &future, w.bob.did(), "blog", &HashSet::new()),
            Err(UcanError::NotYetValid)
        );
    }

    #[test]
    fn tampering_and_foreign_issuers_are_rejected() {
        let w = world();
        let token = grant(&w, w.bob.did(), query(&w, "blog"), HOUR);
        // Re-encode the payload with a different space but keep the signature.
        let parts: Vec<&str> = token.split('.').collect();
        let mut payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        payload["att"][0]["with"] =
            serde_json::Value::String(format!("space://{}/orders", w.owner.did()));
        let forged = format!(
            "{}.{}.{}",
            parts[0],
            b64(&serde_json::to_vec(&payload).unwrap()),
            parts[2]
        );
        assert_eq!(
            check(&w, &forged, w.bob.did(), "orders", &HashSet::new()),
            Err(UcanError::BadSignature)
        );

        // Self-issued by bob: valid signature, but not rooted at the owner.
        let self_issued = UcanBuilder::new(w.bob.did().clone(), HOUR)
            .capability(query(&w, "blog"))
            .issued_at(w.now)
            .sign(&w.bob)
            .unwrap();
        assert!(matches!(
            check(&w, &self_issued, w.bob.did(), "blog", &HashSet::new()),
            Err(UcanError::UntrustedIssuer(_))
        ));

        // `alg: none` style headers are refused before anything else.
        let none_header = b64(br#"{"alg":"none","typ":"JWT"}"#);
        let unsigned = format!("{none_header}.{}.", parts[1]);
        assert!(matches!(
            Ucan::decode(&unsigned),
            Err(UcanError::UnsupportedHeader(_))
        ));
        assert!(matches!(Ucan::decode("a.b"), Err(UcanError::Malformed(_))));
    }

    #[test]
    fn delegation_chain_attenuates_and_revocation_propagates() {
        let w = world();
        let root = grant(&w, w.bob.did(), query(&w, "blog"), 2 * HOUR);
        let delegated = UcanBuilder::new(w.carol.did().clone(), HOUR)
            .capability(query(&w, "blog"))
            .proof(root.clone())
            .issued_at(w.now)
            .sign(&w.bob)
            .unwrap();
        let ok = check(&w, &delegated, w.carol.did(), "blog", &HashSet::new()).unwrap();
        assert_eq!(ok.chain, vec![token_id(&delegated), token_id(&root)]);
        // Carol acts with the rights the owner gave Bob.
        assert_eq!(ok.principal, *w.bob.did());
        assert_eq!(
            Ucan::decode(&delegated).unwrap().root_issuer().unwrap(),
            *w.owner.did()
        );

        // Revoking the root kills every token delegated from it.
        let revoked: HashSet<String> = [token_id(&root)].into();
        assert!(matches!(
            check(&w, &delegated, w.carol.did(), "blog", &revoked),
            Err(UcanError::Revoked(_))
        ));

        // Bob cannot delegate a Space he was not given.
        let escalated = UcanBuilder::new(w.carol.did().clone(), HOUR)
            .capability(query(&w, "orders"))
            .proof(root.clone())
            .issued_at(w.now)
            .sign(&w.bob)
            .unwrap();
        assert!(matches!(
            check(&w, &escalated, w.carol.did(), "orders", &HashSet::new()),
            Err(UcanError::NotCovered(_))
        ));

        // Nor can a delegation outlive its proof.
        let longer = UcanBuilder::new(w.carol.did().clone(), 3 * HOUR)
            .capability(query(&w, "blog"))
            .proof(root.clone())
            .issued_at(w.now)
            .sign(&w.bob)
            .unwrap();
        assert_eq!(
            check(&w, &longer, w.carol.did(), "blog", &HashSet::new()),
            Err(UcanError::OutlivesProof)
        );

        // A proof issued to someone else does not authorize bob's delegation.
        let to_carol = grant(&w, w.carol.did(), query(&w, "blog"), 2 * HOUR);
        let stolen = UcanBuilder::new(w.carol.did().clone(), HOUR)
            .capability(query(&w, "blog"))
            .proof(to_carol)
            .issued_at(w.now)
            .sign(&w.bob)
            .unwrap();
        assert!(matches!(
            check(&w, &stolen, w.carol.did(), "blog", &HashSet::new()),
            Err(UcanError::WrongAudience { .. })
        ));
    }

    #[test]
    fn overly_deep_chains_are_rejected() {
        let w = world();
        let ids: Vec<NodeIdentity> = (10..30u8)
            .map(|i| NodeIdentity::from_seed(&[i; 32]))
            .collect();
        let mut token = grant(&w, ids[0].did(), query(&w, "blog"), HOUR);
        for i in 1..=MAX_CHAIN_DEPTH {
            token = UcanBuilder::new(ids[i].did().clone(), HOUR)
                .capability(query(&w, "blog"))
                .proof(token)
                .issued_at(w.now)
                .sign(&ids[i - 1])
                .unwrap();
        }
        assert_eq!(
            check(
                &w,
                &token,
                ids[MAX_CHAIN_DEPTH].did(),
                "blog",
                &HashSet::new()
            ),
            Err(UcanError::ChainTooDeep)
        );
    }
}
