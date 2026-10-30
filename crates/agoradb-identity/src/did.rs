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

//! `did:key` identifiers for Ed25519 keys
//! (<https://w3c-ccg.github.io/did-method-key/>): `did:key:z` followed by the
//! base58btc encoding of the multicodec prefix `0xed 0x01` and the 32-byte
//! public key.

use std::fmt;
use std::str::FromStr;

use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::error::IdentityError;

const PREFIX: &str = "did:key:z";
const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

/// A `did:key` identifier of an Ed25519 public key. Always well-formed:
/// constructing one validates the encoding and the key.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Did {
    text: String,
    key: [u8; 32],
}

impl Did {
    /// The DID of a public key.
    pub fn from_public_key(key: &VerifyingKey) -> Self {
        let mut bytes = Vec::with_capacity(34);
        bytes.extend_from_slice(&ED25519_MULTICODEC);
        bytes.extend_from_slice(key.as_bytes());
        Self {
            text: format!("{PREFIX}{}", bs58::encode(bytes).into_string()),
            key: key.to_bytes(),
        }
    }

    /// Parse and validate a `did:key` string.
    pub fn parse(text: &str) -> Result<Self, IdentityError> {
        let invalid = |why: &str| IdentityError::InvalidDid(text.to_string(), why.to_string());
        let encoded = text
            .strip_prefix(PREFIX)
            .ok_or_else(|| invalid("expected did:key:z…"))?;
        let bytes = bs58::decode(encoded)
            .into_vec()
            .map_err(|e| invalid(&e.to_string()))?;
        if bytes.len() != 34 || bytes[..2] != ED25519_MULTICODEC {
            return Err(invalid("not an Ed25519 did:key"));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes[2..]);
        VerifyingKey::from_bytes(&key).map_err(|e| invalid(&e.to_string()))?;
        Ok(Self {
            text: text.to_string(),
            key,
        })
    }

    /// The DID as a string.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The Ed25519 public key.
    pub fn public_key(&self) -> VerifyingKey {
        // Validated on construction.
        VerifyingKey::from_bytes(&self.key).unwrap_or_else(|_| unreachable!("validated key"))
    }

    /// Verify `signature` over `message` with this DID's key. Uses strict
    /// verification (rejects small-order and non-canonical encodings).
    pub fn verify(&self, message: &[u8], signature: &[u8]) -> Result<(), IdentityError> {
        let signature =
            Signature::from_slice(signature).map_err(|_| IdentityError::InvalidSignature)?;
        self.public_key()
            .verify_strict(message, &signature)
            .map_err(|_| IdentityError::InvalidSignature)
    }
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl fmt::Debug for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Did({})", self.text)
    }
}

impl FromStr for Did {
    type Err = IdentityError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl TryFrom<String> for Did {
    type Error = IdentityError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(&s)
    }
}

impl From<Did> for String {
    fn from(did: Did) -> Self {
        did.text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 8032 §7.1 test 1 public key.
    const RFC8032_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    /// The same key encoded as did:key by an independent implementation.
    const RFC8032_DID: &str = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn known_key_encodes_to_known_did() {
        let bytes: [u8; 32] = hex(RFC8032_PUBLIC).try_into().unwrap();
        let did = Did::from_public_key(&VerifyingKey::from_bytes(&bytes).unwrap());
        assert_eq!(did.as_str(), RFC8032_DID);
        assert!(did.as_str().starts_with("did:key:z6Mk"));
        assert_eq!(Did::parse(RFC8032_DID).unwrap(), did);
    }

    #[test]
    fn invalid_dids_are_rejected() {
        for bad in [
            "",
            "did:web:example.com",
            "did:key:6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw",
            "did:key:z0OIl",
            // secp256k1 multicodec (0xe7 0x01) is not Ed25519.
            "did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
        ] {
            assert!(Did::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn serde_roundtrip_validates() {
        let did = Did::parse(RFC8032_DID).unwrap();
        let json = serde_json::to_string(&did).unwrap();
        assert_eq!(json, format!("\"{RFC8032_DID}\""));
        assert_eq!(serde_json::from_str::<Did>(&json).unwrap(), did);
        assert!(serde_json::from_str::<Did>("\"did:key:zbad\"").is_err());
    }
}
