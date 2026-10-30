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

//! A node's Ed25519 signing key.
//!
//! The 32-byte seed comes from the [`ENV_NODE_KEY`] environment variable
//! (base64url, for containers and secret managers) or from a private file
//! that is created with mode 0600 and refused if group/other can read it.
//! The seed is zeroized on drop and never formatted by `Debug`.

use std::fmt;
use std::io::Write;
use std::path::Path;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use zeroize::Zeroizing;

use crate::did::Did;
use crate::error::IdentityError;

/// Environment variable holding the node's base64url-encoded 32-byte seed.
pub const ENV_NODE_KEY: &str = "AGORADB_NODE_KEY";

/// File name of the node key inside `<root>/.agora/identity/`.
pub const NODE_KEY_FILE: &str = "node_ed25519";

/// A node's signing identity.
pub struct NodeIdentity {
    signing: SigningKey,
    did: Did,
}

impl fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("did", &self.did)
            .finish_non_exhaustive()
    }
}

impl NodeIdentity {
    /// Create an identity from a 32-byte seed.
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(seed);
        let did = Did::from_public_key(&signing.verifying_key());
        Self { signing, did }
    }

    /// Generate a fresh identity from the OS random number generator.
    pub fn generate() -> Result<Self, IdentityError> {
        let mut seed = Zeroizing::new([0u8; 32]);
        getrandom::fill(seed.as_mut()).map_err(|e| IdentityError::Random(e.to_string()))?;
        Ok(Self::from_seed(&seed))
    }

    /// Decode a base64url seed (as stored in [`ENV_NODE_KEY`] or the key file).
    pub fn from_base64_seed(encoded: &str) -> Result<Self, IdentityError> {
        let bytes = Zeroizing::new(
            URL_SAFE_NO_PAD
                .decode(encoded.trim())
                .map_err(|_| IdentityError::InvalidKey("seed is not base64url".to_string()))?,
        );
        let seed: Zeroizing<[u8; 32]> = Zeroizing::new(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| IdentityError::InvalidKey("seed must be 32 bytes".to_string()))?,
        );
        Ok(Self::from_seed(&seed))
    }

    /// Load the identity from `path`, creating it (mode 0600) if missing.
    ///
    /// Creation is atomic: the key is written to a private temporary file
    /// and linked into place only if `path` still does not exist, so two
    /// processes starting on the same directory end up with the same key.
    pub fn load_or_create(path: &Path) -> Result<Self, IdentityError> {
        if path.exists() {
            return Self::load(path);
        }
        let identity = Self::generate()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(identity.signing.to_bytes()));
        let mut suffix = [0u8; 8];
        getrandom::fill(&mut suffix).map_err(|e| IdentityError::Random(e.to_string()))?;
        let suffix: String = suffix.iter().map(|b| format!("{b:02x}")).collect();
        let tmp = path.with_extension(format!("tmp-{}-{suffix}", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        {
            let mut file = create_private(&tmp)?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
        }
        let linked = std::fs::hard_link(&tmp, path);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => Ok(identity),
            // Someone else created it first: use theirs.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Self::load(path),
            // Filesystems without hard links: plain exclusive create.
            Err(_) => {
                let mut file = create_private(path)?;
                file.write_all(encoded.as_bytes())?;
                file.sync_all()?;
                Ok(identity)
            }
        }
    }

    fn load(path: &Path) -> Result<Self, IdentityError> {
        check_private(path)?;
        let encoded = Zeroizing::new(std::fs::read_to_string(path)?);
        Self::from_base64_seed(&encoded)
    }

    /// This node's DID.
    pub fn did(&self) -> &Did {
        &self.did
    }

    /// Sign `message` with the node key.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing.sign(message).to_bytes()
    }

    /// A self-signed TLS certificate for this node's own Ed25519 key, with
    /// its DID as subject. Peers accept it only if its key matches the DID
    /// they expect (architecture v3 §9), so no CA or host name is involved.
    /// Generated on demand; neither the certificate nor the key is stored.
    pub fn tls_identity(&self) -> Result<TlsIdentity, IdentityError> {
        use ed25519_dalek::pkcs8::EncodePrivateKey;
        let tls = |e: &dyn fmt::Display| IdentityError::Tls(e.to_string());
        let pkcs8 = self.signing.to_pkcs8_der().map_err(|e| tls(&e))?;
        let key = rcgen::KeyPair::try_from(pkcs8.as_bytes()).map_err(|e| tls(&e))?;
        let mut params =
            rcgen::CertificateParams::new(Vec::<String>::new()).map_err(|e| tls(&e))?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, self.did.as_str());
        let cert = params.self_signed(&key).map_err(|e| tls(&e))?;
        Ok(TlsIdentity {
            cert_der: cert.der().to_vec(),
            cert_pem: cert.pem(),
            key_pem: Zeroizing::new(key.serialize_pem()),
        })
    }

    /// Run `f` with the 32-byte secret seed, for deriving another key of the
    /// same identity (the libp2p identity used by discovery). The seed is
    /// not kept by this call; `f` must not keep it either.
    pub fn with_seed<R>(&self, f: impl FnOnce(&[u8; 32]) -> R) -> R {
        let seed = Zeroizing::new(self.signing.to_bytes());
        f(&seed)
    }
}

/// A node's TLS certificate and private key (see
/// [`NodeIdentity::tls_identity`]). `Debug` never prints the key.
pub struct TlsIdentity {
    pub cert_der: Vec<u8>,
    pub cert_pem: String,
    pub key_pem: Zeroizing<String>,
}

impl fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsIdentity")
            .field("cert_pem", &self.cert_pem)
            .field("key_pem", &"<redacted>")
            .finish()
    }
}

/// Load the node identity: from [`ENV_NODE_KEY`] when set, otherwise from
/// `<root>/.agora/identity/node_ed25519` (created on first use).
pub fn load_node_identity(root: &Path) -> Result<NodeIdentity, IdentityError> {
    match std::env::var(ENV_NODE_KEY) {
        Ok(encoded) if !encoded.trim().is_empty() => {
            let encoded = Zeroizing::new(encoded);
            NodeIdentity::from_base64_seed(&encoded)
        }
        _ => {
            NodeIdentity::load_or_create(&root.join(".agora").join("identity").join(NODE_KEY_FILE))
        }
    }
}

#[cfg(unix)]
fn create_private(path: &Path) -> Result<std::fs::File, IdentityError> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?)
}

#[cfg(not(unix))]
fn create_private(path: &Path) -> Result<std::fs::File, IdentityError> {
    Ok(std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?)
}

#[cfg(unix)]
fn check_private(path: &Path) -> Result<(), IdentityError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(IdentityError::InsecureKeyFile(path.display().to_string()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_path: &Path) -> Result<(), IdentityError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_certificates_carry_the_node_key() {
        let id = NodeIdentity::from_seed(&[7; 32]);
        let tls = id.tls_identity().unwrap();
        // An Ed25519 SubjectPublicKeyInfo: fixed 12-byte prefix, then the key.
        let prefix = [
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
        ];
        let mut spki = prefix.to_vec();
        spki.extend_from_slice(id.did().public_key().as_bytes());
        assert!(tls
            .cert_der
            .windows(spki.len())
            .any(|w| w == spki.as_slice()));
        assert!(tls.cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(tls.key_pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(!format!("{tls:?}").contains("PRIVATE KEY"));
        assert_eq!(id.with_seed(|s| *s), [7; 32]);
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// RFC 8032 §7.1 test 1: secret key, public key and signature of "".
    #[test]
    fn rfc8032_test_vector() {
        let seed: [u8; 32] =
            hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
                .try_into()
                .unwrap();
        let id = NodeIdentity::from_seed(&seed);
        assert_eq!(
            id.did().as_str(),
            "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw"
        );
        let sig = id.sign(b"");
        assert_eq!(
            sig.to_vec(),
            hex("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b")
        );
        id.did().verify(b"", &sig).unwrap();
        assert!(id.did().verify(b"x", &sig).is_err());
    }

    #[test]
    fn debug_never_prints_the_key() {
        let id = NodeIdentity::generate().unwrap();
        let text = format!("{id:?}");
        assert!(text.contains(id.did().as_str()));
        assert!(!text.to_lowercase().contains("signing"));
    }

    #[test]
    fn key_file_is_created_private_and_reloaded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity").join(NODE_KEY_FILE);
        let first = NodeIdentity::load_or_create(&path).unwrap();
        let again = NodeIdentity::load_or_create(&path).unwrap();
        assert_eq!(first.did(), again.did());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(
                NodeIdentity::load_or_create(&path),
                Err(IdentityError::InsecureKeyFile(_))
            ));
        }
    }

    #[test]
    fn concurrent_creation_agrees_on_one_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(NODE_KEY_FILE);
        let dids: Vec<Did> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| NodeIdentity::load_or_create(&path).unwrap().did().clone()))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(dids.windows(2).all(|w| w[0] == w[1]));
        let leftovers = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(leftovers, 1, "no temporary files left behind");
    }

    #[test]
    fn malformed_seeds_are_rejected() {
        assert!(NodeIdentity::from_base64_seed("not base64!").is_err());
        assert!(NodeIdentity::from_base64_seed("AAAA").is_err());
    }
}
