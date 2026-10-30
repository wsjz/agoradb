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

//! TLS between nodes (architecture v3 §9).
//!
//! Every node presents a self-signed certificate for its own Ed25519 key
//! (`NodeIdentity::tls_identity`). A client knows which DID it expects (the
//! root issuer of the UCAN it presents) and accepts the server only if the
//! certificate carries that DID's key and the handshake is signed with it.
//! There is no CA and no host-name check: the DID is the identity.

use std::sync::{Arc, Once};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error, SignatureScheme};

/// DER prefix of an Ed25519 SubjectPublicKeyInfo; the 32-byte key follows.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Make ring the process-wide rustls backend, once. Servers build their TLS
/// configuration from the process default, which is ambiguous when more
/// than one backend is compiled in.
pub fn install_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Err means another backend was installed first; keep it.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Accepts only a certificate whose key is `expected`, an Ed25519 public
/// key (a peer's DID).
#[derive(Debug)]
pub struct DidCertVerifier {
    expected: [u8; 32],
    algorithms: WebPkiSupportedAlgorithms,
}

impl DidCertVerifier {
    pub fn new(expected: [u8; 32]) -> Arc<Self> {
        Arc::new(Self {
            expected,
            algorithms: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        })
    }
}

impl ServerCertVerifier for DidCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let cert = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|_| Error::InvalidCertificate(CertificateError::BadEncoding))?;
        let spki = cert.subject_public_key_info();
        let spki: &[u8] = spki.as_ref();
        let key = spki
            .strip_prefix(ED25519_SPKI_PREFIX.as_slice())
            .filter(|key| key.len() == 32)
            .ok_or(Error::InvalidCertificate(CertificateError::BadEncoding))?;
        if key != self.expected {
            return Err(Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }
        // The handshake signature (checked below) proves the server holds
        // the key; the certificate only has to carry it.
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agoradb_identity::NodeIdentity;

    fn check(verifier: &DidCertVerifier, cert: &[u8]) -> Result<ServerCertVerified, Error> {
        verifier.verify_server_cert(
            &CertificateDer::from(cert.to_vec()),
            &[],
            &ServerName::try_from("agoradb.peer").unwrap(),
            &[],
            UnixTime::now(),
        )
    }

    #[test]
    fn only_the_expected_did_is_accepted() {
        let a = NodeIdentity::from_seed(&[1; 32]);
        let b = NodeIdentity::from_seed(&[2; 32]);
        let cert = a.tls_identity().unwrap().cert_der;

        let for_a = DidCertVerifier::new(a.did().public_key().to_bytes());
        assert!(check(&for_a, &cert).is_ok());

        let for_b = DidCertVerifier::new(b.did().public_key().to_bytes());
        assert!(matches!(
            check(&for_b, &cert),
            Err(Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure
            ))
        ));
        assert!(check(&for_a, b"not a certificate").is_err());
        assert_eq!(
            for_a.supported_verify_schemes(),
            vec![SignatureScheme::ED25519]
        );
    }
}
