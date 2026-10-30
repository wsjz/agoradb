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

use thiserror::Error;

/// Errors about identities, keys and signatures.
#[derive(Error, Debug)]
pub enum IdentityError {
    #[error("invalid DID '{0}': {1}")]
    InvalidDid(String, String),

    #[error("invalid signature")]
    InvalidSignature,

    #[error("invalid key material: {0}")]
    InvalidKey(String),

    #[error("insecure permissions on {0}: the key file must not be readable by group or others (chmod 600)")]
    InsecureKeyFile(String),

    #[error("random number generator failed: {0}")]
    Random(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("TLS certificate: {0}")]
    Tls(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
