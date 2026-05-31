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

use std::fmt;

/// A parsed AgoraDB space URI.
///
/// Format: `space://<owner_did>/<name>`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceUri {
    pub owner_did: String,
    pub name: String,
}

impl SpaceUri {
    /// Parse a space URI string.
    ///
    /// Expected format: `space://<did>/<name>`
    pub fn parse(uri: &str) -> Result<Self, crate::error::AgoraError> {
        let stripped = uri
            .strip_prefix("space://")
            .ok_or_else(|| crate::error::AgoraError::InvalidSpaceUri(uri.to_string()))?;
        let parts: Vec<&str> = stripped.splitn(2, '/').collect();
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(crate::error::AgoraError::InvalidSpaceUri(uri.to_string()));
        }
        Ok(Self {
            owner_did: parts[0].to_string(),
            name: parts[1].to_string(),
        })
    }
}

impl fmt::Display for SpaceUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "space://{}/{}", self.owner_did, self.name)
    }
}

/// Storage strategy for a space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageStrategy {
    Browser,
    Disk,
    S3,
    BrowserDisk,
    BrowserS3,
    DiskS3,
    All,
}

/// Mode of a space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Table,
    Graph,
    Vector,
    Fts,
    Blob,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_space_uri() {
        let uri = SpaceUri::parse("space://did:ethr:0x123/my-space").unwrap();
        assert_eq!(uri.owner_did, "did:ethr:0x123");
        assert_eq!(uri.name, "my-space");
    }

    #[test]
    fn test_parse_missing_prefix() {
        let result = SpaceUri::parse("did:ethr:0x123/my-space");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_name() {
        let result = SpaceUri::parse("space://did:ethr:0x123");
        assert!(result.is_err());
    }

    #[test]
    fn test_display_space_uri() {
        let uri = SpaceUri {
            owner_did: "did:ethr:0x123".to_string(),
            name: "my-space".to_string(),
        };
        assert_eq!(uri.to_string(), "space://did:ethr:0x123/my-space");
    }
}
