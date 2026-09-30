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
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// A parsed AgoraDB space URI.
///
/// Format: `space://<owner_did>/<name>`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// The storage class of a Space, which also selects its default engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpaceKind {
    /// Iceberg + Parquet storage, served by an analytical engine (DuckDB).
    Analytical,
    /// A single SQLite database file, served by SQLite.
    Transactional,
}

impl SpaceKind {
    /// The engine a Space of this kind uses unless overridden.
    pub fn default_engine(self) -> EngineKind {
        match self {
            SpaceKind::Analytical => EngineKind::DuckDb,
            SpaceKind::Transactional => EngineKind::Sqlite,
        }
    }
}

impl fmt::Display for SpaceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpaceKind::Analytical => f.write_str("analytical"),
            SpaceKind::Transactional => f.write_str("transactional"),
        }
    }
}

impl FromStr for SpaceKind {
    type Err = crate::error::AgoraError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "analytical" => Ok(SpaceKind::Analytical),
            "transactional" => Ok(SpaceKind::Transactional),
            other => Err(crate::error::AgoraError::InvalidArgument(format!(
                "unknown space kind '{other}' (expected 'analytical' or 'transactional')"
            ))),
        }
    }
}

/// The query engine that computes over a Space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineKind {
    /// DuckDB: analytical, reads Parquet directly.
    DuckDb,
    /// SQLite: transactional, row-oriented.
    Sqlite,
}

impl fmt::Display for EngineKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineKind::DuckDb => f.write_str("duckdb"),
            EngineKind::Sqlite => f.write_str("sqlite"),
        }
    }
}

impl FromStr for EngineKind {
    type Err = crate::error::AgoraError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "duckdb" => Ok(EngineKind::DuckDb),
            "sqlite" => Ok(EngineKind::Sqlite),
            other => Err(crate::error::AgoraError::InvalidArgument(format!(
                "unknown engine '{other}' (expected 'duckdb' or 'sqlite')"
            ))),
        }
    }
}

/// Whether a Space may write to the Location it is bound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// The Space is the single writer of its Location.
    Writable,
    /// The Space only reads its Location.
    ReadOnly,
}

impl fmt::Display for AccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessMode::Writable => f.write_str("writable"),
            AccessMode::ReadOnly => f.write_str("readonly"),
        }
    }
}

impl FromStr for AccessMode {
    type Err = crate::error::AgoraError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "writable" | "readwrite" | "rw" => Ok(AccessMode::Writable),
            "readonly" | "read_only" | "ro" => Ok(AccessMode::ReadOnly),
            other => Err(crate::error::AgoraError::InvalidArgument(format!(
                "unknown access mode '{other}' (expected 'writable' or 'readonly')"
            ))),
        }
    }
}

/// What `CREATE SPACE` asks for. Unset fields take the defaults described in
/// the v3 architecture (§3.2): `kind` = analytical, `engine` = the kind's
/// default engine, a new Location named after the Space, `access` = writable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateSpaceRequest {
    pub name: String,
    pub kind: Option<SpaceKind>,
    pub engine: Option<EngineKind>,
    /// Bind to an existing Location instead of creating one.
    pub location: Option<String>,
    pub access: Option<AccessMode>,
    /// Storage strategy (`'disk'` only in 3.0).
    pub storage: Option<String>,
}

impl CreateSpaceRequest {
    /// A request with only the name set.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
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

    #[test]
    fn test_space_kind_default_engine() {
        assert_eq!(SpaceKind::Analytical.default_engine(), EngineKind::DuckDb);
        assert_eq!(
            SpaceKind::Transactional.default_engine(),
            EngineKind::Sqlite
        );
    }

    #[test]
    fn test_enums_parse_case_insensitively() {
        assert_eq!(
            "ANALYTICAL".parse::<SpaceKind>().unwrap(),
            SpaceKind::Analytical
        );
        assert_eq!("DuckDB".parse::<EngineKind>().unwrap(), EngineKind::DuckDb);
        assert_eq!(
            "ReadOnly".parse::<AccessMode>().unwrap(),
            AccessMode::ReadOnly
        );
        assert!("graph".parse::<SpaceKind>().is_err());
    }

    #[test]
    fn test_enums_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&SpaceKind::Transactional).unwrap(),
            "\"transactional\""
        );
        assert_eq!(
            serde_json::to_string(&EngineKind::DuckDb).unwrap(),
            "\"duck_db\""
        );
        let mode: AccessMode = serde_json::from_str("\"read_only\"").unwrap();
        assert_eq!(mode, AccessMode::ReadOnly);
    }
}
