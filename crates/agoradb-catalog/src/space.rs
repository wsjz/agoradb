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

//! Space (logical) and Location (physical) definitions plus the binding rules
//! of architecture v3 §3.2.

use agoradb_core::{AccessMode, CatalogError, CreateSpaceRequest, EngineKind, SpaceKind, SpaceUri};
use serde::{Deserialize, Serialize};

/// Identifier of a [`Location`]; defaults to the name of the Space that created it.
pub type LocationId = String;

/// Relative (to the catalog root) directory that holds SQLite Space files.
pub const SQLITE_DIR: &str = ".agora/sqlite";

/// The only storage strategy supported in 3.0.
pub const STORAGE_DISK: &str = "disk";

/// A logical Space: the unit of sovereignty, authorization and engine binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Space {
    /// Node-local name; also the SQL qualifier (`<name>.<table>`).
    pub name: String,
    /// Global identity once the node has a DID (3.1).
    pub uri: Option<SpaceUri>,
    pub kind: SpaceKind,
    pub engine: EngineKind,
    /// The physical Location this Space is bound to.
    pub location: LocationId,
    pub access: AccessMode,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_at_ms: u64,
    /// Set on an analytical Space written only by `PUBLISH SPACE <source>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_from: Option<String>,
}

/// The on-disk format of a Location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum LocationFormat {
    /// An Iceberg namespace (directory of tables) under the catalog root.
    IcebergParquet { namespace: String },
    /// A single SQLite database file, path relative to the catalog root.
    SqliteFile { path: String },
}

impl LocationFormat {
    /// Short name for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            LocationFormat::IcebergParquet { .. } => "iceberg_parquet",
            LocationFormat::SqliteFile { .. } => "sqlite_file",
        }
    }
}

/// A physical Location. Several Spaces may bind to it, at most one as writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub id: LocationId,
    #[serde(flatten)]
    pub format: LocationFormat,
    /// Storage strategy tokens (`"disk"` only in 3.0).
    pub residency: Vec<String>,
    /// Name of the Space that owns the Location, if any.
    pub writer: Option<String>,
}

/// Whether `engine` can read `format` at all.
pub fn engine_reads(engine: EngineKind, format: &LocationFormat) -> bool {
    match (engine, format) {
        (EngineKind::DuckDb, _) => true,
        (EngineKind::Sqlite, LocationFormat::SqliteFile { .. }) => true,
        (EngineKind::Sqlite, LocationFormat::IcebergParquet { .. }) => false,
    }
}

/// Apply the defaults and binding rules to a `CREATE SPACE` request.
///
/// `existing` is the Location named by `request.location`, if the request
/// names one. Returns the Space to register together with the Location as it
/// must be stored afterwards (new, or updated with the new writer).
pub fn validate_binding(
    request: &CreateSpaceRequest,
    existing: Option<&Location>,
    now_ms: u64,
) -> Result<(Space, Location), CatalogError> {
    if request.name.is_empty() {
        return Err(CatalogError::InvalidProperty(
            "space name must not be empty".to_string(),
        ));
    }
    if let Some(storage) = &request.storage {
        if storage != STORAGE_DISK {
            return Err(CatalogError::InvalidProperty(format!(
                "STORAGE = '{storage}' is not supported yet (only '{STORAGE_DISK}')"
            )));
        }
    }
    if request.location.is_some() && existing.is_none() {
        return Err(CatalogError::LocationNotFound(
            request.location.clone().unwrap_or_default(),
        ));
    }

    let kind = request.kind.unwrap_or(SpaceKind::Analytical);
    let engine = request.engine.unwrap_or_else(|| kind.default_engine());

    let mut location = match existing {
        Some(loc) => loc.clone(),
        None => Location {
            id: request.name.clone(),
            format: match kind {
                SpaceKind::Analytical => LocationFormat::IcebergParquet {
                    namespace: request.name.clone(),
                },
                SpaceKind::Transactional => LocationFormat::SqliteFile {
                    path: format!("{SQLITE_DIR}/{}.sqlite", request.name),
                },
            },
            residency: vec![STORAGE_DISK.to_string()],
            writer: None,
        },
    };

    let access = match (kind, &location.format) {
        (SpaceKind::Transactional, LocationFormat::IcebergParquet { .. }) => {
            return Err(CatalogError::KindFormatMismatch {
                kind: kind.to_string(),
                format: location.format.name().to_string(),
            })
        }
        (SpaceKind::Transactional, LocationFormat::SqliteFile { .. }) => {
            if request.access == Some(AccessMode::ReadOnly) {
                return Err(CatalogError::InvalidProperty(
                    "transactional spaces are always writable".to_string(),
                ));
            }
            AccessMode::Writable
        }
        (SpaceKind::Analytical, LocationFormat::SqliteFile { .. }) => {
            // An analytical view over a transactional file never writes to it.
            if request.access == Some(AccessMode::Writable) {
                return Err(CatalogError::KindFormatMismatch {
                    kind: kind.to_string(),
                    format: location.format.name().to_string(),
                });
            }
            AccessMode::ReadOnly
        }
        (SpaceKind::Analytical, LocationFormat::IcebergParquet { .. }) => {
            request.access.unwrap_or(if existing.is_some() {
                AccessMode::ReadOnly
            } else {
                AccessMode::Writable
            })
        }
    };

    if !engine_reads(engine, &location.format) {
        return Err(CatalogError::IncompatibleFormat {
            engine: engine.to_string(),
            format: location.format.name().to_string(),
        });
    }

    if access == AccessMode::Writable {
        if let Some(writer) = &location.writer {
            return Err(CatalogError::LocationHasWriter {
                location: location.id.clone(),
                writer: writer.clone(),
            });
        }
        location.writer = Some(request.name.clone());
    }

    let space = Space {
        name: request.name.clone(),
        uri: None,
        kind,
        engine,
        location: location.id.clone(),
        access,
        created_at_ms: now_ms,
        published_from: None,
    };
    Ok((space, location))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(name: &str) -> CreateSpaceRequest {
        CreateSpaceRequest::new(name)
    }

    fn parquet_location(id: &str, writer: Option<&str>) -> Location {
        Location {
            id: id.to_string(),
            format: LocationFormat::IcebergParquet {
                namespace: id.to_string(),
            },
            residency: vec![STORAGE_DISK.to_string()],
            writer: writer.map(str::to_string),
        }
    }

    fn sqlite_location(id: &str, writer: Option<&str>) -> Location {
        Location {
            id: id.to_string(),
            format: LocationFormat::SqliteFile {
                path: format!("{SQLITE_DIR}/{id}.sqlite"),
            },
            residency: vec![STORAGE_DISK.to_string()],
            writer: writer.map(str::to_string),
        }
    }

    #[test]
    fn defaults_from_kind() {
        let (space, loc) = validate_binding(&req("blog"), None, 7).unwrap();
        assert_eq!(space.kind, SpaceKind::Analytical);
        assert_eq!(space.engine, EngineKind::DuckDb);
        assert_eq!(space.access, AccessMode::Writable);
        assert_eq!(space.location, "blog");
        assert_eq!(space.created_at_ms, 7);
        assert_eq!(loc, parquet_location("blog", Some("blog")));

        let mut r = req("orders");
        r.kind = Some(SpaceKind::Transactional);
        let (space, loc) = validate_binding(&r, None, 0).unwrap();
        assert_eq!(space.engine, EngineKind::Sqlite);
        assert_eq!(space.access, AccessMode::Writable);
        assert_eq!(loc, sqlite_location("orders", Some("orders")));
    }

    #[test]
    fn transactional_requires_sqlite_file() {
        let mut r = req("orders");
        r.kind = Some(SpaceKind::Transactional);
        r.location = Some("blog".to_string());
        let err = validate_binding(&r, Some(&parquet_location("blog", None)), 0).unwrap_err();
        assert!(matches!(err, CatalogError::KindFormatMismatch { .. }));

        r.access = Some(AccessMode::ReadOnly);
        r.location = None;
        assert!(matches!(
            validate_binding(&r, None, 0),
            Err(CatalogError::InvalidProperty(_))
        ));
    }

    #[test]
    fn analytical_over_sqlite_must_be_readonly() {
        let mut r = req("orders_ro");
        r.location = Some("orders".to_string());
        let loc = sqlite_location("orders", Some("orders"));

        let (space, updated) = validate_binding(&r, Some(&loc), 0).unwrap();
        assert_eq!(space.access, AccessMode::ReadOnly);
        assert_eq!(space.engine, EngineKind::DuckDb);
        assert_eq!(
            updated.writer.as_deref(),
            Some("orders"),
            "writer unchanged"
        );

        r.access = Some(AccessMode::Writable);
        assert!(matches!(
            validate_binding(&r, Some(&loc), 0),
            Err(CatalogError::KindFormatMismatch { .. })
        ));
    }

    #[test]
    fn second_writer_rejected_and_readonly_second_binding_allowed() {
        let owned = parquet_location("blog", Some("blog"));

        let mut r = req("blog_rw");
        r.location = Some("blog".to_string());
        r.access = Some(AccessMode::Writable);
        assert!(matches!(
            validate_binding(&r, Some(&owned), 0),
            Err(CatalogError::LocationHasWriter { ref writer, .. }) if writer == "blog"
        ));

        r.access = None; // defaults to readonly when binding an existing location
        let (space, loc) = validate_binding(&r, Some(&owned), 0).unwrap();
        assert_eq!(space.access, AccessMode::ReadOnly);
        assert_eq!(loc.writer.as_deref(), Some("blog"));

        // An unowned existing location can be claimed explicitly.
        let free = parquet_location("archive", None);
        let mut r = req("arch");
        r.location = Some("archive".to_string());
        r.access = Some(AccessMode::Writable);
        let (_, loc) = validate_binding(&r, Some(&free), 0).unwrap();
        assert_eq!(loc.writer.as_deref(), Some("arch"));
    }

    #[test]
    fn engine_format_matrix() {
        assert!(engine_reads(
            EngineKind::DuckDb,
            &parquet_location("a", None).format
        ));
        assert!(engine_reads(
            EngineKind::DuckDb,
            &sqlite_location("a", None).format
        ));
        assert!(engine_reads(
            EngineKind::Sqlite,
            &sqlite_location("a", None).format
        ));
        assert!(!engine_reads(
            EngineKind::Sqlite,
            &parquet_location("a", None).format
        ));

        let mut r = req("x");
        r.engine = Some(EngineKind::Sqlite);
        assert!(matches!(
            validate_binding(&r, None, 0),
            Err(CatalogError::IncompatibleFormat { .. })
        ));
    }

    #[test]
    fn storage_other_than_disk_rejected_and_missing_location_errors() {
        let mut r = req("x");
        r.storage = Some("s3".to_string());
        assert!(matches!(
            validate_binding(&r, None, 0),
            Err(CatalogError::InvalidProperty(_))
        ));

        let mut r = req("x");
        r.location = Some("ghost".to_string());
        assert!(matches!(
            validate_binding(&r, None, 0),
            Err(CatalogError::LocationNotFound(l)) if l == "ghost"
        ));
    }

    #[test]
    fn location_serde_is_tagged_by_format() {
        let json = serde_json::to_string(&sqlite_location("o", Some("o"))).unwrap();
        assert!(json.contains("\"format\":\"sqlite_file\""));
        let back: Location = serde_json::from_str(&json).unwrap();
        assert_eq!(back, sqlite_location("o", Some("o")));
    }
}
