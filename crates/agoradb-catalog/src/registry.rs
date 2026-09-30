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

//! Node-level Space / Location registries, persisted as JSON under
//! `<root>/.agora/`. The directory starts with a dot so Iceberg namespace
//! listing ignores it.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agoradb_core::{CatalogError, CreateSpaceRequest, SpaceKind};
use iceberg::{Catalog, NamespaceIdent};
use serde::{Deserialize, Serialize};

use crate::catalog::AgoraCatalog;
use crate::space::{validate_binding, Location, LocationFormat, LocationId, Space};

/// Name of the hidden directory holding registries and SQLite files.
pub const AGORA_DIR: &str = ".agora";
const SPACES_FILE: &str = "spaces.json";
const LOCATIONS_FILE: &str = "locations.json";
const REGISTRY_VERSION: u32 = 1;

/// Namespace property marking a namespace as backing an analytical Space.
pub const NS_PROP_SPACE_KIND: &str = "agora.space.kind";

#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile<T> {
    version: u32,
    items: Vec<T>,
}

/// In-memory copy of both registries.
#[derive(Debug, Default, Clone)]
pub(crate) struct Registry {
    pub(crate) spaces: BTreeMap<String, Space>,
    pub(crate) locations: BTreeMap<LocationId, Location>,
}

fn registry_err(context: &str, e: impl std::fmt::Display) -> CatalogError {
    CatalogError::Registry(format!("{context}: {e}"))
}

fn read_items<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, CatalogError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let file: RegistryFile<T> = serde_json::from_slice(&bytes)
                .map_err(|e| registry_err(&format!("parsing {}", path.display()), e))?;
            if file.version != REGISTRY_VERSION {
                return Err(CatalogError::Registry(format!(
                    "{} has version {}, expected {}",
                    path.display(),
                    file.version,
                    REGISTRY_VERSION
                )));
            }
            Ok(file.items)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn write_items<T: Serialize>(path: &Path, items: Vec<T>) -> Result<(), CatalogError> {
    let file = RegistryFile {
        version: REGISTRY_VERSION,
        items,
    };
    let bytes = serde_json::to_vec_pretty(&file)
        .map_err(|e| registry_err(&format!("serialising {}", path.display()), e))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

impl Registry {
    pub(crate) fn load(root: &Path) -> Result<Self, CatalogError> {
        let dir = root.join(AGORA_DIR);
        let spaces: Vec<Space> = read_items(&dir.join(SPACES_FILE))?;
        let locations: Vec<Location> = read_items(&dir.join(LOCATIONS_FILE))?;
        Ok(Self {
            spaces: spaces.into_iter().map(|s| (s.name.clone(), s)).collect(),
            locations: locations.into_iter().map(|l| (l.id.clone(), l)).collect(),
        })
    }

    pub(crate) fn save(&self, root: &Path) -> Result<(), CatalogError> {
        let dir = root.join(AGORA_DIR);
        std::fs::create_dir_all(&dir)?;
        write_items(
            &dir.join(SPACES_FILE),
            self.spaces.values().cloned().collect(),
        )?;
        write_items(
            &dir.join(LOCATIONS_FILE),
            self.locations.values().cloned().collect(),
        )?;
        Ok(())
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl AgoraCatalog {
    /// The hidden directory holding registries and SQLite files.
    pub fn agora_dir(&self) -> PathBuf {
        Path::new(self.root_path()).join(AGORA_DIR)
    }

    fn with_registry<T>(&self, f: impl FnOnce(&Registry) -> T) -> T {
        let guard = self.registry.read().unwrap_or_else(|p| p.into_inner());
        f(&guard)
    }

    /// Create a Space (and, unless `LOCATION` names an existing one, its Location).
    ///
    /// Analytical Spaces get an Iceberg namespace of the same name; the
    /// database file of a transactional Space is created by its engine on
    /// first open.
    pub async fn create_space(&self, request: CreateSpaceRequest) -> Result<Space, CatalogError> {
        let (space, location, is_new_location) = self.with_registry(|reg| {
            if reg.spaces.contains_key(&request.name) {
                return Err(CatalogError::SpaceExists(request.name.clone()));
            }
            let existing = match &request.location {
                Some(id) => reg.locations.get(id),
                None => None,
            };
            if request.location.is_none() && reg.locations.contains_key(&request.name) {
                return Err(CatalogError::Registry(format!(
                    "a location named '{}' already exists; bind it with LOCATION = '{}'",
                    request.name, request.name
                )));
            }
            let (space, location) = validate_binding(&request, existing, now_ms())?;
            Ok((space, location, existing.is_none()))
        })?;

        if is_new_location {
            match &location.format {
                LocationFormat::IcebergParquet { namespace } => {
                    let ns = NamespaceIdent::new(namespace.clone());
                    let mut props = HashMap::new();
                    props.insert(
                        NS_PROP_SPACE_KIND.to_string(),
                        SpaceKind::Analytical.to_string(),
                    );
                    self.create_namespace(&ns, props)
                        .await
                        .map_err(|e| CatalogError::Iceberg(e.to_string()))?;
                }
                LocationFormat::SqliteFile { path } => {
                    if let Some(parent) = Path::new(self.root_path()).join(path).parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
            }
        }

        let mut guard = self.registry.write().unwrap_or_else(|p| p.into_inner());
        guard.spaces.insert(space.name.clone(), space.clone());
        guard.locations.insert(location.id.clone(), location);
        guard.save(Path::new(self.root_path()))?;
        Ok(space)
    }

    /// Look up a Space by name.
    pub fn get_space(&self, name: &str) -> Result<Space, CatalogError> {
        self.with_registry(|reg| {
            reg.spaces
                .get(name)
                .cloned()
                .ok_or_else(|| CatalogError::SpaceNotFound(name.to_string()))
        })
    }

    /// All Spaces, ordered by name.
    pub fn list_spaces(&self) -> Vec<Space> {
        self.with_registry(|reg| reg.spaces.values().cloned().collect())
    }

    /// Remove a Space from the registry.
    ///
    /// Data is left in place: the Iceberg namespace or SQLite file stays on
    /// disk and the Location remains registered (with its writer cleared if
    /// this Space owned it) so it can be re-bound later.
    pub fn drop_space(&self, name: &str) -> Result<(), CatalogError> {
        let mut guard = self.registry.write().unwrap_or_else(|p| p.into_inner());
        let space = guard
            .spaces
            .remove(name)
            .ok_or_else(|| CatalogError::SpaceNotFound(name.to_string()))?;
        if let Some(location) = guard.locations.get_mut(&space.location) {
            if location.writer.as_deref() == Some(name) {
                location.writer = None;
            }
        }
        guard.save(Path::new(self.root_path()))
    }

    /// Look up a Location by id.
    pub fn get_location(&self, id: &str) -> Result<Location, CatalogError> {
        self.with_registry(|reg| {
            reg.locations
                .get(id)
                .cloned()
                .ok_or_else(|| CatalogError::LocationNotFound(id.to_string()))
        })
    }

    /// All Locations, ordered by id.
    pub fn list_locations(&self) -> Vec<Location> {
        self.with_registry(|reg| reg.locations.values().cloned().collect())
    }

    /// The Iceberg namespace backing an analytical Space.
    pub fn space_namespace(&self, space: &Space) -> Result<NamespaceIdent, CatalogError> {
        match self.get_location(&space.location)?.format {
            LocationFormat::IcebergParquet { namespace } => Ok(NamespaceIdent::new(namespace)),
            other => Err(CatalogError::KindFormatMismatch {
                kind: space.kind.to_string(),
                format: other.name().to_string(),
            }),
        }
    }

    /// Absolute path of a SQLite Location's database file.
    pub fn sqlite_path(&self, location: &Location) -> Result<PathBuf, CatalogError> {
        match &location.format {
            LocationFormat::SqliteFile { path } => Ok(Path::new(self.root_path()).join(path)),
            other => Err(CatalogError::KindFormatMismatch {
                kind: "sqlite".to_string(),
                format: other.name().to_string(),
            }),
        }
    }
}
