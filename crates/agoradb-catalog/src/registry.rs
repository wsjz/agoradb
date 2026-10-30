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
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agoradb_core::{CatalogError, CreateSpaceRequest, SpaceKind};
use iceberg::{Catalog, NamespaceIdent};
use serde::{Deserialize, Serialize};

use crate::acl::{Grant, RowPolicy, ViewDef};
use crate::catalog::AgoraCatalog;
use crate::space::{validate_binding, Location, LocationFormat, LocationId, Space};
use crate::ucan::{IssuedUcan, RemoteToken, Revocation};

/// Name of the hidden directory holding registries and SQLite files.
pub const AGORA_DIR: &str = ".agora";
const SPACES_FILE: &str = "spaces.json";
const LOCATIONS_FILE: &str = "locations.json";
const VIEWS_FILE: &str = "views.json";
const GRANTS_FILE: &str = "grants.json";
const POLICIES_FILE: &str = "policies.json";
const UCANS_FILE: &str = "ucans.json";
const REVOCATIONS_FILE: &str = "revocations.json";
/// Held UCANs live apart from the other registries, readable by the owner only.
const IDENTITY_DIR: &str = "identity";
const REMOTE_TOKENS_FILE: &str = "remote_tokens.json";
const REGISTRY_VERSION: u32 = 1;

/// Namespace property marking a namespace as backing an analytical Space.
pub const NS_PROP_SPACE_KIND: &str = "agora.space.kind";

#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile<T> {
    version: u32,
    items: Vec<T>,
}

/// In-memory copy of all node-level registries.
#[derive(Debug, Default, Clone)]
pub(crate) struct Registry {
    pub(crate) spaces: BTreeMap<String, Space>,
    pub(crate) locations: BTreeMap<LocationId, Location>,
    /// Keyed by `(space, view)`.
    pub(crate) views: BTreeMap<(String, String), ViewDef>,
    /// Keyed by `(principal, space, relation)`.
    pub(crate) grants: BTreeMap<(String, String, String), Grant>,
    /// Keyed by `(space, relation, policy name)`.
    pub(crate) policies: BTreeMap<(String, String, String), RowPolicy>,
    /// UCANs this node issued, keyed by token id.
    pub(crate) issued: BTreeMap<String, IssuedUcan>,
    /// Revoked token ids.
    pub(crate) revoked: BTreeMap<String, Revocation>,
    /// UCANs held for remote Spaces, keyed by Space name.
    pub(crate) tokens: BTreeMap<String, RemoteToken>,
}

fn registry_err(context: &str, e: impl std::fmt::Display) -> CatalogError {
    CatalogError::Registry(format!("{context}: {e}"))
}

/// Read a registry file, feeding its bytes (or their absence) to `hasher`.
fn read_items<T: for<'de> Deserialize<'de>>(
    path: &Path,
    hasher: &mut impl Hasher,
) -> Result<Vec<T>, CatalogError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            bytes.hash(hasher);
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
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            None::<()>.hash(hasher);
            Ok(Vec::new())
        }
        Err(e) => Err(e.into()),
    }
}

fn serialise<T: Serialize>(path: &Path, items: Vec<T>) -> Result<Vec<u8>, CatalogError> {
    let file = RegistryFile {
        version: REGISTRY_VERSION,
        items,
    };
    serde_json::to_vec_pretty(&file)
        .map_err(|e| registry_err(&format!("serialising {}", path.display()), e))
}

fn write_items<T: Serialize>(path: &Path, items: Vec<T>) -> Result<(), CatalogError> {
    let bytes = serialise(path, items)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Like [`write_items`], but the file is created readable by the owner only.
fn write_items_private<T: Serialize>(path: &Path, items: Vec<T>) -> Result<(), CatalogError> {
    let bytes = serialise(path, items)?;
    let tmp = path.with_extension("json.tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

impl Registry {
    /// Load every registry file, returning a fingerprint of their contents
    /// so later changes (by another process) can be detected.
    pub(crate) fn load(root: &Path) -> Result<(Self, u64), CatalogError> {
        let dir = root.join(AGORA_DIR);
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let spaces: Vec<Space> = read_items(&dir.join(SPACES_FILE), &mut h)?;
        let locations: Vec<Location> = read_items(&dir.join(LOCATIONS_FILE), &mut h)?;
        let views: Vec<ViewDef> = read_items(&dir.join(VIEWS_FILE), &mut h)?;
        let grants: Vec<Grant> = read_items(&dir.join(GRANTS_FILE), &mut h)?;
        let policies: Vec<RowPolicy> = read_items(&dir.join(POLICIES_FILE), &mut h)?;
        let issued: Vec<IssuedUcan> = read_items(&dir.join(UCANS_FILE), &mut h)?;
        let revoked: Vec<Revocation> = read_items(&dir.join(REVOCATIONS_FILE), &mut h)?;
        let tokens: Vec<RemoteToken> =
            read_items(&dir.join(IDENTITY_DIR).join(REMOTE_TOKENS_FILE), &mut h)?;
        let registry = Self {
            spaces: spaces.into_iter().map(|s| (s.name.clone(), s)).collect(),
            locations: locations.into_iter().map(|l| (l.id.clone(), l)).collect(),
            views: views.into_iter().map(|v| (v.key(), v)).collect(),
            grants: grants.into_iter().map(|g| (g.key(), g)).collect(),
            policies: policies.into_iter().map(|p| (p.key(), p)).collect(),
            issued: issued.into_iter().map(|u| (u.id.clone(), u)).collect(),
            revoked: revoked.into_iter().map(|r| (r.id.clone(), r)).collect(),
            tokens: tokens.into_iter().map(|t| (t.space.clone(), t)).collect(),
        };
        Ok((registry, h.finish()))
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
        write_items(
            &dir.join(VIEWS_FILE),
            self.views.values().cloned().collect(),
        )?;
        write_items(
            &dir.join(GRANTS_FILE),
            self.grants.values().cloned().collect(),
        )?;
        write_items(
            &dir.join(POLICIES_FILE),
            self.policies.values().cloned().collect(),
        )?;
        write_items(
            &dir.join(UCANS_FILE),
            self.issued.values().cloned().collect(),
        )?;
        write_items(
            &dir.join(REVOCATIONS_FILE),
            self.revoked.values().cloned().collect(),
        )?;
        if !self.tokens.is_empty() || dir.join(IDENTITY_DIR).join(REMOTE_TOKENS_FILE).exists() {
            let identity = dir.join(IDENTITY_DIR);
            std::fs::create_dir_all(&identity)?;
            write_items_private(
                &identity.join(REMOTE_TOKENS_FILE),
                self.tokens.values().cloned().collect(),
            )?;
        }
        Ok(())
    }
}

pub(crate) fn now_ms() -> u64 {
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

    pub(crate) fn with_registry<T>(&self, f: impl FnOnce(&Registry) -> T) -> T {
        let guard = self.registry.read().unwrap_or_else(|p| p.into_inner());
        f(&guard)
    }

    /// Apply `f` to the registry and persist it if `f` succeeds.
    pub(crate) fn mutate_registry<T>(
        &self,
        f: impl FnOnce(&mut Registry) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        let mut guard = self.registry.write().unwrap_or_else(|p| p.into_inner());
        let mut next = guard.clone();
        let out = f(&mut next)?;
        next.save(Path::new(self.root_path()))?;
        *guard = next;
        // Unknown until re-read: the next refresh() reloads from disk.
        *self.registry_fp.lock().unwrap_or_else(|p| p.into_inner()) = None;
        Ok(out)
    }

    /// Reload the registries if their files changed since they were last
    /// read, e.g. because `agoradb sql` ran a `GRANT` or `REVOKE UCAN` in
    /// another process. Returns whether anything was reloaded.
    ///
    /// Long-running servers call this before authorizing a request, so a
    /// revocation takes effect on the next call.
    pub fn refresh(&self) -> Result<bool, CatalogError> {
        let (fresh, fingerprint) = Registry::load(Path::new(self.root_path()))?;
        let mut fp = self.registry_fp.lock().unwrap_or_else(|p| p.into_inner());
        if *fp == Some(fingerprint) {
            return Ok(false);
        }
        *self.registry.write().unwrap_or_else(|p| p.into_inner()) = fresh;
        *fp = Some(fingerprint);
        Ok(true)
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
                // validate_binding never creates these (see create_peer_space).
                LocationFormat::Remote { .. } | LocationFormat::Replica { .. } => {}
            }
        }

        self.mutate_registry(|reg| {
            reg.spaces.insert(space.name.clone(), space.clone());
            reg.locations.insert(location.id.clone(), location);
            Ok(())
        })?;
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

    /// Remove a Space from the registry, together with its views, grants and
    /// row policies.
    ///
    /// Data is left in place: the Iceberg namespace or SQLite file stays on
    /// disk and the Location remains registered (with its writer cleared if
    /// this Space owned it) so it can be re-bound later.
    pub fn drop_space(&self, name: &str) -> Result<(), CatalogError> {
        self.mutate_registry(|reg| {
            let space = reg
                .spaces
                .remove(name)
                .ok_or_else(|| CatalogError::SpaceNotFound(name.to_string()))?;
            if let Some(location) = reg.locations.get_mut(&space.location) {
                if location.writer.as_deref() == Some(name) {
                    location.writer = None;
                }
            }
            reg.views.retain(|(s, _), _| s != name);
            reg.grants.retain(|(_, s, _), _| s != name);
            reg.policies.retain(|(s, _, _), _| s != name);
            // Another node's Space: forget its Location with the token. The
            // caller removes a replica's files (see `replica_dir`).
            if reg
                .locations
                .get(&space.location)
                .is_some_and(|l| l.format.is_peer_space())
            {
                reg.locations.remove(&space.location);
            }
            reg.tokens.remove(name);
            Ok(())
        })
    }

    /// Mark `target` as the publication Space of `source`: from now on it is
    /// only written by `PUBLISH SPACE source`.
    pub fn mark_published(&self, target: &str, source: &str) -> Result<Space, CatalogError> {
        self.mutate_registry(|reg| {
            let space = reg
                .spaces
                .get_mut(target)
                .ok_or_else(|| CatalogError::SpaceNotFound(target.to_string()))?;
            space.published_from = Some(source.to_string());
            Ok(space.clone())
        })
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
