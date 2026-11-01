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

//! Lance Locations (architecture v3 §7.6): the datasets of a Location and
//! the files of one version of a dataset.
//!
//! DuckDB's `lance` extension reads and writes the datasets but cannot list
//! a version's files, which subscription needs to ship exactly one version
//! (§9): its manifest, data files, deletion files and indexes. Those are
//! read here from the version's manifest with `lance-table`, so files of
//! other versions, such as rows deleted since, are never shipped.

use std::path::Path;

use arrow_schema::SchemaRef;
use oikome_core::CatalogError;

/// Suffix of a dataset directory: table `t` is `t.lance`.
pub const DATASET_SUFFIX: &str = ".lance";

/// Tables of the Lance Location at `root`: one per `<table>.lance`
/// directory whose name is a plain identifier, sorted.
pub fn lance_tables(root: &Path) -> Result<Vec<String>, CatalogError> {
    let mut tables = Vec::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(tables),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(table) = name.to_str().and_then(|n| n.strip_suffix(DATASET_SUFFIX)) else {
            continue;
        };
        let plain = table
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if plain && entry.file_type()?.is_dir() {
            tables.push(table.to_string());
        }
    }
    tables.sort();
    Ok(tables)
}

/// One version of a Lance dataset and its files.
#[derive(Debug, Clone, PartialEq)]
pub struct LanceVersion {
    pub version: u64,
    pub schema: SchemaRef,
    /// The version's manifest, relative to the dataset directory.
    pub manifest: String,
    /// Data, deletion and index files the manifest references, relative to
    /// the dataset directory, sorted.
    pub files: Vec<String>,
}

#[cfg(feature = "lance")]
mod read {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_schema::Schema;
    use lance_io::object_store::ObjectStore;
    use lance_table::io::commit::commit_handler_from_url;
    use lance_table::io::deletion::relative_deletion_file_path;
    use lance_table::io::manifest::{read_manifest, read_manifest_indexes};
    use oikome_core::CatalogError;

    use super::LanceVersion;

    fn lance_err(e: impl std::fmt::Display) -> CatalogError {
        CatalogError::Registry(format!("lance dataset: {e}"))
    }

    fn unsupported(what: &str) -> CatalogError {
        CatalogError::Registry(format!(
            "lance dataset uses {what}, which Oikome cannot copy yet"
        ))
    }

    /// Every file under `dir`, relative to `base`.
    fn walk(dir: &Path, base: &Path, out: &mut BTreeSet<String>) -> Result<(), CatalogError> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                walk(&path, base, out)?;
            } else if let Ok(relative) = path.strip_prefix(base) {
                out.insert(relative.to_string_lossy().replace('\\', "/"));
            }
        }
        Ok(())
    }

    /// The files of `version` (default: the latest) of the dataset at
    /// `dataset`, a local directory.
    pub async fn lance_version(
        dataset: &Path,
        version: Option<u64>,
    ) -> Result<LanceVersion, CatalogError> {
        let uri = dataset
            .to_str()
            .ok_or_else(|| lance_err("the dataset path is not valid UTF-8"))?;
        let (store, base) = ObjectStore::from_uri(uri).await.map_err(lance_err)?;
        let handler = commit_handler_from_url(uri, &None)
            .await
            .map_err(lance_err)?;
        let location = match version {
            Some(v) => {
                handler
                    .resolve_version_location(&base, v, store.inner.as_ref())
                    .await
            }
            None => handler.resolve_latest_location(&base, &store).await,
        }
        .map_err(lance_err)?;
        let manifest = read_manifest(&store, &location.path, location.size)
            .await
            .map_err(lance_err)?;
        let indexes = read_manifest_indexes(&store, &location, &manifest)
            .await
            .map_err(lance_err)?;
        let relative = |path: &str| {
            path.strip_prefix(base.as_ref())
                .map(|p| p.trim_start_matches('/').to_string())
                .ok_or_else(|| lance_err(format!("{path} is outside the dataset")))
        };
        let mut files = BTreeSet::new();
        for fragment in manifest.fragments.iter() {
            if !fragment.overlays.is_empty() {
                return Err(unsupported("data overlays"));
            }
            for file in &fragment.files {
                if file.base_id.is_some() {
                    return Err(unsupported("data files in another dataset"));
                }
                files.insert(format!("data/{}", file.path));
            }
            if let Some(deletion) = &fragment.deletion_file {
                if deletion.base_id.is_some() {
                    return Err(unsupported("deletion files in another dataset"));
                }
                files.insert(relative_deletion_file_path(fragment.id, deletion));
            }
        }
        for index in &indexes {
            let dir = dataset.join("_indices").join(index.uuid.to_string());
            if !dir.is_dir() {
                return Err(unsupported("indexes stored outside the dataset"));
            }
            walk(&dir, dataset, &mut files)?;
        }
        Ok(LanceVersion {
            version: manifest.version,
            schema: Arc::new(Schema::from(&manifest.schema)),
            manifest: relative(location.path.as_ref())?,
            files: files.into_iter().collect(),
        })
    }
}

#[cfg(feature = "lance")]
pub use read::lance_version;
