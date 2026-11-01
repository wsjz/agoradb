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

//! Subscription (architecture v3 §5.3): a local copy of another node's Space.
//!
//! `CREATE SPACE <name> WITH PEER = ..., TOKEN = ..., MODE = 'subscribe'`
//! registers a replica Location and copies the owner's current snapshot;
//! `REFRESH SPACE <name>` (or [`spawn_refresher`]) copies newer ones. The
//! copy is read by the local DuckDB, so it stays queryable while the owner
//! is offline.
//!
//! A refresh downloads only the files it does not hold yet, checks each one
//! against the size and BLAKE3 hash the owner announced, and then replaces
//! `manifest.json` with one rename. A failed refresh leaves the previous
//! copy in place. Revoking the token stops further refreshes; files already
//! copied stay on this node until the Space is dropped.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use oikome_catalog::{HeldToken, Location, LocationFormat, OikomeCatalog};
use oikome_core::{CatalogError, CreateSpaceRequest};
use oikome_engine::EngineError;
use oikome_identity::ucan::ABILITY_READ;
use oikome_identity::{unix_now_ms, Did};
use oikome_network::protocol::{
    decode_schema, is_file_hash, is_identifier, is_lance_file_path, FetchFileRequest, FileEntry,
    TableFormat, TableSnapshot,
};
use oikome_network::{ChunkStream, PeerClient};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use crate::error::SessionError;
use crate::result::QueryResult;
use crate::session::OikomeSession;

const MANIFEST_FILE: &str = "manifest.json";
const FILES_DIR: &str = "files";
/// Where a copy of a Lance Space keeps its datasets, `<table>.lance` each.
const LANCE_DIR: &str = "lance";

/// What a replica holds: per table, the owner's snapshot, schema and files.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct ReplicaManifest {
    tables: BTreeMap<String, ReplicaTable>,
    refreshed_at_ms: u64,
    /// A copy of a Lance Space: its datasets are under `lance/`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    lance: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ReplicaTable {
    snapshot_id: Option<i64>,
    /// Arrow schema as encoded by the owner (`protocol::encode_schema`).
    schema: String,
    /// BLAKE3 hashes of the data files (Iceberg).
    files: Vec<String>,
    /// Files of the Lance version held, relative to the dataset directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    paths: Vec<String>,
}

/// One table of a [`RefreshReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshedTable {
    pub table: String,
    /// The owner's snapshot now held; `None` for an empty or restricted table.
    pub snapshot_id: Option<i64>,
    pub files_fetched: u64,
    pub bytes_fetched: u64,
    /// The owner restricts this table (column grant or row policy): it can
    /// be queried remotely but not copied.
    pub restricted: bool,
}

/// What `REFRESH SPACE` did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefreshReport {
    pub space: String,
    pub tables: Vec<RefreshedTable>,
}

impl RefreshReport {
    /// The report as a result set:
    /// `(table, snapshot_id, files_fetched, bytes_fetched, status)`, with
    /// status `copied` or `restricted`.
    pub fn into_result(self) -> Result<QueryResult, SessionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("files_fetched", DataType::Int64, false),
            Field::new("bytes_fetched", DataType::Int64, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        let t = &self.tables;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(t.iter().map(|r| &r.table))),
            Arc::new(Int64Array::from_iter(t.iter().map(|r| r.snapshot_id))),
            Arc::new(Int64Array::from_iter_values(
                t.iter().map(|r| r.files_fetched as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                t.iter().map(|r| r.bytes_fetched as i64),
            )),
            Arc::new(StringArray::from_iter_values(t.iter().map(|r| {
                if r.restricted {
                    "restricted"
                } else {
                    "copied"
                }
            }))),
        ];
        let batch = arrow_array::RecordBatch::try_new(schema.clone(), columns)?;
        Ok(QueryResult::Batches {
            schema,
            batches: vec![batch],
        })
    }
}

fn io(e: std::io::Error) -> SessionError {
    CatalogError::Io(e).into()
}

fn bad_peer(msg: impl Into<String>) -> SessionError {
    EngineError::Remote(msg.into()).into()
}

fn file_path(files_dir: &Path, hash: &str) -> PathBuf {
    files_dir.join(format!("{hash}.parquet"))
}

/// A name no other writer in this process uses, for temporary files.
fn unique_suffix() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// One refresh at a time per replica directory.
fn refresh_lock(dir: &Path) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> =
        LazyLock::new(Default::default);
    LOCKS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(dir.to_path_buf())
        .or_default()
        .clone()
}

fn read_manifest(dir: &Path) -> Result<ReplicaManifest, SessionError> {
    match std::fs::read(dir.join(MANIFEST_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|e| CatalogError::Registry(format!("replica manifest: {e}")).into()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(ReplicaManifest::default()),
        Err(e) => Err(io(e)),
    }
}

async fn write_manifest(dir: &Path, manifest: &ReplicaManifest) -> Result<(), SessionError> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| CatalogError::Registry(format!("replica manifest: {e}")))?;
    let tmp = dir.join(format!(".{MANIFEST_FILE}.tmp-{}", unique_suffix()));
    let written = async {
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, dir.join(MANIFEST_FILE)).await
    }
    .await;
    if written.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    written.map_err(io)
}

/// The directory of datasets of a replica of a Lance Space, or `None` for a
/// replica of an Iceberg Space.
pub(crate) fn replica_lance_root(
    catalog: &OikomeCatalog,
    location: &Location,
) -> Result<Option<PathBuf>, SessionError> {
    let dir = catalog.replica_dir(&location.id);
    Ok(read_manifest(&dir)?.lance.then(|| dir.join(LANCE_DIR)))
}

/// The tables of the replica Location `location`: name, local files and
/// schema, ready to attach to DuckDB.
pub(crate) fn replica_tables(
    catalog: &OikomeCatalog,
    location: &Location,
) -> Result<Vec<(String, Vec<PathBuf>, SchemaRef)>, SessionError> {
    let dir = catalog.replica_dir(&location.id);
    let files_dir = dir.join(FILES_DIR);
    read_manifest(&dir)?
        .tables
        .into_iter()
        .map(|(name, table)| {
            if table.files.iter().any(|h| !is_file_hash(h)) {
                return Err(CatalogError::Registry(format!(
                    "replica manifest of '{}' names an invalid file",
                    location.id
                ))
                .into());
            }
            let files = table
                .files
                .iter()
                .map(|h| file_path(&files_dir, h))
                .collect();
            Ok((name, files, decode_schema(&table.schema)?))
        })
        .collect()
}

/// Write `entry` from `chunks` to `target`, checking its size and hash;
/// nothing is left behind on failure. Returns the bytes written.
async fn download(
    mut chunks: ChunkStream,
    entry: &FileEntry,
    target: &Path,
) -> Result<u64, SessionError> {
    let parent = target
        .parent()
        .ok_or_else(|| bad_peer("a file without a directory"))?;
    let tmp = parent.join(format!(".tmp-{}-{}", entry.hash, unique_suffix()));
    let result = async {
        let mut out = tokio::fs::File::create(&tmp).await.map_err(io)?;
        let mut hasher = blake3::Hasher::new();
        let mut written = 0u64;
        while let Some(chunk) = chunks.try_next().await? {
            written += chunk.len() as u64;
            if written > entry.size {
                return Err(bad_peer(format!(
                    "file {} is larger than the {} bytes announced",
                    entry.hash, entry.size
                )));
            }
            hasher.update(&chunk);
            out.write_all(&chunk).await.map_err(io)?;
        }
        out.sync_all().await.map_err(io)?;
        if written != entry.size {
            return Err(bad_peer(format!(
                "file {} has {written} bytes, {} were announced",
                entry.hash, entry.size
            )));
        }
        if hasher.finalize().to_hex().as_str() != entry.hash {
            return Err(bad_peer(format!(
                "file {} does not match its hash",
                entry.hash
            )));
        }
        tokio::fs::rename(&tmp, target).await.map_err(io)?;
        Ok(written)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Delete every file in `files_dir` that neither `current` nor `previous`
/// references. Keeping the previous copy's files lets queries started
/// before this refresh finish.
async fn remove_unreferenced(
    files_dir: &Path,
    current: &ReplicaManifest,
    previous: &ReplicaManifest,
) -> Result<(), SessionError> {
    let keep: HashSet<String> = [current, previous]
        .iter()
        .flat_map(|m| m.tables.values())
        .flat_map(|t| t.files.iter().map(|h| format!("{h}.parquet")))
        .collect();
    let mut entries = tokio::fs::read_dir(files_dir).await.map_err(io)?;
    while let Some(entry) = entries.next_entry().await.map_err(io)? {
        if !keep.contains(entry.file_name().to_string_lossy().as_ref()) {
            tokio::fs::remove_file(entry.path()).await.map_err(io)?;
        }
    }
    Ok(())
}

/// Copy the owner's current snapshot of `remote` into `dir`.
async fn refresh(
    client: &PeerClient,
    remote: &str,
    dir: &Path,
) -> Result<Vec<RefreshedTable>, SessionError> {
    let snapshot = client.snapshot(remote).await?;
    let files_dir = dir.join(FILES_DIR);
    tokio::fs::create_dir_all(&files_dir).await.map_err(io)?;
    let previous = read_manifest(dir)?;
    let lance = snapshot
        .tables
        .first()
        .is_some_and(|t| t.format == TableFormat::Lance);
    if snapshot
        .tables
        .iter()
        .any(|t| (t.format == TableFormat::Lance) != lance)
        || (!previous.tables.is_empty() && previous.lance != lance)
    {
        return Err(bad_peer("the space changed its storage format"));
    }
    let mut manifest = ReplicaManifest {
        tables: BTreeMap::new(),
        refreshed_at_ms: unix_now_ms(),
        lance: lance || previous.lance,
    };
    let mut report = Vec::new();
    for table in snapshot.tables {
        if !is_identifier(&table.name) || manifest.tables.contains_key(&table.name) {
            return Err(bad_peer(format!("invalid table name '{}'", table.name)));
        }
        decode_schema(&table.schema)?;
        if lance {
            let (copied, paths) = refresh_lance_table(client, remote, dir, &table).await?;
            report.push(copied);
            manifest.tables.insert(
                table.name,
                ReplicaTable {
                    snapshot_id: table.snapshot_id,
                    schema: table.schema,
                    files: Vec::new(),
                    paths,
                },
            );
            continue;
        }
        let (mut fetched, mut bytes) = (0, 0);
        for file in &table.files {
            if !is_file_hash(&file.hash) {
                return Err(bad_peer("invalid file hash in snapshot"));
            }
            if tokio::fs::try_exists(file_path(&files_dir, &file.hash))
                .await
                .map_err(io)?
            {
                continue;
            }
            let snapshot_id = table
                .snapshot_id
                .ok_or_else(|| bad_peer("a table without a snapshot lists files"))?;
            let chunks = client
                .fetch_file(&FetchFileRequest {
                    space: remote.to_string(),
                    table: table.name.clone(),
                    snapshot_id,
                    hash: file.hash.clone(),
                })
                .await?;
            bytes += download(chunks, file, &file_path(&files_dir, &file.hash)).await?;
            fetched += 1;
        }
        report.push(RefreshedTable {
            table: table.name.clone(),
            snapshot_id: table.snapshot_id,
            files_fetched: fetched,
            bytes_fetched: bytes,
            restricted: false,
        });
        manifest.tables.insert(
            table.name,
            ReplicaTable {
                snapshot_id: table.snapshot_id,
                schema: table.schema,
                files: table.files.into_iter().map(|f| f.hash).collect(),
                paths: Vec::new(),
            },
        );
    }
    for name in snapshot.restricted.into_iter().filter(|n| is_identifier(n)) {
        report.push(RefreshedTable {
            table: name,
            snapshot_id: None,
            files_fetched: 0,
            bytes_fetched: 0,
            restricted: true,
        });
    }
    write_manifest(dir, &manifest).await?;
    remove_unreferenced(&files_dir, &manifest, &previous).await?;
    if manifest.lance {
        remove_unreferenced_lance(&dir.join(LANCE_DIR), &manifest, &previous).await?;
    }
    Ok(report)
}

/// Copy one version of a Lance table into `dir/lance/<table>.lance`: data,
/// deletion and index files first, the version's manifest last, so readers
/// see either the previous version or the whole new one. Files already held
/// are not fetched again; Lance never rewrites a file under the same name.
async fn refresh_lance_table(
    client: &PeerClient,
    remote: &str,
    dir: &Path,
    table: &TableSnapshot,
) -> Result<(RefreshedTable, Vec<String>), SessionError> {
    let version = table
        .snapshot_id
        .ok_or_else(|| bad_peer("a Lance table without a version"))?;
    let mut entries: Vec<(&FileEntry, &str)> = Vec::with_capacity(table.files.len());
    for file in &table.files {
        let path = file
            .path
            .as_deref()
            .filter(|p| is_lance_file_path(p) && is_file_hash(&file.hash))
            .ok_or_else(|| bad_peer("invalid file in a Lance version"))?;
        entries.push((file, path));
    }
    let is_manifest = |path: &str| path.starts_with("_versions/");
    if entries.iter().filter(|(_, p)| is_manifest(p)).count() != 1 {
        return Err(bad_peer("a Lance version needs exactly one manifest"));
    }
    entries.sort_by_key(|(_, p)| is_manifest(p));
    let dataset = dir.join(LANCE_DIR).join(format!(
        "{}{}",
        table.name,
        oikome_catalog::lance::DATASET_SUFFIX
    ));
    let (mut fetched, mut bytes) = (0, 0);
    for (file, path) in &entries {
        let target = dataset.join(path);
        if tokio::fs::metadata(&target)
            .await
            .is_ok_and(|m| m.len() == file.size)
        {
            continue;
        }
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(io)?;
        }
        let chunks = client
            .fetch_file(&FetchFileRequest {
                space: remote.to_string(),
                table: table.name.clone(),
                snapshot_id: version,
                hash: file.hash.clone(),
            })
            .await?;
        bytes += download(chunks, file, &target).await?;
        fetched += 1;
    }
    let copied = RefreshedTable {
        table: table.name.clone(),
        snapshot_id: Some(version),
        files_fetched: fetched,
        bytes_fetched: bytes,
        restricted: false,
    };
    Ok((copied, entries.iter().map(|(_, p)| p.to_string()).collect()))
}

/// Delete the datasets and files under `lance_dir` that neither `current`
/// nor `previous` references.
async fn remove_unreferenced_lance(
    lance_dir: &Path,
    current: &ReplicaManifest,
    previous: &ReplicaManifest,
) -> Result<(), SessionError> {
    let suffix = oikome_catalog::lance::DATASET_SUFFIX;
    let mut datasets = match tokio::fs::read_dir(lance_dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io(e)),
    };
    while let Some(entry) = datasets.next_entry().await.map_err(io)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let keep: HashSet<&str> = name
            .strip_suffix(suffix)
            .map(|table| {
                [current, previous]
                    .iter()
                    .filter_map(|m| m.tables.get(table))
                    .flat_map(|t| t.paths.iter().map(String::as_str))
                    .collect()
            })
            .unwrap_or_default();
        if keep.is_empty() {
            tokio::fs::remove_dir_all(entry.path()).await.map_err(io)?;
            continue;
        }
        let dataset = entry.path();
        let mut pending = vec![dataset.clone()];
        while let Some(dir) = pending.pop() {
            let mut files = tokio::fs::read_dir(&dir).await.map_err(io)?;
            while let Some(file) = files.next_entry().await.map_err(io)? {
                let path = file.path();
                if file.file_type().await.map_err(io)?.is_dir() {
                    pending.push(path);
                    continue;
                }
                let relative = path
                    .strip_prefix(&dataset)
                    .map(|p| p.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_default();
                if !keep.contains(relative.as_str()) {
                    tokio::fs::remove_file(&path).await.map_err(io)?;
                }
            }
        }
    }
    Ok(())
}

impl OikomeSession {
    /// `CREATE SPACE <name> WITH PEER = ..., TOKEN = ..., MODE = 'subscribe'`:
    /// register a copy of a Space of another node and take the first copy.
    /// The token must grant this node `space/read`. If the first copy
    /// fails, nothing is registered.
    pub(crate) async fn create_subscribed_space(
        &self,
        request: CreateSpaceRequest,
    ) -> Result<QueryResult, SessionError> {
        let name = request.name.clone();
        let (peer, remote, token) = Self::peer_space_request(request)?;
        let owner = self.peer_space_owner(&token, &remote, ABILITY_READ)?;
        self.engines()
            .peer_endpoint(peer.as_deref(), owner.as_str())?;
        let space = self.catalog.create_replica_space(
            &name,
            peer.as_deref(),
            &remote,
            owner.as_str(),
            HeldToken::new(token),
        )?;
        match self.refresh_space(&name).await {
            Ok(report) => report.into_result(),
            Err(e) => {
                if let Err(undo) = self.catalog.drop_space(&name) {
                    tracing::warn!(space = %name, error = %undo, "could not unregister a failed subscription");
                }
                let _ = tokio::fs::remove_dir_all(self.catalog.replica_dir(&space.location)).await;
                Err(e)
            }
        }
    }

    /// `REFRESH SPACE <name>`: copy the owner's current snapshot of a
    /// subscribed Space, downloading only files not held yet.
    pub async fn refresh_space(&self, name: &str) -> Result<RefreshReport, SessionError> {
        self.deny_principal("REFRESH SPACE")?;
        let space = self.space(name)?;
        let location = self.catalog.get_location(&space.location)?;
        let LocationFormat::Replica {
            peer,
            space: remote,
            owner,
        } = &location.format
        else {
            return Err(SessionError::Unsupported(format!(
                "space '{name}' is not a subscribed space"
            )));
        };
        let token = self.catalog.remote_token(name)?;
        let client = PeerClient::new(
            self.engines().peer_endpoint(peer.as_deref(), owner)?,
            Did::parse(owner)?,
            token.expose().to_string(),
            self.engines().identity()?,
        )
        .allow_http(self.engines().allow_http());
        let dir = self.catalog.replica_dir(&location.id);
        let lock = refresh_lock(&dir);
        let _guard = lock.lock().await;
        let tables = refresh(&client, remote, &dir).await?;
        // A copy of a Lance Space is attached whole; attach it afresh so the
        // new version is read.
        self.engines().duckdb()?.detach_space(name).await?;
        Ok(RefreshReport {
            space: name.to_string(),
            tables,
        })
    }

    /// Remove the files of a dropped subscribed Space.
    pub(crate) async fn remove_replica(&self, location: &str) -> Result<(), SessionError> {
        let dir = self.catalog.replica_dir(location);
        let lock = refresh_lock(&dir);
        let _guard = lock.lock().await;
        match tokio::fs::remove_dir_all(&dir).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(io(e)),
            _ => Ok(()),
        }
    }
}

/// Refresh the subscribed Space `space` every `every` on a background task,
/// starting now. Failures (the owner offline, a revoked token) are logged
/// and retried on the next tick; the previous copy stays queryable. Abort
/// the returned handle to stop.
pub fn spawn_refresher(
    session: Arc<OikomeSession>,
    space: String,
    every: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(e) = session.refresh_space(&space).await {
                tracing::warn!(%space, error = %e, "scheduled refresh failed");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;

    fn entry(data: &[u8]) -> FileEntry {
        FileEntry {
            hash: blake3::hash(data).to_hex().to_string(),
            size: data.len() as u64,
            path: None,
        }
    }

    fn chunks(parts: &[&'static [u8]]) -> ChunkStream {
        Box::pin(stream::iter(
            parts
                .iter()
                .map(|p| Ok(Bytes::from_static(p)))
                .collect::<Vec<_>>(),
        ))
    }

    fn leftovers(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[tokio::test]
    async fn downloads_are_checked_against_size_and_hash() {
        let dir = tempfile::tempdir().unwrap();
        let good = entry(b"parquet bytes");
        let target = file_path(dir.path(), &good.hash);
        let n = download(chunks(&[b"parquet ", b"bytes"]), &good, &target)
            .await
            .unwrap();
        assert_eq!(n, 13);
        assert_eq!(
            std::fs::read(file_path(dir.path(), &good.hash)).unwrap(),
            b"parquet bytes"
        );

        // Same size, other bytes: the hash catches it.
        let tampered = download(chunks(&[b"parquet bytez"]), &good, &target).await;
        assert!(tampered.unwrap_err().to_string().contains("hash"));
        // Truncated, then padded.
        assert!(download(chunks(&[b"parquet"]), &good, &target)
            .await
            .is_err());
        assert!(download(chunks(&[b"parquet bytes!"]), &good, &target)
            .await
            .is_err());
        // A failing stream.
        let failing: ChunkStream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(b"par")),
            Err(EngineError::Remote("connection reset".to_string())),
        ]));
        assert!(download(failing, &good, &target).await.is_err());

        // Only the verified file is left.
        assert_eq!(
            leftovers(dir.path()),
            vec![format!("{}.parquet", good.hash)]
        );
    }

    #[tokio::test]
    async fn unreferenced_files_are_removed_after_one_refresh() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["a", "b", "c"] {
            std::fs::write(dir.path().join(format!("{name}.parquet")), name).unwrap();
        }
        std::fs::write(dir.path().join(".tmp-x-1"), "partial").unwrap();
        let manifest = |files: &[&str]| ReplicaManifest {
            tables: BTreeMap::from([(
                "t".to_string(),
                ReplicaTable {
                    snapshot_id: Some(1),
                    schema: String::new(),
                    files: files.iter().map(|f| f.to_string()).collect(),
                    paths: Vec::new(),
                },
            )]),
            refreshed_at_ms: 0,
            lance: false,
        };
        remove_unreferenced(dir.path(), &manifest(&["c"]), &manifest(&["b"]))
            .await
            .unwrap();
        let mut left = leftovers(dir.path());
        left.sort();
        assert_eq!(left, vec!["b.parquet", "c.parquet"]);
    }

    #[tokio::test]
    async fn lance_files_of_older_versions_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let lance = dir.path().join(LANCE_DIR);
        for path in [
            "items.lance/data/a.lance",
            "items.lance/data/b.lance",
            "items.lance/data/c.lance",
            "items.lance/_versions/3.manifest",
            "items.lance/_versions/2.manifest",
            "items.lance/_versions/1.manifest",
            "items.lance/_indices/u1/index.idx",
            "gone.lance/data/x.lance",
        ] {
            let file = lance.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, path).unwrap();
        }
        let manifest = |paths: &[&str]| ReplicaManifest {
            tables: BTreeMap::from([(
                "items".to_string(),
                ReplicaTable {
                    snapshot_id: Some(1),
                    schema: String::new(),
                    files: Vec::new(),
                    paths: paths.iter().map(|p| p.to_string()).collect(),
                },
            )]),
            refreshed_at_ms: 0,
            lance: true,
        };
        let current = manifest(&[
            "data/b.lance",
            "data/c.lance",
            "_indices/u1/index.idx",
            "_versions/3.manifest",
        ]);
        let previous = manifest(&["data/a.lance", "data/b.lance", "_versions/2.manifest"]);
        remove_unreferenced_lance(&lance, &current, &previous)
            .await
            .unwrap();
        let mut left = Vec::new();
        let mut pending = vec![lance.clone()];
        while let Some(d) = pending.pop() {
            for e in std::fs::read_dir(d).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    pending.push(p);
                } else {
                    left.push(
                        p.strip_prefix(&lance)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
            }
        }
        left.sort();
        assert_eq!(
            left,
            vec![
                "items.lance/_indices/u1/index.idx",
                "items.lance/_versions/2.manifest",
                "items.lance/_versions/3.manifest",
                "items.lance/data/a.lance",
                "items.lance/data/b.lance",
                "items.lance/data/c.lance",
            ]
        );
    }

    #[tokio::test]
    async fn manifests_round_trip_and_default_to_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            read_manifest(dir.path()).unwrap(),
            ReplicaManifest::default()
        );
        let manifest = ReplicaManifest {
            tables: BTreeMap::from([(
                "posts".to_string(),
                ReplicaTable {
                    snapshot_id: Some(42),
                    schema: "s".to_string(),
                    files: vec!["h".to_string()],
                    paths: Vec::new(),
                },
            )]),
            refreshed_at_ms: 7,
            lance: false,
        };
        write_manifest(dir.path(), &manifest).await.unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), manifest);
        assert_eq!(leftovers(dir.path()), vec![MANIFEST_FILE.to_string()]);
    }
}
