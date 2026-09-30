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

//! Make a Space's tables visible to its engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, LocationFormat, Space};
use agoradb_core::{EngineKind, SpaceKind};
use agoradb_engine::{QualifiedName, QueryEngine, TableSource};

use crate::error::SessionError;
use crate::registry::EngineRegistry;

/// Snapshot to use per table when binding an analytical Space; `None`
/// entries (or a missing table) mean "current".
pub type SnapshotPins = BTreeMap<String, Option<i64>>;

/// Attach every table of `space` to its engine and return the engine.
///
/// Attaching is idempotent, so calling this before every statement only
/// costs a registry lookup per table when nothing changed.
pub async fn bind_space(
    catalog: &AgoraCatalog,
    engines: &EngineRegistry,
    space: &Space,
    pins: Option<&SnapshotPins>,
) -> Result<Arc<dyn QueryEngine>, SessionError> {
    let engine = engines.engine_for(space)?;
    if space.kind == SpaceKind::Transactional {
        // The SQLite engine attaches its file when opened.
        return Ok(engine);
    }

    let location = catalog.get_location(&space.location)?;
    match &location.format {
        LocationFormat::IcebergParquet { .. } => {
            for table in catalog.space_tables(space).await? {
                let pin = pins.and_then(|p| p.get(&table).copied().flatten());
                let resolved = catalog.resolve_table(space, &table, pin).await?;
                engine
                    .attach(
                        &QualifiedName::new(&space.name, &table),
                        TableSource::ParquetFiles {
                            files: resolved.files,
                            schema: resolved.schema,
                        },
                    )
                    .await?;
            }
        }
        LocationFormat::SqliteFile { .. } => {
            if space.engine != EngineKind::DuckDb {
                return Err(SessionError::Unsupported(format!(
                    "space '{}' is analytical over a SQLite location but uses engine {}",
                    space.name, space.engine
                )));
            }
            let path = catalog.sqlite_path(&location)?;
            // The writer's SQLite engine knows the table names; open it (read
            // access only) to enumerate them.
            let lister = engines.sqlite_for(space)?;
            for table in lister.table_names(&space.name).await? {
                engine
                    .attach(
                        &QualifiedName::new(&space.name, &table),
                        TableSource::SqliteFile {
                            path: path.clone(),
                            table,
                        },
                    )
                    .await?;
            }
        }
    }
    Ok(engine)
}
