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

//! Space/Location registry persistence and snapshot resolution.

use std::sync::Arc;

use agoradb_catalog::{AgoraCatalog, LocationFormat};
use agoradb_core::{AccessMode, CatalogError, CreateSpaceRequest, EngineKind, SpaceKind};
use agoradb_storage::StorageEngine;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, TableCreation, TableIdent};

fn open(root: &str) -> AgoraCatalog {
    AgoraCatalog::open(FileIO::new_with_fs(), root).unwrap()
}

#[tokio::test]
async fn create_space_persists_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let catalog = open(&root);

    let blog = catalog
        .create_space(CreateSpaceRequest::new("blog"))
        .await
        .unwrap();
    assert_eq!(blog.kind, SpaceKind::Analytical);
    assert_eq!(blog.engine, EngineKind::DuckDb);

    let mut req = CreateSpaceRequest::new("orders");
    req.kind = Some(SpaceKind::Transactional);
    let orders = catalog.create_space(req).await.unwrap();
    assert_eq!(orders.engine, EngineKind::Sqlite);
    let loc = catalog.get_location(&orders.location).unwrap();
    assert!(matches!(loc.format, LocationFormat::SqliteFile { .. }));
    assert!(catalog
        .sqlite_path(&loc)
        .unwrap()
        .starts_with(dir.path().join(".agora")));

    // Duplicate names are rejected.
    assert!(matches!(
        catalog.create_space(CreateSpaceRequest::new("blog")).await,
        Err(CatalogError::SpaceExists(_))
    ));

    // The analytical Space has an Iceberg namespace; the hidden dir is not listed.
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    let names: Vec<String> = namespaces.iter().map(|n| n.to_url_string()).collect();
    assert!(names.contains(&"blog".to_string()));
    assert!(!names.iter().any(|n| n.starts_with('.')));
    assert!(dir.path().join(".agora/spaces.json").exists());
    assert!(!dir.path().join(".agora/spaces.json.tmp").exists());

    // A fresh catalog sees the same registry.
    let reopened = open(&root);
    let spaces = reopened.list_spaces();
    assert_eq!(
        spaces.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
        vec!["blog", "orders"]
    );
    assert_eq!(reopened.get_space("orders").unwrap(), orders);
    assert!(matches!(
        reopened.get_space("nope"),
        Err(CatalogError::SpaceNotFound(_))
    ));
}

#[tokio::test]
async fn second_binding_over_same_location() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = open(dir.path().to_str().unwrap());
    let mut req = CreateSpaceRequest::new("orders");
    req.kind = Some(SpaceKind::Transactional);
    catalog.create_space(req).await.unwrap();

    let mut ro = CreateSpaceRequest::new("orders_ro");
    ro.location = Some("orders".to_string());
    let ro = catalog.create_space(ro).await.unwrap();
    assert_eq!(ro.kind, SpaceKind::Analytical);
    assert_eq!(ro.engine, EngineKind::DuckDb);
    assert_eq!(ro.access, AccessMode::ReadOnly);
    assert_eq!(ro.location, "orders");
    assert_eq!(
        catalog.get_location("orders").unwrap().writer.as_deref(),
        Some("orders")
    );

    // Dropping the writer frees the location but keeps it registered.
    catalog.drop_space("orders").unwrap();
    assert!(catalog.get_location("orders").unwrap().writer.is_none());
    assert_eq!(catalog.list_spaces().len(), 1);

    let reopened = open(dir.path().to_str().unwrap());
    assert!(reopened.get_location("orders").unwrap().writer.is_none());
}

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

async fn write_rows(
    catalog: &Arc<AgoraCatalog>,
    ident: &TableIdent,
    ids: Vec<i64>,
    tmp: &std::path::Path,
) {
    let mut engine = StorageEngine::new_with_catalog(
        catalog.clone(),
        FileIO::new_with_fs(),
        ident.clone(),
        arrow_schema(),
        tmp.to_path_buf(),
    );
    let batch = RecordBatch::try_new(
        arrow_schema(),
        vec![Arc::new(Int64Array::from(ids)) as ArrayRef],
    )
    .unwrap();
    engine.append(batch).await.unwrap();
    engine.flush().await.unwrap();
}

#[tokio::test]
async fn resolve_table_follows_snapshots_and_pins() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(open(dir.path().to_str().unwrap()));
    let space = catalog
        .create_space(CreateSpaceRequest::new("blog"))
        .await
        .unwrap();

    let ns = catalog.space_namespace(&space).unwrap();
    let iceberg_schema = IcebergSchema::builder()
        .with_fields(vec![NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        )
        .into()])
        .build()
        .unwrap();
    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("posts".to_string())
                .schema(iceberg_schema)
                .build(),
        )
        .await
        .unwrap();
    let ident = TableIdent::new(ns.clone(), "posts".to_string());

    // 0 files, no snapshot.
    let resolved = catalog.resolve_table(&space, "posts", None).await.unwrap();
    assert_eq!(resolved.snapshot_id, None);
    assert!(resolved.files.is_empty());
    assert_eq!(resolved.schema.field(0).data_type(), &DataType::Int64);
    assert!(
        resolved.schema.field(0).metadata().is_empty(),
        "field ids stripped"
    );

    // 1 file.
    write_rows(&catalog, &ident, vec![1, 2], dir.path()).await;
    let first = catalog.resolve_table(&space, "posts", None).await.unwrap();
    assert_eq!(first.files.len(), 1);
    assert!(first.files[0].is_absolute());
    assert!(first.files[0].starts_with(dir.path().join("blog/posts/data")));
    let first_snapshot = first.snapshot_id.unwrap();

    // 3 files.
    write_rows(&catalog, &ident, vec![3], dir.path()).await;
    write_rows(&catalog, &ident, vec![4], dir.path()).await;
    let latest = catalog.resolve_table(&space, "posts", None).await.unwrap();
    assert_eq!(latest.files.len(), 3);
    assert_ne!(latest.snapshot_id, Some(first_snapshot));

    // Pinning the first snapshot yields the first file only.
    let pinned = catalog
        .resolve_table(&space, "posts", Some(first_snapshot))
        .await
        .unwrap();
    assert_eq!(pinned.files, first.files);

    assert_eq!(catalog.space_tables(&space).await.unwrap(), vec!["posts"]);
    let ids = catalog.current_snapshot_ids(&space).await.unwrap();
    assert_eq!(ids.get("posts").copied().flatten(), latest.snapshot_id);

    assert!(matches!(
        catalog.resolve_table(&space, "missing", None).await,
        Err(CatalogError::TableNotFound(_))
    ));
}

#[tokio::test]
async fn views_grants_and_policies_persist_and_cascade() {
    use agoradb_catalog::{Grant, RowPolicy, ViewDef};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let catalog = open(&root);
    catalog
        .create_space(CreateSpaceRequest::new("blog"))
        .await
        .unwrap();

    catalog
        .create_view(
            ViewDef::new("blog", "recent", "SELECT id FROM blog.posts"),
            false,
        )
        .unwrap();
    assert!(matches!(
        catalog.create_view(ViewDef::new("blog", "recent", "SELECT 1"), false),
        Err(CatalogError::ViewExists(_))
    ));
    catalog
        .create_view(ViewDef::new("blog", "recent", "SELECT 2"), true)
        .unwrap();
    assert!(matches!(
        catalog.create_view(ViewDef::new("ghost", "v", "SELECT 1"), false),
        Err(CatalogError::SpaceNotFound(_))
    ));
    catalog
        .grant_select(Grant {
            principal: "alice".into(),
            space: "blog".into(),
            relation: "recent".into(),
            columns: Some(vec!["id".into()]),
        })
        .unwrap();
    catalog
        .create_policy(RowPolicy {
            name: "p".into(),
            space: "blog".into(),
            relation: "recent".into(),
            principals: vec!["alice".into()],
            predicate: "id > 1".into(),
        })
        .unwrap();

    let reopened = open(&root);
    assert_eq!(
        reopened.get_view("blog", "recent").unwrap().query,
        "SELECT 2"
    );
    assert_eq!(
        reopened
            .get_grant("alice", "blog", "recent")
            .unwrap()
            .columns,
        Some(vec!["id".to_string()])
    );
    assert!(reopened.has_grants_in("alice", "blog"));
    assert_eq!(reopened.policies_on("blog", "recent").len(), 1);

    // Dropping the view removes its grants and policies.
    reopened.drop_view("blog", "recent").unwrap();
    assert!(reopened.get_grant("alice", "blog", "recent").is_none());
    assert!(reopened.policies_on("blog", "recent").is_empty());
    assert!(!open(&root).has_grants_in("alice", "blog"));
}
