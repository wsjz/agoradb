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

//! Property graphs on Lance Spaces, queried with SQL/PGQ (DuckPGQ,
//! architecture v3 §8).
//!
//! Tests marked `#[ignore]` need the `lance` and `duckpgq` extensions; they
//! download them once into the target directory
//! (`cargo test -p oikome-node --test graph -- --ignored`).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use iceberg::io::FileIO;
use oikome_catalog::OikomeCatalog;
use oikome_engine_duckdb::DuckDbConfig;
use oikome_node::{
    EngineRegistry, NodeConfig, OikomeSession, QueryResult, SessionConfig, SessionError,
};

fn extension_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("duckdb-extensions")
}

fn session(catalog: &Arc<OikomeCatalog>, principal: Option<&str>) -> OikomeSession {
    let engines = Arc::new(
        EngineRegistry::new(
            catalog.clone(),
            NodeConfig {
                duckdb: DuckDbConfig {
                    extension_dir: Some(extension_dir()),
                    allow_extension_install: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap(),
    );
    OikomeSession::new(
        catalog.clone(),
        engines,
        SessionConfig {
            principal: principal.map(str::to_string),
            default_space: None,
        },
    )
}

async fn sql(s: &OikomeSession, sql: &str) -> QueryResult {
    s.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn err(s: &OikomeSession, sql: &str) -> SessionError {
    match s.sql(sql).await {
        Ok(_) => panic!("{sql}: expected an error"),
        Err(e) => e,
    }
}

fn strings(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let a = b
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ints(result: &QueryResult) -> Vec<i64> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

fn catalog() -> (tempfile::TempDir, Arc<OikomeCatalog>) {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(OikomeCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    (dir, catalog)
}

const FRIENDS: &str = "CREATE PROPERTY GRAPH social.friends \
     VERTEX TABLES (people LABEL person) \
     EDGE TABLES (knows SOURCE KEY (src) REFERENCES people (id) \
                  DESTINATION KEY (dst) REFERENCES people (id) LABEL knows)";

const EDGES: &str = "FROM GRAPH_TABLE (social.friends MATCH (a:person)-[k:knows]->(b:person) \
     COLUMNS (a.name AS a, b.name AS b)) ORDER BY a, b";

#[tokio::test]
async fn graphs_belong_to_lance_spaces_and_their_owner() {
    let (_dir, catalog) = catalog();
    let owner = session(&catalog, None);
    sql(&owner, "CREATE SPACE ice").await;
    sql(&owner, "CREATE TABLE ice.people (id BIGINT)").await;
    let e = err(&owner, "CREATE PROPERTY GRAPH ice.g VERTEX TABLES (people)")
        .await
        .to_string();
    assert!(e.contains("Lance"), "{e}");
    let e = err(&owner, "FROM GRAPH_TABLE (ice.g MATCH (a) COLUMNS (a.id))")
        .await
        .to_string();
    assert!(e.contains("Lance"), "{e}");

    let peer = session(
        &catalog,
        Some("did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw"),
    );
    for sql in [
        "CREATE PROPERTY GRAPH ice.g VERTEX TABLES (people)",
        "FROM GRAPH_TABLE (ice.g MATCH (a) COLUMNS (a.id))",
    ] {
        assert!(
            matches!(err(&peer, sql).await, SessionError::PermissionDenied(_)),
            "{sql}"
        );
    }
}

#[tokio::test]
#[ignore = "downloads the DuckDB lance and duckpgq extensions"]
async fn graphs_are_queried_and_survive_a_new_duckdb() {
    let (_dir, catalog) = catalog();
    let owner = session(&catalog, None);
    for statement in [
        "CREATE SPACE social WITH FORMAT = 'lance'",
        "CREATE TABLE social.people (id BIGINT, name VARCHAR)",
        "INSERT INTO social.people VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')",
        "CREATE TABLE social.knows (src BIGINT, dst BIGINT)",
        "INSERT INTO social.knows VALUES (1, 2), (2, 3), (3, 4), (1, 3)",
        "CREATE SPACE other WITH FORMAT = 'lance'",
        "CREATE TABLE other.pets (id BIGINT)",
        FRIENDS,
    ] {
        sql(&owner, statement).await;
    }
    let edges = sql(&owner, EDGES).await;
    assert_eq!(strings(&edges, 0), vec!["alice", "alice", "bob", "carol"]);
    assert_eq!(strings(&edges, 1), vec!["bob", "carol", "carol", "dave"]);
    let hops = sql(
        &owner,
        "FROM GRAPH_TABLE (social.friends MATCH p = ANY SHORTEST \
         (a:person WHERE a.name = 'alice')-[k:knows]->+(b:person WHERE b.name = 'dave') \
         COLUMNS (path_length(p) AS hops))",
    )
    .await;
    assert_eq!(ints(&hops), vec![2]);

    // A DuckDB that has never seen the graph (a restart, another process)
    // creates it from the catalog; it reads the tables as they are now.
    sql(&owner, "INSERT INTO social.knows VALUES (4, 1)").await;
    let fresh = session(&catalog, None);
    assert_eq!(sql(&fresh, EDGES).await.num_rows(), 5);

    for (statement, needle) in [
        (FRIENDS, "already exists"),
        (
            "CREATE PROPERTY GRAPH social.g VERTEX TABLES (other.pets)",
            "belong to its space",
        ),
        (
            "CREATE PROPERTY GRAPH social.g VERTEX TABLES (cats)",
            "cats",
        ),
        (
            "FROM GRAPH_TABLE (social.friends MATCH (a:person) COLUMNS (a.id)) g \
             JOIN other.pets p ON p.id = g.id",
            "one space",
        ),
    ] {
        let e = err(&owner, statement).await.to_string();
        assert!(e.contains(needle), "{statement}: {e}");
    }
    sql(
        &owner,
        &FRIENDS.replace(
            "CREATE PROPERTY GRAPH",
            "CREATE PROPERTY GRAPH IF NOT EXISTS",
        ),
    )
    .await;

    sql(&owner, "DROP PROPERTY GRAPH social.friends").await;
    let e = err(&fresh, EDGES).await.to_string();
    assert!(e.contains("friends"), "{e}");
    sql(&owner, "DROP PROPERTY GRAPH IF EXISTS social.friends").await;
}
