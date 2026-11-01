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

//! Lance Spaces (architecture v3 §7.6): tables written, indexed and searched
//! through DuckDB's `lance` extension.
//!
//! Tests marked `#[ignore]` need the extension; they download it once into
//! the target directory (`cargo test -p oikome-node --test lance -- --ignored`).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float32Array, Int64Array, StringArray};
use iceberg::io::FileIO;
use oikome_catalog::{LocationFormat, OikomeCatalog};
use oikome_engine_duckdb::DuckDbConfig;
use oikome_node::{
    EngineRegistry, NodeConfig, OikomeSession, QueryResult, SessionConfig, SessionError,
};

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<OikomeCatalog>,
    engines: Arc<EngineRegistry>,
    owner: OikomeSession,
}

/// Extensions shared by the tests that need them, downloaded on first use.
fn extension_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("duckdb-extensions")
}

fn node_with(duckdb: DuckDbConfig) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(OikomeCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    let config = NodeConfig {
        duckdb,
        ..Default::default()
    };
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), config).unwrap());
    let owner = OikomeSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
    Node {
        _dir: dir,
        catalog,
        engines,
        owner,
    }
}

/// A node with no extensions at all, which never downloads any.
fn offline_node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    node_with(DuckDbConfig {
        extension_dir: Some(dir.keep().join("extensions")),
        ..Default::default()
    })
}

/// A node that loads the `lance` extension.
fn lance_node() -> Node {
    node_with(DuckDbConfig {
        extension_dir: Some(extension_dir()),
        allow_extension_install: true,
        ..Default::default()
    })
}

impl Node {
    async fn sql(&self, sql: &str) -> QueryResult {
        self.owner
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    async fn err(&self, sql: &str) -> SessionError {
        match self.owner.sql(sql).await {
            Ok(_) => panic!("{sql}: expected an error"),
            Err(e) => e,
        }
    }
}

fn ints(result: &QueryResult, column: usize) -> Vec<i64> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let a = b
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
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

const ITEMS: &str =
    "CREATE TABLE notes.items (id BIGINT, title VARCHAR, body VARCHAR, embedding FLOAT[4])";

/// 300 items: every third body mentions bananas; embeddings repeat.
const SEED: &str = "INSERT INTO notes.items \
     SELECT i, 'title ' || i, \
            CASE WHEN i % 3 = 0 THEN 'apple banana' ELSE 'cherry date' END || ' note ' || i, \
            [i % 7, i % 5, i % 3, 1.0]::FLOAT[4] \
     FROM (SELECT unnest(range(300)) AS i) t";

#[tokio::test]
async fn format_lance_makes_a_lance_location() {
    let n = offline_node();
    n.sql("CREATE SPACE notes WITH FORMAT = 'lance'").await;
    let space = n.catalog.get_space("notes").unwrap();
    let location = n.catalog.get_location(&space.location).unwrap();
    assert!(
        matches!(&location.format, LocationFormat::Lance { root } if root == ".oikome/lance/notes"),
        "{location:?}"
    );
    assert!(n.catalog.lance_root(&location).unwrap().is_dir());

    for (sql, needle) in [
        (
            "CREATE SPACE t WITH KIND = 'transactional', FORMAT = 'lance'",
            "FORMAT",
        ),
        (
            "CREATE SPACE ro WITH LOCATION = 'notes', FORMAT = 'lance'",
            "FORMAT",
        ),
        ("CREATE SPACE p WITH FORMAT = 'parquet'", "unknown format"),
    ] {
        let err = n.err(sql).await.to_string();
        assert!(err.contains(needle), "{sql}: {err}");
    }
}

#[tokio::test]
async fn without_the_extension_a_lance_space_says_how_to_install_it() {
    let n = offline_node();
    n.sql("CREATE SPACE notes WITH FORMAT = 'lance'").await;
    let err = n.err(ITEMS).await.to_string();
    assert!(err.contains("oikome extensions install"), "{err}");
}

#[tokio::test]
async fn search_and_indexes_are_refused_outside_lance_spaces() {
    let n = offline_node();
    for sql in [
        "CREATE SPACE blog",
        "CREATE TABLE blog.posts (id BIGINT, title VARCHAR)",
        "INSERT INTO blog.posts VALUES (1, 'hello')",
        "CREATE SPACE shop WITH KIND = 'transactional'",
        "CREATE TABLE shop.orders (id INTEGER, note TEXT)",
    ] {
        n.sql(sql).await;
    }
    for (sql, needle) in [
        (
            "CREATE INDEX t ON blog.posts USING BTREE (id)",
            "indexes are for Lance spaces",
        ),
        (
            "CREATE INDEX t ON shop.orders USING BTREE (id)",
            "indexes are for Lance spaces",
        ),
        (
            "SELECT * FROM lance_fts(blog.posts, 'title', 'hello')",
            "not stored as Lance",
        ),
        (
            "SELECT * FROM lance_fts(blog.posts, 'title', 'x') a JOIN shop.orders o ON a.id = o.id",
            "several spaces",
        ),
        (
            "MERGE INTO blog.posts t USING blog.posts s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "append-only",
        ),
    ] {
        let err = n.err(sql).await.to_string();
        assert!(err.contains(needle), "{sql}: {err}");
    }

    // A read-only binding never writes to its Location.
    n.sql("CREATE SPACE blog_ro WITH LOCATION = 'blog', ACCESS = 'readonly'")
        .await;
    assert_eq!(
        strings(&n.sql("SELECT title FROM blog_ro.posts").await, 0),
        vec!["hello"]
    );
    for sql in [
        "INSERT INTO blog_ro.posts VALUES (2, 'x')",
        "CREATE TABLE blog_ro.more (id BIGINT)",
    ] {
        assert!(
            matches!(n.err(sql).await, SessionError::ReadOnlySpace(ref s) if s == "blog_ro"),
            "{sql}"
        );
    }

    // Peers never call search functions: they would read past grants.
    let peer = OikomeSession::new(
        n.catalog.clone(),
        n.engines.clone(),
        SessionConfig {
            principal: Some("did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw".into()),
            default_space: None,
        },
    );
    let err = peer
        .sql("SELECT * FROM lance_fts(blog.posts, 'title', 'hello')")
        .await
        .unwrap_err();
    assert!(
        matches!(err, SessionError::PermissionDenied(ref m) if m.contains("subscribe")),
        "{err}"
    );
}

#[tokio::test]
#[ignore = "downloads the DuckDB lance extension"]
async fn lance_tables_are_written_indexed_and_searched() {
    let n = lance_node();
    n.sql("CREATE SPACE notes WITH FORMAT = 'lance'").await;
    n.sql(ITEMS).await;
    assert_eq!(n.sql(SEED).await.rows_affected(), Some(300));
    assert_eq!(
        n.sql("UPDATE notes.items SET title = 'first' WHERE id = 0")
            .await
            .rows_affected(),
        Some(1)
    );
    n.sql("DELETE FROM notes.items WHERE id = 1").await;
    n.sql(
        "MERGE INTO notes.items t USING (SELECT 2 AS id, 'merged' AS title) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET title = s.title",
    )
    .await;
    let rows = n
        .sql("SELECT id, title FROM notes.items WHERE id < 4 ORDER BY id")
        .await;
    assert_eq!(ints(&rows, 0), vec![0, 2, 3]);
    assert_eq!(strings(&rows, 1), vec!["first", "merged", "title 3"]);

    n.sql(
        "CREATE INDEX emb ON notes.items USING IVF_FLAT (embedding) \
         WITH (num_partitions = 2, metric_type = 'l2')",
    )
    .await;
    n.sql("CREATE INDEX body ON notes.items USING INVERTED (body)")
        .await;
    let dataset = n
        .catalog
        .lance_root(
            &n.catalog
                .get_location(&n.catalog.get_space("notes").unwrap().location)
                .unwrap(),
        )
        .unwrap()
        .join("items.lance");
    assert_eq!(
        std::fs::read_dir(dataset.join("_indices")).unwrap().count(),
        2
    );

    // Vector search: the closest embeddings are exact matches.
    let nearest = n
        .sql(
            "SELECT id, _distance FROM lance_vector_search(notes.items, 'embedding', \
             [0, 0, 0, 1]::FLOAT[4], k => 3) ORDER BY _distance, id",
        )
        .await;
    assert_eq!(ints(&nearest, 0), vec![0, 105, 210]);
    let distance = nearest.batches()[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .value(0);
    assert_eq!(distance, 0.0);

    // Full-text and hybrid search, with SET SPACE resolving the table.
    n.sql("SET SPACE notes").await;
    let found = n
        .sql("SELECT id FROM lance_fts(items, 'body', 'banana', k => 5) ORDER BY id")
        .await;
    assert_eq!(found.num_rows(), 5);
    assert!(ints(&found, 0).iter().all(|id| id % 3 == 0));
    let hybrid = n
        .sql(
            "SELECT count(*) FROM lance_hybrid_search(items, 'embedding', [0, 0, 0, 1]::FLOAT[4], \
             'body', 'banana', k => 4)",
        )
        .await;
    assert_eq!(ints(&hybrid, 0), vec![4]);

    // A Lance table joins tables of other Spaces.
    for sql in [
        "CREATE SPACE tags",
        "CREATE TABLE tags.t (id BIGINT, tag VARCHAR)",
        "INSERT INTO tags.t VALUES (0, 'zero'), (3, 'three')",
    ] {
        n.sql(sql).await;
    }
    let joined = n
        .sql("SELECT i.id, t.tag FROM notes.items i JOIN tags.t t ON t.id = i.id ORDER BY i.id")
        .await;
    assert_eq!(strings(&joined, 1), vec!["zero", "three"]);

    // A read-only binding of the same Location reads it but never writes.
    n.sql("CREATE SPACE notes_ro WITH LOCATION = 'notes', ACCESS = 'readonly'")
        .await;
    assert_eq!(
        ints(&n.sql("SELECT count(*) FROM notes_ro.items").await, 0),
        vec![299]
    );
    assert!(matches!(
        n.err("DELETE FROM notes_ro.items").await,
        SessionError::ReadOnlySpace(_)
    ));
    assert!(matches!(
        n.err("DROP INDEX emb ON notes_ro.items").await,
        SessionError::ReadOnlySpace(_)
    ));

    n.sql("DROP INDEX emb ON notes.items").await;
    n.sql("DROP TABLE notes.items").await;
    let err = n.err("SELECT * FROM notes.items").await.to_string();
    assert!(err.contains("items"), "{err}");
    n.sql("DROP SPACE notes").await;
    assert!(n
        .err("SELECT * FROM notes.items")
        .await
        .to_string()
        .contains("notes"));
}

#[tokio::test]
#[ignore = "downloads the DuckDB lance extension"]
async fn writes_by_another_process_are_seen() {
    let n = lance_node();
    n.sql("CREATE SPACE notes WITH FORMAT = 'lance'").await;
    n.sql(ITEMS).await;
    n.sql(SEED).await;
    // A second DuckDB on the same data directory, as `oikome serve` has
    // while `oikome sql` writes.
    let other = OikomeSession::new(
        n.catalog.clone(),
        Arc::new(
            EngineRegistry::new(
                n.catalog.clone(),
                NodeConfig {
                    duckdb: DuckDbConfig {
                        extension_dir: Some(extension_dir()),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap(),
        ),
        SessionConfig::default(),
    );
    let count = || async {
        ints(
            &other.sql("SELECT count(*) FROM notes.items").await.unwrap(),
            0,
        )
    };
    assert_eq!(count().await, vec![300]);
    n.sql("DELETE FROM notes.items WHERE id < 10").await;
    assert_eq!(count().await, vec![290]);
}
