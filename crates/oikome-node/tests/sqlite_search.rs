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

//! Full-text (FTS5) and vector (sqlite-vec) search in transactional Spaces
//! (3.3 exit criterion, architecture v3 §8).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use iceberg::io::FileIO;
use oikome_catalog::OikomeCatalog;
use oikome_node::{
    EngineRegistry, NodeConfig, OikomeSession, QueryResult, SessionConfig, SessionError,
};

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<OikomeCatalog>,
    engines: Arc<EngineRegistry>,
    owner: OikomeSession,
}

fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let catalog =
        Arc::new(OikomeCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap());
    let engines = Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
    let owner = OikomeSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
    Node {
        _dir: dir,
        catalog,
        engines,
        owner,
    }
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

/// `shop` with notes in an FTS5 table and their embeddings in a vec0 table.
async fn seed(n: &Node) {
    for sql in [
        "CREATE SPACE shop WITH KIND = 'transactional'",
        "CREATE TABLE shop.notes (id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
        "INSERT INTO shop.notes VALUES (1, 'apples', 'red apples and green apples'), \
         (2, 'bananas', 'yellow bananas'), (3, 'fruit', 'apples, bananas and cherries')",
        "CREATE VIRTUAL TABLE shop.docs USING fts5(title, body, content = 'notes', \
         content_rowid = 'id', tokenize = 'porter')",
        "INSERT INTO shop.docs (docs) VALUES ('rebuild')",
        "CREATE VIRTUAL TABLE IF NOT EXISTS shop.vecs USING vec0(embedding float[4])",
        "INSERT INTO shop.vecs (rowid, embedding) VALUES \
         (1, '[1, 0, 0, 0]'), (2, '[0, 1, 0, 0]'), (3, '[0.9, 0.1, 0, 0]')",
    ] {
        n.sql(sql).await;
    }
}

#[tokio::test]
async fn full_text_and_vector_search_in_a_transactional_space() {
    let n = node();
    seed(&n).await;

    // Full text, best match first; the porter stemmer matches "apple".
    let found = n
        .sql(
            "SELECT rowid, title FROM shop.docs WHERE docs MATCH 'apple' \
             ORDER BY bm25(docs)",
        )
        .await;
    assert_eq!(ints(&found), vec![1, 3]);

    // Nearest neighbours, with SET SPACE resolving the table, joined to
    // the rows they index.
    n.sql("SET SPACE shop").await;
    let nearest = n
        .sql(
            "SELECT n.id, n.title FROM vecs v JOIN notes n ON n.id = v.rowid \
             WHERE v.embedding MATCH '[1, 0, 0, 0]' AND k = 2 ORDER BY v.distance",
        )
        .await;
    assert_eq!(ints(&nearest), vec![1, 3]);
    assert_eq!(strings(&nearest, 1), vec!["apples", "fruit"]);

    // Inside a transaction (a private connection) search sees its own rows.
    n.sql("BEGIN").await;
    n.sql("INSERT INTO vecs (rowid, embedding) VALUES (9, '[0, 0, 1, 0]')")
        .await;
    let inside = n
        .sql("SELECT rowid FROM vecs WHERE embedding MATCH '[0, 0, 1, 0]' AND k = 1")
        .await;
    assert_eq!(ints(&inside), vec![9]);
    n.sql("ROLLBACK").await;

    // The tables behind FTS5 and vec0 are not relations of the Space.
    assert_eq!(
        n.owner.readable_relations("shop").await.unwrap(),
        vec!["docs", "notes", "vecs"]
    );

    // PUBLISH ships the notes, not the indexes over them.
    let report = n.sql("PUBLISH SPACE shop").await;
    assert_eq!(strings(&report, 0), vec!["notes"]);
    let err = n.err("PUBLISH SPACE shop TABLES (docs)").await.to_string();
    assert!(err.contains("virtual table"), "{err}");
}

#[tokio::test]
async fn search_is_refused_where_it_cannot_run() {
    let n = node();
    seed(&n).await;
    n.sql("CREATE SPACE blog").await;
    n.sql("CREATE TABLE blog.posts (id BIGINT, title VARCHAR)")
        .await;
    for (sql, needle) in [
        (
            "CREATE VIRTUAL TABLE blog.docs USING fts5(title)",
            "transactional spaces",
        ),
        ("CREATE VIRTUAL TABLE shop.r USING rtree(id, a, b)", "rtree"),
        (
            "SELECT * FROM blog.posts WHERE title MATCH 'x'",
            "one transactional space",
        ),
        (
            "SELECT * FROM shop.docs d JOIN blog.posts p ON p.id = d.rowid WHERE docs MATCH 'x'",
            "one transactional space",
        ),
    ] {
        let err = n.err(sql).await.to_string();
        assert!(err.contains(needle), "{sql}: {err}");
    }

    // Peers never run MATCH: it reads past grants and row policies.
    let did = "did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw";
    n.sql(&format!("GRANT SELECT ON shop.docs TO \"{did}\""))
        .await;
    let peer = OikomeSession::new(
        n.catalog.clone(),
        n.engines.clone(),
        SessionConfig {
            principal: Some(did.into()),
            default_space: None,
        },
    );
    let err = peer
        .sql("SELECT rowid FROM shop.docs WHERE docs MATCH 'apple'")
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::PermissionDenied(_)), "{err}");
    let err = peer
        .sql("CREATE VIRTUAL TABLE shop.more USING fts5(a)")
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::PermissionDenied(_)), "{err}");
}
