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

//! Subscribing to a Lance Space (3.3 exit criterion): the copy holds one
//! version of each dataset, its indexes included, so the subscriber
//! searches without building any index.
//!
//! Needs DuckDB's `lance` extension, downloaded once into the target
//! directory: `cargo test -p oikome-server --test lance_subscription -- --ignored`.

mod common;

use std::path::PathBuf;

use common::{ints, strings, Node};
use oikome_catalog::lance::lance_version;

/// The dataset of `space.table` on `node`, a Lance Space of its own.
fn owner_dataset(node: &Node, space: &str, table: &str) -> PathBuf {
    let location = node
        .catalog
        .get_location(&node.catalog.get_space(space).unwrap().location)
        .unwrap();
    node.catalog
        .lance_root(&location)
        .unwrap()
        .join(format!("{table}.lance"))
}

/// The dataset of `table` in `node`'s copy `space`.
fn copy_dataset(node: &Node, space: &str, table: &str) -> PathBuf {
    node.catalog
        .replica_dir(space)
        .join("lance")
        .join(format!("{table}.lance"))
}

#[tokio::test]
#[ignore = "downloads the DuckDB lance extension"]
async fn a_subscribed_lance_space_is_searched_with_the_owners_indexes() {
    let a = Node::with_lance(41).serve().await;
    let b = Node::with_lance(42);
    for sql in [
        "CREATE SPACE notes WITH FORMAT = 'lance'".to_string(),
        "CREATE TABLE notes.items (id BIGINT, title VARCHAR, body VARCHAR, embedding FLOAT[4])"
            .to_string(),
        "INSERT INTO notes.items \
         SELECT i, 'title ' || i, \
                CASE WHEN i % 3 = 0 THEN 'apple banana' ELSE 'cherry date' END || ' note ' || i, \
                [i % 7, i % 5, i % 3, 1.0]::FLOAT[4] \
         FROM (SELECT unnest(range(300)) AS i) t"
            .to_string(),
        "DELETE FROM notes.items WHERE id = 1".to_string(),
        "CREATE INDEX emb ON notes.items USING IVF_FLAT (embedding) \
         WITH (num_partitions = 2, metric_type = 'l2')"
            .to_string(),
        "CREATE INDEX body ON notes.items USING INVERTED (body)".to_string(),
        format!("GRANT SELECT ON notes.items TO \"{}\"", b.did()),
    ] {
        a.sql(&sql).await;
    }
    let (_, token) = a
        .issue(&format!(
            "ISSUE UCAN ON SPACE notes TO '{}' CAN 'space/read'",
            b.did()
        ))
        .await;

    let report = b
        .sql(&format!(
            "CREATE SPACE copy WITH PEER = '{}', REMOTE = 'notes', TOKEN = '{token}', MODE = 'subscribe'",
            a.url()
        ))
        .await;
    assert_eq!(strings(&report, 0), vec!["items"]);
    assert_eq!(strings(&report, 4), vec!["copied"]);

    // The copy is exactly A's current version, index files included: B
    // never built an index.
    let owned = lance_version(&owner_dataset(&a, "notes", "items"), None)
        .await
        .unwrap();
    let copied = lance_version(&copy_dataset(&b, "copy", "items"), None)
        .await
        .unwrap();
    assert_eq!(copied.version, owned.version);
    assert_eq!(copied.files, owned.files);
    assert_eq!(
        copied
            .files
            .iter()
            .filter(|f| f.starts_with("_indices/"))
            .map(|f| f.split('/').nth(1).unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );

    // B searches its copy; the row A deleted is gone.
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM copy.items").await, 0),
        vec![299]
    );
    let nearest = b
        .sql(
            "SELECT id FROM lance_vector_search(copy.items, 'embedding', [0, 0, 0, 1]::FLOAT[4], k => 3) \
             ORDER BY _distance, id",
        )
        .await;
    assert_eq!(ints(&nearest, 0), vec![0, 105, 210]);
    let found = b
        .sql("SELECT id FROM lance_fts(copy.items, 'body', 'banana', k => 5)")
        .await;
    assert_eq!(found.num_rows(), 5);
    assert!(ints(&found, 0).iter().all(|id| id % 3 == 0));

    // A writes a new version; a refresh fetches only its new files.
    a.sql("INSERT INTO notes.items VALUES (1000, 'late', 'banana late', [0, 0, 0, 1]::FLOAT[4])")
        .await;
    let refreshed = b.sql("REFRESH SPACE copy").await;
    let fetched = ints(&refreshed, 2)[0];
    assert!(
        fetched >= 2 && (fetched as usize) < owned.files.len(),
        "{fetched}"
    );
    assert_eq!(
        ints(&b.sql("SELECT count(*) FROM copy.items").await, 0),
        vec![300]
    );
    let late = b
        .sql("SELECT id FROM lance_fts(copy.items, 'body', 'late', k => 1)")
        .await;
    assert_eq!(ints(&late, 0), vec![1000]);

    // The copy is read-only.
    let err = b.sql_err("DELETE FROM copy.items").await;
    assert!(err.contains("read-only"), "{err}");
}
