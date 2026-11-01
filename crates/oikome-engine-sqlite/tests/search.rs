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

//! Full-text (FTS5) and vector (sqlite-vec) search in a transactional
//! Space's SQLite file (architecture v3 §8).

use arrow_array::{Array, Int64Array, RecordBatch};
use futures::StreamExt;
use oikome_engine::{Capabilities, QueryEngine};
use oikome_engine_sqlite::SqliteEngine;

async fn collect(engine: &SqliteEngine, sql: &str) -> Vec<RecordBatch> {
    let (_, stream) = engine.query(sql, &[]).await.unwrap();
    stream.map(|b| b.unwrap()).collect().await
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn fts5_and_vec0_tables_are_searched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shop.sqlite");
    let engine = SqliteEngine::open("shop", &path).unwrap();
    assert!(engine
        .capabilities()
        .contains(Capabilities::EXT_FTS | Capabilities::EXT_VECTOR));

    for sql in [
        "CREATE TABLE shop.orders (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE VIRTUAL TABLE shop.docs USING fts5(title, body, tokenize = 'porter')",
        "INSERT INTO shop.docs (rowid, title, body) VALUES \
         (1, 'apples', 'red apples and green apples'), \
         (2, 'bananas', 'yellow bananas'), \
         (3, 'fruit', 'apples, bananas and cherries')",
        "CREATE VIRTUAL TABLE shop.vecs USING vec0(embedding float[4])",
        "INSERT INTO shop.vecs (rowid, embedding) VALUES \
         (1, '[1, 0, 0, 0]'), (2, '[0, 1, 0, 0]'), (3, '[0.9, 0.1, 0, 0]')",
    ] {
        engine.execute(sql, &[]).await.unwrap();
    }

    // Full text, best match first (bm25 is lower for better matches).
    let found = collect(
        &engine,
        "SELECT rowid FROM shop.docs WHERE docs MATCH 'apple' ORDER BY bm25(docs)",
    )
    .await;
    assert_eq!(ids(&found), vec![1, 3]);

    // Nearest neighbours of [1, 0, 0, 0].
    let nearest = collect(
        &engine,
        "SELECT rowid FROM shop.vecs WHERE embedding MATCH '[1, 0, 0, 0]' AND k = 2 \
         ORDER BY distance",
    )
    .await;
    assert_eq!(ids(&nearest), vec![1, 3]);

    // Virtual tables are listed, the tables that back them are not.
    assert_eq!(
        engine.table_names("shop").await.unwrap(),
        vec!["docs", "orders", "vecs"]
    );

    // Another connection to the file (as a transaction opens) has vec0 too.
    let other = SqliteEngine::open("shop", &path).unwrap();
    let again = collect(
        &other,
        "SELECT rowid FROM shop.vecs WHERE embedding MATCH '[0, 1, 0, 0]' AND k = 1",
    )
    .await;
    assert_eq!(ids(&again), vec![2]);
}
