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

//! Views, column grants and row policies (3.0-C exit criteria).

use std::sync::Arc;

use agoradb_catalog::AgoraCatalog;
use agoradb_core::CatalogError;
use agoradb_node::{
    AgoraSession, EngineRegistry, NodeConfig, QueryResult, SessionConfig, SessionError,
};
use agoradb_semantic::SemanticError;
use arrow_array::{Array, Int64Array, StringArray};
use iceberg::io::FileIO;

struct Node {
    _dir: tempfile::TempDir,
    catalog: Arc<AgoraCatalog>,
    engines: Arc<EngineRegistry>,
    owner: AgoraSession,
}

impl Node {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(
            AgoraCatalog::open(FileIO::new_with_fs(), dir.path().to_str().unwrap()).unwrap(),
        );
        let engines =
            Arc::new(EngineRegistry::new(catalog.clone(), NodeConfig::default()).unwrap());
        let owner = AgoraSession::new(catalog.clone(), engines.clone(), SessionConfig::default());
        Self {
            _dir: dir,
            catalog,
            engines,
            owner,
        }
    }

    fn as_principal(&self, principal: &str) -> AgoraSession {
        AgoraSession::new(
            self.catalog.clone(),
            self.engines.clone(),
            SessionConfig {
                principal: Some(principal.to_string()),
                ..Default::default()
            },
        )
    }

    async fn run(&self, sqls: &[&str]) {
        for sql in sqls {
            self.owner
                .sql(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }
}

fn ints(result: &QueryResult, column: usize) -> Vec<i64> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            b.column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn strings(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .batches()
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn column_names(result: &QueryResult) -> Vec<String> {
    result
        .schema()
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// blog (analytical, Parquet) + orders (transactional, SQLite).
async fn seed(n: &Node) {
    n.run(&[
        "CREATE SPACE blog",
        "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR, draft BOOLEAN)",
        "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello', false), (2, 'bob', 'World', false), (3, 'alice', 'Secret', true)",
        "CREATE SPACE orders WITH KIND = 'transactional'",
        "CREATE TABLE orders.orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, amount REAL)",
        "INSERT INTO orders.orders VALUES (10, 'alice', 9.5), (11, 'bob', 20.0), (12, 'alice', 0.5)",
    ])
    .await;
}

#[tokio::test]
async fn view_in_one_space_is_pushed_to_its_engine() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "CREATE VIEW blog.published AS SELECT id, author, title FROM blog.posts WHERE NOT draft",
    ])
    .await;
    let result = n
        .owner
        .sql("SELECT id, title FROM blog.published ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![1, 2]);
    assert_eq!(strings(&result, 1), vec!["Hello", "World"]);

    // Views persist in the catalog and survive a restart.
    let reopened = AgoraCatalog::open(FileIO::new_with_fs(), n.catalog.root_path()).unwrap();
    assert!(reopened.get_view("blog", "published").is_some());

    // Views compose, and SET SPACE lets them be referenced by bare name.
    n.run(&[
        "SET SPACE blog",
        "CREATE VIEW by_alice AS SELECT id, title FROM published WHERE author = 'alice'",
    ])
    .await;
    let result = n.owner.sql("SELECT title FROM by_alice").await.unwrap();
    assert_eq!(strings(&result, 0), vec!["Hello"]);
}

#[tokio::test]
async fn view_spanning_spaces_is_federated() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "CREATE SPACE sem",
        "CREATE VIEW sem.customer_posts AS \
         SELECT o.customer, count(DISTINCT p.id) AS posts, count(DISTINCT o.id) AS orders \
         FROM orders.orders o JOIN blog.posts p ON p.author = o.customer GROUP BY o.customer",
    ])
    .await;
    let result = n
        .owner
        .sql("SELECT customer, posts, orders FROM sem.customer_posts ORDER BY customer")
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["alice", "bob"]);
    assert_eq!(ints(&result, 1), vec![2, 1]);
    assert_eq!(ints(&result, 2), vec![2, 1]);
}

#[tokio::test]
async fn invalid_views_are_rejected_before_they_are_stored() {
    let n = Node::new();
    seed(&n).await;
    // References a missing column.
    assert!(n
        .owner
        .sql("CREATE VIEW blog.bad AS SELECT nope FROM blog.posts")
        .await
        .is_err());
    assert!(n.catalog.get_view("blog", "bad").is_none());
    // Same name as a table.
    assert!(matches!(
        n.owner
            .sql("CREATE VIEW blog.posts AS SELECT 1 AS one")
            .await,
        Err(SessionError::AlreadyExists(_))
    ));
    // Replacing a view with one that references itself is a cycle.
    n.run(&["CREATE VIEW blog.v AS SELECT id FROM blog.posts"])
        .await;
    assert!(matches!(
        n.owner
            .sql("CREATE OR REPLACE VIEW blog.v AS SELECT id FROM blog.v")
            .await,
        Err(SessionError::Semantic(SemanticError::ViewRecursion(..)))
    ));
    assert!(matches!(
        n.owner.sql("CREATE VIEW blog.v AS SELECT 1 AS one").await,
        Err(SessionError::Catalog(CatalogError::ViewExists(_)))
    ));
    n.run(&[
        "CREATE VIEW IF NOT EXISTS blog.v AS SELECT 1 AS one",
        "DROP VIEW blog.v",
        "DROP VIEW IF EXISTS blog.v",
    ])
    .await;
    assert!(n
        .owner
        .sql("CREATE TABLE blog.published (id BIGINT)")
        .await
        .is_ok());
    n.run(&["CREATE VIEW blog.pv AS SELECT id FROM blog.published"])
        .await;
    assert!(matches!(
        n.owner.sql("CREATE TABLE blog.pv (id BIGINT)").await,
        Err(SessionError::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn column_grant_limits_star_and_hides_other_columns() {
    let n = Node::new();
    seed(&n).await;
    n.run(&["GRANT SELECT (id, title) ON blog.posts TO alice"])
        .await;
    let alice = n.as_principal("alice");

    let result = alice
        .sql("SELECT * FROM blog.posts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(column_names(&result), vec!["id", "title"]);
    assert_eq!(ints(&result, 0), vec![1, 2, 3]);

    // A column outside the grant does not exist for alice.
    assert!(alice.sql("SELECT author FROM blog.posts").await.is_err());

    // Re-granting replaces the column list.
    n.run(&["GRANT SELECT ON blog.posts TO alice"]).await;
    let result = alice.sql("SELECT * FROM blog.posts").await.unwrap();
    assert_eq!(column_names(&result).len(), 4);

    n.run(&["REVOKE SELECT ON blog.posts FROM alice"]).await;
    assert!(matches!(
        alice.sql("SELECT * FROM blog.posts").await,
        Err(SessionError::Semantic(SemanticError::TableNotFound(_)))
    ));
}

#[tokio::test]
async fn row_policies_filter_inside_the_engine() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "GRANT SELECT ON orders.orders TO alice, bob, carol",
        "CREATE POLICY own_orders ON orders.orders FOR SELECT TO alice, bob USING (customer = current_user)",
        "CREATE POLICY big_orders ON orders.orders FOR SELECT TO alice USING (amount > 10)",
    ])
    .await;

    // alice: her own orders OR big ones (policies combine with OR).
    let result = n
        .as_principal("alice")
        .sql("SELECT id FROM orders.orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![10, 11, 12]);

    let result = n
        .as_principal("bob")
        .sql("SELECT id FROM orders.orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![11]);

    // carol is granted the table but named by no policy: no rows.
    let result = n
        .as_principal("carol")
        .sql("SELECT count(*) FROM orders.orders")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![0]);

    n.run(&["DROP POLICY big_orders ON orders.orders"]).await;
    let result = n
        .as_principal("alice")
        .sql("SELECT id FROM orders.orders ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![10, 12]);

    assert!(matches!(
        n.owner.sql("DROP POLICY big_orders ON orders.orders").await,
        Err(SessionError::Catalog(CatalogError::PolicyNotFound(_)))
    ));
    n.run(&["DROP POLICY IF EXISTS big_orders ON orders.orders"])
        .await;
}

#[tokio::test]
async fn policy_predicate_is_pushed_into_federated_engine_sql() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "GRANT SELECT ON orders.orders TO alice",
        "GRANT SELECT (id, author, title) ON blog.posts TO alice",
        "CREATE POLICY own_orders ON orders.orders TO alice USING (customer = current_user)",
    ])
    .await;
    let alice = n.as_principal("alice");
    let sql = "SELECT p.title, o.id FROM blog.posts p JOIN orders.orders o ON p.author = o.customer ORDER BY o.id, p.id";

    let plan = alice.explain(sql).await.unwrap();
    let sqlite_leaf = plan
        .lines()
        .find(|l| l.contains("name=orders"))
        .unwrap_or_else(|| panic!("no SQLite leaf:\n{plan}"));
    assert!(sqlite_leaf.contains("'alice'"), "{sqlite_leaf}");
    assert!(!sqlite_leaf.contains("draft"), "{plan}");

    let result = alice.sql(sql).await.unwrap();
    assert_eq!(
        strings(&result, 0),
        vec!["Hello", "Secret", "Hello", "Secret"]
    );
    assert_eq!(ints(&result, 1), vec![10, 10, 12, 12]);
}

#[tokio::test]
async fn ungranted_and_missing_tables_are_indistinguishable() {
    let n = Node::new();
    seed(&n).await;
    n.run(&["GRANT SELECT ON blog.posts TO alice"]).await;
    let alice = n.as_principal("alice");

    let hidden = alice.sql("SELECT * FROM orders.orders").await.unwrap_err();
    let missing = alice.sql("SELECT * FROM orders.nope").await.unwrap_err();
    let no_space = alice.sql("SELECT * FROM ghost.t").await.unwrap_err();
    assert_eq!(hidden.to_string(), "Table not found: orders.orders");
    assert_eq!(missing.to_string(), "Table not found: orders.nope");
    assert_eq!(no_space.to_string(), "Table not found: ghost.t");

    // Joining a visible table with a hidden one fails the same way.
    assert!(matches!(
        alice
            .sql("SELECT * FROM blog.posts p JOIN orders.orders o ON p.author = o.customer")
            .await,
        Err(SessionError::Semantic(SemanticError::TableNotFound(_)))
    ));

    // SET SPACE does not reveal Spaces alice has no grant in.
    alice.sql("SET SPACE blog").await.unwrap();
    assert!(matches!(
        alice.sql("SET SPACE orders").await,
        Err(SessionError::Catalog(CatalogError::SpaceNotFound(_)))
    ));
}

#[tokio::test]
async fn granted_view_exposes_data_without_exposing_its_tables() {
    let n = Node::new();
    seed(&n).await;
    n.run(&[
        "CREATE VIEW blog.public_posts AS SELECT id, title FROM blog.posts WHERE NOT draft",
        "GRANT SELECT ON blog.public_posts TO guest",
    ])
    .await;
    let guest = n.as_principal("guest");
    let result = guest
        .sql("SELECT title FROM blog.public_posts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(strings(&result, 0), vec!["Hello", "World"]);
    assert!(matches!(
        guest.sql("SELECT * FROM blog.posts").await,
        Err(SessionError::Semantic(SemanticError::TableNotFound(_)))
    ));

    // Dropping the view drops its grants.
    n.run(&["DROP VIEW blog.public_posts"]).await;
    assert!(n
        .catalog
        .get_grant("guest", "blog", "public_posts")
        .is_none());
}

#[tokio::test]
async fn principals_are_read_only() {
    let n = Node::new();
    seed(&n).await;
    n.run(&["GRANT SELECT ON orders.orders TO alice"]).await;
    let alice = n.as_principal("alice");
    for sql in [
        "INSERT INTO orders.orders VALUES (99, 'alice', 1.0)",
        "UPDATE orders.orders SET amount = 0",
        "CREATE TABLE orders.mine (id INTEGER)",
        "CREATE VIEW orders.v AS SELECT id FROM orders.orders",
        "GRANT SELECT ON orders.orders TO mallory",
        "CREATE SPACE mine",
        "DROP SPACE orders",
        "BEGIN",
    ] {
        assert!(
            matches!(alice.sql(sql).await, Err(SessionError::PermissionDenied(_))),
            "{sql} must be denied"
        );
    }
}

#[tokio::test]
async fn grants_and_policies_are_validated() {
    let n = Node::new();
    seed(&n).await;
    assert!(matches!(
        n.owner.sql("GRANT SELECT ON blog.nope TO alice").await,
        Err(SessionError::Catalog(CatalogError::TableNotFound(_)))
    ));
    assert!(n
        .owner
        .sql("GRANT SELECT (id, nope) ON blog.posts TO alice")
        .await
        .is_err());
    assert!(n.catalog.get_grant("alice", "blog", "posts").is_none());
    assert!(matches!(
        n.owner.sql("GRANT INSERT ON blog.posts TO alice").await,
        Err(SessionError::Unsupported(_))
    ));
    assert!(n
        .owner
        .sql("CREATE POLICY p ON blog.posts TO alice USING (nope = 1)")
        .await
        .is_err());
    assert!(matches!(
        n.owner
            .sql("CREATE POLICY p ON blog.posts FOR DELETE TO alice USING (true)")
            .await,
        Err(SessionError::Unsupported(_))
    ));
    n.run(&["CREATE POLICY p ON blog.posts TO alice USING (author = current_user)"])
        .await;
    assert!(matches!(
        n.owner
            .sql("CREATE POLICY p ON blog.posts TO bob USING (true)")
            .await,
        Err(SessionError::Catalog(CatalogError::PolicyExists(_)))
    ));

    // Dropping a Space removes its semantic metadata.
    n.run(&["GRANT SELECT ON blog.posts TO alice", "DROP SPACE blog"])
        .await;
    assert!(n.catalog.get_grant("alice", "blog", "posts").is_none());
    assert!(n.catalog.policies_on("blog", "posts").is_empty());
}

#[tokio::test]
async fn ctes_are_not_mistaken_for_tables() {
    let n = Node::new();
    seed(&n).await;
    let result = n
        .owner
        .sql("WITH a AS (SELECT id FROM blog.posts WHERE author = 'alice') SELECT count(*) FROM a")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![2]);

    n.run(&["GRANT SELECT ON blog.posts TO alice"]).await;
    let result = n
        .as_principal("alice")
        .sql("WITH a AS (SELECT id FROM blog.posts) SELECT count(*) FROM a")
        .await
        .unwrap();
    assert_eq!(ints(&result, 0), vec![3]);
}
