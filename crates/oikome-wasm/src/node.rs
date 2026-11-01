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

//! [`BrowserNode`]: statement handling of the browser node, independent of
//! JavaScript so it is tested natively against DuckDB.
//!
//! A browser node has one owner and one local engine (duckdb-wasm). Each
//! Space is a schema of that engine; the registry of Spaces is kept by the
//! host (IndexedDB) as bytes, see [`BrowserNode::registry_bytes`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use oikome_core::{CatalogError, CreateSpaceRequest, OikomeError, SpaceKind};
use oikome_engine::{EngineError, QueryEngine};
use oikome_identity::NodeIdentity;
use oikome_semantic::rewrite::quote_ident;
use oikome_semantic::{classify, parse_single, OikomeStatement, SemanticError, StatementClass};
use serde::{Deserialize, Serialize};

/// What went wrong running a statement.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(transparent)]
    Semantic(#[from] SemanticError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error("arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("registry: {0}")]
    Registry(String),
    #[error("not available in the browser node: {0}")]
    Unsupported(String),
}

impl From<OikomeError> for NodeError {
    fn from(e: OikomeError) -> Self {
        NodeError::Unsupported(e.to_string())
    }
}

/// The result of one statement.
#[derive(Debug)]
pub enum Output {
    Rows {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    RowsAffected(u64),
    Done,
}

/// A Space of the browser node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BrowserSpace {
    name: String,
    kind: SpaceKind,
}

/// The node's registry, persisted by the host.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Registry {
    spaces: BTreeMap<String, BrowserSpace>,
    default_space: Option<String>,
}

/// The browser node: an identity, a registry and the local engine.
pub struct BrowserNode {
    identity: NodeIdentity,
    engine: Arc<dyn QueryEngine>,
    registry: Mutex<Registry>,
}

impl std::fmt::Debug for BrowserNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserNode")
            .field("did", self.identity.did())
            .finish_non_exhaustive()
    }
}

impl BrowserNode {
    /// A node with `identity`, running on `engine`, restoring `registry`
    /// (bytes from [`registry_bytes`](Self::registry_bytes)) when given.
    pub fn open(
        identity: NodeIdentity,
        engine: Arc<dyn QueryEngine>,
        registry: Option<&[u8]>,
    ) -> Result<Self, NodeError> {
        let registry = match registry {
            Some(bytes) => {
                serde_json::from_slice(bytes).map_err(|e| NodeError::Registry(e.to_string()))?
            }
            None => Registry::default(),
        };
        Ok(Self {
            identity,
            engine,
            registry: Mutex::new(registry),
        })
    }

    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// The registry, for the host to persist after a statement changed it.
    pub fn registry_bytes(&self) -> Result<Vec<u8>, NodeError> {
        serde_json::to_vec(&*self.registry()).map_err(|e| NodeError::Registry(e.to_string()))
    }

    fn registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn require_space(&self, name: &str) -> Result<(), NodeError> {
        if self.registry().spaces.contains_key(name) {
            Ok(())
        } else {
            Err(CatalogError::SpaceNotFound(name.to_string()).into())
        }
    }

    /// Run one statement as the node's owner.
    pub async fn sql(&self, text: &str) -> Result<Output, NodeError> {
        match parse_single(text)? {
            OikomeStatement::CreateSpace(request) => self.create_space(request).await,
            OikomeStatement::DropSpace(name) => {
                self.require_space(&name)?;
                self.engine
                    .execute(&format!("DROP SCHEMA {} CASCADE", quote_ident(&name)), &[])
                    .await?;
                let mut reg = self.registry();
                reg.spaces.remove(&name);
                if reg.default_space.as_deref() == Some(name.as_str()) {
                    reg.default_space = None;
                }
                Ok(Output::Done)
            }
            OikomeStatement::SetSpace(name) => {
                self.require_space(&name)?;
                self.registry().default_space = Some(name);
                Ok(Output::Done)
            }
            OikomeStatement::ShowIdentity => {
                let schema = Arc::new(Schema::new(vec![Field::new("did", DataType::Utf8, false)]));
                let did = StringArray::from(vec![self.identity.did().to_string()]);
                let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(did)])?;
                Ok(Output::Rows {
                    schema,
                    batches: vec![batch],
                })
            }
            OikomeStatement::Sql(stmt) => self.run(*stmt).await,
            other => Err(NodeError::Unsupported(statement_name(&other).to_string())),
        }
    }

    async fn create_space(&self, request: CreateSpaceRequest) -> Result<Output, NodeError> {
        match request.kind {
            None | Some(SpaceKind::Analytical) => {}
            Some(SpaceKind::Transactional) => {
                return Err(NodeError::Unsupported(
                    "transactional spaces (only analytical spaces on duckdb-wasm)".to_string(),
                ))
            }
            Some(SpaceKind::Remote) => {
                return Err(NodeError::Unsupported(
                    "remote spaces (they arrive with the WebRTC transport)".to_string(),
                ))
            }
        }
        if request.engine.is_some()
            || request.location.is_some()
            || request.access.is_some()
            || request.storage.is_some()
        {
            return Err(NodeError::Unsupported(
                "ENGINE, LOCATION, ACCESS and STORAGE (every browser space is a duckdb-wasm schema)"
                    .to_string(),
            ));
        }
        let name = request.name;
        if self.registry().spaces.contains_key(&name) {
            return Err(CatalogError::SpaceExists(name).into());
        }
        // Idempotent in the engine, so a Space whose registry entry was lost
        // can be created again over its surviving data.
        self.engine
            .execute(
                &format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(&name)),
                &[],
            )
            .await?;
        self.registry().spaces.insert(
            name.clone(),
            BrowserSpace {
                name,
                kind: SpaceKind::Analytical,
            },
        );
        Ok(Output::Done)
    }

    async fn run(&self, mut stmt: sqlparser::ast::Statement) -> Result<Output, NodeError> {
        let default_space = self.registry().default_space.clone();
        let class = classify(&mut stmt, default_space.as_deref())?;
        let named = match &class {
            StatementClass::Query { spaces } => spaces.clone(),
            StatementClass::TableDdl { space } => [space.clone()].into(),
            StatementClass::Dml { space, spaces, .. } => {
                let mut all = spaces.clone();
                all.insert(space.clone());
                all
            }
            StatementClass::Tcl(_) => return Err(NodeError::Unsupported("transactions".into())),
            StatementClass::ViewDdl => return Err(NodeError::Unsupported("views".into())),
            StatementClass::Acl => {
                return Err(NodeError::Unsupported(
                    "GRANT / POLICY (the browser node serves no peers)".into(),
                ))
            }
        };
        for space in &named {
            self.require_space(space)?;
        }
        // Table names are qualified in place by `classify`.
        let sql = stmt.to_string();
        match class {
            StatementClass::Query { .. } => {
                let (schema, stream) = self.engine.query(&sql, &[]).await?;
                let batches = stream.try_collect().await?;
                Ok(Output::Rows { schema, batches })
            }
            _ => Ok(Output::RowsAffected(self.engine.execute(&sql, &[]).await?)),
        }
    }
}

fn statement_name(stmt: &OikomeStatement) -> &'static str {
    match stmt {
        OikomeStatement::PublishSpace(_) => "PUBLISH SPACE",
        OikomeStatement::RefreshSpace(_) => "REFRESH SPACE",
        OikomeStatement::IssueUcan(_) => "ISSUE UCAN (the browser node serves no peers)",
        OikomeStatement::RevokeUcan(_) => "REVOKE UCAN",
        OikomeStatement::ShowUcans => "SHOW UCANS",
        OikomeStatement::ShowPeers => "SHOW PEERS",
        _ => "this statement",
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array};
    use oikome_engine_duckdb::DuckDbEngine;

    use super::*;

    fn engine() -> Arc<dyn QueryEngine> {
        Arc::new(DuckDbEngine::open_in_memory(Default::default()).unwrap())
    }

    fn node(engine: Arc<dyn QueryEngine>, registry: Option<&[u8]>) -> BrowserNode {
        BrowserNode::open(NodeIdentity::from_seed(&[3; 32]), engine, registry).unwrap()
    }

    async fn ints(node: &BrowserNode, sql: &str) -> Vec<i64> {
        let Output::Rows { batches, .. } = node.sql(sql).await.unwrap() else {
            panic!("{sql}: expected rows");
        };
        batches
            .iter()
            .flat_map(|b| {
                let col = arrow_cast_i64(b.column(0).as_ref());
                col.values().to_vec()
            })
            .collect()
    }

    fn arrow_cast_i64(array: &dyn Array) -> Int64Array {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .unwrap_or_else(|| panic!("expected BIGINT, got {}", array.data_type()))
    }

    #[tokio::test]
    async fn spaces_are_schemas_of_the_local_engine() {
        let engine = engine();
        let n = node(engine.clone(), None);
        n.sql("CREATE SPACE notes").await.unwrap();
        n.sql("CREATE TABLE notes.items (id BIGINT NOT NULL, title VARCHAR)")
            .await
            .unwrap();
        let inserted = n
            .sql("INSERT INTO notes.items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();
        assert!(matches!(inserted, Output::RowsAffected(3)), "{inserted:?}");
        assert_eq!(
            ints(&n, "SELECT id FROM notes.items WHERE id > 1 ORDER BY id").await,
            vec![2, 3]
        );

        // Bare names resolve against SET SPACE.
        n.sql("SET SPACE notes").await.unwrap();
        assert_eq!(ints(&n, "SELECT count(*) FROM items").await, vec![3]);

        // The registry survives a restart over the same engine.
        let reopened = node(engine, Some(&n.registry_bytes().unwrap()));
        assert_eq!(ints(&reopened, "SELECT count(*) FROM items").await, vec![3]);

        reopened.sql("DROP SPACE notes").await.unwrap();
        let err = reopened.sql("SELECT * FROM notes.items").await.unwrap_err();
        assert!(err.to_string().contains("notes"), "{err}");
    }

    #[tokio::test]
    async fn unknown_spaces_and_server_statements_are_refused() {
        let n = node(engine(), None);
        let err = n.sql("SELECT * FROM nowhere.t").await.unwrap_err();
        assert!(
            matches!(err, NodeError::Catalog(CatalogError::SpaceNotFound(_))),
            "{err}"
        );
        n.sql("CREATE SPACE notes").await.unwrap();
        assert!(matches!(
            n.sql("CREATE SPACE notes").await,
            Err(NodeError::Catalog(CatalogError::SpaceExists(_)))
        ));
        for sql in [
            "CREATE SPACE shop WITH KIND = 'transactional'",
            "CREATE SPACE pub WITH TOKEN = 'a.b.c'",
            "CREATE SPACE x WITH STORAGE = 'disk'",
            "ISSUE UCAN ON SPACE notes TO 'did:key:z6MktwupdmLXVVqTzCw4i46r4uGyosGXRnR3XjN4Zq7oMMsw'",
            "GRANT SELECT ON notes.items TO alice",
            "BEGIN",
        ] {
            assert!(
                matches!(n.sql(sql).await, Err(NodeError::Unsupported(_))),
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn the_identity_comes_from_the_seed() {
        let n = node(engine(), None);
        let Output::Rows { batches, .. } = n.sql("SHOW IDENTITY").await.unwrap() else {
            panic!("expected rows");
        };
        let did = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(did, NodeIdentity::from_seed(&[3; 32]).did().to_string());
    }
}
