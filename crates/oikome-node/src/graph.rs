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

//! Property graphs over the tables of a Lance Space (architecture v3 §8),
//! queried with SQL/PGQ through DuckDB's `duckpgq` extension.
//!
//! DuckPGQ cannot define a graph over views, and Iceberg tables reach DuckDB
//! as views, so graphs belong to Lance Spaces, whose tables DuckDB reads
//! directly. A graph `<space>.<graph>` is `<space>__<graph>` in DuckDB. Its
//! definition is kept in the catalog and created again in a DuckDB that
//! does not have it yet (after a restart, in another process).

use std::sync::Arc;

use futures::TryStreamExt;
use oikome_catalog::{GraphDef, Space};
use oikome_engine::name::quote_ident;
use oikome_engine::{Capabilities, EngineError, QueryEngine};
use oikome_semantic::{
    render_graph_parts, CreateGraphRequest, DropGraphRequest, GraphPart, GraphQuery,
};

use crate::error::SessionError;
use crate::lance::lance_root;
use crate::result::QueryResult;
use crate::session::OikomeSession;

/// The name DuckDB knows `space.graph` by.
fn engine_name(space: &str, graph: &str) -> String {
    format!("{space}__{graph}")
}

fn create_sql(graph: &GraphDef) -> String {
    format!(
        "CREATE OR REPLACE PROPERTY GRAPH {} {}",
        engine_name(&graph.space, &graph.name),
        graph.body
    )
}

impl OikomeSession {
    fn graph_space(&self, space: Option<&str>) -> Result<Space, SessionError> {
        let name = match space {
            Some(space) => space.to_string(),
            None => self.default_space().ok_or(SessionError::NoDefaultSpace)?,
        };
        let space = self.space(&name)?;
        if lance_root(&self.catalog, &space)?.is_none() {
            return Err(SessionError::Unsupported(format!(
                "property graphs are defined on Lance spaces (FORMAT = 'lance'); \
                 '{name}' is not stored as Lance"
            )));
        }
        Ok(space)
    }

    /// Bind `space` with DuckPGQ loaded and every graph of the Space present.
    async fn graph_engine(&self, space: &Space) -> Result<Arc<dyn QueryEngine>, SessionError> {
        let engine = self.bind(space).await?;
        engine.enable(Capabilities::EXT_GRAPH).await?;
        for graph in self.catalog.list_graphs(&space.name) {
            let name = engine_name(&graph.space, &graph.name);
            let sql = create_sql(&graph);
            if !self.engines().has_graph(&name, &sql) {
                engine.execute(&sql, &[]).await?;
                self.engines().set_graph(&name, Some(sql));
            }
        }
        Ok(engine)
    }

    /// `CREATE [OR REPLACE] PROPERTY GRAPH [IF NOT EXISTS] <space>.<graph> ...`
    pub(crate) async fn create_property_graph(
        &self,
        request: CreateGraphRequest,
    ) -> Result<QueryResult, SessionError> {
        let space = self.graph_space(request.space.as_deref())?;
        if self
            .catalog
            .get_graph(&space.name, &request.graph)
            .is_some()
        {
            if request.if_not_exists {
                return Ok(QueryResult::Empty);
            }
            if !request.or_replace {
                return Err(SessionError::AlreadyExists(format!(
                    "property graph {}.{}",
                    space.name, request.graph
                )));
            }
        }
        let engine = self.graph_engine(&space).await?;
        let tables = engine.table_names(&space.name).await?;
        let body = render_graph_parts(&request.body, |part| match part {
            GraphPart::Table { space: s, table } => {
                if s.as_deref().is_some_and(|s| s != space.name) {
                    return Err(SessionError::Unsupported(format!(
                        "a graph of '{}' over {}.{table}: a graph's tables belong to its space",
                        space.name,
                        s.as_deref().unwrap_or_default()
                    )));
                }
                if !tables.contains(table) {
                    return Err(
                        EngineError::TableNotFound(format!("{}.{table}", space.name)).into(),
                    );
                }
                Ok(format!(
                    "{}.{}",
                    quote_ident(&space.name),
                    quote_ident(table)
                ))
            }
            _ => Err(SessionError::Unsupported(
                "a graph inside a graph".to_string(),
            )),
        })?;
        let graph = GraphDef::new(&space.name, &request.graph, body);
        // DuckPGQ checks the definition; only one it accepts is kept.
        let sql = create_sql(&graph);
        engine.execute(&sql, &[]).await?;
        self.engines()
            .set_graph(&engine_name(&space.name, &request.graph), Some(sql));
        self.catalog.create_graph(graph, true)?;
        Ok(QueryResult::Empty)
    }

    /// `DROP PROPERTY GRAPH [IF EXISTS] <space>.<graph>`
    pub(crate) async fn drop_property_graph(
        &self,
        request: DropGraphRequest,
    ) -> Result<QueryResult, SessionError> {
        let space = self.graph_space(request.space.as_deref())?;
        if !self.catalog.drop_graph(&space.name, &request.graph)? {
            if request.if_exists {
                return Ok(QueryResult::Empty);
            }
            return Err(EngineError::TableNotFound(format!(
                "property graph {}.{}",
                space.name, request.graph
            ))
            .into());
        }
        let name = engine_name(&space.name, &request.graph);
        let engine = self.bind(&space).await?;
        engine.enable(Capabilities::EXT_GRAPH).await?;
        engine
            .execute(&format!("DROP PROPERTY GRAPH IF EXISTS {name}"), &[])
            .await?;
        self.engines().set_graph(&name, None);
        Ok(QueryResult::Empty)
    }

    /// A query using `GRAPH_TABLE`: it reads the graphs of one Lance Space,
    /// and only the node's owner runs it (a graph reads its tables past
    /// column grants and row policies).
    pub(crate) async fn graph_query(&self, query: GraphQuery) -> Result<QueryResult, SessionError> {
        self.deny_principal("graph queries")?;
        let mut space: Option<Space> = None;
        for (s, graph) in query.graphs() {
            let found = self.graph_space(s)?;
            if space.as_ref().is_some_and(|sp| sp.name != found.name) {
                return Err(SessionError::Unsupported(
                    "a graph query reads the graphs of one space".to_string(),
                ));
            }
            if self.catalog.get_graph(&found.name, graph).is_none() {
                return Err(EngineError::TableNotFound(format!(
                    "property graph {}.{graph}",
                    found.name
                ))
                .into());
            }
            space = Some(found);
        }
        let space = space.ok_or_else(|| SessionError::Unsupported("GRAPH_TABLE".to_string()))?;
        for (a, _) in &query.qualified {
            if *a != space.name && self.catalog.get_space(a).is_ok() {
                return Err(SessionError::Unsupported(format!(
                    "a graph query reads one space; '{a}' is another"
                )));
            }
        }
        let engine = self.graph_engine(&space).await?;
        let sql = render_graph_parts(&query.parts, |part| match part {
            GraphPart::Graph { graph, .. } => {
                Ok::<_, SessionError>(engine_name(&space.name, graph))
            }
            _ => Err(SessionError::Unsupported(
                "a table name in GRAPH_TABLE".to_string(),
            )),
        })?;
        let (schema, stream) = engine.query(&sql, &[]).await?;
        let batches = stream.try_collect().await?;
        Ok(QueryResult::Batches { schema, batches })
    }
}
