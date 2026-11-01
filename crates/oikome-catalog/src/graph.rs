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

//! Property graphs of Lance Spaces (architecture v3 §8). DuckPGQ keeps a
//! graph in the DuckDB database, which lives in memory, so the definitions
//! are kept here and created again in each new DuckDB.

use oikome_core::CatalogError;
use serde::{Deserialize, Serialize};

use crate::catalog::OikomeCatalog;
use crate::registry::now_ms;

/// A property graph over tables of one Space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphDef {
    pub space: String,
    pub name: String,
    /// `VERTEX TABLES (...) EDGE TABLES (...)`, every table qualified as
    /// `<space>.<table>`.
    pub body: String,
    pub created_at_ms: u64,
}

impl GraphDef {
    pub fn new(space: impl Into<String>, name: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            space: space.into(),
            name: name.into(),
            body: body.into(),
            created_at_ms: now_ms(),
        }
    }

    pub(crate) fn key(&self) -> (String, String) {
        (self.space.clone(), self.name.clone())
    }
}

impl OikomeCatalog {
    /// Store a graph; with `or_replace` an existing one of that name is
    /// replaced, otherwise it is an error.
    pub fn create_graph(&self, graph: GraphDef, or_replace: bool) -> Result<(), CatalogError> {
        self.get_space(&graph.space)?;
        self.mutate_registry(|reg| {
            let key = graph.key();
            if !or_replace && reg.graphs.contains_key(&key) {
                return Err(CatalogError::InvalidProperty(format!(
                    "property graph {}.{} already exists",
                    key.0, key.1
                )));
            }
            reg.graphs.insert(key, graph);
            Ok(())
        })
    }

    /// Look up a graph.
    pub fn get_graph(&self, space: &str, name: &str) -> Option<GraphDef> {
        self.with_registry(|reg| {
            reg.graphs
                .get(&(space.to_string(), name.to_string()))
                .cloned()
        })
    }

    /// Graphs of a Space, ordered by name.
    pub fn list_graphs(&self, space: &str) -> Vec<GraphDef> {
        self.with_registry(|reg| {
            reg.graphs
                .values()
                .filter(|g| g.space == space)
                .cloned()
                .collect()
        })
    }

    /// Remove a graph; returns whether it existed.
    pub fn drop_graph(&self, space: &str, name: &str) -> Result<bool, CatalogError> {
        self.mutate_registry(|reg| {
            Ok(reg
                .graphs
                .remove(&(space.to_string(), name.to_string()))
                .is_some())
        })
    }
}
