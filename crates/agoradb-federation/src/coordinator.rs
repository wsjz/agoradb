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

//! Assemble a DataFusion session whose only tables are federated views of
//! engine-attached tables, then run or explain a statement on it.

use std::collections::BTreeMap;
use std::sync::Arc;

use agoradb_engine::{QueryEngine, RecordBatchStream};
use arrow_schema::SchemaRef;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::optimizer::Optimizer;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};
use datafusion_federation::{
    FederatedQueryPlanner, FederatedTableProviderAdaptor, FederationOptimizerRule,
};
use futures::TryStreamExt;

use crate::error::FederationError;
use crate::executor::EngineSqlExecutor;
use crate::table::AgoraRemoteTable;

/// One table an engine has attached, with the schema DataFusion plans against.
#[derive(Debug, Clone)]
pub struct BoundTable {
    pub name: String,
    pub schema: SchemaRef,
    /// Iceberg snapshot the table is pinned to, for analytical Spaces.
    pub snapshot_id: Option<i64>,
}

/// A Space whose tables are attached to `engine` and ready to be queried.
#[derive(Clone)]
pub struct BoundSpace {
    pub space: String,
    pub engine: Arc<dyn QueryEngine>,
    pub tables: Vec<BoundTable>,
}

impl std::fmt::Debug for BoundSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundSpace")
            .field("space", &self.space)
            .field("engine", &self.engine.instance_id())
            .field("tables", &self.tables)
            .finish()
    }
}

/// Build the coordinator's session state.
///
/// Like [`default_session_state`], but the federation rule runs *after*
/// `push_down_filter` instead of right after `scalar_subquery_to_join`:
/// filters that sit above a join (`WHERE a.x = b.y AND b.z > 1`) are then
/// already pushed to the leaf they belong to when subtrees are federated,
/// so they become a `WHERE` in the engine SQL instead of a residual filter
/// evaluated in the coordinator.
fn session_state() -> SessionState {
    let mut rules = Optimizer::new().rules;
    let pos = rules
        .iter()
        .position(|r| r.name() == "push_down_filter")
        .map(|p| p + 1)
        .unwrap_or(rules.len());
    rules.insert(pos, Arc::new(FederationOptimizerRule::new()));
    SessionStateBuilder::new()
        .with_optimizer_rules(rules)
        .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
        .with_default_features()
        .build()
}

/// A DataFusion session over a fixed set of bound Spaces.
pub struct Coordinator {
    ctx: SessionContext,
    pins: BTreeMap<(String, String), Option<i64>>,
}

impl Coordinator {
    /// Build a session in which `<space>.<table>` resolves to a federated
    /// table backed by that Space's engine.
    pub fn new(spaces: &[BoundSpace]) -> Result<Self, FederationError> {
        let ctx = SessionContext::new_with_state(session_state());
        let default_catalog = ctx.state().config_options().catalog.default_catalog.clone();
        let catalog: Arc<dyn CatalogProvider> = ctx.catalog(&default_catalog).ok_or_else(|| {
            DataFusionError::Internal(format!("default catalog '{default_catalog}' missing"))
        })?;

        let mut pins = BTreeMap::new();
        for bound in spaces {
            let executor = Arc::new(EngineSqlExecutor::new(&bound.space, bound.engine.clone()));
            let provider = Arc::new(SQLFederationProvider::new(executor));
            let schema_provider = MemorySchemaProvider::new();
            for table in &bound.tables {
                let remote = AgoraRemoteTable::new(&bound.space, &table.name, table.schema.clone());
                let source = Arc::new(SQLTableSource::new_with_table(
                    provider.clone(),
                    Arc::new(remote),
                ));
                schema_provider.register_table(
                    table.name.clone(),
                    Arc::new(FederatedTableProviderAdaptor::new(source)),
                )?;
                pins.insert((bound.space.clone(), table.name.clone()), table.snapshot_id);
            }
            catalog.register_schema(&bound.space, Arc::new(schema_provider))?;
        }
        Ok(Self { ctx, pins })
    }

    /// Snapshot each analytical table was bound at when the coordinator was built.
    pub fn pins(&self) -> &BTreeMap<(String, String), Option<i64>> {
        &self.pins
    }

    /// Plan and execute `sql`, streaming the merged result.
    pub async fn run(&self, sql: &str) -> Result<(SchemaRef, RecordBatchStream), FederationError> {
        let df = self.ctx.sql(sql).await?;
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let stream = df
            .execute_stream()
            .await?
            .map_err(|e| agoradb_engine::EngineError::Sql(e.to_string()));
        Ok((schema, Box::pin(stream)))
    }

    /// The physical plan for `sql`, showing which subtrees were pushed down
    /// (`VirtualExecutionPlan name=<space> ... base_sql=<engine SQL>`).
    pub async fn explain(&self, sql: &str) -> Result<String, FederationError> {
        let plan = self.ctx.sql(sql).await?.create_physical_plan().await?;
        let text = displayable(plan.as_ref()).indent(true).to_string();
        Ok(text)
    }
}
