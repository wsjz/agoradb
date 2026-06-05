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

//! Full pipeline runner for integration tests.

use agoradb_catalog::AgoraCatalog;
use agoradb_execution::chunk::DataChunk;
use agoradb_execution::executor::Executor;
use agoradb_core::SchemaProvider;
use agoradb_query::logical::analyzer::Analyzer;
use agoradb_query::parser::SqlParser;
use agoradb_query::physical::planner::PhysicalPlanner;
use agoradb_query::StageBuilder;
use std::collections::HashMap;
use std::sync::Arc;

/// Run a SQL query through the complete pipeline and return result chunks.
///
/// Pipeline: SQL → Parser → Analyzer → PhysicalPlanner → StageBuilder → Executor
pub async fn run_sql_pipeline(
    sql: &str,
    catalog: &Arc<AgoraCatalog>,
    schema_provider: &dyn SchemaProvider,
    schema_map: &HashMap<String, usize>,
) -> Result<Vec<DataChunk>, agoradb_core::ExecutionError> {
    let parser = SqlParser::new();
    let mut logical_plan = parser.parse(sql)?;

    let analyzer = Analyzer::new();
    analyzer.analyze(&mut logical_plan, schema_provider)?;

    let planner = PhysicalPlanner::new();
    let physical_plan = planner.plan(&logical_plan, schema_map)?;

    let stage_builder = StageBuilder::new();
    let stage_plan = stage_builder.build(&physical_plan)?;

    let executor = Executor::new();
    executor.execute(&stage_plan, catalog).await
}
