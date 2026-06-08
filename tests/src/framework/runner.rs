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
//!
//! DEPRECATED: The self-built query pipeline has been replaced by Apache DataFusion.
//! Use `AgoraSessionContext::sql()` instead.

use agoradb_catalog::AgoraCatalog;
use agoradb_execution::chunk::DataChunk;
use agoradb_core::SchemaProvider;
use std::collections::HashMap;
use std::sync::Arc;

/// Run a SQL query through the complete pipeline and return result chunks.
///
/// DEPRECATED: This function is a stub. The old pipeline (Parser → Analyzer →
/// PhysicalPlanner → StageBuilder → Executor) has been removed in favor of
/// Apache DataFusion. Use `AgoraSessionContext::sql()` instead.
pub async fn run_sql_pipeline(
    _sql: &str,
    _catalog: &Arc<AgoraCatalog>,
    _schema_provider: &dyn SchemaProvider,
    _schema_map: &HashMap<String, usize>,
) -> Result<Vec<DataChunk>, agoradb_core::ExecutionError> {
    Err(agoradb_core::ExecutionError::OperatorError(
        "The self-built query pipeline has been removed. Use AgoraSessionContext::sql() with DataFusion instead.".to_string(),
    ))
}
