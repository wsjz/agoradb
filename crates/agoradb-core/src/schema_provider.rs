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

use crate::{DataType, ExecutionError};
use std::collections::HashMap;

/// Provides table schema information for the query analyzer.
///
/// Implementations can be backed by a catalog, a static map, or any other
/// schema source. The analyzer uses this trait to validate column references
/// and populate [`Scan`](crate::StagePlan::Scan) schemas.
pub trait SchemaProvider {
    /// Return the schema (column name → [`DataType`]) for a given table.
    fn get_table_schema(&self, table: &str) -> Result<HashMap<String, DataType>, ExecutionError>;
}
