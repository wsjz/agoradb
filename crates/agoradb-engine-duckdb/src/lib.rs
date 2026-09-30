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

//! DuckDB as the analytical engine of AgoraDB.
//!
//! One in-memory [`DuckDbEngine`] serves every analytical Space of a node:
//! each Space becomes a DuckDB schema and each table a view over the Parquet
//! files of the Space's current (or pinned) Iceberg snapshot, so
//! `<space>.<table>` resolves natively and joins across analytical Spaces
//! are pushed down as a single statement.

mod engine;
pub mod types;

pub use engine::{DuckDbConfig, DuckDbEngine};
