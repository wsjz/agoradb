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

pub mod chunk;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod limit;
pub mod operator;
pub mod parallel;
pub mod pipeline;
pub mod project;
pub mod runner;
pub mod scan;

pub use chunk::{ColumnVector, DataChunk, DataType};
pub use filter::{FilterOperator, PredicateFn};
pub use hash_aggregate::{AggFunc, HashAggregateOperator};
pub use hash_join::HashJoinOperator;
pub use limit::LimitOperator;
pub use operator::{Operator, SinkOperator};
pub use parallel::{MorselScheduler, ParallelExecutor};
pub use pipeline::{Pipeline, QueryExecutor};
pub use project::ProjectOperator;
pub use runner::run_physical_plan;
pub use scan::ScanOperator;
