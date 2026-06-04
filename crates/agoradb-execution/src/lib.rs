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

pub mod adapters;
pub mod chunk;
pub mod executor;
pub mod filter;
pub mod hash_aggregate;
pub mod hash_join;
pub mod limit;
pub mod local_exchange;
pub mod operator;
pub mod morsel_scheduler;
pub mod parallel_executor;
pub mod pipeline;
pub mod pipeline_builder;
pub mod predicate_builder;
pub mod project;
pub mod scheduler;
pub mod scan;
pub mod sort;
pub mod source;

pub use agoradb_core::DataType;
pub use chunk::{ColumnVector, DataChunk};
pub use executor::Executor;
pub use filter::{FilterOperator, FilterPipelineOperator, PredicateFn};
pub use hash_aggregate::{HashAggregateAccumulateSink, HashAggregateEmitOperator, HashAggregateOperator};
pub use hash_join::{HashJoinBuildSink, HashJoinGlobalState, HashJoinOperator, HashJoinProbeOperator};
pub use limit::{LimitOperator, LimitPipelineOperator};
pub use local_exchange::{LocalExchangeBuffer, LocalExchangeSink, LocalExchangeSource};
pub use morsel_scheduler::MorselScheduler;
pub use operator::Operator;
pub use parallel_executor::ParallelExecutor;
pub use pipeline::{Pipeline, PipelineOperator, PipelineTask, Sink, TaskStatus, PipelineState};
pub use project::{ProjectOperator, ProjectPipelineOperator};
pub use scheduler::{TaskScheduler, SchedulerMetrics, spawn_workers};
pub use scan::ScanOperator;
pub use sort::{SortCollectSink, SortEmitOperator, SortState};
pub use source::{EmptySource, EmitSource, ExchangeSource, InMemorySource, Source, SourceResult, TableScanSource};
