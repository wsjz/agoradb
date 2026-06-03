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

use crate::operator::{AggFunction, JoinType, PredicateDef};
use crate::SpaceUri;

/// Identifier for a stage within an [`ExecutionPlan`].
pub type StageId = usize;

/// Identifier for a pipeline within a stage.
pub type PipelineId = usize;

/// Identifier for global state (e.g. hash join table, aggregate state).
pub type GlobalStateId = usize;

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// How data is partitioned across workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partitioning {
    /// Each worker processes a distinct subset of rows (e.g. file ranges).
    RowRange,
    /// Rows are hash-partitioned by the given column indices.
    Hash(Vec<usize>),
    /// All rows go to a single worker.
    Singleton,
}

/// The type of exchange between stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeType {
    /// Gather all partitions to a single output.
    Gather,
    /// Re-partition data using hash on given columns.
    HashPartition(Vec<usize>),
    /// Broadcast the same data to all consumers.
    Broadcast,
}

/// Specification for data exchange between stages.
#[derive(Debug, Clone)]
pub struct ExchangeSpec {
    pub exchange_type: ExchangeType,
    pub num_partitions: usize,
}

/// A single stage in an [`ExecutionPlan`] — the unit of scheduling.
///
/// Produced by `StageBuilder` (query layer), consumed by `Executor`
/// (execution layer).  A stage is self-contained and can be serialized
/// and sent to a remote node for execution.
#[derive(Debug, Clone)]
pub struct Stage {
    pub id: StageId,
    pub label: String,
    /// Stage IDs that must complete before this stage starts.
    pub dependencies: Vec<StageId>,
    /// Target number of parallel workers for this stage.
    pub parallelism: usize,
    /// The execution plan for this stage.
    pub plan: StagePlan,
    /// Output exchange specification (if this stage feeds into another).
    pub output: Option<ExchangeSpec>,
}

/// The complete execution plan produced by `StageBuilder`.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub stages: Vec<Stage>,
}

/// A single-stage execution plan — the plan fragment executed within one [`Stage`].
///
/// This is a simplified, serializable representation of the operator tree
/// that the execution layer can directly interpret without depending on
/// the query layer's `PhysicalPlan`.
#[derive(Debug, Clone)]
pub enum StagePlan {
    /// Read data from a table (space).
    Scan {
        space: SpaceUri,
        projection: Option<Vec<usize>>,
        filter: Option<PredicateDef>,
    },
    /// Filter rows using a predicate.
    Filter {
        predicate: PredicateDef,
        input: Box<StagePlan>,
    },
    /// Project (select) columns.
    Project {
        columns: Vec<usize>,
        input: Box<StagePlan>,
    },
    /// Limit the number of output rows.
    Limit {
        skip: usize,
        fetch: usize,
        input: Box<StagePlan>,
    },
    /// HashJoin — complete operator with two inputs.
    HashJoin {
        left: Box<StagePlan>,
        right: Box<StagePlan>,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
    },
    /// HashAggregate — complete operator with one input.
    HashAggregate {
        input: Box<StagePlan>,
        group_columns: Vec<usize>,
        agg_columns: Vec<(usize, AggFunction)>,
    },
    /// Sort — complete operator with one input.
    Sort {
        input: Box<StagePlan>,
        sort_columns: Vec<usize>,
        directions: Vec<SortDirection>,
        /// If set, only the top-K rows are retained.
        limit: Option<usize>,
    },
    /// Build the hash table side of a HashJoin.
    HashJoinBuild {
        join_id: usize,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
        input: Box<StagePlan>,
    },
    /// Probe the hash table side of a HashJoin.
    HashJoinProbe {
        join_id: usize,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
        input: Box<StagePlan>,
    },
    /// Accumulate aggregate state.
    HashAggregateAccumulate {
        agg_id: usize,
        group_columns: Vec<usize>,
        agg_columns: Vec<(usize, AggFunction)>,
        input: Box<StagePlan>,
    },
    /// Emit aggregate results.
    HashAggregateEmit {
        agg_id: usize,
        group_columns: Vec<usize>,
        agg_columns: Vec<(usize, AggFunction)>,
    },
    /// Read from an exchange (inter-stage data transfer).
    ExchangeSource,
    /// Collect all input data for sorting (pipeline breaker).
    SortCollect {
        sort_id: usize,
        sort_columns: Vec<usize>,
        directions: Vec<SortDirection>,
        /// If set, only the top-K rows are retained (Top-K optimization).
        limit: Option<usize>,
        input: Box<StagePlan>,
    },
    /// Emit sorted data.
    SortEmit {
        sort_id: usize,
    },
}
