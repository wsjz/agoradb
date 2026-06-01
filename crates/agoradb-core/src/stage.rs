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

use crate::operator::{AggFunction, JoinType, OperatorDef};

/// The kind of work a [`Stage`] performs.
///
/// Every variant carries the operator pipeline (`operators`) that feeds data
/// into this stage's processing logic.  The pipeline is executed by the
/// [`Executor`], which may create multiple parallel instances of the pipeline
/// when `stage.parallelism > 1`.
#[derive(Debug, Clone)]
pub enum StageTask {
    /// A linear pipeline with no pipeline breakers.
    Pipeline {
        operators: Vec<OperatorDef>,
    },

    /// Build the hash table side of a HashJoin.
    /// Must run single-threaded (needs complete build side).
    HashJoinBuild {
        join_id: usize,
        operators: Vec<OperatorDef>,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
    },

    /// Probe the hash table side of a HashJoin.
    /// Can be parallelized after build completes.
    HashJoinProbe {
        join_id: usize,
        operators: Vec<OperatorDef>,
        /// Operators to run *after* the join probe (e.g. Project, Limit).
        post_operators: Vec<OperatorDef>,
    },

    /// Accumulate aggregate state.
    /// Must run single-threaded (needs complete input).
    AggregateAccumulate {
        agg_id: usize,
        operators: Vec<OperatorDef>,
        group_columns: Vec<usize>,
        agg_columns: Vec<(usize, AggFunction)>,
    },

    /// Emit aggregate results.
    /// Usually single-threaded (small output).
    AggregateEmit {
        agg_id: usize,
        /// Operators to run *after* emitting aggregate results (e.g. Limit).
        post_operators: Vec<OperatorDef>,
    },
}

/// A single stage in a [`StagePlan`] — the unit of scheduling.
///
/// Produced by `StageBuilder` (query layer), consumed by `Executor`
/// (execution layer).  A stage is self-contained and can be serialized
/// and sent to a remote node for execution.
#[derive(Debug, Clone)]
pub struct Stage {
    pub id: usize,
    pub label: String,
    /// Stage IDs that must complete before this stage starts.
    pub dependencies: Vec<usize>,
    /// Target number of parallel workers for this stage.
    pub parallelism: usize,
    /// The kind of work this stage performs.
    pub task: StageTask,
}

/// The complete execution plan produced by `StageBuilder`.
#[derive(Debug, Clone)]
pub struct StagePlan {
    pub stages: Vec<Stage>,
}
