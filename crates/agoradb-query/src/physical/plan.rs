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

use agoradb_core::{AggFunction, JoinType};
use agoradb_core::SpaceUri;
use crate::{BinaryOp, LiteralValue};

/// A physical query plan — the output of the PhysicalPlanner.
///
/// This is a tree of physical operators that the `StageBuilder` consumes
/// to produce a [`StagePlan`](agoradb_core::StagePlan).
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Scan {
        space: SpaceUri,
        projection: Option<Vec<usize>>,
        filter: Option<PhysicalExpr>,
    },
    Filter {
        predicate: PhysicalExpr,
        input: Box<PhysicalPlan>,
    },
    Project {
        expressions: Vec<PhysicalExpr>,
        input: Box<PhysicalPlan>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_exprs: Vec<PhysicalExpr>,
        agg_exprs: Vec<(PhysicalExpr, AggFunction)>,
    },
    Sort {
        expressions: Vec<(PhysicalExpr, agoradb_core::SortDirection)>,
        input: Box<PhysicalPlan>,
    },
    Limit {
        skip: usize,
        fetch: usize,
        input: Box<PhysicalPlan>,
    },
}

/// A physical expression — column reference, literal, or binary operation.
#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    Column(usize),
    Literal(LiteralValue),
    BinaryOp {
        op: BinaryOp,
        left: Box<PhysicalExpr>,
        right: Box<PhysicalExpr>,
    },
}

