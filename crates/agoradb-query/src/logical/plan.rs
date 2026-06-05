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

use crate::{BinaryOp, LiteralValue};
pub use agoradb_core::{AggFunction, DataType, JoinType, SortDirection};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        alias: Option<String>,
        schema: Vec<(String, DataType)>,
    },
    Filter {
        predicate: LogicalExpr,
        input: Box<LogicalPlan>,
    },
    Project {
        expressions: Vec<(String, LogicalExpr)>,
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: LogicalExpr,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<(String, AggFunction, LogicalExpr)>,
    },
    Sort {
        expressions: Vec<(LogicalExpr, SortDirection)>,
        input: Box<LogicalPlan>,
    },
    Limit {
        skip: usize,
        fetch: usize,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(String),
    Literal(LiteralValue),
    BinaryOp {
        op: BinaryOp,
        left: Box<LogicalExpr>,
        right: Box<LogicalExpr>,
    },
    Function {
        name: String,
        args: Vec<LogicalExpr>,
    },
}
