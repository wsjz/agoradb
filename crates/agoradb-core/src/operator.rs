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

use crate::SpaceUri;

/// Supported data types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// A predicate definition — serializable, self-contained filter expression.
/// Used by [`OperatorSpec::Filter`] and [`OperatorSpec::Scan`].
#[derive(Debug, Clone)]
pub enum PredicateDef {
    Eq {
        column: usize,
        value: i64,
    },
    Neq {
        column: usize,
        value: i64,
    },
    Lt {
        column: usize,
        value: i64,
    },
    LtEq {
        column: usize,
        value: i64,
    },
    Gt {
        column: usize,
        value: i64,
    },
    GtEq {
        column: usize,
        value: i64,
    },
    And {
        left: Box<PredicateDef>,
        right: Box<PredicateDef>,
    },
    Or {
        left: Box<PredicateDef>,
        right: Box<PredicateDef>,
    },
}

/// An operator definition — a serializable description of a single operator
/// in a pipeline.  The [`Executor`] builds the actual runtime operator from
/// this definition.
#[derive(Debug, Clone)]
pub enum OperatorSpec {
    /// Read data from a table (space).
    Scan {
        space: SpaceUri,
        projection: Option<Vec<usize>>,
        filter: Option<PredicateDef>,
    },
    /// Filter rows using a predicate.
    Filter { predicate: PredicateDef },
    /// Project (select) columns.
    Project { columns: Vec<usize> },
    /// Limit the number of output rows.
    Limit { skip: usize, fetch: usize },
}
