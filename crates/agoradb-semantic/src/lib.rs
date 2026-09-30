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

//! The semantic layer of AgoraDB (architecture v3 §6).
//!
//! This crate parses AgoraDB's own statements (`CREATE SPACE`,
//! `DROP SPACE`, `SET SPACE`), hands everything else to `sqlparser`, and
//! classifies statements so the session can route them. [`rewrite`] adds
//! views and per-principal column grants and row policies.

pub mod classify;
pub mod error;
pub mod rewrite;
pub mod sql_parser;

pub use classify::{
    classify, collect_spaces, cte_names, qualify_tables, DmlKind, StatementClass, TclKind,
};
pub use error::SemanticError;
pub use rewrite::{
    apply_access_control, inline_views, AccessResolver, RelationAccess, ViewResolver,
};
pub use sql_parser::{parse, parse_single, AgoraStatement};
