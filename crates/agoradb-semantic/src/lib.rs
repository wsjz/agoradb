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
//! In 3.0-A/B this crate parses AgoraDB's own statements (`CREATE SPACE`,
//! `DROP SPACE`, `SET SPACE`), hands everything else to `sqlparser`, and
//! classifies statements so the session can route them. Views and UCAN
//! policy rewriting land here in 3.0-C.

pub mod classify;
pub mod error;
pub mod sql_parser;

pub use classify::{classify, qualify_tables, DmlKind, StatementClass, TclKind};
pub use error::SemanticError;
pub use sql_parser::{parse, parse_single, AgoraStatement};
