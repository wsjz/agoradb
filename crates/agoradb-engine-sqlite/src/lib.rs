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

//! SQLite as the transactional engine of AgoraDB.
//!
//! One [`SqliteEngine`] serves one transactional Space: the Space's database
//! file is attached under the Space name, so user SQL written as
//! `<space>.<table>` resolves natively and `BEGIN` / `COMMIT` are scoped to
//! that Space. Results are converted row by row into Arrow batches
//! ([`convert`]).

pub mod convert;
mod engine;

pub use engine::SqliteEngine;
