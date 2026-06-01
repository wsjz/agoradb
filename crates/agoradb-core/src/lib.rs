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

pub mod constants;
pub mod error;
pub mod plan;
pub mod space;

pub use constants::*;
pub use error::{
    AgoraError, CatalogError, CompactionError, ExecutionError, StorageError, VfsError,
};
pub use plan::{AggFunction, BinaryOp, JoinType, PhysicalExpr, PhysicalPlan};
pub use space::{Mode, SpaceUri, StorageStrategy};

/// A unit of parallel work — a fixed-size row range within a single Parquet file.
/// The scheduler assigns morsels to worker threads. Default size: 10K rows.
#[derive(Debug, Clone)]
pub struct Morsel {
    pub file_path: String,
    pub row_start: usize,
    pub row_count: usize,
}

/// Supported VFS backend schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsScheme {
    File,
    S3,
}
