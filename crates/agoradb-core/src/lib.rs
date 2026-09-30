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

//! Core types shared by every AgoraDB crate: errors, Space identity and constants.
//!
//! This crate is deliberately dependency-free (apart from `thiserror`) so that
//! it can be used by engines, the catalog and the federation layer alike.

pub mod constants;
pub mod error;
pub mod space;

pub use constants::*;
pub use error::{AgoraError, CatalogError, CompactionError, Result, StorageError};
pub use space::{AccessMode, CreateSpaceRequest, EngineKind, SpaceKind, SpaceUri};
