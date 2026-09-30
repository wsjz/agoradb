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

use agoradb_engine::EngineError;
use datafusion::error::DataFusionError;
use thiserror::Error;

/// Errors raised by the federation coordinator.
#[derive(Error, Debug)]
pub enum FederationError {
    #[error("federation planning failed: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error(transparent)]
    Engine(#[from] EngineError),
}

/// Wrap an engine error so DataFusion can carry it through a stream.
pub fn engine_to_df(e: EngineError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
