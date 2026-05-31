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

use agoradb_core::{CatalogError, SpaceUri};
use async_trait::async_trait;
use iceberg::expr::Predicate;
use iceberg::scan::ArrowRecordBatchStream;

#[async_trait]
pub trait StorageScanProvider: Send + Sync {
    /// Scan a table at the given snapshot and return a stream of RecordBatches.
    ///
    /// If `filter` is provided, row group and page-level filtering are enabled
    /// via Iceberg's predicate pushdown.
    async fn scan_table(
        &self,
        space: &SpaceUri,
        snapshot_id: i64,
        filter: Option<Predicate>,
    ) -> Result<ArrowRecordBatchStream, CatalogError>;
}
