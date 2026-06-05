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

//! Catalog helper for integration tests.

use agoradb_catalog::AgoraCatalog;
use iceberg::io::FileIO;
use std::path::PathBuf;
use std::sync::Arc;

/// Path to the pre-generated test data directory.
pub fn test_data_dir() -> PathBuf {
    // tests/ is a workspace member, so CARGO_MANIFEST_DIR points to tests/
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agora-local")
}

/// Load the pre-generated AgoraCatalog from `tests/agora-local/`.
///
/// Panics with a helpful message if the test data has not been generated yet.
pub async fn setup_catalog() -> Arc<AgoraCatalog> {
    let root = test_data_dir();
    assert!(
        root.exists(),
        "Test data not found at {}.\n\
         Run: cargo run -p test-data-gen --bin generate-test-data -- --force",
        root.display()
    );

    let root_path = root.to_str().unwrap().to_string();
    let file_io = FileIO::new_with_fs();
    Arc::new(AgoraCatalog::new(file_io, &root_path))
}
