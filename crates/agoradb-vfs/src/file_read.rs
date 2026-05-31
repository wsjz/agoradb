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

//! OpenDAL-based file read adapter for Iceberg integration.

use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::FileRead;
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;

/// OpenDAL-based file read implementation.
#[derive(Debug, Clone)]
pub struct OpenDalFileRead {
    operator: Operator,
    path: String,
}

impl OpenDalFileRead {
    /// Create a new [`OpenDalFileRead`] with the given operator and path.
    pub fn new(operator: Operator, path: String) -> Self {
        Self { operator, path }
    }
}

#[async_trait]
impl FileRead for OpenDalFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let start = range.start;
        let end = range.end;

        let buf = self
            .operator
            .read_with(&self.path)
            .range(start..end)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Failed to read range {start}..{end} from {}: {e}",
                        self.path
                    ),
                )
            })?;

        Ok(buf.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_range_read() {
        let dir = TempDir::new().unwrap();
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(dir.path().to_str().unwrap()),
        )
        .unwrap()
        .finish();
        op.write("test.txt", "hello world").await.unwrap();

        let reader = OpenDalFileRead::new(op, "test.txt".to_string());
        let chunk = reader.read(0..5).await.unwrap();
        assert_eq!(chunk, Bytes::from("hello"));
    }
}
