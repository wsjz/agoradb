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

//! OpenDAL-based file write adapter for Iceberg integration.

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::FileWrite;
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;

/// OpenDAL-based file write implementation.
#[derive(Debug)]
pub struct OpenDalFileWrite {
    operator: Operator,
    path: String,
    buffer: Vec<u8>,
}

impl OpenDalFileWrite {
    /// Create a new [`OpenDalFileWrite`] with the given operator and path.
    pub fn new(operator: Operator, path: String) -> Self {
        Self {
            operator,
            path,
            buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl FileWrite for OpenDalFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.buffer.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let bs = Bytes::from(std::mem::take(&mut self.buffer));
        self.operator.write(&self.path, bs).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to write {}: {e}", self.path),
            )
        })?;
        Ok(())
    }
}
