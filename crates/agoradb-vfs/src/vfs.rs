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

use agoradb_core::{VfsError, VfsScheme};
use opendal::{Entry, Operator};

/// Configuration for the virtual file system backend.
#[derive(Debug, Clone, Default)]
pub struct VfsConfig {
    /// Root directory for the local file-system backend.
    pub fs_root: Option<String>,
    /// S3 bucket name.
    pub s3_bucket: Option<String>,
    /// S3 region.
    pub s3_region: Option<String>,
    /// S3 endpoint URL (optional, for MinIO-compatible stores).
    pub s3_endpoint: Option<String>,
}

/// Virtual file system backed by OpenDAL.
#[derive(Debug, Clone)]
pub struct Vfs {
    operator: Operator,
    scheme: VfsScheme,
}

impl Vfs {
    /// Create a new VFS instance for the given scheme and configuration.
    pub fn new(scheme: VfsScheme, config: &VfsConfig) -> Result<Self, VfsError> {
        let operator = match scheme {
            VfsScheme::File => {
                let root = config
                    .fs_root
                    .as_ref()
                    .map_or_else(|| ".".to_string(), Clone::clone);
                let builder = opendal::services::Fs::default().root(&root);
                Operator::new(builder)
                    .map_err(|e| VfsError::OpenDal(e.to_string()))?
                    .finish()
            }
            VfsScheme::S3 => {
                let bucket = config
                    .s3_bucket
                    .as_ref()
                    .ok_or_else(|| {
                        VfsError::BackendNotSupported(
                            "S3 bucket must be provided for S3 scheme".to_string(),
                        )
                    })?
                    .clone();
                let mut builder = opendal::services::S3::default().bucket(&bucket);
                if let Some(region) = &config.s3_region {
                    builder = builder.region(region);
                }
                if let Some(endpoint) = &config.s3_endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Operator::new(builder)
                    .map_err(|e| VfsError::OpenDal(e.to_string()))?
                    .finish()
            }
        };
        Ok(Self { operator, scheme })
    }

    /// Write bytes to the given path.
    pub async fn write(&self, path: &str, data: Vec<u8>) -> Result<(), VfsError> {
        self.operator
            .write(path, data)
            .await
            .map_err(|e| VfsError::OpenDal(e.to_string()))?;
        Ok(())
    }

    /// Read all bytes from the given path.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, VfsError> {
        let buf = self
            .operator
            .read(path)
            .await
            .map_err(|e| VfsError::OpenDal(e.to_string()))?;
        Ok(buf.to_vec())
    }

    /// List entries under the given path.
    pub async fn list(&self, path: &str) -> Result<Vec<Entry>, VfsError> {
        self.operator
            .list(path)
            .await
            .map_err(|e| VfsError::OpenDal(e.to_string()))?
            .into_iter()
            .map(Ok)
            .collect()
    }

    /// Get metadata for the given path.
    pub async fn stat(&self, path: &str) -> Result<opendal::Metadata, VfsError> {
        self.operator
            .stat(path)
            .await
            .map_err(|e| VfsError::OpenDal(e.to_string()))
    }

    /// Delete the object at the given path.
    pub async fn delete(&self, path: &str) -> Result<(), VfsError> {
        self.operator
            .delete(path)
            .await
            .map_err(|e| VfsError::OpenDal(e.to_string()))
    }

    /// Return the VFS scheme.
    pub fn scheme(&self) -> VfsScheme {
        self.scheme
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_fs_write_and_read() {
        let dir = TempDir::new().unwrap();
        let config = VfsConfig {
            fs_root: Some(dir.path().to_string_lossy().to_string()),
            ..Default::default()
        };
        let vfs = Vfs::new(VfsScheme::File, &config).unwrap();

        vfs.write("hello.txt", b"world".to_vec()).await.unwrap();
        let data = vfs.read("hello.txt").await.unwrap();
        assert_eq!(data, b"world");
    }

    #[tokio::test]
    async fn test_fs_list() {
        let dir = TempDir::new().unwrap();
        let config = VfsConfig {
            fs_root: Some(dir.path().to_string_lossy().to_string()),
            ..Default::default()
        };
        let vfs = Vfs::new(VfsScheme::File, &config).unwrap();

        vfs.write("a.txt", b"a".to_vec()).await.unwrap();
        vfs.write("b.txt", b"b".to_vec()).await.unwrap();

        let entries = vfs.list("/").await.unwrap();
        let mut names: Vec<String> = entries
            .iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.name().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a.txt", "b.txt"]);
    }
}
