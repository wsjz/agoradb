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

//! OpenDAL-based storage implementation for Iceberg integration.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::file_read::OpenDalFileRead;
use crate::file_write::OpenDalFileWrite;

/// Configuration for OpenDAL-backed Iceberg storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDalStorageConfig {
    /// Storage scheme: "fs", "s3", or "opfs".
    pub scheme: String,
    /// Root directory for file-system backends.
    pub root: Option<String>,
    /// S3 bucket name.
    pub bucket: Option<String>,
    /// S3 region.
    pub region: Option<String>,
    /// S3 endpoint URL (optional, for MinIO-compatible stores).
    pub endpoint: Option<String>,
}

/// OpenDAL-backed Iceberg storage.
///
/// Delegates all I/O operations to an OpenDAL [`Operator`], which is
/// lazily initialized on first access based on [`OpenDalStorageConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenDalStorage {
    config: OpenDalStorageConfig,
    #[serde(skip)]
    operator: Arc<Mutex<Option<Operator>>>,
}

impl OpenDalStorage {
    /// Create a new [`OpenDalStorage`] with the given configuration.
    pub fn new(config: OpenDalStorageConfig) -> Self {
        Self {
            config,
            operator: Arc::new(Mutex::new(None)),
        }
    }

    /// Get or lazily initialize the OpenDAL [`Operator`].
    fn operator(&self) -> Result<Operator> {
        {
            let guard = self.operator.lock().map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to lock operator: {e}"),
                )
            })?;
            if let Some(op) = guard.as_ref() {
                return Ok(op.clone());
            }
        }
        let op = self.build_operator()?;
        let mut guard = self.operator.lock().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to lock operator: {e}"),
            )
        })?;
        if guard.is_none() {
            *guard = Some(op.clone());
        }
        Ok(guard.as_ref().unwrap().clone())
    }

    /// Build an OpenDAL [`Operator`] from the current configuration.
    fn build_operator(&self) -> Result<Operator> {
        match self.config.scheme.as_str() {
            "fs" => {
                let root = self.config.root.as_deref().unwrap_or(".");
                let builder = opendal::services::Fs::default().root(root);
                Ok(Operator::new(builder)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("Failed to create Fs operator: {e}"),
                        )
                    })?
                    .finish())
            }
            "s3" => {
                let bucket = self.config.bucket.as_ref().ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "S3 bucket must be provided")
                })?;
                let mut builder = opendal::services::S3::default().bucket(bucket);
                if let Some(region) = &self.config.region {
                    builder = builder.region(region);
                }
                if let Some(endpoint) = &self.config.endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Ok(Operator::new(builder)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("Failed to create S3 operator: {e}"),
                        )
                    })?
                    .finish())
            }
            "opfs" => {
                let root = self.config.root.as_deref().unwrap_or("/");
                let builder = opendal::services::Fs::default().root(root);
                Ok(Operator::new(builder)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("Failed to create OPFS operator: {e}"),
                        )
                    })?
                    .finish())
            }
            other => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Unsupported storage scheme: {other}"),
            )),
        }
    }
}

#[async_trait]
#[typetag::serde(name = "opendal")]
impl Storage for OpenDalStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let op = self.operator()?;
        op.exists(path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to check existence of {path}: {e}"),
            )
        })
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let op = self.operator()?;
        let meta = op.stat(path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to get metadata for {path}: {e}"),
            )
        })?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let op = self.operator()?;
        let buf = op.read(path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to read {path}: {e}"),
            )
        })?;
        Ok(buf.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let op = self.operator()?;
        Ok(Box::new(OpenDalFileRead::new(op.clone(), path.to_string())))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let op = self.operator()?;
        let _ = op.write(path, bs).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to write {path}: {e}"),
            )
        })?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let op = self.operator()?;
        Ok(Box::new(OpenDalFileWrite::new(
            op.clone(),
            path.to_string(),
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let op = self.operator()?;
        op.delete(path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to delete {path}: {e}"),
            )
        })
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let op = self.operator()?;
        op.remove_all(path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to delete prefix {path}: {e}"),
            )
        })
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        let op = self.operator()?;
        while let Some(path) = paths.next().await {
            op.delete(&path).await.map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to delete {path}: {e}"),
                )
            })?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Factory for creating OpenDAL-backed storage instances.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OpenDalStorageFactory;

#[typetag::serde(name = "opendal")]
impl StorageFactory for OpenDalStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let scheme = config
            .get("scheme")
            .map(|s| s.as_str())
            .unwrap_or("fs")
            .to_string();
        let opendal_config = OpenDalStorageConfig {
            scheme,
            root: config.get("root").map(|s| s.to_string()),
            bucket: config.get("bucket").map(|s| s.to_string()),
            region: config.get("region").map(|s| s.to_string()),
            endpoint: config.get("endpoint").map(|s| s.to_string()),
        };
        Ok(Arc::new(OpenDalStorage::new(opendal_config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_fs_storage() -> (TempDir, OpenDalStorage) {
        let dir = TempDir::new().unwrap();
        let config = OpenDalStorageConfig {
            scheme: "fs".to_string(),
            root: Some(dir.path().to_string_lossy().to_string()),
            bucket: None,
            region: None,
            endpoint: None,
        };
        (dir, OpenDalStorage::new(config))
    }

    #[tokio::test]
    async fn test_fs_exists() {
        let (_dir, storage) = temp_fs_storage();

        // Nonexistent file returns false
        assert!(!storage.exists("not_found.txt").await.unwrap());

        // Write a file
        storage
            .write("hello.txt", Bytes::from("world"))
            .await
            .unwrap();

        // Now it exists
        assert!(storage.exists("hello.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_fs_read_write() {
        let (_dir, storage) = temp_fs_storage();

        storage
            .write("test.txt", Bytes::from("hello"))
            .await
            .unwrap();
        let data = storage.read("test.txt").await.unwrap();
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_fs_metadata() {
        let (_dir, storage) = temp_fs_storage();

        storage
            .write("meta.txt", Bytes::from("hello world"))
            .await
            .unwrap();
        let meta = storage.metadata("meta.txt").await.unwrap();
        assert_eq!(meta.size, 11);
    }

    #[tokio::test]
    async fn test_fs_delete() {
        let (_dir, storage) = temp_fs_storage();

        storage.write("del.txt", Bytes::from("bye")).await.unwrap();
        assert!(storage.exists("del.txt").await.unwrap());

        storage.delete("del.txt").await.unwrap();
        assert!(!storage.exists("del.txt").await.unwrap());
    }
}
