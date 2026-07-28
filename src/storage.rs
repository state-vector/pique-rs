//! Storage backend abstraction — enables testing locally and running on S3.
//!
//! The key insight: the reader only needs two operations:
//! 1. Read the last N bytes (footer)
//! 2. Read a byte range [start, end)
//!
//! The writer only needs:
//! 1. Put a complete object
//!
//! This minimal interface maps cleanly to both local files and S3.

use std::ops::Range;
use std::path::{Path, PathBuf};

/// Trait for storage backends.
///
/// All operations are async to support S3's HTTP-based access pattern.
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    /// Read a byte range from an object.
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Vec<u8>, StorageError>;

    /// Read the last N bytes of an object (for footer reads).
    async fn read_tail(&self, path: &str, len: u64) -> Result<Vec<u8>, StorageError>;

    /// Get the total size of an object.
    async fn object_size(&self, path: &str) -> Result<u64, StorageError>;

    /// Read an entire object into memory.
    async fn read_all(&self, path: &str) -> Result<Vec<u8>, StorageError>;

    /// Write a complete object.
    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError>;

    /// Delete an object.
    async fn delete(&self, path: &str) -> Result<(), StorageError>;
}

// ---------------------------------------------------------------------------
// Local filesystem backend
// ---------------------------------------------------------------------------

/// Local filesystem backend — stores segments as files in a directory.
/// Used for testing and development.
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    /// Create a new local backend rooted at the given directory.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn resolve(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

#[async_trait::async_trait]
impl StorageBackend for LocalBackend {
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Vec<u8>, StorageError> {
        use std::io::{Read, Seek, SeekFrom};

        let file_path = self.resolve(path);
        let mut file =
            std::fs::File::open(&file_path).map_err(|e| StorageError::Io(e.to_string()))?;

        file.seek(SeekFrom::Start(range.start))
            .map_err(|e| StorageError::Io(e.to_string()))?;

        let len = (range.end - range.start) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| StorageError::Io(e.to_string()))?;

        Ok(buf)
    }

    async fn read_tail(&self, path: &str, len: u64) -> Result<Vec<u8>, StorageError> {
        use std::io::{Read, Seek, SeekFrom};

        let file_path = self.resolve(path);
        let mut file =
            std::fs::File::open(&file_path).map_err(|e| StorageError::Io(e.to_string()))?;

        let file_size = file
            .seek(SeekFrom::End(0))
            .map_err(|e| StorageError::Io(e.to_string()))?;

        if len > file_size {
            return Err(StorageError::Io(format!(
                "Requested tail {} bytes but file is only {} bytes",
                len, file_size
            )));
        }

        file.seek(SeekFrom::End(-(len as i64)))
            .map_err(|e| StorageError::Io(e.to_string()))?;

        let mut buf = vec![0u8; len as usize];
        file.read_exact(&mut buf)
            .map_err(|e| StorageError::Io(e.to_string()))?;

        Ok(buf)
    }

    async fn object_size(&self, path: &str) -> Result<u64, StorageError> {
        let file_path = self.resolve(path);
        let metadata =
            std::fs::metadata(&file_path).map_err(|e| StorageError::Io(e.to_string()))?;
        Ok(metadata.len())
    }

    async fn read_all(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let file_path = self.resolve(path);
        std::fs::read(&file_path).map_err(|e| StorageError::Io(e.to_string()))
    }

    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let file_path = self.resolve(path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::Io(e.to_string()))?;
        }
        std::fs::write(&file_path, data).map_err(|e| StorageError::Io(e.to_string()))
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let file_path = self.resolve(path);
        match std::fs::remove_file(&file_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// S3 backend
// ---------------------------------------------------------------------------

#[cfg(feature = "s3")]
pub struct S3Backend {
    client: aws_sdk_s3::Client,
    bucket: String,
}

#[cfg(feature = "s3")]
impl S3Backend {
    /// Create a new S3 backend.
    pub fn new(client: aws_sdk_s3::Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    /// Create from environment (uses default AWS config).
    pub async fn from_env(bucket: String) -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        Self { client, bucket }
    }

    /// Create with a custom endpoint (for MinIO/LocalStack).
    pub async fn with_endpoint(bucket: String, endpoint: &str) -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(s3_config);
        Self { client, bucket }
    }
}

#[cfg(feature = "s3")]
#[async_trait::async_trait]
impl StorageBackend for S3Backend {
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Vec<u8>, StorageError> {
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .range(range_header)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(bytes.to_vec())
    }

    async fn read_tail(&self, path: &str, len: u64) -> Result<Vec<u8>, StorageError> {
        let range_header = format!("bytes=-{}", len);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .range(range_header)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(bytes.to_vec())
    }

    async fn object_size(&self, path: &str) -> Result<u64, StorageError> {
        let resp = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(resp.content_length().unwrap_or(0) as u64)
    }

    async fn read_all(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(bytes.to_vec())
    }

    async fn put(&self, path: &str, data: Vec<u8>) -> Result<(), StorageError> {
        use aws_sdk_s3::primitives::ByteStream;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| StorageError::S3(format!("{e:#}")))?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(String),

    #[cfg(feature = "s3")]
    #[error("S3 error: {0}")]
    S3(String),

    #[error("Object not found: {0}")]
    NotFound(String),
}
