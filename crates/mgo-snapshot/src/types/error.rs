// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Error types for the snapshot system

use thiserror::Error;

/// Main error type for snapshot operations
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// I/O related errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// JSON serialization errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// YAML serialization errors
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Compression errors
    #[error("Compression error: {0}")]
    Compression(String),

    /// Decompression errors
    #[error("Decompression error: {0}")]
    Decompression(String),

    /// Checksum validation errors
    #[error("Checksum validation failed: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    /// Snapshot not found
    #[error("Snapshot not found: {id}")]
    SnapshotNotFound { id: String },

    /// Invalid snapshot format
    #[error("Invalid snapshot format: {reason}")]
    InvalidFormat { reason: String },

    /// Storage backend errors
    #[error("Storage backend error: {0}")]
    Storage(#[from] StorageError),

    /// Validation errors
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Timeout errors
    #[error("Operation timed out after {timeout_secs} seconds")]
    Timeout { timeout_secs: u64 },

    /// Resource exhaustion
    #[error("Resource exhausted: {resource}")]
    ResourceExhausted { resource: String },

    /// Concurrent access conflicts
    #[error("Concurrent access conflict: {operation}")]
    ConcurrentAccess { operation: String },

    /// Authority state errors
    #[error("Authority state error: {0}")]
    AuthorityState(String),

    /// Checkpoint store errors
    #[error("Checkpoint store error: {0}")]
    CheckpointStore(String),

    /// Epoch store errors
    #[error("Epoch store error: {0}")]
    EpochStore(String),

    /// Consensus errors
    #[error("Consensus error: {0}")]
    Consensus(String),

    /// Network errors
    #[error("Network error: {0}")]
    Network(String),

    /// Permission denied
    #[error("Permission denied: {operation}")]
    PermissionDenied { operation: String },

    /// Generic error with context
    #[error("Snapshot operation failed: {context}")]
    Generic { context: String },
}

/// Storage backend specific errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// Local file system errors
    #[error("Local storage error: {0}")]
    Local(#[from] std::io::Error),

    /// Network storage errors
    #[error("Network storage error: {0}")]
    Network(String),

    /// Authentication failures
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// Insufficient storage space
    #[error("Insufficient storage space: required {required_bytes} bytes, available {available_bytes} bytes")]
    InsufficientSpace {
        required_bytes: u64,
        available_bytes: u64,
    },

    /// Storage backend not available
    #[error("Storage backend not available: {backend}")]
    BackendUnavailable { backend: String },

    /// Data corruption detected
    #[error("Data corruption detected in storage: {details}")]
    DataCorruption { details: String },
}

/// Validation specific errors
#[derive(Error, Debug)]
pub enum ValidationError {
    /// Schema validation failure
    #[error("Schema validation failed: {field} - {reason}")]
    Schema { field: String, reason: String },

    /// Business logic validation failure
    #[error("Business logic validation failed: {rule} - {details}")]
    BusinessLogic { rule: String, details: String },

    /// Dependency validation failure
    #[error("Dependency validation failed: missing {dependency}")]
    Dependency { dependency: String },

    /// Consistency check failure
    #[error("Consistency check failed: {component} - {inconsistency}")]
    Consistency {
        component: String,
        inconsistency: String,
    },

    /// Version compatibility issues
    #[error("Version incompatible: snapshot version {snapshot_version}, system version {system_version}")]
    VersionIncompatible {
        snapshot_version: String,
        system_version: String,
    },
}

/// Result type for snapshot operations
pub type SnapshotResult<T> = Result<T, SnapshotError>;

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Result type for validation operations
pub type ValidationResult<T> = Result<T, ValidationError>;

impl From<SnapshotError> for StorageError {
    fn from(err: SnapshotError) -> Self {
        match err {
            SnapshotError::Storage(storage_err) => storage_err,
            SnapshotError::Io(io_err) => StorageError::Local(io_err),
            other => StorageError::DataCorruption {
                details: format!("Snapshot error: {}", other),
            },
        }
    }
}

impl SnapshotError {
    /// Create a configuration error
    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        Self::Configuration(msg.into())
    }

    /// Create a generic error with context
    pub fn generic<S: Into<String>>(context: S) -> Self {
        Self::Generic {
            context: context.into(),
        }
    }

    /// Create a timeout error
    pub fn timeout(timeout_secs: u64) -> Self {
        Self::Timeout { timeout_secs }
    }

    /// Create a resource exhausted error
    pub fn resource_exhausted<S: Into<String>>(resource: S) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
        }
    }

    /// Create a concurrent access error
    pub fn concurrent_access<S: Into<String>>(operation: S) -> Self {
        Self::ConcurrentAccess {
            operation: operation.into(),
        }
    }

    /// Create a permission denied error
    pub fn permission_denied<S: Into<String>>(operation: S) -> Self {
        Self::PermissionDenied {
            operation: operation.into(),
        }
    }

    /// Check if error is recoverable
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Timeout { .. }
            | Self::Network(_)
            | Self::Storage(StorageError::Network(_))
            | Self::Storage(StorageError::BackendUnavailable { .. })
            | Self::ConcurrentAccess { .. } => true,
            Self::ChecksumMismatch { .. }
            | Self::InvalidFormat { .. }
            | Self::PermissionDenied { .. }
            | Self::Storage(StorageError::Authentication(_))
            | Self::Validation(_) => false,
            _ => false,
        }
    }

    /// Check if error indicates corruption
    pub fn indicates_corruption(&self) -> bool {
        matches!(
            self,
            Self::ChecksumMismatch { .. }
                | Self::InvalidFormat { .. }
                | Self::Storage(StorageError::DataCorruption { .. })
        )
    }
}
