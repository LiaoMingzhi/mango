//! REST API request and response types
//! 
//! This module defines the data structures used in the snapshot REST API.

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::types::{
    SnapshotId, SnapshotType, SnapshotMetadata, ComponentType,
    ValidationLevel, CompressionType, SnapshotFilter, SnapshotInfo, CompressionLevel,
    validation::ValidationResult as SnapshotValidationResult,
};

/// Request to create a new snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    /// Type of snapshot to create
    pub snapshot_type: SnapshotType,
    /// Optional checkpoint sequence number
    pub checkpoint_seq: Option<u64>,
    /// Optional epoch number
    pub epoch: Option<u64>,
    /// Components to include in snapshot
    pub components: Vec<ComponentType>,
    /// Whether to compress the snapshot
    pub compress: bool,
    /// Compression type if compression is enabled
    pub compression_type: Option<CompressionType>,
    /// Description for the snapshot
    pub description: Option<String>,
    /// Tags for categorization
    pub tags: Vec<String>,
}

/// Response for snapshot creation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    /// ID of the created snapshot
    pub snapshot_id: SnapshotId,
    /// Current status of the creation process
    pub status: CreationStatus,
    /// Progress percentage (0-100)
    pub progress: u8,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Any error message if creation failed
    pub error: Option<String>,
}

/// Status of snapshot creation process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CreationStatus {
    /// Creation is queued
    Queued,
    /// Currently collecting state
    Collecting,
    /// Currently compressing data
    Compressing,
    /// Currently validating snapshot
    Validating,
    /// Currently uploading to storage
    Uploading,
    /// Creation completed successfully
    Completed,
    /// Creation failed
    Failed,
    /// Creation was cancelled
    Cancelled,
}

/// Request to restore from a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreSnapshotRequest {
    /// ID of the snapshot to restore from
    pub snapshot_id: SnapshotId,
    /// Validation level for restoration
    pub validation_level: ValidationLevel,
    /// Whether to create a backup before restoration
    pub create_backup: bool,
    /// Whether to force restoration even if validation fails
    pub force: bool,
    /// Target checkpoint to restore to (if different from snapshot)
    pub target_checkpoint: Option<u64>,
}

/// Response for snapshot restoration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreSnapshotResponse {
    /// ID of the restore operation
    pub operation_id: String,
    /// Current status of the restoration process
    pub status: RestoreStatus,
    /// Progress percentage (0-100)
    pub progress: u8,
    /// ID of backup snapshot if created
    pub backup_snapshot_id: Option<SnapshotId>,
    /// Validation result if completed
    pub validation_result: Option<SnapshotValidationResult>,
    /// Any error message if restoration failed
    pub error: Option<String>,
}

/// Status of snapshot restoration process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RestoreStatus {
    /// Restoration is queued
    Queued,
    /// Creating backup snapshot
    CreatingBackup,
    /// Downloading snapshot data
    Downloading,
    /// Decompressing snapshot data
    Decompressing,
    /// Validating snapshot integrity
    Validating,
    /// Applying snapshot state
    Applying,
    /// Verifying restoration completeness
    Verifying,
    /// Restoration completed successfully
    Completed,
    /// Restoration failed
    Failed,
    /// Restoration was cancelled
    Cancelled,
}

/// Request to list snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSnapshotsRequest {
    /// Filter criteria
    pub filter: Option<SnapshotFilter>,
    /// Maximum number of results to return
    pub limit: Option<u32>,
    /// Offset for pagination
    pub offset: Option<u32>,
    /// Sort order (asc/desc)
    pub sort_order: Option<SortOrder>,
    /// Field to sort by
    pub sort_by: Option<SortField>,
}

/// Sort order for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    /// Ascending order
    Ascending,
    /// Descending order
    Descending,
}

/// Fields available for sorting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortField {
    /// Sort by creation time
    CreatedAt,
    /// Sort by snapshot size
    Size,
    /// Sort by checkpoint sequence
    CheckpointSeq,
    /// Sort by epoch number
    Epoch,
}

/// Response for listing snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSnapshotsResponse {
    /// List of snapshot information
    pub snapshots: Vec<SnapshotInfo>,
    /// Total number of snapshots available
    pub total_count: u64,
    /// Whether there are more results available
    pub has_more: bool,
    /// Continuation token for pagination
    pub next_token: Option<String>,
}

/// Request to get snapshot details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSnapshotRequest {
    /// ID of the snapshot to retrieve
    pub snapshot_id: SnapshotId,
    /// Whether to include detailed validation info
    pub include_validation: Option<bool>,
}

/// Response with snapshot details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSnapshotResponse {
    /// Snapshot metadata
    pub metadata: SnapshotMetadata,
    /// Detailed validation result if requested
    pub validation_result: Option<SnapshotValidationResult>,
    /// Storage statistics
    pub storage_stats: Option<SnapshotStorageStats>,
}

/// Storage statistics for a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStorageStats {
    /// Total size on disk
    pub disk_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Uncompressed size
    pub uncompressed_size: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Number of files/chunks
    pub file_count: u32,
    /// Storage backend used
    pub storage_backend: String,
}

/// Request to delete a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSnapshotRequest {
    /// ID of the snapshot to delete
    pub snapshot_id: SnapshotId,
    /// Whether to force deletion even if snapshot is in use
    pub force: bool,
}

/// Response for snapshot deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSnapshotResponse {
    /// Whether deletion was successful
    pub success: bool,
    /// Amount of storage space freed
    pub space_freed: Option<u64>,
    /// Any error message if deletion failed
    pub error: Option<String>,
}

/// Request to get system metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetricsRequest {
    /// Time range for metrics (start)
    pub start_time: Option<DateTime<Utc>>,
    /// Time range for metrics (end)
    pub end_time: Option<DateTime<Utc>>,
    /// Metric granularity in seconds
    pub granularity: Option<u64>,
}

/// Response with system metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetricsResponse {
    /// Total number of snapshots
    pub total_snapshots: u64,
    /// Total storage used
    pub total_storage_used: u64,
    /// Average compression ratio
    pub average_compression_ratio: f64,
    /// Snapshot creation rate (per hour)
    pub creation_rate: f64,
    /// Snapshot restoration rate (per hour)
    pub restoration_rate: f64,
    /// Success rate for operations
    pub success_rate: f64,
    /// Current active operations
    pub active_operations: u32,
    /// Storage backend health
    pub storage_health: StorageHealthStatus,
}

/// Storage backend health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageHealthStatus {
    /// All storage backends are healthy
    Healthy,
    /// Some storage backends have issues
    Degraded,
    /// Storage backends are experiencing failures
    Unhealthy,
    /// Storage backends are unavailable
    Offline,
}

/// Generic API error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    /// Error code
    pub error_code: String,
    /// Human-readable error message
    pub message: String,
    /// Additional error details
    pub details: Option<serde_json::Value>,
    /// Request ID for tracking
    pub request_id: Option<String>,
    /// Timestamp of the error
    pub timestamp: DateTime<Utc>,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Overall system status
    pub status: HealthStatus,
    /// Version information
    pub version: String,
    /// Uptime in seconds
    pub uptime: u64,
    /// Component health statuses
    pub components: std::collections::HashMap<String, ComponentHealth>,
}

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    /// System is healthy
    Healthy,
    /// System has minor issues
    Warning,
    /// System has critical issues
    Critical,
}

/// Component health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component status
    pub status: HealthStatus,
    /// Status message
    pub message: Option<String>,
    /// Last check time
    pub last_check: DateTime<Utc>,
    /// Additional metrics
    pub metrics: Option<serde_json::Value>,
}

impl Default for CreateSnapshotRequest {
    fn default() -> Self {
        Self {
            snapshot_type: SnapshotType::Full {
                include_history: true,
                compression_level: CompressionLevel::Medium,
            },
            checkpoint_seq: None,
            epoch: None,
            components: vec![
                ComponentType::AuthorityState,
                ComponentType::CheckpointStore,
                ComponentType::EpochStore,
            ],
            compress: true,
            compression_type: Some(CompressionType::Zstd),
            description: None,
            tags: Vec::new(),
        }
    }
}

impl Default for RestoreSnapshotRequest {
    fn default() -> Self {
        Self {
            snapshot_id: SnapshotId::default(),
            validation_level: ValidationLevel::Basic,
            create_backup: true,
            force: false,
            target_checkpoint: None,
        }
    }
}

impl Default for ListSnapshotsRequest {
    fn default() -> Self {
        Self {
            filter: None,
            limit: Some(50),
            offset: Some(0),
            sort_order: Some(SortOrder::Descending),
            sort_by: Some(SortField::CreatedAt),
        }
    }
}
