// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Storage backend types and interfaces

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{SnapshotData, SnapshotId, SnapshotMetadata};


/// Storage operation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageOpResult {
    /// Operation success status
    pub success: bool,
    /// Storage location identifier
    pub location: String,
    /// Size of stored data
    pub stored_size: u64,
    /// Storage duration in milliseconds
    pub duration_ms: u64,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl StorageOpResult {
    /// Create successful storage result
    pub fn success(location: String, stored_size: u64, duration_ms: u64) -> Self {
        Self {
            success: true,
            location,
            stored_size,
            duration_ms,
            metadata: HashMap::new(),
        }
    }

    /// Create failed storage result
    pub fn failure(location: String) -> Self {
        Self {
            success: false,
            location,
            stored_size: 0,
            duration_ms: 0,
            metadata: HashMap::new(),
        }
    }
}

/// Verification results for snapshot integrity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Whether verification passed
    pub valid: bool,
    /// Checksum verification status
    pub checksum_valid: bool,
    /// Size verification status
    pub size_valid: bool,
    /// Format verification status
    pub format_valid: bool,
    /// Any detected issues
    pub issues: Vec<String>,
    /// Verification duration in milliseconds
    pub duration_ms: u64,
}

impl VerificationResult {
    /// Create successful verification result
    pub fn valid() -> Self {
        Self {
            valid: true,
            checksum_valid: true,
            size_valid: true,
            format_valid: true,
            issues: Vec::new(),
            duration_ms: 0,
        }
    }

    /// Create failed verification result
    pub fn invalid(issues: Vec<String>) -> Self {
        Self {
            valid: false,
            checksum_valid: false,
            size_valid: false,
            format_valid: false,
            issues,
            duration_ms: 0,
        }
    }

    /// Add an issue to the verification result
    pub fn add_issue(&mut self, issue: String) {
        self.issues.push(issue);
        self.valid = false;
    }
}

/// Main storage backend trait
#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    /// Store snapshot data with metadata
    async fn store_snapshot(
        &self,
        snapshot_id: SnapshotId,
        data: SnapshotData,
        metadata: SnapshotMetadata,
    ) -> crate::types::error::StorageResult<StorageOpResult>;

    /// Retrieve snapshot data by ID
    async fn retrieve_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotData>;

    /// Retrieve snapshot metadata by ID
    async fn retrieve_metadata(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotMetadata>;

    /// Delete snapshot and its metadata
    async fn delete_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<()>;

    /// List available snapshots with optional filtering
    async fn list_snapshots(
        &self,
        filter: Option<&HashMap<String, String>>,
    ) -> crate::types::error::StorageResult<Vec<SnapshotId>>;

    /// Verify snapshot integrity
    async fn verify_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<VerificationResult>;

    /// Get storage statistics
    async fn get_storage_stats(&self) -> crate::types::error::StorageResult<StorageStats>;

    /// Check if snapshot exists
    async fn snapshot_exists(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<bool>;

    /// Get available storage space
    async fn get_available_space(&self) -> crate::types::error::StorageResult<u64>;

    /// Cleanup orphaned or corrupted data
    async fn cleanup_storage(&self) -> crate::types::error::StorageResult<CleanupResult>;
}

/// Storage backend statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Total storage capacity in bytes
    pub total_capacity: u64,
    /// Used storage space in bytes
    pub used_space: u64,
    /// Available storage space in bytes
    pub available_space: u64,
    /// Number of stored snapshots
    pub snapshot_count: u64,
    /// Total size of all snapshots
    pub total_snapshot_size: u64,
    /// Average snapshot size
    pub average_snapshot_size: u64,
    /// Storage backend type
    pub backend_type: String,
    /// Last updated timestamp
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl StorageStats {
    /// Calculate storage utilization percentage
    pub fn utilization_percentage(&self) -> f64 {
        if self.total_capacity == 0 {
            0.0
        } else {
            (self.used_space as f64 / self.total_capacity as f64) * 100.0
        }
    }

    /// Check if storage is nearly full
    pub fn is_nearly_full(&self, threshold: f64) -> bool {
        self.utilization_percentage() > threshold
    }
}

/// Result of storage cleanup operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupResult {
    /// Number of files cleaned up
    pub files_cleaned: u64,
    /// Total space reclaimed in bytes
    pub space_reclaimed: u64,
    /// Number of errors encountered
    pub errors_encountered: u64,
    /// Cleanup duration in milliseconds
    pub duration_ms: u64,
    /// Detailed cleanup log
    pub cleanup_log: Vec<String>,
}

impl CleanupResult {
    /// Create new cleanup result
    pub fn new() -> Self {
        Self {
            files_cleaned: 0,
            space_reclaimed: 0,
            errors_encountered: 0,
            duration_ms: 0,
            cleanup_log: Vec::new(),
        }
    }

    /// Add cleanup log entry
    pub fn log_entry(&mut self, entry: String) {
        self.cleanup_log.push(entry);
    }

    /// Record cleaned file
    pub fn record_cleaned_file(&mut self, size: u64) {
        self.files_cleaned += 1;
        self.space_reclaimed += size;
    }

    /// Record cleanup error
    pub fn record_error(&mut self, error: String) {
        self.errors_encountered += 1;
        self.log_entry(format!("ERROR: {}", error));
    }
}

impl Default for CleanupResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Delta storage for incremental snapshots
#[async_trait]
pub trait DeltaStorage: Send + Sync {
    /// Store delta data for incremental snapshots
    async fn store_delta(
        &self,
        snapshot_id: SnapshotId,
        base_snapshot: SnapshotId,
        delta_data: Vec<u8>,
    ) -> crate::types::error::StorageResult<StorageOpResult>;

    /// Retrieve delta data
    async fn retrieve_delta(
        &self,
        snapshot_id: SnapshotId,
    ) -> crate::types::error::StorageResult<(SnapshotId, Vec<u8>)>;

    /// Get delta chain for incremental snapshot
    async fn get_delta_chain(
        &self,
        base_snapshot: SnapshotId,
        target_snapshot: SnapshotId,
    ) -> crate::types::error::StorageResult<Vec<(SnapshotId, Vec<u8>)>>;

    /// Delete delta data
    async fn delete_delta(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<()>;
}

/// Distributed consensus interface for cluster snapshots
#[async_trait]
pub trait DistributedConsensus: Send + Sync {
    /// Coordinate snapshot creation across cluster
    async fn coordinate_snapshot_creation(
        &self,
        snapshot_type: super::SnapshotType,
        checkpoint_seq: u64,
    ) -> crate::types::error::StorageResult<super::ClusterSnapshotId>;

    /// Coordinate snapshot restoration across cluster
    async fn coordinate_snapshot_restoration(
        &self,
        cluster_snapshot_id: super::ClusterSnapshotId,
    ) -> crate::types::error::StorageResult<Vec<SnapshotId>>;

    /// Verify cluster-wide snapshot consistency
    async fn verify_cluster_consistency(
        &self,
        snapshots: &[SnapshotId],
    ) -> crate::types::error::StorageResult<bool>;

    /// Get cluster status
    async fn get_cluster_status(&self) -> crate::types::error::StorageResult<ClusterStatus>;
}

/// Cluster status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// Total number of nodes in cluster
    pub total_nodes: u32,
    /// Number of healthy nodes
    pub healthy_nodes: u32,
    /// Number of nodes currently taking snapshots
    pub snapshotting_nodes: u32,
    /// Current cluster epoch
    pub current_epoch: u64,
    /// Current checkpoint sequence
    pub current_checkpoint: u64,
    /// Last successful cluster snapshot
    pub last_cluster_snapshot: Option<super::ClusterSnapshotId>,
}

impl ClusterStatus {
    /// Check if cluster is healthy enough for snapshots
    pub fn is_healthy_for_snapshots(&self) -> bool {
        let health_ratio = self.healthy_nodes as f64 / self.total_nodes as f64;
        health_ratio >= 0.67 // At least 2/3 of nodes must be healthy
    }

    /// Check if cluster is busy with snapshots
    pub fn is_busy_snapshotting(&self) -> bool {
        let snapshot_ratio = self.snapshotting_nodes as f64 / self.total_nodes as f64;
        snapshot_ratio >= 0.5 // More than half of nodes are snapshotting
    }
}
