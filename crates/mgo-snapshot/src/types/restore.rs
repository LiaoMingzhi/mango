//! Restore operation configuration and types
//! 
//! This module defines types and options for snapshot restoration operations.

use serde::{Deserialize, Serialize};
use crate::types::config::ValidationLevel;
use crate::types::SnapshotId;

/// Options for snapshot restoration operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOptions {
    /// Validation level to apply during restoration
    pub validation_level: ValidationLevel,
    
    /// Whether to create a backup of current state before restoration
    pub backup_current: bool,
    
    /// Whether to force restoration even if validation fails
    pub force_restore: bool,
    
    /// Whether to restore data in parallel (when possible)
    pub parallel_restore: bool,
    
    /// Maximum number of retries for failed operations
    pub max_retries: u32,
    
    /// Timeout for restoration operations (in seconds)
    pub timeout_seconds: u64,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            validation_level: ValidationLevel::Basic,
            backup_current: true,
            force_restore: false,
            parallel_restore: true,
            max_retries: 3,
            timeout_seconds: 300, // 5 minutes
        }
    }
}

/// Request for snapshot restoration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreSnapshotRequest {
    /// ID of the snapshot to restore
    pub snapshot_id: SnapshotId,
    
    /// Validation level to apply during restoration
    pub validation_level: ValidationLevel,
    
    /// Whether to create a backup of current state before restoration
    pub create_backup: bool,
    
    /// Whether to force restoration even if validation fails
    pub force_restore: bool,
    
    /// Maximum number of retries for failed operations
    pub max_retries: u32,
    
    /// Timeout for restoration operations (in seconds)
    pub timeout_seconds: u64,
}

impl Default for RestoreSnapshotRequest {
    fn default() -> Self {
        Self {
            snapshot_id: SnapshotId::new(),
            validation_level: ValidationLevel::Basic,
            create_backup: true,
            force_restore: false,
            max_retries: 3,
            timeout_seconds: 300, // 5 minutes
        }
    }
}

/// Result of a restoration operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResult {
    /// Operation ID that was used for this restoration
    pub operation_id: String,
    
    /// ID of the snapshot that was restored
    pub snapshot_id: crate::types::SnapshotId,
    
    /// ID of the backup snapshot created (if any)
    pub backup_snapshot_id: Option<crate::types::SnapshotId>,
    
    /// Checkpoint sequence number after restoration
    pub restored_checkpoint: u64,
    
    /// Epoch number after restoration
    pub restored_epoch: u64,
    
    /// Timestamp when restoration completed
    pub restore_time: chrono::DateTime<chrono::Utc>,
    
    /// Validation result (if validation was performed)
    pub validation_result: Option<crate::types::validation::ValidationResult>,
}
