//! Types and data structures for rollback operations

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use crate::types::{SnapshotId, ComponentType};

/// Target for rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackTarget {
    /// Rollback to a specific epoch
    Epoch { epoch: u64 },
    /// Rollback to a specific checkpoint
    Checkpoint { checkpoint: u64 },
    /// Rollback using a specific snapshot
    Snapshot { snapshot_id: SnapshotId },
}

/// Options for rollback operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackOptions {
    /// Force rollback even if validation fails
    pub force: bool,
    /// Create backup before rollback
    pub create_backup: bool,
    /// Validation level for the rollback operation
    pub validation_level: ValidationLevel,
    /// Maximum time to wait for rollback completion
    pub timeout: Duration,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Whether to restart consensus processes automatically
    pub auto_restart_consensus: bool,
    /// Whether to sync network state after rollback
    pub auto_sync_network: bool,
    /// Custom components to include in rollback
    pub components: Vec<ComponentType>,
    /// Batch size for parallel operations
    pub batch_size: Option<usize>,
}

impl Default for RollbackOptions {
    fn default() -> Self {
        Self {
            force: false,
            create_backup: true,
            validation_level: ValidationLevel::Basic,
            timeout: Duration::from_secs(600),
            max_retries: 3,
            auto_restart_consensus: true,
            auto_sync_network: true,
            components: vec![
                ComponentType::AuthorityState,
                ComponentType::EpochStore,
                ComponentType::CheckpointStore,
                ComponentType::ConsensusState,
            ],
            batch_size: Some(1000),
        }
    }
}

/// Validation level for rollback operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ValidationLevel {
    /// No validation
    None,
    /// Basic validation (existence checks)
    Basic,
    /// Full validation (integrity checks)
    Full,
    /// Comprehensive validation (includes consistency checks)
    Comprehensive,
}

/// Result of a rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResult {
    /// Unique operation ID
    pub operation_id: String,
    /// Target that was rolled back to
    pub target: RollbackTarget,
    /// Number of items restored
    pub items_restored: u64,
    /// Total operation duration
    pub duration: Duration,
    /// Backup snapshot ID (if created)
    pub backup_snapshot_id: Option<SnapshotId>,
    /// Components that were rolled back
    pub components_restored: Vec<ComponentType>,
    /// Any warnings encountered during rollback
    pub warnings: Vec<String>,
    /// Final status
    pub status: RollbackStatus,
}

/// Status of a rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackStatus {
    /// Operation is being initialized
    Initializing,
    /// Validating rollback target
    Validating,
    /// Creating backup snapshot
    CreatingBackup,
    /// Stopping consensus processes
    StoppingConsensus,
    /// Performing database rollback
    RollingBackDatabase,
    /// Validating rollback result
    ValidatingResult,
    /// Restarting consensus processes
    RestartingConsensus,
    /// Syncing network state
    SyncingNetwork,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed { error: String },
    /// Operation was cancelled
    Cancelled,
}

/// Detailed system status for rollback operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackSystemStatus {
    /// Current epoch
    pub current_epoch: u64,
    /// Last executed checkpoint
    pub last_checkpoint: Option<u64>,
    /// Consensus process status
    pub consensus_status: ConsensusStatus,
    /// Database size information
    pub database_size_mb: f64,
    /// Available disk space
    pub available_disk_space_gb: f64,
    /// Active rollback operations
    pub active_operations: Vec<ActiveRollbackOperation>,
    /// Last rollback operation info
    pub last_rollback: Option<LastRollbackInfo>,
    /// System health warnings
    pub warnings: Vec<String>,
}

/// Consensus process status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusStatus {
    /// All processes running normally
    Running,
    /// Some processes not responding
    Degraded,
    /// All processes stopped
    Stopped,
    /// Processes in unknown state
    Unknown,
}

/// Information about an active rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveRollbackOperation {
    /// Unique operation ID
    pub operation_id: String,
    /// Type of rollback operation
    pub operation_type: String,
    /// When the operation started
    pub start_time: SystemTime,
    /// Current status
    pub status: RollbackStatus,
    /// Target for the rollback
    pub target: RollbackTarget,
    /// Progress percentage (0-100)
    pub progress_percentage: u8,
    /// Estimated time remaining
    pub estimated_time_remaining: Option<Duration>,
}

/// Information about the last rollback operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastRollbackInfo {
    /// When the last rollback completed
    pub completion_time: SystemTime,
    /// Target that was rolled back to
    pub target: RollbackTarget,
    /// Whether it was successful
    pub successful: bool,
    /// Any error message if failed
    pub error_message: Option<String>,
}

/// Validation result for rollback targets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the target is valid
    pub is_valid: bool,
    /// Detailed validation checks
    pub checks: HashMap<String, ValidationCheck>,
    /// Overall confidence score (0-100)
    pub confidence_score: u8,
    /// Recommendations for the user
    pub recommendations: Vec<String>,
}

/// Individual validation check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationCheck {
    /// Name of the check
    pub check_name: String,
    /// Whether this check passed
    pub passed: bool,
    /// Detailed message
    pub message: String,
    /// Severity level
    pub severity: ValidationSeverity,
}

/// Severity level for validation issues
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ValidationSeverity {
    /// Informational only
    Info,
    /// Warning - operation can proceed but with caution
    Warning,
    /// Error - operation should not proceed
    Error,
    /// Critical - operation will definitely fail
    Critical,
}

/// Progress tracking for rollback operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackProgress {
    /// Current phase of operation
    pub current_phase: RollbackStatus,
    /// Progress within current phase (0-100)
    pub phase_progress: u8,
    /// Overall progress (0-100)
    pub overall_progress: u8,
    /// Number of completed steps
    pub completed_steps: u32,
    /// Total number of steps
    pub total_steps: u32,
    /// Current operation description
    pub current_operation: String,
    /// Time elapsed since start
    pub elapsed_time: Duration,
    /// Estimated time remaining
    pub estimated_remaining_time: Option<Duration>,
}
