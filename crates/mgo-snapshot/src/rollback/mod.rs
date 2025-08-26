//! Blockchain state rollback functionality
//! 
//! This module provides high-level rollback capabilities for the Mango blockchain,
//! including rollback to specific epochs and checkpoints.

pub mod manager;
pub mod types;
pub mod operations;

pub use manager::RollbackManager;
pub use types::*;
pub use operations::*;

use crate::types::{SnapshotId, SnapshotResult};

/// Rollback capability trait for different blockchain components
#[async_trait::async_trait]
pub trait RollbackCapable {
    /// Check if component supports rollback to the specified epoch
    async fn can_rollback_to_epoch(&self, epoch: u64) -> SnapshotResult<bool>;
    
    /// Check if component supports rollback to the specified checkpoint  
    async fn can_rollback_to_checkpoint(&self, checkpoint: u64) -> SnapshotResult<bool>;
    
    /// Perform actual rollback operation
    async fn perform_rollback(&self, target: RollbackTarget) -> SnapshotResult<RollbackResult>;
}

/// High-level rollback operations interface
#[async_trait::async_trait]
pub trait RollbackOperations {
    /// Rollback to a specific epoch using the most suitable snapshot
    async fn rollback_to_epoch(
        &self, 
        epoch: u64, 
        options: RollbackOptions
    ) -> SnapshotResult<RollbackResult>;
    
    /// Rollback to a specific checkpoint
    async fn rollback_to_checkpoint(
        &self, 
        checkpoint: u64, 
        options: RollbackOptions
    ) -> SnapshotResult<RollbackResult>;
    
    /// Rollback using a specific snapshot ID
    async fn rollback_from_snapshot(
        &self, 
        snapshot_id: &SnapshotId, 
        options: RollbackOptions
    ) -> SnapshotResult<RollbackResult>;
    
    /// Get rollback status and progress
    async fn get_rollback_status(&self, operation_id: &str) -> SnapshotResult<RollbackStatus>;
    
    /// Cancel an ongoing rollback operation
    async fn cancel_rollback(&self, operation_id: &str) -> SnapshotResult<()>;
    
    /// Validate that a rollback target is feasible
    async fn validate_rollback_target(&self, target: &RollbackTarget) -> SnapshotResult<ValidationResult>;
}
