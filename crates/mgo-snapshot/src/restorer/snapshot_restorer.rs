//! Core snapshot restoration logic
//! 
//! This module implements the SnapshotRestorer which handles restoring snapshots
//! to the blockchain state.

use crate::types::{
    SnapshotId, SnapshotMetadata,
    error::{SnapshotError, SnapshotResult},
    config::{SnapshotConfig, RestoreOptions},
    storage::SnapshotStorage,
    validation::ValidationResult,
};
use crate::restorer::{StateApplier, SnapshotDecompressor, RestoreVerifier};



use chrono::{DateTime, Utc};
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::epoch::committee_store::CommitteeStore;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn, error, instrument};

/// Core snapshot restoration engine
/// 
/// Responsible for orchestrating the entire snapshot restoration process including
/// downloading, decompression, validation, and state application.
pub struct SnapshotRestorer {
    /// Configuration for snapshot restoration
    config: SnapshotConfig,
    
    /// State applier for applying restored data
    state_applier: Arc<StateApplier>,
    
    /// Decompressor for data decompression
    decompressor: Arc<SnapshotDecompressor>,
    
    /// Verifier for restoration verification
    verifier: Arc<RestoreVerifier>,
    
    /// Storage backend for retrieving snapshots
    storage: Arc<dyn SnapshotStorage>,
    
    /// Active restoration operations
    active_operations: Arc<RwLock<HashMap<SnapshotId, RestoreStatus>>>,
}

/// Status of an ongoing snapshot restoration operation
#[derive(Debug, Clone)]
pub struct RestoreStatus {
    /// Restoration start time
    pub started_at: DateTime<Utc>,
    
    /// Current phase
    pub phase: RestorePhase,
    
    /// Progress percentage (0-100)
    pub progress: u8,
    
    /// Human-readable status message
    pub message: String,
    
    /// Total bytes processed
    pub bytes_processed: u64,
    
    /// Estimated remaining time in seconds
    pub estimated_remaining_secs: Option<u64>,
}

/// Phases of snapshot restoration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestorePhase {
    /// Initializing the restoration process
    Initializing,
    
    /// Downloading snapshot from storage
    Downloading,
    
    /// Decompressing snapshot data
    Decompressing,
    
    /// Validating snapshot integrity
    Validating,
    
    /// Applying state to stores
    ApplyingState,
    
    /// Verifying restoration
    Verifying,
    
    /// Finalizing restoration
    Finalizing,
    
    /// Completed successfully
    Completed,
    
    /// Failed with error
    Failed(String),
}

/// Configuration for snapshot restoration request
#[derive(Debug, Clone)]
pub struct RestoreSnapshotRequest {
    /// ID of snapshot to restore
    pub snapshot_id: SnapshotId,
    
    /// Restoration options
    pub options: RestoreOptions,
    
    /// Whether to verify the restoration
    pub verify: bool,
    
    /// Whether to force restoration even if target epoch is newer
    pub force: bool,
    
    /// Custom metadata for the restoration
    pub metadata: HashMap<String, String>,
}

/// Result of snapshot restoration
#[derive(Debug, Clone)]
pub struct RestoreSnapshotResult {
    /// Restored snapshot ID
    pub snapshot_id: SnapshotId,
    
    /// Restored snapshot metadata
    pub metadata: SnapshotMetadata,
    
    /// Target epoch that was restored
    pub restored_epoch: u64,
    
    /// Target checkpoint sequence (if applicable)
    pub restored_checkpoint: Option<u64>,
    
    /// Restoration statistics
    pub stats: RestoreStats,
    
    /// Verification result
    pub verification: Option<ValidationResult>,
}

/// Statistics about snapshot restoration
#[derive(Debug, Clone)]
pub struct RestoreStats {
    /// Total time taken
    pub duration_secs: u64,
    
    /// Downloaded data size in bytes
    pub downloaded_size_bytes: u64,
    
    /// Decompressed size in bytes
    pub decompressed_size_bytes: u64,
    
    /// Applied data size in bytes
    pub applied_size_bytes: u64,
    
    /// Number of components restored
    pub component_count: usize,
    
    /// Number of objects restored
    pub object_count: u64,
    
    /// Verification time in seconds
    pub verification_time_secs: u64,
}

impl SnapshotRestorer {
    /// Create a new SnapshotRestorer
    pub fn new(
        config: SnapshotConfig,
        storage: Arc<dyn SnapshotStorage>,
    ) -> SnapshotResult<Self> {
        let state_applier = Arc::new(StateApplier::new(config.clone())?);
        let decompressor = Arc::new(SnapshotDecompressor::new()?);
        let validation_level = if config.validation.deep_validation {
            crate::types::config::ValidationLevel::Deep
        } else {
            crate::types::config::ValidationLevel::Basic
        };
        let verifier = Arc::new(RestoreVerifier::new(validation_level)?);
        
        Ok(Self {
            config,
            state_applier,
            decompressor,
            verifier,
            storage,
            active_operations: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Restore a snapshot to the blockchain state
    #[instrument(level = "info", skip(self, perpetual_db, checkpoint_store, committee_store))]
    pub async fn restore_snapshot(
        &self,
        request: RestoreSnapshotRequest,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> SnapshotResult<RestoreSnapshotResult> {
        let start_time = Utc::now();
        let snapshot_id = request.snapshot_id.clone();
        
        info!(
            snapshot_id = %snapshot_id,
            "Starting snapshot restoration"
        );
        
        // Initialize status tracking
        let status = RestoreStatus {
            started_at: start_time,
            phase: RestorePhase::Initializing,
            progress: 0,
            message: "Initializing snapshot restoration".to_string(),
            bytes_processed: 0,
            estimated_remaining_secs: None,
        };
        
        {
            let mut operations = self.active_operations.write().await;
            operations.insert(snapshot_id.clone(), status);
        }
        
        // Execute restoration pipeline
        let result = self.restore_snapshot_internal(
            request,
            perpetual_db,
            checkpoint_store,
            committee_store,
            start_time,
        ).await;
        
        // Clean up status tracking
        {
            let mut operations = self.active_operations.write().await;
            operations.remove(&snapshot_id);
        }
        
        match result {
            Ok(result) => {
                info!(
                    snapshot_id = %snapshot_id,
                    restored_epoch = result.restored_epoch,
                    duration_secs = result.stats.duration_secs,
                    "Snapshot restoration completed successfully"
                );
                Ok(result)
            }
            Err(error) => {
                error!(
                    snapshot_id = %snapshot_id,
                    error = %error,
                    "Snapshot restoration failed"
                );
                
                // Update status to failed
                self.update_status(&snapshot_id, RestorePhase::Failed(error.to_string()), 100).await;
                
                Err(error)
            }
        }
    }
    
    /// Internal snapshot restoration implementation
    async fn restore_snapshot_internal(
        &self,
        request: RestoreSnapshotRequest,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
        start_time: DateTime<Utc>,
    ) -> SnapshotResult<RestoreSnapshotResult> {
        let snapshot_id = request.snapshot_id.clone();
        
        // Phase 1: Download snapshot
        self.update_status(&snapshot_id, RestorePhase::Downloading, 10).await;
        
        let snapshot_data = self.storage.retrieve_snapshot(snapshot_id.clone()).await
            .map_err(|e| SnapshotError::Storage(e))?;
        
        let snapshot_metadata = self.storage.retrieve_metadata(snapshot_id.clone()).await
            .map_err(|e| SnapshotError::Storage(e))?;
        
        debug!(
            snapshot_id = %snapshot_id,
            downloaded_size = snapshot_data.data.len(),
            "Snapshot download completed"
        );
        
        // Phase 2: Decompress data
        self.update_status(&snapshot_id, RestorePhase::Decompressing, 25).await;
        
        let data_size = snapshot_data.data.len() as u64;
        let decompressed_data = if snapshot_metadata.compressed {
            self.decompressor.decompress(&snapshot_data.data).await?
        } else {
            snapshot_data.data
        };
        
        debug!(
            snapshot_id = %snapshot_id,
            decompressed_size = decompressed_data.len(),
            "Snapshot decompression completed"
        );
        
        // Phase 3: Validate snapshot
        self.update_status(&snapshot_id, RestorePhase::Validating, 40).await;
        
        let validation_start = Utc::now();
        let validation_result = if request.verify {
            Some(self.verifier.verify_snapshot(&decompressed_data, &snapshot_metadata).await?)
        } else {
            None
        };
        let verification_time = (Utc::now() - validation_start).num_seconds() as u64;
        
        // Phase 4: Apply state
        self.update_status(&snapshot_id, RestorePhase::ApplyingState, 60).await;
        
        let applied_size = self.state_applier.apply_snapshot_state(
            &decompressed_data,
            &snapshot_metadata,
            &request.options,
            perpetual_db.clone(),
            checkpoint_store.clone(),
            committee_store.clone(),
        ).await?;
        
        debug!(
            snapshot_id = %snapshot_id,
            applied_size = applied_size,
            "Snapshot state application completed"
        );
        
        // Phase 5: Verify restoration
        self.update_status(&snapshot_id, RestorePhase::Verifying, 80).await;
        
        if request.verify {
            self.verifier.verify_restoration(
                &snapshot_metadata,
                perpetual_db.clone(),
                checkpoint_store.clone(),
                committee_store.clone(),
            ).await?;
        }
        
        // Phase 6: Finalize
        self.update_status(&snapshot_id, RestorePhase::Finalizing, 90).await;
        
        let end_time = Utc::now();
        let duration = (end_time - start_time).num_seconds() as u64;
        
        let stats = RestoreStats {
            duration_secs: duration,
            downloaded_size_bytes: data_size,
            decompressed_size_bytes: decompressed_data.len() as u64,
            applied_size_bytes: applied_size,
            component_count: snapshot_metadata.components.len(),
            object_count: 0, // TODO: Count actual objects
            verification_time_secs: verification_time,
        };
        
        self.update_status(&snapshot_id, RestorePhase::Completed, 100).await;
        
        Ok(RestoreSnapshotResult {
            snapshot_id,
            metadata: snapshot_metadata.clone(),
            restored_epoch: snapshot_metadata.epoch,
            restored_checkpoint: snapshot_metadata.checkpoint_seq,
            stats,
            verification: validation_result,
        })
    }
    
    /// Update the status of an ongoing snapshot restoration
    async fn update_status(&self, snapshot_id: &SnapshotId, phase: RestorePhase, progress: u8) {
        let message = match &phase {
            RestorePhase::Initializing => "Initializing snapshot restoration".to_string(),
            RestorePhase::Downloading => "Downloading snapshot from storage".to_string(),
            RestorePhase::Decompressing => "Decompressing snapshot data".to_string(),
            RestorePhase::Validating => "Validating snapshot integrity".to_string(),
            RestorePhase::ApplyingState => "Applying snapshot state to blockchain stores".to_string(),
            RestorePhase::Verifying => "Verifying restoration completeness".to_string(),
            RestorePhase::Finalizing => "Finalizing snapshot restoration".to_string(),
            RestorePhase::Completed => "Snapshot restoration completed successfully".to_string(),
            RestorePhase::Failed(error) => format!("Snapshot restoration failed: {}", error),
        };
        
        let mut operations = self.active_operations.write().await;
        if let Some(status) = operations.get_mut(snapshot_id) {
            status.phase = phase;
            status.progress = progress;
            status.message = message;
        }
    }
    
    /// Get the status of an ongoing snapshot restoration
    pub async fn get_restore_status(&self, snapshot_id: &SnapshotId) -> Option<RestoreStatus> {
        let operations = self.active_operations.read().await;
        operations.get(snapshot_id).cloned()
    }
    
    /// List all active snapshot restoration operations
    pub async fn list_active_operations(&self) -> Vec<(SnapshotId, RestoreStatus)> {
        let operations = self.active_operations.read().await;
        operations.iter().map(|(id, status)| (id.clone(), status.clone())).collect()
    }
    
    /// Cancel an ongoing snapshot restoration
    pub async fn cancel_restoration(&self, snapshot_id: &SnapshotId) -> SnapshotResult<()> {
        warn!(snapshot_id = %snapshot_id, "Cancelling snapshot restoration");
        
        let mut operations = self.active_operations.write().await;
        if operations.remove(snapshot_id).is_some() {
            info!(snapshot_id = %snapshot_id, "Snapshot restoration cancelled");
            Ok(())
        } else {
            Err(SnapshotError::SnapshotNotFound { 
                id: snapshot_id.to_string() 
            })
        }
    }
    
    /// List available snapshots for restoration
    pub async fn list_available_snapshots(
        &self,
        filter: Option<&HashMap<String, String>>,
    ) -> SnapshotResult<Vec<SnapshotId>> {
        self.storage.list_snapshots(filter).await
            .map_err(|e| SnapshotError::Storage(e))
    }
    
    /// Get snapshot metadata
    pub async fn get_snapshot_metadata(&self, snapshot_id: &SnapshotId) -> SnapshotResult<SnapshotMetadata> {
        self.storage.retrieve_metadata(snapshot_id.clone()).await
            .map_err(|e| SnapshotError::Storage(e))
    }
    
    /// Check if a snapshot exists
    pub async fn snapshot_exists(&self, snapshot_id: &SnapshotId) -> SnapshotResult<bool> {
        self.storage.snapshot_exists(snapshot_id.clone()).await
            .map_err(|e| SnapshotError::Storage(e))
    }
}
