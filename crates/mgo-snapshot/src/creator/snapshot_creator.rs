//! Core snapshot creation logic
//! 
//! This module implements the SnapshotCreator which handles creating snapshots
//! from the current blockchain state.

use crate::types::{
    SnapshotId, SnapshotData, SnapshotMetadata, SnapshotType, ComponentType,
    error::{SnapshotError, SnapshotResult},
    config::SnapshotConfig,
    storage::SnapshotStorage,
};
use crate::creator::{StateCollector, SnapshotCompressor, SnapshotValidator, ObjectStoreSnapshot, TransactionStoreSnapshot, CheckpointStoreSnapshot};



use chrono::{DateTime, Utc};
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::epoch::committee_store::CommitteeStore;
use mgo_types::accumulator::Accumulator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug, warn, error, instrument};


/// Core snapshot creation engine
/// 
/// Responsible for orchestrating the entire snapshot creation process including
/// state collection, compression, validation, and storage.
#[allow(dead_code)]
pub struct SnapshotCreator {
    /// Configuration for snapshot creation
    config: SnapshotConfig,
    
    /// State collector for gathering blockchain data
    state_collector: Arc<StateCollector>,
    
    /// Compressor for data compression
    compressor: Arc<SnapshotCompressor>,
    
    /// Validator for snapshot validation
    validator: Arc<SnapshotValidator>,
    
    /// Storage backend for persisting snapshots
    storage: Arc<dyn SnapshotStorage>,
    
    /// Active snapshot creation operations
    active_operations: Arc<RwLock<HashMap<SnapshotId, SnapshotCreationStatus>>>,
}

/// Status of an ongoing snapshot creation operation
#[derive(Debug, Clone)]
pub struct SnapshotCreationStatus {
    /// Creation start time
    pub started_at: DateTime<Utc>,
    
    /// Current phase
    pub phase: CreationPhase,
    
    /// Progress percentage (0-100)
    pub progress: u8,
    
    /// Human-readable status message
    pub message: String,
    
    /// Total bytes processed
    pub bytes_processed: u64,
    
    /// Estimated remaining time in seconds
    pub estimated_remaining_secs: Option<u64>,
}

/// Phases of snapshot creation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreationPhase {
    /// Initializing the creation process
    Initializing,
    
    /// Collecting state data from stores
    CollectingState,
    
    /// Compressing collected data
    Compressing,
    
    /// Validating the created snapshot
    Validating,
    
    /// Uploading to storage
    Uploading,
    
    /// Finalizing metadata
    Finalizing,
    
    /// Completed successfully
    Completed,
    
    /// Failed with error
    Failed(String),
}

/// Configuration for snapshot creation request
#[derive(Debug, Clone)]
pub struct CreateSnapshotRequest {
    /// Type of snapshot to create
    pub snapshot_type: SnapshotType,
    
    /// Target epoch for the snapshot
    pub epoch: u64,
    
    /// Specific checkpoint sequence number (for checkpoint snapshots)
    pub checkpoint_seq: Option<u64>,
    
    /// Components to include in the snapshot
    pub components: Vec<ComponentType>,
    
    /// Whether to compress the snapshot
    pub compress: bool,
    
    /// Whether to encrypt the snapshot
    pub encrypt: bool,
    
    /// Custom metadata to attach
    pub metadata: HashMap<String, String>,
    
    /// Force creation even if similar snapshot exists
    pub force: bool,
}

/// Result of snapshot creation
#[derive(Debug, Clone)]
pub struct CreateSnapshotResult {
    /// Created snapshot ID
    pub snapshot_id: SnapshotId,
    
    /// Snapshot metadata
    pub metadata: SnapshotMetadata,
    
    /// Creation statistics
    pub stats: CreationStats,
}

/// Statistics about snapshot creation
#[derive(Debug, Clone)]
pub struct CreationStats {
    /// Total time taken
    pub duration_secs: u64,
    
    /// Original data size in bytes
    pub original_size_bytes: u64,
    
    /// Compressed size in bytes
    pub compressed_size_bytes: u64,
    
    /// Compression ratio (0.0 - 1.0)
    pub compression_ratio: f64,
    
    /// Number of components included
    pub component_count: usize,
    
    /// Number of objects included
    pub object_count: u64,
    
    /// Validation time in seconds
    pub validation_time_secs: u64,
}

impl SnapshotCreator {
    /// Create a new SnapshotCreator
    pub fn new(
        config: SnapshotConfig,
        storage: Arc<dyn SnapshotStorage>,
    ) -> SnapshotResult<Self> {
        let state_collector = Arc::new(StateCollector::new(config.clone())?);
        let compressor = Arc::new(SnapshotCompressor::new(config.storage.local.compression)?);
        let validation_level = if config.validation.deep_validation {
            crate::types::config::ValidationLevel::Deep
        } else {
            crate::types::config::ValidationLevel::Basic
        };
        let validator = Arc::new(SnapshotValidator::new(validation_level)?);
        
        Ok(Self {
            config,
            state_collector,
            compressor,
            validator,
            storage,
            active_operations: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Create a snapshot from the current blockchain state
    #[instrument(level = "info", skip(self, perpetual_db, checkpoint_store, committee_store))]
    pub async fn create_snapshot(
        &self,
        request: CreateSnapshotRequest,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> SnapshotResult<CreateSnapshotResult> {
        let start_time = Utc::now();
        let snapshot_id = SnapshotId::new();
        
        info!(
            snapshot_id = %snapshot_id,
            snapshot_type = ?request.snapshot_type,
            epoch = request.epoch,
            "Starting snapshot creation"
        );
        
        // Initialize status tracking
        let status = SnapshotCreationStatus {
            started_at: start_time,
            phase: CreationPhase::Initializing,
            progress: 0,
            message: "Initializing snapshot creation".to_string(),
            bytes_processed: 0,
            estimated_remaining_secs: None,
        };
        
        {
            let mut operations = self.active_operations.write().await;
            operations.insert(snapshot_id.clone(), status);
        }
        
        // Execute creation pipeline
        let result = self.create_snapshot_internal(
            snapshot_id.clone(),
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
                    duration_secs = result.stats.duration_secs,
                    compressed_size = result.stats.compressed_size_bytes,
                    "Snapshot creation completed successfully"
                );
                Ok(result)
            }
            Err(error) => {
                error!(
                    snapshot_id = %snapshot_id,
                    error = %error,
                    "Snapshot creation failed"
                );
                
                // Update status to failed
                self.update_status(&snapshot_id, CreationPhase::Failed(error.to_string()), 100).await;
                
                Err(error)
            }
        }
    }
    
    /// Internal snapshot creation implementation
    async fn create_snapshot_internal(
        &self,
        snapshot_id: SnapshotId,
        request: CreateSnapshotRequest,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
        start_time: DateTime<Utc>,
    ) -> SnapshotResult<CreateSnapshotResult> {
        // Phase 1: Collect state data
        self.update_status(&snapshot_id, CreationPhase::CollectingState, 10).await;
        
        let collected_data = self.state_collector.collect_state(
            &request.snapshot_type,
            request.epoch,
            &request.components,
            perpetual_db,
            checkpoint_store,
            committee_store,
        ).await.map_err(|e| SnapshotError::StateCollection { 
            component: "all".to_string(),
            details: e.to_string()
        })?;
        
        debug!(
            snapshot_id = %snapshot_id,
            collected_bytes = collected_data.total_size(),
            "State collection completed"
        );
        
        // Phase 2: Compress data if requested
        self.update_status(&snapshot_id, CreationPhase::Compressing, 40).await;
        
        let uncompressed_size = collected_data.total_size();
        
        let compressed_data = if request.compress {
            self.compressor.compress(&collected_data).await?
        } else {
            collected_data.clone()
        };
        
        // Phase 3: Validate snapshot
        self.update_status(&snapshot_id, CreationPhase::Validating, 60).await;
        
        let validation_start = Utc::now();
        self.validator.validate_snapshot(&compressed_data).await?;
        let validation_time = (Utc::now() - validation_start).num_seconds() as u64;
        
        // Phase 4: Upload to storage
        self.update_status(&snapshot_id, CreationPhase::Uploading, 75).await;
        
        let mut metadata = SnapshotMetadata::new(
            snapshot_id.clone(),
            request.snapshot_type.clone(),
            request.checkpoint_seq,
            request.epoch,
            request.components.clone(),
        );
        metadata.created_at = start_time;
        let compressed_size = compressed_data.total_size();
        let checksum = compressed_data.compute_checksum()?;
        let data_bytes = compressed_data.clone().into_bytes()?;
        
        metadata.compressed_size = compressed_size;
        metadata.uncompressed_size = uncompressed_size;
        metadata.compressed = request.compress;
        metadata.encrypted = request.encrypt;
        metadata.checksum = checksum;
        metadata.custom_metadata = request.metadata;
        
        let snapshot_data = SnapshotData {
            metadata: metadata.clone(),
            data: data_bytes,
        };
        
        self.storage.store_snapshot(
            snapshot_id.clone(),
            snapshot_data,
            metadata.clone(),
        ).await.map_err(|e| SnapshotError::Storage(e))?;
        
        // Phase 5: Finalize
        self.update_status(&snapshot_id, CreationPhase::Finalizing, 90).await;
        
        let end_time = Utc::now();
        let duration = (end_time - start_time).num_seconds() as u64;
        
        let stats = CreationStats {
            duration_secs: duration,
            original_size_bytes: uncompressed_size,
            compressed_size_bytes: compressed_size,
            compression_ratio: if uncompressed_size > 0 {
                compressed_size as f64 / uncompressed_size as f64
            } else {
                1.0
            },
            component_count: request.components.len(),
            object_count: self.count_objects_in_collected_data(&collected_data).await,
            validation_time_secs: validation_time,
        };
        
        self.update_status(&snapshot_id, CreationPhase::Completed, 100).await;
        
        Ok(CreateSnapshotResult {
            snapshot_id,
            metadata,
            stats,
        })
    }
    
    /// Update the status of an ongoing snapshot creation
    async fn update_status(&self, snapshot_id: &SnapshotId, phase: CreationPhase, progress: u8) {
        let message = match &phase {
            CreationPhase::Initializing => "Initializing snapshot creation".to_string(),
            CreationPhase::CollectingState => "Collecting state data from blockchain stores".to_string(),
            CreationPhase::Compressing => "Compressing snapshot data".to_string(),
            CreationPhase::Validating => "Validating snapshot integrity".to_string(),
            CreationPhase::Uploading => "Uploading snapshot to storage".to_string(),
            CreationPhase::Finalizing => "Finalizing snapshot metadata".to_string(),
            CreationPhase::Completed => "Snapshot creation completed successfully".to_string(),
            CreationPhase::Failed(error) => format!("Snapshot creation failed: {}", error),
        };
        
        let mut operations = self.active_operations.write().await;
        if let Some(status) = operations.get_mut(snapshot_id) {
            status.phase = phase;
            status.progress = progress;
            status.message = message;
        }
    }
    
    /// Get the status of an ongoing snapshot creation
    pub async fn get_creation_status(&self, snapshot_id: &SnapshotId) -> Option<SnapshotCreationStatus> {
        let operations = self.active_operations.read().await;
        operations.get(snapshot_id).cloned()
    }
    
    /// List all active snapshot creation operations
    pub async fn list_active_operations(&self) -> Vec<(SnapshotId, SnapshotCreationStatus)> {
        let operations = self.active_operations.read().await;
        operations.iter().map(|(id, status)| (id.clone(), status.clone())).collect()
    }

    /// Count objects in collected state data
    async fn count_objects_in_collected_data(&self, collected_data: &CollectedStateData) -> u64 {
        let mut total_objects = 0u64;

        // Count objects in object store data
        if let Some(ref object_data) = collected_data.object_store {
            if let Ok(object_snapshot) = bcs::from_bytes::<ObjectStoreSnapshot>(object_data) {
                total_objects += object_snapshot.objects.len() as u64;
            }
        }

        // Count transactions (also considered objects in a broader sense)
        if let Some(ref tx_data) = collected_data.transaction_store {
            if let Ok(tx_snapshot) = bcs::from_bytes::<TransactionStoreSnapshot>(tx_data) {
                total_objects += tx_snapshot.transactions.len() as u64;
            }
        }

        // Count checkpoints
        if let Some(ref checkpoint_data) = collected_data.checkpoint_store {
            if let Ok(checkpoint_snapshot) = bcs::from_bytes::<CheckpointStoreSnapshot>(checkpoint_data) {
                total_objects += checkpoint_snapshot.checkpoints.len() as u64;
            }
        }

        total_objects
    }
    
    /// Cancel an ongoing snapshot creation
    pub async fn cancel_creation(&self, snapshot_id: &SnapshotId) -> SnapshotResult<()> {
        warn!(snapshot_id = %snapshot_id, "Cancelling snapshot creation");
        
        let mut operations = self.active_operations.write().await;
        if operations.remove(snapshot_id).is_some() {
            info!(snapshot_id = %snapshot_id, "Snapshot creation cancelled");
            Ok(())
        } else {
            Err(SnapshotError::SnapshotNotFound { 
                id: snapshot_id.to_string() 
            })
        }
    }
}

/// Collected state data from blockchain stores
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectedStateData {
    /// Authority state data
    pub authority_state: Option<Vec<u8>>,
    
    /// Epoch store data
    pub epoch_store: Option<Vec<u8>>,
    
    /// Checkpoint store data
    pub checkpoint_store: Option<Vec<u8>>,
    
    /// Object store data
    pub object_store: Option<Vec<u8>>,
    
    /// Transaction store data
    pub transaction_store: Option<Vec<u8>>,
    
    /// Index store data
    pub index_store: Option<Vec<u8>>,
    
    /// Consensus state data
    pub consensus_state: Option<Vec<u8>>,
    
    /// Associated accumulator
    pub accumulator: Option<Accumulator>,
    
    /// Collection epoch
    pub epoch: u64,
    
    /// Collection checkpoint sequence
    pub checkpoint_seq: u64,
    
    /// Collection timestamp
    pub collection_time: chrono::DateTime<chrono::Utc>,
}

impl CollectedStateData {
    /// Create new empty CollectedStateData
    pub fn new() -> Self {
        Self {
            authority_state: None,
            epoch_store: None,
            checkpoint_store: None,
            object_store: None,
            transaction_store: None,
            index_store: None,
            consensus_state: None,
            accumulator: None,
            epoch: 0,
            checkpoint_seq: 0,
            collection_time: chrono::Utc::now(),
        }
    }

    /// Calculate total size of collected data
    pub fn total_size(&self) -> u64 {
        [
            &self.authority_state,
            &self.epoch_store,
            &self.checkpoint_store,
            &self.object_store,
            &self.transaction_store,
            &self.index_store,
            &self.consensus_state,
        ]
        .iter()
        .filter_map(|data| data.as_ref())
        .map(|data| data.len() as u64)
        .sum()
    }
    
    /// Get object count by analyzing collected state data
    pub fn object_count(&self) -> u64 {
        let mut total_objects = 0u64;

        // Count objects in object store data
        if let Some(ref object_data) = self.object_store {
            if let Ok(object_snapshot) = bcs::from_bytes::<ObjectStoreSnapshot>(object_data) {
                total_objects += object_snapshot.objects.len() as u64;
            }
        }

        // Count transactions (also considered objects in a broader sense)
        if let Some(ref tx_data) = self.transaction_store {
            if let Ok(tx_snapshot) = bcs::from_bytes::<TransactionStoreSnapshot>(tx_data) {
                total_objects += tx_snapshot.transactions.len() as u64;
            }
        }

        // Count checkpoints
        if let Some(ref checkpoint_data) = self.checkpoint_store {
            if let Ok(checkpoint_snapshot) = bcs::from_bytes::<CheckpointStoreSnapshot>(checkpoint_data) {
                total_objects += checkpoint_snapshot.checkpoints.len() as u64;
            }
        }

        total_objects
    }
    
    /// Convert to serialized bytes
    pub fn into_bytes(self) -> Result<Vec<u8>, SnapshotError> {
        bcs::to_bytes(&self).map_err(|e| SnapshotError::DataAccess {
            operation: "serialize_collected_data".to_string(),
            details: format!("Failed to serialize collected state data: {}", e),
        })
    }
    
    /// Compute Blake3 checksum of all data
    pub fn compute_checksum(&self) -> Result<String, SnapshotError> {
        use blake3::Hasher;
        
        let mut hasher = Hasher::new();
        
        // Hash all components
        if let Some(ref data) = self.authority_state {
            hasher.update(data);
        }
        if let Some(ref data) = self.epoch_store {
            hasher.update(data);
        }
        if let Some(ref data) = self.checkpoint_store {
            hasher.update(data);
        }
        if let Some(ref data) = self.object_store {
            hasher.update(data);
        }
        if let Some(ref data) = self.transaction_store {
            hasher.update(data);
        }
        if let Some(ref data) = self.index_store {
            hasher.update(data);
        }
        if let Some(ref data) = self.consensus_state {
            hasher.update(data);
        }
        
        let hash = hasher.finalize();
        Ok(format!("{}", hash.to_hex()))
    }
}
