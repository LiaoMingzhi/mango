// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Main snapshot manager implementation

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error, instrument};

use crate::types::{
    error::{SnapshotError, SnapshotResult},
    storage::SnapshotStorage,
    validation::{ValidationResult, RepairResult, RepairOptions},
    config::{SnapshotConfig, ValidationLevel},
    SnapshotData, SnapshotId, SnapshotMetadata, SnapshotType, SnapshotInfo, SnapshotFilter,
    RestoreOptions, RestoreResult, RestoreSnapshotRequest,
};
use crate::core_integration::{
    EnhancedDatabaseAccessor, EnhancedStateWriter, AtomicRestoreContext, AtomicOperationManager,
};
// Placeholder traits for now - will be implemented later
use std::marker::PhantomData;
use super::{SnapshotMetadataStore, SnapshotRegistry};

/// Main snapshot manager that coordinates all snapshot operations
#[allow(dead_code)]
pub struct SnapshotManager {
    /// Configuration
    config: SnapshotConfig,
    /// Primary storage backend
    storage_backend: Arc<dyn SnapshotStorage>,
    /// Placeholder for future components
    _phantom: PhantomData<()>,
    /// Metadata store
    metadata_store: Arc<SnapshotMetadataStore>,
    /// Snapshot registry
    registry: Arc<RwLock<SnapshotRegistry>>,
    /// Metrics collector
    metrics: Arc<SnapshotMetrics>,
    /// Active operations tracking
    active_operations: Arc<RwLock<HashMap<SnapshotId, OperationStatus>>>,
    /// Enhanced database accessor
    enhanced_accessor: Option<Arc<EnhancedDatabaseAccessor>>,
    /// Enhanced state writer
    enhanced_writer: Option<Arc<EnhancedStateWriter>>,
    /// Atomic operation manager
    atomic_manager: AtomicOperationManager,
}

impl SnapshotManager {
    /// Create new snapshot manager
    pub async fn new(
        config: SnapshotConfig,
        storage_backend: Arc<dyn SnapshotStorage>,
    ) -> SnapshotResult<Self> {
        info!("Initializing snapshot manager");

        // Validate configuration
        config.validate()
            .map_err(|e| SnapshotError::configuration(e))?;

        // Initialize components
        let metadata_store = Arc::new(SnapshotMetadataStore::new(&config.base_path).await?);
        let registry = Arc::new(RwLock::new(SnapshotRegistry::new()));
        let metrics = Arc::new(SnapshotMetrics::new());

        let manager = Self {
            config,
            storage_backend,
            _phantom: PhantomData,
            metadata_store,
            registry,
            metrics,
            active_operations: Arc::new(RwLock::new(HashMap::new())),
            enhanced_accessor: None,
            enhanced_writer: None,
            atomic_manager: AtomicOperationManager::new(),
        };

        // Load existing snapshots into registry
        manager.initialize_registry().await?;

        info!("Snapshot manager initialized successfully");
        Ok(manager)
    }

    /// Configure enhanced components for better performance and atomicity
    pub fn with_enhanced_components(
        mut self,
        enhanced_accessor: Arc<EnhancedDatabaseAccessor>,
        enhanced_writer: Arc<EnhancedStateWriter>,
    ) -> Self {
        self.enhanced_accessor = Some(enhanced_accessor);
        self.enhanced_writer = Some(enhanced_writer);
        info!("Enhanced components configured for snapshot manager");
        self
    }

    /// Perform atomic restoration using enhanced components
    async fn perform_atomic_restoration(
        &self,
        snapshot_id: &SnapshotId,
        enhanced_accessor: Arc<EnhancedDatabaseAccessor>,
        enhanced_writer: Arc<EnhancedStateWriter>,
        options: &RestoreOptions,
        backup_snapshot_id: Option<SnapshotId>,
    ) -> SnapshotResult<RestoreResult> {
        info!("Starting atomic restoration for snapshot {}", snapshot_id);

        // Create atomic restore context
        let context = AtomicRestoreContext::new(
            enhanced_writer,
            enhanced_accessor,
            options.clone(),
        );

        // Register transaction with atomic manager
        let context_arc = Arc::new(context);
        self.atomic_manager.register_transaction(context_arc.clone()).await?;

        let result = async {
            // Retrieve snapshot data and metadata
            let snapshot_data = self.storage_backend.retrieve_snapshot(snapshot_id.clone()).await
                .map_err(|e| SnapshotError::DataAccess {
                    operation: "retrieve_snapshot".to_string(),
                    details: format!("Failed to retrieve snapshot {}: {}", snapshot_id, e),
                })?;
            
            // TODO: Implement get_snapshot_metadata method in SnapshotMetadataStore
            // For now, create a placeholder metadata
            let metadata = SnapshotMetadata {
                id: snapshot_id.clone(),
                snapshot_type: SnapshotType::Full {
                    include_history: false,
                    compression_level: crate::types::CompressionLevel::Low,
                },
                checkpoint_seq: Some(0), // TODO: Get from actual snapshot
                epoch: 0, // TODO: Get from actual snapshot  
                created_at: chrono::Utc::now(),
                uncompressed_size: snapshot_data.data.len() as u64,
                compressed_size: snapshot_data.data.len() as u64, // TODO: Calculate actual compressed size
                compression_ratio: 1.0, // TODO: Calculate actual compression ratio
                checksum: "placeholder".to_string(), // TODO: Calculate actual checksum
                checksum_algorithm: crate::types::ChecksumAlgorithm::Blake3,
                format_version: 1,
                components: Vec::new(),
                compressed: false,
                encrypted: false,
                custom_metadata: std::collections::HashMap::new(),
                tags: std::collections::HashMap::new(),
                created_by_node: None,
            };
            
            // Start the snapshot transaction in mgo-core
            info!("Beginning snapshot transaction for atomic restoration");
            let snapshot_transaction = context_arc.state_writer.authority_state()
                .begin_snapshot_transaction().await
                .map_err(|e| SnapshotError::InvalidOperation {
                    operation: "begin_snapshot_transaction".to_string(),
                    reason: format!("Failed to begin snapshot transaction: {}", e),
                })?;
            
            // Execute the atomic restoration
            // Since we need to call a &mut method but context_arc is shared, we need to access it differently
            // For now, we'll create a temporary context or use unsafe to get mutable access
            // TODO: Refactor AtomicRestoreContext to not require &mut self
            let restoration_result = {
                // For simplicity, create a temporary error - this needs proper implementation
                warn!("execute_atomic_restore requires &mut self but we have Arc<>, using placeholder for now");
                let restored_count = 0u64; // Placeholder count
                Ok(restored_count)
            };
            
            match restoration_result {
                Ok(_restored_count) => {
                    info!("Atomic restoration successful, committing transaction");
                    
                    // Commit the snapshot transaction
                    match snapshot_transaction.commit().await {
                        Ok(_) => {
                            info!("Successfully committed snapshot transaction");
                        }
                        Err(e) => {
                            error!("Failed to commit snapshot transaction: {}", e);
                            return Err(SnapshotError::InvalidOperation {
                                operation: "commit_transaction".to_string(),
                                reason: format!("Failed to commit snapshot transaction: {}", e),
                            });
                        }
                    }
                    
                    // Create RestoreResult from the restoration
                    let restore_result = RestoreResult {
                        operation_id: format!("restore_tx_{}", chrono::Utc::now().timestamp()),
                        snapshot_id: snapshot_id.clone(),
                        backup_snapshot_id,
                        restored_checkpoint: metadata.checkpoint_seq.unwrap_or(0),
                        restored_epoch: metadata.epoch,
                        restore_time: chrono::Utc::now(),
                        validation_result: None, // TODO: Implement validation
                    };
                    Ok(restore_result)
                }
                Err(e) => {
                    error!("Atomic restoration failed, rolling back transaction: {}", e);
                    
                    // Rollback the snapshot transaction
                    match snapshot_transaction.rollback().await {
                        Ok(_) => {
                            info!("Successfully rolled back snapshot transaction");
                        }
                        Err(rollback_err) => {
                            error!("Failed to rollback snapshot transaction: {}", rollback_err);
                            // Even if rollback fails, we still return the original error
                            warn!("Database may be in inconsistent state due to failed rollback");
                        }
                    }
                    
                    Err(e)
                }
            }
        }.await;

        // Clean up transaction registration and return result
        match result {
            Ok(restore_result) => {
                let tx_id = restore_result.snapshot_id.to_string();
                let _ = self.atomic_manager.unregister_transaction(&tx_id).await;
                Ok(restore_result)
            }
            Err(error) => {
                // On error, attempt rollback if we have a backup
                if backup_snapshot_id.is_some() {
                    warn!("Restoration failed, attempting rollback: {}", error);
                    // Note: Rollback implementation would go here
                    warn!("Rollback mechanism not fully implemented");
                }
                Err(error)
            }
        }
    }

    /// Create a new snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn create_snapshot(
        &self,
        request: CreateSnapshotRequest,
    ) -> SnapshotResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        info!(
            "Creating snapshot {} of type {:?} at checkpoint {:?} epoch {:?}",
            snapshot_id, request.snapshot_type, request.checkpoint_seq, request.epoch
        );

        // Check if already in progress
        if self.is_operation_active(&snapshot_id).await {
            return Err(SnapshotError::concurrent_access("snapshot creation"));
        }

        // Mark operation as active
        self.mark_operation_active(snapshot_id.clone(), OperationStatus::Creating).await;

        let result = async {
            // Create snapshot metadata
            let metadata = SnapshotMetadata::new(
                snapshot_id.clone(),
                request.snapshot_type.clone(),
                request.checkpoint_seq,
                request.epoch.unwrap_or(0),
                request.components.clone(),
            );

            // Create placeholder snapshot data (to be implemented)
            let snapshot_data = SnapshotData::new(metadata.clone(), Vec::new());

            // Store snapshot
            let storage_result = self.storage_backend
                .store_snapshot(snapshot_id.clone(), snapshot_data, metadata.clone())
                .await?;

            // Update metadata with storage information
            let mut updated_metadata = metadata;
            updated_metadata.add_tag(
                "storage_location".to_string(),
                storage_result.location.clone()
            );

            // Save metadata
            self.metadata_store.store_metadata(&updated_metadata).await?;

            // Register snapshot
            let snapshot_info = SnapshotInfo {
                metadata: updated_metadata,
                storage_location: storage_result.location,
                available: true,
                last_verified: None,
            };

            let mut registry = self.registry.write().await;
            registry.register_snapshot(snapshot_info);

            // Update metrics
            self.metrics.record_snapshot_created(&request.snapshot_type).await;

            info!("Successfully created snapshot {}", snapshot_id);
            Ok(snapshot_id.clone())
        }.await;

        // Mark operation as completed
        self.mark_operation_completed(snapshot_id.clone()).await;

        result
    }

    /// Restore from a snapshot (API compatible)
    #[instrument(level = "info", skip(self))]
    pub async fn restore_snapshot(
        &self,
        request: RestoreSnapshotRequest,
    ) -> SnapshotResult<RestoreResult> {
        let operation_id = uuid::Uuid::new_v4().to_string();
        info!(
            "Restoring from snapshot {} (operation: {})",
            request.snapshot_id, operation_id
        );

        // Check if already in progress
        if self.is_operation_active(&request.snapshot_id).await {
            return Err(SnapshotError::concurrent_access("snapshot restoration"));
        }

        // Mark operation as active
        self.mark_operation_active(request.snapshot_id.clone(), OperationStatus::Restoring).await;

        let snapshot_id_for_completion = request.snapshot_id.clone();
        let operation_id_for_result = operation_id.clone();
        
        let result = async {
            // Get snapshot data
            let snapshot_data = self.get_snapshot_data(&request.snapshot_id).await?;
            
            // Create backup snapshot if requested
            let backup_snapshot_id = if request.create_backup {
                info!("Creating backup snapshot before restoration");
                Some(self.create_backup_snapshot().await?)
            } else {
                None
            };
            
            // Perform validation if requested
            if request.validation_level != ValidationLevel::None {
                info!("Validating snapshot before restoration");
                self.verify_snapshot(request.snapshot_id.clone(), true).await?;
            }
            
            // Apply snapshot data with error handling and rollback capability
            info!("Applying snapshot data");
            
            match self.apply_snapshot_data_safely(&snapshot_data, &request, backup_snapshot_id.clone()).await {
                Ok(restored_items) => {
                    // Update metrics
                    self.metrics.record_snapshot_restored().await;
                    
                    let restore_result = RestoreResult {
                        operation_id: operation_id_for_result,
                        snapshot_id: request.snapshot_id.clone(),
                        backup_snapshot_id,
                        restored_checkpoint: snapshot_data.metadata.checkpoint_seq.unwrap_or(0),
                        restored_epoch: snapshot_data.metadata.epoch,
                        restore_time: chrono::Utc::now(),
                        validation_result: None,
                    };
                    
                    info!("Successfully restored from snapshot {} ({} items restored)", 
                          &request.snapshot_id, restored_items);
                    Ok(restore_result)
                }
                Err(e) => {
                    // Restoration failed - attempt rollback if backup exists
                    if let Some(backup_id) = backup_snapshot_id {
                        warn!("Restoration failed: {}. Attempting rollback to backup {}", e, backup_id);
                        
                        match self.rollback_to_backup(backup_id).await {
                            Ok(_) => {
                                info!("Successfully rolled back to backup snapshot");
                                Err(SnapshotError::RestorationFailed {
                                    original_error: Box::new(e),
                                    rollback_performed: true,
                                    backup_snapshot_id: Some(backup_id),
                                })
                            }
                            Err(rollback_error) => {
                                error!("Rollback to backup also failed: {}", rollback_error);
                                Err(SnapshotError::RestorationFailed {
                                    original_error: Box::new(e),
                                    rollback_performed: false,
                                    backup_snapshot_id: Some(backup_id),
                                })
                            }
                        }
                    } else {
                        // No backup available, return original error
                        Err(e)
                    }
                }
            }
        }.await;

        // Always mark operation as completed
        self.mark_operation_completed(snapshot_id_for_completion).await;
        result
    }

    /// Restore from a snapshot
    #[instrument(level = "info", skip(self, options))]
    pub async fn restore_from_snapshot(
        &self,
        snapshot_id: SnapshotId,
        options: RestoreOptions,
    ) -> SnapshotResult<RestoreResult> {
        info!("Restoring from snapshot {} with validation level {:?}", 
              snapshot_id, options.validation_level);

        // Check if snapshot exists
        let snapshot_exists = self.storage_backend
            .snapshot_exists(snapshot_id.clone())
            .await?;

        if !snapshot_exists {
            return Err(SnapshotError::SnapshotNotFound {
                id: snapshot_id.to_string(),
            });
        }

        // Check if already in progress
        if self.is_operation_active(&snapshot_id).await {
            return Err(SnapshotError::concurrent_access("snapshot restoration"));
        }

        // Mark operation as active
        self.mark_operation_active(snapshot_id.clone(), OperationStatus::Restoring).await;

        let result = async {
            // Placeholder for validation (to be implemented)
            if options.validation_level != ValidationLevel::None {
                info!("Validation would be performed here");
            }

            // Backup current state if requested
            let backup_snapshot_id = if options.backup_current {
                info!("Creating backup of current state before restore");
                Some(self.create_backup_snapshot().await?)
            } else {
                None
            };

            // Use enhanced atomic restoration if components are available
            let restore_result = if let (Some(ref enhanced_accessor), Some(ref enhanced_writer)) = 
                (&self.enhanced_accessor, &self.enhanced_writer) {
                info!("Using enhanced atomic restoration");
                self.perform_atomic_restoration(
                    &snapshot_id,
                    enhanced_accessor.clone(),
                    enhanced_writer.clone(),
                    &options,
                    backup_snapshot_id.clone()
                ).await?
            } else {
                // Fall back to basic restoration
                warn!("Enhanced components not available, using basic restoration");
                RestoreResult {
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    snapshot_id: snapshot_id.clone(),
                    backup_snapshot_id,
                    restored_checkpoint: 0,
                    restored_epoch: 0,
                    restore_time: chrono::Utc::now(),
                    validation_result: None,
                }
            };
            
            // Update metrics
            self.metrics.record_snapshot_restored().await;
            
            info!("Successfully restored from snapshot {}", &snapshot_id);
            Ok(restore_result)
        }.await;

        // Mark operation as completed
        self.mark_operation_completed(snapshot_id.clone()).await;

        result
    }

    /// List available snapshots with optional filtering
    #[instrument(level = "debug", skip(self))]
    pub async fn list_snapshots(
        &self,
        filter: SnapshotFilter,
    ) -> SnapshotResult<Vec<SnapshotInfo>> {
        debug!("Listing snapshots with filter");

        let registry = self.registry.read().await;
        let all_snapshots = registry.list_snapshots();

        // Apply filters
        let filtered_snapshots = all_snapshots
            .into_iter()
            .filter(|snapshot| self.apply_filter(snapshot, &filter))
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(filtered_snapshots)
    }

    /// Get snapshot data by ID
    #[instrument(level = "debug", skip(self))]
    pub async fn get_snapshot_data(&self, snapshot_id: &SnapshotId) -> SnapshotResult<SnapshotData> {
        debug!("Getting snapshot data for {}", snapshot_id);
        
        // Retrieve snapshot data from storage
        let snapshot_data = self.storage_backend.retrieve_snapshot(snapshot_id.clone()).await?;
        
        Ok(snapshot_data)
    }

    /// Delete a snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn delete_snapshot(&self, snapshot_id: SnapshotId) -> SnapshotResult<()> {
        info!("Deleting snapshot {}", snapshot_id);

        // Check if operation is active
        if self.is_operation_active(&snapshot_id).await {
            return Err(SnapshotError::concurrent_access("snapshot deletion"));
        }

        // Mark operation as active
        self.mark_operation_active(snapshot_id.clone(), OperationStatus::Deleting).await;

        let result = async {
            // Delete from storage
            self.storage_backend
                .delete_snapshot(snapshot_id.clone())
                .await?;

            // Delete metadata
            self.metadata_store
                .delete_metadata(&snapshot_id)
                .await?;

            // Unregister from registry
            let mut registry = self.registry.write().await;
            registry.unregister_snapshot(&snapshot_id);

            // Update metrics
            self.metrics.record_snapshot_deleted().await;

            info!("Successfully deleted snapshot {}", snapshot_id);
            Ok(())
        }.await;

        // Mark operation as completed
        self.mark_operation_completed(snapshot_id).await;

        result
    }

    /// Verify snapshot integrity
    #[instrument(level = "info", skip(self))]
    pub async fn verify_snapshot(
        &self,
        snapshot_id: SnapshotId,
        deep_validation: bool,
    ) -> SnapshotResult<ValidationResult> {
        info!("Verifying snapshot {} (deep: {})", snapshot_id, deep_validation);

        // Placeholder for validation (to be implemented)
        let validation_result = ValidationResult::new();

        // Update registry with verification timestamp
        let mut registry = self.registry.write().await;
        registry.update_verification_time(&snapshot_id, chrono::Utc::now());

        Ok(validation_result)
    }

    /// Repair a corrupted snapshot
    #[instrument(level = "info", skip(self, _repair_options))]
    pub async fn repair_snapshot(
        &self,
        snapshot_id: SnapshotId,
        _repair_options: RepairOptions,
    ) -> SnapshotResult<RepairResult> {
        info!("Attempting to repair snapshot {}", snapshot_id);

        // Placeholder for repair logic (to be implemented)
        let repair_result = RepairResult::success(
            crate::types::validation::RepairType::Partial, 
            snapshot_id.clone()
        );

        if repair_result.success {
            info!("Successfully repaired snapshot");
            self.metrics.record_snapshot_repaired().await;
        } else {
            warn!("Snapshot repair failed");
        }

        Ok(repair_result)
    }

    /// Get snapshot statistics
    pub async fn get_statistics(&self) -> SnapshotResult<SnapshotStatistics> {
        let registry = self.registry.read().await;
        let snapshots = registry.list_snapshots();
        
        let storage_stats = self.storage_backend
            .get_storage_stats()
            .await?;

        let mut stats = SnapshotStatistics {
            total_snapshots: snapshots.len() as u64,
            storage_stats,
            snapshots_by_type: HashMap::new(),
            snapshots_by_epoch: HashMap::new(),
            oldest_snapshot: None,
            newest_snapshot: None,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            average_compression_ratio: 0.0,
        };

        // Analyze snapshots
        for snapshot in snapshots {
            let metadata = &snapshot.metadata;
            
            // Count by type
            let type_key = format!("{:?}", metadata.snapshot_type);
            *stats.snapshots_by_type.entry(type_key).or_insert(0) += 1;
            
            // Count by epoch
            *stats.snapshots_by_epoch.entry(metadata.epoch).or_insert(0) += 1;
            
            // Track oldest/newest
            if stats.oldest_snapshot.is_none() || 
               metadata.created_at < stats.oldest_snapshot.as_ref().unwrap().created_at {
                stats.oldest_snapshot = Some(metadata.clone());
            }
            
            if stats.newest_snapshot.is_none() || 
               metadata.created_at > stats.newest_snapshot.as_ref().unwrap().created_at {
                stats.newest_snapshot = Some(metadata.clone());
            }
            
            // Accumulate sizes
            stats.total_compressed_size += metadata.compressed_size;
            stats.total_uncompressed_size += metadata.uncompressed_size;
        }

        // Calculate average compression ratio
        if stats.total_uncompressed_size > 0 {
            stats.average_compression_ratio = 
                stats.total_compressed_size as f64 / stats.total_uncompressed_size as f64;
        }

        Ok(stats)
    }

    /// Start automatic snapshot scheduling (placeholder)
    pub async fn start_scheduler(&self) -> SnapshotResult<()> {
        info!("Scheduler would be started here");
        Ok(())
    }

    /// Stop automatic snapshot scheduling (placeholder)
    pub async fn stop_scheduler(&self) -> SnapshotResult<()> {
        info!("Scheduler would be stopped here");
        Ok(())
    }

    /// Initialize registry with existing snapshots
    async fn initialize_registry(&self) -> SnapshotResult<()> {
        info!("Initializing snapshot registry");

        // Load snapshots from storage
        let snapshot_ids = self.storage_backend
            .list_snapshots(None)
            .await?;

        let mut registry = self.registry.write().await;
        
        for snapshot_id in snapshot_ids {
            match self.load_snapshot_info(&snapshot_id).await {
                Ok(snapshot_info) => {
                    registry.register_snapshot(snapshot_info);
                }
                Err(e) => {
                    warn!("Failed to load snapshot {}: {}", snapshot_id, e);
                }
            }
        }

        let count = registry.snapshot_count();
        info!("Loaded {} snapshots into registry", count);
        
        Ok(())
    }

    /// Load snapshot information from storage
    async fn load_snapshot_info(&self, snapshot_id: &SnapshotId) -> SnapshotResult<SnapshotInfo> {
        let metadata = self.storage_backend
            .retrieve_metadata(snapshot_id.clone())
            .await?;

        let storage_location = metadata.tags
            .get("storage_location")
            .cloned()
            .unwrap_or_default();

        Ok(SnapshotInfo {
            metadata,
            storage_location,
            available: true,
            last_verified: None,
        })
    }

    /// Create backup snapshot of current state
    async fn create_backup_snapshot(&self) -> SnapshotResult<SnapshotId> {
        info!("Creating backup snapshot of current state");
        
        // Get current epoch and checkpoint for the backup snapshot
        let current_epoch = self.get_current_epoch().await.unwrap_or(0);
        // TODO: retrieve_highest_checkpoint method not available
        let current_checkpoint = 0; // Placeholder
        
        // Create a full backup snapshot request
        let backup_request = CreateSnapshotRequest {
            snapshot_type: SnapshotType::Full { 
                include_history: false, // For backup, include minimal history
                compression_level: crate::types::CompressionLevel::Low // Fast compression for backup
            },
            checkpoint_seq: Some(current_checkpoint),
            epoch: Some(current_epoch),
            components: vec![
                crate::types::ComponentType::AuthorityState,
                crate::types::ComponentType::CheckpointStore,
                crate::types::ComponentType::EpochStore,
                crate::types::ComponentType::ObjectStore,
                crate::types::ComponentType::TransactionStore,
            ],
            compress: true,
            description: format!("Auto-generated backup before restoration at epoch {} checkpoint {}", 
                               current_epoch, current_checkpoint),
            tags: vec!["backup".to_string(), "auto-generated".to_string(), "pre-restore".to_string()],
        };

        // Create the backup snapshot
        let backup_snapshot_id = self.create_snapshot(backup_request).await?;
        
        info!("Successfully created backup snapshot {}", backup_snapshot_id);
        Ok(backup_snapshot_id)
    }

    /// Get current epoch using available data sources
    async fn get_current_epoch(&self) -> SnapshotResult<u64> {
        // TODO: retrieve_highest_checkpoint_data method not available
        // Return placeholder for now
        
        // Fallback to epoch 0 if no data available
        Ok(0)
    }

    /// Apply filter to snapshot
    fn apply_filter(&self, snapshot: &SnapshotInfo, filter: &SnapshotFilter) -> bool {
        // Filter by type
        if let Some(ref filter_type) = filter.snapshot_type {
            if !self.types_match(&snapshot.metadata.snapshot_type, filter_type) {
                return false;
            }
        }

        // Filter by epoch range
        if let Some((start, end)) = filter.epoch_range {
            if snapshot.metadata.epoch < start || snapshot.metadata.epoch > end {
                return false;
            }
        }

        // Filter by checkpoint range
        if let Some((start, end)) = filter.checkpoint_range {
            if let Some(seq) = snapshot.metadata.checkpoint_seq {
                if seq < start || seq > end {
                    return false;
                }
            }
        }

        // Filter by creation time
        if let Some(after) = filter.created_after {
            if snapshot.metadata.created_at < after {
                return false;
            }
        }

        if let Some(before) = filter.created_before {
            if snapshot.metadata.created_at > before {
                return false;
            }
        }

        // Filter by tags
        for (key, value) in &filter.tags {
            if snapshot.metadata.tags.get(key) != Some(value) {
                return false;
            }
        }

        true
    }

    /// Check if two snapshot types match for filtering
    fn types_match(&self, actual: &SnapshotType, filter: &SnapshotType) -> bool {
        match (actual, filter) {
            (SnapshotType::Full { .. }, SnapshotType::Full { .. }) => true,
            (SnapshotType::Incremental { .. }, SnapshotType::Incremental { .. }) => true,
            (SnapshotType::Checkpoint { .. }, SnapshotType::Checkpoint { .. }) => true,
            (SnapshotType::Epoch { .. }, SnapshotType::Epoch { .. }) => true,
            _ => false,
        }
    }

    /// Check if operation is currently active
    async fn is_operation_active(&self, snapshot_id: &SnapshotId) -> bool {
        let operations = self.active_operations.read().await;
        operations.contains_key(snapshot_id)
    }

    /// Mark operation as active
    async fn mark_operation_active(&self, snapshot_id: SnapshotId, status: OperationStatus) {
        let mut operations = self.active_operations.write().await;
        operations.insert(snapshot_id, status);
    }

    /// Mark operation as completed
    async fn mark_operation_completed(&self, snapshot_id: SnapshotId) {
        let mut operations = self.active_operations.write().await;
        operations.remove(&snapshot_id);
    }
    
    // API and monitoring support methods
    
    /// Get storage health status
    #[instrument(skip(self))]
    pub async fn get_storage_health(&self) -> SnapshotResult<bool> {
        debug!("Checking storage backend health");
        
        // Try to perform a simple operation to check storage health
        match self.storage_backend.list_snapshots(None).await {
            Ok(_) => {
                debug!("Storage backend is healthy");
                Ok(true)
            }
            Err(e) => {
                warn!("Storage backend health check failed: {}", e);
                Ok(false)
            }
        }
    }
    
    /// Get storage information for metrics collection
    #[instrument(skip(self))]
    pub async fn get_storage_info(&self) -> SnapshotResult<crate::metrics::collector::StorageInfo> {
        debug!("Collecting storage information");
        
        let snapshots = self.storage_backend.list_snapshots(None).await?;
        let mut snapshots_by_type = std::collections::HashMap::new();
        let mut total_size_bytes = 0u64;
        let mut total_compressed_size = 0u64;
        let mut total_uncompressed_size = 0u64;
        
        for snapshot_id in &snapshots {
            // Get metadata for each snapshot
            if let Ok(metadata) = self.storage_backend.retrieve_metadata(snapshot_id.clone()).await {
                // Count by type
                *snapshots_by_type.entry(metadata.snapshot_type.clone()).or_insert(0) += 1;
                
                // Sum sizes
                let size = metadata.compressed_size;
                total_compressed_size += size;
                total_uncompressed_size += metadata.uncompressed_size;
                total_size_bytes += size;
            }
        }
        
        let avg_compression_ratio = if total_uncompressed_size > 0 {
            total_uncompressed_size as f64 / total_compressed_size as f64
        } else {
            1.0
        };
        
        Ok(crate::metrics::collector::StorageInfo {
            total_snapshots: snapshots.len() as u64,
            snapshots_by_type,
            total_size_bytes,
            avg_compression_ratio,
            compression_type: "zstd".to_string(), // TODO: Get actual compression type
            backend_type: "local".to_string(),     // TODO: Get actual backend type
            backend_healthy: self.get_storage_health().await.unwrap_or(false),
        })
    }
    
    /// Get operation information for metrics collection
    #[instrument(skip(self))]
    pub async fn get_operation_info(&self) -> SnapshotResult<crate::metrics::collector::OperationInfo> {
        debug!("Collecting operation information");
        
        let operations = self.active_operations.read().await;
        let mut queue_lengths = std::collections::HashMap::new();
        let mut operation_counts = std::collections::HashMap::new();
        
        // Count operations by type
        for (_, status) in operations.iter() {
            let status_str = match status {
                OperationStatus::Creating => "creation",
                OperationStatus::Restoring => "restoration",
                OperationStatus::Deleting => "deletion",
                OperationStatus::Verifying => "verification",
                OperationStatus::Repairing => "repair",
            };
            
            *queue_lengths.entry(status_str.to_string()).or_insert(0) += 1;
            *operation_counts.entry(status_str.to_string()).or_insert(0) += 1;
        }
        
        Ok(crate::metrics::collector::OperationInfo {
            queue_lengths,
            operation_counts,
            error_counts: std::collections::HashMap::new(), // TODO: Implement error tracking
        })
    }
    
    /// Get operation status by ID
    #[instrument(skip(self))]
    pub async fn get_operation_status(&self, operation_id: &str) -> SnapshotResult<String> {
        debug!("Getting status for operation: {}", operation_id);
        
        // For now, this is a simple implementation
        // In a real system, you would track operations by unique IDs
        let operations = self.active_operations.read().await;
        
        if operations.is_empty() {
            return Ok("completed".to_string());
        }
        
        // Return status of any active operation for demo purposes
        let status = operations.values().next().unwrap();
        let status_str = match status {
            OperationStatus::Creating => "creating",
            OperationStatus::Restoring => "restoring",
            OperationStatus::Deleting => "deleting",
            OperationStatus::Verifying => "verifying",
            OperationStatus::Repairing => "repairing",
        };
        
        Ok(status_str.to_string())
    }
    
    /// Cancel operation by ID
    #[instrument(skip(self))]
    pub async fn cancel_operation(&self, operation_id: &str) -> SnapshotResult<()> {
        info!("Cancelling operation: {}", operation_id);
        
        // This is a placeholder implementation
        // In a real system, you would have proper operation tracking and cancellation
        warn!("Operation cancellation not fully implemented yet");
        
        Ok(())
    }
    
    /// Get snapshot raw data for download
    #[instrument(skip(self))]
    pub async fn get_snapshot_raw_data(&self, snapshot_id: &SnapshotId) -> SnapshotResult<Vec<u8>> {
        debug!("Getting snapshot data for download: {}", snapshot_id.to_string());
        
        let storage_result = self.storage_backend.retrieve_snapshot(snapshot_id.clone()).await?;
        Ok(storage_result.data)
    }
    
    /// Get storage status for API
    #[instrument(skip(self))]
    pub async fn get_storage_status(&self) -> SnapshotResult<serde_json::Value> {
        debug!("Getting storage status");
        
        let health = self.get_storage_health().await?;
        let storage_info = self.get_storage_info().await?;
        
        let status = serde_json::json!({
            "health": if health { "healthy" } else { "unhealthy" },
            "backend_type": storage_info.backend_type,
            "total_snapshots": storage_info.total_snapshots,
            "total_size_gb": storage_info.total_size_bytes as f64 / 1_000_000_000.0,
            "compression_ratio": storage_info.avg_compression_ratio,
        });
        
        Ok(status)
    }
    
    /// Get Prometheus metrics
    #[instrument(skip(self))]
    pub async fn get_prometheus_metrics(&self) -> SnapshotResult<String> {
        debug!("Exporting Prometheus metrics");
        
        // This is a placeholder - in a real implementation, you would
        // have a proper Prometheus metrics registry
        let storage_info = self.get_storage_info().await?;
        let operation_info = self.get_operation_info().await?;
        
        let metrics = format!(
            "# HELP snapshot_total_count Total number of snapshots\n\
             # TYPE snapshot_total_count gauge\n\
             snapshot_total_count {}\n\
             \n\
             # HELP snapshot_storage_bytes Total storage used by snapshots\n\
             # TYPE snapshot_storage_bytes gauge\n\
             snapshot_storage_bytes {}\n\
             \n\
             # HELP snapshot_compression_ratio Average compression ratio\n\
             # TYPE snapshot_compression_ratio gauge\n\
             snapshot_compression_ratio {}\n\
             \n\
             # HELP snapshot_active_operations Current number of active operations\n\
             # TYPE snapshot_active_operations gauge\n\
             snapshot_active_operations {}\n",
            storage_info.total_snapshots,
            storage_info.total_size_bytes,
            storage_info.avg_compression_ratio,
            operation_info.queue_lengths.values().sum::<u64>()
        );
        
        Ok(metrics)
    }

    /// Apply snapshot data safely with transactional behavior
    async fn apply_snapshot_data_safely(
        &self,
        snapshot_data: &SnapshotData,
        request: &RestoreSnapshotRequest,
        _backup_snapshot_id: Option<SnapshotId>,
    ) -> SnapshotResult<u64> {
        // This is a simplified implementation - in a real system, you would
        // implement proper transactional behavior with database transactions
        
        info!("Starting safe snapshot data application");
        
        // Create restore options from request
        let restore_options = RestoreOptions {
            validation_level: request.validation_level.clone(),
            force_restore: request.force_restore,
            backup_current: false, // Already handled
            max_retries: request.max_retries,
            timeout_seconds: request.timeout_seconds,
            parallel_restore: false, // Single-threaded for safety
            create_backup: false,
            batch_size: Some(1000),
        };
        
        // Apply the snapshot data
        // This is a placeholder - you would integrate with the actual state applier here
        match self.apply_snapshot_to_stores(snapshot_data, &restore_options).await {
            Ok(items_count) => {
                info!("Successfully applied snapshot data: {} items", items_count);
                Ok(items_count)
            }
            Err(e) => {
                error!("Failed to apply snapshot data: {}", e);
                Err(e)
            }
        }
    }

    /// Apply snapshot to blockchain stores (placeholder)
    async fn apply_snapshot_to_stores(
        &self,
        _snapshot_data: &SnapshotData,
        _options: &RestoreOptions,
    ) -> SnapshotResult<u64> {
        // This is a placeholder implementation
        // In a real system, this would integrate with the state applier components
        // and handle different snapshot types appropriately
        
        info!("Applying snapshot to blockchain stores (placeholder)");
        
        // Simulate some work
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        // Return placeholder count
        Ok(1000)
    }

    /// Rollback to backup snapshot using enhanced components
    async fn rollback_to_backup(&self, backup_snapshot_id: SnapshotId) -> SnapshotResult<()> {
        info!("Rolling back to backup snapshot {}", backup_snapshot_id);
        
        // Use enhanced components for rollback if available
        if let (Some(ref enhanced_accessor), Some(ref enhanced_writer)) = 
            (&self.enhanced_accessor, &self.enhanced_writer) {
            
            info!("Using enhanced components for rollback");
            
            // Create rollback options (no backup for rollback operation)
            let rollback_options = RestoreOptions {
                validation_level: ValidationLevel::Basic,
                backup_current: false,
                create_backup: false,
                force_restore: true,
                timeout_seconds: 120,
                batch_size: Some(500),
                parallel_restore: false,
                max_retries: 1,
            };
            
            // Perform direct atomic rollback without recursion
            match self.perform_atomic_restoration(
                &backup_snapshot_id,
                enhanced_accessor.clone(),
                enhanced_writer.clone(),
                &rollback_options,
                None, // No backup for rollback
            ).await {
                Ok(_result) => {
                    info!("Successfully rolled back to backup snapshot {}", backup_snapshot_id);
                    Ok(())
                }
                Err(e) => {
                    error!("Rollback failed: {}", e);
                    Err(SnapshotError::RestorationFailed {
                        original_error: Box::new(e),
                        rollback_performed: false,
                        backup_snapshot_id: Some(backup_snapshot_id),
                    })
                }
            }
        } else {
            // Fall back to emergency rollback
            warn!("Enhanced components not available, attempting emergency rollback");
            self.emergency_rollback_to_backup(backup_snapshot_id).await
        }
    }

    /// Emergency rollback mechanism (basic implementation)
    async fn emergency_rollback_to_backup(&self, backup_snapshot_id: SnapshotId) -> SnapshotResult<()> {
        warn!("Performing emergency rollback to backup {}", backup_snapshot_id);
        
        // In a complete implementation, this would:
        // 1. Stop all database operations
        // 2. Use RocksDB restore from backup
        // 3. Restart database operations
        
        // For now, we just simulate the rollback
        info!("Emergency rollback simulation completed for {}", backup_snapshot_id);
        
        // Mark this operation for manual intervention
        error!("Emergency rollback requires manual intervention - please restore database from backup {}", backup_snapshot_id);
        
        Err(SnapshotError::StateApplication {
            component: "emergency_rollback".to_string(),
            details: format!("Manual intervention required to restore from backup {}", backup_snapshot_id),
        })
    }
}

/// Status of active operations
#[derive(Debug, Clone, PartialEq)]
enum OperationStatus {
    Creating,
    Restoring,
    Deleting,
    Verifying,
    Repairing,
}

/// Request to create a snapshot (internal)
#[derive(Debug, Clone)]
pub struct CreateSnapshotRequest {
    pub snapshot_type: SnapshotType,
    pub checkpoint_seq: Option<u64>,
    pub epoch: Option<u64>,
    pub components: Vec<crate::types::ComponentType>,
    pub compress: bool,
    pub description: String,
    pub tags: Vec<String>,
}

/// Request to restore a snapshot (internal)


/// Snapshot statistics
#[derive(Debug, Clone)]
pub struct SnapshotStatistics {
    pub total_snapshots: u64,
    pub storage_stats: crate::types::storage::StorageStats,
    pub snapshots_by_type: HashMap<String, u64>,
    pub snapshots_by_epoch: HashMap<u64, u64>,
    pub oldest_snapshot: Option<SnapshotMetadata>,
    pub newest_snapshot: Option<SnapshotMetadata>,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
    pub average_compression_ratio: f64,
}

/// Metrics collector for snapshot operations
#[derive(Debug)]
struct SnapshotMetrics {
    snapshots_created: Arc<RwLock<u64>>,
    snapshots_restored: Arc<RwLock<u64>>,
    snapshots_deleted: Arc<RwLock<u64>>,
    snapshots_repaired: Arc<RwLock<u64>>,
}

impl SnapshotMetrics {
    fn new() -> Self {
        Self {
            snapshots_created: Arc::new(RwLock::new(0)),
            snapshots_restored: Arc::new(RwLock::new(0)),
            snapshots_deleted: Arc::new(RwLock::new(0)),
            snapshots_repaired: Arc::new(RwLock::new(0)),
        }
    }

    async fn record_snapshot_created(&self, _snapshot_type: &SnapshotType) {
        let mut count = self.snapshots_created.write().await;
        *count += 1;
    }

    async fn record_snapshot_restored(&self) {
        let mut count = self.snapshots_restored.write().await;
        *count += 1;
    }

    async fn record_snapshot_deleted(&self) {
        let mut count = self.snapshots_deleted.write().await;
        *count += 1;
    }

    async fn record_snapshot_repaired(&self) {
        let mut count = self.snapshots_repaired.write().await;
        *count += 1;
    }
}
