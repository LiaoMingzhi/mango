// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Incremental snapshot manager

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tracing::{debug, info, warn, instrument};


use mgo_types::base_types::EpochId;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

use crate::incremental::{DeltaComputer, DeltaApplier, DeltaData};
use crate::core_integration::{DatabaseAccessor, EnhancedStateApplier};
use crate::types::{SnapshotId, SnapshotData, SnapshotMetadata, SnapshotType, ComponentType};
use crate::types::error::SnapshotError;
use crate::types::restore::RestoreOptions;
use crate::types::config::SnapshotConfig;
use crate::storage::SnapshotStorage;

/// Manages incremental snapshots with delta computation and application
pub struct IncrementalSnapshotManager {
    delta_computer: Arc<DeltaComputer>,
    delta_applier: Arc<DeltaApplier>,
    storage_backend: Arc<dyn SnapshotStorage>,
    config: SnapshotConfig,
    // Cache for base snapshots to avoid repeated loading
    base_snapshot_cache: tokio::sync::Mutex<HashMap<SnapshotId, (SnapshotData, SnapshotMetadata)>>,
}

impl IncrementalSnapshotManager {
    /// Create a new incremental snapshot manager
    pub fn new(
        db_accessor: Arc<DatabaseAccessor>,
        state_applier: Arc<EnhancedStateApplier>,
        storage_backend: Arc<dyn SnapshotStorage>,
        config: SnapshotConfig,
    ) -> Self {
        let delta_computer = Arc::new(DeltaComputer::new(db_accessor.clone()));
        let delta_applier = Arc::new(DeltaApplier::new(db_accessor, state_applier));

        Self {
            delta_computer,
            delta_applier,
            storage_backend,
            config,
            base_snapshot_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Create an incremental snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn create_incremental_snapshot(
        &self,
        base_snapshot_id: SnapshotId,
        target_checkpoint: Option<CheckpointSequenceNumber>,
        target_epoch: Option<EpochId>,
        _description: Option<String>,
    ) -> Result<SnapshotId, SnapshotError> {
        info!("Creating incremental snapshot from base {}", base_snapshot_id);

        // Load base snapshot
        let (base_snapshot_data, base_metadata) = self.load_base_snapshot(base_snapshot_id).await?;

        // Compute delta
        let delta_data = self.delta_computer.compute_delta(
            base_snapshot_id,
            &base_snapshot_data,
            &base_metadata,
            target_checkpoint,
            target_epoch,
        ).await?;

        // Check if delta is empty
        if delta_data.is_empty() {
            warn!("Delta is empty, no changes since base snapshot");
            return Err(SnapshotError::StateCollection {
                component: "delta".to_string(),
                details: "No changes detected since base snapshot".to_string(),
            });
        }

        // Create snapshot metadata
        let snapshot_id = SnapshotId::new();
        let delta_bytes = bcs::to_bytes(&delta_data)?;
        
        let metadata = SnapshotMetadata::new(
            snapshot_id,
            SnapshotType::Incremental {
                base_snapshot: base_snapshot_id,
                changed_components: self.get_changed_components(&delta_data),
            },
            Some(delta_data.target_checkpoint),
            delta_data.target_epoch,
            self.get_changed_components(&delta_data),
        );

        // Create snapshot data container
        let snapshot_data = SnapshotData::new(metadata.clone(), delta_bytes);

        // Store the incremental snapshot
        self.storage_backend.store_snapshot(snapshot_id, snapshot_data, metadata.clone()).await?;

        info!("Incremental snapshot {} created successfully with {} changes", 
              snapshot_id, delta_data.total_changes());

        Ok(snapshot_id)
    }

    /// Restore from an incremental snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn restore_from_incremental_snapshot(
        &self,
        incremental_snapshot_id: SnapshotId,
        options: RestoreOptions,
    ) -> Result<IncrementalRestoreResult, SnapshotError> {
        info!("Restoring from incremental snapshot {}", incremental_snapshot_id);

        // Load incremental snapshot
        let _incremental_data = self.storage_backend.retrieve_snapshot(incremental_snapshot_id).await?;
        let incremental_metadata = self.storage_backend.retrieve_metadata(incremental_snapshot_id).await
            .map_err(|e| SnapshotError::StorageError { source: Box::new(e) })?;

        // Extract base snapshot ID
        let base_snapshot_id = match &incremental_metadata.snapshot_type {
            SnapshotType::Incremental { base_snapshot, .. } => base_snapshot.clone(),
            _ => return Err(SnapshotError::InvalidFormat {
                reason: "Not an incremental snapshot".to_string(),
            }),
        };

        // Build and apply the delta chain
        let restore_result = self.apply_incremental_chain(
            incremental_snapshot_id,
            base_snapshot_id,
            options,
        ).await?;

        info!("Incremental restoration completed successfully");

        Ok(restore_result)
    }

    /// Apply a chain of incremental snapshots
    async fn apply_incremental_chain(
        &self,
        target_snapshot_id: SnapshotId,
        base_snapshot_id: SnapshotId,
        options: RestoreOptions,
    ) -> Result<IncrementalRestoreResult, SnapshotError> {
        // Build the chain of snapshots from base to target
        let snapshot_chain = self.build_snapshot_chain(target_snapshot_id, base_snapshot_id).await?;

        let mut total_applied = 0u64;
        let mut deltas_applied = 0u32;

        // First, restore the base snapshot if needed
        if let Some((_base_data, base_metadata)) = snapshot_chain.first() {
            // Check if we need to restore base snapshot
            // For now, assume we're starting from the correct base state
            debug!("Starting from base snapshot {}", base_metadata.id);
        }

        // Apply each delta in the chain
        for (delta_data, metadata) in snapshot_chain.iter().skip(1) {
            // Extract delta from snapshot data
            // TODO: Adapt to new SnapshotData structure - try to deserialize as DeltaData
            let delta_bytes = &delta_data.data;

            let delta: DeltaData = bcs::from_bytes(delta_bytes)
                .map_err(|e| SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize delta: {}", e),
                })?;

            // Apply the delta
            let application_result = self.delta_applier.apply_delta(&delta, &options).await?;
            total_applied += application_result.total_applied;
            deltas_applied += 1;

            info!("Applied delta {} with {} changes", metadata.id, application_result.total_applied);
        }

        Ok(IncrementalRestoreResult {
            target_snapshot_id,
            base_snapshot_id,
            deltas_applied,
            total_items_applied: total_applied,
            restoration_time: std::time::SystemTime::now(),
        })
    }

    /// Build the chain of snapshots from base to target
    async fn build_snapshot_chain(
        &self,
        target_snapshot_id: SnapshotId,
        base_snapshot_id: SnapshotId,
    ) -> Result<Vec<(SnapshotData, SnapshotMetadata)>, SnapshotError> {
        let mut chain = Vec::new();
        let mut current_id = target_snapshot_id;

        // Traverse backwards to build the chain
        loop {
            let snapshot_data = self.storage_backend.retrieve_snapshot(current_id).await?;
            let metadata = self.storage_backend.retrieve_metadata(current_id).await
                .map_err(|e| SnapshotError::StorageError { source: Box::new(e) })?;

            chain.push((snapshot_data, metadata.clone()));

            match &metadata.snapshot_type {
                SnapshotType::Incremental { base_snapshot, .. } => {
                    if *base_snapshot == base_snapshot_id {
                        // We've reached the base, add it and stop
                        let base_data = self.storage_backend.retrieve_snapshot(base_snapshot_id).await?;
                        let base_metadata = self.storage_backend.retrieve_metadata(base_snapshot_id).await
                            .map_err(|e| SnapshotError::StorageError { source: Box::new(e) })?;
                        chain.push((base_data, base_metadata));
                        break;
                    } else {
                        current_id = base_snapshot.clone();
                    }
                }
                _ => {
                    // We've reached a full snapshot, treat it as base
                    break;
                }
            }
        }

        // Reverse the chain so it goes from base to target
        chain.reverse();

        Ok(chain)
    }

    /// Load base snapshot with caching
    async fn load_base_snapshot(
        &self,
        base_snapshot_id: SnapshotId,
    ) -> Result<(SnapshotData, SnapshotMetadata), SnapshotError> {
        // Check cache first
        {
            let cache = self.base_snapshot_cache.lock().await;
            if let Some(cached) = cache.get(&base_snapshot_id) {
                return Ok(cached.clone());
            }
        }

        // Load from storage
        let snapshot_data = self.storage_backend.retrieve_snapshot(base_snapshot_id).await?;
        let metadata = self.storage_backend.retrieve_metadata(base_snapshot_id).await
            .map_err(|e| SnapshotError::StorageError { source: Box::new(e) })?;

        // Cache it
        {
            let mut cache = self.base_snapshot_cache.lock().await;
            cache.insert(base_snapshot_id, (snapshot_data.clone(), metadata.clone()));
            
            // Limit cache size
            if cache.len() > 1000 { // TODO: Add max_cache_size to PerformanceConfig
                // Remove oldest entry (simple eviction)
                if let Some(first_key) = cache.keys().next().copied() {
                    cache.remove(&first_key);
                }
            }
        }

        Ok((snapshot_data, metadata))
    }

    /// Get list of changed components from delta data
    fn get_changed_components(&self, delta_data: &DeltaData) -> Vec<ComponentType> {
        let mut components = Vec::new();

        if delta_data.object_delta.total_changes() > 0 {
            components.push(ComponentType::ObjectStore);
        }
        if delta_data.transaction_delta.total_changes() > 0 {
            components.push(ComponentType::TransactionStore);
        }
        if delta_data.checkpoint_delta.total_changes() > 0 {
            components.push(ComponentType::CheckpointStore);
        }
        if delta_data.committee_delta.total_changes() > 0 {
            components.push(ComponentType::EpochStore);
        }

        components
    }

    /// Get statistics about incremental snapshots
    pub async fn get_incremental_statistics(&self) -> Result<IncrementalStatistics, SnapshotError> {
        // This would require enumerating all snapshots and analyzing the incremental chains
        // For now, return basic statistics
        Ok(IncrementalStatistics {
            total_incremental_snapshots: 0,
            total_full_snapshots: 0,
            average_delta_size: 0,
            average_compression_ratio: 0.0,
            longest_chain_length: 0,
        })
    }

    /// Clean up incremental snapshots that are no longer needed
    pub async fn cleanup_incremental_chains(
        &self,
        _max_chain_length: u32,
        _max_age_days: u32,
    ) -> Result<CleanupResult, SnapshotError> {
        // This would implement logic to:
        // 1. Find incremental chains that are too long
        // 2. Consolidate them into new full snapshots
        // 3. Remove old incremental snapshots that are no longer needed
        
        Ok(CleanupResult {
            snapshots_removed: 0,
            snapshots_consolidated: 0,
            space_freed: 0,
        })
    }
}

/// Result of incremental snapshot restoration
#[derive(Debug, Clone)]
pub struct IncrementalRestoreResult {
    /// ID of the target snapshot that was restored
    pub target_snapshot_id: SnapshotId,
    /// ID of the base snapshot used
    pub base_snapshot_id: SnapshotId,
    /// Number of deltas applied
    pub deltas_applied: u32,
    /// Total number of items applied
    pub total_items_applied: u64,
    /// Time when restoration completed
    pub restoration_time: std::time::SystemTime,
}

/// Statistics about incremental snapshots
#[derive(Debug, Clone)]
pub struct IncrementalStatistics {
    /// Total number of incremental snapshots
    pub total_incremental_snapshots: u64,
    /// Total number of full snapshots used as bases
    pub total_full_snapshots: u64,
    /// Average size of delta files in bytes
    pub average_delta_size: u64,
    /// Average compression ratio achieved
    pub average_compression_ratio: f64,
    /// Length of the longest incremental chain
    pub longest_chain_length: u32,
}

/// Result of cleanup operations
#[derive(Debug, Clone)]
pub struct CleanupResult {
    /// Number of snapshots removed
    pub snapshots_removed: u32,
    /// Number of snapshots consolidated
    pub snapshots_consolidated: u32,
    /// Amount of disk space freed in bytes
    pub space_freed: u64,
}
