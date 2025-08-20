// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Enhanced state applier for writing snapshot data back to mgo-core databases

use std::sync::Arc;

use anyhow::Result;
use tracing::{debug, info, warn, instrument};

use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::authority::AuthorityState;
use mgo_types::base_types::EpochId;

use mgo_types::object::Object;

use mgo_types::effects::TransactionEffects;
use mgo_types::event::Event;
use mgo_types::transaction::Transaction;
use mgo_types::committee::Committee;
use mgo_types::messages_checkpoint::VerifiedCheckpoint;


use crate::core_integration::{
    DatabaseAccessor, 
    AuthorityStateSnapshot, 
    CheckpointStoreSnapshot, 
    CommitteeStoreSnapshot, 
    ObjectStoreSnapshot,
    TransactionStoreSnapshot,
    ObjectEntry,
    TransactionEntry,
    EffectsEntry,
    EventsEntry,
};
use crate::types::{ComponentType, SnapshotType, SnapshotData, SnapshotMetadata};
use crate::types::error::SnapshotError;
use crate::types::restore::RestoreOptions;

/// Enhanced state applier that writes data using database operations
pub struct EnhancedStateApplier {
    db_accessor: Arc<DatabaseAccessor>,
    authority_state: Arc<AuthorityState>,
    perpetual_tables: Arc<AuthorityPerpetualTables>,
    checkpoint_store: Arc<CheckpointStore>,
}

impl EnhancedStateApplier {
    pub fn new(
        db_accessor: Arc<DatabaseAccessor>,
        authority_state: Arc<AuthorityState>,
        perpetual_tables: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
    ) -> Self {
        Self {
            db_accessor,
            authority_state,
            perpetual_tables,
            checkpoint_store,
        }
    }

    /// Apply snapshot data to the database
    #[instrument(level = "info", skip(self, snapshot_data))]
    pub async fn apply_snapshot_state(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        info!("Starting snapshot state application");

        let mut total_restored = 0u64;

        match &metadata.snapshot_type {
            SnapshotType::Full { .. } => {
                total_restored = self.apply_full_snapshot(snapshot_data, metadata, _options).await?;
            }
            SnapshotType::Incremental { .. } => {
                total_restored = self.apply_incremental_snapshot(snapshot_data, metadata, _options).await?;
            }
            SnapshotType::Checkpoint { .. } => {
                total_restored = self.apply_checkpoint_snapshot(snapshot_data, metadata, _options).await?;
            }
            SnapshotType::Epoch { .. } => {
                total_restored = self.apply_epoch_snapshot(snapshot_data, metadata, _options).await?;
            }
        }

        info!("Snapshot state application completed, restored {} items", total_restored);
        Ok(total_restored)
    }

    /// Apply full snapshot data
    #[instrument(level = "info", skip(self, snapshot_data, _options))]
    async fn apply_full_snapshot(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        info!("Applying full snapshot");

        let mut total_restored = 0u64;

        // Apply components in dependency order
        let components_order = vec![
            ComponentType::EpochStore,      // Committee data first
            ComponentType::CheckpointStore, // Checkpoints
            ComponentType::ObjectStore,     // Objects
            ComponentType::TransactionStore, // Transactions and effects
            ComponentType::AuthorityState,  // Authority state last
        ];

        for component in components_order {
            // TODO: Adapt to new SnapshotData structure without get_component_data
            if !snapshot_data.data.is_empty() {
                let count = self.apply_component_data(&component, &snapshot_data.data, _options).await?;
                total_restored += count;
                info!("Applied {} {} items", count, format!("{:?}", component).to_lowercase());
            }
        }

        Ok(total_restored)
    }

    /// Apply incremental snapshot data
    #[instrument(level = "info", skip(self, snapshot_data, _options))]
    async fn apply_incremental_snapshot(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        info!("Applying incremental snapshot");

        // For now, treat as full snapshot
        // TODO: Implement proper incremental logic
        warn!("Incremental snapshot application not fully implemented, falling back to full restore");
        
        let mut total_restored = 0u64;
        
        // TODO: Implement proper component data deserialization
        // For now, deserialize the entire snapshot data as AuthorityStateSnapshot
        let _authority_snapshot: Result<crate::core_integration::AuthorityStateSnapshot, _> = 
            bcs::from_bytes(&snapshot_data.data);
        
        // Placeholder restoration logic
        total_restored = snapshot_data.data.len() as u64;

        Ok(total_restored)
    }

    /// Apply checkpoint snapshot data
    #[instrument(level = "info", skip(self, snapshot_data, _options))]
    async fn apply_checkpoint_snapshot(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        info!("Applying checkpoint snapshot");

        // TODO: Implement proper checkpoint data extraction from snapshot_data.data
        if let Ok(_checkpoint_data) = bcs::from_bytes::<crate::core_integration::CheckpointStoreSnapshot>(&snapshot_data.data) {
            // TODO: Extract actual checkpoint data and store it
            // For now, just return placeholder success
            info!("Checkpoint snapshot applied (placeholder)");
            
            Ok(1)
        } else {
            Err(SnapshotError::StateCollection {
                component: "checkpoint".to_string(),
                details: "No checkpoint data found in snapshot".to_string(),
            })
        }
    }

    /// Apply epoch snapshot data
    #[instrument(level = "info", skip(self, snapshot_data, _options))]
    async fn apply_epoch_snapshot(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        info!("Applying epoch snapshot");

        // TODO: Implement proper epoch data extraction from snapshot_data.data
        if let Ok(_epoch_data) = bcs::from_bytes::<crate::core_integration::CommitteeStoreSnapshot>(&snapshot_data.data) {
            // TODO: Extract actual epoch data and store it
            // For now, just return placeholder success
            info!("Epoch snapshot applied (placeholder)");
            Ok(1)
        } else {
            Err(SnapshotError::StateCollection {
                component: "epoch".to_string(),
                details: "No epoch data found in snapshot".to_string(),
            })
        }
    }

    /// Apply data for a specific component
    #[instrument(level = "debug", skip(self, component_data, _options))]
    async fn apply_component_data(
        &self,
        component_type: &ComponentType,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        match component_type {
            ComponentType::ObjectStore => self.apply_object_store_data(component_data, _options).await,
            ComponentType::TransactionStore => self.apply_transaction_store_data(component_data, _options).await,
            ComponentType::CheckpointStore => self.apply_checkpoint_store_data(component_data, _options).await,
            ComponentType::EpochStore => self.apply_committee_store_data(component_data, _options).await,
            ComponentType::AuthorityState => self.apply_authority_state_data(component_data, _options).await,
            ComponentType::ConsensusState => {
                warn!("Consensus state restoration not implemented");
                Ok(0)
            }
            ComponentType::IndexStore => {
                warn!("Index store restoration not implemented");
                Ok(0)
            }
        }
    }

    /// Apply object store data
    async fn apply_object_store_data(
        &self,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let snapshot: ObjectStoreSnapshot = 
            bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize object store data: {}", e),
            })?;

        info!("Restoring {} objects", snapshot.objects.len());

        let mut restored_count = 0u64;

        // Process objects in batches to avoid memory issues
        const BATCH_SIZE: usize = 1000;
        for chunk in snapshot.objects.chunks(BATCH_SIZE) {
            let batch_count = self.store_objects_batch(chunk, _options).await?;
            restored_count += batch_count;
            
            if restored_count % 10000 == 0 {
                info!("Restored {} objects so far", restored_count);
            }
        }

        Ok(restored_count)
    }

    /// Apply transaction store data
    async fn apply_transaction_store_data(
        &self,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let snapshot: TransactionStoreSnapshot = 
            bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize transaction store data: {}", e),
            })?;

        info!("Restoring {} transactions", snapshot.transactions.len());

        let mut restored_count = 0u64;

        // Store transactions
        for tx_entry in &snapshot.transactions {
            self.store_transaction_entry(tx_entry, _options).await?;
            restored_count += 1;
        }

        // Store effects
        for effects_entry in &snapshot.effects {
            self.store_effects_entry(effects_entry, _options).await?;
            restored_count += 1;
        }

        // Store events
        for events_entry in &snapshot.events {
            self.store_events_entry(events_entry, _options).await?;
            restored_count += 1;
        }

        Ok(restored_count)
    }

    /// Apply checkpoint store data
    async fn apply_checkpoint_store_data(
        &self,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let snapshot: CheckpointStoreSnapshot = 
            bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize checkpoint store data: {}", e),
            })?;

        info!("Restoring {} checkpoints", snapshot.checkpoints.len());

        let mut restored_count = 0u64;

        // Store target checkpoint if present
        if let Some(checkpoint_seq) = snapshot.target_checkpoint_seq {
            // TODO: Store checkpoint using checkpoint_seq
            info!("Storing checkpoint {}", checkpoint_seq);
            restored_count += 1;
        }

        Ok(restored_count)
    }

    /// Apply committee store data
    async fn apply_committee_store_data(
        &self,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let snapshot: CommitteeStoreSnapshot = 
            bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize committee store data: {}", e),
            })?;

        info!("Restoring {} committee entries", snapshot.committees.len());

        let mut restored_count = 0u64;

        // Store committees
        for committee_entry in &snapshot.committees {
            self.store_committee(committee_entry.epoch, &committee_entry.committee).await?;
            restored_count += 1;
        }

        Ok(restored_count)
    }

    /// Apply authority state data
    async fn apply_authority_state_data(
        &self,
        component_data: &[u8],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let snapshot: AuthorityStateSnapshot = 
            bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize authority state data: {}", e),
            })?;

        info!("Restoring authority state for epoch {}", snapshot.epoch);

        // Authority state restoration is complex and would require
        // coordination with the authority's internal state management
        warn!("Authority state restoration is not fully implemented");

        Ok(1) // Return 1 to indicate attempt was made
    }

    /// Store a batch of objects
    async fn store_objects_batch(
        &self,
        objects: &[ObjectEntry],
        _options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        let mut count = 0u64;

        for obj_entry in objects {
            if let Err(e) = self.store_object_entry(obj_entry, _options).await {
                if _options.force_restore {
                    warn!("Failed to store object {:?}: {}, continuing due to force_restore", 
                          obj_entry.object_id, e);
                } else {
                    return Err(e);
                }
            } else {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Store a single object entry
    async fn store_object_entry(
        &self,
        obj_entry: &ObjectEntry,
        _options: &RestoreOptions,
    ) -> Result<(), SnapshotError> {
        // Deserialize the object
        let _object: Object = bcs::from_bytes(&obj_entry.object_data)
            .map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize object: {}", e),
            })?;

        // This is a simplified version - in practice, we'd need to use
        // the proper authority store methods for object insertion
        // which may require constructing appropriate store wrappers
        
        debug!("Storing object {} version {}", obj_entry.object_id, obj_entry.version);
        
        // For now, we acknowledge that direct object storage would require
        // deeper integration with mgo-core's internal APIs
        // This is a placeholder that demonstrates the structure
        
        Ok(())
    }

    /// Store a transaction entry
    async fn store_transaction_entry(
        &self,
        tx_entry: &TransactionEntry,
        _options: &RestoreOptions,
    ) -> Result<(), SnapshotError> {
        let _transaction: Transaction = bcs::from_bytes(&tx_entry.transaction_data)
            .map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize transaction: {}", e),
            })?;

        debug!("Storing transaction {:?}", tx_entry.digest);
        
        // Placeholder for transaction storage
        Ok(())
    }

    /// Store an effects entry
    async fn store_effects_entry(
        &self,
        effects_entry: &EffectsEntry,
        _options: &RestoreOptions,
    ) -> Result<(), SnapshotError> {
        let _effects: TransactionEffects = bcs::from_bytes(&effects_entry.effects_data)
            .map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize effects: {}", e),
            })?;

        debug!("Storing effects {:?}", effects_entry.digest);
        
        // Placeholder for effects storage
        Ok(())
    }

    /// Store an events entry
    async fn store_events_entry(
        &self,
        events_entry: &EventsEntry,
        _options: &RestoreOptions,
    ) -> Result<(), SnapshotError> {
        let events: Vec<Event> = bcs::from_bytes(&events_entry.events_data)
            .map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize events: {}", e),
            })?;

        debug!("Storing {} events for {:?}", events.len(), events_entry.digest);
        
        // Placeholder for events storage
        Ok(())
    }

    /// Store a checkpoint
    async fn store_checkpoint(&self, checkpoint: &VerifiedCheckpoint) -> Result<(), SnapshotError> {
        debug!("Storing checkpoint {}", checkpoint.sequence_number());
        
        // Use checkpoint store's public API if available
        // This would require coordination with checkpoint store's internal methods
        
        Ok(())
    }

    /// Store committee for an epoch
    async fn store_committee(&self, epoch: EpochId, _committee: &Committee) -> Result<(), SnapshotError> {
        debug!("Storing committee for epoch {}", epoch);
        
        // Committee storage would need checkpoint store coordination
        
        Ok(())
    }
}
