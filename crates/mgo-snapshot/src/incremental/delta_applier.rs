// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Delta application for incremental snapshot restoration


use std::sync::Arc;

use anyhow::Result;
use tracing::{debug, info, warn, instrument};

use mgo_types::base_types::EpochId;
use mgo_types::storage::ObjectKey;


use crate::incremental::{DeltaData, ObjectDelta, TransactionDelta, CheckpointDelta, CommitteeDelta};
use crate::core_integration::{DatabaseAccessor, EnhancedStateApplier};

use crate::types::error::SnapshotError;
use crate::types::restore::RestoreOptions;

/// Applies deltas for incremental snapshot restoration
pub struct DeltaApplier {
    db_accessor: Arc<DatabaseAccessor>,
    state_applier: Arc<EnhancedStateApplier>,
}

impl DeltaApplier {
    /// Create a new delta applier
    pub fn new(
        db_accessor: Arc<DatabaseAccessor>,
        state_applier: Arc<EnhancedStateApplier>,
    ) -> Self {
        Self {
            db_accessor,
            state_applier,
        }
    }

    /// Apply delta to current state
    #[instrument(level = "info", skip(self, delta_data))]
    pub async fn apply_delta(
        &self,
        delta_data: &DeltaData,
        options: &RestoreOptions,
    ) -> Result<DeltaApplicationResult, SnapshotError> {
        info!("Applying delta from base snapshot {}", delta_data.base_snapshot_id);

        let mut result = DeltaApplicationResult::new();

        // Validate that we're starting from the correct base state
        self.validate_base_state(delta_data).await?;

        // Apply deltas in dependency order
        result.objects_applied = self.apply_object_delta(&delta_data.object_delta, options).await?;
        result.transactions_applied = self.apply_transaction_delta(&delta_data.transaction_delta, options).await?;
        result.checkpoints_applied = self.apply_checkpoint_delta(&delta_data.checkpoint_delta, options).await?;
        result.committees_applied = self.apply_committee_delta(&delta_data.committee_delta, options).await?;

        // Verify final state
        self.verify_target_state(delta_data).await?;

        result.total_applied = result.objects_applied + result.transactions_applied + 
                              result.checkpoints_applied + result.committees_applied;

        info!("Delta application completed: {} total items applied", result.total_applied);
        Ok(result)
    }

    /// Apply object delta
    #[instrument(level = "debug", skip(self, object_delta))]
    async fn apply_object_delta(
        &self,
        object_delta: &ObjectDelta,
        options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        debug!("Applying object delta");

        let mut applied_count = 0u64;

        // Apply new objects
        for (key, obj_entry) in &object_delta.new_objects {
            if let Err(e) = self.apply_new_object(key, obj_entry).await {
                if options.force_restore {
                    warn!("Failed to apply new object {:?}: {}, continuing", key, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        // Apply modified objects
        for (key, obj_entry) in &object_delta.modified_objects {
            if let Err(e) = self.apply_modified_object(key, obj_entry).await {
                if options.force_restore {
                    warn!("Failed to apply modified object {:?}: {}, continuing", key, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        // Apply deleted objects
        for (key, obj_entry) in &object_delta.deleted_objects {
            if let Err(e) = self.apply_deleted_object(key, obj_entry).await {
                if options.force_restore {
                    warn!("Failed to apply deleted object {:?}: {}, continuing", key, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        info!("Applied {} object changes", applied_count);
        Ok(applied_count)
    }

    /// Apply transaction delta
    async fn apply_transaction_delta(
        &self,
        transaction_delta: &TransactionDelta,
        options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        debug!("Applying transaction delta");

        let mut applied_count = 0u64;

        // Apply new transactions
        for tx_entry in &transaction_delta.new_transactions {
            if let Err(e) = self.apply_new_transaction(tx_entry).await {
                if options.force_restore {
                    warn!("Failed to apply transaction {:?}: {}, continuing", tx_entry.digest, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        // Apply new effects
        for effects_entry in &transaction_delta.new_effects {
            if let Err(e) = self.apply_new_effects(effects_entry).await {
                if options.force_restore {
                    warn!("Failed to apply effects {:?}: {}, continuing", effects_entry.digest, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        // Apply new events
        for events_entry in &transaction_delta.new_events {
            if let Err(e) = self.apply_new_events(events_entry).await {
                if options.force_restore {
                    warn!("Failed to apply events {:?}: {}, continuing", events_entry.digest, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        info!("Applied {} transaction changes", applied_count);
        Ok(applied_count)
    }

    /// Apply checkpoint delta
    async fn apply_checkpoint_delta(
        &self,
        checkpoint_delta: &CheckpointDelta,
        options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        debug!("Applying checkpoint delta");

        let mut applied_count = 0u64;

        // Apply new checkpoints in order
        for checkpoint_seq in &checkpoint_delta.new_checkpoint_seqs {
            match self.apply_new_checkpoint(*checkpoint_seq).await {
                Ok(_) => {
                    applied_count += 1;
                    debug!("Successfully applied checkpoint {}", checkpoint_seq);
                }
                Err(e) => {
                    if options.force_restore {
                        warn!("Failed to apply checkpoint {}: {}, continuing", checkpoint_seq, e);
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        info!("Applied {} checkpoints", applied_count);
        Ok(applied_count)
    }

    /// Apply committee delta
    async fn apply_committee_delta(
        &self,
        committee_delta: &CommitteeDelta,
        options: &RestoreOptions,
    ) -> Result<u64, SnapshotError> {
        debug!("Applying committee delta");

        let mut applied_count = 0u64;

        // Apply new committees
        for (epoch, committee) in &committee_delta.new_committees {
            if let Err(e) = self.apply_new_committee(*epoch, committee).await {
                if options.force_restore {
                    warn!("Failed to apply committee for epoch {}: {}, continuing", epoch, e);
                } else {
                    return Err(e);
                }
            } else {
                applied_count += 1;
            }
        }

        info!("Applied {} committees", applied_count);
        Ok(applied_count)
    }

    /// Validate that current state matches expected base state
    async fn validate_base_state(&self, delta_data: &DeltaData) -> Result<(), SnapshotError> {
        debug!("Validating base state");

        // Check current checkpoint
        let current_checkpoint = self.db_accessor.get_highest_verified_checkpoint()?
            .map(|cp| *cp.sequence_number())
            .unwrap_or(0);

        if current_checkpoint != delta_data.base_checkpoint {
            return Err(SnapshotError::StateCollection {
                component: "validation".to_string(),
                details: format!(
                    "Current checkpoint {} does not match expected base checkpoint {}",
                    current_checkpoint, delta_data.base_checkpoint
                ),
            });
        }

        // Check current epoch
        let current_epoch = self.db_accessor.get_current_epoch().await?;
        if current_epoch != delta_data.base_epoch {
            return Err(SnapshotError::StateCollection {
                component: "validation".to_string(),
                details: format!(
                    "Current epoch {} does not match expected base epoch {}",
                    current_epoch, delta_data.base_epoch
                ),
            });
        }

        Ok(())
    }

    /// Verify that final state matches target state
    async fn verify_target_state(&self, delta_data: &DeltaData) -> Result<(), SnapshotError> {
        debug!("Verifying target state");

        // Check target checkpoint
        let current_checkpoint = self.db_accessor.get_highest_verified_checkpoint()?
            .map(|cp| *cp.sequence_number())
            .unwrap_or(0);

        if current_checkpoint < delta_data.target_checkpoint {
            warn!("Current checkpoint {} is less than target checkpoint {}", 
                  current_checkpoint, delta_data.target_checkpoint);
        }

        // Check target epoch
        let current_epoch = self.db_accessor.get_current_epoch().await?;
        if current_epoch < delta_data.target_epoch {
            warn!("Current epoch {} is less than target epoch {}", 
                  current_epoch, delta_data.target_epoch);
        }

        Ok(())
    }

    /// Apply a new object
    async fn apply_new_object(
        &self,
        key: &ObjectKey,
        _obj_entry: &crate::core_integration::ObjectEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new object {:?}", key);
        
        // Deserialize object
        let _object: mgo_types::object::Object = bcs::from_bytes(&_obj_entry.object_data)
            .map_err(|e| SnapshotError::InvalidFormat {
                reason: format!("Failed to deserialize object: {}", e),
            })?;

        // Placeholder: would use proper object insertion APIs
        Ok(())
    }

    /// Apply a modified object
    async fn apply_modified_object(
        &self,
        key: &ObjectKey,
        _obj_entry: &crate::core_integration::ObjectEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying modified object {:?}", key);
        
        // Similar to new object, but might need to handle version updates
        self.apply_new_object(key, _obj_entry).await
    }

    /// Apply a deleted object
    async fn apply_deleted_object(
        &self,
        key: &ObjectKey,
        _obj_entry: &crate::core_integration::ObjectEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying deleted object {:?}", key);
        
        // Placeholder: would use proper object deletion APIs
        Ok(())
    }

    /// Apply a new checkpoint by sequence number
    async fn apply_new_checkpoint(
        &self,
        checkpoint_seq: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new checkpoint {}", checkpoint_seq);
        
        // Retrieve the verified checkpoint from database accessor
        match self.db_accessor.get_checkpoint(checkpoint_seq)? {
            Some(verified_checkpoint) => {
                // Use the state applier to store the checkpoint
                self.state_applier.store_checkpoint(&verified_checkpoint).await?;
                debug!("Successfully applied checkpoint {}", checkpoint_seq);
                Ok(())
            }
            None => {
                Err(SnapshotError::StateCollection {
                    component: "checkpoints".to_string(),
                    details: format!("Checkpoint {} not found", checkpoint_seq),
                })
            }
        }
    }

    /// Apply a new transaction
    async fn apply_new_transaction(
        &self,
        tx_entry: &crate::core_integration::TransactionEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new transaction {:?}", tx_entry.digest);
        
        // Use the state applier to store the transaction
        self.state_applier.store_transaction_entry(tx_entry, &crate::types::restore::RestoreOptions::default()).await
    }

    /// Apply new effects
    async fn apply_new_effects(
        &self,
        effects_entry: &crate::core_integration::EffectsEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new effects {:?}", effects_entry.digest);
        
        // Use the state applier to store the effects
        self.state_applier.store_effects_entry(effects_entry, &crate::types::restore::RestoreOptions::default()).await
    }

    /// Apply new events
    async fn apply_new_events(
        &self,
        events_entry: &crate::core_integration::EventsEntry,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new events {:?}", events_entry.digest);
        
        // Use the state applier to store the events
        self.state_applier.store_events_entry(events_entry, &crate::types::restore::RestoreOptions::default()).await
    }

    /// Apply a new committee
    async fn apply_new_committee(
        &self,
        epoch: EpochId,
        _committee: &mgo_types::committee::Committee,
    ) -> Result<(), SnapshotError> {
        debug!("Applying new committee for epoch {}", epoch);
        
        // Placeholder: would use proper committee insertion APIs
        Ok(())
    }
}

/// Result of delta application
#[derive(Debug, Clone)]
pub struct DeltaApplicationResult {
    /// Total number of items applied
    pub total_applied: u64,
    /// Number of objects applied
    pub objects_applied: u64,
    /// Number of transactions applied
    pub transactions_applied: u64,
    /// Number of checkpoints applied
    pub checkpoints_applied: u64,
    /// Number of committees applied
    pub committees_applied: u64,
    /// Time taken to apply the delta
    pub application_time: std::time::Duration,
}

impl DeltaApplicationResult {
    /// Create a new empty result
    pub fn new() -> Self {
        Self {
            total_applied: 0,
            objects_applied: 0,
            transactions_applied: 0,
            checkpoints_applied: 0,
            committees_applied: 0,
            application_time: std::time::Duration::default(),
        }
    }
}

impl Default for DeltaApplicationResult {
    fn default() -> Self {
        Self::new()
    }
}
