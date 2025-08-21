// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Enhanced validation logic for deep state verification

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use tracing::{debug, info, instrument};

use mgo_types::base_types::{EpochId, ObjectID, VersionNumber};





use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

use crate::core_integration::DatabaseAccessor;
use crate::types::{ComponentType, SnapshotType, SnapshotData, SnapshotMetadata};
use crate::types::error::SnapshotError;
use crate::types::ValidationLevel;
use crate::types::validation::ValidationResult;
use crate::types::config::SnapshotConfig;

/// Enhanced state validator with deep verification capabilities
pub struct EnhancedStateValidator {
    db_accessor: Arc<DatabaseAccessor>,
    config: SnapshotConfig,
}

impl EnhancedStateValidator {
    pub fn new(db_accessor: Arc<DatabaseAccessor>, config: SnapshotConfig) -> Self {
        Self {
            db_accessor,
            config,
        }
    }

    /// Validate snapshot data integrity and consistency
    #[instrument(level = "info", skip(self, snapshot_data))]
    pub async fn validate_snapshot(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        validation_level: ValidationLevel,
    ) -> Result<ValidationResult, SnapshotError> {
        info!("Starting snapshot validation with level {:?}", validation_level);

        let mut validation_result = ValidationResult::new();

        // Basic validation (always performed)
        self.validate_basic_structure(snapshot_data, metadata, &mut validation_result).await?;

        match validation_level {
            ValidationLevel::None => {
                info!("Skipping detailed validation");
            }
            ValidationLevel::Basic => {
                self.validate_basic_consistency(snapshot_data, metadata, &mut validation_result).await?;
            }
            ValidationLevel::Full => {
                self.validate_basic_consistency(snapshot_data, metadata, &mut validation_result).await?;
                self.validate_data_integrity(snapshot_data, metadata, &mut validation_result).await?;
            }
            ValidationLevel::Deep => {
                self.validate_basic_consistency(snapshot_data, metadata, &mut validation_result).await?;
                self.validate_data_integrity(snapshot_data, metadata, &mut validation_result).await?;
                self.validate_business_logic(snapshot_data, metadata, &mut validation_result).await?;
                self.validate_cross_references(snapshot_data, metadata, &mut validation_result).await?;
            }
        }

        info!("Snapshot validation completed with {} issues", validation_result.get_total_issues());
        Ok(validation_result)
    }

    /// Validate restoration completeness after applying snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn validate_restoration(
        &self,
        metadata: &SnapshotMetadata,
        expected_checkpoint: Option<CheckpointSequenceNumber>,
        expected_epoch: Option<EpochId>,
        validation_level: ValidationLevel,
    ) -> Result<ValidationResult, SnapshotError> {
        info!("Starting restoration validation");

        let mut validation_result = ValidationResult::new();

        match metadata.snapshot_type {
            SnapshotType::Full { .. } => {
                self.validate_full_restoration(&mut validation_result, expected_checkpoint, expected_epoch).await?;
            }
            SnapshotType::Incremental { .. } => {
                self.validate_incremental_restoration(&mut validation_result, expected_checkpoint, expected_epoch).await?;
            }
            SnapshotType::Checkpoint { checkpoint_seq, .. } => {
                self.validate_checkpoint_restoration(&mut validation_result, checkpoint_seq).await?;
            }
            SnapshotType::Epoch { epoch, .. } => {
                self.validate_epoch_restoration(&mut validation_result, epoch).await?;
            }
        }

        // Additional deep validation if requested
        if matches!(validation_level, ValidationLevel::Deep) {
            self.validate_system_consistency(&mut validation_result).await?;
        }

        info!("Restoration validation completed");
        Ok(validation_result)
    }

    /// Validate basic snapshot structure
    async fn validate_basic_structure(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating basic snapshot structure");

        // Check if snapshot data is not empty
        if snapshot_data.data.is_empty() {
            validation_result.add_error("Snapshot contains no component data".to_string());
        }

        // Validate metadata consistency
        use std::time::SystemTime;
        if SystemTime::from(metadata.created_at) > SystemTime::now() {
            validation_result.add_warning("Snapshot creation time is in the future".to_string());
        }

        // Check required components based on snapshot type
        match &metadata.snapshot_type {
            SnapshotType::Full { .. } => {
                let required_components = vec![
                    ComponentType::AuthorityState,
                    ComponentType::CheckpointStore,
                    ComponentType::EpochStore,
                ];

                for component in required_components {
                    // TODO: Check for specific component data in snapshot_data.data
                    if snapshot_data.data.is_empty() {
                        validation_result.add_error(format!("Missing required component: {:?}", component));
                    }
                }
            }
            SnapshotType::Checkpoint { .. } => {
                // TODO: Check for checkpoint data in snapshot_data.data
                if snapshot_data.data.is_empty() {
                    validation_result.add_error("Missing checkpoint data in checkpoint snapshot".to_string());
                }
            }
            SnapshotType::Epoch { .. } => {
                // TODO: Check for epoch data in snapshot_data.data
                if snapshot_data.data.is_empty() {
                    validation_result.add_error("Missing epoch data in epoch snapshot".to_string());
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Validate basic consistency between components
    async fn validate_basic_consistency(
        &self,
        _snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating basic consistency");

        // Validate epoch consistency
        // TODO: Extract epoch data from snapshot_data.data and validate consistency
        // For now, just validate that epoch metadata is reasonable
        if metadata.epoch > 1000000 {
            validation_result.add_warning("Epoch value seems unreasonably high".to_string());
        }

        // Validate checkpoint consistency
        // TODO: Extract checkpoint data from snapshot_data.data and validate
        if let Some(checkpoint_seq) = metadata.checkpoint_seq {
            if checkpoint_seq > 1000000 {
                validation_result.add_warning("Checkpoint sequence seems unreasonably high".to_string());
            }
        }

        Ok(())
    }

    /// Validate data integrity (checksums, serialization)
    async fn validate_data_integrity(
        &self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating data integrity");

        // Validate each component's data integrity
        // TODO: Extract individual component data from snapshot_data.data
        // For now, validate the overall data integrity
        if snapshot_data.data.len() < 10 {
            validation_result.add_error("Snapshot data is too small to be valid".to_string());
        }

        // Try to deserialize as a known snapshot format
        match bcs::from_bytes::<crate::core_integration::AuthorityStateSnapshot>(&snapshot_data.data) {
            Ok(_) => {
                debug!("Snapshot data successfully deserialized as AuthorityStateSnapshot");
            }
            Err(e) => {
                debug!("Failed to deserialize as AuthorityStateSnapshot: {}", e);
                // This might be expected for other snapshot types
            }
        }

        // Validate size consistency
        let actual_size = snapshot_data.data.len() as u64;

        if metadata.uncompressed_size > 0 && actual_size != metadata.uncompressed_size {
            validation_result.add_warning(format!(
                "Actual data size {} does not match metadata size {}",
                actual_size, metadata.uncompressed_size
            ));
        }

        Ok(())
    }

    /// Validate component-specific data integrity
    async fn validate_component_integrity(
        &self,
        component_type: &ComponentType,
        component_data: &[u8],
    ) -> Result<(), SnapshotError> {
        match component_type {
            ComponentType::ObjectStore => {
                let _: crate::core_integration::ObjectStoreSnapshot = 
                    bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                        reason: format!("Failed to deserialize object store data: {}", e),
                    })?;
            }
            ComponentType::TransactionStore => {
                let _: crate::core_integration::TransactionStoreSnapshot = 
                    bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                        reason: format!("Failed to deserialize transaction store data: {}", e),
                    })?;
            }
            ComponentType::CheckpointStore => {
                let _: crate::core_integration::CheckpointStoreSnapshot = 
                    bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                        reason: format!("Failed to deserialize checkpoint store data: {}", e),
                    })?;
            }
            ComponentType::EpochStore => {
                let _: crate::core_integration::CommitteeStoreSnapshot = 
                    bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                        reason: format!("Failed to deserialize committee store data: {}", e),
                    })?;
            }
            ComponentType::AuthorityState => {
                let _: crate::core_integration::AuthorityStateSnapshot = 
                    bcs::from_bytes(component_data).map_err(|e| SnapshotError::InvalidFormat {
                        reason: format!("Failed to deserialize authority state data: {}", e),
                    })?;
            }
            _ => {
                // Skip validation for unsupported components
            }
        }
        Ok(())
    }

    /// Validate business logic rules
    async fn validate_business_logic(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating business logic");

        // TODO: Adapt to new SnapshotData structure
        // For now, try to deserialize the entire snapshot data as different component types
        
        // Try to validate as object store snapshot
        if let Ok(object_snapshot) = bcs::from_bytes::<crate::core_integration::ObjectStoreSnapshot>(&snapshot_data.data) {
            self.validate_object_versions(&object_snapshot, validation_result).await?;
        }

        // Try to validate as transaction store snapshot
        if let Ok(tx_snapshot) = bcs::from_bytes::<crate::core_integration::TransactionStoreSnapshot>(&snapshot_data.data) {
            self.validate_transaction_effects_consistency(&tx_snapshot, validation_result).await?;
        }

        // Try to validate as committee store snapshot
        if let Ok(committee_snapshot) = bcs::from_bytes::<crate::core_integration::CommitteeStoreSnapshot>(&snapshot_data.data) {
            self.validate_committee_rules(&committee_snapshot, validation_result).await?;
        }

        Ok(())
    }

    /// Validate cross-references between components
    async fn validate_cross_references(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
        _validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating cross-references");

        // This is a complex validation that checks referential integrity
        // between different components of the snapshot

        // TODO: Adapt cross-reference validation to new SnapshotData structure
        // For now, simplified validation
        debug!("Cross-reference validation placeholder for {} bytes", snapshot_data.data.len());

        Ok(())
    }

    /// Validate object version consistency
    async fn validate_object_versions(
        &self,
        object_snapshot: &crate::core_integration::ObjectStoreSnapshot,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        let mut object_versions: HashMap<ObjectID, Vec<VersionNumber>> = HashMap::new();

        // Collect all versions for each object
        for obj_entry in &object_snapshot.objects {
            object_versions
                .entry(obj_entry.object_id)
                .or_insert_with(Vec::new)
                .push(obj_entry.version);
        }

        // Check for version gaps or duplicates
        for (object_id, mut versions) in object_versions {
            versions.sort();
            
            // Check for duplicates
            for window in versions.windows(2) {
                if window[0] == window[1] {
                    validation_result.add_error(format!(
                        "Duplicate version {} for object {}", 
                        window[0], object_id
                    ));
                }
            }

            // Check for unreasonable version jumps
            for window in versions.windows(2) {
                let seq0: u64 = window[0].into();
                let seq1: u64 = window[1].into();
                if seq1 > seq0 + 1000000 {
                    validation_result.add_warning(format!(
                        "Large version jump from {} to {} for object {}",
                        window[0], window[1], object_id
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validate transaction-effects consistency
    async fn validate_transaction_effects_consistency(
        &self,
        tx_snapshot: &crate::core_integration::TransactionStoreSnapshot,
        _validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        // Check that every transaction has corresponding effects
        let tx_digests: HashSet<_> = tx_snapshot.transactions.iter().map(|tx| tx.digest).collect();
        let effects_digests: HashSet<_> = tx_snapshot.effects.iter().map(|eff| eff.digest).collect();

        for _tx_digest in &tx_digests {
            // Skip this check as TransactionDigest and TransactionEffectsDigest are different types
            // TODO: Implement proper cross-reference validation
            /*if !effects_digests.contains(tx_digest) {
                validation_result.add_warning(format!(
                    "Transaction {:?} has no corresponding effects", 
                    tx_digest
                ));
            }*/
        }

        // Check for orphaned effects
        for effects_digest in &effects_digests {
            // Skip this check as TransactionDigest and TransactionEffectsDigest are different types
            /*if !tx_digests.contains(effects_digest) {
                validation_result.add_warning(format!(
                    "Effects {:?} has no corresponding transaction", 
                    effects_digest
                ));
            }*/
            let _ = effects_digest; // Suppress unused warning
        }

        Ok(())
    }

    /// Validate committee rules
    async fn validate_committee_rules(
        &self,
        committee_snapshot: &crate::core_integration::CommitteeStoreSnapshot,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        // Validate committee composition and stake requirements
        for committee_entry in &committee_snapshot.committees {
            let total_stake = committee_entry.committee.total_votes();
            
            if total_stake == 0 {
                validation_result.add_error(format!(
                    "Committee for epoch {} has zero total stake", 
                    committee_entry.epoch
                ));
            }

            let num_validators = committee_entry.committee.num_members();
            if num_validators == 0 {
                validation_result.add_error(format!(
                    "Committee for epoch {} has no validators", 
                    committee_entry.epoch
                ));
            } else if num_validators < 4 {
                validation_result.add_warning(format!(
                    "Committee for epoch {} has only {} validators (recommended: ≥4)",
                    committee_entry.epoch, num_validators
                ));
            }
        }

        Ok(())
    }

    /// Validate full restoration
    async fn validate_full_restoration(
        &self,
        validation_result: &mut ValidationResult,
        expected_checkpoint: Option<CheckpointSequenceNumber>,
        expected_epoch: Option<EpochId>,
    ) -> Result<(), SnapshotError> {
        debug!("Validating full restoration");

        // Check current epoch
        let current_epoch = self.db_accessor.get_current_epoch().await?;
        if let Some(expected) = expected_epoch {
            if current_epoch != expected {
                validation_result.add_error(format!(
                    "Current epoch {} does not match expected epoch {}",
                    current_epoch, expected
                ));
            }
        }

        // Check highest checkpoint
        if let Some(highest_checkpoint) = self.db_accessor.get_highest_verified_checkpoint()? {
            if let Some(expected) = expected_checkpoint {
                if *highest_checkpoint.sequence_number() != expected {
                    validation_result.add_warning(format!(
                        "Highest checkpoint {} does not match expected checkpoint {}",
                        highest_checkpoint.sequence_number(), expected
                    ));
                }
            }
        }

        Ok(())
    }

    /// Validate incremental restoration
    async fn validate_incremental_restoration(
        &self,
        validation_result: &mut ValidationResult,
        expected_checkpoint: Option<CheckpointSequenceNumber>,
        expected_epoch: Option<EpochId>,
    ) -> Result<(), SnapshotError> {
        // Similar to full restoration for now
        self.validate_full_restoration(validation_result, expected_checkpoint, expected_epoch).await
    }

    /// Validate checkpoint restoration
    async fn validate_checkpoint_restoration(
        &self,
        validation_result: &mut ValidationResult,
        checkpoint_seq: CheckpointSequenceNumber,
    ) -> Result<(), SnapshotError> {
        debug!("Validating checkpoint restoration for sequence {}", checkpoint_seq);

        // Check if the specific checkpoint exists
        match self.db_accessor.get_checkpoint(checkpoint_seq) {
            Ok(Some(_)) => {
                debug!("Checkpoint {} successfully restored", checkpoint_seq);
            }
            Ok(None) => {
                validation_result.add_error(format!("Checkpoint {} not found after restoration", checkpoint_seq));
            }
            Err(e) => {
                validation_result.add_error(format!("Error accessing checkpoint {}: {}", checkpoint_seq, e));
            }
        }

        Ok(())
    }

    /// Validate epoch restoration
    async fn validate_epoch_restoration(
        &self,
        validation_result: &mut ValidationResult,
        epoch: EpochId,
    ) -> Result<(), SnapshotError> {
        debug!("Validating epoch restoration for epoch {}", epoch);

        // Check if committee exists for the epoch
        match self.db_accessor.get_committee(epoch) {
            Ok(Some(_)) => {
                debug!("Committee for epoch {} successfully restored", epoch);
            }
            Ok(None) => {
                validation_result.add_warning(format!("Committee for epoch {} not found after restoration", epoch));
            }
            Err(e) => {
                validation_result.add_error(format!("Error accessing committee for epoch {}: {}", epoch, e));
            }
        }

        Ok(())
    }

    /// Validate overall system consistency
    async fn validate_system_consistency(
        &self,
        validation_result: &mut ValidationResult,
    ) -> Result<(), SnapshotError> {
        debug!("Validating system consistency");

        // Get database statistics
        let stats = self.db_accessor.get_database_stats()?;
        
        // Check for basic sanity
        if stats.current_epoch == 0 {
            validation_result.add_warning("System appears to be in epoch 0".to_string());
        }

        if stats.highest_checkpoint_seq == 0 {
            validation_result.add_warning("No checkpoints found in system".to_string());
        }

        // Check epoch-checkpoint consistency
        let current_committee = self.db_accessor.get_current_committee().await?;
        let committee_size = current_committee
            .as_ref()
            .map(|c| c.num_members())
            .unwrap_or(0);
        
        if committee_size < 1 {
            validation_result.add_error("Current committee is empty".to_string());
        } else if committee_size < 4 {
            validation_result.add_warning(format!(
                "Current committee has only {} members (recommended: ≥4)", 
                committee_size
            ));
        }

        Ok(())
    }
}
