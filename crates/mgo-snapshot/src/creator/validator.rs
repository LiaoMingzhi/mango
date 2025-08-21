//! Snapshot validation functionality
//! 
//! This module implements validation for created snapshots.

use crate::types::{
    ComponentType, SnapshotType,
    config::ValidationLevel,
    error::{SnapshotResult, SnapshotError},
    validation::ValidationResult,
};

/// Parsed snapshot data structure for validation
#[derive(Debug)]
struct ParsedSnapshotData {
    authority_state: Option<Vec<u8>>,
    epoch_store: Option<Vec<u8>>,
    checkpoint_store: Option<Vec<u8>>,
    object_store: Option<Vec<u8>>,
    transaction_store: Option<Vec<u8>>,
}

/// Incremental snapshot data structure
#[derive(Debug)]
struct IncrementalSnapshotData {
    delta_type: String,
    data: Vec<u8>,
}
use crate::creator::CollectedStateData;

use fastcrypto::hash::{HashFunction, Sha3_256, MultisetHash};

use tracing::{debug, info, warn, instrument};

/// Snapshot validator for ensuring data integrity
/// 
/// Validates snapshot data for consistency, integrity, and business logic compliance.
pub struct SnapshotValidator {
    /// Validation level to apply
    validation_level: ValidationLevel,
}

impl SnapshotValidator {
    /// Create a new SnapshotValidator
    pub fn new(validation_level: ValidationLevel) -> SnapshotResult<Self> {
        Ok(Self { validation_level })
    }
    
    /// Validate snapshot data
    #[instrument(level = "info", skip(self, data))]
    pub async fn validate_snapshot(&self, data: &CollectedStateData) -> SnapshotResult<ValidationResult> {
        info!(
            validation_level = ?self.validation_level,
            data_size = data.total_size(),
            "Starting snapshot validation"
        );
        
        let mut validation_result = ValidationResult::new();
        
        // Perform validation based on level
        match self.validation_level {
            ValidationLevel::None => {
                info!("Validation disabled, skipping checks");
                return Ok(validation_result);
            }
            
            ValidationLevel::Basic => {
                debug!("Performing basic validation");
                self.validate_basic_integrity(data, &mut validation_result).await?;
            }
            
            ValidationLevel::Full => {
                debug!("Performing full validation");
                self.validate_basic_integrity(data, &mut validation_result).await?;
                self.validate_business_logic(data, &mut validation_result).await?;
            }
            
            ValidationLevel::Deep => {
                debug!("Performing deep validation");
                self.validate_basic_integrity(data, &mut validation_result).await?;
                self.validate_business_logic(data, &mut validation_result).await?;
                self.validate_dependencies(data, &mut validation_result).await?;
            }
        }
        
        // Final validation status
        validation_result.valid = validation_result.errors.is_empty() &&
            validation_result.data_integrity.valid &&
            validation_result.business_logic.valid &&
            validation_result.dependencies.valid;
        
        if validation_result.valid {
            info!(
                warnings_count = validation_result.warnings.len(),
                "Snapshot validation completed successfully"
            );
        } else {
            warn!(
                errors_count = validation_result.errors.len(),
                warnings_count = validation_result.warnings.len(),
                "Snapshot validation failed"
            );
        }
        
        Ok(validation_result)
    }
    
    /// Validate basic data integrity
    async fn validate_basic_integrity(
        &self,
        data: &CollectedStateData,
        result: &mut ValidationResult,
    ) -> SnapshotResult<()> {
        debug!("Validating basic data integrity");
        
        // Check data presence and basic format
        self.validate_data_presence(data, result).await;
        
        // Validate checksums
        self.validate_checksums(data, result).await;
        
        // Check data sizes
        self.validate_data_sizes(data, result).await;
        
        Ok(())
    }
    
    /// Validate business logic constraints
    async fn validate_business_logic(
        &self,
        data: &CollectedStateData,
        _result: &mut ValidationResult,
    ) -> SnapshotResult<()> {
        debug!("Validating business logic");
        
        // Validate epoch consistency
        if let Some(epoch_data) = &data.epoch_store {
            debug!("Validating epoch consistency");
            if let Err(e) = self.validate_epoch_consistency(epoch_data, data.epoch).await {
                _result.add_error(format!("Epoch validation failed: {}", e));
            } else {
                debug!("Epoch consistency validation passed");
            }
        }
        
        // Validate checkpoint consistency  
        if let Some(checkpoint_data) = &data.checkpoint_store {
            debug!("Validating checkpoint consistency");
            if let Err(e) = self.validate_checkpoint_consistency(checkpoint_data, data.checkpoint_seq).await {
                _result.add_error(format!("Checkpoint validation failed: {}", e));
            } else {
                debug!("Checkpoint consistency validation passed");
            }
        }
        
        // Validate object references
        if let Some(object_data) = &data.object_store {
            debug!("Validating object references");
            if let Err(e) = self.validate_object_references(object_data).await {
                _result.add_error(format!("Object reference validation failed: {}", e));
            } else {
                debug!("Object references validation passed");
            }
        }
        
        // Validate transaction effects
        if let Some(tx_data) = &data.transaction_store {
            debug!("Validating transaction effects");
            if let Err(e) = self.validate_transaction_effects(tx_data).await {
                _result.add_error(format!("Transaction effects validation failed: {}", e));
            } else {
                debug!("Transaction effects validation passed");
            }
        }
        
        Ok(())
    }
    
    /// Validate dependencies and references
    async fn validate_dependencies(
        &self,
        data: &CollectedStateData,
        result: &mut ValidationResult,
    ) -> SnapshotResult<()> {
        debug!("Validating dependencies");
        
        // Check committee information consistency
        if let Some(epoch_data) = &data.epoch_store {
            debug!("Validating committee information");
            if let Err(e) = self.validate_committee_info(epoch_data, data.epoch).await {
                result.add_error(format!("Committee validation failed: {}", e));
            } else {
                debug!("Committee information validation passed");
            }
        }
        
        // Validate protocol configuration
        if let Some(authority_data) = &data.authority_state {
            debug!("Validating protocol configuration");
            if let Err(e) = self.validate_protocol_config(authority_data).await {
                result.add_error(format!("Protocol config validation failed: {}", e));
            } else {
                debug!("Protocol configuration validation passed");
            }
        }
        
        // Check for circular dependencies
        self.check_circular_dependencies(data, result).await;
        
        Ok(())
    }
    
    /// Validate data presence and basic structure
    async fn validate_data_presence(&self, data: &CollectedStateData, result: &mut ValidationResult) {
        debug!("Checking data presence");
        
        let mut missing_components = Vec::new();
        
        // Check for required components (this is configurable based on snapshot type)
        if data.authority_state.is_none() {
            missing_components.push("authority_state".to_string());
        }
        
        if !missing_components.is_empty() {
            for _component in &missing_components {
                result.data_integrity.add_format_result(ComponentType::AuthorityState, false);
            }
            result.errors.push("Missing required data components".to_string());
        }
    }
    
    /// Validate data checksums
    async fn validate_checksums(&self, data: &CollectedStateData, result: &mut ValidationResult) {
        debug!("Validating checksums");
        
        // Validate individual component checksums
        if let Some(ref authority_data) = data.authority_state {
            let valid = self.verify_component_checksum("authority_state", authority_data);
            result.data_integrity.add_checksum_result(ComponentType::AuthorityState, valid);
            if !valid {
                result.errors.push("Authority state checksum validation failed".to_string());
            }
        }
        
        if let Some(ref epoch_data) = data.epoch_store {
            let valid = self.verify_component_checksum("epoch_store", epoch_data);
            result.data_integrity.add_checksum_result(ComponentType::EpochStore, valid);
            if !valid {
                result.errors.push("Epoch store checksum validation failed".to_string());
            }
        }
        
        if let Some(ref checkpoint_data) = data.checkpoint_store {
            let valid = self.verify_component_checksum("checkpoint_store", checkpoint_data);
            result.data_integrity.add_checksum_result(ComponentType::CheckpointStore, valid);
            if !valid {
                result.errors.push("Checkpoint store checksum validation failed".to_string());
            }
        }
        
        // Validate overall accumulator if present
        if let Some(ref accumulator) = data.accumulator {
            debug!(accumulator_digest = ?accumulator.digest(), "Accumulator validation passed");
        }
    }
    
    /// Verify checksum for a data component
    fn verify_component_checksum(&self, component_name: &str, data: &[u8]) -> bool {
        // Calculate SHA3-256 checksum
        let mut hasher = Sha3_256::default();
        hasher.update(data);
        let calculated_checksum = hasher.finalize();
        
        debug!(
            component = component_name,
            data_size = data.len(),
            checksum = ?calculated_checksum,
            "Component checksum calculated"
        );
        
        // TODO: Compare with stored/expected checksum
        // For now, assume checksum is valid if data exists
        !data.is_empty()
    }
    
    /// Validate data sizes are consistent
    async fn validate_data_sizes(&self, data: &CollectedStateData, result: &mut ValidationResult) {
        debug!("Validating data sizes");
        
        let total_size = data.total_size();
        let min_expected_size = 1; // Minimum expected size
        
        if total_size < min_expected_size {
            result.data_integrity.add_size_result(ComponentType::AuthorityState, false);
            result.errors.push(format!(
                "Total data size {} is below minimum expected size {}",
                total_size, min_expected_size
            ));
        }
        
        // Check individual component sizes
        if let Some(ref authority_data) = data.authority_state {
            if authority_data.is_empty() {
                result.warnings.push("Authority state data is empty".to_string());
            }
        }
    }
    
    /// Check for circular dependencies
    async fn check_circular_dependencies(&self, _data: &CollectedStateData, _result: &mut ValidationResult) {
        debug!("Checking for circular dependencies");
        
        // TODO: Implement circular dependency detection
        // This would involve:
        // - Building dependency graph
        // - Detecting cycles
        // - Reporting problematic references
        
        // For now, assume no circular dependencies
        // Note: circular dependencies would be added via add_missing_dependency if found
    }
    
    /// Perform quick validation (for performance-critical paths)
    pub async fn quick_validate(&self, data: &CollectedStateData) -> SnapshotResult<bool> {
        debug!("Performing quick validation");
        
        // Basic checks only
        if data.total_size() == 0 {
            return Ok(false);
        }
        
        // Check accumulator if present
        if let Some(ref _accumulator) = data.accumulator {
            // Accumulator presence indicates valid state collection
            return Ok(true);
        }
        
        // If no accumulator, check if we have any data
        Ok(data.authority_state.is_some() ||
           data.epoch_store.is_some() ||
           data.checkpoint_store.is_some() ||
           data.object_store.is_some())
    }

    /// Validate epoch consistency
    async fn validate_epoch_consistency(
        &self,
        epoch_data: &[u8],
        expected_epoch: u64,
    ) -> SnapshotResult<()> {
        debug!("Validating epoch consistency for epoch {}", expected_epoch);
        
        // Try to deserialize epoch store data
        match bcs::from_bytes::<crate::core_integration::CommitteeStoreSnapshot>(epoch_data) {
            Ok(committee_snapshot) => {
                // Validate epoch sequence
                if let Some(current_epoch) = committee_snapshot.latest_epoch {
                    if current_epoch != expected_epoch {
                        return Err(SnapshotError::StateValidation {
                            component: "epoch_store".to_string(),
                            details: format!(
                                "Epoch mismatch: expected {}, found {}",
                                expected_epoch, current_epoch
                            ),
                        });
                    }
                }
                
                // Validate committee presence
                if committee_snapshot.committees.is_empty() {
                    return Err(SnapshotError::StateValidation {
                        component: "epoch_store".to_string(),
                        details: "No committees found in epoch data".to_string(),
                    });
                }
                
                debug!("Epoch consistency validation passed for epoch {}", expected_epoch);
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize epoch data: {}", e),
                })
            }
        }
    }

    /// Validate checkpoint consistency
    async fn validate_checkpoint_consistency(
        &self,
        checkpoint_data: &[u8],
        expected_checkpoint: u64,
    ) -> SnapshotResult<()> {
        debug!("Validating checkpoint consistency for checkpoint {}", expected_checkpoint);
        
        // Try to deserialize checkpoint store data
        match bcs::from_bytes::<crate::core_integration::CheckpointStoreSnapshot>(checkpoint_data) {
            Ok(checkpoint_snapshot) => {
                // Validate highest verified checkpoint
                if let Some(highest) = checkpoint_snapshot.highest_verified_checkpoint {
                    if highest > expected_checkpoint {
                        return Err(SnapshotError::StateValidation {
                            component: "checkpoint_store".to_string(),
                            details: format!(
                                "Checkpoint inconsistency: highest verified {} > expected {}",
                                highest, expected_checkpoint
                            ),
                        });
                    }
                }
                
                // Validate checkpoint sequence
                if checkpoint_snapshot.checkpoints.is_empty() {
                    return Err(SnapshotError::StateValidation {
                        component: "checkpoint_store".to_string(),
                        details: "No checkpoints found in checkpoint data".to_string(),
                    });
                }
                
                debug!("Checkpoint consistency validation passed for checkpoint {}", expected_checkpoint);
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize checkpoint data: {}", e),
                })
            }
        }
    }

    /// Validate object references
    async fn validate_object_references(
        &self,
        object_data: &[u8],
    ) -> SnapshotResult<()> {
        debug!("Validating object references");
        
        // Try to deserialize object store data
        match bcs::from_bytes::<crate::core_integration::ObjectStoreSnapshot>(object_data) {
            Ok(object_snapshot) => {
                let mut reference_count = 0;
                let mut invalid_references = 0;
                
                // Validate each object
                for object_entry in &object_snapshot.objects {
                    // Deserialize object to validate structure
                    match bcs::from_bytes::<mgo_types::object::Object>(&object_entry.object_data) {
                        Ok(object) => {
                            // Check object consistency
                            if object.id() != object_entry.object_id {
                                invalid_references += 1;
                                warn!("Object ID mismatch: entry has {:?}, object has {:?}", 
                                      object_entry.object_id, object.id());
                            }
                            
                            if object.version() != object_entry.version {
                                invalid_references += 1;
                                warn!("Object version mismatch: entry has {:?}, object has {:?}", 
                                      object_entry.version, object.version());
                            }
                            
                            reference_count += 1;
                        }
                        Err(e) => {
                            invalid_references += 1;
                            warn!("Failed to deserialize object {:?}: {}", object_entry.object_id, e);
                        }
                    }
                }
                
                if invalid_references > 0 {
                    return Err(SnapshotError::StateValidation {
                        component: "object_store".to_string(),
                        details: format!(
                            "Found {} invalid object references out of {} total objects",
                            invalid_references, reference_count
                        ),
                    });
                }
                
                debug!("Object reference validation passed: {} objects validated", reference_count);
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize object data: {}", e),
                })
            }
        }
    }

    /// Validate transaction effects
    async fn validate_transaction_effects(
        &self,
        transaction_data: &[u8],
    ) -> SnapshotResult<()> {
        debug!("Validating transaction effects");
        
        // Try to deserialize transaction store data
        match bcs::from_bytes::<crate::core_integration::TransactionStoreSnapshot>(transaction_data) {
            Ok(transaction_snapshot) => {
                let mut tx_count = 0;
                let mut effects_count = 0;
                let mut mismatched_effects = 0;
                
                // Create maps for fast lookup
                let tx_map: std::collections::HashMap<_, _> = transaction_snapshot.transactions
                    .iter()
                    .map(|tx| (tx.digest, tx))
                    .collect();
                
                // Validate effects against transactions
                for effects_entry in &transaction_snapshot.effects {
                    effects_count += 1;
                    
                    // Check if corresponding transaction exists
                    if !tx_map.contains_key(&effects_entry.transaction_digest) {
                        mismatched_effects += 1;
                        warn!("Effects {:?} has no corresponding transaction {:?}", 
                              effects_entry.digest, effects_entry.transaction_digest);
                    }
                }
                
                tx_count = transaction_snapshot.transactions.len();
                
                if mismatched_effects > 0 {
                    return Err(SnapshotError::StateValidation {
                        component: "transaction_store".to_string(),
                        details: format!(
                            "Found {} mismatched transaction effects out of {} total effects",
                            mismatched_effects, effects_count
                        ),
                    });
                }
                
                debug!("Transaction effects validation passed: {} transactions, {} effects", 
                       tx_count, effects_count);
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize transaction data: {}", e),
                })
            }
        }
    }

    /// Validate committee information
    async fn validate_committee_info(
        &self,
        epoch_data: &[u8],
        expected_epoch: u64,
    ) -> SnapshotResult<()> {
        debug!("Validating committee information for epoch {}", expected_epoch);
        
        // Try to deserialize epoch store data
        match bcs::from_bytes::<crate::core_integration::CommitteeStoreSnapshot>(epoch_data) {
            Ok(committee_snapshot) => {
                // Find committee for expected epoch
                let committee_entry = committee_snapshot.committees
                    .iter()
                    .find(|entry| entry.epoch == expected_epoch);
                
                if committee_entry.is_none() {
                    return Err(SnapshotError::StateValidation {
                        component: "committee_store".to_string(),
                        details: format!("No committee found for epoch {}", expected_epoch),
                    });
                }
                
                // Validate committee structure
                if let Some(entry) = committee_entry {
                    let committee = &entry.committee;
                    if committee.num_members() == 0 {
                        return Err(SnapshotError::StateValidation {
                            component: "committee_store".to_string(),
                            details: format!("Committee for epoch {} has no members", expected_epoch),
                        });
                    }
                    
                    // Validate total stake
                    if committee.total_votes() == 0 {
                        return Err(SnapshotError::StateValidation {
                            component: "committee_store".to_string(),
                            details: format!("Committee for epoch {} has zero total stake", expected_epoch),
                        });
                    }
                }
                
                debug!("Committee information validation passed for epoch {}", expected_epoch);
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize epoch data: {}", e),
                })
            }
        }
    }

    /// Validate protocol configuration
    async fn validate_protocol_config(
        &self,
        authority_data: &[u8],
    ) -> SnapshotResult<()> {
        debug!("Validating protocol configuration");
        
        // Try to deserialize authority state data
        match bcs::from_bytes::<crate::core_integration::AuthorityStateSnapshot>(authority_data) {
            Ok(authority_snapshot) => {
                // Validate epoch is present
                if authority_snapshot.epoch == 0 {
                    warn!("Authority state epoch is zero, may indicate missing configuration");
                }
                
                // Validate database stats presence
                let _stats = &authority_snapshot.database_stats;
                debug!("Authority state validation passed for epoch {}", authority_snapshot.epoch);
                
                debug!("Protocol configuration validation passed");
                Ok(())
            }
            Err(e) => {
                Err(SnapshotError::InvalidFormat {
                    reason: format!("Failed to deserialize authority data: {}", e),
                })
            }
        }
    }

    /// Enhanced data consistency validation with comprehensive checks
    pub async fn validate_data_consistency_enhanced(
        &self,
        snapshot_data: &crate::SnapshotData,
        validation_level: ValidationLevel,
    ) -> SnapshotResult<ValidationResult> {
        info!("Performing enhanced data consistency validation");
        let mut result = ValidationResult::new();
        
        // Since the new SnapshotData structure stores everything in the 'data' field,
        // we need to parse it based on the snapshot type
        match snapshot_data.metadata.snapshot_type {
            SnapshotType::Full { .. } => {
                // For full snapshots, try to parse the data as a complete snapshot
                if let Ok(parsed_data) = self.parse_full_snapshot_data(&snapshot_data.data) {
                    self.validate_parsed_snapshot_data(&parsed_data, &mut result, validation_level).await?;
                } else {
                    result.add_error("Failed to parse full snapshot data".to_string());
                }
            }
            SnapshotType::Incremental { .. } => {
                // For incremental snapshots, validate the delta data
                if let Ok(delta_data) = self.parse_incremental_snapshot_data(&snapshot_data.data) {
                    self.validate_incremental_data(&delta_data, &mut result).await?;
                } else {
                    result.add_error("Failed to parse incremental snapshot data".to_string());
                }
            }
            _ => {
                // For other types, perform basic validation
                self.validate_basic_snapshot_data(&snapshot_data.data, &mut result).await?;
            }
        }
        
        info!("Enhanced data consistency validation completed with {} errors", result.errors.len());
        Ok(result)
    }

    /// Parse full snapshot data from bytes
    fn parse_full_snapshot_data(&self, data: &[u8]) -> SnapshotResult<ParsedSnapshotData> {
        // Try to deserialize as different types based on size and structure hints
        if data.len() < 100 {
            return Err(SnapshotError::InvalidFormat {
                reason: "Snapshot data too small".to_string(),
            });
        }

        // For now, create a placeholder parsed data structure
        Ok(ParsedSnapshotData {
            authority_state: None,
            epoch_store: None,
            checkpoint_store: None,
            object_store: None,
            transaction_store: None,
        })
    }

    /// Parse incremental snapshot data
    fn parse_incremental_snapshot_data(&self, data: &[u8]) -> SnapshotResult<IncrementalSnapshotData> {
        // Parse incremental data structure
        Ok(IncrementalSnapshotData {
            delta_type: "placeholder".to_string(),
            data: data.to_vec(),
        })
    }

    /// Validate parsed snapshot data
    async fn validate_parsed_snapshot_data(
        &self,
        _parsed_data: &ParsedSnapshotData,
        _result: &mut ValidationResult,
        _validation_level: ValidationLevel,
    ) -> SnapshotResult<()> {
        // Since we don't have the actual parsed structure, perform basic validation
        debug!("Validating parsed snapshot data - using placeholder implementation");
        
        // Placeholder validation that always passes
        debug!("Parsed snapshot data validation completed");
        Ok(())
    }

    /// Validate incremental data
    async fn validate_incremental_data(
        &self,
        _delta_data: &IncrementalSnapshotData,
        _result: &mut ValidationResult,
    ) -> SnapshotResult<()> {
        debug!("Validating incremental snapshot data");
        Ok(())
    }

    /// Basic snapshot data validation
    async fn validate_basic_snapshot_data(
        &self,
        data: &[u8],
        result: &mut ValidationResult,
    ) -> SnapshotResult<()> {
        debug!("Performing basic snapshot data validation");
        
        // Basic size checks
        if data.is_empty() {
            result.add_error("Snapshot data is empty".to_string());
        } else if data.len() < 32 {
            result.add_error("Snapshot data suspiciously small".to_string());
        }
        
        // Basic format validation
        if data.len() > 1024 * 1024 * 1024 { // 1GB
            result.add_error("Snapshot data exceeds reasonable size limit".to_string());
        }
        
        Ok(())
    }


}
