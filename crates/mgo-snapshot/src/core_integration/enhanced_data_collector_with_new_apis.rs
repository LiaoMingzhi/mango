// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Enhanced data collector using proposed mgo-core APIs
//! This demonstrates how to efficiently collect transaction data across checkpoint ranges

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn, debug, error};

use mgo_core::authority::AuthorityState;
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_types::base_types::TransactionDigest;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;
use mgo_types::transaction::TrustedTransaction;
use mgo_types::effects::TransactionEffects;

use crate::types::error::{SnapshotError, SnapshotResult};

/// Enhanced data collector that uses the proposed mgo-core APIs
/// This will replace the current placeholder implementation once APIs are available
pub struct FutureEnhancedDataCollector {
    authority_state: Arc<AuthorityState>,
    perpetual_tables: Arc<AuthorityPerpetualTables>,
    checkpoint_store: Arc<CheckpointStore>,
    batch_size: usize,
}

impl FutureEnhancedDataCollector {
    pub fn new(
        authority_state: Arc<AuthorityState>,
        perpetual_tables: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
    ) -> Self {
        Self {
            authority_state,
            perpetual_tables,
            checkpoint_store,
            batch_size: 1000,
        }
    }

    /// Collect transactions within a checkpoint range using the new iterator API
    pub async fn collect_transactions_in_range_with_new_apis(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        end_checkpoint: CheckpointSequenceNumber,
    ) -> SnapshotResult<Vec<u8>> {
        info!("Collecting transactions from checkpoint {} to {}", start_checkpoint, end_checkpoint);

        let mut collected_transactions = Vec::new();
        let mut transaction_count = 0;

        // Use the new iterator API for memory-efficient processing
        let transaction_iterator = self.authority_state
            .iter_transactions_in_checkpoint_range(start_checkpoint, end_checkpoint);

        for transaction_result in transaction_iterator {
            match transaction_result {
                Ok((digest, transaction)) => {
                    collected_transactions.push((digest, transaction));
                    transaction_count += 1;

                    // Process in batches to manage memory
                    if transaction_count % self.batch_size == 0 {
                        debug!("Processed {} transactions so far", transaction_count);
                    }
                }
                Err(e) => {
                    error!("Failed to retrieve transaction: {}", e);
                    return Err(SnapshotError::DataAccess {
                        operation: "iterate_transactions".to_string(),
                        details: format!("Failed to iterate transactions: {}", e),
                    });
                }
            }
        }

        info!("Successfully collected {} transactions", transaction_count);

        // Serialize the collected data
        bcs::to_bytes(&collected_transactions).map_err(|e| {
            SnapshotError::StateCollection {
                component: "transactions".to_string(),
                details: format!("Failed to serialize transactions: {}", e),
            }
        })
    }

    /// Collect transactions with effects for a specific checkpoint
    pub async fn collect_checkpoint_transactions_with_effects_using_new_apis(
        &self,
        checkpoint_seq: CheckpointSequenceNumber,
    ) -> SnapshotResult<Vec<u8>> {
        info!("Collecting transactions with effects for checkpoint {}", checkpoint_seq);

        // Use the new checkpoint API to get full transaction data
        let transactions_with_effects = self.authority_state
            .get_checkpoint_transactions_with_effects(checkpoint_seq)
            .await
            .map_err(|e| SnapshotError::DataAccess {
                operation: "get_checkpoint_transactions_with_effects".to_string(),
                details: format!("Failed to get checkpoint transactions: {}", e),
            })?;

        info!("Collected {} transactions with effects", transactions_with_effects.len());

        // Serialize the collected data
        bcs::to_bytes(&transactions_with_effects).map_err(|e| {
            SnapshotError::StateCollection {
                component: "checkpoint_transactions".to_string(),
                details: format!("Failed to serialize checkpoint transactions: {}", e),
            }
        })
    }

    /// Collect transactions in batch mode for large ranges
    pub async fn collect_transactions_in_batch_mode_with_new_apis(
        &self,
        start_checkpoint: CheckpointSequenceNumber,
        end_checkpoint: CheckpointSequenceNumber,
        batch_checkpoint_size: u64,
    ) -> SnapshotResult<Vec<u8>> {
        info!("Collecting transactions in batch mode: {} to {} (batch size: {})", 
               start_checkpoint, end_checkpoint, batch_checkpoint_size);

        let mut all_transactions = Vec::new();
        let mut current_start = start_checkpoint;

        while current_start <= end_checkpoint {
            let current_end = std::cmp::min(
                current_start + batch_checkpoint_size - 1,
                end_checkpoint
            );

            debug!("Processing checkpoint batch: {} to {}", current_start, current_end);

            // Use the new range API for efficient batch processing
            let batch_transactions = self.authority_state
                .get_transactions_in_checkpoint_range(current_start, current_end, true)
                .await
                .map_err(|e| SnapshotError::DataAccess {
                    operation: "get_transactions_in_checkpoint_range".to_string(),
                    details: format!("Failed to get transactions in range: {}", e),
                })?;

            all_transactions.extend(batch_transactions);
            current_start = current_end + 1;

            // Log progress
            info!("Processed checkpoints {} to {}, total transactions: {}", 
                  start_checkpoint, current_end, all_transactions.len());
        }

        info!("Successfully collected {} transactions in total", all_transactions.len());

        // Serialize the collected data
        bcs::to_bytes(&all_transactions).map_err(|e| {
            SnapshotError::StateCollection {
                component: "batch_transactions".to_string(),
                details: format!("Failed to serialize batch transactions: {}", e),
            }
        })
    }

    /// Collect objects using read-only table access
    pub async fn collect_objects_with_new_apis(
        &self,
        start_object_id: Option<mgo_types::base_types::ObjectID>,
        limit: Option<usize>,
    ) -> SnapshotResult<Vec<u8>> {
        info!("Collecting objects using enhanced table access");

        // Get read-only access to the objects table
        let objects_table = self.perpetual_tables.get_objects_table_for_snapshot();
        let mut collected_objects = Vec::new();
        let mut object_count = 0;
        let limit = limit.unwrap_or(usize::MAX);

        // Iterate through objects
        let iter = if let Some(start_id) = start_object_id {
            // Start from specific object ID
            objects_table.iter().skip_while(move |(key, _)| key.0 != start_id)
        } else {
            // Start from beginning
            objects_table.iter()
        };

        for result in iter.take(limit) {
            match result {
                Ok((key, wrapper)) => {
                    collected_objects.push((key, wrapper));
                    object_count += 1;

                    if object_count % self.batch_size == 0 {
                        debug!("Collected {} objects so far", object_count);
                    }
                }
                Err(e) => {
                    error!("Failed to read object: {}", e);
                    return Err(SnapshotError::DataAccess {
                        operation: "iterate_objects".to_string(),
                        details: format!("Failed to iterate objects: {}", e),
                    });
                }
            }
        }

        info!("Successfully collected {} objects", object_count);

        // Serialize the collected data
        bcs::to_bytes(&collected_objects).map_err(|e| {
            SnapshotError::StateCollection {
                component: "objects".to_string(),
                details: format!("Failed to serialize objects: {}", e),
            }
        })
    }

    /// Collect effects using read-only table access
    pub async fn collect_effects_with_new_apis(
        &self,
        transaction_digests: &[TransactionDigest],
    ) -> SnapshotResult<Vec<u8>> {
        info!("Collecting effects for {} transactions", transaction_digests.len());

        let effects_table = self.perpetual_tables.get_effects_table_for_snapshot();
        let executed_effects_table = self.perpetual_tables.get_executed_effects_table_for_snapshot();
        
        let mut collected_effects = Vec::new();

        for tx_digest in transaction_digests {
            // Get the effects digest for this transaction
            if let Ok(Some(effects_digest)) = executed_effects_table.get(tx_digest) {
                // Get the actual effects
                if let Ok(Some(effects)) = effects_table.get(&effects_digest) {
                    collected_effects.push((effects_digest, effects, *tx_digest));
                } else {
                    warn!("Effects not found for digest: {}", effects_digest);
                }
            } else {
                warn!("Executed effects not found for transaction: {}", tx_digest);
            }
        }

        info!("Successfully collected {} effects", collected_effects.len());

        // Serialize the collected data
        bcs::to_bytes(&collected_effects).map_err(|e| {
            SnapshotError::StateCollection {
                component: "effects".to_string(),
                details: format!("Failed to serialize effects: {}", e),
            }
        })
    }

    /// Get checkpoint with full transaction data
    pub async fn get_checkpoint_with_full_data_using_new_apis(
        &self,
        checkpoint_seq: CheckpointSequenceNumber,
    ) -> SnapshotResult<Vec<u8>> {
        info!("Getting checkpoint {} with full transaction data", checkpoint_seq);

        // Use the new checkpoint API
        let checkpoint_with_transactions = self.checkpoint_store
            .get_checkpoint_with_full_transactions(checkpoint_seq, &self.perpetual_tables)
            .await
            .map_err(|e| SnapshotError::DataAccess {
                operation: "get_checkpoint_with_full_transactions".to_string(),
                details: format!("Failed to get checkpoint with transactions: {}", e),
            })?
            .ok_or_else(|| SnapshotError::DataAccess {
                operation: "checkpoint_not_found".to_string(),
                details: format!("Checkpoint {} not found", checkpoint_seq),
            })?;

        info!("Successfully retrieved checkpoint with {} transactions", 
              checkpoint_with_transactions.transactions.len());

        // Serialize the collected data
        bcs::to_bytes(&checkpoint_with_transactions).map_err(|e| {
            SnapshotError::StateCollection {
                component: "checkpoint_with_transactions".to_string(),
                details: format!("Failed to serialize checkpoint with transactions: {}", e),
            }
        })
    }

    /// Set batch size for operations
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size;
        debug!("Set batch size to {}", batch_size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests would need the enhanced mgo-core APIs to be implemented
    
    #[tokio::test]
    #[ignore = "Requires enhanced mgo-core APIs"]
    async fn test_transaction_range_collection() {
        // Test would verify transaction collection across checkpoint ranges
        // once the enhanced APIs are available in mgo-core
    }

    #[tokio::test]
    #[ignore = "Requires enhanced mgo-core APIs"]
    async fn test_checkpoint_transaction_collection() {
        // Test would verify checkpoint transaction collection with effects
        // once the enhanced APIs are available in mgo-core
    }

    #[tokio::test]
    #[ignore = "Requires enhanced mgo-core APIs"]
    async fn test_batch_mode_collection() {
        // Test would verify batch mode collection for large ranges
        // once the enhanced APIs are available in mgo-core
    }

    #[tokio::test]
    #[ignore = "Requires enhanced mgo-core APIs"]
    async fn test_object_iteration() {
        // Test would verify object iteration using table access
        // once the enhanced APIs are available in mgo-core
    }
}
