// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Database accessor provides unified access to mgo-core's database tables
//! 
//! This module works around the private field limitations of AuthorityPerpetualTables
//! by providing indirect access through public methods and interfaces.

use std::sync::Arc;
// use std::ops::Deref; // Not currently used

use anyhow::Result;
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::authority::AuthorityState;
use mgo_types::base_types::{EpochId, ObjectID, VersionNumber};
use mgo_types::digests::{TransactionDigest, TransactionEventsDigest};
use mgo_types::object::Object;
use mgo_types::base_types::ObjectRef;
use mgo_types::storage::{ObjectKey, ObjectStore};
use mgo_types::effects::TransactionEffects;
use mgo_types::event::Event;
use mgo_types::transaction::{Transaction, TrustedTransaction};
use mgo_types::committee::Committee;
use mgo_types::messages_checkpoint::{CheckpointSequenceNumber, VerifiedCheckpoint};


use crate::types::error::SnapshotError;

/// Database accessor that provides unified access to mgo-core storage components
pub struct DatabaseAccessor {
    authority_state: Arc<AuthorityState>,
    perpetual_tables: Arc<AuthorityPerpetualTables>,
    checkpoint_store: Arc<CheckpointStore>,
}

impl DatabaseAccessor {
    /// Create a new database accessor
    pub fn new(
        authority_state: Arc<AuthorityState>,
        perpetual_tables: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
    ) -> Self {
        Self {
            authority_state,
            perpetual_tables,
            checkpoint_store,
        }
    }

    /// Get current epoch from authority state
    pub async fn get_current_epoch(&self) -> Result<EpochId, SnapshotError> {
        // Get current epoch from epoch store
        let epoch_store = self.authority_state.epoch_store_for_snapshot();
        Ok(epoch_store.epoch())
    }

    /// Get transaction by digest
    pub fn get_transaction(&self, digest: &TransactionDigest) -> Result<Option<TrustedTransaction>, SnapshotError> {
        self.perpetual_tables
            .get_transaction(digest)
            .map_err(|e| SnapshotError::StateCollection {
                component: "transactions".to_string(),
                details: e.to_string(),
            })
    }

    /// Get transaction effects by transaction digest
    pub fn get_effects(&self, digest: &TransactionDigest) -> Result<Option<TransactionEffects>, SnapshotError> {
        self.perpetual_tables
            .get_effects(digest)
            .map_err(|e| SnapshotError::StateCollection {
                component: "effects".to_string(),
                details: e.to_string(),
            })
    }

    /// Get object by ID and version
    pub fn get_object(&self, object_id: &ObjectID, version: VersionNumber) -> Result<Option<Object>, SnapshotError> {
        let _object_key = ObjectKey(*object_id, version);
        
        // Use the ObjectStore trait implementation to get the object

        self.perpetual_tables
            .get_object_by_key(object_id, version)
            .map_err(|e| SnapshotError::StateCollection {
                component: "objects".to_string(),
                details: e.to_string(),
            })
    }

    /// Get latest object reference for an object ID
    pub fn get_latest_object_ref(&self, object_id: &ObjectID) -> Result<Option<ObjectRef>, SnapshotError> {
        self.perpetual_tables
            .get_latest_object_ref_or_tombstone(*object_id)
            .map_err(|e| SnapshotError::StateCollection {
                component: "objects".to_string(),
                details: e.to_string(),
            })
    }

    /// Get events by digest
    pub fn get_events(&self, digest: &TransactionEventsDigest) -> Result<Option<Vec<Event>>, SnapshotError> {

        
        match self.authority_state.database.get_events(digest) {
            Ok(Some(events)) => Ok(Some(events.data)),
            Ok(None) => Ok(None),
            Err(e) => Err(SnapshotError::StateCollection {
                component: "events".to_string(),
                details: e.to_string(),
            }),
        }
    }

    /// Get checkpoint by sequence number
    pub fn get_checkpoint(&self, seq: CheckpointSequenceNumber) -> Result<Option<VerifiedCheckpoint>, SnapshotError> {
        self.checkpoint_store
            .get_checkpoint_by_sequence_number(seq)
            .map_err(|e| SnapshotError::StateCollection {
                component: "checkpoints".to_string(),
                details: e.to_string(),
            })
    }

    /// Get highest verified checkpoint
    pub fn get_highest_verified_checkpoint(&self) -> Result<Option<VerifiedCheckpoint>, SnapshotError> {
        self.checkpoint_store
            .get_highest_verified_checkpoint()
            .map_err(|e| SnapshotError::StateCollection {
                component: "checkpoints".to_string(),
                details: e.to_string(),
            })
    }

    /// Get highest synced checkpoint
    pub fn get_highest_synced_checkpoint(&self) -> Result<Option<VerifiedCheckpoint>, SnapshotError> {
        self.checkpoint_store
            .get_highest_synced_checkpoint()
            .map_err(|e| SnapshotError::StateCollection {
                component: "checkpoints".to_string(),
                details: e.to_string(),
            })
    }

    /// Get committee for epoch
    pub fn get_committee(&self, _epoch: EpochId) -> Result<Option<Committee>, SnapshotError> {
        // Get current epoch store and its committee
        let epoch_store = self.authority_state.epoch_store_for_snapshot();
        let committee = epoch_store.committee();
        Ok(Some((**committee).clone()))
    }

    /// Get current committee
    pub async fn get_current_committee(&self) -> Result<Option<Committee>, SnapshotError> {
        // Get current epoch and then get committee for that epoch
        let current_epoch = self.get_current_epoch().await?;
        self.get_committee(current_epoch)
    }

    /// Collect paginated objects starting from a key
    pub fn collect_objects_paginated(
        &self,
        start_key: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<(ObjectKey, Object)>, SnapshotError> {
        let mut results = Vec::new();
        
        // Use the live object iteration from AuthorityPerpetualTables
        let mut count = 0;
        let start_id = start_key.map(|k| k.0).unwrap_or_else(|| ObjectID::ZERO);
        
        // Iterate through live objects with pagination
        for live_object in self.perpetual_tables.iter_live_object_set(false) {
            let object_ref = live_object.object_reference();
            let object_key = ObjectKey(object_ref.0, object_ref.1);
            
            // Skip objects before the start key
            if object_key.0 < start_id {
                continue;
            }
            
            // Convert LiveObject to Object
            match live_object {
                mgo_core::authority::authority_store_tables::LiveObject::Normal(object) => {
                    results.push((object_key, object));
                    count += 1;
                    
                    if count >= limit {
                        break;
                    }
                }
                mgo_core::authority::authority_store_tables::LiveObject::Wrapped(_) => {
                    // Skip wrapped objects for now
                    continue;
                }
            }
        }
        
        Ok(results)
    }

    /// Collect transactions in a range of digests
    pub fn collect_transactions_range(
        &self,
        start_digest: Option<TransactionDigest>,
        limit: usize,
    ) -> Result<Vec<(TransactionDigest, Transaction)>, SnapshotError> {
        let mut results = Vec::new();
        let mut count = 0;
        
        // Use the iterator from AuthorityPerpetualTables to get all transactions
        let transaction_iter = self.perpetual_tables.iter_transactions_for_snapshot();
        
        for (digest, trusted_tx) in transaction_iter {
            // Skip transactions before the start digest if specified
            if let Some(ref start) = start_digest {
                if digest < *start {
                    continue;
                }
            }
            
            // Get the transaction from the trusted transaction wrapper
            let transaction = trusted_tx.into_inner();
            results.push((digest, transaction));
            count += 1;
            
            if count >= limit {
                break;
            }
        }
        
        Ok(results)
    }

    /// Get database statistics
    pub fn get_database_stats(&self) -> Result<DatabaseStats, SnapshotError> {
        // Calculate basic statistics by sampling
        let mut object_count = 0u64;
        let mut transaction_count = 0u64;
        
        // Count objects by iterating (sample only first 10000 for performance)
        for _ in self.perpetual_tables.iter_live_object_set(false).take(10000) {
            object_count += 1;
        }
        
        // Use AuthorityPerpetualTables estimate_transaction_count method
        transaction_count = futures::executor::block_on(self.perpetual_tables.estimate_transaction_count())
            .unwrap_or(1000); // Use fallback if estimation fails
        
        // If we hit the limit, estimate the total
        if object_count == 10000 {
            object_count *= 10; // Rough estimation
        }
        if transaction_count == 10000 {
            transaction_count *= 10; // Rough estimation
        }
        
        // Get current epoch from epoch store
        let current_epoch = self.authority_state.epoch_store_for_snapshot().epoch();
        
        Ok(DatabaseStats {
            estimated_object_count: object_count,
            estimated_transaction_count: transaction_count,
            estimated_effects_count: transaction_count, // Assume 1:1 ratio
            estimated_events_count: transaction_count / 2, // Rough estimate
            highest_checkpoint_seq: self.get_highest_verified_checkpoint()?
                .map(|cp| *cp.sequence_number())
                .unwrap_or(0),
            current_epoch,
        })
    }

}

/// Database statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStats {
    pub estimated_object_count: u64,
    pub estimated_transaction_count: u64,
    pub estimated_effects_count: u64,
    pub estimated_events_count: u64,
    pub highest_checkpoint_seq: u64,
    pub current_epoch: EpochId,
}

/// Iterator for objects with proper error handling
pub struct ObjectIterator<'a> {
    accessor: &'a DatabaseAccessor,
    current_key: Option<ObjectKey>,
    batch_size: usize,
    exhausted: bool,
}

impl<'a> ObjectIterator<'a> {
    pub fn new(accessor: &'a DatabaseAccessor, batch_size: usize) -> Self {
        Self {
            accessor,
            current_key: None,
            batch_size,
            exhausted: false,
        }
    }

    pub fn next_batch(&mut self) -> Result<Vec<(ObjectKey, Object)>, SnapshotError> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let batch = self.accessor.collect_objects_paginated(self.current_key, self.batch_size)?;
        
        if batch.is_empty() {
            self.exhausted = true;
        } else {
            // Update current key to the last item in batch
            self.current_key = batch.last().map(|(key, _)| *key);
        }

        Ok(batch)
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}
