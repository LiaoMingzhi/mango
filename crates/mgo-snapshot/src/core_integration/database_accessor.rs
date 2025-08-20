// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Database accessor provides unified access to mgo-core's database tables
//! 
//! This module works around the private field limitations of AuthorityPerpetualTables
//! by providing indirect access through public methods and interfaces.

use std::sync::Arc;

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
        // TODO: Find the correct way to get current epoch from AuthorityState
        // For now, return a placeholder value
        Ok(0)
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
        // TODO: Implement proper committee access when the correct API is available
        // For now, return None as placeholder
        Ok(None)
    }

    /// Get current committee
    pub fn get_current_committee(&self) -> Result<Option<Committee>, SnapshotError> {
        // TODO: Find the correct way to get current epoch from AuthorityState
        // For now, return None as placeholder
        Ok(None)
    }

    /// Collect paginated objects starting from a key
    pub fn collect_objects_paginated(
        &self,
        start_key: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<(ObjectKey, Object)>, SnapshotError> {

        
        let results = Vec::new();
        
        // Get an iterator over all objects in the store
        // This is a workaround since we can't directly access the private objects field
        let _start_object_id = start_key.map(|k| k.0).unwrap_or_else(|| ObjectID::ZERO);
        
        // This is a simplified placeholder implementation
        // In practice, we would need proper iteration APIs from mgo-core
        // For now, just return empty results as this requires complex workarounds
        if limit > 0 {
            // Placeholder logic - would need proper object iteration
        }
        
        Ok(results)
    }

    /// Collect transactions in a range of digests
    pub fn collect_transactions_range(
        &self,
        _start_digest: Option<TransactionDigest>,
        _limit: usize,
    ) -> Result<Vec<(TransactionDigest, Transaction)>, SnapshotError> {
        // This is a simplified implementation
        // In practice, we'd need better iteration support
        let results = Vec::new();
        
        // For now, return empty - this requires more complex iteration
        // which would need additional public APIs from mgo-core
        Ok(results)
    }

    /// Get database statistics
    pub fn get_database_stats(&self) -> Result<DatabaseStats, SnapshotError> {
        // Calculate statistics without direct access to private fields
        // This is an approximation and could be improved with better APIs
        
        Ok(DatabaseStats {
            estimated_object_count: 0, // Would need counting API
            estimated_transaction_count: 0,
            estimated_effects_count: 0,
            estimated_events_count: 0,
            highest_checkpoint_seq: self.get_highest_verified_checkpoint()?
                .map(|cp| *cp.sequence_number())
                .unwrap_or(0),
            current_epoch: 0, // TODO: Get actual current epoch
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
