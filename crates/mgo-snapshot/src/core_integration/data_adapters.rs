// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Data access adapters for mgo-core integration
//! Provides safe abstractions over mgo-core's internal structures

use std::sync::Arc;
use tracing::{debug, info, warn, error};
use mgo_types::{
    base_types::{ObjectID, TransactionDigest, EpochId},

    effects::TransactionEffects,
    object::Object,
    transaction::Transaction,
    storage::ObjectStore,
};
use mgo_core::{
    authority::{AuthorityState, TransactionWithEffects},
    authority::authority_store_tables::{AuthorityPerpetualTables, LiveObject},
    checkpoints::CheckpointStore,
};
use mgo_types::messages_checkpoint::VerifiedCheckpoint;

use crate::types::{
    error::{SnapshotError, SnapshotResult},
};

/// Enhanced database accessor with adapter pattern
pub struct EnhancedDatabaseAccessor {
    authority_state: Arc<AuthorityState>,
    perpetual_tables: Arc<AuthorityPerpetualTables>,
    checkpoint_store: Arc<CheckpointStore>,
}

impl EnhancedDatabaseAccessor {
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

    /// Safe iterator over live objects using public API
    pub fn iter_live_objects(&self, include_wrapped: bool) -> impl Iterator<Item = LiveObject> + '_ {
        info!("Starting live objects iteration (include_wrapped: {})", include_wrapped);
        self.perpetual_tables.iter_live_object_set(include_wrapped)
    }

    /// Get object by ID using public API
    pub fn get_object(&self, object_id: &ObjectID) -> SnapshotResult<Option<Object>> {
        debug!("Getting object: {}", object_id);
        
        match ObjectStore::get_object(&*self.authority_state.database, object_id) {
            Ok(obj) => Ok(obj),
            Err(e) => {
                error!("Failed to get object {}: {}", object_id, e);
                Err(SnapshotError::DataAccess {
                    operation: "get_object".to_string(),
                    details: format!("Failed to retrieve object {}: {}", object_id, e),
                })
            }
        }
    }

    /// Get transaction by digest using public API
    pub fn get_transaction(&self, digest: &TransactionDigest) -> SnapshotResult<Option<Transaction>> {
        debug!("Getting transaction: {}", digest);
        
        match self.authority_state.database.get_transaction_block(digest) {
            Ok(Some(verified_tx)) => Ok(Some(verified_tx.into_inner())),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Failed to get transaction {}: {}", digest, e);
                Err(SnapshotError::DataAccess {
                    operation: "get_transaction".to_string(),
                    details: format!("Failed to retrieve transaction {}: {}", digest, e),
                })
            }
        }
    }

    /// Get transaction effects using public API
    pub fn get_transaction_effects(&self, digest: &TransactionDigest) -> SnapshotResult<Option<TransactionEffects>> {
        debug!("Getting transaction effects: {}", digest);
        
        match self.authority_state.database.get_executed_effects(digest) {
            Ok(Some(effects)) => Ok(Some(effects)),
            Ok(None) => Ok(None),
            Err(e) => {
                error!("Failed to get effects for {}: {}", digest, e);
                Err(SnapshotError::DataAccess {
                    operation: "get_transaction_effects".to_string(),
                    details: format!("Failed to retrieve effects for {}: {}", digest, e),
                })
            }
        }
    }

    /// Get transactions in checkpoint range using new public API
    pub async fn get_transactions_in_checkpoint_range(
        &self,
        start_checkpoint: u64,
        end_checkpoint: u64,
        include_effects: bool,
    ) -> SnapshotResult<Vec<TransactionWithEffects>> {
        info!(
            "Getting transactions in checkpoint range: {} to {} (include_effects: {})",
            start_checkpoint, end_checkpoint, include_effects
        );

        // TODO: This method is not yet implemented in mgo-core
        warn!("get_transactions_in_checkpoint_range not implemented in mgo-core, using placeholder");
        Ok(Vec::new())
    }

    /// Get current epoch using public API
    pub fn get_current_epoch(&self) -> SnapshotResult<EpochId> {
        debug!("Getting current epoch");
        
        match self.authority_state.get_latest_checkpoint_sequence_number() {
            Ok(checkpoint_seq) => {
                            // Try to get epoch from latest checkpoint
            match self.checkpoint_store.get_checkpoint_by_sequence_number(checkpoint_seq) {
                    Ok(Some(verified_checkpoint)) => {
                        let epoch = verified_checkpoint.epoch();
                        debug!("Current epoch from checkpoint: {}", epoch);
                        Ok(epoch)
                    }
                    Ok(None) => {
                        warn!("No checkpoint found for sequence: {}", checkpoint_seq);
                        Ok(0) // Fallback to epoch 0
                    }
                    Err(e) => {
                        error!("Failed to get checkpoint: {}", e);
                        Ok(0) // Fallback to epoch 0
                    }
                }
            }
            Err(e) => {
                error!("Failed to get latest checkpoint sequence: {}", e);
                Ok(0) // Fallback to epoch 0
            }
        }
    }

    /// Get committee for specific epoch using public API
    pub async fn get_committee_for_epoch(&self, epoch: EpochId) -> SnapshotResult<Option<Vec<u8>>> {
        debug!("Getting committee for epoch: {}", epoch);
        
        // Try to get committee info from checkpoint store
        // This is a simplified approach - in production, you'd use the committee store
        warn!("Committee retrieval not fully implemented - returning placeholder");
        
        // Return a placeholder committee structure
        let placeholder_committee = format!("committee_epoch_{}", epoch);
        Ok(Some(placeholder_committee.into_bytes()))
    }

    /// Get highest synced checkpoint
    pub fn get_highest_synced_checkpoint(&self) -> SnapshotResult<Option<VerifiedCheckpoint>> {
        info!("Getting highest synced checkpoint");
        match self.checkpoint_store.get_highest_synced_checkpoint() {
            Ok(checkpoint) => {
                if let Some(ref checkpoint) = checkpoint {
                    debug!("Found highest synced checkpoint: seq={}, epoch={}", 
                           checkpoint.sequence_number(), checkpoint.epoch());
                } else {
                    debug!("No synced checkpoint found");
                }
                Ok(checkpoint)
            }
            Err(e) => {
                error!("Failed to get highest synced checkpoint: {}", e);
                Err(SnapshotError::InvalidOperation {
                    operation: "get_highest_synced_checkpoint".to_string(),
                    reason: format!("Checkpoint store error: {}", e),
                })
            }
        }
    }

    /// Count objects in the system
    pub fn count_objects(&self) -> SnapshotResult<u64> {
        debug!("Counting objects in system");
        
        let mut count = 0u64;
        for _object in self.iter_live_objects(false) {
            count += 1;
        }
        
        info!("Total objects counted: {}", count);
        Ok(count)
    }

    /// Get transactions for a specific checkpoint
    pub async fn get_checkpoint_transactions_with_effects(
        &self,
        checkpoint_seq: u64,
    ) -> SnapshotResult<Vec<TransactionWithEffects>> {
        info!("Getting transactions with effects for checkpoint: {}", checkpoint_seq);
        
        // TODO: This method is not yet implemented in mgo-core
        warn!("get_checkpoint_transactions_with_effects not implemented in mgo-core, using placeholder");
        Ok(Vec::new())
    }

    /// Create checkpoint transaction iterator for memory-efficient processing
    pub fn iter_checkpoint_transactions(
        &self,
        start_checkpoint: u64,
        end_checkpoint: u64,
    ) -> CheckpointTransactionIteratorAdapter {
        info!("Creating checkpoint transaction iterator for range: {} to {}", start_checkpoint, end_checkpoint);
        
        // TODO: This method is not yet implemented in mgo-core
        warn!("iter_transactions_in_checkpoint_range not implemented in mgo-core, using empty iterator");
        CheckpointTransactionIteratorAdapter::empty()
    }

    /// Get database statistics
    pub fn get_database_statistics(&self) -> SnapshotResult<DatabaseStatistics> {
        info!("Collecting database statistics");
        
        let object_count = self.count_objects()?;
        let current_epoch = self.get_current_epoch()?;
        
        // Get latest checkpoint sequence
        let latest_checkpoint = self.authority_state
            .get_latest_checkpoint_sequence_number()
            .unwrap_or_else(|_| 0u64.into());
        
        Ok(DatabaseStatistics {
            object_count,
            current_epoch,
            latest_checkpoint: latest_checkpoint.into(),
            transaction_count: 0, // Would need more complex iteration to count
        })
    }
}

/// Database statistics structure
#[derive(Debug, Clone)]
pub struct DatabaseStatistics {
    pub object_count: u64,
    pub current_epoch: EpochId,
    pub latest_checkpoint: u64,
    pub transaction_count: u64,
}

/// Enhanced transaction iterator adapter using new mgo-core APIs
pub struct CheckpointTransactionIteratorAdapter {
    inner: Box<dyn Iterator<Item = mgo_types::error::MgoResult<(TransactionDigest, mgo_types::transaction::TrustedTransaction)>> + Send>,
}

impl CheckpointTransactionIteratorAdapter {
    pub fn new(
        iterator: impl Iterator<Item = mgo_types::error::MgoResult<(TransactionDigest, mgo_types::transaction::TrustedTransaction)>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::new(iterator),
        }
    }
    
    pub fn empty() -> Self {
        Self {
            inner: Box::new(std::iter::empty()),
        }
    }
}

impl Iterator for CheckpointTransactionIteratorAdapter {
    type Item = SnapshotResult<(TransactionDigest, Transaction)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((digest, trusted_tx))) => {
                let transaction = trusted_tx.into_inner();
                Some(Ok((digest, transaction)))
            }
            Some(Err(e)) => {
                Some(Err(SnapshotError::DataAccess {
                    operation: "checkpoint_transaction_iteration".to_string(),
                    details: format!("Failed to iterate transaction: {}", e),
                }))
            }
            None => None,
        }
    }
}

/// Legacy transaction iterator for backward compatibility
pub struct TransactionIterator {
    accessor: Arc<EnhancedDatabaseAccessor>,
    checkpoint_range: std::ops::Range<u64>,
    batch_size: usize,
    current_batch: Vec<TransactionWithEffects>,
    current_index: usize,
    checkpoint_index: u64,
    runtime: tokio::runtime::Handle,
}

impl TransactionIterator {
    pub fn new(
        accessor: Arc<EnhancedDatabaseAccessor>,
        start_checkpoint: u64,
        end_checkpoint: u64,
        batch_size: usize,
    ) -> Self {
        let runtime = tokio::runtime::Handle::current();
        Self {
            accessor,
            checkpoint_range: start_checkpoint..end_checkpoint,
            batch_size,
            current_batch: Vec::new(),
            current_index: 0,
            checkpoint_index: start_checkpoint,
            runtime,
        }
    }

    /// Get next batch of transactions using new API
    fn load_next_batch(&mut self) -> SnapshotResult<bool> {
        if self.checkpoint_index >= self.checkpoint_range.end {
            return Ok(false);
        }

        let end_checkpoint = std::cmp::min(
            self.checkpoint_index + self.batch_size as u64,
            self.checkpoint_range.end,
        );

        let future = self.accessor.get_transactions_in_checkpoint_range(
            self.checkpoint_index,
            end_checkpoint - 1,
            true, // include effects
        );
        
        match self.runtime.block_on(future) {
            Ok(transactions) => {
                self.current_batch = transactions;
                self.current_index = 0;
                self.checkpoint_index = end_checkpoint;
                Ok(!self.current_batch.is_empty())
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for TransactionIterator {
    type Item = SnapshotResult<TransactionWithEffects>;

    fn next(&mut self) -> Option<Self::Item> {
        // If current batch is exhausted, load next batch
        if self.current_index >= self.current_batch.len() {
            match self.load_next_batch() {
                Ok(false) => return None, // No more data
                Ok(true) => {}, // Successfully loaded batch
                Err(e) => return Some(Err(e)),
            }
        }

        // Return next transaction from current batch
        if self.current_index < self.current_batch.len() {
            let transaction_with_effects = self.current_batch[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(transaction_with_effects))
        } else {
            None
        }
    }
}

/// Object iterator adapter using public APIs
pub struct ObjectIterator<'a> {
    live_object_iter: Box<dyn Iterator<Item = LiveObject> + 'a>,
}

impl<'a> ObjectIterator<'a> {
    pub fn new(accessor: &'a EnhancedDatabaseAccessor, include_wrapped: bool) -> Self {
        let iter = accessor.iter_live_objects(include_wrapped);
        Self {
            live_object_iter: Box::new(iter),
        }
    }
}

impl<'a> Iterator for ObjectIterator<'a> {
    type Item = SnapshotResult<(ObjectID, Object)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.live_object_iter.next() {
            Some(live_object) => {
                match live_object {
                    LiveObject::Normal(obj) => {
                        let object_id = obj.id();
                        Some(Ok((object_id, obj)))
                    }
                    LiveObject::Wrapped(_) => {
                        // Skip wrapped objects for now or handle them differently
                        self.next()
                    }
                }
            }
            None => None,
        }
    }
}
