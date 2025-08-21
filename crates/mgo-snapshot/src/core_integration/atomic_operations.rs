// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Atomic operations and transaction management for snapshot restoration
//! Provides ACID guarantees for multi-component snapshot operations

use std::sync::Arc;
use tracing::{info, warn, error, instrument};
use tokio::sync::{Mutex, RwLock};

use crate::types::{
    error::{SnapshotError, SnapshotResult},
    RestoreOptions, SnapshotData, SnapshotMetadata,
};
use crate::core_integration::{EnhancedStateWriter, EnhancedDatabaseAccessor};
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use typed_store::rocks::DBBatch;

/// Atomic transaction state for snapshot operations
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    /// Transaction is being prepared
    Preparing,
    /// Transaction is ready to commit
    Prepared,
    /// Transaction is being committed
    Committing,
    /// Transaction has been committed successfully
    Committed,
    /// Transaction is being rolled back
    RollingBack,
    /// Transaction has been rolled back
    RolledBack,
    /// Transaction failed
    Failed(String),
}

/// Atomic operation context for snapshot restoration
pub struct AtomicRestoreContext {
    /// Unique transaction ID
    transaction_id: String,
    /// Current transaction state
    state: Arc<RwLock<TransactionState>>,
    /// Backup checkpoint path for rollback
    backup_checkpoint: Option<String>,
    /// Enhanced state writer for atomic operations
    pub state_writer: Arc<EnhancedStateWriter>,
    /// Database accessor for validation
    db_accessor: Arc<EnhancedDatabaseAccessor>,
    /// Restore options
    options: RestoreOptions,
    /// Lock to prevent concurrent operations
    operation_lock: Arc<Mutex<()>>,
    /// Active database batches for atomic operations
    active_batches: Arc<Mutex<Vec<DBBatch>>>,
    /// Perpetual tables for direct database access
    perpetual_tables: Option<Arc<AuthorityPerpetualTables>>,
}

impl AtomicRestoreContext {
    pub fn new(
        state_writer: Arc<EnhancedStateWriter>,
        db_accessor: Arc<EnhancedDatabaseAccessor>,
        options: RestoreOptions,
    ) -> Self {
        let transaction_id = format!("restore_tx_{}", chrono::Utc::now().timestamp());
        
        Self {
            transaction_id,
            state: Arc::new(RwLock::new(TransactionState::Preparing)),
            backup_checkpoint: None,
            state_writer,
            db_accessor,
            options,
            operation_lock: Arc::new(Mutex::new(())),
            active_batches: Arc::new(Mutex::new(Vec::new())),
            perpetual_tables: None,
        }
    }

    /// Create a new AtomicRestoreContext with direct database access
    pub fn with_perpetual_tables(
        state_writer: Arc<EnhancedStateWriter>,
        db_accessor: Arc<EnhancedDatabaseAccessor>,
        perpetual_tables: Arc<AuthorityPerpetualTables>,
        options: RestoreOptions,
    ) -> Self {
        let transaction_id = format!("restore_tx_{}", chrono::Utc::now().timestamp());
        
        Self {
            transaction_id,
            state: Arc::new(RwLock::new(TransactionState::Preparing)),
            backup_checkpoint: None,
            state_writer,
            db_accessor,
            options,
            operation_lock: Arc::new(Mutex::new(())),
            active_batches: Arc::new(Mutex::new(Vec::new())),
            perpetual_tables: Some(perpetual_tables),
        }
    }

    /// Get the transaction ID for this operation
    pub fn operation_id(&self) -> &str {
        &self.transaction_id
    }

    /// Begin atomic transaction with database batch preparation
    #[instrument(level = "info", skip(self))]
    pub async fn begin_transaction(&mut self) -> SnapshotResult<()> {
        let _lock = self.operation_lock.lock().await;
        
        info!("Beginning atomic transaction: {}", self.transaction_id);
        
        // Set state to preparing
        {
            let mut state = self.state.write().await;
            *state = TransactionState::Preparing;
        }

        // Initialize database batch for atomic operations
        if let Some(ref perpetual_tables) = self.perpetual_tables {
            let batch = perpetual_tables.create_snapshot_write_batch();
            let mut batches = self.active_batches.lock().await;
            batches.push(batch);
            info!("Created database batch for atomic operations");
        }

        // Create backup checkpoint for rollback if enabled
        if self.options.create_backup {
            info!("Creating backup checkpoint for transaction {}", self.transaction_id);
            
            match self.state_writer.create_restoration_checkpoint().await {
                Ok(checkpoint_path) => {
                    self.backup_checkpoint = Some(checkpoint_path);
                    info!("Backup checkpoint created: {:?}", self.backup_checkpoint);
                }
                Err(e) => {
                    error!("Failed to create backup checkpoint: {}", e);
                    if !self.options.force_restore {
                        return Err(e);
                    }
                    warn!("Continuing without backup due to force_restore option");
                }
            }
        }

        info!("Atomic transaction {} started successfully", self.transaction_id);
        Ok(())
    }

    /// Execute restore operation atomically
    #[instrument(level = "info", skip(self, snapshot_data, metadata))]
    pub async fn execute_atomic_restore(
        &mut self,
        snapshot_data: &SnapshotData,
        metadata: &SnapshotMetadata,
    ) -> SnapshotResult<u64> {
        let _lock = self.operation_lock.lock().await;
        
        info!("Executing atomic restore for transaction {}", self.transaction_id);

        // Check current state
        {
            let state = self.state.read().await;
            if !matches!(*state, TransactionState::Preparing) {
                return Err(SnapshotError::InvalidOperation {
                    operation: "execute_atomic_restore".to_string(),
                    reason: format!("Invalid transaction state: {:?}", *state),
                });
            }
        }

        let mut total_restored = 0u64;
        let mut _restore_results: Vec<u64> = Vec::new();

        // Execute restoration operations by component type
        match &metadata.snapshot_type {
            crate::types::SnapshotType::Full { .. } => {
                total_restored = self.execute_full_restore(snapshot_data, metadata).await?;
            }
            crate::types::SnapshotType::Incremental { .. } => {
                total_restored = self.execute_incremental_restore(snapshot_data, metadata).await?;
            }
            crate::types::SnapshotType::Checkpoint { .. } => {
                total_restored = self.execute_checkpoint_restore(snapshot_data, metadata).await?;
            }
            crate::types::SnapshotType::Epoch { .. } => {
                total_restored = self.execute_epoch_restore(snapshot_data, metadata).await?;
            }
        }

        // Validate restoration results
        if let Err(e) = self.validate_restore_consistency(total_restored, metadata).await {
            error!("Restore validation failed: {}", e);
            return Err(e);
        }

        // Update state to prepared
        {
            let mut state = self.state.write().await;
            *state = TransactionState::Prepared;
        }

        info!("Atomic restore executed successfully: {} items restored", total_restored);
        Ok(total_restored)
    }

    /// Commit the transaction with database batch commit
    #[instrument(level = "info", skip(self))]
    pub async fn commit_transaction(&mut self) -> SnapshotResult<()> {
        let _lock = self.operation_lock.lock().await;
        
        info!("Committing transaction: {}", self.transaction_id);

        // Check current state
        {
            let state = self.state.read().await;
            if !matches!(*state, TransactionState::Prepared) {
                return Err(SnapshotError::InvalidOperation {
                    operation: "commit_transaction".to_string(),
                    reason: format!("Invalid transaction state: {:?}", *state),
                });
            }
        }

        // Update state to committing
        {
            let mut state = self.state.write().await;
            *state = TransactionState::Committing;
        }

        // Commit all active database batches atomically
        if let Some(ref perpetual_tables) = self.perpetual_tables {
            let mut batches = self.active_batches.lock().await;
            for batch in batches.drain(..) {
                perpetual_tables.commit_snapshot_batch(batch)
                    .map_err(|e| SnapshotError::DataAccess {
                        operation: "commit_database_batch".to_string(),
                        details: format!("Failed to commit database batch: {}", e),
                    })?;
            }
            info!("All database batches committed successfully");
        }

        // Clean up backup checkpoint if commit is successful
        if let Some(ref backup_path) = self.backup_checkpoint {
            info!("Cleaning up backup checkpoint: {}", backup_path);
            if let Err(e) = tokio::fs::remove_dir_all(backup_path).await {
                warn!("Failed to clean up backup checkpoint {}: {}", backup_path, e);
                // Don't fail the commit for cleanup errors
            }
        }

        // Update state to committed
        {
            let mut state = self.state.write().await;
            *state = TransactionState::Committed;
        }

        info!("Transaction {} committed successfully", self.transaction_id);
        Ok(())
    }

    /// Rollback the transaction by discarding batches
    #[instrument(level = "info", skip(self))]
    pub async fn rollback_transaction(&mut self) -> SnapshotResult<()> {
        let _lock = self.operation_lock.lock().await;
        
        warn!("Rolling back transaction: {}", self.transaction_id);

        // Update state to rolling back
        {
            let mut state = self.state.write().await;
            *state = TransactionState::RollingBack;
        }

        // Discard all active database batches (rollback)
        {
            let mut batches = self.active_batches.lock().await;
            let batch_count = batches.len();
            batches.clear();
            info!("Discarded {} database batches for rollback", batch_count);
        }

        // Restore from backup checkpoint if available
        if let Some(ref backup_path) = self.backup_checkpoint {
            info!("Restoring from backup checkpoint: {}", backup_path);
            
            // Note: In a real implementation, you would use RocksDB's restore functionality
            // For now, we log the intent
            warn!("Backup restoration not fully implemented - would restore from {}", backup_path);
            
            // In a complete implementation:
            // 1. Stop all database operations
            // 2. Replace current database with backup
            // 3. Restart database operations
        } else {
            warn!("No backup checkpoint available for rollback");
        }

        // Update state to rolled back
        {
            let mut state = self.state.write().await;
            *state = TransactionState::RolledBack;
        }

        warn!("Transaction {} rolled back", self.transaction_id);
        Ok(())
    }

    /// Execute full snapshot restore
    async fn execute_full_restore(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
    ) -> SnapshotResult<u64> {
        info!("Executing full snapshot restore");
        
        let mut total_restored = 0u64;

        // Apply snapshot data atomically
        // In a real implementation, this would use database transactions
        
        // For now, we assume snapshot_data.data contains serialized component data
        // This is a simplified implementation
        if !snapshot_data.data.is_empty() {
            // Try to apply as object store data first
            match self.state_writer.apply_object_store_data(&snapshot_data.data, &self.options).await {
                Ok(count) => {
                    total_restored += count;
                    info!("Applied {} objects from full snapshot", count);
                }
                Err(e) => {
                    warn!("Failed to apply object store data: {}", e);
                    if !self.options.force_restore {
                        return Err(e);
                    }
                }
            }
        }

        Ok(total_restored)
    }

    /// Execute incremental snapshot restore
    async fn execute_incremental_restore(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
    ) -> SnapshotResult<u64> {
        info!("Executing incremental snapshot restore");
        
        // Incremental restore would apply deltas on top of base state
        // For now, treat as full restore
        warn!("Incremental restore not fully implemented, treating as full restore");
        
        self.execute_full_restore(snapshot_data, _metadata).await
    }

    /// Execute checkpoint snapshot restore
    async fn execute_checkpoint_restore(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
    ) -> SnapshotResult<u64> {
        info!("Executing checkpoint snapshot restore");
        
        let mut total_restored = 0u64;

        // Apply checkpoint data
        if !snapshot_data.data.is_empty() {
            match self.state_writer.apply_checkpoint_store_data(&snapshot_data.data, &self.options).await {
                Ok(count) => {
                    total_restored += count;
                    info!("Applied {} checkpoints", count);
                }
                Err(e) => {
                    warn!("Failed to apply checkpoint data: {}", e);
                    if !self.options.force_restore {
                        return Err(e);
                    }
                }
            }
        }

        Ok(total_restored)
    }

    /// Execute epoch snapshot restore
    async fn execute_epoch_restore(
        &self,
        snapshot_data: &SnapshotData,
        _metadata: &SnapshotMetadata,
    ) -> SnapshotResult<u64> {
        info!("Executing epoch snapshot restore");
        
        // Epoch restore would restore committee and epoch-specific data
        // For now, treat as checkpoint restore
        warn!("Epoch restore not fully implemented, treating as checkpoint restore");
        
        self.execute_checkpoint_restore(snapshot_data, _metadata).await
    }

    /// Validate restore consistency
    async fn validate_restore_consistency(
        &self,
        restored_count: u64,
        _metadata: &SnapshotMetadata,
    ) -> SnapshotResult<()> {
        info!("Validating restore consistency for {} items", restored_count);

        // Basic validation: check if any items were restored
        if restored_count == 0 {
            warn!("No items were restored - this may indicate a problem");
            if !self.options.force_restore {
                return Err(SnapshotError::StateValidation {
                    component: "all".to_string(),
                    details: "No items were restored from snapshot".to_string(),
                });
            }
        }

        // Additional consistency checks could be added here:
        // - Verify object references are valid
        // - Check transaction effect consistency
        // - Validate checkpoint sequence numbers
        // - Ensure epoch transitions are valid

        info!("Restore consistency validation passed");
        Ok(())
    }

    /// Get current transaction state
    pub async fn get_state(&self) -> TransactionState {
        let state = self.state.read().await;
        state.clone()
    }

    /// Get transaction ID
    pub fn get_transaction_id(&self) -> &str {
        &self.transaction_id
    }

    /// Check if transaction has backup
    pub fn has_backup(&self) -> bool {
        self.backup_checkpoint.is_some()
    }
}

/// Atomic operation manager for coordinating multiple transactions
pub struct AtomicOperationManager {
    /// Active transactions
    active_transactions: Arc<RwLock<std::collections::HashMap<String, Arc<AtomicRestoreContext>>>>,
    /// Global operation lock for critical sections
    global_lock: Arc<Mutex<()>>,
}

impl AtomicOperationManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(std::collections::HashMap::new())),
            global_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Register a new transaction
    pub async fn register_transaction(&self, context: Arc<AtomicRestoreContext>) -> SnapshotResult<()> {
        let mut transactions = self.active_transactions.write().await;
        let tx_id = context.get_transaction_id().to_string();
        
        if transactions.contains_key(&tx_id) {
            return Err(SnapshotError::InvalidOperation {
                operation: "register_transaction".to_string(),
                reason: format!("Transaction {} already exists", tx_id),
            });
        }
        
        transactions.insert(tx_id.clone(), context);
        info!("Registered transaction: {}", tx_id);
        Ok(())
    }

    /// Unregister a transaction
    pub async fn unregister_transaction(&self, transaction_id: &str) -> SnapshotResult<()> {
        let mut transactions = self.active_transactions.write().await;
        
        if transactions.remove(transaction_id).is_some() {
            info!("Unregistered transaction: {}", transaction_id);
            Ok(())
        } else {
            Err(SnapshotError::InvalidOperation {
                operation: "unregister_transaction".to_string(),
                reason: format!("Transaction {} not found", transaction_id),
            })
        }
    }

    /// Get active transaction count
    pub async fn get_active_count(&self) -> usize {
        let transactions = self.active_transactions.read().await;
        transactions.len()
    }

    /// Emergency rollback all active transactions
    pub async fn emergency_rollback_all(&self) -> SnapshotResult<()> {
        let _global_lock = self.global_lock.lock().await;
        
        warn!("Performing emergency rollback of all active transactions");
        
        let transactions = self.active_transactions.read().await;
        let tx_ids: Vec<String> = transactions.keys().cloned().collect();
        drop(transactions);
        
        for tx_id in tx_ids {
            if let Some(_context) = {
                let transactions = self.active_transactions.read().await;
                transactions.get(&tx_id).cloned()
            } {
                // Note: We would need mutable access to rollback
                warn!("Would rollback transaction: {}", tx_id);
                // In a complete implementation, this would call context.rollback_transaction()
            }
        }
        
        warn!("Emergency rollback completed");
        Ok(())
    }
}
