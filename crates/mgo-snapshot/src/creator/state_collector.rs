//! State collection functionality
//! 
//! This module implements state collection from various blockchain stores.

use crate::types::{
    SnapshotType, ComponentType,
    config::SnapshotConfig,
    error::SnapshotResult,
};
use crate::creator::CollectedStateData;

use anyhow::Result;
use fastcrypto::hash::MultisetHash;
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::epoch::committee_store::CommitteeStore;
use mgo_types::accumulator::Accumulator;
use mgo_types::base_types::TransactionDigest;
use mgo_types::committee::Committee;
use mgo_types::message_envelope::Message;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;
use mgo_types::storage::ObjectKey;
use mgo_types::digests::{TransactionEventsDigest, TransactionEffectsDigest};
use serde::{Serialize, Deserialize};

use std::ops::Deref;
use std::sync::Arc;
use tracing::{info, debug, warn, instrument};

use crate::core_integration::{
    EnhancedDatabaseAccessor, EnhancedObjectIterator, TransactionIterator
};


/// Authority state snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityStateSnapshot {
    pub objects: Vec<ObjectEntry>,
    pub transactions: Vec<TransactionEntry>,
    pub effects: Vec<EffectsEntry>,
    pub events: Vec<EventsEntry>,
}

impl AuthorityStateSnapshot {
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            transactions: Vec::new(),
            effects: Vec::new(),
            events: Vec::new(),
        }
    }
}

/// Object entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub object_id: mgo_types::base_types::ObjectID,
    pub version: mgo_types::base_types::VersionNumber,
    pub object_data: Vec<u8>, // Serialized object
}

/// Transaction entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEntry {
    pub digest: TransactionDigest,
    pub transaction_data: Vec<u8>, // Serialized transaction
}

/// Effects entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectsEntry {
    pub digest: TransactionEffectsDigest,
    pub transaction_digest: TransactionDigest,
    pub effects_data: Vec<u8>, // Serialized effects
}

/// Events entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsEntry {
    pub digest: TransactionEventsDigest,
    pub events_data: Vec<u8>, // Serialized events
}

/// Checkpoint store snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStoreSnapshot {
    pub checkpoints: Vec<CheckpointEntry>,
    pub latest_checkpoint_sequence: Option<CheckpointSequenceNumber>,
    pub highest_verified_checkpoint: Option<CheckpointSequenceNumber>,
    pub highest_synced_checkpoint: Option<CheckpointSequenceNumber>,
}

impl CheckpointStoreSnapshot {
    pub fn new() -> Self {
        Self {
            checkpoints: Vec::new(),
            latest_checkpoint_sequence: None,
            highest_verified_checkpoint: None,
            highest_synced_checkpoint: None,
        }
    }
}

/// Checkpoint entry in checkpoint store snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub sequence_number: CheckpointSequenceNumber,
    pub checkpoint_digest: String,
    pub checkpoint_summary: Vec<u8>, // Serialized checkpoint summary
}

/// Committee store snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeStoreSnapshot {
    pub committees: Vec<CommitteeEntry>,
    pub latest_epoch: Option<u64>,
}

impl CommitteeStoreSnapshot {
    pub fn new() -> Self {
        Self {
            committees: Vec::new(),
            latest_epoch: None,
        }
    }
}

/// Committee entry in committee store snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeEntry {
    pub epoch: u64,
    pub committee: Committee,
}

/// Object store snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreSnapshot {
    pub objects: Vec<ObjectEntry>,
    pub total_count: u64,
}

impl ObjectStoreSnapshot {
    pub fn new() -> Self {
        Self {
            objects: Vec::new(),
            total_count: 0,
        }
    }
}

/// Transaction store snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStoreSnapshot {
    pub transactions: Vec<TransactionEntry>,
    pub effects: Vec<EffectsEntry>,
    pub events: Vec<EventsEntry>,
    pub total_count: u64,
}

impl TransactionStoreSnapshot {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
            effects: Vec::new(),
            events: Vec::new(),
            total_count: 0,
        }
    }
}

/// State collector for gathering blockchain data
/// 
/// Responsible for collecting state from various blockchain stores according
/// to the requested snapshot type and components.
pub struct StateCollector {
    /// Configuration for state collection
    config: SnapshotConfig,
    /// Enhanced database accessor for improved data access
    enhanced_accessor: Option<Arc<EnhancedDatabaseAccessor>>,
}

impl StateCollector {
    /// Create a new StateCollector
    pub fn new(config: SnapshotConfig) -> SnapshotResult<Self> {
        Ok(Self { 
            config,
            enhanced_accessor: None,
        })
    }

    /// Create a new StateCollector with enhanced data accessor
    pub fn with_enhanced_accessor(
        config: SnapshotConfig,
        enhanced_accessor: Arc<EnhancedDatabaseAccessor>,
    ) -> SnapshotResult<Self> {
        Ok(Self { 
            config,
            enhanced_accessor: Some(enhanced_accessor),
        })
    }
    
    /// Collect state data from blockchain stores
    #[instrument(level = "info", skip(self, perpetual_db, checkpoint_store, committee_store))]
    pub async fn collect_state(
        &self,
        snapshot_type: &SnapshotType,
        epoch: u64,
        components: &[ComponentType],
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> Result<CollectedStateData> {
        info!(
            snapshot_type = ?snapshot_type,
            epoch = epoch,
            components = ?components,
            "Starting state collection"
        );
        
        // Collect state based on snapshot type and requested components
        let collected = match snapshot_type {
            SnapshotType::Full { .. } => {
                // For full snapshots, collect all available data
                self.collect_full_state(
                    epoch,
                    components,
                    perpetual_db,
                    checkpoint_store,
                    committee_store,
                ).await?
            }
            
            SnapshotType::Incremental { base_snapshot, .. } => {
                // For incremental snapshots, collect only changed data
                self.collect_incremental_state(
                    epoch,
                    base_snapshot,
                    components,
                    perpetual_db,
                    checkpoint_store,
                    committee_store,
                ).await?
            }
            
            SnapshotType::Checkpoint { checkpoint_seq, .. } => {
                // For checkpoint snapshots, collect state at specific checkpoint
                self.collect_checkpoint_state(
                    *checkpoint_seq,
                    components,
                    perpetual_db,
                    checkpoint_store,
                    committee_store,
                ).await?
            }
            
            SnapshotType::Epoch { epoch: target_epoch, .. } => {
                // For epoch snapshots, collect end-of-epoch state
                self.collect_epoch_state(
                    *target_epoch,
                    components,
                    perpetual_db,
                    checkpoint_store,
                    committee_store,
                ).await?
            }
        };
        
        debug!(
            total_size = collected.total_size(),
            object_count = collected.object_count(),
            "State collection completed"
        );
        
        Ok(collected)
    }
    
    /// Collect full state from all stores
    async fn collect_full_state(
        &self,
        epoch: u64,
        components: &[ComponentType],
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> Result<CollectedStateData> {
        let mut collected = CollectedStateData {
            authority_state: None,
            epoch_store: None,
            checkpoint_store: None,
            object_store: None,
            transaction_store: None,
            index_store: None,
            consensus_state: None,
            accumulator: None,
        };
        
        for component in components {
            match component {
                ComponentType::AuthorityState => {
                    debug!("Collecting authority state data");
                    collected.authority_state = Some(self.collect_authority_state(&perpetual_db).await?);
                }
                
                ComponentType::EpochStore => {
                    debug!("Collecting epoch store data");
                    collected.epoch_store = Some(self.collect_epoch_store_data(epoch, &committee_store).await?);
                }
                
                ComponentType::CheckpointStore => {
                    debug!("Collecting checkpoint store data");
                    collected.checkpoint_store = Some(self.collect_checkpoint_store_data(&checkpoint_store).await?);
                }
                
                ComponentType::ObjectStore => {
                    debug!("Collecting object store data");
                    collected.object_store = Some(self.collect_object_store_data(&perpetual_db).await?);
                }
                
                ComponentType::TransactionStore => {
                    debug!("Collecting transaction store data");
                    collected.transaction_store = Some(self.collect_transaction_store_data(&perpetual_db).await?);
                }
                
                ComponentType::IndexStore => {
                    debug!("Collecting index store data");
                    collected.index_store = Some(self.collect_index_store_data(&perpetual_db).await?);
                }
                
                ComponentType::ConsensusState => {
                    debug!("Collecting consensus state data");
                    collected.consensus_state = Some(self.collect_consensus_state_data().await?);
                }
            }
        }
        
        // Create accumulator for the collected state
        collected.accumulator = Some(self.create_state_accumulator(&collected).await?);
        
        Ok(collected)
    }
    
    /// Collect incremental state (changes since base snapshot)
    async fn collect_incremental_state(
        &self,
        epoch: u64,
        _base_snapshot: &crate::types::SnapshotId,
        components: &[ComponentType],
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> Result<CollectedStateData> {
        // TODO: Implement incremental collection logic
        // For now, fall back to full collection
        warn!("Incremental state collection not yet implemented, falling back to full collection");
        self.collect_full_state(epoch, components, perpetual_db, checkpoint_store, committee_store).await
    }
    
    /// Collect state at specific checkpoint
    async fn collect_checkpoint_state(
        &self,
        checkpoint_seq: u64,
        components: &[ComponentType],
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> Result<CollectedStateData> {
        info!(checkpoint_seq = checkpoint_seq, "Collecting checkpoint state");
        
        // Get checkpoint information
        let checkpoint = checkpoint_store
            .get_checkpoint_by_sequence_number(checkpoint_seq)?
            .ok_or_else(|| anyhow::anyhow!("Checkpoint {} not found", checkpoint_seq))?;
        
        let epoch = checkpoint.epoch();
        
        // Collect state for the checkpoint epoch
        self.collect_full_state(epoch, components, perpetual_db, checkpoint_store, committee_store).await
    }
    
    /// Collect end-of-epoch state
    async fn collect_epoch_state(
        &self,
        epoch: u64,
        components: &[ComponentType],
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        committee_store: Arc<CommitteeStore>,
    ) -> Result<CollectedStateData> {
        info!(epoch = epoch, "Collecting epoch state");
        
        // For epoch snapshots, we want the state at the end of the epoch
        self.collect_full_state(epoch, components, perpetual_db, checkpoint_store, committee_store).await
    }
    
    /// Collect authority state data
    async fn collect_authority_state(&self, perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting authority state from perpetual DB");
        
        let mut authority_state = AuthorityStateSnapshot::new();
        
        // Collect a limited set of recent objects and transactions for authority state
        let max_objects = self.config.performance.max_objects_per_snapshot.map(|n| n / 2).unwrap_or(10000);
        let _max_transactions = self.config.performance.max_transactions_per_snapshot.map(|n| n / 2).unwrap_or(5000);
        
        let mut object_count = 0;
        let mut _tx_count = 0;
        
        // Collect recent objects
        for live_object in perpetual_db.iter_live_object_set(false) {
            if object_count >= max_objects {
                break;
            }
            
            let object_ref = live_object.object_reference();
            
            match live_object {
                mgo_core::authority::authority_store_tables::LiveObject::Normal(object) => {
                    let object_data = bcs::to_bytes(&object)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize object {}: {}", object_ref.0, e))?;
                    
                    authority_state.objects.push(ObjectEntry {
                        object_id: object_ref.0,
                        version: object_ref.1,
                        object_data,
                    });
                    
                    object_count += 1;
                }
                mgo_core::authority::authority_store_tables::LiveObject::Wrapped(_) => {
                    // Skip wrapped objects for now
                    continue;
                }
            }
        }
        
        // Collect recent transactions
        // TODO: Transaction iteration requires access to private fields
        // For now, we'll collect a limited number of transactions differently
        warn!("Transaction collection not yet implemented due to API limitations");
        
        // Transaction collection temporarily disabled due to API limitations
        // tx_count remains 0
        
        info!(
            "Authority state collection complete: {} objects, {} transactions, {} effects, {} events",
            authority_state.objects.len(),
            authority_state.transactions.len(), 
            authority_state.effects.len(),
            authority_state.events.len()
        );
        
        // Serialize the collected state using BCS
        let serialized = bcs::to_bytes(&authority_state)
            .map_err(|e| anyhow::anyhow!("Failed to serialize authority state: {}", e))?;
            
        Ok(serialized)
    }
    
    /// Collect epoch store data
    async fn collect_epoch_store_data(&self, epoch: u64, committee_store: &CommitteeStore) -> Result<Vec<u8>> {
        debug!(epoch = epoch, "Collecting epoch store data");
        
        let mut committee_snapshot = CommitteeStoreSnapshot::new();
        committee_snapshot.latest_epoch = Some(epoch);
        
        // Collect committee information from committee store
        info!("Collecting committee information for epoch {}", epoch);
        
        // Get the committee for this epoch
        if let Ok(Some(committee)) = committee_store.get_committee(&epoch) {
            committee_snapshot.committees.push(CommitteeEntry {
                epoch,
                committee: committee.deref().clone(),
            });
            debug!("Collected committee for epoch {}", epoch);
        } else {
            warn!("No committee found for epoch {}", epoch);
        }
        
        // Collect committees for recent epochs (last 5 epochs for recovery purposes)
        for past_epoch in epoch.saturating_sub(4)..epoch {
            if let Ok(Some(committee)) = committee_store.get_committee(&past_epoch) {
                committee_snapshot.committees.push(CommitteeEntry {
                    epoch: past_epoch,
                    committee: committee.deref().clone(),
                });
                debug!("Collected committee for past epoch {}", past_epoch);
            }
        }
        
        info!("Committee store collection complete: {} committees", committee_snapshot.committees.len());
        
        // Serialize the collected committee store data
        let serialized = bcs::to_bytes(&committee_snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize committee store: {}", e))?;
            
        Ok(serialized)
    }
    
    /// Collect checkpoint store data
    async fn collect_checkpoint_store_data(&self, checkpoint_store: &CheckpointStore) -> Result<Vec<u8>> {
        debug!("Collecting checkpoint store data");
        
        let mut checkpoint_snapshot = CheckpointStoreSnapshot::new();
        
        // Get current checkpoint information
        if let Some(latest_checkpoint) = checkpoint_store.get_highest_executed_checkpoint_seq_number()? {
            checkpoint_snapshot.latest_checkpoint_sequence = Some(latest_checkpoint);
            info!("Latest checkpoint sequence: {}", latest_checkpoint);
        }
        
        if let Some(highest_verified_checkpoint) = checkpoint_store.get_highest_verified_checkpoint()? {
            let seq_num = highest_verified_checkpoint.sequence_number;
            checkpoint_snapshot.highest_verified_checkpoint = Some(seq_num);
            info!("Highest verified checkpoint: {}", seq_num);
        }
        
        if let Some(highest_synced_checkpoint) = checkpoint_store.get_highest_synced_checkpoint()? {
            let seq_num = highest_synced_checkpoint.sequence_number;
            checkpoint_snapshot.highest_synced_checkpoint = Some(seq_num);
            info!("Highest synced checkpoint: {}", seq_num);
        }
        
        // Collect checkpoint data with limits to avoid memory issues
        let max_checkpoints = self.config.performance.max_checkpoints_per_snapshot.unwrap_or(10000);
        let start_seq = checkpoint_snapshot.latest_checkpoint_sequence
            .unwrap_or(0)
            .saturating_sub(max_checkpoints as u64);
        
        info!("Collecting checkpoints from sequence {} to latest", start_seq);
        
        if let Some(end_seq) = checkpoint_snapshot.latest_checkpoint_sequence {
            let mut collected_count = 0;
            for seq in start_seq..=end_seq {
                if collected_count >= max_checkpoints {
                    warn!("Reached maximum checkpoints limit, stopping collection");
                    break;
                }
                
                if let Ok(Some(checkpoint)) = checkpoint_store.get_checkpoint_by_sequence_number(seq) {
                    // Convert checkpoint to serializable format
                    let checkpoint_digest = checkpoint.digest().to_string();
                    let checkpoint_summary = bcs::to_bytes(checkpoint.data())
                        .map_err(|e| anyhow::anyhow!("Failed to serialize checkpoint summary: {}", e))?;
                        
                    checkpoint_snapshot.checkpoints.push(CheckpointEntry {
                        sequence_number: seq,
                        checkpoint_digest,
                        checkpoint_summary,
                    });
                    collected_count += 1;
                    
                    if collected_count % 1000 == 0 {
                        debug!("Collected {} checkpoints", collected_count);
                    }
                }
            }
            
            info!("Checkpoint store collection complete: {} checkpoints", collected_count);
        }
        
        // Serialize the collected checkpoint store data
        let serialized = bcs::to_bytes(&checkpoint_snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize checkpoint store: {}", e))?;
            
        Ok(serialized)
    }
    
    /// Collect object store data
    async fn collect_object_store_data(&self, perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting object store data");
        
        let mut object_snapshot = ObjectStoreSnapshot::new();
        let max_objects = self.config.performance.max_objects_per_snapshot.unwrap_or(100000);
        let mut collected_count = 0;
        
        // Use enhanced accessor if available for better performance
        if let Some(ref enhanced_accessor) = self.enhanced_accessor {
            info!("Using enhanced data accessor for object collection");
            
            let object_iter = EnhancedObjectIterator::new(enhanced_accessor.as_ref(), false);
            
            for result in object_iter {
                if collected_count >= max_objects {
                    warn!("Reached maximum objects limit ({}), stopping collection", max_objects);
                    break;
                }
                
                match result {
                    Ok((object_id, object)) => {
                        let object_data = bcs::to_bytes(&object)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize object {}: {}", object_id, e))?;
                        
                        object_snapshot.objects.push(ObjectEntry {
                            object_id,
                            version: object.version(),
                            object_data,
                        });
                        
                        collected_count += 1;
                        
                        if collected_count % 10000 == 0 {
                            debug!("Collected {} objects", collected_count);
                        }
                    }
                    Err(e) => {
                        warn!("Error collecting object: {}", e);
                        continue;
                    }
                }
            }
        } else {
            // Fall back to direct access
            info!("Using direct perpetual_db access for object collection");
            
            for live_object in perpetual_db.iter_live_object_set(false) {
                if collected_count >= max_objects {
                    warn!("Reached maximum objects limit ({}), stopping collection", max_objects);
                    break;
                }
                
                let object_ref = live_object.object_reference();
                let object_key = ObjectKey(object_ref.0, object_ref.1);
                
                // Convert live object to storable format
                match live_object {
                    mgo_core::authority::authority_store_tables::LiveObject::Normal(object) => {
                        // Serialize the object
                        let object_data = bcs::to_bytes(&object)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize object {}: {}", object_key.0, e))?;
                        
                        object_snapshot.objects.push(ObjectEntry {
                            object_id: object_key.0,
                            version: object_key.1,
                            object_data,
                        });
                        
                        collected_count += 1;
                        
                        if collected_count % 10000 == 0 {
                            debug!("Collected {} objects", collected_count);
                        }
                    }
                    mgo_core::authority::authority_store_tables::LiveObject::Wrapped(_) => {
                        // Skip wrapped objects for now
                        continue;
                    }
                }
            }
        }
        
        object_snapshot.total_count = collected_count as u64;
        
        info!("Object store collection complete: {} objects", collected_count);
        
        // Serialize the collected object store data
        let serialized = bcs::to_bytes(&object_snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize object store: {}", e))?;
            
        Ok(serialized)
    }
    
    /// Collect transaction store data
    async fn collect_transaction_store_data(&self, perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting transaction store data");
        
        let mut transaction_snapshot = TransactionStoreSnapshot::new();
        
        // Collect transactions with pagination to avoid memory issues
        let max_transactions = self.config.performance.max_transactions_per_snapshot.unwrap_or(50000);
        let mut collected_count = 0;
        
        // Use enhanced accessor if available for transaction collection
        if let Some(ref enhanced_accessor) = self.enhanced_accessor {
            info!("Using enhanced data accessor for transaction collection");
            
            // Get current database statistics to determine checkpoint range
            let stats = enhanced_accessor.get_database_statistics()
                .map_err(|e| anyhow::anyhow!("Failed to get database statistics: {}", e))?;
            
            if stats.latest_checkpoint > 0 {
                // Collect transactions from recent checkpoints
                let start_checkpoint = if stats.latest_checkpoint > 100 {
                    stats.latest_checkpoint - 100 // Last 100 checkpoints
                } else {
                    0
                };
                
                let transaction_iter = TransactionIterator::new(
                    enhanced_accessor.clone(),
                    start_checkpoint,
                    stats.latest_checkpoint,
                    1000 // Batch size
                );
                
                for result in transaction_iter {
                    if collected_count >= max_transactions {
                        warn!("Reached maximum transactions limit ({}), stopping collection", max_transactions);
                        break;
                    }
                    
                    match result {
                        Ok(tx_with_effects) => {
                            // Serialize transaction data
                            let transaction_data = bcs::to_bytes(&tx_with_effects.transaction)
                                .map_err(|e| anyhow::anyhow!("Failed to serialize transaction {}: {}", tx_with_effects.transaction.inner().digest(), e))?;
                            
                            transaction_snapshot.transactions.push(TransactionEntry {
                                digest: *tx_with_effects.transaction.inner().digest(),
                                transaction_data,
                            });
                            
                            // Include effects if available
                            if let Some(effects) = tx_with_effects.effects {
                                let effects_data = bcs::to_bytes(&effects)
                                    .map_err(|e| anyhow::anyhow!("Failed to serialize effects {}: {}", effects.digest(), e))?;
                                
                                transaction_snapshot.effects.push(EffectsEntry {
                                    digest: effects.digest(),
                                    transaction_digest: *tx_with_effects.transaction.inner().digest(),
                                    effects_data,
                                });
                            }
                            
                            collected_count += 1;
                            
                            if collected_count % 1000 == 0 {
                                debug!("Collected {} transactions", collected_count);
                            }
                        }
                        Err(e) => {
                            warn!("Error collecting transaction: {}", e);
                            continue;
                        }
                    }
                }
            } else {
                info!("No checkpoints found, skipping transaction collection");
            }
        } else {
            // Fall back to placeholder implementation
            warn!("Transaction store collection not yet implemented due to API limitations");
            let _transactions_iter: Vec<(mgo_types::base_types::TransactionDigest, ())> = vec![];
        }
        
        for (tx_digest, _tx_key) in vec![] as Vec<(TransactionDigest, ())> {
            if collected_count >= max_transactions {
                warn!("Reached maximum transactions limit ({}), stopping collection", max_transactions);
                break;
            }
            
            // Get transaction data
            if let Ok(Some(trusted_tx)) = perpetual_db.get_transaction(&tx_digest) {
                let transaction = trusted_tx.into_inner();
                let transaction_data = bcs::to_bytes(&transaction)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize transaction {}: {}", tx_digest, e))?;
                
                transaction_snapshot.transactions.push(TransactionEntry {
                    digest: tx_digest,
                    transaction_data,
                });
                
                // Get corresponding effects
                if let Ok(Some(effects)) = perpetual_db.get_effects(&tx_digest) {
                    let effects_data = bcs::to_bytes(&effects)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize effects for {}: {}", tx_digest, e))?;
                    
                    transaction_snapshot.effects.push(EffectsEntry {
                        digest: effects.digest(),
                        transaction_digest: tx_digest,
                        effects_data,
                    });
                    
                    // TODO: Events access not yet available due to API limitations
                    // if let Some(events_digest) = effects.events_digest() {
                    //     if let Ok(Some(events)) = perpetual_db.get_events(events_digest) {
                    //         let events_data = bcs::to_bytes(&events)?;
                    //         transaction_snapshot.events.push(EventsEntry {
                    //             digest: *events_digest,
                    //             events_data,
                    //         });
                    //     }
                    // }
                }
                
                collected_count += 1;
                
                if collected_count % 5000 == 0 {
                    debug!("Collected {} transactions", collected_count);
                }
            }
        }
        
        transaction_snapshot.total_count = collected_count as u64;
        
        info!("Transaction store collection complete: {} transactions, {} effects, {} events", 
              transaction_snapshot.transactions.len(),
              transaction_snapshot.effects.len(),
              transaction_snapshot.events.len());
        
        // Serialize the collected transaction store data
        let serialized = bcs::to_bytes(&transaction_snapshot)
            .map_err(|e| anyhow::anyhow!("Failed to serialize transaction store: {}", e))?;
            
        Ok(serialized)
    }

    /// Efficient transaction collection using checkpoint iterator
    async fn collect_transactions_in_checkpoint_range(
        &self,
        start_checkpoint: u64,
        end_checkpoint: u64,
        max_transactions: usize,
    ) -> Result<TransactionStoreSnapshot> {
        let mut transaction_snapshot = TransactionStoreSnapshot::new();
        
        if let Some(ref enhanced_accessor) = self.enhanced_accessor {
            info!("Collecting transactions from checkpoints {} to {} using new APIs", start_checkpoint, end_checkpoint);
            
            // Use the new efficient API to get transactions with effects
            match enhanced_accessor.get_transactions_in_checkpoint_range(
                start_checkpoint,
                end_checkpoint,
                true, // include effects
            ).await {
                Ok(transactions) => {
                    let mut collected_count = 0;
                    
                    for tx_with_effects in transactions {
                        if collected_count >= max_transactions {
                            warn!("Reached maximum transactions limit ({}), stopping collection", max_transactions);
                            break;
                        }
                        
                        // Serialize transaction
                        let transaction_data = bcs::to_bytes(&tx_with_effects.transaction)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize transaction {}: {}", tx_with_effects.transaction.inner().digest(), e))?;
                        
                        transaction_snapshot.transactions.push(TransactionEntry {
                            digest: *tx_with_effects.transaction.inner().digest(),
                            transaction_data,
                        });
                        
                        // Include effects if available
                        if let Some(effects) = tx_with_effects.effects {
                            let effects_data = bcs::to_bytes(&effects)
                                .map_err(|e| anyhow::anyhow!("Failed to serialize effects {}: {}", effects.digest(), e))?;
                            
                            transaction_snapshot.effects.push(EffectsEntry {
                                digest: effects.digest(),
                                transaction_digest: *tx_with_effects.transaction.inner().digest(),
                                effects_data,
                            });
                        }
                        
                        collected_count += 1;
                        
                        if collected_count % 1000 == 0 {
                            debug!("Collected {} transactions from checkpoint range", collected_count);
                        }
                    }
                    
                    info!("Successfully collected {} transactions from checkpoint range", collected_count);
                }
                Err(e) => {
                    warn!("Failed to collect transactions from checkpoint range: {}", e);
                }
            }
        }
        
        Ok(transaction_snapshot)
    }
    
    /// Collect index store data
    async fn collect_index_store_data(&self, _perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting index store data");
        
        // TODO: Implement actual index store serialization
        // This would involve collecting:
        // - Various indexes
        // - Lookup tables
        // - Cached data
        
        Ok(b"index_store_placeholder".to_vec())
    }
    
    /// Collect consensus state data
    async fn collect_consensus_state_data(&self) -> Result<Vec<u8>> {
        debug!("Collecting consensus state data");
        
        // TODO: Implement actual consensus state serialization
        // This would involve collecting:
        // - Consensus protocol state
        // - Validator information
        // - Round data
        
        Ok(b"consensus_state_placeholder".to_vec())
    }
    
    /// Create accumulator for the collected state
    async fn create_state_accumulator(&self, collected: &CollectedStateData) -> Result<Accumulator> {
        debug!("Creating state accumulator");
        
        let mut accumulator = Accumulator::default();
        
        // Add state components to accumulator
        if let Some(ref data) = collected.authority_state {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.epoch_store {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.checkpoint_store {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.object_store {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.transaction_store {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.index_store {
            accumulator.insert(data);
        }
        
        if let Some(ref data) = collected.consensus_state {
            accumulator.insert(data);
        }
        
        info!(
            digest = ?accumulator.digest(),
            "State accumulator created"
        );
        
        Ok(accumulator)
    }
}
