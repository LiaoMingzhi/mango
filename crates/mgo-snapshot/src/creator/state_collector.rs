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
use mgo_core::authority::authority_store_types::StoreObjectWrapper;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::epoch::committee_store::CommitteeStore;
use mgo_types::accumulator::Accumulator;
use mgo_types::base_types::TransactionDigest;
use mgo_types::committee::Committee;
use mgo_types::effects::{TransactionEffects, TransactionEvents};
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;
use mgo_types::storage::ObjectKey;
use mgo_types::transaction::Transaction;
use mgo_types::digests::TransactionEventsDigest;
use serde::{Serialize, Deserialize};

use std::ops::Deref;
use std::sync::Arc;
use tracing::{info, debug, warn, instrument};


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
    pub key: ObjectKey,
    pub wrapper: StoreObjectWrapper,
}

/// Transaction entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEntry {
    pub digest: TransactionDigest,
    pub transaction: Transaction,
}

/// Effects entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectsEntry {
    pub digest: TransactionDigest,
    pub effects: TransactionEffects,
}

/// Events entry in authority state snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsEntry {
    pub digest: TransactionEventsDigest,
    pub events: TransactionEvents,
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

/// State collector for gathering blockchain data
/// 
/// Responsible for collecting state from various blockchain stores according
/// to the requested snapshot type and components.
pub struct StateCollector {
    /// Configuration for state collection
    config: SnapshotConfig,
}

impl StateCollector {
    /// Create a new StateCollector
    pub fn new(config: SnapshotConfig) -> SnapshotResult<Self> {
        Ok(Self { config })
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
    async fn collect_authority_state(&self, _perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting authority state from perpetual DB");
        
        // For now, create a simplified snapshot with limited data
        // In the future, this would be expanded to capture more comprehensive state
        let authority_state = AuthorityStateSnapshot::new();
        
        info!(
            "Authority state collection complete (simplified implementation): {} objects, {} transactions, {} effects, {} events",
            authority_state.objects.len(),
            authority_state.transactions.len(), 
            authority_state.effects.len(),
            authority_state.events.len()
        );
        
        // TODO: Implement actual data collection once we have proper access methods
        // This is a placeholder implementation that creates a valid but empty snapshot
        
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
    async fn collect_object_store_data(&self, _perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting object store data");
        
        // TODO: Implement actual object store serialization
        // This would involve collecting:
        // - Live objects
        // - Object metadata
        // - Object references
        
        Ok(b"object_store_placeholder".to_vec())
    }
    
    /// Collect transaction store data
    async fn collect_transaction_store_data(&self, _perpetual_db: &AuthorityPerpetualTables) -> Result<Vec<u8>> {
        debug!("Collecting transaction store data");
        
        // TODO: Implement actual transaction store serialization
        // This would involve collecting:
        // - Transaction data
        // - Transaction effects
        // - Transaction indexes
        
        Ok(b"transaction_store_placeholder".to_vec())
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
