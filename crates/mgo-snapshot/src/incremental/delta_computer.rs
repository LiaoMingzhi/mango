// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Delta computation for incremental snapshots

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

use mgo_types::base_types::EpochId;

use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

use mgo_types::storage::ObjectKey;


use crate::core_integration::{
    DatabaseAccessor, 

    CheckpointStoreSnapshot, 
    CommitteeStoreSnapshot, 
    ObjectStoreSnapshot,
    TransactionStoreSnapshot,
    ObjectEntry,
    TransactionEntry,
    EffectsEntry,
    EventsEntry,
};
use crate::types::{SnapshotId, SnapshotData, SnapshotMetadata};
use crate::types::error::SnapshotError;

/// Computes deltas between snapshots for incremental snapshots
pub struct DeltaComputer {
    db_accessor: Arc<DatabaseAccessor>,
}

impl DeltaComputer {
    pub fn new(db_accessor: Arc<DatabaseAccessor>) -> Self {
        Self { db_accessor }
    }

    /// Compute delta between current state and base snapshot
    #[instrument(level = "info", skip(self, base_snapshot_data))]
    pub async fn compute_delta(
        &self,
        base_snapshot_id: SnapshotId,
        base_snapshot_data: &SnapshotData,
        base_metadata: &SnapshotMetadata,
        _target_checkpoint: Option<CheckpointSequenceNumber>,
        _target_epoch: Option<EpochId>,
    ) -> Result<DeltaData, SnapshotError> {
        info!("Computing delta from base snapshot {}", base_snapshot_id);

        let mut delta = DeltaData::new(base_snapshot_id);

        // Compute deltas for each component type
        delta.object_delta = self.compute_object_delta(base_snapshot_data, _target_checkpoint).await?;
        delta.transaction_delta = self.compute_transaction_delta(base_snapshot_data, _target_checkpoint).await?;
        delta.checkpoint_delta = self.compute_checkpoint_delta(base_snapshot_data, _target_checkpoint).await?;
        delta.committee_delta = self.compute_committee_delta(base_snapshot_data, _target_epoch).await?;

        // Set delta metadata
        delta.target_checkpoint = _target_checkpoint.unwrap_or(base_metadata.checkpoint_seq.unwrap_or(0));
        delta.target_epoch = _target_epoch.unwrap_or(base_metadata.epoch);
        delta.base_checkpoint = base_metadata.checkpoint_seq.unwrap_or(0);
        delta.base_epoch = base_metadata.epoch;

        info!("Delta computation completed");
        Ok(delta)
    }

    /// Compute object store delta
    async fn compute_object_delta(
        &self,
        base_snapshot_data: &SnapshotData,
        _target_checkpoint: Option<CheckpointSequenceNumber>,
    ) -> Result<ObjectDelta, SnapshotError> {
        debug!("Computing object delta");

        let mut delta = ObjectDelta::new();

        // Get base object state - try to deserialize as ObjectStoreSnapshot
        let base_objects = if let Ok(base_snapshot) = bcs::from_bytes::<ObjectStoreSnapshot>(&base_snapshot_data.data) {
            base_snapshot.objects.into_iter().map(|obj| (obj.key, obj)).collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };

        // Collect current objects (simplified - would need proper iteration)
        let mut current_objects = HashMap::new();
        let mut iterator = crate::core_integration::ObjectIterator::new(&self.db_accessor, 1000);
        
        while !iterator.is_exhausted() {
            let batch = iterator.next_batch()?;
            for (key, object) in batch {
                let entry = ObjectEntry {
                    key,
                    object_id: object.id(),
                    version: object.version(),
                    object_data: bcs::to_bytes(&object)?,
                };
                current_objects.insert(key, entry);
            }
        }

        // Compute differences
        for (key, current_obj) in &current_objects {
            match base_objects.get(key) {
                Some(base_obj) => {
                    // Check if object has changed
                    if current_obj.object_data != base_obj.object_data {
                        delta.modified_objects.insert(*key, current_obj.clone());
                    }
                }
                None => {
                    // New object
                    delta.new_objects.insert(*key, current_obj.clone());
                }
            }
        }

        // Find deleted objects
        for (key, base_obj) in &base_objects {
            if !current_objects.contains_key(key) {
                delta.deleted_objects.insert(*key, base_obj.clone());
            }
        }

        info!("Object delta: {} new, {} modified, {} deleted", 
              delta.new_objects.len(), delta.modified_objects.len(), delta.deleted_objects.len());

        Ok(delta)
    }

    /// Compute transaction store delta
    async fn compute_transaction_delta(
        &self,
        base_snapshot_data: &SnapshotData,
        _target_checkpoint: Option<CheckpointSequenceNumber>,
    ) -> Result<TransactionDelta, SnapshotError> {
        debug!("Computing transaction delta");

        let mut delta = TransactionDelta::new();

        // Get base transaction state - try to deserialize as TransactionStoreSnapshot  
        let _base_transactions = if let Ok(base_snapshot) = bcs::from_bytes::<TransactionStoreSnapshot>(&base_snapshot_data.data) {
            base_snapshot.transactions.into_iter().map(|tx| (tx.digest, tx)).collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };

        // For now, assume all current transactions are new
        // In practice, would need better transaction iteration APIs
        delta.new_transactions = Vec::new(); // Placeholder
        delta.new_effects = Vec::new(); // Placeholder
        delta.new_events = Vec::new(); // Placeholder

        Ok(delta)
    }

    /// Compute checkpoint store delta
    async fn compute_checkpoint_delta(
        &self,
        base_snapshot_data: &SnapshotData,
        _target_checkpoint: Option<CheckpointSequenceNumber>,
    ) -> Result<CheckpointDelta, SnapshotError> {
        debug!("Computing checkpoint delta");

        let mut delta = CheckpointDelta::new();

        // Get base checkpoint state - try to deserialize as CheckpointStoreSnapshot
        let base_highest_checkpoint = if let Ok(base_snapshot) = bcs::from_bytes::<CheckpointStoreSnapshot>(&base_snapshot_data.data) {
            base_snapshot.highest_verified_checkpoint.unwrap_or(0)
        } else {
            0
        };

        // Get current highest checkpoint
        let current_highest = self.db_accessor.get_highest_verified_checkpoint()?
            .map(|cp| *cp.sequence_number())
            .unwrap_or(0);

        // Collect new checkpoints
        for seq in (base_highest_checkpoint + 1)..=current_highest {
            if let Ok(Some(_checkpoint)) = self.db_accessor.get_checkpoint(seq) {
                delta.new_checkpoint_seqs.push(seq);
            }
        }

        info!("Checkpoint delta: {} new checkpoints", delta.new_checkpoint_seqs.len());

        Ok(delta)
    }

    /// Compute committee store delta
    async fn compute_committee_delta(
        &self,
        base_snapshot_data: &SnapshotData,
        _target_epoch: Option<EpochId>,
    ) -> Result<CommitteeDelta, SnapshotError> {
        debug!("Computing committee delta");

        let mut delta = CommitteeDelta::new();

        // Get base committee state - try to deserialize as CommitteeStoreSnapshot
        let base_latest_epoch = if let Ok(base_snapshot) = bcs::from_bytes::<CommitteeStoreSnapshot>(&base_snapshot_data.data) {
            base_snapshot.latest_epoch.unwrap_or(0)
        } else {
            0
        };

        // Get current epoch
        let current_epoch = self.db_accessor.get_current_epoch().await?;

        // Collect new committees
        for epoch in (base_latest_epoch + 1)..=current_epoch {
            if let Ok(Some(committee)) = self.db_accessor.get_committee(epoch) {
                delta.new_committees.insert(epoch, committee);
            }
        }

        info!("Committee delta: {} new committees", delta.new_committees.len());

        Ok(delta)
    }
}

/// Complete delta data representing changes from base snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaData {
    /// ID of the base snapshot
    pub base_snapshot_id: SnapshotId,
    /// Target checkpoint sequence number
    pub target_checkpoint: CheckpointSequenceNumber,
    /// Target epoch ID
    pub target_epoch: EpochId,
    /// Base checkpoint sequence number
    pub base_checkpoint: CheckpointSequenceNumber,
    /// Base epoch ID
    pub base_epoch: EpochId,
    /// Object state changes
    pub object_delta: ObjectDelta,
    /// Transaction state changes
    pub transaction_delta: TransactionDelta,
    /// Checkpoint state changes
    pub checkpoint_delta: CheckpointDelta,
    /// Committee state changes
    pub committee_delta: CommitteeDelta,
    /// Time when the delta was created
    pub creation_time: std::time::SystemTime,
}

impl DeltaData {
    /// Create a new delta data structure
    pub fn new(base_snapshot_id: SnapshotId) -> Self {
        Self {
            base_snapshot_id,
            target_checkpoint: 0,
            target_epoch: 0,
            base_checkpoint: 0,
            base_epoch: 0,
            object_delta: ObjectDelta::new(),
            transaction_delta: TransactionDelta::new(),
            checkpoint_delta: CheckpointDelta::new(),
            committee_delta: CommitteeDelta::new(),
            creation_time: std::time::SystemTime::now(),
        }
    }

    /// Get total number of changes in this delta
    pub fn total_changes(&self) -> usize {
        self.object_delta.total_changes() +
        self.transaction_delta.total_changes() +
        self.checkpoint_delta.total_changes() +
        self.committee_delta.total_changes()
    }

    /// Check if delta is empty (no changes)
    pub fn is_empty(&self) -> bool {
        self.total_changes() == 0
    }
}

/// Object store delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectDelta {
    /// Newly created objects
    pub new_objects: HashMap<ObjectKey, ObjectEntry>,
    /// Modified existing objects
    pub modified_objects: HashMap<ObjectKey, ObjectEntry>,
    /// Deleted objects
    pub deleted_objects: HashMap<ObjectKey, ObjectEntry>,
}

impl ObjectDelta {
    /// Create a new empty object delta
    pub fn new() -> Self {
        Self {
            new_objects: HashMap::new(),
            modified_objects: HashMap::new(),
            deleted_objects: HashMap::new(),
        }
    }

    /// Get the total number of changes in this delta
    pub fn total_changes(&self) -> usize {
        self.new_objects.len() + self.modified_objects.len() + self.deleted_objects.len()
    }
}

impl Default for ObjectDelta {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction store delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionDelta {
    /// Newly added transactions
    pub new_transactions: Vec<TransactionEntry>,
    /// Newly added transaction effects
    pub new_effects: Vec<EffectsEntry>,
    /// Newly added transaction events
    pub new_events: Vec<EventsEntry>,
}

impl TransactionDelta {
    /// Create a new empty transaction delta
    pub fn new() -> Self {
        Self {
            new_transactions: Vec::new(),
            new_effects: Vec::new(),
            new_events: Vec::new(),
        }
    }

    /// Get the total number of changes in this delta
    pub fn total_changes(&self) -> usize {
        self.new_transactions.len() + self.new_effects.len() + self.new_events.len()
    }
}

impl Default for TransactionDelta {
    fn default() -> Self {
        Self::new()
    }
}

/// Checkpoint store delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointDelta {
    /// Newly added checkpoint sequence numbers
    pub new_checkpoint_seqs: Vec<CheckpointSequenceNumber>,
}

impl CheckpointDelta {
    /// Create a new empty checkpoint delta
    pub fn new() -> Self {
        Self {
            new_checkpoint_seqs: Vec::new(),
        }
    }

    /// Get the total number of changes in this delta
    pub fn total_changes(&self) -> usize {
        self.new_checkpoint_seqs.len()
    }
}

impl Default for CheckpointDelta {
    fn default() -> Self {
        Self::new()
    }
}

/// Committee store delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeDelta {
    /// Newly added committees by epoch
    pub new_committees: HashMap<EpochId, mgo_types::committee::Committee>,
}

impl CommitteeDelta {
    /// Create a new empty committee delta
    pub fn new() -> Self {
        Self {
            new_committees: HashMap::new(),
        }
    }

    /// Get the total number of changes in this delta
    pub fn total_changes(&self) -> usize {
        self.new_committees.len()
    }
}

impl Default for CommitteeDelta {
    fn default() -> Self {
        Self::new()
    }
}
