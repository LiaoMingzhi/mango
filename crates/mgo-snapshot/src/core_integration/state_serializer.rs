// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Enhanced state serializer using database accessor for deep integration


use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, instrument};

use mgo_types::base_types::{EpochId, ObjectID, VersionNumber};
use mgo_types::digests::{TransactionDigest, TransactionEffectsDigest, TransactionEventsDigest};

use mgo_types::storage::ObjectKey;
use mgo_types::committee::Committee;
use mgo_types::messages_checkpoint::{CheckpointSequenceNumber, VerifiedCheckpoint};

use crate::core_integration::DatabaseAccessor;
use crate::types::{ComponentType, SnapshotType, SnapshotData, SnapshotMetadata, SnapshotId, CompressionLevel};
use crate::types::error::SnapshotError;
use crate::types::config::SnapshotConfig;

/// Full state snapshot containing all components
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FullStateSnapshot {
    authority_state: AuthorityStateSnapshot,
    checkpoint_store: CheckpointStoreSnapshot,
    committee_store: CommitteeStoreSnapshot,
    object_store: ObjectStoreSnapshot,
    transaction_store: Option<TransactionStoreSnapshot>,
}

/// Enhanced state serializer that collects data using DatabaseAccessor
pub struct EnhancedStateSerializer {
    db_accessor: Arc<DatabaseAccessor>,
    config: SnapshotConfig,
}

impl EnhancedStateSerializer {
    pub fn new(db_accessor: Arc<DatabaseAccessor>, config: SnapshotConfig) -> Self {
        Self {
            db_accessor,
            config,
        }
    }

    /// Serialize complete state based on snapshot type
    #[instrument(level = "info", skip(self))]
    pub async fn serialize_state(
        &self,
        snapshot_type: &SnapshotType,
        target_checkpoint: Option<CheckpointSequenceNumber>,
        target_epoch: Option<EpochId>,
    ) -> Result<SnapshotData, SnapshotError> {
        match snapshot_type {
            SnapshotType::Full { include_history, .. } => {
                self.serialize_full_state(*include_history, target_checkpoint, target_epoch).await
            }
            SnapshotType::Incremental { .. } => {
                // For now, treat as full snapshot - incremental logic will be added later
                self.serialize_full_state(false, target_checkpoint, target_epoch).await
            }
            SnapshotType::Checkpoint { checkpoint_seq, include_transactions } => {
                self.serialize_checkpoint_state(*checkpoint_seq, *include_transactions).await
            }
            SnapshotType::Epoch { epoch, include_committee_info } => {
                self.serialize_epoch_state(*epoch, *include_committee_info).await
            }
        }
    }

    /// Serialize complete system state
    #[instrument(level = "info", skip(self))]
    async fn serialize_full_state(
        &self,
        include_history: bool,
        target_checkpoint: Option<CheckpointSequenceNumber>,
        target_epoch: Option<EpochId>,
    ) -> Result<SnapshotData, SnapshotError> {
        info!("Starting full state serialization");

        let current_epoch = self.db_accessor.get_current_epoch().await?;
        let epoch = target_epoch.unwrap_or(current_epoch);

        // Collect all state components
        let full_state = FullStateSnapshot {
            authority_state: self.collect_authority_state_data(epoch).await?,
            checkpoint_store: self.collect_checkpoint_store_data(target_checkpoint).await?,
            committee_store: self.collect_committee_store_data(epoch).await?,
            object_store: self.collect_object_store_data(include_history).await?,
            transaction_store: if include_history {
                Some(self.collect_transaction_store_data(target_checkpoint).await?)
            } else {
                None
            },
        };

        // Create metadata
        let metadata = SnapshotMetadata::new(
            SnapshotId::new(),
            SnapshotType::Full { include_history, compression_level: CompressionLevel::Medium },
            target_checkpoint.map(|seq| seq as u64),
            epoch,
            vec![
                ComponentType::AuthorityState,
                ComponentType::CheckpointStore,
                ComponentType::EpochStore,
                ComponentType::ObjectStore,
            ],
        );

        // Serialize the full state
        let data = bcs::to_bytes(&full_state)?;
        let snapshot_data = SnapshotData::new(metadata, data);

        info!("Full state serialization completed successfully");
        Ok(snapshot_data)
    }

    /// Serialize checkpoint-specific state
    #[instrument(level = "info", skip(self))]
    async fn serialize_checkpoint_state(
        &self,
        checkpoint_seq: CheckpointSequenceNumber,
        include_transactions: bool,
    ) -> Result<SnapshotData, SnapshotError> {
        info!("Serializing checkpoint state for sequence {}", checkpoint_seq);

        // Create metadata for checkpoint snapshot
        let metadata = SnapshotMetadata::new(
            SnapshotId::new(),
            SnapshotType::Checkpoint { checkpoint_seq, include_transactions },
            Some(checkpoint_seq as u64),
            0, // epoch will be filled later
            vec![ComponentType::CheckpointStore],
        );

        // Get the specific checkpoint
        let checkpoint = self.db_accessor.get_checkpoint(checkpoint_seq)?
            .ok_or_else(|| SnapshotError::StateCollection {
                component: "checkpoint".to_string(),
                details: format!("Checkpoint {} not found", checkpoint_seq),
            })?;

        // Serialize checkpoint data
        let checkpoint_data = CheckpointSnapshotData {
            checkpoint_seq,
            sequence_number: checkpoint_seq,
            transactions: if include_transactions {
                self.collect_checkpoint_transactions(&checkpoint).await?
            } else {
                Vec::new()
            },
        };

        let data = bcs::to_bytes(&checkpoint_data)?;
        let snapshot_data = SnapshotData::new(metadata, data);

        info!("Checkpoint state serialization completed");
        Ok(snapshot_data)
    }

    /// Serialize epoch-specific state
    #[instrument(level = "info", skip(self))]
    async fn serialize_epoch_state(
        &self,
        epoch: EpochId,
        include_committee_info: bool,
    ) -> Result<SnapshotData, SnapshotError> {
        info!("Serializing epoch state for epoch {}", epoch);

        // Create metadata for epoch snapshot
        let metadata = SnapshotMetadata::new(
            SnapshotId::new(),
            SnapshotType::Epoch { epoch, include_committee_info },
            None,
            epoch,
            vec![ComponentType::EpochStore],
        );

        // Collect epoch-specific data
        let mut epoch_data = EpochSnapshotData {
            epoch,
            committee: None,
            epoch_start_checkpoint: None,
            epoch_end_checkpoint: None,
        };

        if include_committee_info {
            epoch_data.committee = self.db_accessor.get_committee(epoch)?;
        }

        let data = bcs::to_bytes(&epoch_data)?;
        let snapshot_data = SnapshotData::new(metadata, data);

        info!("Epoch state serialization completed");
        Ok(snapshot_data)
    }

    /// Collect authority state data using available public methods
    async fn collect_authority_state_data(&self, epoch: EpochId) -> Result<AuthorityStateSnapshot, SnapshotError> {
        let stats = self.db_accessor.get_database_stats()?;
        
        Ok(AuthorityStateSnapshot {
            epoch,
            database_stats: stats,
            // Add more fields as needed when they become accessible
        })
    }

    /// Collect checkpoint store data
    async fn collect_checkpoint_store_data(
        &self,
        target_checkpoint: Option<CheckpointSequenceNumber>,
    ) -> Result<CheckpointStoreSnapshot, SnapshotError> {
        let highest_verified = self.db_accessor.get_highest_verified_checkpoint()?;
        let highest_synced = self.db_accessor.get_highest_synced_checkpoint()?;

        let _target_checkpoint_data = if let Some(seq) = target_checkpoint {
            self.db_accessor.get_checkpoint(seq)?
        } else {
            highest_verified.clone()
        };

        Ok(CheckpointStoreSnapshot {
            checkpoints: vec![], // Will be populated with actual checkpoint data
            latest_checkpoint_sequence: highest_verified.as_ref().map(|cp| *cp.sequence_number()),
            highest_verified_checkpoint: highest_verified.as_ref().map(|cp| *cp.sequence_number()),
            highest_synced_checkpoint: highest_synced.as_ref().map(|cp| *cp.sequence_number()),
            target_checkpoint_seq: target_checkpoint.map(|seq| seq as u64),
        })
    }

    /// Collect committee store data
    async fn collect_committee_store_data(&self, epoch: EpochId) -> Result<CommitteeStoreSnapshot, SnapshotError> {
        let current_committee = self.db_accessor.get_committee(epoch)?;
        
        // Collect committees for recent epochs
        let mut committees = Vec::new();
        for e in epoch.saturating_sub(2)..=epoch {
            if let Ok(Some(committee)) = self.db_accessor.get_committee(e) {
                committees.push(CommitteeEntry { epoch: e, committee });
            }
        }

        Ok(CommitteeStoreSnapshot {
            committees,
            latest_epoch: Some(epoch),
            current_committee,
        })
    }

    /// Collect object store data
    async fn collect_object_store_data(&self, _include_history: bool) -> Result<ObjectStoreSnapshot, SnapshotError> {
        let mut objects = Vec::new();
        let batch_size = self.config.performance.max_objects_per_snapshot.unwrap_or(10000);

        // Use the iterator to collect objects in batches
        let mut iterator = crate::core_integration::ObjectIterator::new(&self.db_accessor, batch_size);
        
        while !iterator.is_exhausted() {
            let batch = iterator.next_batch()?;
            for (key, object) in batch {
                objects.push(ObjectEntry {
                    key,
                    object_id: object.id(),
                    version: object.version(),
                    object_data: bcs::to_bytes(&object)?,
                });

                if objects.len() >= batch_size as usize {
                    warn!("Reached max objects limit of {}", batch_size);
                    break;
                }
            }

            if objects.len() >= batch_size as usize {
                break;
            }
        }

        let total_objects = objects.len() as u64;
        info!("Collected {} objects", total_objects);

        Ok(ObjectStoreSnapshot {
            objects,
            total_objects,
        })
    }

    /// Collect transaction store data
    async fn collect_transaction_store_data(
        &self,
        _target_checkpoint: Option<CheckpointSequenceNumber>,
    ) -> Result<TransactionStoreSnapshot, SnapshotError> {
        // For now, return empty collection due to iteration limitations
        // This would need enhanced APIs from mgo-core for efficient transaction iteration
        
        Ok(TransactionStoreSnapshot {
            transactions: Vec::new(),
            effects: Vec::new(),
            events: Vec::new(),
            total_transactions: 0,
        })
    }

    /// Collect transactions for a specific checkpoint
    async fn collect_checkpoint_transactions(
        &self,
        _checkpoint: &VerifiedCheckpoint,
    ) -> Result<Vec<TransactionDigest>, SnapshotError> {
        // Would extract transaction digests from checkpoint content
        // For now, return empty
        Ok(Vec::new())
    }
}

// Enhanced data structures for better organization

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityStateSnapshot {
    pub epoch: EpochId,
    pub database_stats: crate::core_integration::DatabaseStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointStoreSnapshot {
    pub checkpoints: Vec<CheckpointEntry>,
    pub latest_checkpoint_sequence: Option<CheckpointSequenceNumber>,
    pub highest_verified_checkpoint: Option<CheckpointSequenceNumber>,
    pub highest_synced_checkpoint: Option<CheckpointSequenceNumber>,
    pub target_checkpoint_seq: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub sequence_number: CheckpointSequenceNumber,
    pub checkpoint_digest: String,
    pub checkpoint_data: Vec<u8>, // Serialized checkpoint summary
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeStoreSnapshot {
    pub committees: Vec<CommitteeEntry>,
    pub latest_epoch: Option<EpochId>,
    pub current_committee: Option<Committee>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitteeEntry {
    pub epoch: EpochId,
    pub committee: Committee,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreSnapshot {
    pub objects: Vec<ObjectEntry>,
    pub total_objects: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub key: ObjectKey,
    pub object_id: ObjectID,
    pub version: VersionNumber,
    pub object_data: Vec<u8>, // Serialized object
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStoreSnapshot {
    pub transactions: Vec<TransactionEntry>,
    pub effects: Vec<EffectsEntry>,
    pub events: Vec<EventsEntry>,
    pub total_transactions: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEntry {
    pub digest: TransactionDigest,
    pub transaction_data: Vec<u8>, // Serialized transaction
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectsEntry {
    pub digest: TransactionEffectsDigest,
    pub effects_data: Vec<u8>, // Serialized effects
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsEntry {
    pub digest: TransactionEventsDigest,
    pub events_data: Vec<u8>, // Serialized events
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointSnapshotData {
    pub checkpoint_seq: CheckpointSequenceNumber,
    pub sequence_number: CheckpointSequenceNumber,
    pub transactions: Vec<TransactionDigest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSnapshotData {
    pub epoch: EpochId,
    pub committee: Option<Committee>,
    pub epoch_start_checkpoint: Option<CheckpointSequenceNumber>,
    pub epoch_end_checkpoint: Option<CheckpointSequenceNumber>,
}
