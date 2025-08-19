// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Snapshot data types and structures

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Unique identifier for a snapshot
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(pub Uuid);

impl SnapshotId {
    /// Generate a new unique snapshot ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create snapshot ID from string
    pub fn from_string(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Cluster-wide snapshot identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterSnapshotId(pub Uuid);

impl ClusterSnapshotId {
    /// Generate a new cluster snapshot ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ClusterSnapshotId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ClusterSnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cluster-{}", self.0)
    }
}

/// Types of snapshots that can be created
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SnapshotType {
    /// Full snapshot containing all component states
    Full {
        include_history: bool,
        compression_level: CompressionLevel,
    },
    /// Incremental snapshot relative to a base snapshot
    Incremental {
        base_snapshot: SnapshotId,
        changed_components: Vec<ComponentType>,
    },
    /// Checkpoint-specific snapshot
    Checkpoint {
        checkpoint_seq: u64,
        include_transactions: bool,
    },
    /// Epoch boundary snapshot
    Epoch {
        epoch: u64,
        include_committee_info: bool,
    },
}

/// Compression levels for snapshot data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionLevel {
    None,
    Low,
    Medium,
    High,
    Maximum,
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::Medium
    }
}

/// Component types that can be included in snapshots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ComponentType {
    AuthorityState,
    EpochStore,
    CheckpointStore,
    ConsensusState,
    TransactionStore,
    ObjectStore,
    IndexStore,
}

/// Complete snapshot data containing all component states
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Authority state data
    pub authority_state: Vec<u8>,
    /// Epoch store data
    pub epoch_store: Vec<u8>,
    /// Checkpoint store data
    pub checkpoint_store: Vec<u8>,
    /// Consensus state data
    pub consensus_state: Vec<u8>,
    /// Transaction store data
    pub transaction_store: Vec<u8>,
    /// Object store data
    pub object_store: Vec<u8>,
    /// Index store data (optional)
    pub index_store: Option<Vec<u8>>,
    /// Checkpoint sequence number
    pub checkpoint_seq: u64,
    /// Epoch number
    pub epoch: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

impl SnapshotData {
    /// Create new empty snapshot data
    pub fn new() -> Self {
        Self {
            authority_state: Vec::new(),
            epoch_store: Vec::new(),
            checkpoint_store: Vec::new(),
            consensus_state: Vec::new(),
            transaction_store: Vec::new(),
            object_store: Vec::new(),
            index_store: None,
            checkpoint_seq: 0,
            epoch: 0,
            created_at: Utc::now(),
        }
    }

    /// Calculate total size of snapshot data
    pub fn total_size(&self) -> usize {
        self.authority_state.len()
            + self.epoch_store.len()
            + self.checkpoint_store.len()
            + self.consensus_state.len()
            + self.transaction_store.len()
            + self.object_store.len()
            + self.index_store.as_ref().map_or(0, |data| data.len())
    }
}

impl Default for SnapshotData {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata associated with a snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Unique snapshot identifier
    pub id: SnapshotId,
    /// Type of snapshot
    pub snapshot_type: SnapshotType,
    /// Checkpoint sequence number
    pub checkpoint_seq: u64,
    /// Epoch number
    pub epoch: u64,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Uncompressed size in bytes
    pub uncompressed_size: u64,
    /// Compressed size in bytes
    pub compressed_size: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Checksum for data integrity
    pub checksum: String,
    /// Checksum algorithm used
    pub checksum_algorithm: ChecksumAlgorithm,
    /// Version of the snapshot format
    pub format_version: u32,
    /// Additional custom tags
    pub tags: HashMap<String, String>,
    /// Node ID that created this snapshot
    pub created_by_node: Option<String>,
}

impl SnapshotMetadata {
    /// Create new snapshot metadata
    pub fn new(
        id: SnapshotId,
        snapshot_type: SnapshotType,
        checkpoint_seq: u64,
        epoch: u64,
    ) -> Self {
        Self {
            id,
            snapshot_type,
            checkpoint_seq,
            epoch,
            created_at: Utc::now(),
            uncompressed_size: 0,
            compressed_size: 0,
            compression_ratio: 1.0,
            checksum: String::new(),
            checksum_algorithm: ChecksumAlgorithm::Blake3,
            format_version: 1,
            tags: HashMap::new(),
            created_by_node: None,
        }
    }

    /// Add a custom tag
    pub fn add_tag(&mut self, key: String, value: String) {
        self.tags.insert(key, value);
    }

    /// Update compression metrics
    pub fn update_compression_metrics(&mut self, uncompressed: u64, compressed: u64) {
        self.uncompressed_size = uncompressed;
        self.compressed_size = compressed;
        self.compression_ratio = if uncompressed > 0 {
            compressed as f64 / uncompressed as f64
        } else {
            1.0
        };
    }
}

/// Supported checksum algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Blake3,
    Sha256,
    Sha3_256,
}

impl Default for ChecksumAlgorithm {
    fn default() -> Self {
        Self::Blake3
    }
}

/// Information about available snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot metadata
    pub metadata: SnapshotMetadata,
    /// Storage location
    pub storage_location: String,
    /// Availability status
    pub available: bool,
    /// Last verified timestamp
    pub last_verified: Option<DateTime<Utc>>,
}

/// Filter criteria for listing snapshots
#[derive(Debug, Clone, Default)]
pub struct SnapshotFilter {
    /// Filter by snapshot type
    pub snapshot_type: Option<SnapshotType>,
    /// Filter by epoch range
    pub epoch_range: Option<(u64, u64)>,
    /// Filter by checkpoint range  
    pub checkpoint_range: Option<(u64, u64)>,
    /// Filter by creation time range
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    /// Filter by tags
    pub tags: HashMap<String, String>,
    /// Maximum number of results
    pub limit: Option<usize>,
}

impl SnapshotFilter {
    /// Create new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by snapshot type
    pub fn with_type(mut self, snapshot_type: SnapshotType) -> Self {
        self.snapshot_type = Some(snapshot_type);
        self
    }

    /// Filter by epoch
    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch_range = Some((epoch, epoch));
        self
    }

    /// Filter by epoch range
    pub fn with_epoch_range(mut self, start: u64, end: u64) -> Self {
        self.epoch_range = Some((start, end));
        self
    }

    /// Filter by checkpoint
    pub fn with_checkpoint(mut self, checkpoint: u64) -> Self {
        self.checkpoint_range = Some((checkpoint, checkpoint));
        self
    }

    /// Add tag filter
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Limit number of results
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}
