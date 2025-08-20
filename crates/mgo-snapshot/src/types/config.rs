// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Configuration types for the snapshot system


use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{ChecksumAlgorithm, CompressionLevel, SnapshotType};

/// Main configuration for the snapshot system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Whether snapshot system is enabled
    pub enabled: bool,
    /// Base path for snapshot storage
    pub base_path: PathBuf,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Scheduler configuration
    pub scheduler: SchedulerConfig,
    /// Retention policy configuration
    pub retention: RetentionConfig,
    /// Performance configuration
    pub performance: PerformanceConfig,
    /// Validation configuration
    pub validation: ValidationConfig,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_path: PathBuf::from("/data/snapshots"),
            storage: StorageConfig::default(),
            scheduler: SchedulerConfig::default(),
            retention: RetentionConfig::default(),
            performance: PerformanceConfig::default(),
            validation: ValidationConfig::default(),
        }
    }
}

/// Storage backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Primary storage backend type
    pub backend: StorageBackendType,
    /// Local storage configuration
    pub local: LocalStorageConfig,
    /// Distributed storage configuration
    pub distributed: Option<DistributedStorageConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendType::Local,
            local: LocalStorageConfig::default(),
            distributed: None,
        }
    }
}

/// Types of storage backends
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageBackendType {
    Local,
    Distributed,
    Hybrid,
}

/// Local storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    /// Base path for local storage
    pub path: PathBuf,
    /// Compression type
    pub compression: CompressionType,
    /// Whether to enable encryption
    pub encryption: bool,
    /// Maximum file size before splitting
    pub max_file_size: u64,
    /// Number of backup copies
    pub backup_copies: u32,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("/data/snapshots/local"),
            compression: CompressionType::Zstd,
            encryption: true,
            max_file_size: 10 * 1024 * 1024 * 1024, // 10GB
            backup_copies: 2,
        }
    }
}

/// Distributed storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedStorageConfig {
    /// IPFS endpoint URL
    pub ipfs_endpoint: Option<String>,
    /// S3 bucket name
    pub s3_bucket: Option<String>,
    /// S3 region
    pub s3_region: Option<String>,
    /// S3 access key
    pub s3_access_key: Option<String>,
    /// S3 secret key
    pub s3_secret_key: Option<String>,
    /// Replication factor
    pub replication_factor: u32,
    /// Enable multi-region replication
    pub multi_region: bool,
}

impl Default for DistributedStorageConfig {
    fn default() -> Self {
        Self {
            ipfs_endpoint: Some("http://localhost:5001".to_string()),
            s3_bucket: None,
            s3_region: None,
            s3_access_key: None,
            s3_secret_key: None,
            replication_factor: 3,
            multi_region: false,
        }
    }
}

/// Compression types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Zstd,
    Lz4,
    Gzip,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Zstd
    }
}

/// Scheduler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// Snapshot strategies to use
    pub strategies: Vec<SnapshotStrategy>,
    /// Whether to enable automatic scheduling
    pub auto_schedule: bool,
    /// Maximum concurrent snapshots
    pub max_concurrent: u32,
    /// Pause scheduling during high load
    pub pause_on_high_load: bool,
    /// Load threshold for pausing (0.0-1.0)
    pub load_threshold: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            strategies: vec![
                SnapshotStrategy::TimeInterval {
                    interval_hours: 1,
                    snapshot_type: SnapshotType::Incremental {
                        base_snapshot: Default::default(),
                        changed_components: vec![],
                    },
                },
                SnapshotStrategy::CheckpointInterval {
                    count: 1000,
                    snapshot_type: SnapshotType::Full {
                        include_history: false,
                        compression_level: CompressionLevel::Medium,
                    },
                },
            ],
            auto_schedule: true,
            max_concurrent: 2,
            pause_on_high_load: true,
            load_threshold: 0.8,
        }
    }
}

/// Snapshot scheduling strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotStrategy {
    /// Time interval based snapshots
    TimeInterval {
        interval_hours: u64,
        snapshot_type: SnapshotType,
    },
    /// Checkpoint interval based snapshots
    CheckpointInterval {
        count: u64,
        snapshot_type: SnapshotType,
    },
    /// Epoch boundary snapshots
    EpochBoundary {
        snapshot_type: SnapshotType,
    },
    /// Hybrid strategy combining multiple approaches
    Hybrid {
        strategies: Vec<SnapshotStrategy>,
    },
}

/// Retention policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Maximum number of snapshots to keep
    pub max_snapshots: u32,
    /// Maximum age in days
    pub max_age_days: u32,
    /// Whether to keep epoch boundary snapshots
    pub keep_epoch_snapshots: bool,
    /// Whether to keep checkpoint milestone snapshots
    pub keep_milestone_snapshots: bool,
    /// Cleanup interval in hours
    pub cleanup_interval_hours: u64,
    /// Minimum free space threshold (bytes)
    pub min_free_space: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_snapshots: 100,
            max_age_days: 30,
            keep_epoch_snapshots: true,
            keep_milestone_snapshots: true,
            cleanup_interval_hours: 6,
            min_free_space: 100 * 1024 * 1024 * 1024, // 100GB
        }
    }
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum parallel operations
    pub max_parallel_operations: u32,
    /// Compression level (0-9)
    pub compression_level: u32,
    /// Checksum algorithm
    pub checksum_algorithm: ChecksumAlgorithm,
    /// Snapshot operation timeout in seconds
    pub snapshot_timeout_secs: u64,
    /// Buffer size for I/O operations
    pub io_buffer_size: usize,
    /// Whether to use memory mapping for large files
    pub use_memory_mapping: bool,
    /// Maximum memory usage for snapshots (bytes)
    pub max_memory_usage: u64,
    /// Maximum number of objects to include in a snapshot
    pub max_objects_per_snapshot: Option<usize>,
    /// Maximum number of transactions to include in a snapshot
    pub max_transactions_per_snapshot: Option<usize>,
    /// Maximum number of effects to include in a snapshot
    pub max_effects_per_snapshot: Option<usize>,
    /// Maximum number of events to include in a snapshot
    pub max_events_per_snapshot: Option<usize>,
    /// Maximum number of checkpoints to include in a snapshot
    pub max_checkpoints_per_snapshot: Option<usize>,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_parallel_operations: 4,
            compression_level: 6,
            checksum_algorithm: ChecksumAlgorithm::Blake3,
            snapshot_timeout_secs: 30 * 60, // 30 minutes
            io_buffer_size: 64 * 1024,      // 64KB
            use_memory_mapping: true,
            max_memory_usage: 8 * 1024 * 1024 * 1024, // 8GB
            max_objects_per_snapshot: Some(1_000_000),
            max_transactions_per_snapshot: Some(1_000_000),
            max_effects_per_snapshot: Some(1_000_000),
            max_events_per_snapshot: Some(500_000),
            max_checkpoints_per_snapshot: Some(10_000),
        }
    }
}

/// Validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Verify snapshots upon creation
    pub verify_on_create: bool,
    /// Verify snapshots before restore
    pub verify_on_restore: bool,
    /// Enable deep validation (slower but more thorough)
    pub deep_validation: bool,
    /// Maximum repair attempts for corrupted snapshots
    pub repair_attempts: u32,
    /// Enable automatic repair
    pub auto_repair: bool,
    /// Validation timeout in seconds
    pub validation_timeout_secs: u64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            verify_on_create: true,
            verify_on_restore: true,
            deep_validation: false,
            repair_attempts: 3,
            auto_repair: true,
            validation_timeout_secs: 10 * 60, // 10 minutes
        }
    }
}

/// Restore operation options
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Validation level to apply
    pub validation_level: ValidationLevel,
    /// Whether to backup current state before restore
    pub backup_current: bool,
    /// Components to include in restore
    pub include_components: Vec<super::ComponentType>,
    /// Whether to include transaction history
    pub include_transactions: bool,
    /// Whether to include index data
    pub include_indexes: bool,
    /// Custom restore parameters
    pub custom_params: std::collections::HashMap<String, String>,
}

/// Validation levels for restore operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationLevel {
    None,
    Basic,
    Full,
    Deep,
}

impl Default for ValidationLevel {
    fn default() -> Self {
        Self::Full
    }
}

/// Cluster restore options
#[derive(Debug, Clone, Default)]
pub struct ClusterRestoreOptions {
    /// Individual node restore options
    pub node_options: RestoreOptions,
    /// Whether to coordinate restore across all nodes
    pub coordinated_restore: bool,
    /// Maximum time to wait for all nodes (seconds)
    pub coordination_timeout_secs: u64,
    /// Whether to verify cluster consistency after restore
    pub verify_cluster_consistency: bool,
    /// Nodes to exclude from restore
    pub exclude_nodes: Vec<String>,
}

impl SnapshotConfig {
    /// Load configuration from file
    pub fn load_from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn save_to_file<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if !self.base_path.is_absolute() {
            return Err("Base path must be absolute".to_string());
        }

        if self.performance.max_parallel_operations == 0 {
            return Err("max_parallel_operations must be greater than 0".to_string());
        }

        if self.retention.max_snapshots == 0 {
            return Err("max_snapshots must be greater than 0".to_string());
        }

        if self.performance.compression_level > 22 {
            return Err("compression_level must be between 0 and 22".to_string());
        }

        Ok(())
    }
}
