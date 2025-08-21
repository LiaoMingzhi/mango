//! Unit tests for mgo-snapshot components
//! 
//! This module contains focused unit tests for individual components
//! and functions within the snapshot system.

use std::sync::Arc;
use std::collections::HashMap;

use mgo_snapshot::{
    CompressionPriority, CompressionStats,
};
use mgo_snapshot::types::{
    config::{CompressionType, ValidationLevel},
    snapshot::{SnapshotData, SnapshotMetadata, SnapshotId},
    error::{SnapshotResult, SnapshotError},
};

/// Test compression algorithm selection logic
#[test]
fn test_compression_algorithm_selection() {
    // Test speed priority
    assert_eq!(
        mgo_snapshot::creator::compressor::SnapshotCompressor::select_optimal_algorithm(
            500 * 1024, // 500KB
            CompressionPriority::Speed
        ),
        CompressionType::Lz4
    );
    
    assert_eq!(
        mgo_snapshot::creator::compressor::SnapshotCompressor::select_optimal_algorithm(
            100 * 1024, // 100KB
            CompressionPriority::Speed
        ),
        CompressionType::None
    );
    
    // Test balanced priority
    assert_eq!(
        mgo_snapshot::creator::compressor::SnapshotCompressor::select_optimal_algorithm(
            2 * 1024 * 1024, // 2MB
            CompressionPriority::Balanced
        ),
        CompressionType::Zstd
    );
    
    // Test ratio priority
    assert_eq!(
        mgo_snapshot::creator::compressor::SnapshotCompressor::select_optimal_algorithm(
            10 * 1024 * 1024, // 10MB
            CompressionPriority::Ratio
        ),
        CompressionType::Zstd
    );
}

/// Test snapshot ID generation and uniqueness
#[test]
fn test_snapshot_id_generation() {
    let mut ids = std::collections::HashSet::new();
    
    // Generate multiple IDs and ensure they're unique
    for _ in 0..1000 {
        let id = SnapshotId::new();
        assert!(ids.insert(id), "Snapshot ID should be unique");
    }
    
    // Test ID formatting
    let id = SnapshotId::new();
    let id_str = format!("{}", id);
    assert!(!id_str.is_empty());
    assert!(id_str.len() > 10); // Should be a reasonable length
}

/// Test snapshot metadata creation and validation
#[test]
fn test_snapshot_metadata() {
    let metadata = SnapshotMetadata {
        snapshot_type: mgo_snapshot::SnapshotType::Full {
            include_history: true,
            compression_level: mgo_snapshot::types::CompressionLevel::High,
        },
        checkpoint_seq: 12345,
        epoch: 42,
        timestamp: chrono::Utc::now(),
        format_version: 1,
        size: 1024 * 1024, // 1MB
        checksum: Some("abcdef123456".to_string()),
        components: vec![
            mgo_snapshot::types::ComponentType::AuthorityState,
            mgo_snapshot::types::ComponentType::EpochStore,
        ],
    };
    
    // Test serialization/deserialization
    let serialized = serde_json::to_string(&metadata).expect("Should serialize");
    let deserialized: SnapshotMetadata = serde_json::from_str(&serialized)
        .expect("Should deserialize");
    
    assert_eq!(metadata.checkpoint_seq, deserialized.checkpoint_seq);
    assert_eq!(metadata.epoch, deserialized.epoch);
    assert_eq!(metadata.format_version, deserialized.format_version);
    assert_eq!(metadata.size, deserialized.size);
    assert_eq!(metadata.components.len(), deserialized.components.len());
}

/// Test snapshot data creation and manipulation
#[test]
fn test_snapshot_data() {
    let test_data = vec![1, 2, 3, 4, 5];
    let metadata = SnapshotMetadata {
        snapshot_type: mgo_snapshot::SnapshotType::Full {
            include_history: false,
            compression_level: mgo_snapshot::types::CompressionLevel::Medium,
        },
        checkpoint_seq: 100,
        epoch: 5,
        timestamp: chrono::Utc::now(),
        format_version: 1,
        size: test_data.len() as u64,
        checksum: None,
        components: vec![],
    };
    
    let snapshot_data = SnapshotData::new(metadata.clone(), test_data.clone());
    
    assert_eq!(snapshot_data.data, test_data);
    assert_eq!(snapshot_data.metadata.checkpoint_seq, metadata.checkpoint_seq);
    assert_eq!(snapshot_data.total_size(), test_data.len());
    
    // Test compression metrics update
    let mut snapshot_data_mut = snapshot_data.clone();
    snapshot_data_mut.update_compression_metrics(1000, 500);
    // This would update internal compression metrics if implemented
}

/// Test error handling and error types
#[test]
fn test_error_handling() {
    // Test InvalidFormat error
    let error = SnapshotError::InvalidFormat {
        reason: "Test format error".to_string(),
    };
    
    match error {
        SnapshotError::InvalidFormat { reason } => {
            assert_eq!(reason, "Test format error");
        }
        _ => panic!("Wrong error type"),
    }
    
    // Test error display
    let error = SnapshotError::Compression("Test compression error".to_string());
    let error_string = format!("{}", error);
    assert!(error_string.contains("compression"));
    
    // Test error conversion
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
    let snapshot_error: SnapshotError = io_error.into();
    
    match snapshot_error {
        SnapshotError::IoError(_) => {
            // Correct conversion
        }
        _ => panic!("Wrong error conversion"),
    }
}

/// Test configuration validation
#[test]
fn test_configuration_validation() {
    use mgo_snapshot::types::config::SnapshotConfig;
    
    // Test default configuration
    let default_config = SnapshotConfig::default();
    assert_eq!(default_config.compression, CompressionType::Zstd);
    assert_eq!(default_config.validation_level, ValidationLevel::Basic);
    assert!(default_config.auto_cleanup);
    
    // Test configuration with custom values
    let custom_config = SnapshotConfig {
        storage_backend: "distributed".to_string(),
        compression: CompressionType::Lz4,
        validation_level: ValidationLevel::Full,
        auto_cleanup: false,
        max_snapshots: 50,
        retention_hours: 48,
        cleanup_interval_hours: 12,
        parallel_operations: 8,
        enable_metrics: true,
        enable_encryption: false,
        batch_size: 1000,
    };
    
    assert_eq!(custom_config.compression, CompressionType::Lz4);
    assert_eq!(custom_config.max_snapshots, 50);
    assert!(!custom_config.auto_cleanup);
}

/// Test validation levels
#[test]
fn test_validation_levels() {
    let levels = vec![
        ValidationLevel::None,
        ValidationLevel::Basic,
        ValidationLevel::Full,
    ];
    
    for level in levels {
        // Test serialization
        let serialized = serde_json::to_string(&level).expect("Should serialize");
        let deserialized: ValidationLevel = serde_json::from_str(&serialized)
            .expect("Should deserialize");
        assert_eq!(level, deserialized);
    }
    
    // Test ordering (if implemented)
    assert!(ValidationLevel::Full != ValidationLevel::Basic);
    assert!(ValidationLevel::Basic != ValidationLevel::None);
}

/// Test component types
#[test]
fn test_component_types() {
    use mgo_snapshot::types::ComponentType;
    
    let components = vec![
        ComponentType::AuthorityState,
        ComponentType::EpochStore,
        ComponentType::CheckpointStore,
        ComponentType::ConsensusState,
        ComponentType::TransactionStore,
        ComponentType::ObjectStore,
        ComponentType::IndexStore,
    ];
    
    for component in components {
        // Test display formatting
        let display_str = format!("{:?}", component);
        assert!(!display_str.is_empty());
        
        // Test serialization
        let serialized = serde_json::to_string(&component).expect("Should serialize");
        let deserialized: ComponentType = serde_json::from_str(&serialized)
            .expect("Should deserialize");
        assert_eq!(component, deserialized);
    }
}

/// Test compression statistics
#[test]
fn test_compression_stats() {
    let stats = CompressionStats {
        algorithm: CompressionType::Zstd,
        level: 3,
        total_compressed_bytes: 1024,
        total_original_bytes: 2048,
        compression_ratio: 0.5,
    };
    
    assert_eq!(stats.algorithm, CompressionType::Zstd);
    assert_eq!(stats.level, 3);
    assert_eq!(stats.compression_ratio, 0.5);
    
    // Test that compression ratio makes sense
    let calculated_ratio = stats.total_compressed_bytes as f64 / stats.total_original_bytes as f64;
    assert!((calculated_ratio - stats.compression_ratio).abs() < 0.001);
}

/// Test snapshot type variations
#[test]
fn test_snapshot_types() {
    use mgo_snapshot::{SnapshotType, types::CompressionLevel};
    
    // Test Full snapshot
    let full_snapshot = SnapshotType::Full {
        include_history: true,
        compression_level: CompressionLevel::High,
    };
    
    match full_snapshot {
        SnapshotType::Full { include_history, compression_level } => {
            assert!(include_history);
            assert_eq!(compression_level, CompressionLevel::High);
        }
        _ => panic!("Wrong snapshot type"),
    }
    
    // Test Incremental snapshot
    let base_id = SnapshotId::new();
    let incremental_snapshot = SnapshotType::Incremental {
        base_snapshot: base_id,
        changed_components: vec![
            mgo_snapshot::types::ComponentType::AuthorityState,
            mgo_snapshot::types::ComponentType::ObjectStore,
        ],
    };
    
    match incremental_snapshot {
        SnapshotType::Incremental { base_snapshot, changed_components } => {
            assert_eq!(base_snapshot, base_id);
            assert_eq!(changed_components.len(), 2);
        }
        _ => panic!("Wrong snapshot type"),
    }
    
    // Test Checkpoint snapshot
    let checkpoint_snapshot = SnapshotType::Checkpoint {
        checkpoint_seq: 12345,
        include_transactions: true,
    };
    
    match checkpoint_snapshot {
        SnapshotType::Checkpoint { checkpoint_seq, include_transactions } => {
            assert_eq!(checkpoint_seq, 12345);
            assert!(include_transactions);
        }
        _ => panic!("Wrong snapshot type"),
    }
}

/// Test data integrity utilities
#[test]
fn test_data_integrity() {
    let test_data = b"Hello, World! This is test data for integrity checking.";
    
    // Test checksum calculation (if available)
    // This is a placeholder - actual implementation would depend on the checksum algorithm used
    let checksum1 = calculate_test_checksum(test_data);
    let checksum2 = calculate_test_checksum(test_data);
    assert_eq!(checksum1, checksum2, "Same data should produce same checksum");
    
    // Test with different data
    let other_data = b"Different data";
    let checksum3 = calculate_test_checksum(other_data);
    assert_ne!(checksum1, checksum3, "Different data should produce different checksum");
}

/// Test helper: calculate checksum for testing
fn calculate_test_checksum(data: &[u8]) -> String {
    // Simple test checksum implementation
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Test memory management utilities
#[test]
fn test_memory_management() {
    // Test memory pool concepts (simplified)
    let initial_size = 1024;
    let buffer = Vec::with_capacity(initial_size);
    assert_eq!(buffer.capacity(), initial_size);
    
    // Test memory pool size calculations
    let pool_sizes = vec![1024, 4096, 16384, 65536]; // Different buffer sizes
    for size in pool_sizes {
        assert!(size.is_power_of_two() || size % 1024 == 0, 
               "Pool sizes should be reasonable");
    }
}

/// Test async operations and timeouts
#[tokio::test]
async fn test_async_operations() {
    use tokio::time::{timeout, Duration};
    
    // Test basic async operation
    let result = timeout(
        Duration::from_millis(100),
        async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "completed"
        }
    ).await;
    
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "completed");
    
    // Test timeout scenario
    let timeout_result = timeout(
        Duration::from_millis(10),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            "should_timeout"
        }
    ).await;
    
    assert!(timeout_result.is_err(), "Operation should timeout");
}

/// Test concurrent data structures
#[tokio::test]
async fn test_concurrent_structures() {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    
    let shared_data = Arc::new(RwLock::new(HashMap::<String, i32>::new()));
    
    // Test concurrent writes
    let mut handles = Vec::new();
    for i in 0..10 {
        let data_clone = shared_data.clone();
        let handle = tokio::spawn(async move {
            let mut data = data_clone.write().await;
            data.insert(format!("key_{}", i), i);
        });
        handles.push(handle);
    }
    
    // Wait for all writes to complete
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify all data was written
    let data = shared_data.read().await;
    assert_eq!(data.len(), 10);
    
    for i in 0..10 {
        assert_eq!(data.get(&format!("key_{}", i)), Some(&i));
    }
}

/// Run all unit tests
#[tokio::test]
async fn run_all_unit_tests() {
    println!("🧪 Running unit tests...");
    
    test_compression_algorithm_selection();
    test_snapshot_id_generation();
    test_snapshot_metadata();
    test_snapshot_data();
    test_error_handling();
    test_configuration_validation();
    test_validation_levels();
    test_component_types();
    test_compression_stats();
    test_snapshot_types();
    test_data_integrity();
    test_memory_management();
    
    // Async tests
    test_async_operations().await;
    test_concurrent_structures().await;
    
    println!("✅ All unit tests passed!");
}
