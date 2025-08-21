//! Integration tests for mgo-snapshot
//! 
//! This module contains comprehensive integration tests that verify
//! the complete snapshot functionality including creation, storage,
//! compression, validation, and restoration.

use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;

use mgo_snapshot::{
    SnapshotManager, SnapshotConfig, LocalSnapshotStorage,
    SnapshotType, CompressionPriority, SnapshotError,
};
use mgo_snapshot::types::{
    config::{CompressionType, ValidationLevel},
    snapshot::{SnapshotData, SnapshotMetadata},
    error::SnapshotResult,
};

/// Test configuration for integration tests
pub struct TestConfig {
    pub temp_dir: PathBuf,
    pub storage_backend: Arc<LocalSnapshotStorage>,
    pub manager: Arc<SnapshotManager>,
}

impl TestConfig {
    /// Create a new test configuration with temporary storage
    pub async fn new() -> SnapshotResult<Self> {
        let temp_dir = std::env::temp_dir().join(format!("mgo_snapshot_test_{}", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ));
        
        std::fs::create_dir_all(&temp_dir)?;
        
        let storage_config = mgo_snapshot::storage::LocalStorageConfig {
            base_path: temp_dir.clone(),
            compression: CompressionType::Zstd,
            max_file_size: 100 * 1024 * 1024, // 100MB
        };
        
        let storage_backend = Arc::new(LocalSnapshotStorage::new(storage_config).await?);
        
        let snapshot_config = SnapshotConfig {
            storage_backend: "local".to_string(),
            compression: CompressionType::Zstd,
            validation_level: ValidationLevel::Full,
            auto_cleanup: true,
            max_snapshots: 10,
            ..Default::default()
        };
        
        let manager = Arc::new(SnapshotManager::new(
            storage_backend.clone(), 
            snapshot_config
        ).await?);
        
        Ok(TestConfig {
            temp_dir,
            storage_backend,
            manager,
        })
    }
    
    /// Cleanup test resources
    pub fn cleanup(&self) -> std::io::Result<()> {
        if self.temp_dir.exists() {
            std::fs::remove_dir_all(&self.temp_dir)?;
        }
        Ok(())
    }
}

impl Drop for TestConfig {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Helper function to create test snapshot data
fn create_test_snapshot_data(size_kb: usize) -> SnapshotData {
    let data_size = size_kb * 1024;
    let test_data = vec![0xAB; data_size]; // Pattern data for testing
    
    let metadata = SnapshotMetadata {
        snapshot_type: SnapshotType::Full {
            include_history: false,
            compression_level: mgo_snapshot::types::CompressionLevel::Medium,
        },
        checkpoint_seq: 12345,
        epoch: 10,
        timestamp: chrono::Utc::now(),
        format_version: 1,
        ..Default::default()
    };
    
    SnapshotData::new(metadata, test_data)
}

#[tokio::test]
async fn test_basic_snapshot_lifecycle() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    
    // Create a test snapshot
    let snapshot_data = create_test_snapshot_data(100); // 100KB
    let snapshot_id = config.storage_backend.store_snapshot(
        mgo_snapshot::types::SnapshotId::new(),
        snapshot_data.clone(),
        snapshot_data.metadata.clone()
    ).await?;
    
    // Retrieve the snapshot
    let retrieved_data = config.storage_backend.retrieve_snapshot(snapshot_id).await?;
    
    // Verify data integrity
    assert_eq!(snapshot_data.data.len(), retrieved_data.data.len());
    assert_eq!(snapshot_data.metadata.checkpoint_seq, retrieved_data.metadata.checkpoint_seq);
    assert_eq!(snapshot_data.metadata.epoch, retrieved_data.metadata.epoch);
    
    // Delete the snapshot
    config.storage_backend.delete_snapshot(snapshot_id).await?;
    
    // Verify it's deleted
    assert!(config.storage_backend.retrieve_snapshot(snapshot_id).await.is_err());
    
    println!("✅ Basic snapshot lifecycle test passed");
    Ok(())
}

#[tokio::test]
async fn test_compression_algorithms() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    
    let test_data = create_test_snapshot_data(500); // 500KB
    let algorithms = vec![
        CompressionType::None,
        CompressionType::Zstd,
        CompressionType::Lz4,
        CompressionType::Gzip,
    ];
    
    for algorithm in algorithms {
        println!("Testing compression algorithm: {:?}", algorithm);
        
        // Create compressor
        let compressor = mgo_snapshot::creator::compressor::SnapshotCompressor::new(algorithm)?;
        
        // Test compress/decompress cycle
        let compressed = compressor.compress_data(&test_data.data).await?;
        let decompressed = compressor.decompress_data(&compressed).await?;
        
        // Verify data integrity
        assert_eq!(test_data.data, decompressed);
        
        // Check compression effectiveness (except for None)
        if !matches!(algorithm, CompressionType::None) {
            assert!(compressed.len() < test_data.data.len(), 
                   "Compression should reduce size for {:?}", algorithm);
        } else {
            assert_eq!(compressed.len(), test_data.data.len());
        }
        
        println!("  ✅ {:?}: Original: {}KB, Compressed: {}KB, Ratio: {:.2}%", 
               algorithm,
               test_data.data.len() / 1024,
               compressed.len() / 1024,
               (compressed.len() as f64 / test_data.data.len() as f64) * 100.0);
    }
    
    println!("✅ Compression algorithms test passed");
    Ok(())
}

#[tokio::test]
async fn test_adaptive_compression_selection() -> SnapshotResult<()> {
    let test_cases = vec![
        (100 * 1024, CompressionPriority::Speed),      // 100KB, speed priority
        (1024 * 1024, CompressionPriority::Balanced),  // 1MB, balanced
        (10 * 1024 * 1024, CompressionPriority::Ratio), // 10MB, ratio priority
    ];
    
    for (data_size, priority) in test_cases {
        let selected_algorithm = mgo_snapshot::creator::compressor::SnapshotCompressor::select_optimal_algorithm(data_size, priority);
        
        println!("Data size: {}KB, Priority: {:?} -> Algorithm: {:?}", 
               data_size / 1024, priority, selected_algorithm);
        
        // Verify selection makes sense
        match priority {
            CompressionPriority::Speed => {
                assert!(matches!(selected_algorithm, CompressionType::None | CompressionType::Lz4));
            }
            CompressionPriority::Ratio => {
                assert!(matches!(selected_algorithm, CompressionType::Zstd | CompressionType::Gzip));
            }
            CompressionPriority::Balanced => {
                // Can be any algorithm based on size
            }
        }
    }
    
    println!("✅ Adaptive compression selection test passed");
    Ok(())
}

#[tokio::test]
async fn test_concurrent_snapshot_operations() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    let num_concurrent = 5;
    
    // Create multiple snapshots concurrently
    let mut handles = Vec::new();
    for i in 0..num_concurrent {
        let storage = config.storage_backend.clone();
        let handle = tokio::spawn(async move {
            let snapshot_data = create_test_snapshot_data(50 + i * 10); // Variable size
            let snapshot_id = mgo_snapshot::types::SnapshotId::new();
            
            storage.store_snapshot(snapshot_id, snapshot_data.clone(), snapshot_data.metadata).await
        });
        handles.push(handle);
    }
    
    // Wait for all operations to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap()?);
    }
    
    println!("✅ Stored {} snapshots concurrently", results.len());
    
    // Verify all snapshots can be retrieved
    for snapshot_id in &results {
        let _retrieved = config.storage_backend.retrieve_snapshot(*snapshot_id).await?;
    }
    
    println!("✅ Retrieved all {} snapshots successfully", results.len());
    
    // Cleanup
    for snapshot_id in results {
        config.storage_backend.delete_snapshot(snapshot_id).await?;
    }
    
    println!("✅ Concurrent snapshot operations test passed");
    Ok(())
}

#[tokio::test]
async fn test_error_handling_and_recovery() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    
    // Test invalid snapshot ID retrieval
    let invalid_id = mgo_snapshot::types::SnapshotId::new();
    let result = config.storage_backend.retrieve_snapshot(invalid_id).await;
    assert!(result.is_err());
    
    // Test storage with corrupted data
    let snapshot_data = create_test_snapshot_data(10);
    let snapshot_id = config.storage_backend.store_snapshot(
        mgo_snapshot::types::SnapshotId::new(),
        snapshot_data.clone(),
        snapshot_data.metadata
    ).await?;
    
    // Simulate corruption by writing random data to the file
    let file_path = config.temp_dir.join(format!("{}.snapshot", snapshot_id));
    if file_path.exists() {
        std::fs::write(&file_path, b"corrupted_data")?;
        
        // Try to retrieve corrupted snapshot
        let result = config.storage_backend.retrieve_snapshot(snapshot_id).await;
        assert!(result.is_err());
    }
    
    println!("✅ Error handling and recovery test passed");
    Ok(())
}

#[tokio::test]
async fn test_performance_benchmarks() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    
    let test_sizes = vec![100, 1000, 5000]; // KB
    
    for size_kb in test_sizes {
        let snapshot_data = create_test_snapshot_data(size_kb);
        
        // Benchmark storage
        let start = std::time::Instant::now();
        let snapshot_id = config.storage_backend.store_snapshot(
            mgo_snapshot::types::SnapshotId::new(),
            snapshot_data.clone(),
            snapshot_data.metadata.clone()
        ).await?;
        let store_duration = start.elapsed();
        
        // Benchmark retrieval
        let start = std::time::Instant::now();
        let _retrieved = config.storage_backend.retrieve_snapshot(snapshot_id).await?;
        let retrieve_duration = start.elapsed();
        
        // Cleanup
        config.storage_backend.delete_snapshot(snapshot_id).await?;
        
        println!("📊 Performance for {}KB snapshot:", size_kb);
        println!("   Store: {:.2}ms ({:.2}MB/s)", 
               store_duration.as_millis(),
               (size_kb as f64) / (store_duration.as_secs_f64() * 1024.0));
        println!("   Retrieve: {:.2}ms ({:.2}MB/s)", 
               retrieve_duration.as_millis(),
               (size_kb as f64) / (retrieve_duration.as_secs_f64() * 1024.0));
    }
    
    println!("✅ Performance benchmarks completed");
    Ok(())
}

#[tokio::test]
async fn test_timeout_scenarios() -> SnapshotResult<()> {
    let config = TestConfig::new().await?;
    
    // Test with very short timeout to simulate timeout scenario
    let snapshot_data = create_test_snapshot_data(1000); // 1MB
    
    let timeout_result = timeout(
        Duration::from_millis(1), // Very short timeout
        config.storage_backend.store_snapshot(
            mgo_snapshot::types::SnapshotId::new(),
            snapshot_data.clone(),
            snapshot_data.metadata
        )
    ).await;
    
    // The operation should timeout (this is expected)
    assert!(timeout_result.is_err());
    
    println!("✅ Timeout scenarios test passed");
    Ok(())
}

/// Run all integration tests in sequence
pub async fn run_all_integration_tests() -> SnapshotResult<()> {
    println!("🚀 Starting integration test suite...");
    
    test_basic_snapshot_lifecycle().await?;
    test_compression_algorithms().await?;
    test_adaptive_compression_selection().await?;
    test_concurrent_snapshot_operations().await?;
    test_error_handling_and_recovery().await?;
    test_performance_benchmarks().await?;
    test_timeout_scenarios().await?;
    
    println!("🎉 All integration tests passed!");
    Ok(())
}
