//! Performance tests for mgo-snapshot
//! 
//! This module contains comprehensive performance tests that measure
//! throughput, latency, memory usage, and resource utilization.

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;

use mgo_snapshot::{
    SnapshotManager, SnapshotConfig, LocalSnapshotStorage,
    SnapshotType, CompressionPriority, CompressionStats,
};
use mgo_snapshot::types::{
    config::{CompressionType, ValidationLevel},
    snapshot::{SnapshotData, SnapshotMetadata},
    error::SnapshotResult,
};

/// Performance test configuration
#[derive(Debug, Clone)]
pub struct PerformanceTestConfig {
    /// Number of iterations for each test
    pub iterations: usize,
    /// Data sizes to test (in KB)
    pub data_sizes: Vec<usize>,
    /// Compression algorithms to test
    pub compression_algorithms: Vec<CompressionType>,
    /// Concurrency levels to test
    pub concurrency_levels: Vec<usize>,
    /// Whether to include memory profiling
    pub enable_memory_profiling: bool,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            iterations: 10,
            data_sizes: vec![100, 500, 1000, 5000, 10000], // KB
            compression_algorithms: vec![
                CompressionType::None,
                CompressionType::Lz4,
                CompressionType::Zstd,
                CompressionType::Gzip,
            ],
            concurrency_levels: vec![1, 2, 4, 8],
            enable_memory_profiling: true,
        }
    }
}

/// Performance test results
#[derive(Debug, Clone)]
pub struct PerformanceResults {
    pub test_name: String,
    pub iterations: usize,
    pub avg_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub throughput_mbps: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub success_rate: f64,
    pub metadata: HashMap<String, String>,
}

impl PerformanceResults {
    fn new(test_name: String, iterations: usize) -> Self {
        Self {
            test_name,
            iterations,
            avg_duration: Duration::ZERO,
            min_duration: Duration::MAX,
            max_duration: Duration::ZERO,
            throughput_mbps: 0.0,
            memory_usage_mb: 0.0,
            cpu_usage_percent: 0.0,
            success_rate: 0.0,
            metadata: HashMap::new(),
        }
    }
    
    fn update_from_durations(&mut self, durations: &[Duration], data_size_mb: f64) {
        if durations.is_empty() {
            return;
        }
        
        self.min_duration = *durations.iter().min().unwrap();
        self.max_duration = *durations.iter().max().unwrap();
        self.avg_duration = Duration::from_nanos(
            durations.iter().map(|d| d.as_nanos()).sum::<u128>() / durations.len() as u128
        );
        
        if self.avg_duration.as_secs_f64() > 0.0 {
            self.throughput_mbps = data_size_mb / self.avg_duration.as_secs_f64();
        }
        
        self.success_rate = durations.len() as f64 / self.iterations as f64;
    }
    
    fn print_summary(&self) {
        println!("📊 Performance Test: {}", self.test_name);
        println!("   Iterations: {}", self.iterations);
        println!("   Success Rate: {:.1}%", self.success_rate * 100.0);
        println!("   Average Duration: {:.2}ms", self.avg_duration.as_millis());
        println!("   Min/Max Duration: {:.2}ms / {:.2}ms", 
               self.min_duration.as_millis(), self.max_duration.as_millis());
        println!("   Throughput: {:.2} MB/s", self.throughput_mbps);
        if self.memory_usage_mb > 0.0 {
            println!("   Memory Usage: {:.2} MB", self.memory_usage_mb);
        }
        if self.cpu_usage_percent > 0.0 {
            println!("   CPU Usage: {:.1}%", self.cpu_usage_percent);
        }
        for (key, value) in &self.metadata {
            println!("   {}: {}", key, value);
        }
        println!();
    }
}

/// Performance test suite
pub struct PerformanceTestSuite {
    config: PerformanceTestConfig,
    results: Vec<PerformanceResults>,
    temp_dir: std::path::PathBuf,
}

impl PerformanceTestSuite {
    pub fn new(config: PerformanceTestConfig) -> Self {
        let temp_dir = std::env::temp_dir().join(format!("mgo_perf_test_{}", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ));
        
        Self {
            config,
            results: Vec::new(),
            temp_dir,
        }
    }
    
    /// Setup test environment
    async fn setup(&self) -> SnapshotResult<Arc<LocalSnapshotStorage>> {
        std::fs::create_dir_all(&self.temp_dir)?;
        
        let storage_config = mgo_snapshot::storage::LocalStorageConfig {
            base_path: self.temp_dir.clone(),
            compression: CompressionType::Zstd,
            max_file_size: 1024 * 1024 * 1024, // 1GB
        };
        
        Ok(Arc::new(LocalSnapshotStorage::new(storage_config).await?))
    }
    
    /// Cleanup test environment
    fn cleanup(&self) -> std::io::Result<()> {
        if self.temp_dir.exists() {
            std::fs::remove_dir_all(&self.temp_dir)?;
        }
        Ok(())
    }
    
    /// Create test data of specified size
    fn create_test_data(&self, size_kb: usize) -> SnapshotData {
        let data_size = size_kb * 1024;
        let mut test_data = Vec::with_capacity(data_size);
        
        // Create semi-random but compressible data
        for i in 0..data_size {
            test_data.push((i % 256) as u8);
        }
        
        let metadata = SnapshotMetadata {
            snapshot_type: SnapshotType::Full {
                include_history: false,
                compression_level: mgo_snapshot::types::CompressionLevel::Medium,
            },
            checkpoint_seq: (size_kb % 100000) as u64,
            epoch: (size_kb % 1000) as u64,
            timestamp: chrono::Utc::now(),
            format_version: 1,
            ..Default::default()
        };
        
        SnapshotData::new(metadata, test_data)
    }
    
    /// Test compression performance across different algorithms
    pub async fn test_compression_performance(&mut self) -> SnapshotResult<()> {
        println!("🔄 Running compression performance tests...");
        
        for &size_kb in &self.config.data_sizes {
            for &algorithm in &self.config.compression_algorithms {
                let test_name = format!("Compression-{:?}-{}KB", algorithm, size_kb);
                let mut results = PerformanceResults::new(test_name, self.config.iterations);
                
                let test_data = self.create_test_data(size_kb);
                let compressor = mgo_snapshot::creator::compressor::SnapshotCompressor::new(algorithm)?;
                
                let mut durations = Vec::new();
                let mut compression_ratios = Vec::new();
                
                for _ in 0..self.config.iterations {
                    let start = Instant::now();
                    match compressor.compress_data(&test_data.data).await {
                        Ok(compressed) => {
                            let duration = start.elapsed();
                            durations.push(duration);
                            
                            let ratio = compressed.len() as f64 / test_data.data.len() as f64;
                            compression_ratios.push(ratio);
                        }
                        Err(_) => {
                            // Skip failed iterations
                        }
                    }
                }
                
                results.update_from_durations(&durations, size_kb as f64 / 1024.0);
                
                if !compression_ratios.is_empty() {
                    let avg_ratio = compression_ratios.iter().sum::<f64>() / compression_ratios.len() as f64;
                    results.metadata.insert("Compression Ratio".to_string(), format!("{:.2}%", avg_ratio * 100.0));
                }
                
                results.print_summary();
                self.results.push(results);
            }
        }
        
        Ok(())
    }
    
    /// Test storage throughput
    pub async fn test_storage_throughput(&mut self) -> SnapshotResult<()> {
        println!("🔄 Running storage throughput tests...");
        
        let storage = self.setup().await?;
        
        for &size_kb in &self.config.data_sizes {
            let test_name = format!("Storage-Throughput-{}KB", size_kb);
            let mut results = PerformanceResults::new(test_name, self.config.iterations);
            
            let test_data = self.create_test_data(size_kb);
            let mut durations = Vec::new();
            let mut snapshot_ids = Vec::new();
            
            // Store snapshots
            for _ in 0..self.config.iterations {
                let snapshot_id = mgo_snapshot::types::SnapshotId::new();
                let start = Instant::now();
                
                match storage.store_snapshot(snapshot_id, test_data.clone(), test_data.metadata.clone()).await {
                    Ok(_) => {
                        let duration = start.elapsed();
                        durations.push(duration);
                        snapshot_ids.push(snapshot_id);
                    }
                    Err(_) => {
                        // Skip failed iterations
                    }
                }
            }
            
            results.update_from_durations(&durations, size_kb as f64 / 1024.0);
            results.metadata.insert("Operation".to_string(), "Store".to_string());
            results.print_summary();
            self.results.push(results);
            
            // Test retrieval performance
            let mut retrieval_results = PerformanceResults::new(
                format!("Storage-Retrieval-{}KB", size_kb), 
                snapshot_ids.len()
            );
            let mut retrieval_durations = Vec::new();
            
            for snapshot_id in &snapshot_ids {
                let start = Instant::now();
                match storage.retrieve_snapshot(*snapshot_id).await {
                    Ok(_) => {
                        let duration = start.elapsed();
                        retrieval_durations.push(duration);
                    }
                    Err(_) => {
                        // Skip failed iterations
                    }
                }
            }
            
            retrieval_results.update_from_durations(&retrieval_durations, size_kb as f64 / 1024.0);
            retrieval_results.metadata.insert("Operation".to_string(), "Retrieve".to_string());
            retrieval_results.print_summary();
            self.results.push(retrieval_results);
            
            // Cleanup
            for snapshot_id in snapshot_ids {
                let _ = storage.delete_snapshot(snapshot_id).await;
            }
        }
        
        Ok(())
    }
    
    /// Test concurrent operations performance
    pub async fn test_concurrent_performance(&mut self) -> SnapshotResult<()> {
        println!("🔄 Running concurrent performance tests...");
        
        let storage = self.setup().await?;
        
        for &concurrency in &self.config.concurrency_levels {
            for &size_kb in &[100, 1000, 5000] { // Subset of sizes for concurrency tests
                let test_name = format!("Concurrent-{}x-{}KB", concurrency, size_kb);
                let mut results = PerformanceResults::new(test_name, 1); // Single test run
                
                let test_data = self.create_test_data(size_kb);
                
                let start = Instant::now();
                
                // Spawn concurrent tasks
                let mut handles = Vec::new();
                for _ in 0..concurrency {
                    let storage_clone = storage.clone();
                    let data_clone = test_data.clone();
                    
                    let handle = tokio::spawn(async move {
                        let snapshot_id = mgo_snapshot::types::SnapshotId::new();
                        storage_clone.store_snapshot(snapshot_id, data_clone.clone(), data_clone.metadata).await
                    });
                    handles.push(handle);
                }
                
                // Wait for all tasks to complete
                let mut successful = 0;
                let mut snapshot_ids = Vec::new();
                
                for handle in handles {
                    match handle.await {
                        Ok(Ok(snapshot_id)) => {
                            successful += 1;
                            snapshot_ids.push(snapshot_id);
                        }
                        _ => {} // Failed
                    }
                }
                
                let total_duration = start.elapsed();
                let total_data_mb = (size_kb * successful) as f64 / 1024.0;
                
                results.avg_duration = total_duration;
                results.min_duration = total_duration;
                results.max_duration = total_duration;
                results.success_rate = successful as f64 / concurrency as f64;
                
                if total_duration.as_secs_f64() > 0.0 {
                    results.throughput_mbps = total_data_mb / total_duration.as_secs_f64();
                }
                
                results.metadata.insert("Concurrency".to_string(), concurrency.to_string());
                results.metadata.insert("Successful".to_string(), successful.to_string());
                results.print_summary();
                self.results.push(results);
                
                // Cleanup
                for snapshot_id in snapshot_ids {
                    let _ = storage.delete_snapshot(snapshot_id).await;
                }
            }
        }
        
        Ok(())
    }
    
    /// Test memory usage patterns
    pub async fn test_memory_performance(&mut self) -> SnapshotResult<()> {
        if !self.config.enable_memory_profiling {
            return Ok(());
        }
        
        println!("🔄 Running memory performance tests...");
        
        let storage = self.setup().await?;
        
        for &size_kb in &[1000, 5000, 10000] { // Focus on larger sizes
            let test_name = format!("Memory-Usage-{}KB", size_kb);
            let mut results = PerformanceResults::new(test_name, 1);
            
            let test_data = self.create_test_data(size_kb);
            
            // Measure memory before operation
            let memory_before = get_memory_usage_mb();
            
            let start = Instant::now();
            let snapshot_id = mgo_snapshot::types::SnapshotId::new();
            let _ = storage.store_snapshot(snapshot_id, test_data.clone(), test_data.metadata.clone()).await?;
            let duration = start.elapsed();
            
            // Measure memory after operation
            let memory_after = get_memory_usage_mb();
            let memory_delta = memory_after - memory_before;
            
            results.avg_duration = duration;
            results.min_duration = duration;
            results.max_duration = duration;
            results.memory_usage_mb = memory_delta;
            results.success_rate = 1.0;
            
            if duration.as_secs_f64() > 0.0 {
                results.throughput_mbps = (size_kb as f64 / 1024.0) / duration.as_secs_f64();
            }
            
            results.metadata.insert("Memory Delta".to_string(), format!("{:.2} MB", memory_delta));
            results.metadata.insert("Memory Efficiency".to_string(), 
                                  format!("{:.2} MB/MB", memory_delta / (size_kb as f64 / 1024.0)));
            
            results.print_summary();
            self.results.push(results);
            
            // Cleanup
            let _ = storage.delete_snapshot(snapshot_id).await;
        }
        
        Ok(())
    }
    
    /// Generate comprehensive performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        
        report.push_str("# mgo-snapshot Performance Test Report\n\n");
        report.push_str(&format!("Generated at: {}\n\n", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
        
        report.push_str("## Test Configuration\n");
        report.push_str(&format!("- Iterations: {}\n", self.config.iterations));
        report.push_str(&format!("- Data Sizes: {:?} KB\n", self.config.data_sizes));
        report.push_str(&format!("- Compression Algorithms: {:?}\n", self.config.compression_algorithms));
        report.push_str(&format!("- Concurrency Levels: {:?}\n", self.config.concurrency_levels));
        report.push_str(&format!("- Memory Profiling: {}\n\n", self.config.enable_memory_profiling));
        
        report.push_str("## Results Summary\n\n");
        
        // Group results by test type
        let mut by_type: HashMap<String, Vec<&PerformanceResults>> = HashMap::new();
        for result in &self.results {
            let test_type = result.test_name.split('-').next().unwrap_or("Unknown").to_string();
            by_type.entry(test_type).or_default().push(result);
        }
        
        for (test_type, results) in by_type {
            report.push_str(&format!("### {} Tests\n\n", test_type));
            
            for result in results {
                report.push_str(&format!("**{}**\n", result.test_name));
                report.push_str(&format!("- Success Rate: {:.1}%\n", result.success_rate * 100.0));
                report.push_str(&format!("- Average Duration: {:.2}ms\n", result.avg_duration.as_millis()));
                report.push_str(&format!("- Throughput: {:.2} MB/s\n", result.throughput_mbps));
                
                if result.memory_usage_mb > 0.0 {
                    report.push_str(&format!("- Memory Usage: {:.2} MB\n", result.memory_usage_mb));
                }
                
                for (key, value) in &result.metadata {
                    report.push_str(&format!("- {}: {}\n", key, value));
                }
                
                report.push_str("\n");
            }
        }
        
        // Performance insights
        report.push_str("## Performance Insights\n\n");
        
        // Find best performing compression algorithm
        let compression_results: Vec<_> = self.results.iter()
            .filter(|r| r.test_name.starts_with("Compression"))
            .collect();
        
        if !compression_results.is_empty() {
            let best_compression = compression_results.iter()
                .max_by(|a, b| a.throughput_mbps.partial_cmp(&b.throughput_mbps).unwrap())
                .unwrap();
            
            report.push_str(&format!("- **Best Compression Performance**: {} ({:.2} MB/s)\n", 
                                   best_compression.test_name, best_compression.throughput_mbps));
        }
        
        // Find best storage throughput
        let storage_results: Vec<_> = self.results.iter()
            .filter(|r| r.test_name.starts_with("Storage"))
            .collect();
        
        if !storage_results.is_empty() {
            let best_storage = storage_results.iter()
                .max_by(|a, b| a.throughput_mbps.partial_cmp(&b.throughput_mbps).unwrap())
                .unwrap();
            
            report.push_str(&format!("- **Best Storage Performance**: {} ({:.2} MB/s)\n", 
                                   best_storage.test_name, best_storage.throughput_mbps));
        }
        
        report.push_str("\n");
        report.push_str("## Recommendations\n\n");
        report.push_str("- For best compression performance: Use algorithms with highest throughput for your data size\n");
        report.push_str("- For concurrent operations: Consider optimal concurrency level based on system resources\n");
        report.push_str("- For memory efficiency: Monitor memory usage patterns for large snapshots\n");
        
        report
    }
    
    /// Run all performance tests
    pub async fn run_all_tests(&mut self) -> SnapshotResult<()> {
        println!("🚀 Starting performance test suite...");
        
        self.test_compression_performance().await?;
        self.test_storage_throughput().await?;
        self.test_concurrent_performance().await?;
        self.test_memory_performance().await?;
        
        println!("🎉 All performance tests completed!");
        println!("\n{}", self.generate_report());
        
        Ok(())
    }
}

impl Drop for PerformanceTestSuite {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Get current memory usage in MB (placeholder implementation)
fn get_memory_usage_mb() -> f64 {
    // This is a simplified implementation
    // In a real scenario, you would use system APIs or crates like `sysinfo`
    
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<f64>() {
                            return kb / 1024.0; // Convert KB to MB
                        }
                    }
                }
            }
        }
    }
    
    // Fallback: return estimated memory usage
    0.0
}

#[tokio::test]
async fn run_performance_test_suite() -> SnapshotResult<()> {
    let config = PerformanceTestConfig {
        iterations: 3, // Reduced for testing
        data_sizes: vec![100, 500, 1000], // Smaller sizes for testing
        compression_algorithms: vec![CompressionType::None, CompressionType::Zstd],
        concurrency_levels: vec![1, 2],
        enable_memory_profiling: false, // Disabled for CI
    };
    
    let mut test_suite = PerformanceTestSuite::new(config);
    test_suite.run_all_tests().await?;
    
    Ok(())
}
