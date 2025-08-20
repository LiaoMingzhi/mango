//! Metrics collector implementation
//! 
//! This module provides automatic metrics collection and reporting functionality.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, RwLock},
    time::interval,
};
use tracing::{debug, error, info, instrument, warn};

use crate::{
    manager::snapshot_manager::SnapshotManager,
    metrics::prometheus::PrometheusMetrics,
    types::{
        SnapshotType, ComponentType,
        error::SnapshotResult,
    },
};

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsCollectorConfig {
    /// Collection interval in seconds
    pub collection_interval: Duration,
    /// Whether to collect system metrics (CPU, memory, etc.)
    pub collect_system_metrics: bool,
    /// Whether to collect storage metrics
    pub collect_storage_metrics: bool,
    /// Whether to collect operation metrics
    pub collect_operation_metrics: bool,
    /// Maximum number of metrics to keep in memory
    pub max_metrics_cache: usize,
}

/// Metrics collector that automatically gathers metrics
pub struct MetricsCollector {
    /// Configuration
    config: MetricsCollectorConfig,
    /// Prometheus metrics instance
    prometheus: Arc<PrometheusMetrics>,
    /// Snapshot manager reference
    snapshot_manager: Arc<SnapshotManager>,
    /// Shutdown signal
    shutdown_tx: Option<broadcast::Sender<()>>,
    /// Collection start time
    start_time: Instant,
    /// Cached metrics data
    metrics_cache: Arc<RwLock<MetricsCache>>,
}

/// Cached metrics data
#[derive(Debug, Default)]
struct MetricsCache {
    /// Operation counts by type
    operation_counts: std::collections::HashMap<String, u64>,
    /// Error counts by type
    error_counts: std::collections::HashMap<String, u64>,
    /// Storage statistics
    storage_stats: StorageStats,
    /// System statistics
    system_stats: SystemStats,
    /// Last collection time
    last_collection: Option<Instant>,
}

/// Storage statistics
#[derive(Debug, Default)]
struct StorageStats {
    /// Total snapshots by type
    total_snapshots: std::collections::HashMap<SnapshotType, u64>,
    /// Total storage used in bytes
    total_storage_bytes: u64,
    /// Average compression ratio
    avg_compression_ratio: f64,
    /// Storage backend health
    backend_health: bool,
}

/// System statistics
#[derive(Debug, Default)]
struct SystemStats {
    /// Memory usage in bytes
    memory_usage_bytes: u64,
    /// CPU usage percentage
    cpu_usage_percent: f64,
    /// Number of active operations
    active_operations: u64,
    /// Queue lengths by type
    queue_lengths: std::collections::HashMap<String, u64>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    #[instrument(skip(prometheus, snapshot_manager))]
    pub fn new(
        config: MetricsCollectorConfig,
        prometheus: Arc<PrometheusMetrics>,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> Self {
        info!("Creating new metrics collector with interval: {:?}", config.collection_interval);
        
        Self {
            config,
            prometheus,
            snapshot_manager,
            shutdown_tx: None,
            start_time: Instant::now(),
            metrics_cache: Arc::new(RwLock::new(MetricsCache::default())),
        }
    }

    /// Start the metrics collection loop
    #[instrument(skip(self))]
    pub async fn start(&mut self) -> SnapshotResult<()> {
        info!("Starting metrics collector");
        
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);
        
        let mut ticker = interval(self.config.collection_interval);
        
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.collect_metrics().await {
                        error!("Failed to collect metrics: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Metrics collector received shutdown signal");
                    break;
                }
            }
        }
        
        info!("Metrics collector stopped");
        Ok(())
    }

    /// Stop the metrics collector
    pub async fn stop(&self) {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }
    }

    /// Collect all metrics
    #[instrument(skip(self))]
    async fn collect_metrics(&self) -> SnapshotResult<()> {
        debug!("Collecting metrics");
        
        let collection_start = Instant::now();
        
        // Collect different types of metrics based on configuration
        if self.config.collect_system_metrics {
            self.collect_system_metrics().await?;
        }
        
        if self.config.collect_storage_metrics {
            self.collect_storage_metrics().await?;
        }
        
        if self.config.collect_operation_metrics {
            self.collect_operation_metrics().await?;
        }
        
        // Update cache timestamp
        {
            let mut cache = self.metrics_cache.write().await;
            cache.last_collection = Some(collection_start);
        }
        
        let collection_duration = collection_start.elapsed();
        debug!("Metrics collection completed in {:?}", collection_duration);
        
        Ok(())
    }

    /// Collect system metrics (CPU, memory, etc.)
    #[instrument(skip(self))]
    async fn collect_system_metrics(&self) -> SnapshotResult<()> {
        debug!("Collecting system metrics");
        
        // Get system information
        let (memory_bytes, cpu_percent) = self.get_system_info().await?;
        
        // Update Prometheus metrics
        self.prometheus.update_system_metrics(memory_bytes as f64, cpu_percent);
        
        // Update cache
        {
            let mut cache = self.metrics_cache.write().await;
            cache.system_stats.memory_usage_bytes = memory_bytes;
            cache.system_stats.cpu_usage_percent = cpu_percent;
        }
        
        debug!("System metrics collected: memory={}MB, cpu={:.1}%", memory_bytes / 1_000_000, cpu_percent);
        Ok(())
    }

    /// Collect storage metrics
    #[instrument(skip(self))]
    async fn collect_storage_metrics(&self) -> SnapshotResult<()> {
        debug!("Collecting storage metrics");
        
        // Get storage information from snapshot manager
        let storage_info = self.snapshot_manager.get_storage_info().await?;
        
        // Update storage metrics for each snapshot type
        for (snapshot_type, count) in &storage_info.snapshots_by_type {
            self.prometheus.update_storage_metrics(
                snapshot_type,
                &storage_info.backend_type,
                *count as i64,
                storage_info.total_size_bytes as f64,
                &storage_info.compression_type,
                storage_info.avg_compression_ratio,
            );
        }
        
        // Update cache
        {
            let mut cache = self.metrics_cache.write().await;
            cache.storage_stats.total_snapshots = storage_info.snapshots_by_type.clone();
            cache.storage_stats.total_storage_bytes = storage_info.total_size_bytes;
            cache.storage_stats.avg_compression_ratio = storage_info.avg_compression_ratio;
            cache.storage_stats.backend_health = storage_info.backend_healthy;
        }
        
        debug!("Storage metrics collected: {} total snapshots, {:.2}GB used", 
               storage_info.total_snapshots, storage_info.total_size_bytes as f64 / 1_000_000_000.0);
        Ok(())
    }

    /// Collect operation metrics
    #[instrument(skip(self))]
    async fn collect_operation_metrics(&self) -> SnapshotResult<()> {
        debug!("Collecting operation metrics");
        
        // Get operation information from snapshot manager
        let operation_info = self.snapshot_manager.get_operation_info().await?;
        
        // Update queue lengths
        for (queue_type, length) in &operation_info.queue_lengths {
            self.prometheus.update_queue_length(queue_type, *length as i64);
        }
        
        // Update active operations
        let total_active = operation_info.queue_lengths.values().sum::<u64>();
        
        // Update cache
        {
            let mut cache = self.metrics_cache.write().await;
            cache.system_stats.active_operations = total_active;
            cache.system_stats.queue_lengths = operation_info.queue_lengths.clone();
        }
        
        debug!("Operation metrics collected: {} active operations", total_active);
        Ok(())
    }

    /// Get system information (memory, CPU usage)
    #[instrument(skip(self))]
    async fn get_system_info(&self) -> SnapshotResult<(u64, f64)> {
        // This is a simplified implementation
        // In a real system, you would use system APIs to get actual metrics
        
        // Get memory usage (simplified)
        let memory_bytes = self.get_process_memory().await.unwrap_or(0);
        
        // Get CPU usage (simplified)
        let cpu_percent = self.get_process_cpu().await.unwrap_or(0.0);
        
        Ok((memory_bytes, cpu_percent))
    }

    /// Get process memory usage
    async fn get_process_memory(&self) -> Option<u64> {
        // Simplified implementation - in reality, use procfs or system APIs
        // For now, return a placeholder value
        Some(100 * 1024 * 1024) // 100MB placeholder
    }

    /// Get process CPU usage
    async fn get_process_cpu(&self) -> Option<f64> {
        // Simplified implementation - in reality, calculate actual CPU usage
        // For now, return a placeholder value
        Some(5.0) // 5% placeholder
    }

    /// Record operation start
    #[instrument(skip(self))]
    pub async fn record_operation_start(&self, operation_type: &str, snapshot_type: Option<&SnapshotType>, components: Option<&[ComponentType]>) {
        match operation_type {
            "creation" => {
                if let (Some(st), Some(comps)) = (snapshot_type, components) {
                    self.prometheus.record_snapshot_creation_start(st, comps);
                }
            }
            "restoration" => {
                if let Some(st) = snapshot_type {
                    self.prometheus.record_snapshot_restoration_start(st, "basic");
                }
            }
            _ => {
                debug!("Unknown operation type: {}", operation_type);
            }
        }
        
        // Update cache
        let mut cache = self.metrics_cache.write().await;
        *cache.operation_counts.entry(operation_type.to_string()).or_insert(0) += 1;
    }

    /// Record operation success
    #[instrument(skip(self))]
    pub async fn record_operation_success(&self, operation_type: &str, snapshot_type: Option<&SnapshotType>, components: Option<&[ComponentType]>, duration_secs: f64) {
        match operation_type {
            "creation" => {
                if let (Some(st), Some(comps)) = (snapshot_type, components) {
                    self.prometheus.record_snapshot_creation_success(st, comps, duration_secs);
                }
            }
            "restoration" => {
                if let Some(st) = snapshot_type {
                    self.prometheus.record_snapshot_restoration_success(st, "basic", duration_secs);
                }
            }
            _ => {
                debug!("Unknown operation type: {}", operation_type);
            }
        }
    }

    /// Record operation failure
    #[instrument(skip(self))]
    pub async fn record_operation_failure(&self, operation_type: &str, snapshot_type: Option<&SnapshotType>, components: Option<&[ComponentType]>, error_type: &str) {
        match operation_type {
            "creation" => {
                if let (Some(st), Some(comps)) = (snapshot_type, components) {
                    self.prometheus.record_snapshot_creation_failure(st, comps, error_type);
                }
            }
            "restoration" => {
                if let Some(st) = snapshot_type {
                    self.prometheus.record_snapshot_restoration_failure(st, "basic", error_type);
                }
            }
            _ => {
                debug!("Unknown operation type: {}", operation_type);
            }
        }
        
        // Update cache
        let mut cache = self.metrics_cache.write().await;
        *cache.error_counts.entry(error_type.to_string()).or_insert(0) += 1;
    }

    /// Get cached metrics
    #[instrument(skip(self))]
    pub async fn get_cached_metrics(&self) -> MetricsSummary {
        let cache = self.metrics_cache.read().await;
        
        MetricsSummary {
            total_operations: cache.operation_counts.values().sum(),
            total_errors: cache.error_counts.values().sum(),
            total_snapshots: cache.storage_stats.total_snapshots.values().sum(),
            total_storage_bytes: cache.storage_stats.total_storage_bytes,
            avg_compression_ratio: cache.storage_stats.avg_compression_ratio,
            memory_usage_bytes: cache.system_stats.memory_usage_bytes,
            cpu_usage_percent: cache.system_stats.cpu_usage_percent,
            active_operations: cache.system_stats.active_operations,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            last_collection: cache.last_collection.map(|t| t.elapsed().as_secs()),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &MetricsCollectorConfig {
        &self.config
    }
}

/// Summary of collected metrics
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    /// Total number of operations
    pub total_operations: u64,
    /// Total number of errors
    pub total_errors: u64,
    /// Total number of snapshots
    pub total_snapshots: u64,
    /// Total storage used in bytes
    pub total_storage_bytes: u64,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// CPU usage percentage
    pub cpu_usage_percent: f64,
    /// Number of active operations
    pub active_operations: u64,
    /// System uptime in seconds
    pub uptime_seconds: u64,
    /// Seconds since last collection
    pub last_collection: Option<u64>,
}

/// Storage information from snapshot manager
#[derive(Debug, Clone)]
pub struct StorageInfo {
    /// Total number of snapshots
    pub total_snapshots: u64,
    /// Snapshots by type
    pub snapshots_by_type: std::collections::HashMap<SnapshotType, u64>,
    /// Total storage size in bytes
    pub total_size_bytes: u64,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Compression type used
    pub compression_type: String,
    /// Storage backend type
    pub backend_type: String,
    /// Whether backend is healthy
    pub backend_healthy: bool,
}

/// Operation information from snapshot manager
#[derive(Debug, Clone)]
pub struct OperationInfo {
    /// Queue lengths by operation type
    pub queue_lengths: std::collections::HashMap<String, u64>,
    /// Operation counts by type
    pub operation_counts: std::collections::HashMap<String, u64>,
    /// Error counts by type
    pub error_counts: std::collections::HashMap<String, u64>,
}

impl Default for MetricsCollectorConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(30),
            collect_system_metrics: true,
            collect_storage_metrics: true,
            collect_operation_metrics: true,
            max_metrics_cache: 1000,
        }
    }
}
