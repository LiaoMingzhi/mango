//! Prometheus metrics implementation
//! 
//! This module provides Prometheus metrics collection and export functionality.

use prometheus::{
    Gauge, GaugeVec, HistogramOpts, HistogramVec,
    IntCounterVec, IntGaugeVec, Opts, Registry,
};
use std::{sync::Arc, time::Instant};
use tracing::{debug, instrument};

use crate::types::{SnapshotType, ComponentType, error::SnapshotResult};

/// Prometheus metrics for snapshot operations
pub struct PrometheusMetrics {
    /// Prometheus registry
    registry: Arc<Registry>,
    
    // Snapshot creation metrics
    /// Total number of snapshot creation requests
    pub snapshot_creation_total: IntCounterVec,
    /// Number of successful snapshot creations
    pub snapshot_creation_success: IntCounterVec,
    /// Number of failed snapshot creations
    pub snapshot_creation_failures: IntCounterVec,
    /// Duration of snapshot creation operations
    pub snapshot_creation_duration: HistogramVec,
    
    // Snapshot restoration metrics
    /// Total number of snapshot restoration requests
    pub snapshot_restoration_total: IntCounterVec,
    /// Number of successful snapshot restorations
    pub snapshot_restoration_success: IntCounterVec,
    /// Number of failed snapshot restorations
    pub snapshot_restoration_failures: IntCounterVec,
    /// Duration of snapshot restoration operations
    pub snapshot_restoration_duration: HistogramVec,
    
    // Storage metrics
    /// Total number of snapshots stored
    pub snapshots_stored_total: IntGaugeVec,
    /// Total storage space used by snapshots
    pub storage_space_used_bytes: GaugeVec,
    /// Average compression ratio
    pub compression_ratio: GaugeVec,
    
    // Validation metrics
    /// Total number of validation operations
    pub validation_total: IntCounterVec,
    /// Number of successful validations
    pub validation_success: IntCounterVec,
    /// Number of failed validations
    pub validation_failures: IntCounterVec,
    /// Duration of validation operations
    pub validation_duration: HistogramVec,
    
    // System metrics
    /// Current number of active operations
    pub active_operations: IntGaugeVec,
    /// System uptime
    pub system_uptime_seconds: Gauge,
    /// Memory usage
    pub memory_usage_bytes: Gauge,
    /// CPU usage percentage
    pub cpu_usage_percent: Gauge,
    
    // Error metrics
    /// Total number of errors by type
    pub errors_total: IntCounterVec,
    /// Rate of errors
    pub error_rate: GaugeVec,
    
    // Performance metrics
    /// Throughput in operations per second
    pub throughput_ops_per_second: GaugeVec,
    /// Queue length for pending operations
    pub queue_length: IntGaugeVec,
    
    // Enhanced Performance Metrics
    /// Memory usage by component (bytes)
    pub memory_usage_by_component: GaugeVec,
    /// I/O operations per second
    pub io_ops_per_second: GaugeVec,
    /// Network bytes transferred
    pub network_bytes_total: IntCounterVec,
    /// Database connection pool metrics
    pub db_connection_pool_size: IntGaugeVec,
    /// Cache hit ratio
    pub cache_hit_ratio: GaugeVec,
    /// Background task queue metrics
    pub background_task_queue_depth: IntGaugeVec,
    /// Component health status (0=unhealthy, 1=healthy)
    pub component_health_status: IntGaugeVec,
    
    // Advanced Snapshot Metrics
    /// Snapshot size distribution (histogram)
    pub snapshot_size_distribution: HistogramVec,
    /// Compression efficiency by algorithm
    pub compression_efficiency: GaugeVec,
    /// Data deduplication ratio
    pub deduplication_ratio: GaugeVec,
    /// State collection latency by component
    pub state_collection_latency: HistogramVec,
    /// Incremental snapshot delta size
    pub incremental_delta_size: HistogramVec,
    
    // Resource Utilization Metrics
    /// Disk usage by type (snapshots, logs, temp)
    pub disk_usage_by_type: GaugeVec,
    /// CPU usage by operation type
    pub cpu_usage_by_operation: GaugeVec,
    /// Thread pool utilization
    pub thread_pool_utilization: GaugeVec,
    
    /// Start time for uptime calculation
    start_time: Instant,
}

impl PrometheusMetrics {
    /// Create new Prometheus metrics instance
    #[instrument]
    pub fn new() -> SnapshotResult<Self> {
        let registry = Arc::new(Registry::new());
        let start_time = Instant::now();
        
        debug!("Initializing Prometheus metrics");
        
        // Snapshot creation metrics
        let snapshot_creation_total = IntCounterVec::new(
            Opts::new("snapshot_creation_total", "Total number of snapshot creation requests"),
            &["snapshot_type", "components"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_creation_total metric: {}", e)))?;
        
        let snapshot_creation_success = IntCounterVec::new(
            Opts::new("snapshot_creation_success_total", "Number of successful snapshot creations"),
            &["snapshot_type", "components"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_creation_success metric: {}", e)))?;
        
        let snapshot_creation_failures = IntCounterVec::new(
            Opts::new("snapshot_creation_failures_total", "Number of failed snapshot creations"),
            &["snapshot_type", "components", "error_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_creation_failures metric: {}", e)))?;
        
        let snapshot_creation_duration = HistogramVec::new(
            HistogramOpts::new("snapshot_creation_duration_seconds", "Duration of snapshot creation operations"),
            &["snapshot_type", "components"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_creation_duration metric: {}", e)))?;
        
        // Snapshot restoration metrics
        let snapshot_restoration_total = IntCounterVec::new(
            Opts::new("snapshot_restoration_total", "Total number of snapshot restoration requests"),
            &["snapshot_type", "validation_level"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_restoration_total metric: {}", e)))?;
        
        let snapshot_restoration_success = IntCounterVec::new(
            Opts::new("snapshot_restoration_success_total", "Number of successful snapshot restorations"),
            &["snapshot_type", "validation_level"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_restoration_success metric: {}", e)))?;
        
        let snapshot_restoration_failures = IntCounterVec::new(
            Opts::new("snapshot_restoration_failures_total", "Number of failed snapshot restorations"),
            &["snapshot_type", "validation_level", "error_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_restoration_failures metric: {}", e)))?;
        
        let snapshot_restoration_duration = HistogramVec::new(
            HistogramOpts::new("snapshot_restoration_duration_seconds", "Duration of snapshot restoration operations"),
            &["snapshot_type", "validation_level"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_restoration_duration metric: {}", e)))?;
        
        // Storage metrics
        let snapshots_stored_total = IntGaugeVec::new(
            Opts::new("snapshots_stored_total", "Total number of snapshots stored"),
            &["snapshot_type", "storage_backend"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshots_stored_total metric: {}", e)))?;
        
        let storage_space_used_bytes = GaugeVec::new(
            Opts::new("storage_space_used_bytes", "Total storage space used by snapshots"),
            &["storage_backend", "compression_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create storage_space_used_bytes metric: {}", e)))?;
        
        let compression_ratio = GaugeVec::new(
            Opts::new("compression_ratio", "Average compression ratio"),
            &["compression_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create compression_ratio metric: {}", e)))?;
        
        // Validation metrics
        let validation_total = IntCounterVec::new(
            Opts::new("validation_total", "Total number of validation operations"),
            &["validation_level", "snapshot_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create validation_total metric: {}", e)))?;
        
        let validation_success = IntCounterVec::new(
            Opts::new("validation_success_total", "Number of successful validations"),
            &["validation_level", "snapshot_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create validation_success metric: {}", e)))?;
        
        let validation_failures = IntCounterVec::new(
            Opts::new("validation_failures_total", "Number of failed validations"),
            &["validation_level", "snapshot_type", "failure_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create validation_failures metric: {}", e)))?;
        
        let validation_duration = HistogramVec::new(
            HistogramOpts::new("validation_duration_seconds", "Duration of validation operations"),
            &["validation_level", "snapshot_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create validation_duration metric: {}", e)))?;
        
        // System metrics
        let active_operations = IntGaugeVec::new(
            Opts::new("active_operations", "Current number of active operations"),
            &["operation_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create active_operations metric: {}", e)))?;
        
        let system_uptime_seconds = Gauge::new(
            "system_uptime_seconds", "System uptime in seconds"
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create system_uptime_seconds metric: {}", e)))?;
        
        let memory_usage_bytes = Gauge::new(
            "memory_usage_bytes", "Memory usage in bytes"
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create memory_usage_bytes metric: {}", e)))?;
        
        let cpu_usage_percent = Gauge::new(
            "cpu_usage_percent", "CPU usage percentage"
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create cpu_usage_percent metric: {}", e)))?;
        
        // Error metrics
        let errors_total = IntCounterVec::new(
            Opts::new("errors_total", "Total number of errors by type"),
            &["error_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create errors_total metric: {}", e)))?;
        
        let error_rate = GaugeVec::new(
            Opts::new("error_rate", "Rate of errors"),
            &["error_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create error_rate metric: {}", e)))?;
        
        // Performance metrics
        let throughput_ops_per_second = GaugeVec::new(
            Opts::new("throughput_ops_per_second", "Throughput in operations per second"),
            &["operation_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create throughput_ops_per_second metric: {}", e)))?;
        
        let queue_length = IntGaugeVec::new(
            Opts::new("queue_length", "Queue length for pending operations"),
            &["queue_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create queue_length metric: {}", e)))?;

        // Enhanced Performance Metrics
        let memory_usage_by_component = GaugeVec::new(
            Opts::new("memory_usage_by_component_bytes", "Memory usage by component in bytes"),
            &["component", "memory_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create memory_usage_by_component metric: {}", e)))?;

        let io_ops_per_second = GaugeVec::new(
            Opts::new("io_ops_per_second", "I/O operations per second"),
            &["operation_type", "device"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create io_ops_per_second metric: {}", e)))?;

        let network_bytes_total = IntCounterVec::new(
            Opts::new("network_bytes_total", "Total network bytes transferred"),
            &["direction", "protocol"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create network_bytes_total metric: {}", e)))?;

        let db_connection_pool_size = IntGaugeVec::new(
            Opts::new("db_connection_pool_size", "Database connection pool size"),
            &["pool_name", "status"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create db_connection_pool_size metric: {}", e)))?;

        let cache_hit_ratio = GaugeVec::new(
            Opts::new("cache_hit_ratio", "Cache hit ratio (0.0 to 1.0)"),
            &["cache_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create cache_hit_ratio metric: {}", e)))?;

        let background_task_queue_depth = IntGaugeVec::new(
            Opts::new("background_task_queue_depth", "Background task queue depth"),
            &["task_type", "priority"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create background_task_queue_depth metric: {}", e)))?;

        let component_health_status = IntGaugeVec::new(
            Opts::new("component_health_status", "Component health status (0=unhealthy, 1=healthy)"),
            &["component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create component_health_status metric: {}", e)))?;

        // Advanced Snapshot Metrics
        let snapshot_size_distribution = HistogramVec::new(
            HistogramOpts::new("snapshot_size_distribution_bytes", "Snapshot size distribution in bytes"),
            &["snapshot_type", "compression"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create snapshot_size_distribution metric: {}", e)))?;

        let compression_efficiency = GaugeVec::new(
            Opts::new("compression_efficiency_ratio", "Compression efficiency ratio"),
            &["algorithm", "data_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create compression_efficiency metric: {}", e)))?;

        let deduplication_ratio = GaugeVec::new(
            Opts::new("deduplication_ratio", "Data deduplication ratio"),
            &["data_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create deduplication_ratio metric: {}", e)))?;

        let state_collection_latency = HistogramVec::new(
            HistogramOpts::new("state_collection_latency_seconds", "State collection latency by component"),
            &["component", "collection_type"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create state_collection_latency metric: {}", e)))?;

        let incremental_delta_size = HistogramVec::new(
            HistogramOpts::new("incremental_delta_size_bytes", "Incremental snapshot delta size in bytes"),
            &["snapshot_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create incremental_delta_size metric: {}", e)))?;

        // Resource Utilization Metrics
        let disk_usage_by_type = GaugeVec::new(
            Opts::new("disk_usage_by_type_bytes", "Disk usage by type in bytes"),
            &["usage_type", "mount_point"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create disk_usage_by_type metric: {}", e)))?;

        let cpu_usage_by_operation = GaugeVec::new(
            Opts::new("cpu_usage_by_operation_percent", "CPU usage by operation type"),
            &["operation_type", "component"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create cpu_usage_by_operation metric: {}", e)))?;

        let thread_pool_utilization = GaugeVec::new(
            Opts::new("thread_pool_utilization_ratio", "Thread pool utilization ratio"),
            &["pool_name", "status"],
        ).map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to create thread_pool_utilization metric: {}", e)))?;
        
        // Register all metrics
        registry.register(Box::new(snapshot_creation_total.clone()))?;
        registry.register(Box::new(snapshot_creation_success.clone()))?;
        registry.register(Box::new(snapshot_creation_failures.clone()))?;
        registry.register(Box::new(snapshot_creation_duration.clone()))?;
        
        registry.register(Box::new(snapshot_restoration_total.clone()))?;
        registry.register(Box::new(snapshot_restoration_success.clone()))?;
        registry.register(Box::new(snapshot_restoration_failures.clone()))?;
        registry.register(Box::new(snapshot_restoration_duration.clone()))?;
        
        registry.register(Box::new(snapshots_stored_total.clone()))?;
        registry.register(Box::new(storage_space_used_bytes.clone()))?;
        registry.register(Box::new(compression_ratio.clone()))?;
        
        registry.register(Box::new(validation_total.clone()))?;
        registry.register(Box::new(validation_success.clone()))?;
        registry.register(Box::new(validation_failures.clone()))?;
        registry.register(Box::new(validation_duration.clone()))?;
        
        registry.register(Box::new(active_operations.clone()))?;
        registry.register(Box::new(system_uptime_seconds.clone()))?;
        registry.register(Box::new(memory_usage_bytes.clone()))?;
        registry.register(Box::new(cpu_usage_percent.clone()))?;
        
        registry.register(Box::new(errors_total.clone()))?;
        registry.register(Box::new(error_rate.clone()))?;
        
        registry.register(Box::new(throughput_ops_per_second.clone()))?;
        registry.register(Box::new(queue_length.clone()))?;
        
        // Register enhanced metrics
        registry.register(Box::new(memory_usage_by_component.clone()))?;
        registry.register(Box::new(io_ops_per_second.clone()))?;
        registry.register(Box::new(network_bytes_total.clone()))?;
        registry.register(Box::new(db_connection_pool_size.clone()))?;
        registry.register(Box::new(cache_hit_ratio.clone()))?;
        registry.register(Box::new(background_task_queue_depth.clone()))?;
        registry.register(Box::new(component_health_status.clone()))?;
        registry.register(Box::new(snapshot_size_distribution.clone()))?;
        registry.register(Box::new(compression_efficiency.clone()))?;
        registry.register(Box::new(deduplication_ratio.clone()))?;
        registry.register(Box::new(state_collection_latency.clone()))?;
        registry.register(Box::new(incremental_delta_size.clone()))?;
        registry.register(Box::new(disk_usage_by_type.clone()))?;
        registry.register(Box::new(cpu_usage_by_operation.clone()))?;
        registry.register(Box::new(thread_pool_utilization.clone()))?;
        
        debug!("Prometheus metrics initialized successfully");
        
        Ok(Self {
            registry,
            snapshot_creation_total,
            snapshot_creation_success,
            snapshot_creation_failures,
            snapshot_creation_duration,
            snapshot_restoration_total,
            snapshot_restoration_success,
            snapshot_restoration_failures,
            snapshot_restoration_duration,
            snapshots_stored_total,
            storage_space_used_bytes,
            compression_ratio,
            validation_total,
            validation_success,
            validation_failures,
            validation_duration,
            active_operations,
            system_uptime_seconds,
            memory_usage_bytes,
            cpu_usage_percent,
            errors_total,
            error_rate,
            throughput_ops_per_second,
            queue_length,
            memory_usage_by_component,
            io_ops_per_second,
            network_bytes_total,
            db_connection_pool_size,
            cache_hit_ratio,
            background_task_queue_depth,
            component_health_status,
            snapshot_size_distribution,
            compression_efficiency,
            deduplication_ratio,
            state_collection_latency,
            incremental_delta_size,
            disk_usage_by_type,
            cpu_usage_by_operation,
            thread_pool_utilization,
            start_time,
        })
    }
    
    /// Record snapshot creation start
    #[instrument(skip(self))]
    pub fn record_snapshot_creation_start(&self, snapshot_type: &SnapshotType, components: &[ComponentType]) {
        let type_str = format!("{:?}", snapshot_type);
        let components_str = components.iter().map(|c| format!("{:?}", c)).collect::<Vec<_>>().join(",");
        
        self.snapshot_creation_total.with_label_values(&[&type_str, &components_str]).inc();
        self.active_operations.with_label_values(&["creation"]).inc();
        
        debug!("Recorded snapshot creation start for type: {}, components: {}", type_str, components_str);
    }
    
    /// Record snapshot creation success
    #[instrument(skip(self))]
    pub fn record_snapshot_creation_success(&self, snapshot_type: &SnapshotType, components: &[ComponentType], duration_secs: f64) {
        let type_str = format!("{:?}", snapshot_type);
        let components_str = components.iter().map(|c| format!("{:?}", c)).collect::<Vec<_>>().join(",");
        
        self.snapshot_creation_success.with_label_values(&[&type_str, &components_str]).inc();
        self.snapshot_creation_duration.with_label_values(&[&type_str, &components_str]).observe(duration_secs);
        self.active_operations.with_label_values(&["creation"]).dec();
        
        debug!("Recorded snapshot creation success for type: {}, duration: {}s", type_str, duration_secs);
    }
    
    /// Record snapshot creation failure
    #[instrument(skip(self))]
    pub fn record_snapshot_creation_failure(&self, snapshot_type: &SnapshotType, components: &[ComponentType], error_type: &str) {
        let type_str = format!("{:?}", snapshot_type);
        let components_str = components.iter().map(|c| format!("{:?}", c)).collect::<Vec<_>>().join(",");
        
        self.snapshot_creation_failures.with_label_values(&[&type_str, &components_str, error_type]).inc();
        self.active_operations.with_label_values(&["creation"]).dec();
        self.errors_total.with_label_values(&[error_type, "creation"]).inc();
        
        debug!("Recorded snapshot creation failure for type: {}, error: {}", type_str, error_type);
    }
    
    /// Record snapshot restoration start
    #[instrument(skip(self))]
    pub fn record_snapshot_restoration_start(&self, snapshot_type: &SnapshotType, validation_level: &str) {
        let type_str = format!("{:?}", snapshot_type);
        
        self.snapshot_restoration_total.with_label_values(&[&type_str, validation_level]).inc();
        self.active_operations.with_label_values(&["restoration"]).inc();
        
        debug!("Recorded snapshot restoration start for type: {}, validation: {}", type_str, validation_level);
    }
    
    /// Record snapshot restoration success
    #[instrument(skip(self))]
    pub fn record_snapshot_restoration_success(&self, snapshot_type: &SnapshotType, validation_level: &str, duration_secs: f64) {
        let type_str = format!("{:?}", snapshot_type);
        
        self.snapshot_restoration_success.with_label_values(&[&type_str, validation_level]).inc();
        self.snapshot_restoration_duration.with_label_values(&[&type_str, validation_level]).observe(duration_secs);
        self.active_operations.with_label_values(&["restoration"]).dec();
        
        debug!("Recorded snapshot restoration success for type: {}, duration: {}s", type_str, duration_secs);
    }
    
    /// Record snapshot restoration failure
    #[instrument(skip(self))]
    pub fn record_snapshot_restoration_failure(&self, snapshot_type: &SnapshotType, validation_level: &str, error_type: &str) {
        let type_str = format!("{:?}", snapshot_type);
        
        self.snapshot_restoration_failures.with_label_values(&[&type_str, validation_level, error_type]).inc();
        self.active_operations.with_label_values(&["restoration"]).dec();
        self.errors_total.with_label_values(&[error_type, "restoration"]).inc();
        
        debug!("Recorded snapshot restoration failure for type: {}, error: {}", type_str, error_type);
    }
    
    /// Update storage metrics
    #[instrument(skip(self))]
    pub fn update_storage_metrics(&self, snapshot_type: &SnapshotType, storage_backend: &str, total_snapshots: i64, total_size_bytes: f64, compression_type: &str, avg_compression_ratio: f64) {
        let type_str = format!("{:?}", snapshot_type);
        
        self.snapshots_stored_total.with_label_values(&[&type_str, storage_backend]).set(total_snapshots);
        self.storage_space_used_bytes.with_label_values(&[storage_backend, compression_type]).set(total_size_bytes);
        self.compression_ratio.with_label_values(&[compression_type]).set(avg_compression_ratio);
        
        debug!("Updated storage metrics: {} snapshots, {:.2}GB, {:.2}x compression", total_snapshots, total_size_bytes / 1_000_000_000.0, avg_compression_ratio);
    }
    
    /// Update system metrics
    #[instrument(skip(self))]
    pub fn update_system_metrics(&self, memory_bytes: f64, cpu_percent: f64) {
        let uptime_secs = self.start_time.elapsed().as_secs_f64();
        
        self.system_uptime_seconds.set(uptime_secs);
        self.memory_usage_bytes.set(memory_bytes);
        self.cpu_usage_percent.set(cpu_percent);
        
        debug!("Updated system metrics: uptime: {:.1}s, memory: {:.2}MB, CPU: {:.1}%", uptime_secs, memory_bytes / 1_000_000.0, cpu_percent);
    }
    
    /// Update queue length
    #[instrument(skip(self))]
    pub fn update_queue_length(&self, queue_type: &str, length: i64) {
        self.queue_length.with_label_values(&[queue_type]).set(length);
        debug!("Updated {} queue length: {}", queue_type, length);
    }
    
    /// Update enhanced performance metrics
    #[instrument(skip(self))]
    pub fn update_component_memory_usage(&self, component: &str, memory_type: &str, bytes: f64) {
        self.memory_usage_by_component
            .with_label_values(&[component, memory_type])
            .set(bytes);
        debug!("Updated {} {} memory usage: {:.2}MB", component, memory_type, bytes / 1_000_000.0);
    }

    /// Update I/O operations per second
    #[instrument(skip(self))]
    pub fn update_io_ops_per_second(&self, operation_type: &str, device: &str, ops: f64) {
        self.io_ops_per_second
            .with_label_values(&[operation_type, device])
            .set(ops);
        debug!("Updated {} I/O ops on {}: {:.2}/s", operation_type, device, ops);
    }

    /// Update network bytes transferred
    #[instrument(skip(self))]
    pub fn record_network_bytes(&self, direction: &str, protocol: &str, bytes: u64) {
        self.network_bytes_total
            .with_label_values(&[direction, protocol])
            .inc_by(bytes);
        debug!("Recorded {} {} network bytes: {:.2}KB", direction, protocol, bytes as f64 / 1024.0);
    }

    /// Update cache hit ratio
    #[instrument(skip(self))]
    pub fn update_cache_hit_ratio(&self, cache_type: &str, component: &str, ratio: f64) {
        self.cache_hit_ratio
            .with_label_values(&[cache_type, component])
            .set(ratio);
        debug!("Updated {} cache hit ratio for {}: {:.2}%", cache_type, component, ratio * 100.0);
    }

    /// Update component health status
    #[instrument(skip(self))]
    pub fn update_component_health(&self, component: &str, is_healthy: bool) {
        self.component_health_status
            .with_label_values(&[component])
            .set(if is_healthy { 1 } else { 0 });
        debug!("Updated {} health status: {}", component, if is_healthy { "healthy" } else { "unhealthy" });
    }

    /// Record snapshot size distribution
    #[instrument(skip(self))]
    pub fn record_snapshot_size(&self, snapshot_type: &str, compression: &str, size_bytes: f64) {
        self.snapshot_size_distribution
            .with_label_values(&[snapshot_type, compression])
            .observe(size_bytes);
        debug!("Recorded {} {} snapshot size: {:.2}MB", snapshot_type, compression, size_bytes / 1_000_000.0);
    }

    /// Update compression efficiency
    #[instrument(skip(self))]
    pub fn update_compression_efficiency(&self, algorithm: &str, data_type: &str, efficiency: f64) {
        self.compression_efficiency
            .with_label_values(&[algorithm, data_type])
            .set(efficiency);
        debug!("Updated {} compression efficiency for {}: {:.2}%", algorithm, data_type, efficiency * 100.0);
    }

    /// Record state collection latency
    #[instrument(skip(self))]
    pub fn record_state_collection_latency(&self, component: &str, collection_type: &str, duration_seconds: f64) {
        self.state_collection_latency
            .with_label_values(&[component, collection_type])
            .observe(duration_seconds);
        debug!("Recorded {} {} collection latency: {:.2}ms", component, collection_type, duration_seconds * 1000.0);
    }

    /// Update disk usage by type
    #[instrument(skip(self))]
    pub fn update_disk_usage(&self, usage_type: &str, mount_point: &str, bytes: f64) {
        self.disk_usage_by_type
            .with_label_values(&[usage_type, mount_point])
            .set(bytes);
        debug!("Updated {} disk usage on {}: {:.2}GB", usage_type, mount_point, bytes / 1_000_000_000.0);
    }

    /// Record snapshot operation
    #[instrument(skip(self))]
    pub fn record_snapshot_operation(&self, operation_type: &str) {
        self.snapshot_creation_total
            .with_label_values(&[operation_type])
            .inc();
        debug!("Recorded snapshot operation: {}", operation_type);
    }

    /// Record snapshot duration
    #[instrument(skip(self))]
    pub fn record_snapshot_duration(&self, duration_seconds: f64) {
        self.snapshot_creation_duration
            .with_label_values(&["smart_scheduled"])
            .observe(duration_seconds);
        debug!("Recorded snapshot duration: {:.2}s", duration_seconds);
    }

    /// Record error
    #[instrument(skip(self))]
    pub fn record_error(&self, error_type: &str) {
        self.errors_total
            .with_label_values(&[error_type])
            .inc();
        debug!("Recorded error: {}", error_type);
    }

    /// Export metrics in Prometheus format
    #[instrument(skip(self))]
    pub fn export_metrics(&self) -> SnapshotResult<String> {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        
        encoder.encode_to_string(&metric_families)
            .map_err(|e| crate::types::error::SnapshotError::configuration(format!("Failed to encode metrics: {}", e)))
    }
    
    /// Get registry for custom metrics
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to create default PrometheusMetrics")
    }
}
