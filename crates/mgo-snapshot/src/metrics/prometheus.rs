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
