//! Metrics registry implementation
//! 
//! This module provides centralized metrics registry and management.

use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

use crate::{
    metrics::{prometheus::PrometheusMetrics, collector::MetricsCollector},
    types::error::{SnapshotResult, SnapshotError},
};

/// Central metrics registry
pub struct MetricsRegistry {
    /// Prometheus metrics instance
    prometheus: Arc<PrometheusMetrics>,
    /// Metrics collectors by name
    collectors: Arc<RwLock<HashMap<String, Arc<MetricsCollector>>>>,
    /// Custom metrics by name
    custom_metrics: Arc<RwLock<HashMap<String, CustomMetric>>>,
}

/// Custom metric definition
#[derive(Debug, Clone)]
pub struct CustomMetric {
    /// Metric name
    pub name: String,
    /// Metric type
    pub metric_type: MetricType,
    /// Metric description
    pub description: String,
    /// Metric labels
    pub labels: Vec<String>,
    /// Current value
    pub value: MetricValue,
}

/// Metric type enumeration
#[derive(Debug, Clone)]
pub enum MetricType {
    /// Counter metric (monotonically increasing)
    Counter,
    /// Gauge metric (can increase/decrease)
    Gauge,
    /// Histogram metric (distribution of values)
    Histogram,
    /// Summary metric (quantiles)
    Summary,
}

/// Metric value enumeration
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Integer value
    Int(i64),
    /// Float value
    Float(f64),
    /// Histogram buckets
    Histogram(Vec<f64>),
    /// Summary quantiles
    Summary(HashMap<f64, f64>),
}

impl MetricsRegistry {
    /// Create a new metrics registry
    #[instrument]
    pub fn new() -> SnapshotResult<Self> {
        info!("Creating new metrics registry");
        
        let prometheus = Arc::new(PrometheusMetrics::new()?);
        let collectors = Arc::new(RwLock::new(HashMap::new()));
        let custom_metrics = Arc::new(RwLock::new(HashMap::new()));
        
        Ok(Self {
            prometheus,
            collectors,
            custom_metrics,
        })
    }

    /// Get Prometheus metrics instance
    pub fn prometheus(&self) -> &Arc<PrometheusMetrics> {
        &self.prometheus
    }

    /// Register a metrics collector
    #[instrument(skip(self, collector))]
    pub async fn register_collector(&self, name: String, collector: Arc<MetricsCollector>) -> SnapshotResult<()> {
        info!("Registering metrics collector: {}", name);
        
        let mut collectors = self.collectors.write().await;
        collectors.insert(name.clone(), collector);
        
        debug!("Metrics collector {} registered successfully", name);
        Ok(())
    }

    /// Unregister a metrics collector
    #[instrument(skip(self))]
    pub async fn unregister_collector(&self, name: &str) -> SnapshotResult<()> {
        info!("Unregistering metrics collector: {}", name);
        
        let mut collectors = self.collectors.write().await;
        if collectors.remove(name).is_some() {
            debug!("Metrics collector {} unregistered successfully", name);
            Ok(())
        } else {
            Err(SnapshotError::configuration(format!("Collector {} not found", name)))
        }
    }

    /// Get a metrics collector by name
    #[instrument(skip(self))]
    pub async fn get_collector(&self, name: &str) -> Option<Arc<MetricsCollector>> {
        let collectors = self.collectors.read().await;
        collectors.get(name).cloned()
    }

    /// List all registered collectors
    #[instrument(skip(self))]
    pub async fn list_collectors(&self) -> Vec<String> {
        let collectors = self.collectors.read().await;
        collectors.keys().cloned().collect()
    }

    /// Register a custom metric
    #[instrument(skip(self, metric))]
    pub async fn register_custom_metric(&self, metric: CustomMetric) -> SnapshotResult<()> {
        info!("Registering custom metric: {}", metric.name);
        
        let mut custom_metrics = self.custom_metrics.write().await;
        custom_metrics.insert(metric.name.clone(), metric.clone());
        
        debug!("Custom metric {} registered successfully", metric.name);
        Ok(())
    }

    /// Update a custom metric value
    #[instrument(skip(self, value))]
    pub async fn update_custom_metric(&self, name: &str, value: MetricValue) -> SnapshotResult<()> {
        debug!("Updating custom metric: {}", name);
        
        let mut custom_metrics = self.custom_metrics.write().await;
        if let Some(metric) = custom_metrics.get_mut(name) {
            metric.value = value;
            debug!("Custom metric {} updated successfully", name);
            Ok(())
        } else {
            Err(SnapshotError::configuration(format!("Custom metric {} not found", name)))
        }
    }

    /// Get a custom metric by name
    #[instrument(skip(self))]
    pub async fn get_custom_metric(&self, name: &str) -> Option<CustomMetric> {
        let custom_metrics = self.custom_metrics.read().await;
        custom_metrics.get(name).cloned()
    }

    /// List all custom metrics
    #[instrument(skip(self))]
    pub async fn list_custom_metrics(&self) -> Vec<CustomMetric> {
        let custom_metrics = self.custom_metrics.read().await;
        custom_metrics.values().cloned().collect()
    }

    /// Export all metrics in Prometheus format
    #[instrument(skip(self))]
    pub async fn export_prometheus_metrics(&self) -> SnapshotResult<String> {
        debug!("Exporting Prometheus metrics");
        
        let mut output = self.prometheus.export_metrics()?;
        
        // Add custom metrics to the output
        let custom_metrics = self.custom_metrics.read().await;
        for metric in custom_metrics.values() {
            output.push_str(&self.format_custom_metric_prometheus(metric)?);
        }
        
        debug!("Exported {} bytes of Prometheus metrics", output.len());
        Ok(output)
    }

    /// Format a custom metric in Prometheus format
    fn format_custom_metric_prometheus(&self, metric: &CustomMetric) -> SnapshotResult<String> {
        let mut output = String::new();
        
        // Add metric help
        output.push_str(&format!("# HELP {} {}\n", metric.name, metric.description));
        
        // Add metric type
        let type_str = match metric.metric_type {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
            MetricType::Summary => "summary",
        };
        output.push_str(&format!("# TYPE {} {}\n", metric.name, type_str));
        
        // Add metric value
        match &metric.value {
            MetricValue::Int(value) => {
                output.push_str(&format!("{} {}\n", metric.name, value));
            }
            MetricValue::Float(value) => {
                output.push_str(&format!("{} {}\n", metric.name, value));
            }
            MetricValue::Histogram(buckets) => {
                for (i, bucket) in buckets.iter().enumerate() {
                    output.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", metric.name, bucket, i + 1));
                }
                output.push_str(&format!("{}_count {}\n", metric.name, buckets.len()));
                output.push_str(&format!("{}_sum {}\n", metric.name, buckets.iter().sum::<f64>()));
            }
            MetricValue::Summary(quantiles) => {
                for (quantile, value) in quantiles {
                    output.push_str(&format!("{}{{quantile=\"{}\"}} {}\n", metric.name, quantile, value));
                }
                output.push_str(&format!("{}_count {}\n", metric.name, quantiles.len()));
                output.push_str(&format!("{}_sum {}\n", metric.name, quantiles.values().sum::<f64>()));
            }
        }
        
        Ok(output)
    }

    /// Get registry statistics
    #[instrument(skip(self))]
    pub async fn get_registry_stats(&self) -> RegistryStats {
        let collectors = self.collectors.read().await;
        let custom_metrics = self.custom_metrics.read().await;
        
        RegistryStats {
            total_collectors: collectors.len(),
            total_custom_metrics: custom_metrics.len(),
            collector_names: collectors.keys().cloned().collect(),
            custom_metric_names: custom_metrics.keys().cloned().collect(),
        }
    }

    /// Clear all custom metrics
    #[instrument(skip(self))]
    pub async fn clear_custom_metrics(&self) {
        info!("Clearing all custom metrics");
        
        let mut custom_metrics = self.custom_metrics.write().await;
        custom_metrics.clear();
        
        debug!("All custom metrics cleared");
    }

    /// Stop all collectors
    #[instrument(skip(self))]
    pub async fn stop_all_collectors(&self) {
        info!("Stopping all metrics collectors");
        
        let collectors = self.collectors.read().await;
        for (name, collector) in collectors.iter() {
            debug!("Stopping collector: {}", name);
            collector.stop().await;
        }
        
        debug!("All collectors stopped");
    }
}

/// Registry statistics
#[derive(Debug, Clone)]
pub struct RegistryStats {
    /// Total number of registered collectors
    pub total_collectors: usize,
    /// Total number of custom metrics
    pub total_custom_metrics: usize,
    /// Names of registered collectors
    pub collector_names: Vec<String>,
    /// Names of custom metrics
    pub custom_metric_names: Vec<String>,
}

impl CustomMetric {
    /// Create a new counter metric
    pub fn counter(name: String, description: String, labels: Vec<String>) -> Self {
        Self {
            name,
            metric_type: MetricType::Counter,
            description,
            labels,
            value: MetricValue::Int(0),
        }
    }

    /// Create a new gauge metric
    pub fn gauge(name: String, description: String, labels: Vec<String>) -> Self {
        Self {
            name,
            metric_type: MetricType::Gauge,
            description,
            labels,
            value: MetricValue::Float(0.0),
        }
    }

    /// Create a new histogram metric
    pub fn histogram(name: String, description: String, labels: Vec<String>, buckets: Vec<f64>) -> Self {
        Self {
            name,
            metric_type: MetricType::Histogram,
            description,
            labels,
            value: MetricValue::Histogram(buckets),
        }
    }

    /// Create a new summary metric
    pub fn summary(name: String, description: String, labels: Vec<String>, quantiles: HashMap<f64, f64>) -> Self {
        Self {
            name,
            metric_type: MetricType::Summary,
            description,
            labels,
            value: MetricValue::Summary(quantiles),
        }
    }

    /// Update metric value
    pub fn update_value(&mut self, value: MetricValue) {
        self.value = value;
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("Failed to create default MetricsRegistry")
    }
}
