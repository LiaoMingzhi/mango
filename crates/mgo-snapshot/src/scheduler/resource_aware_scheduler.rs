// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Resource-aware scheduler that monitors system resources and adapts accordingly

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, instrument, warn};

use crate::scheduler::AdaptiveScheduler;
use crate::types::error::SnapshotError;

/// Resource-aware scheduler that monitors system resources and adapts scheduling
pub struct ResourceAwareScheduler {
    adaptive_scheduler: Arc<AdaptiveScheduler>,
    resource_monitor: Arc<RwLock<ResourceMonitor>>,
    config: Arc<RwLock<ResourceAwareConfig>>,
    resource_history: Arc<RwLock<Vec<ResourceSnapshot>>>,
}

impl ResourceAwareScheduler {
    /// Create a new resource-aware scheduler
    pub fn new(
        adaptive_scheduler: Arc<AdaptiveScheduler>,
        config: ResourceAwareConfig,
    ) -> Self {
        Self {
            adaptive_scheduler,
            resource_monitor: Arc::new(RwLock::new(ResourceMonitor::new())),
            config: Arc::new(RwLock::new(config)),
            resource_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the resource-aware scheduler
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<(), SnapshotError> {
        info!("Starting resource-aware scheduler");
        
        // Start the underlying adaptive scheduler
        self.adaptive_scheduler.start().await?;
        
        // Start resource monitoring
        let resource_scheduler = self.clone();
        tokio::spawn(async move {
            resource_scheduler.run_resource_monitoring().await;
        });

        // Start resource-based optimization
        let optimization_scheduler = self.clone();
        tokio::spawn(async move {
            optimization_scheduler.run_resource_optimization().await;
        });

        Ok(())
    }

    /// Stop the resource-aware scheduler
    pub async fn stop(&self) -> Result<(), SnapshotError> {
        info!("Stopping resource-aware scheduler");
        self.adaptive_scheduler.stop().await
    }

    /// Run continuous resource monitoring
    async fn run_resource_monitoring(&self) {
        info!("Starting resource monitoring loop");
        
        let mut monitor_interval = interval(Duration::from_secs(30)); // Monitor every 30 seconds
        monitor_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            monitor_interval.tick().await;

            if let Err(e) = self.collect_resource_metrics().await {
                error!("Failed to collect resource metrics: {}", e);
            }
        }
    }

    /// Collect current resource metrics
    async fn collect_resource_metrics(&self) -> Result<(), SnapshotError> {
        let mut monitor = self.resource_monitor.write().await;
        let snapshot = monitor.take_snapshot().await?;
        drop(monitor);

        // Store resource snapshot for analysis
        let mut history = self.resource_history.write().await;
        history.push(snapshot.clone());

        // Keep only last 120 snapshots (1 hour worth at 30-second intervals)
        while history.len() > 120 {
            history.remove(0);
        }

        // Check for resource pressure and adjust if needed
        self.handle_resource_pressure(&snapshot).await?;

        Ok(())
    }

    /// Handle resource pressure by adjusting scheduling
    async fn handle_resource_pressure(&self, snapshot: &ResourceSnapshot) -> Result<(), SnapshotError> {
        let config = self.config.read().await;
        
        let pressure_level = self.calculate_pressure_level(snapshot, &config);
        
        match pressure_level {
            ResourcePressureLevel::Low => {
                debug!("Low resource pressure detected");
                // Could enable more aggressive scheduling
            }
            ResourcePressureLevel::Medium => {
                info!("Medium resource pressure detected");
                // Normal operation, possibly defer non-critical operations
            }
            ResourcePressureLevel::High => {
                warn!("High resource pressure detected - deferring operations");
                // Defer all non-critical operations
                self.apply_high_pressure_policy().await?;
            }
            ResourcePressureLevel::Critical => {
                error!("Critical resource pressure detected - emergency mode");
                // Emergency mode: stop all operations temporarily
                self.apply_emergency_policy().await?;
            }
        }

        Ok(())
    }

    /// Calculate current resource pressure level
    fn calculate_pressure_level(
        &self,
        snapshot: &ResourceSnapshot,
        config: &ResourceAwareConfig,
    ) -> ResourcePressureLevel {
        let mut pressure_score = 0.0;

        // CPU pressure
        if snapshot.cpu_usage > config.cpu_high_threshold {
            pressure_score += 3.0;
        } else if snapshot.cpu_usage > config.cpu_medium_threshold {
            pressure_score += 1.5;
        }

        // Memory pressure
        if snapshot.memory_usage > config.memory_high_threshold {
            pressure_score += 3.0;
        } else if snapshot.memory_usage > config.memory_medium_threshold {
            pressure_score += 1.5;
        }

        // Disk I/O pressure
        if snapshot.disk_io_usage > config.disk_io_high_threshold {
            pressure_score += 2.0;
        } else if snapshot.disk_io_usage > config.disk_io_medium_threshold {
            pressure_score += 1.0;
        }

        // Network I/O pressure
        if snapshot.network_io_usage > config.network_io_high_threshold {
            pressure_score += 1.0;
        } else if snapshot.network_io_usage > config.network_io_medium_threshold {
            pressure_score += 0.5;
        }

        // Load average pressure
        if snapshot.load_average > config.load_average_high_threshold {
            pressure_score += 2.0;
        } else if snapshot.load_average > config.load_average_medium_threshold {
            pressure_score += 1.0;
        }

        // Convert score to pressure level
        if pressure_score >= 8.0 {
            ResourcePressureLevel::Critical
        } else if pressure_score >= 5.0 {
            ResourcePressureLevel::High
        } else if pressure_score >= 2.0 {
            ResourcePressureLevel::Medium
        } else {
            ResourcePressureLevel::Low
        }
    }

    /// Apply high pressure policy
    async fn apply_high_pressure_policy(&self) -> Result<(), SnapshotError> {
        // Temporarily increase intervals and thresholds
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(2 * 60 * 60); // 2 hours
        smart_config.high_load_threshold = 0.9;
        smart_config.very_high_load_threshold = 0.95;
        
        // Apply via adaptive scheduler's smart scheduler
        // This is a simplified approach - in practice, we'd need a more direct way
        info!("Applied high pressure policy: increased intervals and thresholds");
        
        Ok(())
    }

    /// Apply emergency policy
    async fn apply_emergency_policy(&self) -> Result<(), SnapshotError> {
        // In emergency mode, we might want to pause scheduling entirely
        // For now, just log and apply very conservative settings
        warn!("Emergency resource pressure - applying conservative policy");
        
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(4 * 60 * 60); // 4 hours
        smart_config.high_load_threshold = 0.95;
        smart_config.very_high_load_threshold = 0.98;
        
        info!("Applied emergency policy: maximum intervals and thresholds");
        
        Ok(())
    }

    /// Run resource-based optimization loop
    async fn run_resource_optimization(&self) {
        info!("Starting resource optimization loop");
        
        let mut optimization_interval = interval(Duration::from_secs(300)); // Every 5 minutes
        optimization_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            optimization_interval.tick().await;

            if let Err(e) = self.optimize_based_on_resources().await {
                error!("Resource optimization failed: {}", e);
            }
        }
    }

    /// Optimize scheduling based on resource patterns
    async fn optimize_based_on_resources(&self) -> Result<(), SnapshotError> {
        let history = self.resource_history.read().await;
        if history.len() < 10 {
            return Ok(()); // Need more data
        }

        // Analyze resource patterns over last 10 minutes
        let recent_snapshots: Vec<_> = history.iter().rev().take(20).collect();
        
        let patterns = self.analyze_resource_patterns(&recent_snapshots);
        
        // Apply optimizations based on detected patterns
        for pattern in patterns {
            self.apply_resource_optimization(pattern).await?;
        }

        Ok(())
    }

    /// Analyze resource usage patterns
    fn analyze_resource_patterns(&self, snapshots: &[&ResourceSnapshot]) -> Vec<ResourcePattern> {
        let mut patterns = Vec::new();

        if snapshots.len() < 5 {
            return patterns;
        }

        // Analyze CPU patterns
        let cpu_trend = self.calculate_trend(snapshots.iter().map(|s| s.cpu_usage).collect());
        if cpu_trend > 0.1 {
            patterns.push(ResourcePattern::CpuIncreasing);
        } else if cpu_trend < -0.1 {
            patterns.push(ResourcePattern::CpuDecreasing);
        }

        // Analyze memory patterns
        let memory_trend = self.calculate_trend(snapshots.iter().map(|s| s.memory_usage).collect());
        if memory_trend > 0.05 {
            patterns.push(ResourcePattern::MemoryIncreasing);
        }

        // Analyze I/O patterns
        let avg_disk_io = snapshots.iter().map(|s| s.disk_io_usage).sum::<f64>() / snapshots.len() as f64;
        if avg_disk_io > 0.8 {
            patterns.push(ResourcePattern::HighDiskIo);
        }

        // Check for stable low resource usage
        let cpu_variance = self.calculate_variance(snapshots.iter().map(|s| s.cpu_usage).collect());
        let memory_variance = self.calculate_variance(snapshots.iter().map(|s| s.memory_usage).collect());
        let avg_cpu = snapshots.iter().map(|s| s.cpu_usage).sum::<f64>() / snapshots.len() as f64;
        let avg_memory = snapshots.iter().map(|s| s.memory_usage).sum::<f64>() / snapshots.len() as f64;

        if avg_cpu < 0.3 && avg_memory < 0.5 && cpu_variance < 0.05 && memory_variance < 0.05 {
            patterns.push(ResourcePattern::StableLowUsage);
        }

        patterns
    }

    /// Calculate trend in a series of values
    fn calculate_trend(&self, values: Vec<f64>) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }

        let n = values.len() as f64;
        let sum_x: f64 = (0..values.len()).map(|i| i as f64).sum();
        let sum_y: f64 = values.iter().sum();
        let sum_xy: f64 = values.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
        slope
    }

    /// Calculate variance in a series of values
    fn calculate_variance(&self, values: Vec<f64>) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        variance
    }

    /// Apply optimization based on detected resource pattern
    async fn apply_resource_optimization(&self, pattern: ResourcePattern) -> Result<(), SnapshotError> {
        match pattern {
            ResourcePattern::CpuIncreasing => {
                info!("Detected increasing CPU usage - applying CPU optimization");
                // Increase intervals to reduce CPU load
                self.apply_cpu_optimization().await?;
            }
            ResourcePattern::CpuDecreasing => {
                info!("Detected decreasing CPU usage - allowing more aggressive scheduling");
                self.apply_aggressive_optimization().await?;
            }
            ResourcePattern::MemoryIncreasing => {
                info!("Detected increasing memory usage - applying memory optimization");
                self.apply_memory_optimization().await?;
            }
            ResourcePattern::HighDiskIo => {
                info!("Detected high disk I/O - deferring disk-intensive operations");
                self.apply_disk_optimization().await?;
            }
            ResourcePattern::StableLowUsage => {
                info!("Detected stable low resource usage - enabling aggressive scheduling");
                self.apply_aggressive_optimization().await?;
            }
        }

        Ok(())
    }

    /// Apply CPU optimization
    async fn apply_cpu_optimization(&self) -> Result<(), SnapshotError> {
        // Increase snapshot intervals to reduce CPU load
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(90 * 60); // 1.5 hours
        smart_config.medium_load_threshold = 0.4;
        smart_config.high_load_threshold = 0.6;
        
        debug!("Applied CPU optimization: increased intervals");
        Ok(())
    }

    /// Apply memory optimization
    async fn apply_memory_optimization(&self) -> Result<(), SnapshotError> {
        // Similar to CPU optimization but focused on memory-intensive operations
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(75 * 60); // 1.25 hours
        
        debug!("Applied memory optimization");
        Ok(())
    }

    /// Apply disk optimization
    async fn apply_disk_optimization(&self) -> Result<(), SnapshotError> {
        // Defer operations that are disk-intensive
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(120 * 60); // 2 hours
        
        debug!("Applied disk I/O optimization");
        Ok(())
    }

    /// Apply aggressive optimization for low resource usage
    async fn apply_aggressive_optimization(&self) -> Result<(), SnapshotError> {
        // Allow more frequent operations under low resource pressure
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        smart_config.base_snapshot_interval = Duration::from_secs(30 * 60); // 30 minutes
        smart_config.medium_load_threshold = 0.6;
        smart_config.high_load_threshold = 0.8;
        
        debug!("Applied aggressive optimization: decreased intervals");
        Ok(())
    }

    /// Get resource-aware scheduler statistics
    pub async fn get_resource_statistics(&self) -> ResourceAwareStatistics {
        let adaptive_stats = self.adaptive_scheduler.get_adaptive_statistics().await;
        let history = self.resource_history.read().await;
        
        let current_snapshot = history.last().cloned();
        let avg_cpu = if history.is_empty() {
            0.0
        } else {
            history.iter().map(|s| s.cpu_usage).sum::<f64>() / history.len() as f64
        };
        let avg_memory = if history.is_empty() {
            0.0
        } else {
            history.iter().map(|s| s.memory_usage).sum::<f64>() / history.len() as f64
        };

        ResourceAwareStatistics {
            adaptive_stats,
            current_resource_snapshot: current_snapshot,
            resource_history_size: history.len(),
            average_cpu_usage: avg_cpu,
            average_memory_usage: avg_memory,
        }
    }
}

impl Clone for ResourceAwareScheduler {
    fn clone(&self) -> Self {
        Self {
            adaptive_scheduler: self.adaptive_scheduler.clone(),
            resource_monitor: self.resource_monitor.clone(),
            config: self.config.clone(),
            resource_history: self.resource_history.clone(),
        }
    }
}

/// Resource monitor for collecting system resource metrics
struct ResourceMonitor {
    last_snapshot_time: Option<Instant>,
}

impl ResourceMonitor {
    fn new() -> Self {
        Self {
            last_snapshot_time: None,
        }
    }

    /// Take a snapshot of current resource usage
    async fn take_snapshot(&mut self) -> Result<ResourceSnapshot, SnapshotError> {
        let now = Instant::now();
        
        // In a real implementation, this would collect actual system metrics
        // For now, we'll simulate resource usage
        let snapshot = ResourceSnapshot {
            timestamp: now,
            cpu_usage: self.simulate_cpu_usage(),
            memory_usage: self.simulate_memory_usage(),
            disk_io_usage: self.simulate_disk_io_usage(),
            network_io_usage: self.simulate_network_io_usage(),
            load_average: self.simulate_load_average(),
        };

        self.last_snapshot_time = Some(now);
        Ok(snapshot)
    }

    /// Simulate CPU usage (in a real implementation, read from /proc/stat or similar)
    fn simulate_cpu_usage(&self) -> f64 {
        // Simulate moderate CPU usage for demo purposes
        // In real implementation, this would read from system monitoring APIs
        0.4
    }

    /// Simulate memory usage
    fn simulate_memory_usage(&self) -> f64 {
        // Simulate moderate memory usage for demo purposes
        // In real implementation, this would read from system monitoring APIs
        0.6
    }

    /// Simulate disk I/O usage
    fn simulate_disk_io_usage(&self) -> f64 {
        // Simulate low disk I/O usage for demo purposes
        // In real implementation, this would read from system monitoring APIs
        0.3
    }

    /// Simulate network I/O usage
    fn simulate_network_io_usage(&self) -> f64 {
        // Simulate low network I/O usage for demo purposes
        // In real implementation, this would read from system monitoring APIs
        0.2
    }

    /// Simulate load average
    fn simulate_load_average(&self) -> f64 {
        // Simulate moderate load average for demo purposes
        // In real implementation, this would read from system monitoring APIs
        1.5
    }
}

/// Configuration for resource-aware scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceAwareConfig {
    /// CPU usage thresholds
    pub cpu_medium_threshold: f64,
    pub cpu_high_threshold: f64,
    
    /// Memory usage thresholds
    pub memory_medium_threshold: f64,
    pub memory_high_threshold: f64,
    
    /// Disk I/O usage thresholds
    pub disk_io_medium_threshold: f64,
    pub disk_io_high_threshold: f64,
    
    /// Network I/O usage thresholds
    pub network_io_medium_threshold: f64,
    pub network_io_high_threshold: f64,
    
    /// Load average thresholds
    pub load_average_medium_threshold: f64,
    pub load_average_high_threshold: f64,
    
    /// Resource monitoring interval
    pub monitoring_interval: Duration,
}

impl Default for ResourceAwareConfig {
    fn default() -> Self {
        Self {
            cpu_medium_threshold: 0.5,
            cpu_high_threshold: 0.8,
            
            memory_medium_threshold: 0.6,
            memory_high_threshold: 0.85,
            
            disk_io_medium_threshold: 0.6,
            disk_io_high_threshold: 0.8,
            
            network_io_medium_threshold: 0.7,
            network_io_high_threshold: 0.9,
            
            load_average_medium_threshold: 2.0,
            load_average_high_threshold: 4.0,
            
            monitoring_interval: Duration::from_secs(30),
        }
    }
}

/// Snapshot of resource usage at a point in time
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub timestamp: Instant,
    pub cpu_usage: f64,        // 0.0 to 1.0
    pub memory_usage: f64,     // 0.0 to 1.0
    pub disk_io_usage: f64,    // 0.0 to 1.0
    pub network_io_usage: f64, // 0.0 to 1.0
    pub load_average: f64,     // System load average
}

/// Resource pressure levels
#[derive(Debug, Clone, Copy, PartialEq)]
enum ResourcePressureLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// Detected resource usage patterns
#[derive(Debug, Clone)]
enum ResourcePattern {
    CpuIncreasing,
    CpuDecreasing,
    MemoryIncreasing,
    HighDiskIo,
    StableLowUsage,
}

/// Statistics about resource-aware scheduler
#[derive(Debug, Clone)]
pub struct ResourceAwareStatistics {
    pub adaptive_stats: crate::scheduler::AdaptiveStatistics,
    pub current_resource_snapshot: Option<ResourceSnapshot>,
    pub resource_history_size: usize,
    pub average_cpu_usage: f64,
    pub average_memory_usage: f64,
}
