// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Smart scheduler with intelligent scheduling algorithms

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, instrument};

use crate::strategy::{HybridSnapshotStrategy, AutoCleanupManager};
use crate::types::error::SnapshotError;
use crate::metrics::prometheus::PrometheusMetrics;

/// Smart scheduler that adapts to system load and performance patterns
pub struct SmartScheduler {
    config: Arc<RwLock<SmartSchedulerConfig>>,
    state: Arc<RwLock<SchedulerState>>,
    strategy: Arc<HybridSnapshotStrategy>,
    cleanup_manager: Arc<AutoCleanupManager>,
    metrics: Option<Arc<PrometheusMetrics>>,
    is_running: Arc<Mutex<bool>>,
    performance_history: Arc<RwLock<VecDeque<PerformanceMetric>>>,
}

impl SmartScheduler {
    /// Create a new smart scheduler
    pub fn new(
        config: SmartSchedulerConfig,
        strategy: Arc<HybridSnapshotStrategy>,
        cleanup_manager: Arc<AutoCleanupManager>,
        metrics: Option<Arc<PrometheusMetrics>>,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(SchedulerState::default())),
            strategy,
            cleanup_manager,
            metrics,
            is_running: Arc::new(Mutex::new(false)),
            performance_history: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Start the smart scheduler
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<(), SnapshotError> {
        let mut is_running = self.is_running.lock().await;
        if *is_running {
            return Err(SnapshotError::Generic {
                context: "Smart scheduler is already running".to_string(),
            });
        }

        *is_running = true;
        info!("Starting smart scheduler");

        // Start the main scheduler loop
        let scheduler = self.clone();
        tokio::spawn(async move {
            scheduler.run_scheduler_loop().await;
        });

        // Start performance monitoring
        let performance_monitor = self.clone();
        tokio::spawn(async move {
            performance_monitor.run_performance_monitoring().await;
        });

        Ok(())
    }

    /// Stop the smart scheduler
    pub async fn stop(&self) -> Result<(), SnapshotError> {
        let mut is_running = self.is_running.lock().await;
        if !*is_running {
            return Ok(());
        }

        *is_running = false;
        info!("Stopping smart scheduler");
        Ok(())
    }

    /// Main scheduler loop with intelligent decision making
    async fn run_scheduler_loop(&self) {
        info!("Starting smart scheduler main loop");
        
        let mut check_interval = interval(Duration::from_secs(30)); // Check every 30 seconds
        check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            // Check if we should still be running
            if !*self.is_running.lock().await {
                break;
            }

            check_interval.tick().await;

            // Make intelligent scheduling decisions
            if let Err(e) = self.make_scheduling_decisions().await {
                error!("Error in scheduling decisions: {}", e);
            }
        }

        info!("Smart scheduler main loop stopped");
    }

    /// Make intelligent scheduling decisions based on current conditions
    async fn make_scheduling_decisions(&self) -> Result<(), SnapshotError> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;
        let now = Instant::now();

        // Update system load assessment
        let current_load = self.assess_system_load().await?;
        state.current_system_load = current_load;

        // Check if we should create a snapshot
        if self.should_create_snapshot(&config, &state, now).await {
            state.last_snapshot_attempt = Some(now);
            drop(state);
            drop(config);
            
            if let Err(e) = self.execute_smart_snapshot().await {
                error!("Smart snapshot execution failed: {}", e);
                let mut state = self.state.write().await;
                state.consecutive_failures += 1;
            } else {
                let mut state = self.state.write().await;
                state.last_successful_snapshot = Some(now);
                state.consecutive_failures = 0;
            }
        }

        let config = self.config.read().await;
        let mut state = self.state.write().await;

        // Check if we should run cleanup
        if self.should_run_cleanup(&config, &state, now).await {
            state.last_cleanup_attempt = Some(now);
            drop(state);
            drop(config);
            
            if let Err(e) = self.execute_smart_cleanup().await {
                error!("Smart cleanup execution failed: {}", e);
            } else {
                let mut state = self.state.write().await;
                state.last_successful_cleanup = Some(now);
            }
        }

        Ok(())
    }

    /// Determine if we should create a snapshot now
    async fn should_create_snapshot(
        &self,
        config: &SmartSchedulerConfig,
        state: &SchedulerState,
        now: Instant,
    ) -> bool {
        if !config.enable_snapshots {
            return false;
        }

        // Check minimum interval
        if let Some(last_attempt) = state.last_snapshot_attempt {
            if now.duration_since(last_attempt) < config.min_snapshot_interval {
                return false;
            }
        }

        // Check if we're under high load and should defer
        if state.current_system_load > config.high_load_threshold {
            debug!("Deferring snapshot due to high system load: {}", state.current_system_load);
            return false;
        }

        // Calculate adaptive interval based on performance history
        let adaptive_interval = self.calculate_adaptive_interval(
            config.base_snapshot_interval,
            state.current_system_load,
            state.consecutive_failures,
        ).await;

        // Check if enough time has passed since last successful snapshot
        if let Some(last_success) = state.last_successful_snapshot {
            now.duration_since(last_success) >= adaptive_interval
        } else {
            // No previous snapshot, create one now if load is acceptable
            state.current_system_load <= config.medium_load_threshold
        }
    }

    /// Determine if we should run cleanup now
    async fn should_run_cleanup(
        &self,
        config: &SmartSchedulerConfig,
        state: &SchedulerState,
        now: Instant,
    ) -> bool {
        if !config.enable_cleanup {
            return false;
        }

        // Check minimum interval
        if let Some(last_attempt) = state.last_cleanup_attempt {
            if now.duration_since(last_attempt) < config.min_cleanup_interval {
                return false;
            }
        }

        // Cleanup is less sensitive to load than snapshots
        if state.current_system_load > config.very_high_load_threshold {
            return false;
        }

        // Check if enough time has passed since last cleanup
        if let Some(last_cleanup) = state.last_successful_cleanup {
            now.duration_since(last_cleanup) >= config.base_cleanup_interval
        } else {
            true // No previous cleanup, run one now
        }
    }

    /// Calculate adaptive interval based on system performance
    async fn calculate_adaptive_interval(
        &self,
        base_interval: Duration,
        current_load: f64,
        consecutive_failures: u32,
    ) -> Duration {
        let mut multiplier = 1.0;

        // Adjust based on system load
        if current_load > 0.8 {
            multiplier *= 1.5; // Increase interval under high load
        } else if current_load < 0.3 {
            multiplier *= 0.8; // Decrease interval under low load
        }

        // Adjust based on failure history
        if consecutive_failures > 0 {
            multiplier *= 1.0 + (consecutive_failures as f64 * 0.2);
        }

        // Check performance history for patterns
        let history = self.performance_history.read().await;
        if history.len() >= 5 {
            let avg_duration = history.iter()
                .map(|m| m.operation_duration)
                .sum::<Duration>()
                .div_f64(history.len() as f64);
            
            // If operations are taking longer, increase interval
            if avg_duration > Duration::from_secs(300) {
                multiplier *= 1.3;
            }
        }

        Duration::from_millis((base_interval.as_millis() as f64 * multiplier) as u64)
    }

    /// Execute a smart snapshot operation
    async fn execute_smart_snapshot(&self) -> Result<(), SnapshotError> {
        let start_time = Instant::now();
        info!("Executing smart snapshot");

        // Update metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_snapshot_operation("smart_scheduled");
        }

        // Execute the snapshot strategy
        let result = self.strategy.execute_strategy().await;
        let duration = start_time.elapsed();

        // Record performance metric
        self.record_performance_metric(OperationType::Snapshot, duration, result.is_ok()).await;

        match result {
            Ok(decision) => {
                info!("Smart snapshot completed successfully: {:?}", decision.strategy_type);
                
                // Update state
                let mut state = self.state.write().await;
                state.total_snapshots_created += 1;
                
                if let Some(metrics) = &self.metrics {
                    metrics.record_snapshot_duration(duration.as_secs_f64());
                }
                
                Ok(())
            }
            Err(e) => {
                error!("Smart snapshot failed: {}", e);
                
                if let Some(metrics) = &self.metrics {
                    metrics.record_error("snapshot_failed");
                }
                
                Err(SnapshotError::Generic {
                    context: format!("Smart snapshot execution failed: {}", e),
                })
            }
        }
    }

    /// Execute a smart cleanup operation
    async fn execute_smart_cleanup(&self) -> Result<(), SnapshotError> {
        let start_time = Instant::now();
        info!("Executing smart cleanup");

        // Execute cleanup
        let result = self.cleanup_manager.execute_cleanup().await;
        let duration = start_time.elapsed();

        // Record performance metric
        self.record_performance_metric(OperationType::Cleanup, duration, result.is_ok()).await;

        match result {
            Ok(cleanup_result) => {
                info!("Smart cleanup completed: removed {} snapshots, freed {} bytes",
                      cleanup_result.snapshots_removed, cleanup_result.space_freed);
                
                // Update state
                let mut state = self.state.write().await;
                state.total_cleanups_executed += 1;
                
                Ok(())
            }
            Err(e) => {
                error!("Smart cleanup failed: {}", e);
                Err(SnapshotError::Generic {
                    context: format!("Smart cleanup execution failed: {}", e),
                })
            }
        }
    }

    /// Assess current system load
    async fn assess_system_load(&self) -> Result<f64, SnapshotError> {
        // This is a simplified load assessment
        // In a real implementation, this would check:
        // - CPU usage
        // - Memory usage
        // - Disk I/O
        // - Network I/O
        // - Database load
        
        // For now, return a simulated load based on recent performance
        let history = self.performance_history.read().await;
        if history.is_empty() {
            return Ok(0.3); // Default moderate load
        }

        let recent_failures = history.iter()
            .rev()
            .take(5)
            .filter(|m| !m.success)
            .count();

        let base_load = 0.3;
        let failure_penalty = recent_failures as f64 * 0.1;
        
        Ok((base_load + failure_penalty).min(1.0))
    }

    /// Record performance metric
    async fn record_performance_metric(
        &self,
        operation_type: OperationType,
        duration: Duration,
        success: bool,
    ) {
        let metric = PerformanceMetric {
            timestamp: Instant::now(),
            operation_type,
            operation_duration: duration,
            success,
        };

        let mut history = self.performance_history.write().await;
        history.push_back(metric);

        // Keep only last 100 metrics
        while history.len() > 100 {
            history.pop_front();
        }
    }

    /// Run performance monitoring loop
    async fn run_performance_monitoring(&self) {
        let mut monitor_interval = interval(Duration::from_secs(60)); // Monitor every minute
        monitor_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if !*self.is_running.lock().await {
                break;
            }

            monitor_interval.tick().await;

            // Analyze performance patterns and adjust configuration if needed
            if let Err(e) = self.analyze_performance_patterns().await {
                error!("Performance pattern analysis failed: {}", e);
            }
        }
    }

    /// Analyze performance patterns and auto-adjust configuration
    async fn analyze_performance_patterns(&self) -> Result<(), SnapshotError> {
        let history = self.performance_history.read().await;
        if history.len() < 10 {
            return Ok(());
        }

        let recent_metrics: Vec<_> = history.iter().rev().take(10).collect();
        let failure_rate = recent_metrics.iter()
            .filter(|m| !m.success)
            .count() as f64 / recent_metrics.len() as f64;

        let avg_duration = recent_metrics.iter()
            .map(|m| m.operation_duration)
            .sum::<Duration>()
            .div_f64(recent_metrics.len() as f64);

        // Auto-adjust configuration based on patterns
        let mut config = self.config.write().await;
        
        // If failure rate is high, increase intervals
        if failure_rate > 0.3 {
            let new_interval = config.base_snapshot_interval.mul_f64(1.2);
            if new_interval <= Duration::from_secs(4 * 60 * 60) { // Max 4 hours
                config.base_snapshot_interval = new_interval;
                info!("Increased snapshot interval due to high failure rate: {:?}", new_interval);
            }
        }
        
        // If operations are consistently fast and successful, decrease intervals
        if failure_rate < 0.1 && avg_duration < Duration::from_secs(60) {
            let new_interval = config.base_snapshot_interval.mul_f64(0.9);
            if new_interval >= Duration::from_secs(10 * 60) { // Min 10 minutes
                config.base_snapshot_interval = new_interval;
                info!("Decreased snapshot interval due to good performance: {:?}", new_interval);
            }
        }

        Ok(())
    }

    /// Get current scheduler statistics
    pub async fn get_statistics(&self) -> SmartSchedulerStatistics {
        let state = self.state.read().await;
        let config = self.config.read().await;
        let history = self.performance_history.read().await;

        let recent_performance = if history.len() >= 5 {
            let recent: Vec<_> = history.iter().rev().take(5).collect();
            let avg_duration = recent.iter()
                .map(|m| m.operation_duration)
                .sum::<Duration>()
                .div_f64(recent.len() as f64);
            let success_rate = recent.iter()
                .filter(|m| m.success)
                .count() as f64 / recent.len() as f64;
            Some((avg_duration, success_rate))
        } else {
            None
        };

        SmartSchedulerStatistics {
            is_running: *self.is_running.lock().await,
            current_system_load: state.current_system_load,
            total_snapshots_created: state.total_snapshots_created,
            total_cleanups_executed: state.total_cleanups_executed,
            consecutive_failures: state.consecutive_failures,
            last_successful_snapshot: state.last_successful_snapshot,
            last_successful_cleanup: state.last_successful_cleanup,
            current_snapshot_interval: config.base_snapshot_interval,
            current_cleanup_interval: config.base_cleanup_interval,
            recent_avg_operation_duration: recent_performance.map(|(d, _)| d),
            recent_success_rate: recent_performance.map(|(_, r)| r),
        }
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: SmartSchedulerConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
        info!("Smart scheduler configuration updated");
    }
}

impl Clone for SmartScheduler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
            strategy: self.strategy.clone(),
            cleanup_manager: self.cleanup_manager.clone(),
            metrics: self.metrics.clone(),
            is_running: self.is_running.clone(),
            performance_history: self.performance_history.clone(),
        }
    }
}

/// Configuration for the smart scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmartSchedulerConfig {
    /// Enable automatic snapshots
    pub enable_snapshots: bool,
    /// Base interval for snapshots (will be adjusted intelligently)
    pub base_snapshot_interval: Duration,
    /// Minimum interval between snapshot attempts
    pub min_snapshot_interval: Duration,
    
    /// Enable automatic cleanup
    pub enable_cleanup: bool,
    /// Base interval for cleanup
    pub base_cleanup_interval: Duration,
    /// Minimum interval between cleanup attempts
    pub min_cleanup_interval: Duration,
    
    /// System load thresholds
    pub medium_load_threshold: f64,
    pub high_load_threshold: f64,
    pub very_high_load_threshold: f64,
    
    /// Performance adjustment settings
    pub enable_auto_adjustment: bool,
    pub max_interval_multiplier: f64,
    pub min_interval_multiplier: f64,
}

impl Default for SmartSchedulerConfig {
    fn default() -> Self {
        Self {
            enable_snapshots: true,
            base_snapshot_interval: Duration::from_secs(60 * 60), // 1 hour
            min_snapshot_interval: Duration::from_secs(10 * 60),  // 10 minutes
            
            enable_cleanup: true,
            base_cleanup_interval: Duration::from_secs(6 * 60 * 60), // 6 hours
            min_cleanup_interval: Duration::from_secs(30 * 60),      // 30 minutes
            
            medium_load_threshold: 0.5,
            high_load_threshold: 0.7,
            very_high_load_threshold: 0.9,
            
            enable_auto_adjustment: true,
            max_interval_multiplier: 4.0,
            min_interval_multiplier: 0.5,
        }
    }
}

/// Internal state of the smart scheduler
#[derive(Debug, Default)]
struct SchedulerState {
    current_system_load: f64,
    total_snapshots_created: u64,
    total_cleanups_executed: u64,
    consecutive_failures: u32,
    last_snapshot_attempt: Option<Instant>,
    last_successful_snapshot: Option<Instant>,
    last_cleanup_attempt: Option<Instant>,
    last_successful_cleanup: Option<Instant>,
}

/// Performance metric for tracking operation history
#[derive(Debug, Clone)]
struct PerformanceMetric {
    timestamp: Instant,
    operation_type: OperationType,
    operation_duration: Duration,
    success: bool,
}

/// Type of operation being tracked
#[derive(Debug, Clone, Copy)]
enum OperationType {
    Snapshot,
    Cleanup,
}

/// Statistics about the smart scheduler
#[derive(Debug, Clone)]
pub struct SmartSchedulerStatistics {
    pub is_running: bool,
    pub current_system_load: f64,
    pub total_snapshots_created: u64,
    pub total_cleanups_executed: u64,
    pub consecutive_failures: u32,
    pub last_successful_snapshot: Option<Instant>,
    pub last_successful_cleanup: Option<Instant>,
    pub current_snapshot_interval: Duration,
    pub current_cleanup_interval: Duration,
    pub recent_avg_operation_duration: Option<Duration>,
    pub recent_success_rate: Option<f64>,
}
