// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Schedule manager for automatic snapshot operations

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, error, instrument};

use crate::strategy::{HybridSnapshotStrategy, AutoCleanupManager};
use crate::types::error::SnapshotError;

/// Manages scheduling of automatic snapshot operations
pub struct ScheduleManager {
    hybrid_strategy: Arc<HybridSnapshotStrategy>,
    cleanup_manager: Arc<AutoCleanupManager>,
    config: ScheduleConfig,
    is_running: tokio::sync::Mutex<bool>,
}

impl ScheduleManager {
    /// Create a new schedule manager
    pub fn new(
        hybrid_strategy: Arc<HybridSnapshotStrategy>,
        cleanup_manager: Arc<AutoCleanupManager>,
        config: ScheduleConfig,
    ) -> Self {
        Self {
            hybrid_strategy,
            cleanup_manager,
            config,
            is_running: tokio::sync::Mutex::new(false),
        }
    }

    /// Start the scheduled operations
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<(), SnapshotError> {
        let mut is_running = self.is_running.lock().await;
        if *is_running {
            return Err(SnapshotError::Generic {
                context: "Schedule manager is already running".to_string(),
            });
        }

        *is_running = true;
        info!("Starting snapshot schedule manager");

        // Start snapshot creation task
        if self.config.enable_automatic_snapshots {
            let strategy = self.hybrid_strategy.clone();
            let snapshot_interval = self.config.snapshot_interval;
            
            tokio::spawn(async move {
                Self::run_snapshot_schedule(strategy, snapshot_interval).await;
            });
        }

        // Start cleanup task
        if self.config.enable_automatic_cleanup {
            let cleanup = self.cleanup_manager.clone();
            let cleanup_interval = self.config.cleanup_interval;
            
            tokio::spawn(async move {
                Self::run_cleanup_schedule(cleanup, cleanup_interval).await;
            });
        }

        Ok(())
    }

    /// Stop the scheduled operations
    pub async fn stop(&self) -> Result<(), SnapshotError> {
        let mut is_running = self.is_running.lock().await;
        if !*is_running {
            return Ok(());
        }

        *is_running = false;
        info!("Stopping snapshot schedule manager");

        // In a real implementation, we would signal the background tasks to stop
        // For now, we just mark as stopped
        Ok(())
    }

    /// Run snapshot creation schedule
    async fn run_snapshot_schedule(
        strategy: Arc<HybridSnapshotStrategy>,
        interval_duration: Duration,
    ) {
        info!("Starting snapshot creation schedule with interval {:?}", interval_duration);
        
        let mut interval_timer = interval(interval_duration);
        interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;
            
            match strategy.execute_strategy().await {
                Ok(decision) => {
                    info!("Scheduled snapshot creation completed: {:?}", decision.strategy_type);
                }
                Err(e) => {
                    error!("Scheduled snapshot creation failed: {}", e);
                }
            }
        }
    }

    /// Run cleanup schedule
    async fn run_cleanup_schedule(
        cleanup_manager: Arc<AutoCleanupManager>,
        interval_duration: Duration,
    ) {
        info!("Starting cleanup schedule with interval {:?}", interval_duration);
        
        let mut interval_timer = interval(interval_duration);
        interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;
            
            match cleanup_manager.execute_cleanup().await {
                Ok(result) => {
                    info!("Scheduled cleanup completed: removed {} snapshots, freed {} bytes", 
                          result.snapshots_removed, result.space_freed);
                }
                Err(e) => {
                    error!("Scheduled cleanup failed: {}", e);
                }
            }
        }
    }

    /// Check if the schedule manager is running
    pub async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    /// Update schedule configuration
    pub async fn update_config(&self, new_config: ScheduleConfig) -> Result<(), SnapshotError> {
        // In a real implementation, this would update the running schedules
        // For now, just log the change
        info!("Schedule configuration updated: {:?}", new_config);
        Ok(())
    }

    /// Get schedule statistics
    pub async fn get_statistics(&self) -> Result<ScheduleStatistics, SnapshotError> {
        // This would collect statistics from the running schedules
        Ok(ScheduleStatistics {
            is_running: self.is_running().await,
            snapshots_created_by_schedule: 0, // Would track actual count
            cleanups_executed_by_schedule: 0, // Would track actual count
            last_snapshot_time: None, // Would track actual time
            last_cleanup_time: None, // Would track actual time
            next_snapshot_time: None, // Would calculate based on schedule
            next_cleanup_time: None, // Would calculate based on schedule
        })
    }
}

/// Configuration for the schedule manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    /// Enable automatic snapshot creation
    pub enable_automatic_snapshots: bool,
    /// Interval between snapshot creation attempts
    pub snapshot_interval: Duration,
    
    /// Enable automatic cleanup
    pub enable_automatic_cleanup: bool,
    /// Interval between cleanup runs
    pub cleanup_interval: Duration,
    
    /// Enable health monitoring
    pub enable_health_monitoring: bool,
    /// Interval for health checks
    pub health_check_interval: Duration,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            enable_automatic_snapshots: true,
            snapshot_interval: Duration::from_secs(60 * 60), // 1 hour
            
            enable_automatic_cleanup: true,
            cleanup_interval: Duration::from_secs(6 * 60 * 60), // 6 hours
            
            enable_health_monitoring: true,
            health_check_interval: Duration::from_secs(5 * 60), // 5 minutes
        }
    }
}

/// Statistics about scheduled operations
#[derive(Debug, Clone)]
pub struct ScheduleStatistics {
    /// Whether the scheduler is currently running
    pub is_running: bool,
    /// Number of snapshots created by schedule
    pub snapshots_created_by_schedule: u64,
    /// Number of cleanups executed by schedule
    pub cleanups_executed_by_schedule: u64,
    /// Time of last snapshot creation
    pub last_snapshot_time: Option<SystemTime>,
    /// Time of last cleanup execution
    pub last_cleanup_time: Option<SystemTime>,
    /// Time of next scheduled snapshot
    pub next_snapshot_time: Option<SystemTime>,
    /// Time of next scheduled cleanup
    pub next_cleanup_time: Option<SystemTime>,
}
