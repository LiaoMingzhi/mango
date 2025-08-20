// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Automatic cleanup strategies for snapshot management
//!
//! This module implements various cleanup policies to automatically remove
//! old, redundant, or corrupted snapshots while preserving important ones.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn, error, instrument};

use mgo_types::base_types::EpochId;


use crate::manager::SnapshotManager;
use crate::types::{SnapshotId, SnapshotType, SnapshotInfo, SnapshotFilter};
use crate::types::error::SnapshotError;
use crate::storage::SnapshotStorage;

/// Automatic cleanup manager for snapshots
pub struct AutoCleanupManager {
    snapshot_manager: Arc<SnapshotManager>,
    storage_backend: Arc<dyn SnapshotStorage>,
    config: CleanupConfig,
    cleanup_state: tokio::sync::Mutex<CleanupState>,
}

impl AutoCleanupManager {
    /// Create a new auto cleanup manager
    pub fn new(
        snapshot_manager: Arc<SnapshotManager>,
        storage_backend: Arc<dyn SnapshotStorage>,
        config: CleanupConfig,
    ) -> Self {
        Self {
            snapshot_manager,
            storage_backend,
            config,
            cleanup_state: tokio::sync::Mutex::new(CleanupState::new()),
        }
    }

    /// Execute automatic cleanup based on configured policies
    #[instrument(level = "info", skip(self))]
    pub async fn execute_cleanup(&self) -> Result<CleanupResult, SnapshotError> {
        info!("Starting automatic snapshot cleanup");

        let mut state = self.cleanup_state.lock().await;
        let start_time = SystemTime::now();

        // Get all snapshots
        let all_snapshots = self.snapshot_manager.list_snapshots(SnapshotFilter::default()).await?;
        
        // Analyze snapshots for cleanup
        let analysis = self.analyze_snapshots(&all_snapshots).await?;
        
        // Execute cleanup policies
        let mut result = CleanupResult::new();
        
        // Policy 1: Age-based cleanup
        if self.config.enable_age_based_cleanup {
            let age_result = self.execute_age_based_cleanup(&analysis, &all_snapshots).await?;
            result.merge(age_result);
        }

        // Policy 2: Count-based cleanup
        if self.config.enable_count_based_cleanup {
            let count_result = self.execute_count_based_cleanup(&analysis, &all_snapshots).await?;
            result.merge(count_result);
        }

        // Policy 3: Size-based cleanup
        if self.config.enable_size_based_cleanup {
            let size_result = self.execute_size_based_cleanup(&analysis, &all_snapshots).await?;
            result.merge(size_result);
        }

        // Policy 4: Redundancy-based cleanup
        if self.config.enable_redundancy_cleanup {
            let redundancy_result = self.execute_redundancy_cleanup(&analysis, &all_snapshots).await?;
            result.merge(redundancy_result);
        }

        // Policy 5: Corruption cleanup
        if self.config.enable_corruption_cleanup {
            let corruption_result = self.execute_corruption_cleanup(&analysis, &all_snapshots).await?;
            result.merge(corruption_result);
        }

        // Update cleanup state
        state.last_cleanup_time = Some(SystemTime::now());
        state.total_cleanup_runs += 1;
        state.total_snapshots_removed += result.snapshots_removed as u64;
        state.total_space_freed += result.space_freed;

        result.cleanup_duration = start_time.elapsed().unwrap_or_default();

        info!("Cleanup completed: removed {} snapshots, freed {} bytes", 
              result.snapshots_removed, result.space_freed);

        Ok(result)
    }

    /// Analyze snapshots to prepare for cleanup
    async fn analyze_snapshots(
        &self,
        snapshots: &[SnapshotInfo],
    ) -> Result<SnapshotAnalysisResult, SnapshotError> {
        debug!("Analyzing {} snapshots for cleanup", snapshots.len());

        let mut analysis = SnapshotAnalysisResult::new();
        let current_time = SystemTime::now();

        for snapshot in snapshots {
            // Calculate age
            let age = current_time.duration_since(snapshot.metadata.created_at.into()).unwrap_or_default();
            analysis.snapshots_by_age.entry(snapshot.metadata.snapshot_type.clone())
                .or_insert_with(Vec::new)
                .push((snapshot.metadata.id, age));

            // Group by type
            analysis.snapshots_by_type.entry(snapshot.metadata.snapshot_type.clone())
                .or_insert_with(Vec::new)
                .push(snapshot.clone());

            // Group by epoch
            analysis.snapshots_by_epoch.entry(snapshot.metadata.epoch)
                .or_insert_with(Vec::new)
                .push(snapshot.clone());

            // Track size
            analysis.total_size += snapshot.metadata.compressed_size;
            analysis.total_count += 1;

            // Check for potential issues
            if snapshot.metadata.compressed_size == 0 {
                analysis.suspicious_snapshots.push(snapshot.metadata.id);
            }
        }

        Ok(analysis)
    }

    /// Execute age-based cleanup
    async fn execute_age_based_cleanup(
        &self,
        analysis: &SnapshotAnalysisResult,
        _all_snapshots: &[SnapshotInfo],
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing age-based cleanup");

        let mut result = CleanupResult::new();
        let current_time = SystemTime::now();

        for snapshot in _all_snapshots {
            let age = current_time.duration_since(snapshot.metadata.created_at.into()).unwrap_or_default();
            
            let should_remove = match &snapshot.metadata.snapshot_type {
                SnapshotType::Full { .. } => {
                    age > self.config.max_age_full_snapshots &&
                    !self.is_protected_snapshot(snapshot, analysis).await?
                }
                SnapshotType::Incremental { .. } => {
                    age > self.config.max_age_incremental_snapshots
                }
                SnapshotType::Checkpoint { .. } => {
                    age > self.config.max_age_checkpoint_snapshots
                }
                SnapshotType::Epoch { .. } => {
                    age > self.config.max_age_epoch_snapshots &&
                    !self.is_protected_snapshot(snapshot, analysis).await?
                }
            };

            if should_remove {
                if let Err(e) = self.remove_snapshot(snapshot.metadata.id).await {
                    warn!("Failed to remove snapshot {}: {}", snapshot.metadata.id, e);
                } else {
                    result.snapshots_removed += 1;
                    result.space_freed += snapshot.metadata.compressed_size;
                    info!("Removed aged snapshot {}", snapshot.metadata.id);
                }
            }
        }

        Ok(result)
    }

    /// Execute count-based cleanup
    async fn execute_count_based_cleanup(
        &self,
        analysis: &SnapshotAnalysisResult,
        _all_snapshots: &[SnapshotInfo],
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing count-based cleanup");

        let mut result = CleanupResult::new();

        // Group snapshots by type and sort by age
        for (snapshot_type, snapshots) in &analysis.snapshots_by_type {
            let max_count = match snapshot_type {
                SnapshotType::Full { .. } => self.config.max_full_snapshots,
                SnapshotType::Incremental { .. } => self.config.max_incremental_snapshots,
                SnapshotType::Checkpoint { .. } => self.config.max_checkpoint_snapshots,
                SnapshotType::Epoch { .. } => self.config.max_epoch_snapshots,
            };

            if snapshots.len() > max_count {
                // Sort by creation time (oldest first)
                let mut sorted_snapshots = snapshots.clone();
                sorted_snapshots.sort_by_key(|s| s.metadata.created_at);

                // Remove oldest snapshots beyond limit
                let to_remove = snapshots.len() - max_count;
                for snapshot in sorted_snapshots.iter().take(to_remove) {
                    if !self.is_protected_snapshot(snapshot, analysis).await? {
                        if let Err(e) = self.remove_snapshot(snapshot.metadata.id).await {
                            warn!("Failed to remove snapshot {}: {}", snapshot.metadata.id, e);
                        } else {
                            result.snapshots_removed += 1;
                            result.space_freed += snapshot.metadata.compressed_size;
                            info!("Removed excess snapshot {}", snapshot.metadata.id);
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute size-based cleanup
    async fn execute_size_based_cleanup(
        &self,
        analysis: &SnapshotAnalysisResult,
        _all_snapshots: &[SnapshotInfo],
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing size-based cleanup");

        let mut result = CleanupResult::new();

        if analysis.total_size <= self.config.max_total_snapshot_size {
            return Ok(result);
        }

        // Calculate how much space we need to free
        let space_to_free = analysis.total_size - self.config.max_total_snapshot_size;
        let mut space_freed = 0u64;

        // Sort snapshots by priority (oldest, non-protected first)
        let mut snapshots_by_priority = _all_snapshots.to_vec();
        snapshots_by_priority.sort_by_key(|s| s.metadata.created_at);

        for snapshot in snapshots_by_priority {
            if space_freed >= space_to_free {
                break;
            }

            if !self.is_protected_snapshot(&snapshot, analysis).await? {
                if let Err(e) = self.remove_snapshot(snapshot.metadata.id).await {
                    warn!("Failed to remove snapshot {}: {}", snapshot.metadata.id, e);
                } else {
                    space_freed += snapshot.metadata.compressed_size;
                    result.snapshots_removed += 1;
                    result.space_freed += snapshot.metadata.compressed_size;
                    info!("Removed snapshot {} for size limits", snapshot.metadata.id);
                }
            }
        }

        Ok(result)
    }

    /// Execute redundancy-based cleanup
    async fn execute_redundancy_cleanup(
        &self,
        analysis: &SnapshotAnalysisResult,
        _all_snapshots: &[SnapshotInfo],
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing redundancy-based cleanup");

        let mut result = CleanupResult::new();

        // Look for redundant incremental snapshots
        for (_epoch, snapshots) in &analysis.snapshots_by_epoch {
            let incremental_snapshots: Vec<_> = snapshots.iter()
                .filter(|s| matches!(s.metadata.snapshot_type, SnapshotType::Incremental { .. }))
                .collect();

            // If we have a full snapshot for this epoch and many incrementals,
            // we can remove some of the intermediate incrementals
            let has_full_snapshot = snapshots.iter()
                .any(|s| matches!(s.metadata.snapshot_type, SnapshotType::Full { .. }));

            if has_full_snapshot && incremental_snapshots.len() > self.config.max_incremental_per_epoch {
                // Keep the latest incrementals and remove older ones
                let mut sorted_incrementals = incremental_snapshots.clone();
                sorted_incrementals.sort_by_key(|s| s.metadata.created_at);

                let to_remove = incremental_snapshots.len() - self.config.max_incremental_per_epoch;
                for snapshot in sorted_incrementals.iter().take(to_remove) {
                    if let Err(e) = self.remove_snapshot(snapshot.metadata.id).await {
                        warn!("Failed to remove redundant snapshot {}: {}", snapshot.metadata.id, e);
                    } else {
                        result.snapshots_removed += 1;
                        result.space_freed += snapshot.metadata.compressed_size;
                        info!("Removed redundant incremental snapshot {}", snapshot.metadata.id);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Execute corruption cleanup
    async fn execute_corruption_cleanup(
        &self,
        analysis: &SnapshotAnalysisResult,
        _all_snapshots: &[SnapshotInfo],
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing corruption cleanup");

        let mut result = CleanupResult::new();

        // Check suspicious snapshots
        for snapshot_id in &analysis.suspicious_snapshots {
            // Verify snapshot integrity
            if let Err(e) = self.storage_backend.verify_snapshot(*snapshot_id).await {
                warn!("Snapshot {} appears corrupted: {}", snapshot_id, e);
                
                if let Err(e) = self.remove_snapshot(*snapshot_id).await {
                    error!("Failed to remove corrupted snapshot {}: {}", snapshot_id, e);
                } else {
                    result.snapshots_removed += 1;
                    // Find the snapshot to get its size
                    if let Some(snapshot) = _all_snapshots.iter().find(|s| s.metadata.id == *snapshot_id) {
                        result.space_freed += snapshot.metadata.compressed_size;
                    }
                    info!("Removed corrupted snapshot {}", snapshot_id);
                }
            }
        }

        Ok(result)
    }

    /// Check if a snapshot is protected from deletion
    async fn is_protected_snapshot(
        &self,
        snapshot: &SnapshotInfo,
        analysis: &SnapshotAnalysisResult,
    ) -> Result<bool, SnapshotError> {
        // Protect recent snapshots
        let age = SystemTime::now().duration_since(snapshot.metadata.created_at.into()).unwrap_or_default();
        if age < self.config.minimum_retention_period {
            return Ok(true);
        }

        // Protect epoch boundary snapshots
        if matches!(snapshot.metadata.snapshot_type, SnapshotType::Epoch { .. }) {
            return Ok(true);
        }

        // Protect the latest full snapshot for each epoch
        if let SnapshotType::Full { .. } = snapshot.metadata.snapshot_type {
            if let Some(epoch_snapshots) = analysis.snapshots_by_epoch.get(&snapshot.metadata.epoch) {
                let latest_full = epoch_snapshots.iter()
                    .filter(|s| matches!(s.metadata.snapshot_type, SnapshotType::Full { .. }))
                    .max_by_key(|s| s.metadata.created_at);
                
                if let Some(latest) = latest_full {
                    if latest.metadata.id == snapshot.metadata.id {
                        return Ok(true);
                    }
                }
            }
        }

        // Protect snapshots that are base for incremental chains
        if let SnapshotType::Full { .. } = snapshot.metadata.snapshot_type {
            for other_snapshot in analysis.snapshots_by_epoch.get(&snapshot.metadata.epoch).unwrap_or(&Vec::new()) {
                if let SnapshotType::Incremental { base_snapshot, .. } = other_snapshot.metadata.snapshot_type {
                    if base_snapshot == snapshot.metadata.id {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Remove a snapshot
    async fn remove_snapshot(&self, snapshot_id: SnapshotId) -> Result<(), SnapshotError> {
        let snapshot_id_str = snapshot_id.to_string();
        self.storage_backend.delete_snapshot(snapshot_id).await
            .map_err(|e| SnapshotError::Generic {
                context: format!("Failed to delete snapshot {}: {}", snapshot_id_str, e),
            })
    }

    /// Get cleanup statistics
    pub async fn get_cleanup_statistics(&self) -> Result<CleanupStatistics, SnapshotError> {
        let state = self.cleanup_state.lock().await;
        
        Ok(CleanupStatistics {
            total_cleanup_runs: state.total_cleanup_runs,
            total_snapshots_removed: state.total_snapshots_removed,
            total_space_freed: state.total_space_freed,
            last_cleanup_time: state.last_cleanup_time,
            average_cleanup_duration: state.average_cleanup_duration,
        })
    }

    /// Schedule automatic cleanup
    pub async fn start_scheduled_cleanup(&self, interval: Duration) -> Result<(), SnapshotError> {
        info!("Starting scheduled cleanup with interval {:?}", interval);
        
        // This would start a background task that runs cleanup periodically
        // For now, this is a placeholder
        Ok(())
    }
}

// Configuration and data structures

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Configuration for automatic cleanup
pub struct CleanupConfig {
    // Age-based cleanup
    /// Enable age-based cleanup
    pub enable_age_based_cleanup: bool,
    /// Maximum age for full snapshots
    pub max_age_full_snapshots: Duration,
    /// Maximum age for incremental snapshots
    pub max_age_incremental_snapshots: Duration,
    /// Maximum age for checkpoint snapshots
    pub max_age_checkpoint_snapshots: Duration,
    /// Maximum age for epoch snapshots
    pub max_age_epoch_snapshots: Duration,

    // Count-based cleanup
    /// Enable count-based cleanup
    pub enable_count_based_cleanup: bool,
    /// Maximum number of full snapshots to keep
    pub max_full_snapshots: usize,
    /// Maximum number of incremental snapshots to keep
    pub max_incremental_snapshots: usize,
    /// Maximum number of checkpoint snapshots to keep
    pub max_checkpoint_snapshots: usize,
    /// Maximum number of epoch snapshots to keep
    pub max_epoch_snapshots: usize,

    // Size-based cleanup
    /// Enable size-based cleanup
    pub enable_size_based_cleanup: bool,
    /// Maximum total size of all snapshots
    pub max_total_snapshot_size: u64,

    // Redundancy cleanup
    /// Enable redundancy-based cleanup
    pub enable_redundancy_cleanup: bool,
    /// Maximum incremental snapshots per epoch
    pub max_incremental_per_epoch: usize,

    // Corruption cleanup
    /// Enable cleanup of corrupted snapshots
    pub enable_corruption_cleanup: bool,

    // Protection settings
    /// Minimum retention period for any snapshot
    pub minimum_retention_period: Duration,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enable_age_based_cleanup: true,
            max_age_full_snapshots: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
            max_age_incremental_snapshots: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_age_checkpoint_snapshots: Duration::from_secs(3 * 24 * 60 * 60), // 3 days
            max_age_epoch_snapshots: Duration::from_secs(90 * 24 * 60 * 60), // 90 days

            enable_count_based_cleanup: true,
            max_full_snapshots: 10,
            max_incremental_snapshots: 50,
            max_checkpoint_snapshots: 20,
            max_epoch_snapshots: 100,

            enable_size_based_cleanup: true,
            max_total_snapshot_size: 100 * 1024 * 1024 * 1024, // 100GB

            enable_redundancy_cleanup: true,
            max_incremental_per_epoch: 5,

            enable_corruption_cleanup: true,

            minimum_retention_period: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

#[derive(Debug, Clone)]
struct CleanupState {
    pub last_cleanup_time: Option<SystemTime>,
    pub total_cleanup_runs: u64,
    pub total_snapshots_removed: u64,
    pub total_space_freed: u64,
    pub average_cleanup_duration: Duration,
}

impl CleanupState {
    pub fn new() -> Self {
        Self {
            last_cleanup_time: None,
            total_cleanup_runs: 0,
            total_snapshots_removed: 0,
            total_space_freed: 0,
            average_cleanup_duration: Duration::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct SnapshotAnalysisResult {
    pub snapshots_by_age: HashMap<SnapshotType, Vec<(SnapshotId, Duration)>>,
    pub snapshots_by_type: HashMap<SnapshotType, Vec<SnapshotInfo>>,
    pub snapshots_by_epoch: HashMap<EpochId, Vec<SnapshotInfo>>,
    pub suspicious_snapshots: Vec<SnapshotId>,
    pub total_size: u64,
    pub total_count: usize,
}

impl SnapshotAnalysisResult {
    pub fn new() -> Self {
        Self {
            snapshots_by_age: HashMap::new(),
            snapshots_by_type: HashMap::new(),
            snapshots_by_epoch: HashMap::new(),
            suspicious_snapshots: Vec::new(),
            total_size: 0,
            total_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
/// Result of a cleanup operation
pub struct CleanupResult {
    /// Number of snapshots removed
    pub snapshots_removed: u32,
    /// Amount of disk space freed in bytes
    pub space_freed: u64,
    /// Time taken for cleanup operation
    pub cleanup_duration: Duration,
}

impl CleanupResult {
    /// Create a new empty cleanup result
    pub fn new() -> Self {
        Self {
            snapshots_removed: 0,
            space_freed: 0,
            cleanup_duration: Duration::default(),
        }
    }

    /// Merge another cleanup result into this one
    pub fn merge(&mut self, other: CleanupResult) {
        self.snapshots_removed += other.snapshots_removed;
        self.space_freed += other.space_freed;
    }
}

impl Default for CleanupResult {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct CleanupStatistics {
    pub total_cleanup_runs: u64,
    pub total_snapshots_removed: u64,
    pub total_space_freed: u64,
    pub last_cleanup_time: Option<SystemTime>,
    pub average_cleanup_duration: Duration,
}
