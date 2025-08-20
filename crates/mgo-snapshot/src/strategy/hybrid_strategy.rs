// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Hybrid snapshot strategy implementation


use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

use mgo_types::base_types::EpochId;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

use crate::core_integration::DatabaseAccessor;
use crate::incremental::IncrementalSnapshotManager;
use crate::manager::SnapshotManager;
use crate::types::{SnapshotId, SnapshotType};
use crate::types::error::SnapshotError;


/// Hybrid snapshot strategy that intelligently chooses between full and incremental snapshots
pub struct HybridSnapshotStrategy {
    snapshot_manager: Arc<SnapshotManager>,
    incremental_manager: Arc<IncrementalSnapshotManager>,
    db_accessor: Arc<DatabaseAccessor>,
    config: HybridStrategyConfig,
    strategy_state: tokio::sync::Mutex<HybridStrategyState>,
}

impl HybridSnapshotStrategy {
    /// Create a new hybrid snapshot strategy
    pub fn new(
        snapshot_manager: Arc<SnapshotManager>,
        incremental_manager: Arc<IncrementalSnapshotManager>,
        db_accessor: Arc<DatabaseAccessor>,
        config: HybridStrategyConfig,
    ) -> Self {
        Self {
            snapshot_manager,
            incremental_manager,
            db_accessor,
            config,
            strategy_state: tokio::sync::Mutex::new(HybridStrategyState::new()),
        }
    }

    /// Execute the hybrid strategy to create the most appropriate snapshot
    #[instrument(level = "info", skip(self))]
    pub async fn execute_strategy(&self) -> Result<SnapshotDecision, SnapshotError> {
        info!("Executing hybrid snapshot strategy");

        let mut state = self.strategy_state.lock().await;
        
        // Analyze current situation
        let analysis = self.analyze_current_state(&mut state).await?;
        
        // Make decision based on analysis
        let decision = self.make_snapshot_decision(&analysis, &mut state).await?;
        
        // Execute the decision
        let result = self.execute_decision(&decision, &mut state).await?;
        
        // Update strategy state
        self.update_strategy_state(&mut state, &result).await?;

        info!("Hybrid strategy executed: {:?}", decision.strategy_type);
        Ok(decision)
    }

    /// Analyze the current state to inform snapshot decisions
    async fn analyze_current_state(
        &self,
        state: &mut HybridStrategyState,
    ) -> Result<SnapshotAnalysis, SnapshotError> {
        debug!("Analyzing current state for snapshot decision");

        let current_time = SystemTime::now();
        let current_checkpoint = self.db_accessor.get_highest_verified_checkpoint()?
            .map(|cp| *cp.sequence_number())
            .unwrap_or(0);
        let current_epoch = self.db_accessor.get_current_epoch().await?;

        // Calculate time since last snapshot
        let time_since_last = state.last_snapshot_time
            .map(|last| current_time.duration_since(last).unwrap_or_default())
            .unwrap_or(Duration::from_secs(u64::MAX));

        // Calculate checkpoint progress
        let checkpoints_since_last = current_checkpoint.saturating_sub(state.last_snapshot_checkpoint);

        Ok(SnapshotAnalysis {
            current_time,
            current_checkpoint,
            current_epoch,
            time_since_last_snapshot: time_since_last,
            checkpoints_since_last_snapshot: checkpoints_since_last,
            incremental_chain_length: state.incremental_chain.len() as u32,
        })
    }

    /// Make a decision about what type of snapshot to create
    async fn make_snapshot_decision(
        &self,
        analysis: &SnapshotAnalysis,
        state: &mut HybridStrategyState,
    ) -> Result<SnapshotDecision, SnapshotError> {
        debug!("Making snapshot decision based on analysis");

        let mut score_full = 0.0f64;
        let mut score_incremental = 0.0f64;
        let mut reasons = Vec::new();

        // Time-based factors
        if analysis.time_since_last_snapshot >= self.config.max_time_between_full_snapshots {
            score_full += 100.0;
            reasons.push("Maximum time threshold reached".to_string());
        } else if analysis.time_since_last_snapshot >= self.config.incremental_snapshot_interval {
            score_incremental += 50.0;
            reasons.push("Incremental interval reached".to_string());
        }

        // Checkpoint-based factors
        if analysis.checkpoints_since_last_snapshot >= self.config.max_checkpoints_between_full {
            score_full += 80.0;
            reasons.push("Maximum checkpoint threshold reached".to_string());
        }

        // Incremental chain factors
        if analysis.incremental_chain_length >= self.config.max_incremental_chain_length {
            score_full += 90.0;
            reasons.push(format!("Incremental chain too long: {}", analysis.incremental_chain_length));
        }

        // Make final decision
        let strategy_type = if score_full > score_incremental {
            StrategyType::Full {
                include_history: false,
                compression_level: crate::types::CompressionLevel::Medium,
            }
        } else {
            StrategyType::Incremental {
                base_snapshot: state.last_full_snapshot_id
                    .ok_or_else(|| SnapshotError::StateCollection {
                        component: "strategy".to_string(),
                        details: "No base snapshot available for incremental".to_string(),
                    })?,
            }
        };

        Ok(SnapshotDecision {
            strategy_type,
            reasoning: reasons,
            confidence_score: (score_full.max(score_incremental) / 100.0).min(1.0),
        })
    }

    /// Execute the snapshot decision
    async fn execute_decision(
        &self,
        decision: &SnapshotDecision,
        _state: &mut HybridStrategyState,
    ) -> Result<SnapshotExecutionResult, SnapshotError> {
        debug!("Executing snapshot decision: {:?}", decision.strategy_type);

        let start_time = SystemTime::now();

        let result = match &decision.strategy_type {
            StrategyType::Full { include_history, compression_level } => {
                let snapshot_type = SnapshotType::Full {
                    include_history: *include_history,
                    compression_level: *compression_level,
                };

                let request = crate::manager::snapshot_manager::CreateSnapshotRequest {
                    snapshot_type,
                    checkpoint_seq: None,
                    epoch: None,
                    components: vec![
                        crate::types::ComponentType::AuthorityState,
                        crate::types::ComponentType::ObjectStore,
                        crate::types::ComponentType::TransactionStore,
                        crate::types::ComponentType::CheckpointStore,
                        crate::types::ComponentType::EpochStore,
                    ],
                    compress: true,
                    description: "Auto-created by hybrid strategy".to_string(),
                    tags: vec!["hybrid".to_string(), "full".to_string()],
                };
                let snapshot_id = self.snapshot_manager.create_snapshot(request).await?;

                SnapshotExecutionResult {
                    snapshot_id,
                    strategy_type: decision.strategy_type.clone(),
                    creation_duration: start_time.elapsed().unwrap_or_default(),
                    success: true,
                }
            }
            StrategyType::Incremental { base_snapshot } => {
                let snapshot_id = self.incremental_manager.create_incremental_snapshot(
                    *base_snapshot,
                    None,
                    None,
                    Some("Auto-created by hybrid strategy".to_string()),
                ).await?;

                SnapshotExecutionResult {
                    snapshot_id,
                    strategy_type: decision.strategy_type.clone(),
                    creation_duration: start_time.elapsed().unwrap_or_default(),
                    success: true,
                }
            }
        };

        Ok(result)
    }

    /// Update strategy state after snapshot creation
    async fn update_strategy_state(
        &self,
        state: &mut HybridStrategyState,
        result: &SnapshotExecutionResult,
    ) -> Result<(), SnapshotError> {
        state.last_snapshot_time = Some(SystemTime::now());
        state.last_snapshot_id = Some(result.snapshot_id);
        state.last_snapshot_checkpoint = self.db_accessor.get_highest_verified_checkpoint()?
            .map(|cp| *cp.sequence_number())
            .unwrap_or(0);
        state.last_snapshot_epoch = self.db_accessor.get_current_epoch().await?;

        match &result.strategy_type {
            StrategyType::Full { .. } => {
                state.last_full_snapshot_id = Some(result.snapshot_id);
                state.incremental_chain.clear();
            }
            StrategyType::Incremental { .. } => {
                state.incremental_chain.push(result.snapshot_id);
            }
        }

        state.total_snapshots_created += 1;
        Ok(())
    }
}

// Configuration and data structures

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Configuration for hybrid snapshot strategy
pub struct HybridStrategyConfig {
    /// Maximum time allowed between full snapshots
    pub max_time_between_full_snapshots: Duration,
    /// Interval between incremental snapshots
    pub incremental_snapshot_interval: Duration,
    /// Maximum checkpoints between full snapshots
    pub max_checkpoints_between_full: u64,
    /// Maximum length of incremental chain
    pub max_incremental_chain_length: u32,
}

impl Default for HybridStrategyConfig {
    fn default() -> Self {
        Self {
            max_time_between_full_snapshots: Duration::from_secs(24 * 60 * 60),
            incremental_snapshot_interval: Duration::from_secs(60 * 60),
            max_checkpoints_between_full: 10000,
            max_incremental_chain_length: 10,
        }
    }
}

#[derive(Debug, Clone)]
struct HybridStrategyState {
    pub last_snapshot_time: Option<SystemTime>,
    pub last_snapshot_id: Option<SnapshotId>,
    pub last_full_snapshot_id: Option<SnapshotId>,
    pub last_snapshot_checkpoint: CheckpointSequenceNumber,
    pub last_snapshot_epoch: EpochId,
    pub incremental_chain: Vec<SnapshotId>,
    pub total_snapshots_created: u64,
}

impl HybridStrategyState {
    pub fn new() -> Self {
        Self {
            last_snapshot_time: None,
            last_snapshot_id: None,
            last_full_snapshot_id: None,
            last_snapshot_checkpoint: 0,
            last_snapshot_epoch: 0,
            incremental_chain: Vec::new(),
            total_snapshots_created: 0,
        }
    }
}

#[derive(Debug, Clone)]
/// Analysis data used for snapshot decisions
pub struct SnapshotAnalysis {
    /// Current system time
    pub current_time: SystemTime,
    /// Current checkpoint sequence number
    pub current_checkpoint: CheckpointSequenceNumber,
    /// Current epoch ID
    pub current_epoch: EpochId,
    /// Time elapsed since last snapshot
    pub time_since_last_snapshot: Duration,
    /// Number of checkpoints since last snapshot
    pub checkpoints_since_last_snapshot: u64,
    /// Current length of incremental chain
    pub incremental_chain_length: u32,
}

#[derive(Debug, Clone)]
/// Type of snapshot strategy to execute
pub enum StrategyType {
    /// Full snapshot strategy
    Full {
        /// Whether to include transaction history
        include_history: bool,
        /// Compression level to use
        compression_level: crate::types::CompressionLevel,
    },
    /// Incremental snapshot strategy
    Incremental {
        /// Base snapshot to create incremental from
        base_snapshot: SnapshotId,
    },
}

#[derive(Debug, Clone)]
/// Decision about which snapshot strategy to use
pub struct SnapshotDecision {
    /// The chosen strategy type
    pub strategy_type: StrategyType,
    /// Reasoning behind the decision
    pub reasoning: Vec<String>,
    /// Confidence score (0.0 to 1.0)
    pub confidence_score: f64,
}

#[derive(Debug, Clone)]
/// Result of executing a snapshot strategy
pub struct SnapshotExecutionResult {
    /// ID of the created snapshot
    pub snapshot_id: SnapshotId,
    /// Strategy type that was executed
    pub strategy_type: StrategyType,
    /// Time taken to create the snapshot
    pub creation_duration: Duration,
    /// Whether the execution was successful
    pub success: bool,
}