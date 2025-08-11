// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Rollback module type definitions
//! 
//! This module contains all the data structures used in the rollback system.

use std::time::{Duration, Instant};
use mgo_types::digests::TransactionDigest;

/// Rollback configuration
#[derive(Debug, Clone)]
pub struct RollbackConfig {
    /// Whether to force rollback (skip some safety checks)
    pub force: bool,
    /// Rollback timeout duration
    pub timeout: Duration,
    /// Whether to automatically restart consensus after rollback
    pub auto_restart_consensus: bool,
    /// Whether to automatically sync network state after rollback
    pub auto_sync_network: bool,
}

impl Default for RollbackConfig {
    fn default() -> Self {
        Self {
            force: false,
            timeout: Duration::from_secs(300), // 5 minutes
            auto_restart_consensus: true,
            auto_sync_network: true,
        }
    }
}

/// Rollback operation result
#[derive(Debug, Clone)]
pub enum RollbackResult {
    /// Rollback successful
    Success {
        target_checkpoint: u64,
        reverted_transactions: u64,
        duration: Duration,
    },
    /// Rollback failed
    Failed {
        error: String,
        partial_rollback: bool,
    },
    /// Rollback cancelled
    Cancelled,
}

/// Rollback operation errors
#[derive(thiserror::Error, Debug)]
pub enum RollbackError {
    #[error("Checkpoint {checkpoint_seq} not found")]
    CheckpointNotFound {
        checkpoint_seq: u64,
    },
    
    #[error("Checkpoint {checkpoint_seq} invalid: {reason}")]
    InvalidCheckpoint {
        checkpoint_seq: u64,
        reason: String,
    },
    
    #[error("Rollback to checkpoint {checkpoint_seq} not feasible: {reason}")]
    RollbackNotFeasible {
        checkpoint_seq: u64,
        reason: String,
    },
    
    #[error("Consensus stop failed")]
    ConsensusStopFailed {
        #[source]
        source: anyhow::Error,
    },
    
    #[error("State rollback failed")]
    StateRollbackFailed {
        #[source]
        source: anyhow::Error,
    },
    
    #[error("Network state update failed")]
    NetworkStateUpdateFailed {
        #[source]
        source: anyhow::Error,
    },
    
    #[error("Consensus restart failed")]
    ConsensusRestartFailed {
        #[source]
        source: anyhow::Error,
    },
    
    #[error("Rollback operation timeout, duration: {duration:?}")]
    RollbackTimeout {
        duration: Duration,
    },
    
    #[error("Insufficient permissions: {required_permission}")]
    InsufficientPermissions {
        required_permission: String,
    },
    
    #[error("Insufficient storage: required {required} bytes, available {available} bytes")]
    InsufficientStorage {
        required: u64,
        available: u64,
    },
}

/// Current rollback state
#[derive(Debug, Clone)]
pub enum RollbackState {
    /// Idle state
    Idle,
    /// Currently rolling back
    RollingBack {
        target_checkpoint: u64,
        start_time: Instant,
    },
    /// Rollback completed
    Completed {
        target_checkpoint: u64,
        duration: Duration,
    },
    /// Rollback failed
    Failed {
        error: String,
        target_checkpoint: Option<u64>,
    },
    /// Rollback cancelled
    Cancelled,
}

/// Safety checkpoint structure for consensus rollback
#[derive(Debug, Clone)]
pub struct ConsensusSafetyCheckpoint {
    pub timestamp: Instant,
    pub epoch: u64,
    pub last_consensus_index: u64,
    pub pending_certificates: Vec<TransactionDigest>,
    pub consensus_message_count: u64,
}

/// Consensus health report structure
#[derive(Debug, Clone)]
pub struct ConsensusHealthReport {
    pub timestamp: Instant,
    pub overall_healthy: bool,
    pub index_health: bool,
    pub message_health: bool,
    pub consistency_health: bool,
    pub performance_health: bool,
    pub check_duration: Duration,
}

/// System state structure
#[derive(Debug)]
pub struct SystemState {
    pub current_epoch: u64,
    pub highest_checkpoint: u64,
    pub consensus_active: bool,
    pub execution_active: bool,
    pub checkpoint_processing_active: bool,
}

/// Consensus index analysis from multiple sources
#[derive(Debug, Clone)]
pub struct ConsensusIndexAnalysis {
    pub epoch_store_index: Option<u64>,
    pub checkpoint_store_index: Option<u64>,
    pub authority_state_index: Option<u64>,
    pub database_index: Option<u64>,
    pub max_index: u64,
    pub min_index: u64,
    pub avg_index: u64,
    pub median_index: u64,
    pub discrepancy: u64,
    pub confidence_score: f64,
    pub valid_sources_count: u32,
    pub consistency_level: f64,
    pub reliability_score: f64,
    pub optimal_index: u64,
}

impl ConsensusIndexAnalysis {
    pub fn new(
        epoch_store_index: Option<u64>,
        checkpoint_store_index: Option<u64>,
        authority_state_index: Option<u64>,
        database_index: Option<u64>,
    ) -> Self {
        // Collect valid indices
        let valid_indices: Vec<u64> = [
            epoch_store_index,
            checkpoint_store_index,
            authority_state_index,
            database_index,
        ]
        .iter()
        .filter_map(|&opt| opt)
        .collect();
        
        let valid_sources_count = valid_indices.len() as u32;
        
        if valid_indices.is_empty() {
            return Self {
                epoch_store_index,
                checkpoint_store_index,
                authority_state_index,
                database_index,
                max_index: 0,
                min_index: 0,
                avg_index: 0,
                median_index: 0,
                discrepancy: 0,
                confidence_score: 0.0,
                valid_sources_count: 0,
                consistency_level: 0.0,
                reliability_score: 0.0,
                optimal_index: 0,
            };
        }
        
        // Calculate statistics
        let max_index = valid_indices.iter().max().copied().unwrap_or(0);
        let min_index = valid_indices.iter().min().copied().unwrap_or(0);
        let avg_index = valid_indices.iter().sum::<u64>() / valid_indices.len() as u64;
        
        // Calculate median
        let mut sorted_indices = valid_indices.clone();
        sorted_indices.sort();
        let median_index = if sorted_indices.is_empty() {
            0
        } else {
            sorted_indices[sorted_indices.len() / 2]
        };
        
        let discrepancy = max_index - min_index;
        
        // Calculate consistency level (0.0 to 1.0)
        let consistency_level = if max_index == 0 {
            1.0
        } else {
            1.0 - (discrepancy as f64 / max_index as f64).min(1.0)
        };
        
        // Calculate confidence score based on source count and consistency
        let source_confidence = match valid_sources_count {
            4 => 1.0,      // All sources available
            3 => 0.85,     // Most sources available
            2 => 0.65,     // Some sources available
            1 => 0.4,      // Single source only
            _ => 0.0,      // No sources
        };
        
        let consistency_confidence = consistency_level * 0.8 + 0.2; // 0.2 to 1.0 range
        let confidence_score = (source_confidence + consistency_confidence) / 2.0;
        
        // Calculate reliability score
        let reliability_score = if valid_sources_count >= 3 && consistency_level > 0.8 {
            0.95 + (consistency_level - 0.8) * 0.25 // 0.95 to 1.0
        } else if valid_sources_count >= 2 && consistency_level > 0.6 {
            0.7 + (consistency_level - 0.6) * 1.25 // 0.7 to 0.95
        } else {
            consistency_level * 0.7 // 0.0 to 0.7
        };
        
        // Select optimal index (prefer median for robustness)
        let optimal_index = if valid_sources_count >= 3 {
            median_index
        } else if valid_sources_count == 2 {
            max_index // Conservative choice
        } else {
            valid_indices[0] // Single source
        };
        
        Self {
            epoch_store_index,
            checkpoint_store_index,
            authority_state_index,
            database_index,
            max_index,
            min_index,
            avg_index,
            median_index,
            discrepancy,
            confidence_score,
            valid_sources_count,
            consistency_level,
            reliability_score,
            optimal_index,
        }
    }
}

/// Message count analysis structure
#[derive(Debug)]
pub struct MessageCountAnalysis {
    pub calculated_total: u64,
    pub conservative_estimate: u64,
    pub sources_consistent: bool,
    pub confidence_level: f64, // 0.0 to 1.0
    pub discrepancy_detected: bool,
    pub max_discrepancy: u64,
}

/// Epoch progression metrics structure
#[derive(Debug, Clone)]
pub struct EpochProgressionMetrics {
    pub epoch: u64,
    pub highest_checkpoint: u64,
    pub pending_certificates: u64,
    pub participation_rate: f64,
    pub production_rate: f64,
    pub progression_score: f64,
}

impl EpochProgressionMetrics {
    pub fn new(
        epoch: u64,
        highest_checkpoint: u64,
        pending_certificates: u64,
        participation_rate: f64,
        production_rate: f64,
    ) -> Self {
        // Calculate progression score based on multiple factors
        let checkpoint_factor = (highest_checkpoint as f64).log2().max(1.0) / 20.0; // Logarithmic scaling
        let pending_factor = 1.0 - (pending_certificates as f64 / (highest_checkpoint as f64 + 1.0)).min(0.3);
        let progression_score = (participation_rate + production_rate + checkpoint_factor + pending_factor) / 4.0;
        
        Self {
            epoch,
            highest_checkpoint,
            pending_certificates,
            participation_rate,
            production_rate,
            progression_score,
        }
    }
}

/// State machine progression metrics structure
#[derive(Debug, Clone)]
pub struct StateMachineProgressionMetrics {
    pub epoch: u64,
    pub progression_rate: f64,
    pub stability_factor: f64,
    pub efficiency_multiplier: f64,
    pub overall_health: f64,
}

// Analysis structures for various consensus analysis operations

/// Trend pattern recognition characteristics structure
#[derive(Debug, Clone)]
pub struct TrendPatternRecognitionCharacteristics {
    pub epoch: u64,
    pub pattern_accuracy: f64,
    pub pattern_confidence: f64,
    pub pattern_stability: f64,
    pub pattern_complexity: f64,
    pub pattern_reliability: f64,
    pub overall_pattern_quality: f64,
}

/// Velocity trend characteristics structure
#[derive(Debug, Clone)]
pub struct VelocityTrendCharacteristics {
    pub epoch: u64,
    pub trend_direction: f64,
    pub trend_strength: f64,
    pub trend_consistency: f64,
    pub trend_momentum: f64,
    pub trend_reliability: f64,
    pub overall_trend_health: f64,
}

/// Progression velocity characteristics structure
#[derive(Debug, Clone)]
pub struct ProgressionVelocityCharacteristics {
    pub epoch: u64,
    pub base_velocity: f64,
    pub acceleration_factor: f64,
    pub momentum_factor: f64,
    pub optimization_factor: f64,
    pub stability_factor: f64,
    pub overall_velocity_health: f64,
}

/// Sequence pattern recognition characteristics structure
#[derive(Debug, Clone)]
pub struct SequencePatternRecognitionCharacteristics {
    pub epoch: u64,
    pub pattern_accuracy: f64,
    pub pattern_complexity: f64,
    pub pattern_stability: f64,
    pub pattern_efficiency: f64,
    pub pattern_adaptability: f64,
    pub overall_pattern_quality: f64,
}

/// Transition sequence characteristics structure
#[derive(Debug, Clone)]
pub struct TransitionSequenceCharacteristics {
    pub epoch: u64,
    pub sequence_integrity: f64,
    pub sequence_efficiency: f64,
    pub sequence_optimization: f64,
    pub sequence_predictability: f64,
    pub sequence_stability: f64,
    pub overall_sequence_quality: f64,
}

// Weight structures for various analysis algorithms

/// Trend weights structure
#[derive(Debug)]
pub struct TrendWeights {
    pub pattern_weight: f64,
    pub prediction_weight: f64,
    pub computation_weight: f64,
}

impl TrendWeights {
    pub fn calculate_for_epoch(epoch: u64) -> Self {
        if epoch == 0 {
            // Genesis epoch: prioritize prediction
            Self {
                pattern_weight: 0.25,
                prediction_weight: 0.45,
                computation_weight: 0.3,
            }
        } else if epoch < 10 {
            // Early epochs: balanced with prediction emphasis
            Self {
                pattern_weight: 0.3,
                prediction_weight: 0.4,
                computation_weight: 0.3,
            }
        } else if epoch < 30 {
            // Mature epochs: more emphasis on pattern and computation
            Self {
                pattern_weight: 0.45,
                prediction_weight: 0.25,
                computation_weight: 0.3,
            }
        } else {
            // Very mature epochs: balanced with pattern emphasis
            Self {
                pattern_weight: 0.5,
                prediction_weight: 0.2,
                computation_weight: 0.3,
            }
        }
    }
    
    pub fn total_weight(&self) -> f64 {
        self.pattern_weight + self.prediction_weight + self.computation_weight
    }
}

/// Velocity weights structure
#[derive(Debug)]
pub struct VelocityWeights {
    pub trend_weight: f64,
    pub optimization_weight: f64,
    pub performance_weight: f64,
}

/// Progression weights structure
#[derive(Debug)]
pub struct ProgressionWeights {
    pub velocity_weight: f64,
    pub consistency_weight: f64,
    pub acceleration_weight: f64,
}

/// Sequence weights structure
#[derive(Debug)]
pub struct SequenceWeights {
    pub pattern_weight: f64,
    pub optimization_weight: f64,
    pub integrity_weight: f64,
}

impl SequenceWeights {
    pub fn calculate_for_epoch(epoch: u64) -> Self {
        if epoch == 0 {
            // Genesis epoch: prioritize integrity
            Self {
                pattern_weight: 0.28,
                optimization_weight: 0.27,
                integrity_weight: 0.45,
            }
        } else if epoch < 12 {
            // Early epochs: balanced with integrity emphasis
            Self {
                pattern_weight: 0.3,
                optimization_weight: 0.3,
                integrity_weight: 0.4,
            }
        } else if epoch < 35 {
            // Mature epochs: more emphasis on pattern and optimization
            Self {
                pattern_weight: 0.42,
                optimization_weight: 0.35,
                integrity_weight: 0.23,
            }
        } else {
            // Very mature epochs: balanced with pattern emphasis
            Self {
                pattern_weight: 0.48,
                optimization_weight: 0.3,
                integrity_weight: 0.22,
            }
        }
    }
    
    pub fn total_weight(&self) -> f64 {
        self.pattern_weight + self.optimization_weight + self.integrity_weight
    }
}

/// Transition weights structure
#[derive(Debug)]
pub struct TransitionWeights {
    pub sequence_weight: f64,
    pub validation_weight: f64,
    pub consistency_weight: f64,
}

/// State machine weights structure
#[derive(Debug)]
pub struct StateMachineWeights {
    pub progression_weight: f64,
    pub transition_weight: f64,
    pub integrity_weight: f64,
    pub performance_weight: f64,
}

/// Consensus state weights structure
#[derive(Debug)]
pub struct ConsensusStateWeights {
    pub state_machine_weight: f64,
    pub round_tracking_weight: f64,
    pub commit_history_weight: f64,
}

/// Epoch consensus weights structure
#[derive(Debug)]
pub struct EpochConsensusWeights {
    pub state_weight: f64,
    pub committee_weight: f64,
    pub transaction_weight: f64,
    pub validator_weight: f64,
    pub boundary_weight: f64,
}

// Complex analysis structures

/// Trend patterns analysis structure
#[derive(Debug)]
pub struct TrendPatternsAnalysis {
    pub pattern_index: u64,
    pub prediction_index: u64,
    pub computation_index: u64,
    pub epoch: u64,
    pub max_index: u64,
    pub min_index: u64,
    pub avg_index: u64,
    pub median_index: u64,
    pub discrepancy: u64,
    pub consistency_level: f64,
    pub reliability_score: f64,
    pub pattern_quality_factor: f64,
    pub prediction_accuracy_factor: f64,
    pub computation_efficiency_factor: f64,
    pub overall_trend_health: f64,
}

impl TrendPatternsAnalysis {
    pub fn new(
        pattern_index: u64,
        prediction_index: u64,
        computation_index: u64,
        epoch: u64,
    ) -> Self {
        let indices = vec![pattern_index, prediction_index, computation_index];
        
        let max_index = indices.iter().max().copied().unwrap_or(0);
        let min_index = indices.iter().min().copied().unwrap_or(0);
        let avg_index = if !indices.is_empty() {
            indices.iter().sum::<u64>() / indices.len() as u64
        } else {
            0
        };
        
        // Calculate median
        let mut sorted_indices = indices.clone();
        sorted_indices.sort();
        let median_index = sorted_indices[sorted_indices.len() / 2];
        
        let discrepancy = max_index - min_index;
        let consistency_level = if max_index > 0 {
            1.0 - (discrepancy as f64 / max_index as f64)
        } else {
            1.0
        };
        
        // Calculate factors based on epoch
        let pattern_quality_factor = if epoch == 0 {
            0.42 // Genesis epoch pattern quality
        } else if epoch < 8 {
            0.52 + (epoch as f64 * 0.055) // Early epochs: 0.52-0.96 (developing quality)
        } else if epoch < 32 {
            0.96 + (epoch as f64 * 0.001) // Growing epochs: 0.96-0.992 (improving quality)
        } else {
            0.992 + (epoch as f64 * 0.0002).min(0.008) // Mature epochs: up to 1.0
        };
        
        let prediction_accuracy_factor = if epoch < 5 {
            0.74 + (epoch as f64 * 0.045) // Early prediction accuracy: 0.74-0.925
        } else if epoch < 25 {
            0.925 + (epoch as f64 * 0.003) // Improving prediction accuracy: 0.925-1.0
        } else {
            1.0 // High prediction accuracy
        };
        
        let computation_efficiency_factor = if epoch < 10 {
            0.68 + (epoch as f64 * 0.03) // Early computation efficiency: 0.68-0.95
        } else if epoch < 40 {
            0.95 + (epoch as f64 * 0.00125) // Growing computation efficiency: 0.95-1.0
        } else {
            1.0 // High computation efficiency
        };
        
        // Calculate overall trend health
        let overall_trend_health = (pattern_quality_factor + prediction_accuracy_factor + computation_efficiency_factor) / 3.0;
        
        // Calculate reliability score
        let epoch_maturity_bonus = if epoch >= 60 { 0.022 } else if epoch >= 20 { 0.012 } else { 0.0 };
        let reliability_score = (consistency_level * overall_trend_health + epoch_maturity_bonus).min(1.0);
        
        Self {
            pattern_index,
            prediction_index,
            computation_index,
            epoch,
            max_index,
            min_index,
            avg_index,
            median_index,
            discrepancy,
            consistency_level,
            reliability_score,
            pattern_quality_factor,
            prediction_accuracy_factor,
            computation_efficiency_factor,
            overall_trend_health,
        }
    }
}

// Additional analysis structures can be added here as needed...
// For now, we'll keep the core types that are most commonly used.
