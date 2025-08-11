// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Rollback module
//! 
//! This module provides comprehensive rollback functionality for the Mango Network.
//! It supports rolling back the blockchain state to a previous checkpoint with
//! advanced safety measures, health monitoring, and consensus coordination.
//! 
//! ## Module Organization
//! 
//! - `types`: Core data structures and type definitions
//! - `analysis`: Analysis algorithms and data processing
//! - `consensus`: Consensus-related rollback operations
//! - `health`: Health monitoring and diagnostics
//! - `manager`: Main rollback manager implementation
//! - `tests`: Unit and integration tests
//! 
//! ## Key Features
//! 
//! - **Safe Rollback**: Multi-stage validation and safety checkpoints
//! - **Health Monitoring**: Comprehensive system health checks
//! - **Consensus Coordination**: Proper consensus state management
//! - **Advanced Analysis**: Multi-source data analysis and pattern recognition
//! - **Metrics Integration**: Prometheus metrics for monitoring
//! - **Configurable Operations**: Flexible configuration options
//! 
//! ## Usage Example
//! 
//! ```rust,no_run
//! use mgo_core::rollback::{RollbackManager, RollbackConfig, RollbackMetrics};
//! use std::sync::Arc;
//! use prometheus::Registry;
//! 
//! async fn example_rollback() -> anyhow::Result<()> {
//!     let config = RollbackConfig::default();
//!     let registry = Registry::new();
//!     let metrics = RollbackMetrics::new(&registry);
//!     
//!     // Create rollback manager (checkpoint_store, authority_state, network_client needed)
//!     // let manager = RollbackManager::new(config, checkpoint_store, authority_state, network_client, metrics);
//!     
//!     // Perform rollback to checkpoint 100
//!     // let result = manager.rollback_to_checkpoint(100.into(), false).await?;
//!     
//!     Ok(())
//! }
//! ```

// Public module declarations
pub mod types;
pub mod analysis;
pub mod consensus;
pub mod health;
pub mod manager;

// Re-export commonly used types and structs
pub use types::{
    RollbackConfig, RollbackResult, RollbackError, RollbackState,
    ConsensusSafetyCheckpoint, ConsensusHealthReport, SystemState,
    ConsensusIndexAnalysis, MessageCountAnalysis,
    TrendPatternRecognitionCharacteristics, VelocityTrendCharacteristics,
    ProgressionVelocityCharacteristics, SequencePatternRecognitionCharacteristics,
    TransitionSequenceCharacteristics, EpochProgressionMetrics,
};

pub use analysis::RollbackAnalysis;
pub use consensus::{RollbackConsensus, VerifiedCheckpoint};
pub use health::{RollbackHealth, HealthSummary};
pub use manager::{RollbackManager, RollbackMetrics};

// Tests module (only included in test builds)
#[cfg(test)]
pub mod tests;
