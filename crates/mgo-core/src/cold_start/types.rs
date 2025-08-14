// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;
use std::collections::BTreeMap;

use mgo_types::base_types::AuthorityName;
use mgo_types::messages_checkpoint::{
    CheckpointSequenceNumber, VerifiedCheckpoint,
};
use mgo_types::committee::Committee;

/// Cold start configuration
#[derive(Debug, Clone)]
pub struct ColdStartConfig {
    /// Node discovery timeout
    pub discovery_timeout: Duration,
    /// State sync timeout
    pub sync_timeout: Duration,
    /// Consensus restart timeout
    pub consensus_restart_timeout: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Maximum retry attempts
    pub max_retry_attempts: u32,
    /// Whether to automatically execute cold start
    pub auto_cold_start: bool,
}

impl Default for ColdStartConfig {
    fn default() -> Self {
        Self {
            discovery_timeout: Duration::from_secs(60), // 1 minute
            sync_timeout: Duration::from_secs(300), // 5 minutes
            consensus_restart_timeout: Duration::from_secs(120), // 2 minutes
            health_check_interval: Duration::from_secs(10), // 10 seconds
            max_retry_attempts: 3,
            auto_cold_start: false,
        }
    }
}

/// Cold start result
#[derive(Debug, Clone)]
pub enum ColdStartResult {
    /// Cold start successful
    Success {
        sync_source: AuthorityName,
        latest_checkpoint: CheckpointSequenceNumber,
        duration: Duration,
    },
    /// Cold start failed
    Failed {
        error: String,
        phase: ColdStartPhase,
    },
    /// Cold start cancelled
    Cancelled,
}

/// Cold start phase
#[derive(Debug, Clone)]
pub enum ColdStartPhase {
    /// Node discovery phase
    NodeDiscovery,
    /// State sync phase
    StateSync,
    /// Consensus restart phase
    ConsensusRestart,
    /// Network verification phase
    NetworkVerification,
}

/// Cold start error types
#[derive(Debug, thiserror::Error)]
pub enum ColdStartError {
    #[error("No healthy nodes found: scanned {scanned_nodes} nodes")]
    NoHealthyNodesFound {
        scanned_nodes: usize,
    },
    
    #[error("State sync failed")]
    StateSyncFailed {
        #[source]
        source: anyhow::Error,
        phase: String,
    },
    
    #[error("Consensus restart failed")]
    ConsensusRestartFailed {
        #[source]
        source: anyhow::Error,
        epoch: u64,
    },
    
    #[error("Network verification failed: {connectivity_rate:.1}% nodes reachable ({reachable}/{total})")]
    NetworkVerificationFailed {
        connectivity_rate: f64,
        reachable: usize,
        total: usize,
    },
    
    #[error("Cold start operation timeout, duration: {duration:?}, phase: {phase}")]
    ColdStartTimeout {
        duration: Duration,
        phase: String,
    },
    
    #[error("Insufficient permissions: {required_permission}")]
    InsufficientPermissions {
        required_permission: String,
    },
    
    #[error("Node {node_name} connection failed")]
    NodeConnectionFailed {
        node_name: String,
        #[source]
        source: anyhow::Error,
    },
}

/// Network node information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkNode {
    /// Node name
    pub name: AuthorityName,
    /// Node address
    pub address: String,
    /// Node health status
    pub is_healthy: bool,
    /// Latest checkpoint
    pub latest_checkpoint: Option<CheckpointSequenceNumber>,
    /// Latency
    pub latency: Duration,
}

impl NetworkNode {
    /// Create new network node information
    pub fn new(
        name: AuthorityName,
        address: String,
        is_healthy: bool,
        latest_checkpoint: Option<CheckpointSequenceNumber>,
        latency: Duration,
    ) -> Self {
        Self {
            name,
            address,
            is_healthy,
            latest_checkpoint,
            latency,
        }
    }

    /// Check if node is healthy
    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.is_healthy
    }

    /// Get node latency
    #[inline]
    pub fn latency(&self) -> Duration {
        self.latency
    }
}

/// Network state
#[derive(Debug, Clone)]
pub struct NetworkState {
    /// Latest checkpoint
    pub latest_checkpoint: VerifiedCheckpoint,
    /// Network committee
    pub committee: Committee,
    /// Healthy nodes list
    pub healthy_nodes: Vec<NetworkNode>,
}

impl NetworkState {
    pub fn new(latest_checkpoint: VerifiedCheckpoint) -> Self {
        Self {
            latest_checkpoint,
            committee: Committee::new(0, BTreeMap::new()), // TODO: get committee from checkpoint
            healthy_nodes: vec![],
        }
    }
}

/// Cold start state
#[derive(Debug, Clone)]
pub enum ColdStartState {
    /// Idle state
    Idle,
    /// Cold starting
    ColdStarting {
        phase: ColdStartPhase,
        start_time: std::time::Instant,
    },
    /// Cold start completed
    Completed {
        sync_source: AuthorityName,
        latest_checkpoint: CheckpointSequenceNumber,
        duration: Duration,
    },
    /// Cold start failed
    Failed {
        error: String,
        phase: ColdStartPhase,
    },
    /// Cold start cancelled
    Cancelled,
}
