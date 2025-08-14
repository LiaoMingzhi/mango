// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, Result};
use tokio::sync::Mutex;
use tracing::{info, instrument};

use crate::authority::AuthorityState;
use crate::checkpoints::CheckpointStore;
use crate::authority_client::NetworkAuthorityClient;

use super::types::{
    ColdStartConfig, ColdStartResult, ColdStartError, ColdStartPhase, ColdStartState
};
use super::metrics::ColdStartMetrics;
use super::network::NetworkOperations;
use super::state::StateOperations;

/// Cold start manager
pub struct ColdStartManager {
    #[allow(dead_code)]
    config: ColdStartConfig,
    checkpoint_store: Arc<CheckpointStore>,
    network_ops: NetworkOperations,
    state_ops: StateOperations,
    metrics: Arc<ColdStartMetrics>,
    /// Current cold start state
    cold_start_state: Arc<Mutex<ColdStartState>>,
}

impl ColdStartManager {
    /// Create new cold start manager
    pub fn new(
        config: ColdStartConfig,
        checkpoint_store: Arc<CheckpointStore>,
        authority_state: Arc<AuthorityState>,
        network_client: Arc<NetworkAuthorityClient>,
        metrics: Arc<ColdStartMetrics>,
    ) -> Self {
        let network_ops = NetworkOperations::new(
            checkpoint_store.clone(),
            authority_state.clone(),
            network_client.clone(),
            metrics.clone(),
        );
        
        let state_ops = StateOperations::new(
            checkpoint_store.clone(),
            authority_state,
            network_client,
        );

        Self {
            config,
            checkpoint_store,
            network_ops,
            state_ops,
            metrics,
            cold_start_state: Arc::new(Mutex::new(ColdStartState::Idle)),
        }
    }

    /// Perform cold start
    #[instrument(level = "info", skip(self))]
    pub async fn perform_cold_start(&self) -> Result<ColdStartResult>
    where
        Self: Send + Sync,
    {
        let start_time = std::time::Instant::now();
        
        // Update metrics
        self.metrics.cold_start_operations_total.inc();
        self.metrics.cold_start_in_progress.set(1);
        
        // Check if cold start is already in progress
        {
            let state = self.cold_start_state.lock().await;
            if let ColdStartState::ColdStarting { .. } = *state {
                return Err(ColdStartError::ConsensusRestartFailed {
                    source: anyhow!("Another cold start operation is already in progress"),
                    epoch: 0, // Current operation phase does not involve specific epoch
                }.into());
            }
        }
        
        // Update cold start state
        {
            let mut state = self.cold_start_state.lock().await;
            *state = ColdStartState::ColdStarting {
                phase: ColdStartPhase::NodeDiscovery,
                start_time,
            };
        }
        
        let result = self.execute_cold_start().await;
        
        // Update final state
        {
            let mut state = self.cold_start_state.lock().await;
            match &result {
                Ok(ColdStartResult::Success { sync_source, latest_checkpoint, duration, .. }) => {
                    *state = ColdStartState::Completed {
                        sync_source: *sync_source,
                        latest_checkpoint: *latest_checkpoint,
                        duration: *duration,
                    };
                    self.metrics.cold_start_success_total.inc();
                }
                Err(_) => {
                    *state = ColdStartState::Failed {
                        error: format!("{:?}", result.as_ref().err()),
                        phase: ColdStartPhase::NodeDiscovery,
                    };
                    self.metrics.cold_start_failure_total.inc();
                }
                _ => {}
            }
        }
        
        self.metrics.cold_start_in_progress.set(0);
        self.metrics.cold_start_duration.observe(start_time.elapsed().as_secs_f64());
        
        result
    }

    /// Execute cold start operation
    async fn execute_cold_start(&self) -> Result<ColdStartResult> {
        let start_time = std::time::Instant::now();
        
        info!("Starting cold start execution");
        
        // 1. Discover healthy nodes in network (with timeout)
        self.update_cold_start_phase(ColdStartPhase::NodeDiscovery).await;
        let healthy_nodes = tokio::time::timeout(
            self.get_discovery_timeout(),
            self.network_ops.discover_healthy_nodes()
        ).await.map_err(|_| ColdStartError::ColdStartTimeout {
            duration: self.get_discovery_timeout(),
            phase: "node discovery".to_string(),
        })??;
        
        // 2. Select best sync source
        let sync_source = self.network_ops.select_best_sync_source(&healthy_nodes).await?;
        
        // 3. Sync latest state (with timeout)
        self.update_cold_start_phase(ColdStartPhase::StateSync).await;
        let latest_state = tokio::time::timeout(
            self.get_sync_timeout(),
            self.state_ops.sync_latest_state(&sync_source)
        ).await.map_err(|_| ColdStartError::ColdStartTimeout {
            duration: self.get_sync_timeout(),
            phase: "state sync".to_string(),
        })??;
        
        // 4. Recover local state
        self.state_ops.recover_local_state(&latest_state).await?;
        
        // 5. Restart consensus (with timeout)
        self.update_cold_start_phase(ColdStartPhase::ConsensusRestart).await;
        tokio::time::timeout(
            self.get_consensus_restart_timeout(),
            self.state_ops.restart_consensus(&latest_state)
        ).await.map_err(|_| ColdStartError::ColdStartTimeout {
            duration: self.get_consensus_restart_timeout(),
            phase: "consensus restart".to_string(),
        })??;
        
        // 6. Verify network connectivity
        self.update_cold_start_phase(ColdStartPhase::NetworkVerification).await;
        self.network_ops.verify_network_connectivity().await?;
        
        let duration = start_time.elapsed();
        info!(
            "Cold start completed: sync source {}, latest checkpoint {}, duration {:?}",
            sync_source.name, latest_state.latest_checkpoint.sequence_number(), duration
        );
        
        Ok(ColdStartResult::Success {
            sync_source: sync_source.name,
            latest_checkpoint: *latest_state.latest_checkpoint.sequence_number(),
            duration,
        })
    }

    /// Update cold start phase
    async fn update_cold_start_phase(&self, phase: ColdStartPhase) {
        let mut state = self.cold_start_state.lock().await;
        if let ColdStartState::ColdStarting { start_time, .. } = *state {
            *state = ColdStartState::ColdStarting {
                phase: phase.clone(),
                start_time,
            };
        }
        info!("Cold start entering phase: {:?}", phase);
    }

    /// Get current cold start state
    pub async fn get_cold_start_state(&self) -> ColdStartState {
        self.cold_start_state.lock().await.clone()
    }

    /// Get node discovery timeout configuration
    #[inline]
    fn get_discovery_timeout(&self) -> Duration {
        self.config.discovery_timeout
    }

    /// Get state sync timeout configuration
    #[inline]
    fn get_sync_timeout(&self) -> Duration {
        self.config.sync_timeout
    }

    /// Get consensus restart timeout configuration
    #[inline]
    fn get_consensus_restart_timeout(&self) -> Duration {
        self.config.consensus_restart_timeout
    }

    /// Get maximum retry attempts configuration
    #[inline]
    #[allow(dead_code)]
    fn get_max_retry_attempts(&self) -> u32 {
        self.config.max_retry_attempts
    }

    /// Async operation executor with retry
    #[allow(dead_code)]
    async fn execute_with_retry<F, T, E>(&self, operation: F, operation_name: &str) -> Result<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<T, E>> + Send + 'static>>,
        E: Into<anyhow::Error> + std::fmt::Display,
        T: Send,
    {
        let max_attempts = self.get_max_retry_attempts();
        let mut last_error: Option<anyhow::Error> = None;
        
        for attempt in 1..=max_attempts {
            match operation().await {
                Ok(result) => {
                    if attempt > 1 {
                        info!("{} succeeded after {} attempts", operation_name, attempt);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let error = e.into();
                    if attempt < max_attempts {
                        info!(
                            "{} attempt {} failed: {}, will retry",
                            operation_name, attempt, error
                        );
                        // Simple backoff strategy
                        const RETRY_DELAY_MS: u64 = 1000;
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
                    } else {
                        info!(
                            "{} failed after {} attempts: {}",
                            operation_name, max_attempts, error
                        );
                    }
                    last_error = Some(error);
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow!("{} retry attempts exhausted", operation_name)))
    }

    /// Get checkpoint store
    pub fn get_checkpoint_store(&self) -> &Arc<CheckpointStore> {
        &self.checkpoint_store
    }

    /// Cancel current cold start operation
    pub async fn cancel_cold_start(&self) -> Result<()> {
        let mut state = self.cold_start_state.lock().await;
        if let ColdStartState::ColdStarting { .. } = *state {
            *state = ColdStartState::Cancelled;
            info!("Cold start operation cancelled");
            Ok(())
        } else {
            Err(anyhow!("No cold start operation in progress"))
        }
    }
}
