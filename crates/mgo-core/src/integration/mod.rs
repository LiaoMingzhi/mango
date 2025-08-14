// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! System Integration Module
//! 
//! Integrates rollback manager, cold start manager and health monitoring system 
//! into a unified high availability management system

use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use tokio::sync::Mutex;
use tracing::{info, warn, error, instrument};
use prometheus::Registry;

use crate::authority::AuthorityState;
use crate::checkpoints::CheckpointStore;
use crate::authority_client::NetworkAuthorityClient;
use crate::rollback::{RollbackManager, RollbackConfig, RollbackMetrics, RollbackResult};
use crate::cold_start::{ColdStartManager, ColdStartConfig, ColdStartMetrics, ColdStartResult};
use crate::health_monitor::{
    HealthMonitor, HealthChecker, AttackDetector, AlertManager,
    HealthMetrics, MonitorConfig, HealthStatus, AlertLevel,
};
use crate::health_monitor::attack_detector::AttackDetectionConfig;

#[cfg(test)]
mod tests;

/// High availability system configuration
#[derive(Debug, Clone)]
pub struct HighAvailabilityConfig {
    /// Rollback configuration
    pub rollback: RollbackConfig,
    /// Cold start configuration
    pub cold_start: ColdStartConfig,
    /// Monitor configuration
    pub monitor: MonitorConfig,
    /// Enable automatic fault recovery
    pub enable_auto_recovery: bool,
    /// Auto recovery trigger threshold
    pub auto_recovery_threshold: u8,
    /// Fault recovery interval
    pub recovery_interval: Duration,
    /// Maximum consecutive recovery attempts
    pub max_recovery_attempts: u32,
}

impl Default for HighAvailabilityConfig {
    fn default() -> Self {
        Self {
            rollback: RollbackConfig::default(),
            cold_start: ColdStartConfig::default(),
            monitor: MonitorConfig::default(),
            enable_auto_recovery: true,
            auto_recovery_threshold: 3, // Trigger recovery after 3 consecutive health check failures
            recovery_interval: Duration::from_secs(300), // 5 minutes
            max_recovery_attempts: 3,
        }
    }
}

/// System state
#[derive(Debug, Clone, PartialEq)]
pub enum SystemState {
    /// Running normally
    Healthy,
    /// Health degraded, needs attention
    Degraded,
    /// System abnormal, needs intervention
    Unhealthy,
    /// Recovery in progress
    Recovering,
    /// System stopped
    Stopped,
    /// Error state
    Error(String),
}

/// Recovery strategy
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStrategy {
    /// No recovery needed
    None,
    /// Restart services
    RestartServices,
    /// Execute rollback
    Rollback {
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
    },
    /// Execute cold start
    ColdStart,
    /// Hybrid strategy (rollback first then cold start)
    Hybrid {
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
    },
}

/// High availability manager
/// 
/// Unified management of rollback, cold start and health monitoring functions,
/// providing automatic fault detection and recovery capabilities
pub struct HighAvailabilityManager {
    config: HighAvailabilityConfig,
    rollback_manager: Arc<RollbackManager>,
    cold_start_manager: Arc<ColdStartManager>,
    health_monitor: Arc<HealthMonitor>,
    system_state: Arc<Mutex<SystemState>>,
    recovery_attempts: Arc<Mutex<u32>>,
    last_recovery_time: Arc<Mutex<Option<std::time::Instant>>>,
    stop_signal: Arc<Mutex<bool>>,
}

impl HighAvailabilityManager {
    /// Create a new high availability manager
    pub fn new(
        config: HighAvailabilityConfig,
        authority_state: Arc<AuthorityState>,
        checkpoint_store: Arc<CheckpointStore>,
        network_client: Arc<NetworkAuthorityClient>,
        registry: &Registry,
    ) -> Result<Self> {
        // Create metrics for each component
        let rollback_metrics = RollbackMetrics::new(registry);
        let cold_start_metrics = ColdStartMetrics::new(registry);
        let health_metrics = Arc::new(HealthMetrics::default());

        // Create rollback manager
        let rollback_manager = Arc::new(RollbackManager::new(
            config.rollback.clone(),
            checkpoint_store.clone(),
            authority_state.clone(),
            network_client.clone(),
            rollback_metrics,
        ));

        // Create cold start manager
        let cold_start_manager = Arc::new(ColdStartManager::new(
            config.cold_start.clone(),
            checkpoint_store.clone(),
            authority_state.clone(),
            network_client.clone(),
            cold_start_metrics,
        ));

        // Create health monitoring components
        let health_checker = Arc::new(HealthChecker::new(
            config.monitor.health_check.clone(),
            authority_state.clone(),
            checkpoint_store.clone(),
        ));

        let attack_detector = Arc::new(AttackDetector::new(
            AttackDetectionConfig::default(),
            authority_state,
            checkpoint_store,
        ));

        let alert_manager = Arc::new(AlertManager::new(
            config.monitor.alert.clone(),
        ));

        // Create health monitoring manager
        let health_monitor = Arc::new(HealthMonitor::new(
            config.monitor.clone(),
            health_checker,
            attack_detector,
            alert_manager,
            health_metrics,
        ));

        Ok(Self {
            config,
            rollback_manager,
            cold_start_manager,
            health_monitor,
            system_state: Arc::new(Mutex::new(SystemState::Stopped)),
            recovery_attempts: Arc::new(Mutex::new(0)),
            last_recovery_time: Arc::new(Mutex::new(None)),
            stop_signal: Arc::new(Mutex::new(false)),
        })
    }

    /// Start the high availability manager
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting high availability manager");

        // Reset stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // Update system state
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Healthy;
        }

        // Start all components
        self.health_monitor.start().await?;
        info!("Health monitoring system started");

        // Start auto recovery loop
        if self.config.enable_auto_recovery {
            let manager = Arc::new(self.clone());
            tokio::spawn(async move {
                if let Err(e) = manager.run_auto_recovery_loop().await {
                    error!("Auto recovery loop execution failed: {:?}", e);
                }
            });
            info!("Auto recovery system started");
        }

        info!("High availability manager started successfully");
        Ok(())
    }

    /// Stop the high availability manager
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping high availability manager");

        // Set stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // Stop health monitoring
        self.health_monitor.stop().await?;

        // Update system state
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Stopped;
        }

        info!("High availability manager stopped");
        Ok(())
    }

    /// Get current system state
    pub async fn get_system_state(&self) -> SystemState {
        let state = self.system_state.lock().await;
        state.clone()
    }

    /// Manually perform health check
    pub async fn perform_health_check(&self) -> Result<HealthStatus> {
        self.health_monitor.perform_health_check().await
    }

    /// Manually execute rollback
    pub async fn execute_rollback(
        &self,
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
        force: bool,
    ) -> Result<RollbackResult> {
        info!("Manually executing rollback to checkpoint {}", target_checkpoint);
        
        // Update system state
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Recovering;
        }

        let result = self.rollback_manager.rollback_to_checkpoint(target_checkpoint, force).await;

        // Update system state based on result
        {
            let mut state = self.system_state.lock().await;
            match &result {
                Ok(_) => *state = SystemState::Healthy,
                Err(_) => *state = SystemState::Error("Rollback failed".to_string()),
            }
        }

        result
    }

    /// Manually execute cold start
    pub async fn execute_cold_start(&self) -> Result<ColdStartResult> {
        info!("Manually executing cold start");
        
        // Update system state
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Recovering;
        }

        let result = self.cold_start_manager.perform_cold_start().await;

        // Update system state based on result
        {
            let mut state = self.system_state.lock().await;
            match &result {
                Ok(_) => *state = SystemState::Healthy,
                Err(_) => *state = SystemState::Error("Cold start failed".to_string()),
            }
        }

        result
    }

    /// Analyze system health status and determine recovery strategy
    async fn analyze_recovery_strategy(&self, health_status: &HealthStatus) -> RecoveryStrategy {
        let health_score = health_status.health_score();

        if health_score >= 75 {
            // Good health, no recovery needed
            return RecoveryStrategy::None;
        }

        if health_score >= 50 {
            // Medium health, try restarting services
            return RecoveryStrategy::RestartServices;
        }

        // Check attack indicators
        if let Ok(indicators) = self.health_monitor.get_attack_detector().detect_attack_signs().await {
            if !indicators.is_empty() {
                // Attack detected, choose strategy based on attack type
                for indicator in &indicators {
                    if indicator.is_critical() {
                        // Critical attack, execute hybrid recovery strategy
                        if let Ok(latest_checkpoint) = self.cold_start_manager.get_checkpoint_store()
                            .get_highest_executed_checkpoint() {
                            if let Some(checkpoint) = latest_checkpoint {
                                return RecoveryStrategy::Hybrid {
                                    target_checkpoint: *checkpoint.sequence_number(),
                                };
                            }
                        }
                        return RecoveryStrategy::ColdStart;
                    }
                }
            }
        }

        // Low health, try rollback
        if let Ok(latest_checkpoint) = self.cold_start_manager.get_checkpoint_store()
            .get_highest_executed_checkpoint() {
            if let Some(checkpoint) = latest_checkpoint {
                if *checkpoint.sequence_number() > 0 {
                    return RecoveryStrategy::Rollback {
                        target_checkpoint: checkpoint.sequence_number() - 1,
                    };
                }
            }
        }

        // Last resort: cold start
        RecoveryStrategy::ColdStart
    }

    /// Execute recovery strategy
    async fn execute_recovery_strategy(&self, strategy: &RecoveryStrategy) -> Result<()> {
        match strategy {
            RecoveryStrategy::None => {
                info!("System healthy, no recovery needed");
                Ok(())
            }
            RecoveryStrategy::RestartServices => {
                info!("Restarting services to restore system health");
                // Service restart logic can be added here
                // Currently simplified to send alert
                                        self.health_monitor.get_alert_manager()
                    .send_alert(AlertLevel::Warning, "Recommend restarting services to restore system health")
                    .await?;
                Ok(())
            }
            RecoveryStrategy::Rollback { target_checkpoint } => {
                info!("Executing rollback to checkpoint {} to restore system", target_checkpoint);
                self.rollback_manager
                    .rollback_to_checkpoint(*target_checkpoint, false)
                    .await?;
                Ok(())
            }
            RecoveryStrategy::ColdStart => {
                info!("Executing cold start to restore system");
                self.cold_start_manager.perform_cold_start().await?;
                Ok(())
            }
            RecoveryStrategy::Hybrid { target_checkpoint } => {
                info!("Executing hybrid recovery strategy: rollback to checkpoint {} first, then cold start", target_checkpoint);
                
                // Execute rollback first
                match self.rollback_manager
                    .rollback_to_checkpoint(*target_checkpoint, false)
                    .await {
                    Ok(_) => {
                        info!("Rollback successful, starting cold start");
                        // Wait for system to stabilize
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        // Execute cold start
                        self.cold_start_manager.perform_cold_start().await?;
                    }
                    Err(e) => {
                        warn!("Rollback failed: {:?}, executing cold start directly", e);
                        self.cold_start_manager.perform_cold_start().await?;
                    }
                }
                Ok(())
            }
        }
    }

    /// Auto recovery main loop
    async fn run_auto_recovery_loop(&self) -> Result<()> {
        info!("Starting auto recovery monitoring loop");
        
        let mut consecutive_failures = 0u8;
        
        loop {
            // Check stop signal
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("Received stop signal, exiting auto recovery loop");
                    break;
                }
            }

            // Check if need to wait for recovery interval
            {
                let last_recovery = self.last_recovery_time.lock().await;
                if let Some(last_time) = *last_recovery {
                    if last_time.elapsed() < self.config.recovery_interval {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                }
            }

            // Execute health check
            match self.health_monitor.perform_health_check().await {
                Ok(health_status) => {
                    let health_score = health_status.health_score();
                    
                    if health_status.is_healthy() {
                        // System healthy, reset failure count
                        consecutive_failures = 0;
                        
                        // Update system state
                        {
                            let mut state = self.system_state.lock().await;
                            if *state != SystemState::Healthy {
                                *state = SystemState::Healthy;
                                info!("System has restored to healthy state");
                            }
                        }
                    } else {
                        // System unhealthy, increase failure count
                        consecutive_failures += 1;
                        
                        warn!(
                            "Health check failed ({}/{}): health score {}/100",
                            consecutive_failures,
                            self.config.auto_recovery_threshold,
                            health_score
                        );

                        // Update system state
                        {
                            let mut state = self.system_state.lock().await;
                            if health_score >= 50 {
                                *state = SystemState::Degraded;
                            } else {
                                *state = SystemState::Unhealthy;
                            }
                        }

                        // Check if auto recovery needs to be triggered
                        if consecutive_failures >= self.config.auto_recovery_threshold {
                            // Check recovery attempt limit
                            let current_attempts = {
                                let attempts = self.recovery_attempts.lock().await;
                                *attempts
                            };

                            if current_attempts < self.config.max_recovery_attempts {
                                info!("Triggering auto recovery (attempt {}/{})", current_attempts + 1, self.config.max_recovery_attempts);
                                
                                // Update system state to recovering
                                {
                                    let mut state = self.system_state.lock().await;
                                    *state = SystemState::Recovering;
                                }

                                // Analyze and execute recovery strategy
                                let strategy = self.analyze_recovery_strategy(&health_status).await;
                                info!("Selected recovery strategy: {:?}", strategy);

                                match self.execute_recovery_strategy(&strategy).await {
                                    Ok(()) => {
                                        info!("Auto recovery executed successfully");
                                        consecutive_failures = 0;
                                        
                                        // Reset recovery attempt counter
                                        {
                                            let mut attempts = self.recovery_attempts.lock().await;
                                            *attempts = 0;
                                        }
                                    }
                                                                        Err(e) => {
                                        error!("Auto recovery execution failed: {:?}", e);
                                        
                                        // Increase recovery attempt counter
                                        {
                                            let mut attempts = self.recovery_attempts.lock().await;
                                            *attempts += 1;
                                        }

                                        // Send critical alert
                                        let _ = self.health_monitor.get_alert_manager()
                                            .send_alert(
                                                AlertLevel::Critical,
                                                &format!("Auto recovery failed: {:?}", e)
                                            ).await;
                                    }
                                }

                                // Update last recovery time
                                {
                                    let mut last_recovery = self.last_recovery_time.lock().await;
                                    *last_recovery = Some(std::time::Instant::now());
                                }
                            } else {
                                error!(
                                    "Maximum recovery attempts reached ({}), stopping auto recovery",
                                    self.config.max_recovery_attempts
                                );
                                
                                // Send critical alert
                                let _ = self.health_monitor.get_alert_manager()
                                    .send_alert(
                                        AlertLevel::Critical,
                                        "Maximum auto recovery attempts reached, manual intervention required"
                                    ).await;

                                // Update system state to error
                                {
                                    let mut state = self.system_state.lock().await;
                                    *state = SystemState::Error("Maximum recovery attempts reached".to_string());
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Health check execution failed: {:?}", e);
                    consecutive_failures += 1;
                }
            }

            // Wait for next check
            tokio::time::sleep(self.config.monitor.monitor_interval).await;
        }

        info!("Auto recovery monitoring loop ended");
        Ok(())
    }

    /// Reset recovery attempt counter
    pub async fn reset_recovery_attempts(&self) {
        let mut attempts = self.recovery_attempts.lock().await;
        *attempts = 0;
        info!("Recovery attempt counter has been reset");
    }

    /// Get recovery attempt count
    pub async fn get_recovery_attempts(&self) -> u32 {
        let attempts = self.recovery_attempts.lock().await;
        *attempts
    }

    /// Get configuration
    pub fn get_config(&self) -> &HighAvailabilityConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: HighAvailabilityConfig) {
        self.config = config;
        info!("High availability manager configuration updated");
    }
}

impl Clone for HighAvailabilityManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            rollback_manager: Arc::clone(&self.rollback_manager),
            cold_start_manager: Arc::clone(&self.cold_start_manager),
            health_monitor: Arc::clone(&self.health_monitor),
            system_state: Arc::clone(&self.system_state),
            recovery_attempts: Arc::clone(&self.recovery_attempts),
            last_recovery_time: Arc::clone(&self.last_recovery_time),
            stop_signal: Arc::clone(&self.stop_signal),
        }
    }
}