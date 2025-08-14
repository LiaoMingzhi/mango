// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Health monitoring module
//! 
//! This module provides health monitoring and attack detection for Mango Network, including:
//! - Node health status checking
//! - Network anomaly detection
//! - Attack behavior identification
//! - Alert mechanisms

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use anyhow::{anyhow, Result};
use prometheus::{IntCounter, IntGauge, Histogram, register_int_counter, register_int_gauge, register_histogram};
use tracing::{info, error, instrument};

pub mod health_checker;
pub mod attack_detector;
pub mod alert_manager;

#[cfg(test)]
mod tests;

pub use health_checker::{HealthChecker, HealthStatus, HealthCheckConfig};
pub use attack_detector::{AttackDetector, AttackIndicator, AttackType};
pub use alert_manager::{AlertManager, AlertLevel, AlertConfig};

/// Health monitoring metrics
#[derive(Debug)]
pub struct HealthMetrics {
    /// Total number of health checks
    pub health_checks_total: IntCounter,
    /// Total number of failed health checks
    pub health_check_failures_total: IntCounter,
    /// Current health status (1: healthy, 0: unhealthy)
    pub current_health_status: IntGauge,
    /// Total number of attack detections
    pub attack_detections_total: IntCounter,
    /// Number of active attack indicators
    pub active_attack_indicators: IntGauge,
    /// Health check duration
    pub health_check_duration: Histogram,
    /// Total number of alerts sent
    pub alerts_sent_total: IntCounter,
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self {
            health_checks_total: register_int_counter!(
                "mgo_health_checks_total",
                "Total number of health checks"
            ).unwrap(),
            health_check_failures_total: register_int_counter!(
                "mgo_health_check_failures_total", 
                "Total number of failed health checks"
            ).unwrap(),
            current_health_status: register_int_gauge!(
                "mgo_current_health_status",
                "Current health status (1: healthy, 0: unhealthy)"
            ).unwrap(),
            attack_detections_total: register_int_counter!(
                "mgo_attack_detections_total",
                "Total number of attack detections"
            ).unwrap(),
            active_attack_indicators: register_int_gauge!(
                "mgo_active_attack_indicators",
                "Number of active attack indicators"
            ).unwrap(),
            health_check_duration: register_histogram!(
                "mgo_health_check_duration_seconds",
                "Health check duration (seconds)"
            ).unwrap(),
            alerts_sent_total: register_int_counter!(
                "mgo_alerts_sent_total",
                "Total number of alerts sent"
            ).unwrap(),
        }
    }
}

/// Monitor configuration
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Health check configuration
    pub health_check: HealthCheckConfig,
    /// Alert configuration
    pub alert: AlertConfig,
    /// Monitor interval
    pub monitor_interval: Duration,
    /// Whether to enable attack detection
    pub enable_attack_detection: bool,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            health_check: HealthCheckConfig::default(),
            alert: AlertConfig::default(),
            monitor_interval: Duration::from_secs(30),
            enable_attack_detection: true,
        }
    }
}

/// Monitor state
#[derive(Debug, Clone)]
pub enum MonitorState {
    /// Starting
    Starting,
    /// Running
    Running,
    /// Paused
    Paused,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
    /// Error state
    Error(String),
}

/// Health monitor manager
/// 
/// This is the main coordinator of the health monitoring system, responsible for:
/// - Performing regular health checks
/// - Detecting attack signs
/// - Sending alert notifications
/// - Maintaining monitoring state
pub struct HealthMonitor {
    config: MonitorConfig,
    health_checker: Arc<HealthChecker>,
    attack_detector: Arc<AttackDetector>,
    alert_manager: Arc<AlertManager>,
    metrics: Arc<HealthMetrics>,
    state: Arc<Mutex<MonitorState>>,
    stop_signal: Arc<Mutex<bool>>,
}

impl HealthMonitor {
    /// Create new health monitor manager
    pub fn new(
        config: MonitorConfig,
        health_checker: Arc<HealthChecker>,
        attack_detector: Arc<AttackDetector>, 
        alert_manager: Arc<AlertManager>,
        metrics: Arc<HealthMetrics>,
    ) -> Self {
        Self {
            config,
            health_checker,
            attack_detector,
            alert_manager,
            metrics,
            state: Arc::new(Mutex::new(MonitorState::Stopped)),
            stop_signal: Arc::new(Mutex::new(false)),
        }
    }

    /// Start health monitoring
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting health monitoring system");
        
        // Update state
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Starting;
        }

        // Reset stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // Start monitoring loop
        let monitor = Arc::new(self.clone());
        tokio::spawn(async move {
            if let Err(e) = monitor.run_monitor_loop().await {
                error!("Health monitoring failed: {:?}", e);
                let mut state = monitor.state.lock().await;
                *state = MonitorState::Error(format!("{:?}", e));
            }
        });

        // Update state to running
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Running;
        }

        info!("Health monitoring system started successfully");
        Ok(())
    }

    /// Stop health monitoring
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping health monitoring system");
        
        // Set stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // Update state
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Stopping;
        }

        // Wait for monitoring loop to end
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Update state to stopped
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Stopped;
        }

        info!("Health monitoring system stopped");
        Ok(())
    }

    /// Get current monitoring state
    pub async fn get_state(&self) -> MonitorState {
        let state = self.state.lock().await;
        state.clone()
    }

    /// Pause monitoring
    pub async fn pause(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        match *state {
            MonitorState::Running => {
                *state = MonitorState::Paused;
                info!("Health monitoring paused");
                Ok(())
            }
            _ => Err(anyhow!("Can only pause monitoring when running")),
        }
    }

    /// Resume monitoring
    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        match *state {
            MonitorState::Paused => {
                *state = MonitorState::Running;
                info!("Health monitoring resumed");
                Ok(())
            }
            _ => Err(anyhow!("Can only resume monitoring when paused")),
        }
    }

    /// Perform a complete health check and attack detection
    #[instrument(level = "debug", skip(self))]
    pub async fn perform_health_check(&self) -> Result<HealthStatus> {
        let start_time = Instant::now();
        
        // Execute health check
        let health_status = self.health_checker.check_node_health().await?;
        
        // Update metrics
        self.metrics.health_checks_total.inc();
        if !health_status.is_healthy() {
            self.metrics.health_check_failures_total.inc();
            self.metrics.current_health_status.set(0);
        } else {
            self.metrics.current_health_status.set(1);
        }
        
        self.metrics.health_check_duration
            .observe(start_time.elapsed().as_secs_f64());

        // If attack detection is enabled, perform attack detection
        if self.config.enable_attack_detection {
            if let Ok(indicators) = self.attack_detector.detect_attack_signs().await {
                self.metrics.attack_detections_total.inc();
                self.metrics.active_attack_indicators.set(indicators.len() as i64);
                
                // If attack signs detected, send alerts
                if !indicators.is_empty() {
                    self.send_attack_alerts(&indicators).await?;
                }
            }
        }

        // If health status is abnormal, send health alert
        if !health_status.is_healthy() {
            self.send_health_alert(&health_status).await?;
        }

        Ok(health_status)
    }

    /// Monitor main loop
    async fn run_monitor_loop(&self) -> Result<()> {
        info!("Starting health monitoring main loop");
        
        loop {
            // Check stop signal
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("Received stop signal, exiting monitor loop");
                    break;
                }
            }

            // Check state, only execute monitoring when running
            {
                let state = self.state.lock().await;
                if !matches!(*state, MonitorState::Running) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }

            // Execute health check
            if let Err(e) = self.perform_health_check().await {
                error!("Health check failed: {:?}", e);
            }

            // Wait for next check
            tokio::time::sleep(self.config.monitor_interval).await;
        }

        info!("Health monitoring main loop ended");
        Ok(())
    }

    /// Send attack alerts
    async fn send_attack_alerts(&self, indicators: &[AttackIndicator]) -> Result<()> {
        for indicator in indicators {
            let alert_level = match indicator.attack_type {
                AttackType::ConsensusAttack => AlertLevel::Critical,
                AttackType::NetworkAttack => AlertLevel::High,
                AttackType::StateCorruption => AlertLevel::Critical,
                AttackType::ResourceExhaustion => AlertLevel::Medium,
                AttackType::UnknownAttack => AlertLevel::Info,
            };

            let message = format!(
                "Attack signs detected: {} - {} (confidence: {:.2})",
                indicator.attack_type,
                indicator.description,
                indicator.confidence
            );

            self.alert_manager.send_alert(alert_level, &message).await?;
            self.metrics.alerts_sent_total.inc();
        }
        Ok(())
    }

    /// Send health alert
    async fn send_health_alert(&self, health_status: &HealthStatus) -> Result<()> {
        let message = format!(
            "Node health check failed: consensus_healthy={}, network_healthy={}, storage_healthy={}, execution_healthy={}",
            health_status.consensus_healthy,
            health_status.network_healthy,
            health_status.storage_healthy,
            health_status.execution_healthy
        );

        self.alert_manager.send_alert(AlertLevel::High, &message).await?;
        self.metrics.alerts_sent_total.inc();
        
        Ok(())
    }

    /// Get attack detector
    pub fn get_attack_detector(&self) -> &AttackDetector {
        &self.attack_detector
    }

    /// Get alert manager
    pub fn get_alert_manager(&self) -> &AlertManager {
        &self.alert_manager
    }

    /// Get configuration
    pub fn get_config(&self) -> &MonitorConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: MonitorConfig) {
        self.config = config;
        info!("Health monitor configuration updated");
    }
}

impl Clone for HealthMonitor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            health_checker: Arc::clone(&self.health_checker),
            attack_detector: Arc::clone(&self.attack_detector),
            alert_manager: Arc::clone(&self.alert_manager),
            metrics: Arc::clone(&self.metrics),
            state: Arc::clone(&self.state),
            stop_signal: Arc::clone(&self.stop_signal),
        }
    }
}