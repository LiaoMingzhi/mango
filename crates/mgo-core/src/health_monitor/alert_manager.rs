// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Alert manager module
//! 
//! Responsible for managing and sending various alert notifications, including:
//! - Health status alerts
//! - Attack detection alerts
//! - System exception alerts
//! - Performance monitoring alerts

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use anyhow::{anyhow, Result};
use tracing::{info, warn, error, debug, instrument};
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;

/// Alert level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertLevel {
    /// Info level - General information notification
    Info,
    /// Warning level - Requires attention but not urgent
    Warning,
    /// Medium level - Requires timely handling
    Medium,
    /// High level - Requires immediate attention
    High,
    /// Critical level - Requires immediate handling
    Critical,
}

impl AlertLevel {
    /// Get numeric representation of alert level (1-5)
    pub fn to_number(&self) -> u8 {
        match self {
            AlertLevel::Info => 1,
            AlertLevel::Warning => 2,
            AlertLevel::Medium => 3,
            AlertLevel::High => 4,
            AlertLevel::Critical => 5,
        }
    }

    /// Create alert level from numeric value
    pub fn from_number(level: u8) -> Self {
        match level {
            1 => AlertLevel::Info,
            2 => AlertLevel::Warning,
            3 => AlertLevel::Medium,
            4 => AlertLevel::High,
            5 | _ => AlertLevel::Critical,
        }
    }

    /// Check if it's an urgent alert
    pub fn is_urgent(&self) -> bool {
        matches!(self, AlertLevel::High | AlertLevel::Critical)
    }
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Info => write!(f, "Info"),
            AlertLevel::Warning => write!(f, "Warning"),
            AlertLevel::Medium => write!(f, "Medium"),
            AlertLevel::High => write!(f, "High"),
            AlertLevel::Critical => write!(f, "Critical"),
        }
    }
}

/// Alert notification channel
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertChannel {
    /// Log recording
    Log,
    /// Console output
    Console,
    /// File output
    File(String),
    /// Email notification (email address)
    Email(String),
    /// Webhook notification (URL)
    Webhook(String),
    /// Custom notification
    Custom(String),
}

impl std::fmt::Display for AlertChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertChannel::Log => write!(f, "Log"),
            AlertChannel::Console => write!(f, "Console"),
            AlertChannel::File(path) => write!(f, "File: {}", path),
            AlertChannel::Email(email) => write!(f, "Email: {}", email),
            AlertChannel::Webhook(url) => write!(f, "Webhook: {}", url),
            AlertChannel::Custom(name) => write!(f, "Custom: {}", name),
        }
    }
}

/// Alert message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert ID
    pub id: String,
    /// Alert level
    pub level: AlertLevel,
    /// Alert title
    pub title: String,
    /// Alert message content
    pub message: String,
    /// Creation time
    pub created_at: SystemTime,
    /// Send time
    pub sent_at: Option<SystemTime>,
    /// Alert source
    pub source: String,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
    /// Whether acknowledged
    pub acknowledged: bool,
    /// Acknowledgment time
    pub acknowledged_at: Option<SystemTime>,
    /// Retry count
    pub retry_count: u32,
}

impl Alert {
    /// Create new alert
    pub fn new(
        level: AlertLevel,
        title: String,
        message: String,
        source: String,
    ) -> Self {
        let timestamp = SystemTime::now();
        let id = Self::generate_alert_id(&level, &timestamp);
        
        Self {
            id,
            level,
            title,
            message,
            created_at: timestamp,
            sent_at: None,
            source,
            metadata: HashMap::new(),
            acknowledged: false,
            acknowledged_at: None,
            retry_count: 0,
        }
    }

    /// Generate alert ID
    fn generate_alert_id(level: &AlertLevel, timestamp: &SystemTime) -> String {
        let time_str = timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("alert_{}_{}", level.to_number(), time_str)
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Mark as sent
    pub fn mark_sent(&mut self) {
        self.sent_at = Some(SystemTime::now());
    }

    /// Mark as acknowledged
    pub fn acknowledge(&mut self) {
        self.acknowledged = true;
        self.acknowledged_at = Some(SystemTime::now());
    }

    /// Increment retry count
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Get alert age (time from creation to now)
    pub fn age(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.created_at)
            .unwrap_or_default()
    }
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Enabled alert channels
    pub enabled_channels: Vec<AlertChannel>,
    /// Minimum alert level
    pub min_alert_level: AlertLevel,
    /// Maximum retry attempts
    pub max_retry_attempts: u32,
    /// Retry interval
    pub retry_interval: Duration,
    /// Alert deduplication time window
    pub deduplication_window: Duration,
    /// Maximum alert history records
    pub max_alert_history: usize,
    /// Alert timeout duration
    pub alert_timeout: Duration,
    /// Whether to enable alert aggregation
    pub enable_aggregation: bool,
    /// Aggregation time window
    pub aggregation_window: Duration,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled_channels: vec![AlertChannel::Log, AlertChannel::Console],
            min_alert_level: AlertLevel::Warning,
            max_retry_attempts: 3,
            retry_interval: Duration::from_secs(30),
            deduplication_window: Duration::from_secs(300), // 5 minutes
            max_alert_history: 1000,
            alert_timeout: Duration::from_secs(60),
            enable_aggregation: false,
            aggregation_window: Duration::from_secs(60),
        }
    }
}

/// Alert statistics
#[derive(Debug, Clone)]
pub struct AlertStats {
    /// Total alerts count
    pub total_alerts: u64,
    /// Successful sends count
    pub successful_sends: u64,
    /// Failed sends count
    pub failed_sends: u64,
    /// Acknowledged alerts count
    pub acknowledged_alerts: u64,
    /// Alert count grouped by level
    pub alerts_by_level: HashMap<AlertLevel, u64>,
    /// Send count grouped by channel
    pub sends_by_channel: HashMap<AlertChannel, u64>,
}

impl Default for AlertStats {
    fn default() -> Self {
        Self {
            total_alerts: 0,
            successful_sends: 0,
            failed_sends: 0,
            acknowledged_alerts: 0,
            alerts_by_level: HashMap::new(),
            sends_by_channel: HashMap::new(),
        }
    }
}

/// Alert manager
/// 
/// Responsible for managing the alert lifecycle, including:
/// - Receiving and categorizing alerts
/// - Sending alert notifications
/// - Managing alert history
/// - Statistics of alert information
pub struct AlertManager {
    config: AlertConfig,
    alert_history: Arc<Mutex<VecDeque<Alert>>>,
    pending_alerts: Arc<Mutex<VecDeque<Alert>>>,
    stats: Arc<Mutex<AlertStats>>,
    stop_signal: Arc<Mutex<bool>>,
}

impl AlertManager {
    /// Create new alert manager
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config,
            alert_history: Arc::new(Mutex::new(VecDeque::new())),
            pending_alerts: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(AlertStats::default())),
            stop_signal: Arc::new(Mutex::new(false)),
        }
    }

    /// Start alert manager
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("Starting alert manager");
        
        // Reset stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // Start alert processing loop
        let manager = self.clone();
        tokio::spawn(async move {
            if let Err(e) = manager.run_alert_processing_loop().await {
                error!("Alert processing loop failed: {:?}", e);
            }
        });

        info!("Alert manager started successfully");
        Ok(())
    }

    /// Stop alert manager
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping alert manager");
        
        // Set stop signal
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // Wait for remaining alerts to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Alert manager stopped");
        Ok(())
    }

    /// Send alert
    #[instrument(level = "debug", skip(self))]
    pub async fn send_alert(&self, level: AlertLevel, message: &str) -> Result<String> {
        // Check if alert level meets minimum requirement
        if level.to_number() < self.config.min_alert_level.to_number() {
            debug!("Alert level too low, skipping send: {} < {}", level, self.config.min_alert_level);
            return Ok("skipped".to_string());
        }

        let alert = Alert::new(
            level,
            self.generate_alert_title(level, message),
            message.to_string(),
            "health_monitor".to_string(),
        );

        let alert_id = alert.id.clone();

        // Check if deduplication is needed
        if self.should_deduplicate(&alert).await? {
            debug!("Alert deduplicated, skipping send: {}", alert_id);
            return Ok("deduplicated".to_string());
        }

        // Add alert to pending queue
        {
            let mut pending = self.pending_alerts.lock().await;
            pending.push_back(alert.clone());
        }

        // Update statistics
        {
            let mut stats = self.stats.lock().await;
            stats.total_alerts += 1;
            *stats.alerts_by_level.entry(level).or_insert(0) += 1;
        }

        info!("Alert created: [{}] {}", level, message);
        Ok(alert_id)
    }

    /// Create alert with metadata
    pub async fn send_alert_with_metadata(
        &self,
        level: AlertLevel,
        message: &str,
        metadata: HashMap<String, String>,
    ) -> Result<String> {
        let mut alert = Alert::new(
            level,
            self.generate_alert_title(level, message),
            message.to_string(),
            "health_monitor".to_string(),
        );

        // Add metadata
        for (key, value) in metadata {
            alert = alert.with_metadata(key, value);
        }

        let alert_id = alert.id.clone();

        // Check alert level
        if level.to_number() < self.config.min_alert_level.to_number() {
            return Ok("skipped".to_string());
        }

        // Check deduplication
        if self.should_deduplicate(&alert).await? {
            return Ok("deduplicated".to_string());
        }

        // Add to pending queue
        {
            let mut pending = self.pending_alerts.lock().await;
            pending.push_back(alert);
        }

        // Update statistics
        {
            let mut stats = self.stats.lock().await;
            stats.total_alerts += 1;
            *stats.alerts_by_level.entry(level).or_insert(0) += 1;
        }

        info!("Alert with metadata created: [{}] {}", level, message);
        Ok(alert_id)
    }

    /// Get alert history
    pub async fn get_alert_history(&self, limit: Option<usize>) -> Vec<Alert> {
        let history = self.alert_history.lock().await;
        
        match limit {
            Some(n) => {
                let len = history.len();
                if len <= n {
                    history.iter().cloned().collect()
                } else {
                    history.iter().skip(len - n).cloned().collect()
                }
            }
            None => history.iter().cloned().collect(),
        }
    }

    /// Get pending alerts
    pub async fn get_pending_alerts(&self) -> Vec<Alert> {
        let pending = self.pending_alerts.lock().await;
        pending.iter().cloned().collect()
    }

    /// Get alert statistics
    pub async fn get_stats(&self) -> AlertStats {
        let stats = self.stats.lock().await;
        stats.clone()
    }

    /// Acknowledge alert
    pub async fn acknowledge_alert(&self, alert_id: &str) -> Result<()> {
        let mut history = self.alert_history.lock().await;
        
        for alert in history.iter_mut() {
            if alert.id == alert_id {
                alert.acknowledge();
                
                // Update statistics
                drop(history);
                let mut stats = self.stats.lock().await;
                stats.acknowledged_alerts += 1;
                
                info!("Alert acknowledged: {}", alert_id);
                return Ok(());
            }
        }
        
        Err(anyhow!("Alert not found: {}", alert_id))
    }

    /// Clear alert history
    pub async fn clear_history(&self) -> Result<()> {
        let mut history = self.alert_history.lock().await;
        history.clear();
        info!("Alert history cleared");
        Ok(())
    }

    /// Alert processing main loop
    async fn run_alert_processing_loop(&self) -> Result<()> {
        info!("Starting alert processing loop");
        
        loop {
            // Check stop signal
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("Received stop signal, exiting alert processing loop");
                    break;
                }
            }

            // Process pending alerts
            self.process_pending_alerts().await?;

            // Wait for next processing
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Alert processing loop ended");
        Ok(())
    }

    /// Process pending alerts
    async fn process_pending_alerts(&self) -> Result<()> {
        let alert = {
            let mut pending = self.pending_alerts.lock().await;
            pending.pop_front()
        };

        if let Some(mut alert) = alert {
            // Try to send alert
            let send_result = self.send_alert_to_channels(&alert).await;
            
            match send_result {
                Ok(()) => {
                    alert.mark_sent();
                    
                    // Update statistics
                    {
                        let mut stats = self.stats.lock().await;
                        stats.successful_sends += 1;
                    }
                    
                    debug!("Alert sent successfully: {}", alert.id);
                }
                Err(e) => {
                    alert.increment_retry();
                    
                    // Check if retry is needed
                    if alert.retry_count < self.config.max_retry_attempts {
                        // Re-add to pending queue
                        let mut pending = self.pending_alerts.lock().await;
                        pending.push_back(alert.clone());
                        
                        warn!("Alert send failed, will retry: {} (retry count: {})", alert.id, alert.retry_count);
                    } else {
                        // Update statistics
                        {
                            let mut stats = self.stats.lock().await;
                            stats.failed_sends += 1;
                        }
                        
                        error!("Alert send failed, reached maximum retry attempts: {} - {:?}", alert.id, e);
                    }
                }
            }

            // Add alert to history
            {
                let mut history = self.alert_history.lock().await;
                history.push_back(alert);
                
                // Limit history record count
                while history.len() > self.config.max_alert_history {
                    history.pop_front();
                }
            }
        }

        Ok(())
    }

    /// Send alert to all configured channels
    async fn send_alert_to_channels(&self, alert: &Alert) -> Result<()> {
        for channel in &self.config.enabled_channels {
            if let Err(e) = self.send_to_channel(alert, channel).await {
                error!("Failed to send alert to channel {}: {:?}", channel, e);
                // Continue trying other channels
            } else {
                // Update statistics
                let mut stats = self.stats.lock().await;
                *stats.sends_by_channel.entry(channel.clone()).or_insert(0) += 1;
            }
        }
        Ok(())
    }

    /// Send alert to specific channel
    async fn send_to_channel(&self, alert: &Alert, channel: &AlertChannel) -> Result<()> {
        match channel {
            AlertChannel::Log => {
                match alert.level {
                    AlertLevel::Info => info!("[Alert] {}: {}", alert.title, alert.message),
                    AlertLevel::Warning => warn!("[Alert] {}: {}", alert.title, alert.message),
                    AlertLevel::Medium => warn!("[Alert] {}: {}", alert.title, alert.message),
                    AlertLevel::High => error!("[Alert] {}: {}", alert.title, alert.message),
                    AlertLevel::Critical => error!("[Critical Alert] {}: {}", alert.title, alert.message),
                }
            }
            AlertChannel::Console => {
                println!("[{}] {}: {}", alert.level, alert.title, alert.message);
            }
            AlertChannel::File(path) => {
                // File writing logic should be implemented here
                debug!("Writing alert to file: {} - {}", path, alert.message);
            }
            AlertChannel::Email(_email) => {
                // Email sending logic should be implemented here
                debug!("Sending email alert: {}", alert.message);
            }
            AlertChannel::Webhook(_url) => {
                // Webhook sending logic should be implemented here
                debug!("Sending webhook alert: {}", alert.message);
            }
            AlertChannel::Custom(name) => {
                debug!("Sending custom alert to {}: {}", name, alert.message);
            }
        }
        Ok(())
    }

    /// Check if this alert should be deduplicated
    async fn should_deduplicate(&self, alert: &Alert) -> Result<bool> {
        let history = self.alert_history.lock().await;
        let now = SystemTime::now();
        
        for existing_alert in history.iter().rev() {
            // Check time window
            if now.duration_since(existing_alert.created_at).unwrap_or_default() 
                > self.config.deduplication_window {
                break;
            }
            
            // Check if it's the same alert
            if existing_alert.level == alert.level 
                && existing_alert.message == alert.message 
                && existing_alert.source == alert.source {
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    /// Generate alert title
    fn generate_alert_title(&self, level: AlertLevel, message: &str) -> String {
        let prefix = match level {
            AlertLevel::Info => "Info",
            AlertLevel::Warning => "Warning",
            AlertLevel::Medium => "Medium Alert",
            AlertLevel::High => "High Alert",
            AlertLevel::Critical => "Critical Alert",
        };
        
        format!("{}: {}", prefix, message.chars().take(50).collect::<String>())
    }

    /// Get alert configuration
    pub fn get_config(&self) -> &AlertConfig {
        &self.config
    }

    /// Update alert configuration
    pub fn update_config(&mut self, config: AlertConfig) {
        self.config = config;
        info!("Alert manager configuration updated");
    }
}

impl Clone for AlertManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            alert_history: Arc::clone(&self.alert_history),
            pending_alerts: Arc::clone(&self.pending_alerts),
            stats: Arc::clone(&self.stats),
            stop_signal: Arc::clone(&self.stop_signal),
        }
    }
}