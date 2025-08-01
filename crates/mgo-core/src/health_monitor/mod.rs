// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 健康监控模块
//! 
//! 此模块提供了Mango Network的健康监控和攻击检测功能，包括：
//! - 节点健康状态检查
//! - 网络异常检测
//! - 攻击行为识别
//! - 告警机制

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

/// 健康监控指标
#[derive(Debug)]
pub struct HealthMetrics {
    /// 健康检查总次数
    pub health_checks_total: IntCounter,
    /// 健康检查失败次数
    pub health_check_failures_total: IntCounter,
    /// 当前健康状态 (1: 健康, 0: 不健康)
    pub current_health_status: IntGauge,
    /// 攻击检测总次数
    pub attack_detections_total: IntCounter,
    /// 当前攻击指标数量
    pub active_attack_indicators: IntGauge,
    /// 健康检查耗时
    pub health_check_duration: Histogram,
    /// 告警发送总次数
    pub alerts_sent_total: IntCounter,
}

impl Default for HealthMetrics {
    fn default() -> Self {
        Self {
            health_checks_total: register_int_counter!(
                "mgo_health_checks_total",
                "健康检查总次数"
            ).unwrap(),
            health_check_failures_total: register_int_counter!(
                "mgo_health_check_failures_total", 
                "健康检查失败次数"
            ).unwrap(),
            current_health_status: register_int_gauge!(
                "mgo_current_health_status",
                "当前健康状态 (1: 健康, 0: 不健康)"
            ).unwrap(),
            attack_detections_total: register_int_counter!(
                "mgo_attack_detections_total",
                "攻击检测总次数"
            ).unwrap(),
            active_attack_indicators: register_int_gauge!(
                "mgo_active_attack_indicators",
                "当前攻击指标数量"
            ).unwrap(),
            health_check_duration: register_histogram!(
                "mgo_health_check_duration_seconds",
                "健康检查耗时（秒）"
            ).unwrap(),
            alerts_sent_total: register_int_counter!(
                "mgo_alerts_sent_total",
                "告警发送总次数"
            ).unwrap(),
        }
    }
}

/// 监控配置
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// 健康检查配置
    pub health_check: HealthCheckConfig,
    /// 告警配置
    pub alert: AlertConfig,
    /// 监控间隔
    pub monitor_interval: Duration,
    /// 是否启用攻击检测
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

/// 监控状态
#[derive(Debug, Clone)]
pub enum MonitorState {
    /// 启动中
    Starting,
    /// 运行中
    Running,
    /// 暂停中
    Paused,
    /// 停止中
    Stopping,
    /// 已停止
    Stopped,
    /// 异常状态
    Error(String),
}

/// 健康监控管理器
/// 
/// 这是健康监控系统的主要协调器，负责：
/// - 定期执行健康检查
/// - 检测攻击迹象
/// - 发送告警通知
/// - 维护监控状态
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
    /// 创建新的健康监控管理器
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

    /// 启动健康监控
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("启动健康监控系统");
        
        // 更新状态
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Starting;
        }

        // 重置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // 启动监控循环
        let monitor = Arc::new(self.clone());
        tokio::spawn(async move {
            if let Err(e) = monitor.run_monitor_loop().await {
                error!("健康监控运行失败: {:?}", e);
                let mut state = monitor.state.lock().await;
                *state = MonitorState::Error(format!("{:?}", e));
            }
        });

        // 更新状态为运行中
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Running;
        }

        info!("健康监控系统启动成功");
        Ok(())
    }

    /// 停止健康监控
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("停止健康监控系统");
        
        // 设置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // 更新状态
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Stopping;
        }

        // 等待监控循环结束
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 更新状态为已停止
        {
            let mut state = self.state.lock().await;
            *state = MonitorState::Stopped;
        }

        info!("健康监控系统已停止");
        Ok(())
    }

    /// 获取当前监控状态
    pub async fn get_state(&self) -> MonitorState {
        let state = self.state.lock().await;
        state.clone()
    }

    /// 暂停监控
    pub async fn pause(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        match *state {
            MonitorState::Running => {
                *state = MonitorState::Paused;
                info!("健康监控已暂停");
                Ok(())
            }
            _ => Err(anyhow!("只能在运行状态下暂停监控")),
        }
    }

    /// 恢复监控
    pub async fn resume(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        match *state {
            MonitorState::Paused => {
                *state = MonitorState::Running;
                info!("健康监控已恢复");
                Ok(())
            }
            _ => Err(anyhow!("只能在暂停状态下恢复监控")),
        }
    }

    /// 执行一次完整的健康检查和攻击检测
    #[instrument(level = "debug", skip(self))]
    pub async fn perform_health_check(&self) -> Result<HealthStatus> {
        let start_time = Instant::now();
        
        // 执行健康检查
        let health_status = self.health_checker.check_node_health().await?;
        
        // 更新指标
        self.metrics.health_checks_total.inc();
        if !health_status.is_healthy() {
            self.metrics.health_check_failures_total.inc();
            self.metrics.current_health_status.set(0);
        } else {
            self.metrics.current_health_status.set(1);
        }
        
        self.metrics.health_check_duration
            .observe(start_time.elapsed().as_secs_f64());

        // 如果启用了攻击检测，执行攻击检测
        if self.config.enable_attack_detection {
            if let Ok(indicators) = self.attack_detector.detect_attack_signs().await {
                self.metrics.attack_detections_total.inc();
                self.metrics.active_attack_indicators.set(indicators.len() as i64);
                
                // 如果检测到攻击迹象，发送告警
                if !indicators.is_empty() {
                    self.send_attack_alerts(&indicators).await?;
                }
            }
        }

        // 如果健康状态异常，发送健康告警
        if !health_status.is_healthy() {
            self.send_health_alert(&health_status).await?;
        }

        Ok(health_status)
    }

    /// 监控主循环
    async fn run_monitor_loop(&self) -> Result<()> {
        info!("开始健康监控主循环");
        
        loop {
            // 检查停止信号
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("收到停止信号，退出监控循环");
                    break;
                }
            }

            // 检查状态，只在运行状态下执行监控
            {
                let state = self.state.lock().await;
                if !matches!(*state, MonitorState::Running) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }

            // 执行健康检查
            if let Err(e) = self.perform_health_check().await {
                error!("健康检查失败: {:?}", e);
            }

            // 等待下次检查
            tokio::time::sleep(self.config.monitor_interval).await;
        }

        info!("健康监控主循环已结束");
        Ok(())
    }

    /// 发送攻击告警
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
                "检测到攻击迹象: {} - {} (置信度: {:.2})",
                indicator.attack_type,
                indicator.description,
                indicator.confidence
            );

            self.alert_manager.send_alert(alert_level, &message).await?;
            self.metrics.alerts_sent_total.inc();
        }
        Ok(())
    }

    /// 发送健康告警
    async fn send_health_alert(&self, health_status: &HealthStatus) -> Result<()> {
        let message = format!(
            "节点健康检查失败: 共识健康={}, 网络健康={}, 存储健康={}, 执行健康={}",
            health_status.consensus_healthy,
            health_status.network_healthy,
            health_status.storage_healthy,
            health_status.execution_healthy
        );

        self.alert_manager.send_alert(AlertLevel::High, &message).await?;
        self.metrics.alerts_sent_total.inc();
        
        Ok(())
    }

    /// 获取攻击检测器
    pub fn get_attack_detector(&self) -> &AttackDetector {
        &self.attack_detector
    }

    /// 获取告警管理器
    pub fn get_alert_manager(&self) -> &AlertManager {
        &self.alert_manager
    }

    /// 获取配置
    pub fn get_config(&self) -> &MonitorConfig {
        &self.config
    }

    /// 更新配置
    pub fn update_config(&mut self, config: MonitorConfig) {
        self.config = config;
        info!("健康监控配置已更新");
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