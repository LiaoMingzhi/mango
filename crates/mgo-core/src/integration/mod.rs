// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 系统集成模块
//! 
//! 将回滚管理器、冷启动管理器和健康监控系统集成为统一的高可用性管理系统

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

/// 高可用性系统配置
#[derive(Debug, Clone)]
pub struct HighAvailabilityConfig {
    /// 回滚配置
    pub rollback: RollbackConfig,
    /// 冷启动配置
    pub cold_start: ColdStartConfig,
    /// 监控配置
    pub monitor: MonitorConfig,
    /// 是否启用自动故障恢复
    pub enable_auto_recovery: bool,
    /// 自动恢复触发阈值
    pub auto_recovery_threshold: u8,
    /// 故障恢复间隔
    pub recovery_interval: Duration,
    /// 最大连续恢复尝试次数
    pub max_recovery_attempts: u32,
}

impl Default for HighAvailabilityConfig {
    fn default() -> Self {
        Self {
            rollback: RollbackConfig::default(),
            cold_start: ColdStartConfig::default(),
            monitor: MonitorConfig::default(),
            enable_auto_recovery: true,
            auto_recovery_threshold: 3, // 连续3次健康检查失败后触发恢复
            recovery_interval: Duration::from_secs(300), // 5分钟
            max_recovery_attempts: 3,
        }
    }
}

/// 系统状态
#[derive(Debug, Clone, PartialEq)]
pub enum SystemState {
    /// 正常运行
    Healthy,
    /// 健康度下降，需要关注
    Degraded,
    /// 系统异常，需要干预
    Unhealthy,
    /// 正在恢复中
    Recovering,
    /// 系统已停止
    Stopped,
    /// 错误状态
    Error(String),
}

/// 恢复策略
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStrategy {
    /// 无需恢复
    None,
    /// 重启服务
    RestartServices,
    /// 执行回滚
    Rollback {
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
    },
    /// 执行冷启动
    ColdStart,
    /// 混合策略（先回滚再冷启动）
    Hybrid {
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
    },
}

/// 高可用性管理器
/// 
/// 统一管理回滚、冷启动和健康监控功能，提供自动故障检测和恢复能力
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
    /// 创建新的高可用性管理器
    pub fn new(
        config: HighAvailabilityConfig,
        authority_state: Arc<AuthorityState>,
        checkpoint_store: Arc<CheckpointStore>,
        network_client: Arc<NetworkAuthorityClient>,
        registry: &Registry,
    ) -> Result<Self> {
        // 创建各组件的指标
        let rollback_metrics = RollbackMetrics::new(registry);
        let cold_start_metrics = ColdStartMetrics::new(registry);
        let health_metrics = Arc::new(HealthMetrics::default());

        // 创建回滚管理器
        let rollback_manager = Arc::new(RollbackManager::new(
            config.rollback.clone(),
            checkpoint_store.clone(),
            authority_state.clone(),
            network_client.clone(),
            rollback_metrics,
        ));

        // 创建冷启动管理器
        let cold_start_manager = Arc::new(ColdStartManager::new(
            config.cold_start.clone(),
            checkpoint_store.clone(),
            authority_state.clone(),
            network_client.clone(),
            cold_start_metrics,
        ));

        // 创建健康监控组件
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

        // 创建健康监控管理器
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

    /// 启动高可用性管理器
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("启动高可用性管理器");

        // 重置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // 更新系统状态
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Healthy;
        }

        // 启动各个组件
        self.health_monitor.start().await?;
        info!("健康监控系统已启动");

        // 启动自动恢复循环
        if self.config.enable_auto_recovery {
            let manager = Arc::new(self.clone());
            tokio::spawn(async move {
                if let Err(e) = manager.run_auto_recovery_loop().await {
                    error!("自动恢复循环运行失败: {:?}", e);
                }
            });
            info!("自动恢复系统已启动");
        }

        info!("高可用性管理器启动成功");
        Ok(())
    }

    /// 停止高可用性管理器
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("停止高可用性管理器");

        // 设置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // 停止健康监控
        self.health_monitor.stop().await?;

        // 更新系统状态
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Stopped;
        }

        info!("高可用性管理器已停止");
        Ok(())
    }

    /// 获取当前系统状态
    pub async fn get_system_state(&self) -> SystemState {
        let state = self.system_state.lock().await;
        state.clone()
    }

    /// 手动执行健康检查
    pub async fn perform_health_check(&self) -> Result<HealthStatus> {
        self.health_monitor.perform_health_check().await
    }

    /// 手动执行回滚
    pub async fn execute_rollback(
        &self,
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
        force: bool,
    ) -> Result<RollbackResult> {
        info!("手动执行回滚到检查点 {}", target_checkpoint);
        
        // 更新系统状态
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Recovering;
        }

        let result = self.rollback_manager.rollback_to_checkpoint(target_checkpoint, force).await;

        // 根据结果更新系统状态
        {
            let mut state = self.system_state.lock().await;
            match &result {
                Ok(_) => *state = SystemState::Healthy,
                Err(_) => *state = SystemState::Error("回滚失败".to_string()),
            }
        }

        result
    }

    /// 手动执行冷启动
    pub async fn execute_cold_start(&self) -> Result<ColdStartResult> {
        info!("手动执行冷启动");
        
        // 更新系统状态
        {
            let mut state = self.system_state.lock().await;
            *state = SystemState::Recovering;
        }

        let result = self.cold_start_manager.perform_cold_start().await;

        // 根据结果更新系统状态
        {
            let mut state = self.system_state.lock().await;
            match &result {
                Ok(_) => *state = SystemState::Healthy,
                Err(_) => *state = SystemState::Error("冷启动失败".to_string()),
            }
        }

        result
    }

    /// 分析系统健康状态并确定恢复策略
    async fn analyze_recovery_strategy(&self, health_status: &HealthStatus) -> RecoveryStrategy {
        let health_score = health_status.health_score();

        if health_score >= 75 {
            // 健康度良好，无需恢复
            return RecoveryStrategy::None;
        }

        if health_score >= 50 {
            // 健康度中等，尝试重启服务
            return RecoveryStrategy::RestartServices;
        }

        // 检查攻击指标
        if let Ok(indicators) = self.health_monitor.get_attack_detector().detect_attack_signs().await {
            if !indicators.is_empty() {
                // 检测到攻击，根据攻击类型选择策略
                for indicator in &indicators {
                    if indicator.is_critical() {
                        // 严重攻击，执行混合恢复策略
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

        // 健康度较低，尝试回滚
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

        // 最后的选择：冷启动
        RecoveryStrategy::ColdStart
    }

    /// 执行恢复策略
    async fn execute_recovery_strategy(&self, strategy: &RecoveryStrategy) -> Result<()> {
        match strategy {
            RecoveryStrategy::None => {
                info!("系统健康，无需恢复");
                Ok(())
            }
            RecoveryStrategy::RestartServices => {
                info!("重启服务以恢复系统健康");
                // 这里可以添加重启服务的逻辑
                // 目前简化为发送告警
                                        self.health_monitor.get_alert_manager()
                    .send_alert(AlertLevel::Warning, "建议重启服务以恢复系统健康")
                    .await?;
                Ok(())
            }
            RecoveryStrategy::Rollback { target_checkpoint } => {
                info!("执行回滚到检查点 {} 以恢复系统", target_checkpoint);
                self.rollback_manager
                    .rollback_to_checkpoint(*target_checkpoint, false)
                    .await?;
                Ok(())
            }
            RecoveryStrategy::ColdStart => {
                info!("执行冷启动以恢复系统");
                self.cold_start_manager.perform_cold_start().await?;
                Ok(())
            }
            RecoveryStrategy::Hybrid { target_checkpoint } => {
                info!("执行混合恢复策略：先回滚到检查点 {}，然后冷启动", target_checkpoint);
                
                // 先执行回滚
                match self.rollback_manager
                    .rollback_to_checkpoint(*target_checkpoint, false)
                    .await {
                    Ok(_) => {
                        info!("回滚成功，开始冷启动");
                        // 等待一段时间让系统稳定
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        // 执行冷启动
                        self.cold_start_manager.perform_cold_start().await?;
                    }
                    Err(e) => {
                        warn!("回滚失败: {:?}，直接执行冷启动", e);
                        self.cold_start_manager.perform_cold_start().await?;
                    }
                }
                Ok(())
            }
        }
    }

    /// 自动恢复主循环
    async fn run_auto_recovery_loop(&self) -> Result<()> {
        info!("开始自动恢复监控循环");
        
        let mut consecutive_failures = 0u8;
        
        loop {
            // 检查停止信号
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("收到停止信号，退出自动恢复循环");
                    break;
                }
            }

            // 检查是否需要等待恢复间隔
            {
                let last_recovery = self.last_recovery_time.lock().await;
                if let Some(last_time) = *last_recovery {
                    if last_time.elapsed() < self.config.recovery_interval {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                }
            }

            // 执行健康检查
            match self.health_monitor.perform_health_check().await {
                Ok(health_status) => {
                    let health_score = health_status.health_score();
                    
                    if health_status.is_healthy() {
                        // 系统健康，重置失败计数
                        consecutive_failures = 0;
                        
                        // 更新系统状态
                        {
                            let mut state = self.system_state.lock().await;
                            if *state != SystemState::Healthy {
                                *state = SystemState::Healthy;
                                info!("系统已恢复健康状态");
                            }
                        }
                    } else {
                        // 系统不健康，增加失败计数
                        consecutive_failures += 1;
                        
                        warn!(
                            "健康检查失败 ({}/{}): 健康分数 {}/100",
                            consecutive_failures,
                            self.config.auto_recovery_threshold,
                            health_score
                        );

                        // 更新系统状态
                        {
                            let mut state = self.system_state.lock().await;
                            if health_score >= 50 {
                                *state = SystemState::Degraded;
                            } else {
                                *state = SystemState::Unhealthy;
                            }
                        }

                        // 检查是否需要触发自动恢复
                        if consecutive_failures >= self.config.auto_recovery_threshold {
                            // 检查恢复尝试次数限制
                            let current_attempts = {
                                let attempts = self.recovery_attempts.lock().await;
                                *attempts
                            };

                            if current_attempts < self.config.max_recovery_attempts {
                                info!("触发自动恢复 (尝试 {}/{})", current_attempts + 1, self.config.max_recovery_attempts);
                                
                                // 更新系统状态为恢复中
                                {
                                    let mut state = self.system_state.lock().await;
                                    *state = SystemState::Recovering;
                                }

                                // 分析并执行恢复策略
                                let strategy = self.analyze_recovery_strategy(&health_status).await;
                                info!("选择恢复策略: {:?}", strategy);

                                match self.execute_recovery_strategy(&strategy).await {
                                    Ok(()) => {
                                        info!("自动恢复执行成功");
                                        consecutive_failures = 0;
                                        
                                        // 重置恢复尝试计数
                                        {
                                            let mut attempts = self.recovery_attempts.lock().await;
                                            *attempts = 0;
                                        }
                                    }
                                    Err(e) => {
                                        error!("自动恢复执行失败: {:?}", e);
                                        
                                        // 增加恢复尝试计数
                                        {
                                            let mut attempts = self.recovery_attempts.lock().await;
                                            *attempts += 1;
                                        }

                                        // 发送紧急告警
                                                                        let _ = self.health_monitor.get_alert_manager()
                                            .send_alert(
                                                AlertLevel::Critical,
                                                &format!("自动恢复失败: {:?}", e)
                                            ).await;
                                    }
                                }

                                // 更新最后恢复时间
                                {
                                    let mut last_recovery = self.last_recovery_time.lock().await;
                                    *last_recovery = Some(std::time::Instant::now());
                                }
                            } else {
                                error!(
                                    "已达到最大恢复尝试次数 ({}), 停止自动恢复",
                                    self.config.max_recovery_attempts
                                );
                                
                                // 发送紧急告警
                                let _ = self.health_monitor.get_alert_manager()
                                    .send_alert(
                                        AlertLevel::Critical,
                                        "已达到最大自动恢复尝试次数，需要人工干预"
                                    ).await;

                                // 更新系统状态为错误
                                {
                                    let mut state = self.system_state.lock().await;
                                    *state = SystemState::Error("达到最大恢复尝试次数".to_string());
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("健康检查执行失败: {:?}", e);
                    consecutive_failures += 1;
                }
            }

            // 等待下次检查
            tokio::time::sleep(self.config.monitor.monitor_interval).await;
        }

        info!("自动恢复监控循环已结束");
        Ok(())
    }

    /// 重置恢复尝试计数
    pub async fn reset_recovery_attempts(&self) {
        let mut attempts = self.recovery_attempts.lock().await;
        *attempts = 0;
        info!("恢复尝试计数已重置");
    }

    /// 获取恢复尝试次数
    pub async fn get_recovery_attempts(&self) -> u32 {
        let attempts = self.recovery_attempts.lock().await;
        *attempts
    }

    /// 获取配置
    pub fn get_config(&self) -> &HighAvailabilityConfig {
        &self.config
    }

    /// 更新配置
    pub fn update_config(&mut self, config: HighAvailabilityConfig) {
        self.config = config;
        info!("高可用性管理器配置已更新");
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