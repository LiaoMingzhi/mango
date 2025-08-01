// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 告警管理器模块
//! 
//! 负责管理和发送各种告警通知，包括：
//! - 健康状态告警
//! - 攻击检测告警
//! - 系统异常告警
//! - 性能监控告警

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use anyhow::{anyhow, Result};
use tracing::{info, warn, error, debug, instrument};
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;

/// 告警级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertLevel {
    /// 信息级别 - 一般信息通知
    Info,
    /// 警告级别 - 需要关注但不紧急
    Warning,
    /// 中等级别 - 需要及时处理
    Medium,
    /// 高级别 - 需要立即关注
    High,
    /// 紧急级别 - 需要立即处理
    Critical,
}

impl AlertLevel {
    /// 获取告警级别的数值表示 (1-5)
    pub fn to_number(&self) -> u8 {
        match self {
            AlertLevel::Info => 1,
            AlertLevel::Warning => 2,
            AlertLevel::Medium => 3,
            AlertLevel::High => 4,
            AlertLevel::Critical => 5,
        }
    }

    /// 从数值创建告警级别
    pub fn from_number(level: u8) -> Self {
        match level {
            1 => AlertLevel::Info,
            2 => AlertLevel::Warning,
            3 => AlertLevel::Medium,
            4 => AlertLevel::High,
            5 | _ => AlertLevel::Critical,
        }
    }

    /// 判断是否为紧急告警
    pub fn is_urgent(&self) -> bool {
        matches!(self, AlertLevel::High | AlertLevel::Critical)
    }
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Info => write!(f, "信息"),
            AlertLevel::Warning => write!(f, "警告"),
            AlertLevel::Medium => write!(f, "中等"),
            AlertLevel::High => write!(f, "高"),
            AlertLevel::Critical => write!(f, "紧急"),
        }
    }
}

/// 告警通知方式
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlertChannel {
    /// 日志记录
    Log,
    /// 控制台输出
    Console,
    /// 文件输出
    File(String),
    /// 邮件通知 (邮箱地址)
    Email(String),
    /// Webhook通知 (URL)
    Webhook(String),
    /// 自定义通知
    Custom(String),
}

impl std::fmt::Display for AlertChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertChannel::Log => write!(f, "日志"),
            AlertChannel::Console => write!(f, "控制台"),
            AlertChannel::File(path) => write!(f, "文件: {}", path),
            AlertChannel::Email(email) => write!(f, "邮件: {}", email),
            AlertChannel::Webhook(url) => write!(f, "Webhook: {}", url),
            AlertChannel::Custom(name) => write!(f, "自定义: {}", name),
        }
    }
}

/// 告警消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// 告警ID
    pub id: String,
    /// 告警级别
    pub level: AlertLevel,
    /// 告警标题
    pub title: String,
    /// 告警消息内容
    pub message: String,
    /// 创建时间
    pub created_at: SystemTime,
    /// 发送时间
    pub sent_at: Option<SystemTime>,
    /// 告警源
    pub source: String,
    /// 附加元数据
    pub metadata: HashMap<String, String>,
    /// 是否已处理
    pub acknowledged: bool,
    /// 处理时间
    pub acknowledged_at: Option<SystemTime>,
    /// 重试次数
    pub retry_count: u32,
}

impl Alert {
    /// 创建新的告警
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

    /// 生成告警ID
    fn generate_alert_id(level: &AlertLevel, timestamp: &SystemTime) -> String {
        let time_str = timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        format!("alert_{}_{}", level.to_number(), time_str)
    }

    /// 添加元数据
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// 标记为已发送
    pub fn mark_sent(&mut self) {
        self.sent_at = Some(SystemTime::now());
    }

    /// 标记为已处理
    pub fn acknowledge(&mut self) {
        self.acknowledged = true;
        self.acknowledged_at = Some(SystemTime::now());
    }

    /// 增加重试次数
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// 获取告警年龄（从创建到现在的时间）
    pub fn age(&self) -> Duration {
        SystemTime::now()
            .duration_since(self.created_at)
            .unwrap_or_default()
    }
}

/// 告警配置
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// 启用的告警通道
    pub enabled_channels: Vec<AlertChannel>,
    /// 最小告警级别
    pub min_alert_level: AlertLevel,
    /// 最大重试次数
    pub max_retry_attempts: u32,
    /// 重试间隔
    pub retry_interval: Duration,
    /// 告警去重时间窗口
    pub deduplication_window: Duration,
    /// 最大告警历史记录数
    pub max_alert_history: usize,
    /// 告警超时时间
    pub alert_timeout: Duration,
    /// 是否启用告警聚合
    pub enable_aggregation: bool,
    /// 聚合时间窗口
    pub aggregation_window: Duration,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled_channels: vec![AlertChannel::Log, AlertChannel::Console],
            min_alert_level: AlertLevel::Warning,
            max_retry_attempts: 3,
            retry_interval: Duration::from_secs(30),
            deduplication_window: Duration::from_secs(300), // 5分钟
            max_alert_history: 1000,
            alert_timeout: Duration::from_secs(60),
            enable_aggregation: false,
            aggregation_window: Duration::from_secs(60),
        }
    }
}

/// 告警统计信息
#[derive(Debug, Clone)]
pub struct AlertStats {
    /// 总告警数
    pub total_alerts: u64,
    /// 成功发送数
    pub successful_sends: u64,
    /// 失败发送数
    pub failed_sends: u64,
    /// 已处理告警数
    pub acknowledged_alerts: u64,
    /// 按级别分组的告警数
    pub alerts_by_level: HashMap<AlertLevel, u64>,
    /// 按通道分组的发送数
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

/// 告警管理器
/// 
/// 负责管理告警的生命周期，包括：
/// - 接收和分类告警
/// - 发送告警通知
/// - 管理告警历史
/// - 统计告警信息
pub struct AlertManager {
    config: AlertConfig,
    alert_history: Arc<Mutex<VecDeque<Alert>>>,
    pending_alerts: Arc<Mutex<VecDeque<Alert>>>,
    stats: Arc<Mutex<AlertStats>>,
    stop_signal: Arc<Mutex<bool>>,
}

impl AlertManager {
    /// 创建新的告警管理器
    pub fn new(config: AlertConfig) -> Self {
        Self {
            config,
            alert_history: Arc::new(Mutex::new(VecDeque::new())),
            pending_alerts: Arc::new(Mutex::new(VecDeque::new())),
            stats: Arc::new(Mutex::new(AlertStats::default())),
            stop_signal: Arc::new(Mutex::new(false)),
        }
    }

    /// 启动告警管理器
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<()> {
        info!("启动告警管理器");
        
        // 重置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = false;
        }

        // 启动告警处理循环
        let manager = self.clone();
        tokio::spawn(async move {
            if let Err(e) = manager.run_alert_processing_loop().await {
                error!("告警处理循环运行失败: {:?}", e);
            }
        });

        info!("告警管理器启动成功");
        Ok(())
    }

    /// 停止告警管理器
    #[instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> Result<()> {
        info!("停止告警管理器");
        
        // 设置停止信号
        {
            let mut stop_signal = self.stop_signal.lock().await;
            *stop_signal = true;
        }

        // 等待处理完剩余的告警
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("告警管理器已停止");
        Ok(())
    }

    /// 发送告警
    #[instrument(level = "debug", skip(self))]
    pub async fn send_alert(&self, level: AlertLevel, message: &str) -> Result<String> {
        // 检查告警级别是否满足最小要求
        if level.to_number() < self.config.min_alert_level.to_number() {
            debug!("告警级别过低，跳过发送: {} < {}", level, self.config.min_alert_level);
            return Ok("skipped".to_string());
        }

        let alert = Alert::new(
            level,
            self.generate_alert_title(level, message),
            message.to_string(),
            "health_monitor".to_string(),
        );

        let alert_id = alert.id.clone();

        // 检查是否需要去重
        if self.should_deduplicate(&alert).await? {
            debug!("告警被去重，跳过发送: {}", alert_id);
            return Ok("deduplicated".to_string());
        }

        // 将告警添加到待处理队列
        {
            let mut pending = self.pending_alerts.lock().await;
            pending.push_back(alert.clone());
        }

        // 更新统计信息
        {
            let mut stats = self.stats.lock().await;
            stats.total_alerts += 1;
            *stats.alerts_by_level.entry(level).or_insert(0) += 1;
        }

        info!("告警已创建: [{}] {}", level, message);
        Ok(alert_id)
    }

    /// 创建带元数据的告警
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

        // 添加元数据
        for (key, value) in metadata {
            alert = alert.with_metadata(key, value);
        }

        let alert_id = alert.id.clone();

        // 检查告警级别
        if level.to_number() < self.config.min_alert_level.to_number() {
            return Ok("skipped".to_string());
        }

        // 检查去重
        if self.should_deduplicate(&alert).await? {
            return Ok("deduplicated".to_string());
        }

        // 添加到待处理队列
        {
            let mut pending = self.pending_alerts.lock().await;
            pending.push_back(alert);
        }

        // 更新统计信息
        {
            let mut stats = self.stats.lock().await;
            stats.total_alerts += 1;
            *stats.alerts_by_level.entry(level).or_insert(0) += 1;
        }

        info!("带元数据的告警已创建: [{}] {}", level, message);
        Ok(alert_id)
    }

    /// 获取告警历史
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

    /// 获取待处理告警
    pub async fn get_pending_alerts(&self) -> Vec<Alert> {
        let pending = self.pending_alerts.lock().await;
        pending.iter().cloned().collect()
    }

    /// 获取告警统计信息
    pub async fn get_stats(&self) -> AlertStats {
        let stats = self.stats.lock().await;
        stats.clone()
    }

    /// 确认告警
    pub async fn acknowledge_alert(&self, alert_id: &str) -> Result<()> {
        let mut history = self.alert_history.lock().await;
        
        for alert in history.iter_mut() {
            if alert.id == alert_id {
                alert.acknowledge();
                
                // 更新统计信息
                drop(history);
                let mut stats = self.stats.lock().await;
                stats.acknowledged_alerts += 1;
                
                info!("告警已确认: {}", alert_id);
                return Ok(());
            }
        }
        
        Err(anyhow!("未找到告警: {}", alert_id))
    }

    /// 清除告警历史
    pub async fn clear_history(&self) -> Result<()> {
        let mut history = self.alert_history.lock().await;
        history.clear();
        info!("告警历史已清除");
        Ok(())
    }

    /// 告警处理主循环
    async fn run_alert_processing_loop(&self) -> Result<()> {
        info!("开始告警处理循环");
        
        loop {
            // 检查停止信号
            {
                let stop_signal = self.stop_signal.lock().await;
                if *stop_signal {
                    info!("收到停止信号，退出告警处理循环");
                    break;
                }
            }

            // 处理待发送的告警
            self.process_pending_alerts().await?;

            // 等待下次处理
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("告警处理循环已结束");
        Ok(())
    }

    /// 处理待发送的告警
    async fn process_pending_alerts(&self) -> Result<()> {
        let alert = {
            let mut pending = self.pending_alerts.lock().await;
            pending.pop_front()
        };

        if let Some(mut alert) = alert {
            // 尝试发送告警
            let send_result = self.send_alert_to_channels(&alert).await;
            
            match send_result {
                Ok(()) => {
                    alert.mark_sent();
                    
                    // 更新统计信息
                    {
                        let mut stats = self.stats.lock().await;
                        stats.successful_sends += 1;
                    }
                    
                    debug!("告警发送成功: {}", alert.id);
                }
                Err(e) => {
                    alert.increment_retry();
                    
                    // 检查是否需要重试
                    if alert.retry_count < self.config.max_retry_attempts {
                        // 重新加入待处理队列
                        let mut pending = self.pending_alerts.lock().await;
                        pending.push_back(alert.clone());
                        
                        warn!("告警发送失败，将重试: {} (重试次数: {})", alert.id, alert.retry_count);
                    } else {
                        // 更新统计信息
                        {
                            let mut stats = self.stats.lock().await;
                            stats.failed_sends += 1;
                        }
                        
                        error!("告警发送失败，已达到最大重试次数: {} - {:?}", alert.id, e);
                    }
                }
            }

            // 将告警添加到历史记录
            {
                let mut history = self.alert_history.lock().await;
                history.push_back(alert);
                
                // 限制历史记录数量
                while history.len() > self.config.max_alert_history {
                    history.pop_front();
                }
            }
        }

        Ok(())
    }

    /// 发送告警到所有配置的通道
    async fn send_alert_to_channels(&self, alert: &Alert) -> Result<()> {
        for channel in &self.config.enabled_channels {
            if let Err(e) = self.send_to_channel(alert, channel).await {
                error!("发送告警到通道 {} 失败: {:?}", channel, e);
                // 继续尝试其他通道
            } else {
                // 更新统计信息
                let mut stats = self.stats.lock().await;
                *stats.sends_by_channel.entry(channel.clone()).or_insert(0) += 1;
            }
        }
        Ok(())
    }

    /// 发送告警到特定通道
    async fn send_to_channel(&self, alert: &Alert, channel: &AlertChannel) -> Result<()> {
        match channel {
            AlertChannel::Log => {
                match alert.level {
                    AlertLevel::Info => info!("[告警] {}: {}", alert.title, alert.message),
                    AlertLevel::Warning => warn!("[告警] {}: {}", alert.title, alert.message),
                    AlertLevel::Medium => warn!("[告警] {}: {}", alert.title, alert.message),
                    AlertLevel::High => error!("[告警] {}: {}", alert.title, alert.message),
                    AlertLevel::Critical => error!("[紧急告警] {}: {}", alert.title, alert.message),
                }
            }
            AlertChannel::Console => {
                println!("[{}] {}: {}", alert.level, alert.title, alert.message);
            }
            AlertChannel::File(path) => {
                // 这里应该实现文件写入逻辑
                debug!("将告警写入文件: {} - {}", path, alert.message);
            }
            AlertChannel::Email(_email) => {
                // 这里应该实现邮件发送逻辑
                debug!("发送邮件告警: {}", alert.message);
            }
            AlertChannel::Webhook(_url) => {
                // 这里应该实现Webhook发送逻辑
                debug!("发送Webhook告警: {}", alert.message);
            }
            AlertChannel::Custom(name) => {
                debug!("发送自定义告警到 {}: {}", name, alert.message);
            }
        }
        Ok(())
    }

    /// 检查是否应该去重此告警
    async fn should_deduplicate(&self, alert: &Alert) -> Result<bool> {
        let history = self.alert_history.lock().await;
        let now = SystemTime::now();
        
        for existing_alert in history.iter().rev() {
            // 检查时间窗口
            if now.duration_since(existing_alert.created_at).unwrap_or_default() 
                > self.config.deduplication_window {
                break;
            }
            
            // 检查是否为相同的告警
            if existing_alert.level == alert.level 
                && existing_alert.message == alert.message 
                && existing_alert.source == alert.source {
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    /// 生成告警标题
    fn generate_alert_title(&self, level: AlertLevel, message: &str) -> String {
        let prefix = match level {
            AlertLevel::Info => "信息",
            AlertLevel::Warning => "警告",
            AlertLevel::Medium => "中等告警",
            AlertLevel::High => "高级告警",
            AlertLevel::Critical => "紧急告警",
        };
        
        format!("{}: {}", prefix, message.chars().take(50).collect::<String>())
    }

    /// 获取告警配置
    pub fn get_config(&self) -> &AlertConfig {
        &self.config
    }

    /// 更新告警配置
    pub fn update_config(&mut self, config: AlertConfig) {
        self.config = config;
        info!("告警管理器配置已更新");
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