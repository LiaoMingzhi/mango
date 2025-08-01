// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 健康监控系统与mango-metrics指标系统的集成模块
//! 
//! 这个模块提供了将mgo-core健康监控系统集成到mango-metrics指标系统的功能

use std::sync::Arc;
use std::time::Duration;
use prometheus::{
    IntCounter, IntGauge, IntCounterVec, IntGaugeVec, Histogram, HistogramVec,
    register_int_counter_with_registry, register_int_gauge_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
    register_histogram_with_registry, register_histogram_vec_with_registry,
    Registry, HistogramOpts,
};
use tracing::{warn, instrument};

/// 健康监控指标集成器
/// 
/// 这个结构体提供了将健康监控系统的指标集成到现有prometheus指标系统的功能
pub struct HealthMonitoringMetricsIntegrator {
    _registry: Arc<Registry>,
    
    // 健康检查指标
    health_check_total: IntCounter,
    health_check_failures: IntCounter,
    health_check_duration: Histogram,
    health_score_gauge: IntGauge,
    
    // 组件健康状态指标
    component_health_status: IntGaugeVec,
    
    // 攻击检测指标
    attack_detections_total: IntCounter,
    attack_indicators_active: IntGauge,
    attack_types_detected: IntCounterVec,
    attack_confidence_histogram: HistogramVec,
    
    // 告警指标
    alerts_sent_total: IntCounterVec,
    alerts_failed_total: IntCounterVec,
    
    // 恢复系统指标
    recovery_attempts_total: IntCounter,
    recovery_success_total: IntCounter,
    recovery_failure_total: IntCounter,
    recovery_duration: HistogramVec,
    
    // 系统状态指标
    system_state_gauge: IntGauge,
    uptime_seconds: IntGauge,
}

impl HealthMonitoringMetricsIntegrator {
    /// 创建新的健康监控指标集成器
    pub fn new(registry: Arc<Registry>) -> Result<Self, prometheus::Error> {
        // 健康检查指标
        let health_check_total = register_int_counter_with_registry!(
            "mgo_health_check_total",
            "健康检查总次数",
            registry.as_ref()
        )?;

        let health_check_failures = register_int_counter_with_registry!(
            "mgo_health_check_failures_total",
            "健康检查失败次数", 
            registry.as_ref()
        )?;

        let health_check_duration = register_histogram_with_registry!(
            HistogramOpts::new(
                "mgo_health_check_duration_seconds",
                "健康检查耗时（秒）"
            ),
            registry.as_ref()
        )?;

        let health_score_gauge = register_int_gauge_with_registry!(
            "mgo_health_score",
            "当前健康分数 (0-100)",
            registry.as_ref()
        )?;

        // 组件健康状态指标
        let component_health_status = register_int_gauge_vec_with_registry!(
            "mgo_component_health_status",
            "各组件健康状态 (1: 健康, 0: 不健康)",
            &["component"],
            registry.as_ref()
        )?;

        // 攻击检测指标
        let attack_detections_total = register_int_counter_with_registry!(
            "mgo_attack_detections_total",
            "攻击检测总次数",
            registry.as_ref()
        )?;

        let attack_indicators_active = register_int_gauge_with_registry!(
            "mgo_attack_indicators_active",
            "当前活跃的攻击指标数量",
            registry.as_ref()
        )?;

        let attack_types_detected = register_int_counter_vec_with_registry!(
            "mgo_attack_types_detected_total",
            "按类型统计检测到的攻击次数",
            &["attack_type"],
            registry.as_ref()
        )?;

        let attack_confidence_histogram = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "mgo_attack_confidence",
                "攻击检测置信度分布"
            ),
            &["attack_type"],
            registry.as_ref()
        )?;

        // 告警指标
        let alerts_sent_total = register_int_counter_vec_with_registry!(
            "mgo_alerts_sent_total",
            "发送的告警总数",
            &["level", "channel"],
            registry.as_ref()
        )?;

        let alerts_failed_total = register_int_counter_vec_with_registry!(
            "mgo_alerts_failed_total",
            "发送失败的告警总数",
            &["level", "channel", "reason"],
            registry.as_ref()
        )?;

        // 恢复系统指标
        let recovery_attempts_total = register_int_counter_with_registry!(
            "mgo_recovery_attempts_total",
            "恢复尝试总次数",
            registry.as_ref()
        )?;

        let recovery_success_total = register_int_counter_with_registry!(
            "mgo_recovery_success_total",
            "恢复成功总次数",
            registry.as_ref()
        )?;

        let recovery_failure_total = register_int_counter_with_registry!(
            "mgo_recovery_failure_total",
            "恢复失败总次数",
            registry.as_ref()
        )?;

        let recovery_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "mgo_recovery_duration_seconds",
                "恢复操作耗时（秒）"
            ),
            &["strategy"],
            registry.as_ref()
        )?;

        // 系统状态指标
        let system_state_gauge = register_int_gauge_with_registry!(
            "mgo_system_state",
            "系统状态 (0: Stopped, 1: Healthy, 2: Degraded, 3: Unhealthy, 4: Recovering, 5: Error)",
            registry.as_ref()
        )?;

        let uptime_seconds = register_int_gauge_with_registry!(
            "mgo_uptime_seconds",
            "系统运行时间（秒）",
            registry.as_ref()
        )?;

        Ok(Self {
            _registry: registry,
            health_check_total,
            health_check_failures,
            health_check_duration,
            health_score_gauge,
            component_health_status,
            attack_detections_total,
            attack_indicators_active,
            attack_types_detected,
            attack_confidence_histogram,
            alerts_sent_total,
            alerts_failed_total,
            recovery_attempts_total,
            recovery_success_total,
            recovery_failure_total,
            recovery_duration,
            system_state_gauge,
            uptime_seconds,
        })
    }

    /// 记录健康检查指标
    #[instrument(level = "debug", skip(self))]
    pub fn record_health_check(
        &self,
        duration: Duration,
        health_score: u8,
        consensus_healthy: bool,
        network_healthy: bool,
        storage_healthy: bool,
        execution_healthy: bool,
        is_success: bool,
    ) {
        // 记录健康检查总数
        self.health_check_total.inc();
        
        // 记录失败情况
        if !is_success {
            self.health_check_failures.inc();
        }
        
        // 记录耗时
        self.health_check_duration.observe(duration.as_secs_f64());
        
        // 记录健康分数
        self.health_score_gauge.set(health_score as i64);
        
        // 记录各组件健康状态
        self.component_health_status
            .with_label_values(&["consensus"])
            .set(if consensus_healthy { 1 } else { 0 });
        
        self.component_health_status
            .with_label_values(&["network"])
            .set(if network_healthy { 1 } else { 0 });
        
        self.component_health_status
            .with_label_values(&["storage"])
            .set(if storage_healthy { 1 } else { 0 });
        
        self.component_health_status
            .with_label_values(&["execution"])
            .set(if execution_healthy { 1 } else { 0 });
    }

    /// 记录攻击检测指标
    #[instrument(level = "debug", skip(self))]
    pub fn record_attack_detection(
        &self,
        attack_type: &str,
        confidence: f64,
        active_indicators_count: usize,
    ) {
        // 记录攻击检测总数
        self.attack_detections_total.inc();
        
        // 记录活跃攻击指标数量
        self.attack_indicators_active.set(active_indicators_count as i64);
        
        // 记录攻击类型
        self.attack_types_detected
            .with_label_values(&[attack_type])
            .inc();
        
        // 记录置信度分布
        self.attack_confidence_histogram
            .with_label_values(&[attack_type])
            .observe(confidence);
    }

    /// 记录告警指标
    #[instrument(level = "debug", skip(self))]
    pub fn record_alert(
        &self,
        level: &str,
        channel: &str,
        is_success: bool,
        failure_reason: Option<&str>,
    ) {
        if is_success {
            self.alerts_sent_total
                .with_label_values(&[level, channel])
                .inc();
        } else {
            let reason = failure_reason.unwrap_or("unknown");
            self.alerts_failed_total
                .with_label_values(&[level, channel, reason])
                .inc();
        }
    }

    /// 记录恢复操作指标
    #[instrument(level = "debug", skip(self))]
    pub fn record_recovery_operation(
        &self,
        strategy: &str,
        duration: Duration,
        is_success: bool,
    ) {
        // 记录恢复尝试
        self.recovery_attempts_total.inc();
        
        // 记录成功或失败
        if is_success {
            self.recovery_success_total.inc();
        } else {
            self.recovery_failure_total.inc();
        }
        
        // 记录恢复耗时
        self.recovery_duration
            .with_label_values(&[strategy])
            .observe(duration.as_secs_f64());
    }

    /// 更新系统状态
    #[instrument(level = "debug", skip(self))]
    pub fn update_system_state(&self, state: SystemStateCode, uptime: Duration) {
        self.system_state_gauge.set(state as i64);
        self.uptime_seconds.set(uptime.as_secs() as i64);
    }

    /// 获取健康监控指标摘要
    pub fn get_metrics_summary(&self) -> HealthMonitoringMetricsSummary {
        HealthMonitoringMetricsSummary {
            health_checks_total: self.health_check_total.get(),
            health_check_failures: self.health_check_failures.get(),
            current_health_score: self.health_score_gauge.get(),
            attack_detections_total: self.attack_detections_total.get(),
            active_attack_indicators: self.attack_indicators_active.get(),
            recovery_attempts_total: self.recovery_attempts_total.get(),
            recovery_success_total: self.recovery_success_total.get(),
            recovery_failure_total: self.recovery_failure_total.get(),
            current_system_state: self.system_state_gauge.get(),
        }
    }

    /// 重置所有指标（主要用于测试）
    pub fn reset_metrics(&self) {
        // 注意：prometheus指标通常不支持重置，这里只是示例
        self.health_score_gauge.set(0);
        self.attack_indicators_active.set(0);
        self.system_state_gauge.set(0);
        self.uptime_seconds.set(0);
    }
}

/// 系统状态代码
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i64)]
pub enum SystemStateCode {
    Stopped = 0,
    Healthy = 1,
    Degraded = 2,
    Unhealthy = 3,
    Recovering = 4,
    Error = 5,
}

/// 健康监控指标摘要
#[derive(Debug, Clone)]
pub struct HealthMonitoringMetricsSummary {
    pub health_checks_total: u64,
    pub health_check_failures: u64,
    pub current_health_score: i64,
    pub attack_detections_total: u64,
    pub active_attack_indicators: i64,
    pub recovery_attempts_total: u64,
    pub recovery_success_total: u64,
    pub recovery_failure_total: u64,
    pub current_system_state: i64,
}

impl HealthMonitoringMetricsSummary {
    /// 生成指标报告
    pub fn generate_report(&self) -> String {
        let state_name = match self.current_system_state {
            0 => "Stopped",
            1 => "Healthy",
            2 => "Degraded", 
            3 => "Unhealthy",
            4 => "Recovering",
            5 => "Error",
            _ => "Unknown",
        };

        format!(
            "健康监控指标摘要:\n\
             健康检查总数: {}\n\
             健康检查失败: {}\n\
             当前健康分数: {}/100\n\
             攻击检测总数: {}\n\
             活跃攻击指标: {}\n\
             恢复尝试总数: {}\n\
             恢复成功: {}\n\
             恢复失败: {}\n\
             当前系统状态: {}",
            self.health_checks_total,
            self.health_check_failures,
            self.current_health_score,
            self.attack_detections_total,
            self.active_attack_indicators,
            self.recovery_attempts_total,
            self.recovery_success_total,
            self.recovery_failure_total,
            state_name
        )
    }

    /// 计算健康检查成功率
    pub fn health_check_success_rate(&self) -> f64 {
        if self.health_checks_total == 0 {
            return 1.0;
        }
        let successes = self.health_checks_total - self.health_check_failures;
        successes as f64 / self.health_checks_total as f64
    }

    /// 计算恢复成功率
    pub fn recovery_success_rate(&self) -> f64 {
        if self.recovery_attempts_total == 0 {
            return 1.0;
        }
        self.recovery_success_total as f64 / self.recovery_attempts_total as f64
    }
}

/// 健康状态数据结构（不依赖mgo-core）
#[derive(Debug, Clone)]
pub struct HealthStatusData {
    pub health_score: u8,
    pub consensus_healthy: bool,
    pub network_healthy: bool,
    pub storage_healthy: bool,
    pub execution_healthy: bool,
    pub is_healthy: bool,
}

/// 攻击指标数据结构（不依赖mgo-core）
#[derive(Debug, Clone)]
pub struct AttackIndicatorData {
    pub attack_type: String,
    pub confidence: f64,
    pub description: String,
}

/// 告警级别枚举（不依赖mgo-core）
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AlertLevelData {
    Info,
    Warning,
    Medium,
    High,
    Critical,
}

/// 恢复策略枚举（不依赖mgo-core）
#[derive(Debug, Clone, PartialEq)]
pub enum RecoveryStrategyData {
    None,
    RestartServices,
    Rollback { target_checkpoint: u64 },
    ColdStart,
    Hybrid { target_checkpoint: u64 },
}

/// 健康监控适配器特质
/// 
/// 这个特质定义了如何将外部健康监控数据集成到指标系统中
pub trait HealthMonitoringAdapter {
    /// 更新健康状态指标
    fn update_health_status(&self, health_status: &HealthStatusData, duration: Duration);
    
    /// 更新攻击检测指标
    fn update_attack_indicators(&self, indicators: &[AttackIndicatorData]);
    
    /// 更新系统状态指标
    fn update_system_state(&self, state: SystemStateCode, uptime: Duration);
    
    /// 记录告警事件
    fn record_alert(&self, level: AlertLevelData, channel: &str, is_success: bool, failure_reason: Option<&str>);
    
    /// 记录恢复操作事件
    fn record_recovery(&self, strategy: RecoveryStrategyData, duration: Duration, is_success: bool);
}

/// 标准健康监控适配器实现
pub struct StandardHealthMonitoringAdapter {
    integrator: Arc<HealthMonitoringMetricsIntegrator>,
    start_time: std::time::Instant,
}

impl StandardHealthMonitoringAdapter {
    /// 创建新的适配器
    pub fn new(integrator: Arc<HealthMonitoringMetricsIntegrator>) -> Self {
        Self {
            integrator,
            start_time: std::time::Instant::now(),
        }
    }

    /// 获取运行时间
    pub fn get_uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

impl HealthMonitoringAdapter for StandardHealthMonitoringAdapter {
    fn update_health_status(&self, health_status: &HealthStatusData, duration: Duration) {
        self.integrator.record_health_check(
            duration,
            health_status.health_score,
            health_status.consensus_healthy,
            health_status.network_healthy,
            health_status.storage_healthy,
            health_status.execution_healthy,
            health_status.is_healthy,
        );
    }

    fn update_attack_indicators(&self, indicators: &[AttackIndicatorData]) {
        for indicator in indicators {
            self.integrator.record_attack_detection(
                &indicator.attack_type,
                indicator.confidence,
                indicators.len(),
            );
        }
    }

    fn update_system_state(&self, state: SystemStateCode, _uptime: Duration) {
        // 使用内部计算的运行时间，而不是传入的参数
        let actual_uptime = self.start_time.elapsed();
        self.integrator.update_system_state(state, actual_uptime);
    }

    fn record_alert(
        &self,
        level: AlertLevelData,
        channel: &str,
        is_success: bool,
        failure_reason: Option<&str>,
    ) {
        let level_str = match level {
            AlertLevelData::Info => "info",
            AlertLevelData::Warning => "warning",
            AlertLevelData::Medium => "medium",
            AlertLevelData::High => "high",
            AlertLevelData::Critical => "critical",
        };

        self.integrator.record_alert(level_str, channel, is_success, failure_reason);
    }

    fn record_recovery(
        &self,
        strategy: RecoveryStrategyData,
        duration: Duration,
        is_success: bool,
    ) {
        let strategy_str = match strategy {
            RecoveryStrategyData::None => "none",
            RecoveryStrategyData::RestartServices => "restart_services",
            RecoveryStrategyData::Rollback { .. } => "rollback",
            RecoveryStrategyData::ColdStart => "cold_start",
            RecoveryStrategyData::Hybrid { .. } => "hybrid",
        };

        self.integrator.record_recovery_operation(strategy_str, duration, is_success);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_summary_calculations() {
        let summary = HealthMonitoringMetricsSummary {
            health_checks_total: 100,
            health_check_failures: 5,
            current_health_score: 85,
            attack_detections_total: 3,
            active_attack_indicators: 1,
            recovery_attempts_total: 2,
            recovery_success_total: 2,
            recovery_failure_total: 0,
            current_system_state: 1,
        };

        assert_eq!(summary.health_check_success_rate(), 0.95);
        assert_eq!(summary.recovery_success_rate(), 1.0);
        
        let report = summary.generate_report();
        assert!(report.contains("健康检查总数: 100"));
        assert!(report.contains("当前健康分数: 85/100"));
        assert!(report.contains("当前系统状态: Healthy"));
    }

    #[test]
    fn test_system_state_code_conversion() {
        assert_eq!(SystemStateCode::Healthy as i64, 1);
        assert_eq!(SystemStateCode::Error as i64, 5);
    }

    #[tokio::test]
    async fn test_metrics_integrator_creation() {
        let registry = Arc::new(Registry::new());
        let integrator = HealthMonitoringMetricsIntegrator::new(registry);
        
        assert!(integrator.is_ok());
        
        let integrator = integrator.unwrap();
        let summary = integrator.get_metrics_summary();
        
        // 新创建的指标应该都是0
        assert_eq!(summary.health_checks_total, 0);
        assert_eq!(summary.current_health_score, 0);
    }
}