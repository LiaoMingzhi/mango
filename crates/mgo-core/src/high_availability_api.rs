// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! High availability management REST API interface
//! 
//! Provides Web API interface for managing high availability system, 
//! including health checks, recovery operations, configuration management, etc.

use std::sync::Arc;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error, instrument};

use crate::integration::{HighAvailabilityManager, SystemState};

/// Health check API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Overall health status
    pub overall_healthy: bool,
    /// Health score (0-100)
    pub health_score: u8,
    /// Consensus system health
    pub consensus_healthy: bool,
    /// Network health
    pub network_healthy: bool,
    /// Storage health
    pub storage_healthy: bool,
    /// Execution engine health
    pub execution_healthy: bool,
    /// Error details
    pub error_details: Vec<String>,
    /// 检查时间戳
    pub timestamp: u64,
}

/// 系统状态API响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusResponse {
    /// 系统状态
    pub system_state: String,
    /// 恢复尝试次数
    pub recovery_attempts: u32,
    /// 最后恢复时间
    pub last_recovery: Option<u64>,
    /// 自动恢复是否启用
    pub auto_recovery_enabled: bool,
    /// 运行时间（秒）
    pub uptime_seconds: u64,
}

/// 恢复操作请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryRequest {
    /// 恢复策略
    pub strategy: String,
    /// 目标检查点（可选，用于回滚策略）
    pub checkpoint: Option<u64>,
    /// 强制执行
    pub force: bool,
    /// 操作理由
    pub reason: Option<String>,
}

/// 恢复操作响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryResponse {
    /// 操作状态
    pub status: String,
    /// 操作ID
    pub operation_id: String,
    /// 采用的策略
    pub strategy: String,
    /// 预估完成时间（秒）
    pub estimated_duration: u64,
    /// 操作开始时间
    pub started_at: u64,
}

/// 配置更新请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    /// 启用自动恢复
    pub enable_auto_recovery: Option<bool>,
    /// 自动恢复阈值
    pub auto_recovery_threshold: Option<u8>,
    /// 最大恢复尝试次数
    pub max_recovery_attempts: Option<u32>,
    /// 恢复间隔（秒）
    pub recovery_interval_seconds: Option<u64>,
    /// 监控间隔（秒）
    pub monitor_interval_seconds: Option<u64>,
}

/// 配置响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    /// 启用自动恢复
    pub enable_auto_recovery: bool,
    /// 自动恢复阈值
    pub auto_recovery_threshold: u8,
    /// 最大恢复尝试次数
    pub max_recovery_attempts: u32,
    /// 恢复间隔（秒）
    pub recovery_interval_seconds: u64,
    /// 监控间隔（秒）
    pub monitor_interval_seconds: u64,
    /// 配置更新时间
    pub updated_at: u64,
}

/// 攻击检测响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttackDetectionResponse {
    /// 检测到的攻击指标
    pub indicators: Vec<AttackIndicatorData>,
    /// 威胁等级
    pub threat_level: String,
    /// 攻击类型
    pub attack_types: Vec<String>,
    /// 建议采取的行动
    pub recommended_actions: Vec<String>,
}

/// 攻击指标数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttackIndicatorData {
    /// 指标类型
    pub indicator_type: String,
    /// 严重程度
    pub severity: String,
    /// 描述
    pub description: String,
    /// 检测时间
    pub detected_at: u64,
}

/// 告警历史响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertHistoryResponse {
    /// 告警列表
    pub alerts: Vec<AlertData>,
    /// 总数
    pub total_count: usize,
    /// 页数信息
    pub pagination: PaginationInfo,
}

/// 告警数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertData {
    /// 告警ID
    pub id: String,
    /// 告警等级
    pub level: String,
    /// 消息
    pub message: String,
    /// 组件
    pub component: String,
    /// 通道
    pub channel: String,
    /// 创建时间
    pub created_at: u64,
    /// 是否已确认
    pub acknowledged: bool,
}

/// 分页信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationInfo {
    /// 当前页
    pub page: u32,
    /// 每页大小
    pub per_page: u32,
    /// 总页数
    pub total_pages: u32,
    /// 是否有下一页
    pub has_next: bool,
}

/// 查询参数
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    /// 页数
    pub page: Option<u32>,
    /// 每页大小
    pub per_page: Option<u32>,
    /// 过滤条件
    pub filter: Option<String>,
}

/// API处理器集合
pub struct HighAvailabilityApiHandlers {
    pub ha_manager: Arc<HighAvailabilityManager>,
}

impl HighAvailabilityApiHandlers {
    /// 创建新的API处理器
    pub fn new(ha_manager: Arc<HighAvailabilityManager>) -> Self {
        Self { ha_manager }
    }

    /// 获取健康状态
    #[instrument(level = "info", skip(self))]
    pub async fn get_health_status(&self) -> Result<HealthCheckResponse> {
        let system_state = self.ha_manager.get_system_state().await;
        
        // 执行健康检查获取详细状态
        let health_status = match self.ha_manager.perform_health_check().await {
            Ok(status) => status,
            Err(e) => {
                error!("健康检查失败: {}", e);
                return Err(e);
            }
        };

        let response = HealthCheckResponse {
            overall_healthy: matches!(system_state, SystemState::Healthy),
            health_score: health_status.health_score(),
            consensus_healthy: health_status.consensus_healthy,
            network_healthy: health_status.network_healthy,
            storage_healthy: health_status.storage_healthy,
            execution_healthy: health_status.execution_healthy,
            error_details: health_status.error_details,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        info!("健康状态检查完成，整体状态: {}", response.overall_healthy);
        Ok(response)
    }

    /// 获取详细健康状态
    #[instrument(level = "info", skip(self))]
    pub async fn get_detailed_health_status(&self) -> Result<HealthCheckResponse> {
        let system_state = self.ha_manager.get_system_state().await;
        
        // 执行健康检查获取详细状态
        let health_status = self.ha_manager.perform_health_check().await?;
        
        let response = HealthCheckResponse {
            overall_healthy: matches!(system_state, SystemState::Healthy),
            health_score: health_status.health_score(),
            consensus_healthy: health_status.consensus_healthy,
            network_healthy: health_status.network_healthy,
            storage_healthy: health_status.storage_healthy,
            execution_healthy: health_status.execution_healthy,
            error_details: health_status.error_details,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        info!("详细健康状态检查完成");
        Ok(response)
    }

    /// 获取系统状态
    #[instrument(level = "info", skip(self))]
    pub async fn get_system_status(&self) -> Result<SystemStatusResponse> {
        let system_state = self.ha_manager.get_system_state().await;
        
        let response = SystemStatusResponse {
            system_state: format!("{:?}", system_state),
            recovery_attempts: 0, // 在实际实现中从管理器获取
            last_recovery: None,
            auto_recovery_enabled: true, // 从配置获取
            uptime_seconds: 3600, // 模拟运行时间
        };
        
        info!("系统状态获取完成: {:?}", system_state);
        Ok(response)
    }

    /// 触发恢复操作
    #[instrument(level = "info", skip(self))]
    pub async fn trigger_recovery(&self, request: RecoveryRequest) -> Result<RecoveryResponse> {
        info!("收到恢复请求: strategy={}, force={}", request.strategy, request.force);
        
        let operation_id = format!("recovery-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis());
        
        // 根据策略执行不同的恢复操作
        let result = match request.strategy.as_str() {
            "rollback" => {
                let checkpoint = request.checkpoint.unwrap_or(0);
                info!("执行回滚到检查点: {}", checkpoint);
                self.ha_manager.execute_rollback(checkpoint, request.force).await
                    .map(|_| ())
            }
            "cold_start" => {
                info!("执行冷启动");
                self.ha_manager.execute_cold_start().await
                    .map(|_| ())
            }
            "none" | "restart" => {
                // 这些策略暂时只是模拟
                info!("执行策略: {}", request.strategy);
                Ok(())
            }
            _ => {
                warn!("未知的恢复策略: {}", request.strategy);
                return Err(anyhow::anyhow!("未知的恢复策略: {}", request.strategy));
            }
        };
        
        match result {
            Ok(_) => {
                let response = RecoveryResponse {
                    status: "started".to_string(),
                    operation_id,
                    strategy: request.strategy,
                    estimated_duration: 300, // 5分钟估算
                    started_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };
                
                info!("恢复操作已启动: operation_id={}", response.operation_id);
                Ok(response)
            }
            Err(e) => {
                error!("恢复操作启动失败: {}", e);
                Err(e)
            }
        }
    }

    /// 获取配置
    #[instrument(level = "info", skip(self))]
    pub async fn get_config(&self) -> Result<ConfigResponse> {
        // 在实际实现中，这里会从管理器获取当前配置
        let response = ConfigResponse {
            enable_auto_recovery: true,
            auto_recovery_threshold: 3,
            max_recovery_attempts: 3,
            recovery_interval_seconds: 300,
            monitor_interval_seconds: 30,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        info!("配置获取完成");
        Ok(response)
    }

    /// 更新配置
    #[instrument(level = "info", skip(self))]
    pub async fn update_config(&self, request: ConfigUpdateRequest) -> Result<ConfigResponse> {
        info!("收到配置更新请求");
        
        // 在实际实现中，这里会更新管理器的配置
        let response = ConfigResponse {
            enable_auto_recovery: request.enable_auto_recovery.unwrap_or(true),
            auto_recovery_threshold: request.auto_recovery_threshold.unwrap_or(3),
            max_recovery_attempts: request.max_recovery_attempts.unwrap_or(3),
            recovery_interval_seconds: request.recovery_interval_seconds.unwrap_or(300),
            monitor_interval_seconds: request.monitor_interval_seconds.unwrap_or(30),
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        
        info!("配置更新完成");
        Ok(response)
    }

    /// 获取攻击检测结果
    #[instrument(level = "info", skip(self))]
    pub async fn get_attack_detection(&self) -> Result<AttackDetectionResponse> {
        // 在实际实现中，这里会从攻击检测器获取结果
        let response = AttackDetectionResponse {
            indicators: vec![],
            threat_level: "Low".to_string(),
            attack_types: vec![],
            recommended_actions: vec!["继续监控".to_string()],
        };
        
        info!("攻击检测查询完成");
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_response_serialization() {
        let response = HealthCheckResponse {
            overall_healthy: true,
            health_score: 95,
            consensus_healthy: true,
            network_healthy: true,
            storage_healthy: true,
            execution_healthy: true,
            error_details: vec![],
            timestamp: 1234567890,
        };
        
        let json = serde_json::to_string(&response).unwrap();
        let deserialized: HealthCheckResponse = serde_json::from_str(&json).unwrap();
        
        assert!(deserialized.overall_healthy);
        assert_eq!(deserialized.health_score, 95);
    }

    #[test]
    fn test_recovery_request_serialization() {
        let request = RecoveryRequest {
            strategy: "rollback".to_string(),
            checkpoint: Some(12345),
            force: true,
            reason: Some("测试恢复".to_string()),
        };
        
        let json = serde_json::to_string(&request).unwrap();
        let deserialized: RecoveryRequest = serde_json::from_str(&json).unwrap();
        
        assert_eq!(request.strategy, deserialized.strategy);
        assert_eq!(request.checkpoint, deserialized.checkpoint);
        assert_eq!(request.force, deserialized.force);
    }

    #[test]
    fn test_config_update_request() {
        let request = ConfigUpdateRequest {
            enable_auto_recovery: Some(false),
            auto_recovery_threshold: Some(5),
            max_recovery_attempts: Some(2),
            recovery_interval_seconds: Some(600),
            monitor_interval_seconds: Some(60),
        };
        
        assert_eq!(request.enable_auto_recovery, Some(false));
        assert_eq!(request.auto_recovery_threshold, Some(5));
    }

    #[test]
    fn test_attack_detection_response() {
        let indicator = AttackIndicatorData {
            indicator_type: "unusual_pattern".to_string(),
            severity: "Medium".to_string(),
            description: "检测到异常模式".to_string(),
            detected_at: 1234567890,
        };
        
        let response = AttackDetectionResponse {
            indicators: vec![indicator],
            threat_level: "Medium".to_string(),
            attack_types: vec!["ddos".to_string()],
            recommended_actions: vec!["增强监控".to_string()],
        };
        
        assert_eq!(response.indicators.len(), 1);
        assert_eq!(response.threat_level, "Medium");
    }

    #[test]
    fn test_pagination_info() {
        let pagination = PaginationInfo {
            page: 1,
            per_page: 20,
            total_pages: 5,
            has_next: true,
        };
        
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.per_page, 20);
        assert!(pagination.has_next);
    }
}