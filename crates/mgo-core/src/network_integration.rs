// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 网络层集成模块
//! 
//! 将冷启动管理器和健康监控系统与网络层功能进行集成

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn, instrument};

use crate::cold_start::{ColdStartManager, ColdStartResult};
use crate::health_monitor::{HealthMonitor, HealthStatus};
use crate::authority::AuthorityState;

/// 网络集成管理器
/// 
/// 这个结构体负责协调冷启动管理器和健康监控系统与网络层的交互
pub struct NetworkIntegrationManager {
    cold_start_manager: Arc<ColdStartManager>,
    health_monitor: Arc<HealthMonitor>,
    authority_state: Arc<AuthorityState>,
}

impl NetworkIntegrationManager {
    /// 创建新的网络集成管理器
    pub fn new(
        cold_start_manager: Arc<ColdStartManager>,
        health_monitor: Arc<HealthMonitor>,
        authority_state: Arc<AuthorityState>,
    ) -> Self {
        Self {
            cold_start_manager,
            health_monitor,
            authority_state,
        }
    }

    /// 执行网络感知的冷启动
    /// 
    /// 这个方法集成了网络发现和状态同步的冷启动过程
    #[instrument(level = "info", skip(self))]
    pub async fn perform_network_aware_cold_start(&self) -> Result<ColdStartResult> {
        info!("开始网络感知的冷启动");

        // 1. 执行健康检查以了解网络状态
        let health_status = self.health_monitor.perform_health_check().await?;
        
        if !health_status.network_healthy {
            warn!("网络健康状态异常，将在冷启动过程中特别关注网络恢复");
        }

        // 2. 执行冷启动，集成网络层功能
        let cold_start_result = self.cold_start_manager.perform_cold_start().await?;

        // 3. 验证网络恢复状态
        let post_recovery_health = self.health_monitor.perform_health_check().await?;
        
        if post_recovery_health.network_healthy {
            info!("网络感知的冷启动完成，网络健康状态已恢复");
        } else {
            warn!("冷启动完成，但网络健康状态仍需关注");
        }

        Ok(cold_start_result)
    }

    /// 获取网络状态摘要
    pub async fn get_network_status_summary(&self) -> Result<NetworkStatusSummary> {
        let health_status = self.health_monitor.perform_health_check().await?;
        let cold_start_state = self.cold_start_manager.get_cold_start_state().await;
        
        // 获取网络连接信息
        let committee = self.authority_state.committee_store()
            .get_latest_committee();
        
        let network_info = NetworkInfo {
            total_validators: committee.num_members(),
            current_epoch: self.authority_state.current_epoch_for_testing(),
            network_healthy: health_status.network_healthy,
        };

        let consensus_healthy = health_status.consensus_healthy;
        let storage_healthy = health_status.storage_healthy;
        let execution_healthy = health_status.execution_healthy;
        
        Ok(NetworkStatusSummary {
            health_status,
            cold_start_state,
            network_info,
            consensus_healthy,
            storage_healthy,
            execution_healthy,
        })
    }

    /// 触发网络相关的健康检查
    pub async fn trigger_network_health_check(&self) -> Result<NetworkHealthReport> {
        let health_status = self.health_monitor.perform_health_check().await?;
        
        // 分析网络相关的健康状态
        let network_issues = self.analyze_network_issues(&health_status).await;
        
        let recommendations = self.generate_network_recommendations(&health_status, &network_issues).await;
        
        Ok(NetworkHealthReport {
            overall_healthy: health_status.network_healthy,
            health_score: health_status.health_score(),
            network_issues,
            recommendations,
        })
    }

    /// 分析网络问题
    async fn analyze_network_issues(&self, health_status: &HealthStatus) -> Vec<NetworkIssue> {
        let mut issues = Vec::new();

        if !health_status.network_healthy {
            issues.push(NetworkIssue {
                issue_type: NetworkIssueType::ConnectivityProblem,
                severity: IssueSeverity::High,
                description: "网络连接健康检查失败".to_string(),
                affected_components: vec!["network".to_string()],
            });
        }

        if !health_status.consensus_healthy {
            issues.push(NetworkIssue {
                issue_type: NetworkIssueType::ConsensusIssue,
                severity: IssueSeverity::Critical,
                description: "共识系统健康检查失败，可能影响网络同步".to_string(),
                affected_components: vec!["consensus".to_string(), "network".to_string()],
            });
        }

        // 检查是否有错误详情
        for error in &health_status.error_details {
            if error.to_lowercase().contains("network") || error.to_lowercase().contains("connection") {
                issues.push(NetworkIssue {
                    issue_type: NetworkIssueType::ConfigurationError,
                    severity: IssueSeverity::Medium,
                    description: format!("网络配置或连接问题: {}", error),
                    affected_components: vec!["network".to_string()],
                });
            }
        }

        issues
    }

    /// 生成网络恢复建议
    async fn generate_network_recommendations(
        &self,
        health_status: &HealthStatus,
        network_issues: &[NetworkIssue],
    ) -> Vec<NetworkRecommendation> {
        let mut recommendations = Vec::new();

        if !health_status.network_healthy && network_issues.iter().any(|i| i.severity == IssueSeverity::Critical) {
            recommendations.push(NetworkRecommendation {
                action: RecommendedAction::PerformColdStart,
                priority: ActionPriority::High,
                description: "检测到严重网络问题，建议执行冷启动以重新建立网络连接".to_string(),
                estimated_duration: std::time::Duration::from_secs(300), // 5分钟
            });
        } else if !health_status.network_healthy {
            recommendations.push(NetworkRecommendation {
                action: RecommendedAction::RestartNetworkServices,
                priority: ActionPriority::Medium,
                description: "网络健康状态异常，建议重启网络服务".to_string(),
                estimated_duration: std::time::Duration::from_secs(60), // 1分钟
            });
        }

        if health_status.health_score() < 50 {
            recommendations.push(NetworkRecommendation {
                action: RecommendedAction::CheckNetworkConfiguration,
                priority: ActionPriority::Medium,
                description: "整体健康分数较低，建议检查网络配置".to_string(),
                estimated_duration: std::time::Duration::from_secs(120), // 2分钟
            });
        }

        recommendations
    }
}

/// 网络状态摘要
#[derive(Debug, Clone)]
pub struct NetworkStatusSummary {
    /// 健康状态
    pub health_status: HealthStatus,
    /// 冷启动状态
    pub cold_start_state: crate::cold_start::ColdStartState,
    /// 网络信息
    pub network_info: NetworkInfo,
    /// 共识健康状态
    pub consensus_healthy: bool,
    /// 存储健康状态
    pub storage_healthy: bool,
    /// 执行健康状态
    pub execution_healthy: bool,
}

/// 网络信息
#[derive(Debug, Clone)]
pub struct NetworkInfo {
    /// 验证器总数
    pub total_validators: usize,
    /// 当前纪元
    pub current_epoch: u64,
    /// 网络健康状态
    pub network_healthy: bool,
}

/// 网络健康报告
#[derive(Debug, Clone)]
pub struct NetworkHealthReport {
    /// 整体网络健康状态
    pub overall_healthy: bool,
    /// 健康分数
    pub health_score: u8,
    /// 网络问题列表
    pub network_issues: Vec<NetworkIssue>,
    /// 修复建议
    pub recommendations: Vec<NetworkRecommendation>,
}

/// 网络问题
#[derive(Debug, Clone)]
pub struct NetworkIssue {
    /// 问题类型
    pub issue_type: NetworkIssueType,
    /// 严重程度
    pub severity: IssueSeverity,
    /// 问题描述
    pub description: String,
    /// 受影响的组件
    pub affected_components: Vec<String>,
}

/// 网络问题类型
#[derive(Debug, Clone, PartialEq)]
pub enum NetworkIssueType {
    /// 连接问题
    ConnectivityProblem,
    /// 共识问题
    ConsensusIssue,
    /// 配置错误
    ConfigurationError,
    /// 性能问题
    PerformanceIssue,
    /// 同步问题
    SynchronizationIssue,
}

/// 问题严重程度
#[derive(Debug, Clone, PartialEq)]
pub enum IssueSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// 网络恢复建议
#[derive(Debug, Clone)]
pub struct NetworkRecommendation {
    /// 建议的操作
    pub action: RecommendedAction,
    /// 优先级
    pub priority: ActionPriority,
    /// 描述
    pub description: String,
    /// 预估耗时
    pub estimated_duration: std::time::Duration,
}

/// 建议的操作
#[derive(Debug, Clone, PartialEq)]
pub enum RecommendedAction {
    /// 执行冷启动
    PerformColdStart,
    /// 重启网络服务
    RestartNetworkServices,
    /// 检查网络配置
    CheckNetworkConfiguration,
    /// 重新同步状态
    ResynchronizeState,
    /// 联系网络管理员
    ContactAdministrator,
}

/// 操作优先级
#[derive(Debug, Clone, PartialEq)]
pub enum ActionPriority {
    Low,
    Medium,
    High,
    Critical,
}

impl NetworkStatusSummary {
    /// 生成状态报告
    pub fn generate_report(&self) -> String {
        format!(
            "网络状态摘要报告:\n\
             整体健康分数: {}/100\n\
             网络健康: {}\n\
             共识健康: {}\n\
             存储健康: {}\n\
             执行健康: {}\n\
             验证器总数: {}\n\
             当前纪元: {}\n\
             冷启动状态: {:?}",
            self.health_status.health_score(),
            if self.network_info.network_healthy { "✓" } else { "✗" },
            if self.consensus_healthy { "✓" } else { "✗" },
            if self.storage_healthy { "✓" } else { "✗" },
            if self.execution_healthy { "✓" } else { "✗" },
            self.network_info.total_validators,
            self.network_info.current_epoch,
            self.cold_start_state
        )
    }
}

impl NetworkHealthReport {
    /// 生成健康报告
    pub fn generate_report(&self) -> String {
        let mut report = format!(
            "网络健康报告:\n\
             整体状态: {}\n\
             健康分数: {}/100\n",
            if self.overall_healthy { "健康" } else { "异常" },
            self.health_score
        );

        if !self.network_issues.is_empty() {
            report.push_str("\n发现的问题:\n");
            for (i, issue) in self.network_issues.iter().enumerate() {
                report.push_str(&format!(
                    "  {}. [{:?}] {}\n     受影响组件: {}\n",
                    i + 1,
                    issue.severity,
                    issue.description,
                    issue.affected_components.join(", ")
                ));
            }
        }

        if !self.recommendations.is_empty() {
            report.push_str("\n修复建议:\n");
            for (i, rec) in self.recommendations.iter().enumerate() {
                report.push_str(&format!(
                    "  {}. [{:?}] {}\n     预估耗时: {:?}\n",
                    i + 1,
                    rec.priority,
                    rec.description,
                    rec.estimated_duration
                ));
            }
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_issue_types() {
        let issue = NetworkIssue {
            issue_type: NetworkIssueType::ConnectivityProblem,
            severity: IssueSeverity::High,
            description: "测试问题".to_string(),
            affected_components: vec!["network".to_string()],
        };

        assert_eq!(issue.issue_type, NetworkIssueType::ConnectivityProblem);
        assert_eq!(issue.severity, IssueSeverity::High);
    }

    #[test]
    fn test_recommended_actions() {
        let recommendation = NetworkRecommendation {
            action: RecommendedAction::PerformColdStart,
            priority: ActionPriority::High,
            description: "测试建议".to_string(),
            estimated_duration: std::time::Duration::from_secs(300),
        };

        assert_eq!(recommendation.action, RecommendedAction::PerformColdStart);
        assert_eq!(recommendation.priority, ActionPriority::High);
    }

    #[test]
    fn test_network_info() {
        let network_info = NetworkInfo {
            total_validators: 10,
            current_epoch: 5,
            network_healthy: true,
        };

        assert_eq!(network_info.total_validators, 10);
        assert_eq!(network_info.current_epoch, 5);
        assert!(network_info.network_healthy);
    }
}