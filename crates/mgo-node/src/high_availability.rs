// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! High availability extension module for mgo-node
//! 
//! Provides high availability management functionality integrated with MgoNode

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, warn};
use prometheus::Registry;

use mgo_core::authority::AuthorityState;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::authority_client::NetworkAuthorityClient;
use mgo_core::integration::{
    HighAvailabilityManager, HighAvailabilityConfig, SystemState
};

/// High availability extension for MgoNode
/// 
/// This struct integrates the high availability manager with MgoNode,
/// providing node-level fault detection and automatic recovery functionality
pub struct MgoNodeHighAvailability {
    ha_manager: Arc<HighAvailabilityManager>,
    node_name: String,
}

impl MgoNodeHighAvailability {
    /// Create a new high availability extension
    pub fn new(
        config: HighAvailabilityConfig,
        authority_state: Arc<AuthorityState>,
        checkpoint_store: Arc<CheckpointStore>,
        network_client: Arc<NetworkAuthorityClient>,
        registry: &Registry,
        node_name: String,
    ) -> Result<Self> {
        let ha_manager = Arc::new(HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            registry,
        )?);

        Ok(Self {
            ha_manager,
            node_name,
        })
    }

    /// 启动高可用性管理
    pub async fn start(&self) -> Result<()> {
        info!("为节点 {} 启动高可用性管理", self.node_name);
        self.ha_manager.start().await
    }

    /// 停止高可用性管理
    pub async fn stop(&self) -> Result<()> {
        info!("为节点 {} 停止高可用性管理", self.node_name);
        self.ha_manager.stop().await
    }

    /// 获取系统状态
    pub async fn get_system_state(&self) -> SystemState {
        self.ha_manager.get_system_state().await
    }

    /// 获取健康状态摘要
    pub async fn get_health_summary(&self) -> Result<HealthSummary> {
        let health_status = self.ha_manager.perform_health_check().await?;
        let system_state = self.ha_manager.get_system_state().await;
        let recovery_attempts = self.ha_manager.get_recovery_attempts().await;

        Ok(HealthSummary {
            node_name: self.node_name.clone(),
            health_score: health_status.health_score(),
            is_healthy: health_status.is_healthy(),
            system_state,
            recovery_attempts,
            consensus_healthy: health_status.consensus_healthy,
            network_healthy: health_status.network_healthy,
            storage_healthy: health_status.storage_healthy,
            execution_healthy: health_status.execution_healthy,
            error_count: health_status.error_details.len(),
        })
    }

    /// 手动触发健康检查
    pub async fn trigger_health_check(&self) -> Result<mgo_core::health_monitor::HealthStatus> {
        info!("为节点 {} 手动触发健康检查", self.node_name);
        self.ha_manager.perform_health_check().await
    }

    /// 手动执行回滚
    pub async fn trigger_rollback(
        &self,
        target_checkpoint: mgo_types::messages_checkpoint::CheckpointSequenceNumber,
        force: bool,
    ) -> Result<mgo_core::rollback::RollbackResult> {
        warn!(
            "为节点 {} 手动触发回滚到检查点 {}",
            self.node_name, target_checkpoint
        );
        self.ha_manager.execute_rollback(target_checkpoint, force).await
    }

    /// 手动执行冷启动
    pub async fn trigger_cold_start(&self) -> Result<mgo_core::cold_start::ColdStartResult> {
        warn!("为节点 {} 手动触发冷启动", self.node_name);
        self.ha_manager.execute_cold_start().await
    }

    /// 重置恢复尝试计数
    pub async fn reset_recovery_attempts(&self) {
        info!("为节点 {} 重置恢复尝试计数", self.node_name);
        self.ha_manager.reset_recovery_attempts().await;
    }

    /// 获取配置
    pub fn get_config(&self) -> &HighAvailabilityConfig {
        self.ha_manager.get_config()
    }
}

/// 健康状态摘要
#[derive(Debug, Clone)]
pub struct HealthSummary {
    /// 节点名称
    pub node_name: String,
    /// 健康分数 (0-100)
    pub health_score: u8,
    /// 是否健康
    pub is_healthy: bool,
    /// 系统状态
    pub system_state: SystemState,
    /// 恢复尝试次数
    pub recovery_attempts: u32,
    /// 共识系统健康
    pub consensus_healthy: bool,
    /// 网络健康
    pub network_healthy: bool,
    /// 存储健康
    pub storage_healthy: bool,
    /// 执行健康
    pub execution_healthy: bool,
    /// 错误数量
    pub error_count: usize,
}

impl HealthSummary {
    /// 生成健康报告文本
    pub fn generate_report(&self) -> String {
        format!(
            "节点健康报告 - {}\n\
             健康分数: {}/100\n\
             系统状态: {:?}\n\
             恢复尝试: {}\n\
             组件状态:\n\
             - 共识: {}\n\
             - 网络: {}\n\
             - 存储: {}\n\
             - 执行: {}\n\
             错误数量: {}",
            self.node_name,
            self.health_score,
            self.system_state,
            self.recovery_attempts,
            if self.consensus_healthy { "✓" } else { "✗" },
            if self.network_healthy { "✓" } else { "✗" },
            if self.storage_healthy { "✓" } else { "✗" },
            if self.execution_healthy { "✓" } else { "✗" },
            self.error_count
        )
    }
}

/// 高可用性扩展的构建器
pub struct MgoNodeHighAvailabilityBuilder {
    config: HighAvailabilityConfig,
    node_name: String,
}

impl MgoNodeHighAvailabilityBuilder {
    /// 创建新的构建器
    pub fn new() -> Self {
        Self {
            config: HighAvailabilityConfig::default(),
            node_name: "unknown".to_string(),
        }
    }

    /// 设置节点名称
    pub fn with_node_name(mut self, name: String) -> Self {
        self.node_name = name;
        self
    }

    /// 设置配置
    pub fn with_config(mut self, config: HighAvailabilityConfig) -> Self {
        self.config = config;
        self
    }

    /// 启用自动恢复
    pub fn enable_auto_recovery(mut self) -> Self {
        self.config.enable_auto_recovery = true;
        self
    }

    /// 禁用自动恢复
    pub fn disable_auto_recovery(mut self) -> Self {
        self.config.enable_auto_recovery = false;
        self
    }

    /// 设置恢复阈值
    pub fn with_recovery_threshold(mut self, threshold: u8) -> Self {
        self.config.auto_recovery_threshold = threshold;
        self
    }

    /// 构建高可用性扩展
    pub fn build(
        self,
        authority_state: Arc<AuthorityState>,
        checkpoint_store: Arc<CheckpointStore>,
        network_client: Arc<NetworkAuthorityClient>,
        registry: &Registry,
    ) -> Result<MgoNodeHighAvailability> {
        MgoNodeHighAvailability::new(
            self.config,
            authority_state,
            checkpoint_store,
            network_client,
            registry,
            self.node_name,
        )
    }
}

impl Default for MgoNodeHighAvailabilityBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 用于与现有 MgoNode 集成的辅助宏
#[macro_export]
macro_rules! integrate_high_availability {
    ($node:expr, $ha_config:expr) => {{
        use $crate::high_availability::MgoNodeHighAvailabilityBuilder;
        
        let ha_extension = MgoNodeHighAvailabilityBuilder::new()
            .with_node_name($node.state.name.concise().to_string())
            .with_config($ha_config)
            .build(
                $node.state.clone(),
                $node.checkpoint_store.clone(),
                // 注意：这里需要根据实际的网络客户端实现来调整
                // Arc::new(NetworkAuthorityClient::new(...)),
                $node.registry_service.default_registry(),
            )?;
        
        ha_extension.start().await?;
        ha_extension
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use mgo_core::integration::HighAvailabilityConfig;

    #[test]
    fn test_builder_pattern() {
        let builder = MgoNodeHighAvailabilityBuilder::new()
            .with_node_name("test-node".to_string())
            .enable_auto_recovery()
            .with_recovery_threshold(2);

        assert_eq!(builder.node_name, "test-node");
        assert!(builder.config.enable_auto_recovery);
        assert_eq!(builder.config.auto_recovery_threshold, 2);
    }

    #[test]
    fn test_health_summary_report() {
        let summary = HealthSummary {
            node_name: "test-node".to_string(),
            health_score: 85,
            is_healthy: true,
            system_state: SystemState::Healthy,
            recovery_attempts: 0,
            consensus_healthy: true,
            network_healthy: true,
            storage_healthy: false,
            execution_healthy: true,
            error_count: 1,
        };

        let report = summary.generate_report();
        assert!(report.contains("test-node"));
        assert!(report.contains("85/100"));
        assert!(report.contains("错误数量: 1"));
    }

    #[test]
    fn test_default_config() {
        let config = HighAvailabilityConfig::default();
        assert!(config.enable_auto_recovery);
        assert_eq!(config.auto_recovery_threshold, 3);
        assert_eq!(config.max_recovery_attempts, 3);
    }
}