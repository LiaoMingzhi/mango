// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! 健康监控模块测试

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::test_utils::make_authority_test_utils;

    /// 测试健康检查器基本功能
    #[tokio::test]
    async fn test_health_checker_basic() {
        // 设置测试环境
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // 创建健康检查器
        let config = HealthCheckConfig::default();
        let health_checker = HealthChecker::new(
            config,
            authority_state,
            checkpoint_store,
        );

        // 执行健康检查
        let result = health_checker.check_node_health().await;
        assert!(result.is_ok(), "健康检查应该成功: {:?}", result);

        let health_status = result.unwrap();
        
        // 验证健康状态结构
        assert!(health_status.health_score() <= 100, "健康分数应该在0-100之间");
        
        println!(
            "健康检查结果: 总分={}/100, 共识={}, 网络={}, 存储={}, 执行={}",
            health_status.health_score(),
            health_status.consensus_healthy,
            health_status.network_healthy,
            health_status.storage_healthy,
            health_status.execution_healthy
        );
    }

    /// 测试攻击检测器基本功能
    #[tokio::test]
    async fn test_attack_detector_basic() {
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // 创建攻击检测器
        let config = AttackDetectionConfig::default();
        let attack_detector = AttackDetector::new(
            config,
            authority_state,
            checkpoint_store,
        );

        // 执行攻击检测
        let result = attack_detector.detect_attack_signs().await;
        assert!(result.is_ok(), "攻击检测应该成功: {:?}", result);

        let indicators = result.unwrap();
        
        println!("检测到 {} 个攻击指标", indicators.len());
        
        for indicator in &indicators {
            println!(
                "攻击指标: {} - {} (置信度: {:.2})",
                indicator.attack_type,
                indicator.description,
                indicator.confidence
            );
        }

        // 获取检测统计信息
        let stats = attack_detector.get_detection_stats().await;
        assert!(stats.detection_count > 0, "检测次数应该大于0");
    }

    /// 测试告警管理器基本功能
    #[tokio::test]
    async fn test_alert_manager_basic() {
        // 创建告警管理器
        let config = AlertConfig::default();
        let alert_manager = AlertManager::new(config);

        // 启动告警管理器
        let start_result = alert_manager.start().await;
        assert!(start_result.is_ok(), "告警管理器启动应该成功");

        // 发送测试告警
        let alert_id = alert_manager
            .send_alert(AlertLevel::Warning, "测试告警消息")
            .await;
        assert!(alert_id.is_ok(), "发送告警应该成功");

        // 等待告警处理
        sleep(Duration::from_millis(200)).await;

        // 检查告警历史
        let history = alert_manager.get_alert_history(Some(10)).await;
        assert!(!history.is_empty(), "告警历史应该不为空");

        // 检查告警统计
        let stats = alert_manager.get_stats().await;
        assert!(stats.total_alerts > 0, "总告警数应该大于0");

        println!(
            "告警统计: 总数={}, 成功={}, 失败={}",
            stats.total_alerts,
            stats.successful_sends,
            stats.failed_sends
        );

        // 停止告警管理器
        let stop_result = alert_manager.stop().await;
        assert!(stop_result.is_ok(), "告警管理器停止应该成功");
    }

    /// 测试健康监控管理器集成功能
    #[tokio::test]
    async fn test_health_monitor_integration() {
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // 创建各个组件
        let health_checker = Arc::new(HealthChecker::new(
            HealthCheckConfig::default(),
            authority_state.clone(),
            checkpoint_store.clone(),
        ));
        
        let attack_detector = Arc::new(AttackDetector::new(
            AttackDetectionConfig::default(),
            authority_state,
            checkpoint_store,
        ));
        
        let alert_manager = Arc::new(AlertManager::new(AlertConfig::default()));
        let metrics = Arc::new(HealthMetrics::default());
        
        // 创建健康监控管理器
        let monitor_config = MonitorConfig::default();
        let health_monitor = HealthMonitor::new(
            monitor_config,
            health_checker,
            attack_detector,
            alert_manager.clone(),
            metrics,
        );

        // 启动健康监控
        let start_result = health_monitor.start().await;
        assert!(start_result.is_ok(), "健康监控启动应该成功");

        // 等待监控运行
        sleep(Duration::from_millis(500)).await;

        // 检查监控状态
        let state = health_monitor.get_state().await;
        match state {
            MonitorState::Running => println!("健康监控正在运行"),
            _ => println!("健康监控状态: {:?}", state),
        }

        // 执行一次健康检查
        let health_result = health_monitor.perform_health_check().await;
        assert!(health_result.is_ok(), "健康检查应该成功");

        let health_status = health_result.unwrap();
        println!(
            "集成健康检查结果: 总分={}/100",
            health_status.health_score()
        );

        // 检查告警历史
        let alert_history = alert_manager.get_alert_history(None).await;
        println!("生成了 {} 个告警", alert_history.len());

        // 停止健康监控
        let stop_result = health_monitor.stop().await;
        assert!(stop_result.is_ok(), "健康监控停止应该成功");
    }

    /// 测试告警级别和去重功能
    #[tokio::test]
    async fn test_alert_levels_and_deduplication() {
        let alert_manager = AlertManager::new(AlertConfig::default());
        let _ = alert_manager.start().await;

        // 测试不同级别的告警
        let levels = vec![
            AlertLevel::Info,
            AlertLevel::Warning,
            AlertLevel::Medium,
            AlertLevel::High,
            AlertLevel::Critical,
        ];

        for level in levels {
            let result = alert_manager
                .send_alert(level, &format!("测试{}级别告警", level))
                .await;
            assert!(result.is_ok(), "发送{}级别告警应该成功", level);
        }

        // 测试告警去重
        let message = "重复告警测试";
        let _id1 = alert_manager.send_alert(AlertLevel::Warning, message).await.unwrap();
        let _id2 = alert_manager.send_alert(AlertLevel::Warning, message).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        let stats = alert_manager.get_stats().await;
        println!("告警统计信息: {:?}", stats);

        let _ = alert_manager.stop().await;
    }

    /// 设置测试环境
    async fn setup_test_environment() -> (Arc<crate::authority::AuthorityState>, Arc<crate::checkpoints::CheckpointStore>) {
        let authority_test_utils = make_authority_test_utils().await;
        let authority_state = authority_test_utils.state;
        let checkpoint_store = authority_state.checkpoint_store.clone();
        
        (authority_state, checkpoint_store)
    }

    /// 测试攻击指标创建和属性
    #[test]
    fn test_attack_indicator_creation() {
        let indicator = AttackIndicator::new(
            AttackType::ConsensusAttack,
            "测试攻击描述".to_string(),
            0.9,
            8,
        )
        .with_metadata("test_key".to_string(), "test_value".to_string())
        .with_mitigation("测试缓解措施".to_string());

        assert_eq!(indicator.attack_type, AttackType::ConsensusAttack);
        assert_eq!(indicator.confidence, 0.9);
        assert_eq!(indicator.severity, 8);
        assert!(indicator.is_critical());
        assert!(indicator.metadata.contains_key("test_key"));
        assert!(!indicator.mitigation_suggestions.is_empty());
    }

    /// 测试健康状态计算
    #[test]
    fn test_health_status_calculation() {
        let mut status = HealthStatus::new();
        assert_eq!(status.health_score(), 0);
        assert!(!status.is_healthy());

        status.consensus_healthy = true;
        assert_eq!(status.health_score(), 25);

        status.network_healthy = true;
        status.storage_healthy = true;
        status.execution_healthy = true;
        assert_eq!(status.health_score(), 100);
        assert!(status.is_healthy());
    }

    /// 测试告警级别转换
    #[test]
    fn test_alert_level_conversion() {
        assert_eq!(AlertLevel::Info.to_number(), 1);
        assert_eq!(AlertLevel::Critical.to_number(), 5);
        
        assert_eq!(AlertLevel::from_number(1), AlertLevel::Info);
        assert_eq!(AlertLevel::from_number(5), AlertLevel::Critical);
        assert_eq!(AlertLevel::from_number(10), AlertLevel::Critical); // 超出范围应该返回Critical
        
        assert!(AlertLevel::Critical.is_urgent());
        assert!(!AlertLevel::Info.is_urgent());
    }
}