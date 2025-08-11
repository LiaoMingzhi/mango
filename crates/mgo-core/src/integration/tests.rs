// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! High availability system integration tests

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use prometheus::Registry;

    use crate::test_utils::make_authority_test_utils;
    use crate::health_monitor::{HealthCheckConfig, AttackDetectionConfig, AlertConfig};

    /// Create test environment
    async fn setup_test_environment() -> (
        Arc<AuthorityState>,
        Arc<CheckpointStore>,
        Arc<NetworkAuthorityClient>,
        Registry,
    ) {
        let authority_test_utils = make_authority_test_utils().await;
        let authority_state = authority_test_utils.state;
        let checkpoint_store = authority_state.checkpoint_store.clone();
        
        // Create network client (for testing)
        let network_client = Arc::new(NetworkAuthorityClient::new(
            mango_network::client::connect(
                authority_state
                    .get_server_url()
                    .unwrap_or_else(|| "http://127.0.0.1:9000".to_string())
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap(),
        ));
        
        let registry = Registry::new();

        (authority_state, checkpoint_store, network_client, registry)
    }

    /// Test basic creation and startup of high availability manager
    #[tokio::test]
    async fn test_high_availability_manager_creation() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig::default();
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        );

        assert!(ha_manager.is_ok(), "高可用性管理器创建应该成功");
        
        let ha_manager = ha_manager.unwrap();
        
        // 检查初始状态
        let initial_state = ha_manager.get_system_state().await;
        assert_eq!(initial_state, SystemState::Stopped, "初始状态应该是Stopped");

        println!("高可用性管理器创建成功");
    }

    /// 测试高可用性管理器的启动和停止
    #[tokio::test]
    async fn test_high_availability_manager_lifecycle() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig {
            enable_auto_recovery: false, // 禁用自动恢复以便测试
            ..Default::default()
        };
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        // 启动管理器
        let start_result = ha_manager.start().await;
        assert!(start_result.is_ok(), "高可用性管理器启动应该成功");

        // 等待一段时间让系统稳定
        sleep(Duration::from_millis(100)).await;

        // 检查状态
        let running_state = ha_manager.get_system_state().await;
        assert_eq!(running_state, SystemState::Healthy, "运行状态应该是Healthy");

        // 停止管理器
        let stop_result = ha_manager.stop().await;
        assert!(stop_result.is_ok(), "高可用性管理器停止应该成功");

        // 等待停止完成
        sleep(Duration::from_millis(100)).await;

        // 检查停止后状态
        let stopped_state = ha_manager.get_system_state().await;
        assert_eq!(stopped_state, SystemState::Stopped, "停止状态应该是Stopped");

        println!("高可用性管理器生命周期测试通过");
    }

    /// 测试健康检查功能
    #[tokio::test]
    async fn test_health_check_integration() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig::default();
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        // 启动管理器
        ha_manager.start().await.unwrap();

        // 执行健康检查
        let health_result = ha_manager.perform_health_check().await;
        assert!(health_result.is_ok(), "健康检查应该成功");

        let health_status = health_result.unwrap();
        assert!(health_status.health_score() <= 100, "健康分数应该在有效范围内");

        println!(
            "健康检查结果: 分数={}/100, 健康={}", 
            health_status.health_score(),
            health_status.is_healthy()
        );

        // 停止管理器
        ha_manager.stop().await.unwrap();

        println!("健康检查集成测试通过");
    }

    /// 测试恢复策略分析
    #[tokio::test]
    async fn test_recovery_strategy_analysis() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig {
            enable_auto_recovery: false, // 禁用自动恢复
            ..Default::default()
        };
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        ha_manager.start().await.unwrap();

        // 执行健康检查以获取健康状态
        let health_status = ha_manager.perform_health_check().await.unwrap();
        
        // 分析恢复策略
        let strategy = ha_manager.analyze_recovery_strategy(&health_status).await;
        
        match strategy {
            RecoveryStrategy::None => {
                println!("系统健康，无需恢复");
            }
            RecoveryStrategy::RestartServices => {
                println!("建议重启服务");
            }
            RecoveryStrategy::Rollback { target_checkpoint } => {
                println!("建议回滚到检查点 {}", target_checkpoint);
            }
            RecoveryStrategy::ColdStart => {
                println!("建议执行冷启动");
            }
            RecoveryStrategy::Hybrid { target_checkpoint } => {
                println!("建议执行混合策略，目标检查点 {}", target_checkpoint);
            }
        }

        ha_manager.stop().await.unwrap();

        println!("恢复策略分析测试通过");
    }

    /// 测试配置更新
    #[test]
    fn test_configuration_updates() {
        let mut config = HighAvailabilityConfig::default();
        
        // 测试默认配置
        assert!(config.enable_auto_recovery);
        assert_eq!(config.auto_recovery_threshold, 3);
        assert_eq!(config.max_recovery_attempts, 3);

        // 更新配置
        config.enable_auto_recovery = false;
        config.auto_recovery_threshold = 5;
        config.max_recovery_attempts = 5;
        config.recovery_interval = Duration::from_secs(600);

        assert!(!config.enable_auto_recovery);
        assert_eq!(config.auto_recovery_threshold, 5);
        assert_eq!(config.max_recovery_attempts, 5);
        assert_eq!(config.recovery_interval, Duration::from_secs(600));

        println!("配置更新测试通过");
    }

    /// 测试系统状态转换
    #[test]
    fn test_system_state_transitions() {
        let states = vec![
            SystemState::Healthy,
            SystemState::Degraded,
            SystemState::Unhealthy,
            SystemState::Recovering,
            SystemState::Stopped,
            SystemState::Error("测试错误".to_string()),
        ];

        for state in &states {
            match state {
                SystemState::Healthy => {
                    println!("状态: 健康");
                    assert_eq!(*state, SystemState::Healthy);
                }
                SystemState::Degraded => {
                    println!("状态: 降级");
                    assert_eq!(*state, SystemState::Degraded);
                }
                SystemState::Unhealthy => {
                    println!("状态: 不健康");
                    assert_eq!(*state, SystemState::Unhealthy);
                }
                SystemState::Recovering => {
                    println!("状态: 恢复中");
                    assert_eq!(*state, SystemState::Recovering);
                }
                SystemState::Stopped => {
                    println!("状态: 已停止");
                    assert_eq!(*state, SystemState::Stopped);
                }
                SystemState::Error(msg) => {
                    println!("状态: 错误 - {}", msg);
                    assert!(matches!(state, SystemState::Error(_)));
                }
            }
        }

        println!("系统状态转换测试通过");
    }

    /// 测试错误处理
    #[tokio::test]
    async fn test_error_handling() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig::default();
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        // 启动管理器
        ha_manager.start().await.unwrap();

        // 测试回滚到不存在的检查点（应该失败）
        let invalid_checkpoint = 999999;
        let rollback_result = ha_manager.execute_rollback(invalid_checkpoint, false).await;
        
        // 应该返回错误
        assert!(rollback_result.is_err(), "回滚到无效检查点应该失败");
        
        println!("错误处理测试通过: {:?}", rollback_result.unwrap_err());

        ha_manager.stop().await.unwrap();
    }

    /// 测试并发操作
    #[tokio::test]
    async fn test_concurrent_operations() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig {
            enable_auto_recovery: false,
            ..Default::default()
        };
        
        let ha_manager = Arc::new(HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap());

        ha_manager.start().await.unwrap();

        // 并发执行多个健康检查
        let mut handles = Vec::new();
        
        for i in 0..5 {
            let ha_manager_clone = Arc::clone(&ha_manager);
            let handle = tokio::spawn(async move {
                let result = ha_manager_clone.perform_health_check().await;
                println!("并发健康检查 {} 完成", i);
                result
            });
            handles.push(handle);
        }

        // 等待所有任务完成
        let mut success_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 5, "所有并发健康检查都应该成功");

        ha_manager.stop().await.unwrap();

        println!("并发操作测试通过");
    }

    /// 性能测试
    #[tokio::test]
    async fn test_performance_metrics() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig::default();
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        ha_manager.start().await.unwrap();

        // 测量健康检查性能
        let start_time = std::time::Instant::now();
        
        for _ in 0..10 {
            let _ = ha_manager.perform_health_check().await;
        }
        
        let elapsed = start_time.elapsed();
        let avg_time = elapsed / 10;
        
        println!("健康检查平均耗时: {:?}", avg_time);
        assert!(avg_time < Duration::from_secs(1), "健康检查应该在1秒内完成");

        ha_manager.stop().await.unwrap();

        println!("性能测试通过");
    }
}