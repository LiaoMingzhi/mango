// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::path::PathBuf;
use tempfile::tempdir;
use prometheus::Registry;

#[tokio::test]
async fn test_rollback_config_default() {
    let config = RollbackConfig::default();
    assert_eq!(config.timeout, Duration::from_secs(300));
    assert_eq!(config.force, false);
    assert_eq!(config.auto_restart_consensus, true);
    assert_eq!(config.auto_sync_network, true);
}

#[tokio::test]
async fn test_rollback_metrics_creation() {
    let registry = Registry::new();
    let metrics = RollbackMetrics::new(&registry);
    
    // Test if metrics are created properly
    assert!(metrics.rollback_operations_total.get() >= 0);
    assert!(metrics.rollback_success_total.get() >= 0);
    assert!(metrics.rollback_failure_total.get() >= 0);
    assert!(metrics.rollback_in_progress.get() >= 0);
}

#[tokio::test]
async fn test_rollback_state_transitions() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    
    // Create mock components
    let checkpoint_store = Arc::new(CheckpointStore::new(&db_path.join("checkpoints")));
    let registry = Registry::new();
    let metrics = RollbackMetrics::new(&registry);
    
    let config = RollbackConfig::default();
    
    // Create rollback manager (using mock components)
    let rollback_manager = RollbackManager::new(
        config,
        checkpoint_store,
        Arc::new(mock_authority_state()),
        Arc::new(mock_network_client()),
        metrics,
    );
    
    // 测试初始状态
    let initial_state = rollback_manager.get_rollback_state().await;
    assert!(matches!(initial_state, RollbackState::Idle));
    
    // 测试取消操作（当没有回滚进行时）
    let cancel_result = rollback_manager.cancel_rollback().await;
    assert!(cancel_result.is_err());
}

#[tokio::test]
async fn test_rollback_error_types() {
    // 测试回滚错误类型
    let error1 = RollbackError::CheckpointNotFound(100);
    assert_eq!(error1.to_string(), "检查点 100 未找到");
    
    let error2 = RollbackError::InvalidCheckpoint(200);
    assert_eq!(error2.to_string(), "检查点 200 无效");
    
    let error3 = RollbackError::RollbackNotFeasible(300);
    assert_eq!(error3.to_string(), "回滚到检查点 300 不可行");
    
    let error4 = RollbackError::ConsensusStopFailed("test error".to_string());
    assert_eq!(error4.to_string(), "共识停止失败: test error");
}

#[tokio::test]
async fn test_rollback_result_creation() {
    let success_result = RollbackResult::Success {
        target_checkpoint: 100,
        reverted_transactions: 50,
        duration: Duration::from_secs(30),
    };
    
    let failed_result = RollbackResult::Failed {
        error: "test error".to_string(),
        partial_rollback: false,
    };
    
    let cancelled_result = RollbackResult::Cancelled;
    
    // 测试结果创建
    assert!(matches!(success_result, RollbackResult::Success { .. }));
    assert!(matches!(failed_result, RollbackResult::Failed { .. }));
    assert!(matches!(cancelled_result, RollbackResult::Cancelled));
}

// 模拟函数
fn mock_authority_state() -> AuthorityState {
    // 这是一个简化的mock实现
    // 在实际使用中，需要更完整的mock实现
    unimplemented!("需要实现完整的AuthorityState mock")
}

fn mock_network_client() -> NetworkAuthorityClient {
    // 这是一个简化的mock实现
    // 在实际使用中，需要更完整的mock实现
    unimplemented!("需要实现完整的NetworkAuthorityClient mock")
}

#[test]
fn test_rollback_config_clone() {
    let config1 = RollbackConfig {
        force: true,
        timeout: Duration::from_secs(600),
        auto_restart_consensus: false,
        auto_sync_network: false,
    };
    
    let config2 = config1.clone();
    
    assert_eq!(config1.force, config2.force);
    assert_eq!(config1.timeout, config2.timeout);
    assert_eq!(config1.auto_restart_consensus, config2.auto_restart_consensus);
    assert_eq!(config1.auto_sync_network, config2.auto_sync_network);
}

#[test]
fn test_rollback_state_clone() {
    let state1 = RollbackState::Idle;
    let state2 = state1.clone();
    
    assert!(matches!(state1, RollbackState::Idle));
    assert!(matches!(state2, RollbackState::Idle));
    
    let state3 = RollbackState::RollingBack {
        target_checkpoint: 100,
        start_time: std::time::Instant::now(),
    };
    let state4 = state3.clone();
    
    assert!(matches!(state3, RollbackState::RollingBack { .. }));
    assert!(matches!(state4, RollbackState::RollingBack { .. }));
} 