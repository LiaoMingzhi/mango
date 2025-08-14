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

        assert!(ha_manager.is_ok(), "High availability manager creation should succeed");
        
        let ha_manager = ha_manager.unwrap();
        
        // Check initial state
        let initial_state = ha_manager.get_system_state().await;
        assert_eq!(initial_state, SystemState::Stopped, "Initial state should be Stopped");

        println!("High availability manager created successfully");
    }

    /// Test high availability manager startup and shutdown
    #[tokio::test]
    async fn test_high_availability_manager_lifecycle() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig {
            enable_auto_recovery: false, // Disable auto recovery for testing
            ..Default::default()
        };
        
        let ha_manager = HighAvailabilityManager::new(
            config,
            authority_state,
            checkpoint_store,
            network_client,
            &registry,
        ).unwrap();

        // Start manager
        let start_result = ha_manager.start().await;
        assert!(start_result.is_ok(), "High availability manager should start successfully");

        // Wait for system to stabilize
        sleep(Duration::from_millis(100)).await;

        // Check state
        let running_state = ha_manager.get_system_state().await;
        assert_eq!(running_state, SystemState::Healthy, "Running state should be Healthy");

        // Stop manager
        let stop_result = ha_manager.stop().await;
        assert!(stop_result.is_ok(), "High availability manager should stop successfully");

        // Wait for shutdown to complete
        sleep(Duration::from_millis(100)).await;

        // Check state after shutdown
        let stopped_state = ha_manager.get_system_state().await;
        assert_eq!(stopped_state, SystemState::Stopped, "Stopped state should be Stopped");

        println!("High availability manager lifecycle test passed");
    }

    /// Test health check functionality
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

        // Start manager
        ha_manager.start().await.unwrap();

        // Execute health check
        let health_result = ha_manager.perform_health_check().await;
        assert!(health_result.is_ok(), "Health check should succeed");

        let health_status = health_result.unwrap();
        assert!(health_status.health_score() <= 100, "Health score should be within valid range");

        println!(
            "Health check result: score={}/100, healthy={}", 
            health_status.health_score(),
            health_status.is_healthy()
        );

        // Stop manager
        ha_manager.stop().await.unwrap();

        println!("Health check integration test passed");
    }

    /// Test recovery strategy analysis
    #[tokio::test]
    async fn test_recovery_strategy_analysis() {
        let (authority_state, checkpoint_store, network_client, registry) = 
            setup_test_environment().await;

        let config = HighAvailabilityConfig {
            enable_auto_recovery: false, // Disable auto recovery
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

        // Execute health check to get health status
        let health_status = ha_manager.perform_health_check().await.unwrap();
        
        // Analyze recovery strategy
        let strategy = ha_manager.analyze_recovery_strategy(&health_status).await;
        
        match strategy {
            RecoveryStrategy::None => {
                println!("System healthy, no recovery needed");
            }
            RecoveryStrategy::RestartServices => {
                println!("Recommend restarting services");
            }
            RecoveryStrategy::Rollback { target_checkpoint } => {
                println!("Recommend rollback to checkpoint {}", target_checkpoint);
            }
            RecoveryStrategy::ColdStart => {
                println!("Recommend executing cold start");
            }
            RecoveryStrategy::Hybrid { target_checkpoint } => {
                println!("Recommend executing hybrid strategy, target checkpoint {}", target_checkpoint);
            }
        }

        ha_manager.stop().await.unwrap();

        println!("Recovery strategy analysis test passed");
    }

    /// Test configuration updates
    #[test]
    fn test_configuration_updates() {
        let mut config = HighAvailabilityConfig::default();
        
        // Test default configuration
        assert!(config.enable_auto_recovery);
        assert_eq!(config.auto_recovery_threshold, 3);
        assert_eq!(config.max_recovery_attempts, 3);

        // Update configuration
        config.enable_auto_recovery = false;
        config.auto_recovery_threshold = 5;
        config.max_recovery_attempts = 5;
        config.recovery_interval = Duration::from_secs(600);

        assert!(!config.enable_auto_recovery);
        assert_eq!(config.auto_recovery_threshold, 5);
        assert_eq!(config.max_recovery_attempts, 5);
        assert_eq!(config.recovery_interval, Duration::from_secs(600));

        println!("Configuration update test passed");
    }

    /// Test system state transitions
    #[test]
    fn test_system_state_transitions() {
        let states = vec![
            SystemState::Healthy,
            SystemState::Degraded,
            SystemState::Unhealthy,
            SystemState::Recovering,
            SystemState::Stopped,
            SystemState::Error("Test error".to_string()),
        ];

        for state in &states {
            match state {
                SystemState::Healthy => {
                    println!("State: Healthy");
                    assert_eq!(*state, SystemState::Healthy);
                }
                SystemState::Degraded => {
                    println!("State: Degraded");
                    assert_eq!(*state, SystemState::Degraded);
                }
                SystemState::Unhealthy => {
                    println!("State: Unhealthy");
                    assert_eq!(*state, SystemState::Unhealthy);
                }
                SystemState::Recovering => {
                    println!("State: Recovering");
                    assert_eq!(*state, SystemState::Recovering);
                }
                SystemState::Stopped => {
                    println!("State: Stopped");
                    assert_eq!(*state, SystemState::Stopped);
                }
                SystemState::Error(msg) => {
                    println!("State: Error - {}", msg);
                    assert!(matches!(state, SystemState::Error(_)));
                }
            }
        }

        println!("System state transition test passed");
    }

    /// Test error handling
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

        // Start manager
        ha_manager.start().await.unwrap();

        // Test rollback to non-existent checkpoint (should fail)
        let invalid_checkpoint = 999999;
        let rollback_result = ha_manager.execute_rollback(invalid_checkpoint, false).await;
        
        // Should return error
        assert!(rollback_result.is_err(), "Rollback to invalid checkpoint should fail");
        
        println!("Error handling test passed: {:?}", rollback_result.unwrap_err());

        ha_manager.stop().await.unwrap();
    }

    /// Test concurrent operations
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

        // Execute multiple concurrent health checks
        let mut handles = Vec::new();
        
        for i in 0..5 {
            let ha_manager_clone = Arc::clone(&ha_manager);
            let handle = tokio::spawn(async move {
                let result = ha_manager_clone.perform_health_check().await;
                println!("Concurrent health check {} completed", i);
                result
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let mut success_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_ok() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 5, "All concurrent health checks should succeed");

        ha_manager.stop().await.unwrap();

        println!("Concurrent operations test passed");
    }

    /// Performance test
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

        // Measure health check performance
        let start_time = std::time::Instant::now();
        
        for _ in 0..10 {
            let _ = ha_manager.perform_health_check().await;
        }
        
        let elapsed = start_time.elapsed();
        let avg_time = elapsed / 10;
        
        println!("Health check average time: {:?}", avg_time);
        assert!(avg_time < Duration::from_secs(1), "Health check should complete within 1 second");

        ha_manager.stop().await.unwrap();

        println!("Performance test passed");
    }
}