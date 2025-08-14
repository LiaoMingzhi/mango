// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Health monitoring module tests

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::test_utils::make_authority_test_utils;

    /// Test basic health checker functionality
    #[tokio::test]
    async fn test_health_checker_basic() {
        // Setup test environment
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // Create health checker
        let config = HealthCheckConfig::default();
        let health_checker = HealthChecker::new(
            config,
            authority_state,
            checkpoint_store,
        );

        // Execute health check
        let result = health_checker.check_node_health().await;
        assert!(result.is_ok(), "Health check should succeed: {:?}", result);

        let health_status = result.unwrap();
        
        // Verify health status structure
        assert!(health_status.health_score() <= 100, "Health score should be between 0-100");
        
        println!(
            "Health check result: score={}/100, consensus={}, network={}, storage={}, execution={}",
            health_status.health_score(),
            health_status.consensus_healthy,
            health_status.network_healthy,
            health_status.storage_healthy,
            health_status.execution_healthy
        );
    }

    /// Test attack detector basic functionality
    #[tokio::test]
    async fn test_attack_detector_basic() {
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // Create attack detector
        let config = AttackDetectionConfig::default();
        let attack_detector = AttackDetector::new(
            config,
            authority_state,
            checkpoint_store,
        );

        // Execute attack detection
        let result = attack_detector.detect_attack_signs().await;
        assert!(result.is_ok(), "Attack detection should succeed: {:?}", result);

        let indicators = result.unwrap();
        
        println!("Detected {} attack indicators", indicators.len());
        
        for indicator in &indicators {
            println!(
                "Attack indicator: {} - {} (confidence: {:.2})",
                indicator.attack_type,
                indicator.description,
                indicator.confidence
            );
        }

        // Get detection statistics
        let stats = attack_detector.get_detection_stats().await;
        assert!(stats.detection_count > 0, "Detection count should be greater than 0");
    }

    /// Test alert manager basic functionality
    #[tokio::test]
    async fn test_alert_manager_basic() {
        // Create alert manager
        let config = AlertConfig::default();
        let alert_manager = AlertManager::new(config);

        // Start alert manager
        let start_result = alert_manager.start().await;
        assert!(start_result.is_ok(), "Alert manager should start successfully");

        // Send test alert
        let alert_id = alert_manager
            .send_alert(AlertLevel::Warning, "Test alert message")
            .await;
        assert!(alert_id.is_ok(), "Sending alert should succeed");

        // Wait for alert processing
        sleep(Duration::from_millis(200)).await;

        // Check alert history
        let history = alert_manager.get_alert_history(Some(10)).await;
        assert!(!history.is_empty(), "Alert history should not be empty");

        // Check alert statistics
        let stats = alert_manager.get_stats().await;
        assert!(stats.total_alerts > 0, "Total alerts should be greater than 0");

        println!(
            "Alert statistics: total={}, successful={}, failed={}",
            stats.total_alerts,
            stats.successful_sends,
            stats.failed_sends
        );

        // Stop alert manager
        let stop_result = alert_manager.stop().await;
        assert!(stop_result.is_ok(), "Alert manager should stop successfully");
    }

    /// Test health monitor manager integration functionality
    #[tokio::test]
    async fn test_health_monitor_integration() {
        let (authority_state, checkpoint_store) = setup_test_environment().await;
        
        // Create components
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
        
        // Create health monitor manager
        let monitor_config = MonitorConfig::default();
        let health_monitor = HealthMonitor::new(
            monitor_config,
            health_checker,
            attack_detector,
            alert_manager.clone(),
            metrics,
        );

        // Start health monitoring
        let start_result = health_monitor.start().await;
        assert!(start_result.is_ok(), "Health monitoring should start successfully");

        // Wait for monitoring to run
        sleep(Duration::from_millis(500)).await;

        // Check monitoring state
        let state = health_monitor.get_state().await;
        match state {
            MonitorState::Running => println!("Health monitoring is running"),
            _ => println!("Health monitoring state: {:?}", state),
        }

        // Execute one health check
        let health_result = health_monitor.perform_health_check().await;
        assert!(health_result.is_ok(), "Health check should succeed");

        let health_status = health_result.unwrap();
        println!(
            "Integrated health check result: score={}/100",
            health_status.health_score()
        );

        // Check alert history
        let alert_history = alert_manager.get_alert_history(None).await;
        println!("Generated {} alerts", alert_history.len());

        // Stop health monitoring
        let stop_result = health_monitor.stop().await;
        assert!(stop_result.is_ok(), "Health monitoring should stop successfully");
    }

    /// Test alert levels and deduplication functionality
    #[tokio::test]
    async fn test_alert_levels_and_deduplication() {
        let alert_manager = AlertManager::new(AlertConfig::default());
        let _ = alert_manager.start().await;

        // Test different alert levels
        let levels = vec![
            AlertLevel::Info,
            AlertLevel::Warning,
            AlertLevel::Medium,
            AlertLevel::High,
            AlertLevel::Critical,
        ];

        for level in levels {
            let result = alert_manager
                .send_alert(level, &format!("Test {} level alert", level))
                .await;
            assert!(result.is_ok(), "Sending {} level alert should succeed", level);
        }

        // Test alert deduplication
        let message = "Duplicate alert test";
        let _id1 = alert_manager.send_alert(AlertLevel::Warning, message).await.unwrap();
        let _id2 = alert_manager.send_alert(AlertLevel::Warning, message).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        let stats = alert_manager.get_stats().await;
        println!("Alert statistics: {:?}", stats);

        let _ = alert_manager.stop().await;
    }

    /// Setup test environment
    async fn setup_test_environment() -> (Arc<crate::authority::AuthorityState>, Arc<crate::checkpoints::CheckpointStore>) {
        let authority_test_utils = make_authority_test_utils().await;
        let authority_state = authority_test_utils.state;
        let checkpoint_store = authority_state.checkpoint_store.clone();
        
        (authority_state, checkpoint_store)
    }

    /// Test attack indicator creation and properties
    #[test]
    fn test_attack_indicator_creation() {
        let indicator = AttackIndicator::new(
            AttackType::ConsensusAttack,
            "Test attack description".to_string(),
            0.9,
            8,
        )
        .with_metadata("test_key".to_string(), "test_value".to_string())
        .with_mitigation("Test mitigation measure".to_string());

        assert_eq!(indicator.attack_type, AttackType::ConsensusAttack);
        assert_eq!(indicator.confidence, 0.9);
        assert_eq!(indicator.severity, 8);
        assert!(indicator.is_critical());
        assert!(indicator.metadata.contains_key("test_key"));
        assert!(!indicator.mitigation_suggestions.is_empty());
    }

    /// Test health status calculation
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

    /// Test alert level conversion
    #[test]
    fn test_alert_level_conversion() {
        assert_eq!(AlertLevel::Info.to_number(), 1);
        assert_eq!(AlertLevel::Critical.to_number(), 5);
        
        assert_eq!(AlertLevel::from_number(1), AlertLevel::Info);
        assert_eq!(AlertLevel::from_number(5), AlertLevel::Critical);
        assert_eq!(AlertLevel::from_number(10), AlertLevel::Critical); // Out of range should return Critical
        
        assert!(AlertLevel::Critical.is_urgent());
        assert!(!AlertLevel::Info.is_urgent());
    }
}