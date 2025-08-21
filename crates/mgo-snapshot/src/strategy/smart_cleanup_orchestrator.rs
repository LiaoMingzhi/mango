// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Smart cleanup orchestrator that coordinates all cleanup strategies

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, instrument, warn};

use crate::strategy::{
    AutoCleanupManager, CleanupResult, 
    IntelligentCleanupManager, IntelligentCleanupResult,
};
use crate::types::error::SnapshotError;

/// Smart cleanup orchestrator that coordinates multiple cleanup strategies
pub struct SmartCleanupOrchestrator {
    auto_cleanup: Arc<AutoCleanupManager>,
    intelligent_cleanup: Option<Arc<IntelligentCleanupManager>>,
    config: Arc<RwLock<OrchestratorConfig>>,
    state: Arc<RwLock<OrchestratorState>>,
}

impl SmartCleanupOrchestrator {
    /// Create a new smart cleanup orchestrator
    pub fn new(
        auto_cleanup: Arc<AutoCleanupManager>,
        intelligent_cleanup: Option<Arc<IntelligentCleanupManager>>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            auto_cleanup,
            intelligent_cleanup,
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(OrchestratorState::new())),
        }
    }

    /// Start the orchestrated cleanup system
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<(), SnapshotError> {
        info!("Starting smart cleanup orchestrator");
        
        let config = self.config.read().await;
        
        if config.enable_scheduled_cleanup {
            let orchestrator = self.clone();
            tokio::spawn(async move {
                orchestrator.run_scheduled_cleanup().await;
            });
        }
        
        if config.enable_adaptive_cleanup {
            let orchestrator = self.clone();
            tokio::spawn(async move {
                orchestrator.run_adaptive_cleanup().await;
            });
        }
        
        if config.enable_emergency_cleanup {
            let orchestrator = self.clone();
            tokio::spawn(async move {
                orchestrator.monitor_emergency_conditions().await;
            });
        }
        
        Ok(())
    }

    /// Execute comprehensive cleanup using all available strategies
    #[instrument(level = "info", skip(self))]
    pub async fn execute_comprehensive_cleanup(&self) -> Result<ComprehensiveCleanupResult, SnapshotError> {
        info!("Starting comprehensive cleanup");
        
        let start_time = Instant::now();
        let mut result = ComprehensiveCleanupResult::new();
        
        // Phase 1: Intelligent cleanup (if available and enabled)
        if let Some(intelligent_cleanup) = &self.intelligent_cleanup {
            let config = self.config.read().await;
            if config.enable_intelligent_cleanup {
                match intelligent_cleanup.execute_intelligent_cleanup().await {
                    Ok(intelligent_result) => {
                        result.intelligent_result = Some(intelligent_result);
                        info!("Intelligent cleanup completed successfully");
                    }
                    Err(e) => {
                        error!("Intelligent cleanup failed: {}", e);
                        result.errors.push(format!("Intelligent cleanup failed: {}", e));
                    }
                }
            }
        }
        
        // Phase 2: Basic automatic cleanup (always execute as fallback)
        match self.auto_cleanup.execute_cleanup().await {
            Ok(basic_result) => {
                result.basic_result = Some(basic_result);
                info!("Basic cleanup completed successfully");
            }
            Err(e) => {
                error!("Basic cleanup failed: {}", e);
                result.errors.push(format!("Basic cleanup failed: {}", e));
            }
        }
        
        // Phase 3: Post-cleanup analysis and optimization
        result.analysis = Some(self.analyze_cleanup_results(&result).await?);
        
        // Update orchestrator state
        let mut state = self.state.write().await;
        state.last_comprehensive_cleanup = Some(Instant::now());
        state.total_comprehensive_cleanups += 1;
        
        result.total_duration = start_time.elapsed();
        
        Ok(result)
    }

    /// Run scheduled cleanup loop
    async fn run_scheduled_cleanup(&self) {
        info!("Starting scheduled cleanup loop");
        
        let config = self.config.read().await;
        let cleanup_interval = config.scheduled_cleanup_interval;
        drop(config);
        
        let mut interval_timer = interval(cleanup_interval);
        interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;
            
            if let Err(e) = self.execute_scheduled_cleanup().await {
                error!("Scheduled cleanup failed: {}", e);
            }
        }
    }

    /// Execute a single scheduled cleanup
    async fn execute_scheduled_cleanup(&self) -> Result<(), SnapshotError> {
        debug!("Executing scheduled cleanup");
        
        let config = self.config.read().await;
        let strategy = config.scheduled_cleanup_strategy.clone();
        drop(config);
        
        match strategy {
            CleanupStrategy::Basic => {
                self.auto_cleanup.execute_cleanup().await?;
            }
            CleanupStrategy::Intelligent => {
                if let Some(intelligent_cleanup) = &self.intelligent_cleanup {
                    intelligent_cleanup.execute_intelligent_cleanup().await?;
                } else {
                    // Fallback to basic cleanup
                    self.auto_cleanup.execute_cleanup().await?;
                }
            }
            CleanupStrategy::Comprehensive => {
                self.execute_comprehensive_cleanup().await?;
            }
            CleanupStrategy::Adaptive => {
                self.execute_adaptive_cleanup().await?;
            }
        }
        
        Ok(())
    }

    /// Run adaptive cleanup that adjusts based on system conditions
    async fn run_adaptive_cleanup(&self) {
        info!("Starting adaptive cleanup loop");
        
        let mut adaptive_interval = interval(Duration::from_secs(300)); // Check every 5 minutes
        adaptive_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            adaptive_interval.tick().await;
            
            if let Err(e) = self.check_and_execute_adaptive_cleanup().await {
                error!("Adaptive cleanup check failed: {}", e);
            }
        }
    }

    /// Check conditions and execute adaptive cleanup if needed
    async fn check_and_execute_adaptive_cleanup(&self) -> Result<(), SnapshotError> {
        let conditions = self.assess_cleanup_conditions().await?;
        
        if conditions.requires_cleanup {
            debug!("Adaptive cleanup triggered: {:?}", conditions.trigger_reason);
            
            let strategy = self.select_optimal_strategy(&conditions).await?;
            self.execute_strategy(strategy).await?;
        }
        
        Ok(())
    }

    /// Assess current system conditions to determine if cleanup is needed
    async fn assess_cleanup_conditions(&self) -> Result<CleanupConditions, SnapshotError> {
        // In a real implementation, this would check:
        // - Storage usage levels
        // - System performance metrics
        // - Time since last cleanup
        // - Predicted storage needs
        
        let config = self.config.read().await;
        let state = self.state.read().await;
        
        let time_since_last = state.last_comprehensive_cleanup
            .map(|last| last.elapsed())
            .unwrap_or(Duration::from_secs(u64::MAX));
        
        let requires_cleanup = time_since_last > config.max_time_between_cleanups;
        
        let trigger_reason = if requires_cleanup {
            CleanupTrigger::TimeThreshold
        } else {
            CleanupTrigger::None
        };
        
        Ok(CleanupConditions {
            requires_cleanup,
            trigger_reason,
            urgency_level: if requires_cleanup { UrgencyLevel::Medium } else { UrgencyLevel::Low },
            estimated_benefit: 0.7, // Simulated
            system_load: 0.5, // Simulated
        })
    }

    /// Select optimal cleanup strategy based on conditions
    async fn select_optimal_strategy(&self, conditions: &CleanupConditions) -> Result<CleanupStrategy, SnapshotError> {
        let strategy = match conditions.urgency_level {
            UrgencyLevel::Critical => CleanupStrategy::Comprehensive,
            UrgencyLevel::High => CleanupStrategy::Intelligent,
            UrgencyLevel::Medium => CleanupStrategy::Adaptive,
            UrgencyLevel::Low => CleanupStrategy::Basic,
        };
        
        debug!("Selected cleanup strategy: {:?} for urgency: {:?}", strategy, conditions.urgency_level);
        Ok(strategy)
    }

    /// Execute a specific cleanup strategy
    async fn execute_strategy(&self, strategy: CleanupStrategy) -> Result<(), SnapshotError> {
        match strategy {
            CleanupStrategy::Basic => {
                self.auto_cleanup.execute_cleanup().await?;
            }
            CleanupStrategy::Intelligent => {
                if let Some(intelligent_cleanup) = &self.intelligent_cleanup {
                    intelligent_cleanup.execute_intelligent_cleanup().await?;
                } else {
                    self.auto_cleanup.execute_cleanup().await?;
                }
            }
            CleanupStrategy::Comprehensive => {
                self.execute_comprehensive_cleanup().await?;
            }
            CleanupStrategy::Adaptive => {
                self.execute_adaptive_cleanup().await?;
            }
        }
        
        Ok(())
    }

    /// Execute adaptive cleanup based on current conditions
    async fn execute_adaptive_cleanup(&self) -> Result<AdaptiveCleanupResult, SnapshotError> {
        debug!("Executing adaptive cleanup");
        
        let conditions = self.assess_cleanup_conditions().await?;
        let start_time = Instant::now();
        
        // Choose cleanup approach based on system load
        let result = if conditions.system_load > 0.8 {
            // High system load - use gentle cleanup
            let basic_result = self.auto_cleanup.execute_cleanup().await?;
            AdaptiveCleanupResult {
                strategy_used: CleanupStrategy::Basic,
                basic_result: Some(basic_result),
                intelligent_result: None,
                adaptation_reason: "High system load - using gentle cleanup".to_string(),
                execution_time: start_time.elapsed(),
            }
        } else if conditions.urgency_level >= UrgencyLevel::High {
            // High urgency - use intelligent cleanup
            if let Some(intelligent_cleanup) = &self.intelligent_cleanup {
                let intelligent_result = intelligent_cleanup.execute_intelligent_cleanup().await?;
                AdaptiveCleanupResult {
                    strategy_used: CleanupStrategy::Intelligent,
                    basic_result: None,
                    intelligent_result: Some(intelligent_result),
                    adaptation_reason: "High urgency - using intelligent cleanup".to_string(),
                    execution_time: start_time.elapsed(),
                }
            } else {
                let basic_result = self.auto_cleanup.execute_cleanup().await?;
                AdaptiveCleanupResult {
                    strategy_used: CleanupStrategy::Basic,
                    basic_result: Some(basic_result),
                    intelligent_result: None,
                    adaptation_reason: "Intelligent cleanup not available - fallback to basic".to_string(),
                    execution_time: start_time.elapsed(),
                }
            }
        } else {
            // Normal conditions - use basic cleanup
            let basic_result = self.auto_cleanup.execute_cleanup().await?;
            AdaptiveCleanupResult {
                strategy_used: CleanupStrategy::Basic,
                basic_result: Some(basic_result),
                intelligent_result: None,
                adaptation_reason: "Normal conditions - using basic cleanup".to_string(),
                execution_time: start_time.elapsed(),
            }
        };
        
        Ok(result)
    }

    /// Monitor for emergency cleanup conditions
    async fn monitor_emergency_conditions(&self) {
        info!("Starting emergency cleanup monitoring");
        
        let mut monitor_interval = interval(Duration::from_secs(60)); // Check every minute
        monitor_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            monitor_interval.tick().await;
            
            if let Err(e) = self.check_emergency_conditions().await {
                error!("Emergency condition check failed: {}", e);
            }
        }
    }

    /// Check for emergency conditions that require immediate cleanup
    async fn check_emergency_conditions(&self) -> Result<(), SnapshotError> {
        // In a real implementation, this would check:
        // - Available disk space
        // - System performance degradation
        // - Critical storage thresholds
        
        let emergency_triggered = false; // Simulated condition
        
        if emergency_triggered {
            warn!("Emergency cleanup conditions detected - executing immediate cleanup");
            self.execute_emergency_cleanup().await?;
        }
        
        Ok(())
    }

    /// Execute emergency cleanup with maximum aggressiveness
    async fn execute_emergency_cleanup(&self) -> Result<(), SnapshotError> {
        info!("Executing emergency cleanup");
        
        // Execute the most aggressive cleanup strategy available
        if let Some(intelligent_cleanup) = &self.intelligent_cleanup {
            intelligent_cleanup.execute_intelligent_cleanup().await?;
        } else {
            self.auto_cleanup.execute_cleanup().await?;
        }
        
        // Update state to record emergency cleanup
        let mut state = self.state.write().await;
        state.emergency_cleanups_executed += 1;
        state.last_emergency_cleanup = Some(Instant::now());
        
        Ok(())
    }

    /// Analyze the results of cleanup operations
    async fn analyze_cleanup_results(
        &self,
        result: &ComprehensiveCleanupResult,
    ) -> Result<CleanupAnalysis, SnapshotError> {
        let total_space_freed = result.basic_result.as_ref().map(|r| r.space_freed).unwrap_or(0) +
                              result.intelligent_result.as_ref().map(|r| r.base_result.space_freed).unwrap_or(0);
        
        let total_snapshots_removed = result.basic_result.as_ref().map(|r| r.snapshots_removed).unwrap_or(0) +
                                    result.intelligent_result.as_ref().map(|r| r.base_result.snapshots_removed).unwrap_or(0);
        
        let effectiveness_score = if total_space_freed > 1_000_000_000 { // > 1GB
            if result.total_duration < Duration::from_secs(300) { // < 5 minutes
                0.9 // Excellent
            } else {
                0.7 // Good
            }
        } else {
            0.5 // Fair
        };
        
        Ok(CleanupAnalysis {
            total_space_freed,
            total_snapshots_removed,
            effectiveness_score,
            recommendations: self.generate_recommendations(effectiveness_score).await?,
        })
    }

    /// Generate recommendations based on cleanup performance
    async fn generate_recommendations(&self, effectiveness_score: f64) -> Result<Vec<String>, SnapshotError> {
        let mut recommendations = Vec::new();
        
        if effectiveness_score > 0.8 {
            recommendations.push("Cleanup performance is excellent - current strategy is optimal".to_string());
        } else if effectiveness_score > 0.6 {
            recommendations.push("Consider increasing cleanup frequency slightly".to_string());
            recommendations.push("Monitor storage growth patterns for optimization opportunities".to_string());
        } else {
            recommendations.push("Cleanup strategy needs optimization".to_string());
            recommendations.push("Consider enabling intelligent cleanup if not already active".to_string());
            recommendations.push("Review and adjust retention policies".to_string());
        }
        
        Ok(recommendations)
    }

    /// Get comprehensive orchestrator statistics
    pub async fn get_orchestrator_statistics(&self) -> OrchestratorStatistics {
        let state = self.state.read().await;
        
        OrchestratorStatistics {
            total_comprehensive_cleanups: state.total_comprehensive_cleanups,
            emergency_cleanups_executed: state.emergency_cleanups_executed,
            last_comprehensive_cleanup: state.last_comprehensive_cleanup,
            last_emergency_cleanup: state.last_emergency_cleanup,
            average_cleanup_effectiveness: state.calculate_average_effectiveness(),
        }
    }

    /// Update orchestrator configuration
    pub async fn update_config(&self, new_config: OrchestratorConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
        info!("Orchestrator configuration updated");
    }
}

impl Clone for SmartCleanupOrchestrator {
    fn clone(&self) -> Self {
        Self {
            auto_cleanup: self.auto_cleanup.clone(),
            intelligent_cleanup: self.intelligent_cleanup.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }
}

// Configuration and data structures

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    pub enable_scheduled_cleanup: bool,
    pub enable_adaptive_cleanup: bool,
    pub enable_intelligent_cleanup: bool,
    pub enable_emergency_cleanup: bool,
    
    pub scheduled_cleanup_interval: Duration,
    pub scheduled_cleanup_strategy: CleanupStrategy,
    pub max_time_between_cleanups: Duration,
    
    pub emergency_threshold_storage_percent: f64,
    pub emergency_threshold_performance_impact: f64,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            enable_scheduled_cleanup: true,
            enable_adaptive_cleanup: true,
            enable_intelligent_cleanup: true,
            enable_emergency_cleanup: true,
            
            scheduled_cleanup_interval: Duration::from_secs(6 * 60 * 60), // 6 hours
            scheduled_cleanup_strategy: CleanupStrategy::Adaptive,
            max_time_between_cleanups: Duration::from_secs(24 * 60 * 60), // 24 hours
            
            emergency_threshold_storage_percent: 0.95,
            emergency_threshold_performance_impact: 0.8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CleanupStrategy {
    Basic,
    Intelligent,
    Comprehensive,
    Adaptive,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum UrgencyLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
struct OrchestratorState {
    total_comprehensive_cleanups: u64,
    emergency_cleanups_executed: u64,
    last_comprehensive_cleanup: Option<Instant>,
    last_emergency_cleanup: Option<Instant>,
    effectiveness_history: Vec<f64>,
}

impl OrchestratorState {
    fn new() -> Self {
        Self {
            total_comprehensive_cleanups: 0,
            emergency_cleanups_executed: 0,
            last_comprehensive_cleanup: None,
            last_emergency_cleanup: None,
            effectiveness_history: Vec::new(),
        }
    }

    fn calculate_average_effectiveness(&self) -> f64 {
        if self.effectiveness_history.is_empty() {
            return 0.0;
        }
        
        self.effectiveness_history.iter().sum::<f64>() / self.effectiveness_history.len() as f64
    }
}

#[derive(Debug, Clone)]
pub struct ComprehensiveCleanupResult {
    pub basic_result: Option<CleanupResult>,
    pub intelligent_result: Option<IntelligentCleanupResult>,
    pub analysis: Option<CleanupAnalysis>,
    pub errors: Vec<String>,
    pub total_duration: Duration,
}

impl ComprehensiveCleanupResult {
    fn new() -> Self {
        Self {
            basic_result: None,
            intelligent_result: None,
            analysis: None,
            errors: Vec::new(),
            total_duration: Duration::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CleanupConditions {
    pub requires_cleanup: bool,
    pub trigger_reason: CleanupTrigger,
    pub urgency_level: UrgencyLevel,
    pub estimated_benefit: f64,
    pub system_load: f64,
}

#[derive(Debug, Clone)]
pub enum CleanupTrigger {
    None,
    TimeThreshold,
    StorageThreshold,
    PerformanceThreshold,
    Emergency,
}

#[derive(Debug, Clone)]
pub struct AdaptiveCleanupResult {
    pub strategy_used: CleanupStrategy,
    pub basic_result: Option<CleanupResult>,
    pub intelligent_result: Option<IntelligentCleanupResult>,
    pub adaptation_reason: String,
    pub execution_time: Duration,
}

#[derive(Debug, Clone)]
pub struct CleanupAnalysis {
    pub total_space_freed: u64,
    pub total_snapshots_removed: u32,
    pub effectiveness_score: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OrchestratorStatistics {
    pub total_comprehensive_cleanups: u64,
    pub emergency_cleanups_executed: u64,
    pub last_comprehensive_cleanup: Option<Instant>,
    pub last_emergency_cleanup: Option<Instant>,
    pub average_cleanup_effectiveness: f64,
}
