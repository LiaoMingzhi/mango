// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Adaptive scheduler that learns from usage patterns and system behavior

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use crate::scheduler::SmartScheduler;
use crate::types::error::SnapshotError;

/// Adaptive scheduler that learns from patterns and optimizes scheduling decisions
pub struct AdaptiveScheduler {
    smart_scheduler: Arc<SmartScheduler>,
    learning_engine: Arc<RwLock<LearningEngine>>,
    config: Arc<RwLock<AdaptiveConfig>>,
}

impl AdaptiveScheduler {
    /// Create a new adaptive scheduler
    pub fn new(
        smart_scheduler: Arc<SmartScheduler>,
        config: AdaptiveConfig,
    ) -> Self {
        Self {
            smart_scheduler,
            learning_engine: Arc::new(RwLock::new(LearningEngine::new())),
            config: Arc::new(RwLock::new(config)),
        }
    }

    /// Start the adaptive scheduler
    #[instrument(level = "info", skip(self))]
    pub async fn start(&self) -> Result<(), SnapshotError> {
        info!("Starting adaptive scheduler");
        
        // Start the underlying smart scheduler
        self.smart_scheduler.start().await?;
        
        // Start the learning and adaptation loop
        let adaptive_scheduler = self.clone();
        tokio::spawn(async move {
            adaptive_scheduler.run_adaptation_loop().await;
        });

        Ok(())
    }

    /// Stop the adaptive scheduler
    pub async fn stop(&self) -> Result<(), SnapshotError> {
        info!("Stopping adaptive scheduler");
        self.smart_scheduler.stop().await
    }

    /// Main adaptation loop that continuously learns and optimizes
    async fn run_adaptation_loop(&self) {
        info!("Starting adaptive scheduler learning loop");
        
        let mut adaptation_interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes
        adaptation_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            adaptation_interval.tick().await;

            if let Err(e) = self.perform_adaptation_cycle().await {
                warn!("Adaptation cycle failed: {}", e);
            }
        }
    }

    /// Perform one adaptation cycle
    async fn perform_adaptation_cycle(&self) -> Result<(), SnapshotError> {
        // Collect current performance data
        let stats = self.smart_scheduler.get_statistics().await;
        
        // Update learning engine with new data
        let mut learning_engine = self.learning_engine.write().await;
        learning_engine.record_performance_data(PerformanceData {
            timestamp: Instant::now(),
            system_load: stats.current_system_load,
            operation_duration: stats.recent_avg_operation_duration,
            success_rate: stats.recent_success_rate,
            current_interval: stats.current_snapshot_interval,
        });

        // Analyze patterns and generate recommendations
        let recommendations = learning_engine.analyze_and_recommend().await;
        drop(learning_engine);

        // Apply recommendations if they meet confidence thresholds
        self.apply_recommendations(recommendations).await?;

        Ok(())
    }

    /// Apply recommendations from the learning engine
    async fn apply_recommendations(
        &self,
        recommendations: Vec<SchedulingRecommendation>,
    ) -> Result<(), SnapshotError> {
        let config = self.config.read().await;
        
        for recommendation in recommendations {
            if recommendation.confidence >= config.min_confidence_threshold {
                match recommendation.recommendation_type {
                    RecommendationType::AdjustSnapshotInterval { new_interval } => {
                        info!("Applying adaptive recommendation: adjust snapshot interval to {:?} (confidence: {:.2})",
                              new_interval, recommendation.confidence);
                        
                        // Update smart scheduler configuration
                        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
                        smart_config.base_snapshot_interval = new_interval;
                        self.smart_scheduler.update_config(smart_config).await;
                    }
                    RecommendationType::AdjustLoadThresholds { medium, high, very_high } => {
                        info!("Applying adaptive recommendation: adjust load thresholds to {:.2}, {:.2}, {:.2}",
                              medium, high, very_high);
                        
                        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
                        smart_config.medium_load_threshold = medium;
                        smart_config.high_load_threshold = high;
                        smart_config.very_high_load_threshold = very_high;
                        self.smart_scheduler.update_config(smart_config).await;
                    }
                    RecommendationType::OptimizeForPattern { pattern } => {
                        info!("Applying adaptive recommendation: optimize for pattern {:?}", pattern);
                        self.optimize_for_pattern(pattern).await?;
                    }
                }
            } else {
                debug!("Skipping recommendation due to low confidence: {:.2} < {:.2}",
                       recommendation.confidence, config.min_confidence_threshold);
            }
        }

        Ok(())
    }

    /// Optimize scheduler for detected usage patterns
    async fn optimize_for_pattern(&self, pattern: UsagePattern) -> Result<(), SnapshotError> {
        let mut smart_config = crate::scheduler::SmartSchedulerConfig::default();
        
        match pattern {
            UsagePattern::HighFrequencyLowLoad => {
                // Optimize for frequent operations under low load
                smart_config.base_snapshot_interval = Duration::from_secs(30 * 60); // 30 minutes
                smart_config.medium_load_threshold = 0.3;
                smart_config.high_load_threshold = 0.5;
            }
            UsagePattern::LowFrequencyHighLoad => {
                // Optimize for infrequent operations under high load
                smart_config.base_snapshot_interval = Duration::from_secs(2 * 60 * 60); // 2 hours
                smart_config.medium_load_threshold = 0.7;
                smart_config.high_load_threshold = 0.8;
            }
            UsagePattern::BurstyTraffic => {
                // Optimize for bursty traffic patterns
                smart_config.base_snapshot_interval = Duration::from_secs(45 * 60); // 45 minutes
                smart_config.enable_auto_adjustment = true;
                smart_config.max_interval_multiplier = 3.0;
            }
            UsagePattern::SteadyState => {
                // Optimize for steady-state operations
                smart_config.base_snapshot_interval = Duration::from_secs(60 * 60); // 1 hour
                smart_config.enable_auto_adjustment = false;
            }
        }

        self.smart_scheduler.update_config(smart_config).await;
        Ok(())
    }

    /// Get adaptive scheduler statistics
    pub async fn get_adaptive_statistics(&self) -> AdaptiveStatistics {
        let smart_stats = self.smart_scheduler.get_statistics().await;
        let learning_engine = self.learning_engine.read().await;
        
        AdaptiveStatistics {
            smart_scheduler_stats: smart_stats,
            learning_stats: learning_engine.get_statistics(),
            total_adaptations: learning_engine.total_adaptations,
            current_pattern: learning_engine.detected_pattern.clone(),
        }
    }
}

impl Clone for AdaptiveScheduler {
    fn clone(&self) -> Self {
        Self {
            smart_scheduler: self.smart_scheduler.clone(),
            learning_engine: self.learning_engine.clone(),
            config: self.config.clone(),
        }
    }
}

/// Learning engine that analyzes patterns and generates recommendations
struct LearningEngine {
    performance_history: VecDeque<PerformanceData>,
    pattern_analyzer: PatternAnalyzer,
    recommendation_history: VecDeque<AppliedRecommendation>,
    total_adaptations: u64,
    detected_pattern: Option<UsagePattern>,
}

impl LearningEngine {
    fn new() -> Self {
        Self {
            performance_history: VecDeque::new(),
            pattern_analyzer: PatternAnalyzer::new(),
            recommendation_history: VecDeque::new(),
            total_adaptations: 0,
            detected_pattern: None,
        }
    }

    /// Record new performance data
    fn record_performance_data(&mut self, data: PerformanceData) {
        self.performance_history.push_back(data);
        
        // Keep only last 288 data points (24 hours worth at 5-minute intervals)
        while self.performance_history.len() > 288 {
            self.performance_history.pop_front();
        }
    }

    /// Analyze patterns and generate recommendations
    async fn analyze_and_recommend(&mut self) -> Vec<SchedulingRecommendation> {
        if self.performance_history.len() < 12 { // Need at least 1 hour of data
            return vec![];
        }

        // Update pattern analysis
        self.detected_pattern = self.pattern_analyzer.analyze_patterns(&self.performance_history);
        
        let mut recommendations = vec![];

        // Generate interval adjustment recommendations
        if let Some(interval_rec) = self.recommend_interval_adjustment() {
            recommendations.push(interval_rec);
        }

        // Generate load threshold recommendations
        if let Some(threshold_rec) = self.recommend_load_threshold_adjustment() {
            recommendations.push(threshold_rec);
        }

        // Generate pattern-based recommendations
        if let Some(pattern) = &self.detected_pattern {
            if let Some(pattern_rec) = self.recommend_pattern_optimization(pattern.clone()) {
                recommendations.push(pattern_rec);
            }
        }

        recommendations
    }

    /// Recommend interval adjustments based on performance data
    fn recommend_interval_adjustment(&self) -> Option<SchedulingRecommendation> {
        let recent_data: Vec<_> = self.performance_history.iter().rev().take(24).collect(); // Last 2 hours
        if recent_data.len() < 12 {
            return None;
        }

        let avg_success_rate = recent_data.iter()
            .filter_map(|d| d.success_rate)
            .sum::<f64>() / recent_data.len() as f64;

        let avg_duration = recent_data.iter()
            .filter_map(|d| d.operation_duration)
            .sum::<Duration>()
            .div_f64(recent_data.len() as f64);

        let current_interval = recent_data[0].current_interval;

        let confidence = if recent_data.len() >= 24 { 0.8 } else { 0.6 };

        // If success rate is high and operations are fast, suggest shorter interval
        if avg_success_rate > 0.9 && avg_duration < Duration::from_secs(120) {
            let new_interval = Duration::from_millis((current_interval.as_millis() as f64 * 0.8) as u64);
            if new_interval >= Duration::from_secs(15 * 60) { // Min 15 minutes
                return Some(SchedulingRecommendation {
                    recommendation_type: RecommendationType::AdjustSnapshotInterval { new_interval },
                    confidence,
                    reasoning: "High success rate and fast operations suggest shorter interval".to_string(),
                });
            }
        }

        // If success rate is low or operations are slow, suggest longer interval
        if avg_success_rate < 0.7 || avg_duration > Duration::from_secs(300) {
            let new_interval = Duration::from_millis((current_interval.as_millis() as f64 * 1.3) as u64);
            if new_interval <= Duration::from_secs(4 * 60 * 60) { // Max 4 hours
                return Some(SchedulingRecommendation {
                    recommendation_type: RecommendationType::AdjustSnapshotInterval { new_interval },
                    confidence,
                    reasoning: "Low success rate or slow operations suggest longer interval".to_string(),
                });
            }
        }

        None
    }

    /// Recommend load threshold adjustments
    fn recommend_load_threshold_adjustment(&self) -> Option<SchedulingRecommendation> {
        let recent_data: Vec<_> = self.performance_history.iter().rev().take(48).collect(); // Last 4 hours
        if recent_data.len() < 24 {
            return None;
        }

        let avg_load = recent_data.iter()
            .map(|d| d.system_load)
            .sum::<f64>() / recent_data.len() as f64;

        let load_variance = recent_data.iter()
            .map(|d| (d.system_load - avg_load).powi(2))
            .sum::<f64>() / recent_data.len() as f64;

        // If load is consistently low, we can be more aggressive
        if avg_load < 0.4 && load_variance < 0.1 {
            return Some(SchedulingRecommendation {
                recommendation_type: RecommendationType::AdjustLoadThresholds {
                    medium: 0.6,
                    high: 0.8,
                    very_high: 0.95,
                },
                confidence: 0.7,
                reasoning: "Consistently low load allows for more aggressive thresholds".to_string(),
            });
        }

        // If load is consistently high, be more conservative
        if avg_load > 0.7 && load_variance < 0.1 {
            return Some(SchedulingRecommendation {
                recommendation_type: RecommendationType::AdjustLoadThresholds {
                    medium: 0.3,
                    high: 0.5,
                    very_high: 0.7,
                },
                confidence: 0.7,
                reasoning: "Consistently high load requires more conservative thresholds".to_string(),
            });
        }

        None
    }

    /// Recommend pattern-based optimizations
    fn recommend_pattern_optimization(&self, pattern: UsagePattern) -> Option<SchedulingRecommendation> {
        // Only recommend if we haven't already optimized for this pattern recently
        let recent_optimizations = self.recommendation_history.iter()
            .rev()
            .take(10)
            .filter(|r| matches!(r.recommendation_type, RecommendationType::OptimizeForPattern { .. }))
            .count();

        if recent_optimizations > 0 {
            return None;
        }

        Some(SchedulingRecommendation {
            recommendation_type: RecommendationType::OptimizeForPattern { pattern: pattern.clone() },
            confidence: 0.6,
            reasoning: format!("Detected {:?} usage pattern", pattern),
        })
    }

    /// Get learning statistics
    fn get_statistics(&self) -> LearningStatistics {
        LearningStatistics {
            data_points_collected: self.performance_history.len(),
            recommendations_generated: self.recommendation_history.len(),
            current_detected_pattern: self.detected_pattern.clone(),
        }
    }
}

/// Pattern analyzer for detecting usage patterns
struct PatternAnalyzer {
    pattern_history: VecDeque<UsagePattern>,
}

impl PatternAnalyzer {
    fn new() -> Self {
        Self {
            pattern_history: VecDeque::new(),
        }
    }

    /// Analyze performance data to detect usage patterns
    fn analyze_patterns(&mut self, data: &VecDeque<PerformanceData>) -> Option<UsagePattern> {
        if data.len() < 24 { // Need at least 2 hours of data
            return None;
        }

        let recent_data: Vec<_> = data.iter().rev().take(48).collect(); // Last 4 hours

        // Calculate metrics
        let avg_load = recent_data.iter().map(|d| d.system_load).sum::<f64>() / recent_data.len() as f64;
        let load_variance = recent_data.iter()
            .map(|d| (d.system_load - avg_load).powi(2))
            .sum::<f64>() / recent_data.len() as f64;

        let avg_interval = recent_data.iter()
            .map(|d| d.current_interval)
            .sum::<Duration>()
            .div_f64(recent_data.len() as f64);

        // Determine pattern based on load and variance characteristics
        let pattern = if avg_load < 0.4 && avg_interval < Duration::from_secs(45 * 60) {
            UsagePattern::HighFrequencyLowLoad
        } else if avg_load > 0.7 && avg_interval > Duration::from_secs(90 * 60) {
            UsagePattern::LowFrequencyHighLoad
        } else if load_variance > 0.2 {
            UsagePattern::BurstyTraffic
        } else {
            UsagePattern::SteadyState
        };

        // Record pattern for stability analysis
        self.pattern_history.push_back(pattern.clone());
        while self.pattern_history.len() > 10 {
            self.pattern_history.pop_front();
        }

        // Return pattern only if it's been stable for a while
        if self.pattern_history.len() >= 3 {
            let recent_patterns: Vec<_> = self.pattern_history.iter().rev().take(3).collect();
            if recent_patterns.iter().all(|&p| p == &pattern) {
                return Some(pattern);
            }
        }

        None
    }
}

/// Configuration for adaptive scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Minimum confidence threshold for applying recommendations
    pub min_confidence_threshold: f64,
    /// Enable learning and adaptation
    pub enable_learning: bool,
    /// Maximum number of adaptations per hour
    pub max_adaptations_per_hour: u32,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            min_confidence_threshold: 0.7,
            enable_learning: true,
            max_adaptations_per_hour: 3,
        }
    }
}

/// Performance data point for learning
#[derive(Debug, Clone)]
struct PerformanceData {
    timestamp: Instant,
    system_load: f64,
    operation_duration: Option<Duration>,
    success_rate: Option<f64>,
    current_interval: Duration,
}

/// Detected usage patterns
#[derive(Debug, Clone, PartialEq)]
pub enum UsagePattern {
    /// High frequency operations under low system load
    HighFrequencyLowLoad,
    /// Low frequency operations under high system load
    LowFrequencyHighLoad,
    /// Bursty traffic with varying load
    BurstyTraffic,
    /// Steady state operations
    SteadyState,
}

/// Scheduling recommendation from learning engine
#[derive(Debug, Clone)]
struct SchedulingRecommendation {
    recommendation_type: RecommendationType,
    confidence: f64,
    reasoning: String,
}

/// Type of scheduling recommendation
#[derive(Debug, Clone)]
enum RecommendationType {
    AdjustSnapshotInterval { new_interval: Duration },
    AdjustLoadThresholds { medium: f64, high: f64, very_high: f64 },
    OptimizeForPattern { pattern: UsagePattern },
}

/// Applied recommendation for tracking
#[derive(Debug, Clone)]
struct AppliedRecommendation {
    timestamp: Instant,
    recommendation_type: RecommendationType,
    confidence: f64,
}

/// Statistics about the learning engine
#[derive(Debug, Clone)]
pub struct LearningStatistics {
    pub data_points_collected: usize,
    pub recommendations_generated: usize,
    pub current_detected_pattern: Option<UsagePattern>,
}

/// Statistics about the adaptive scheduler
#[derive(Debug, Clone)]
pub struct AdaptiveStatistics {
    pub smart_scheduler_stats: crate::scheduler::SmartSchedulerStatistics,
    pub learning_stats: LearningStatistics,
    pub total_adaptations: u64,
    pub current_pattern: Option<UsagePattern>,
}
