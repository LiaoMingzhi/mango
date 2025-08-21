// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Intelligent cleanup strategies with advanced algorithms

use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

use crate::strategy::{AutoCleanupManager, CleanupResult};
use crate::types::{SnapshotId, SnapshotType};
use crate::types::error::SnapshotError;

/// Intelligent cleanup manager with advanced algorithms
pub struct IntelligentCleanupManager {
    base_cleanup: Arc<AutoCleanupManager>,
    config: Arc<RwLock<IntelligentCleanupConfig>>,
    analytics: Arc<RwLock<CleanupAnalytics>>,
    prediction_engine: Arc<RwLock<PredictionEngine>>,
    optimization_engine: Arc<RwLock<OptimizationEngine>>,
}

impl IntelligentCleanupManager {
    /// Create a new intelligent cleanup manager
    pub fn new(
        base_cleanup: Arc<AutoCleanupManager>,
        config: IntelligentCleanupConfig,
    ) -> Self {
        Self {
            base_cleanup,
            config: Arc::new(RwLock::new(config)),
            analytics: Arc::new(RwLock::new(CleanupAnalytics::new())),
            prediction_engine: Arc::new(RwLock::new(PredictionEngine::new())),
            optimization_engine: Arc::new(RwLock::new(OptimizationEngine::new())),
        }
    }

    /// Execute intelligent cleanup with predictive analysis
    #[instrument(level = "info", skip(self))]
    pub async fn execute_intelligent_cleanup(&self) -> Result<IntelligentCleanupResult, SnapshotError> {
        info!("Starting intelligent cleanup analysis");
        
        let start_time = Instant::now();
        
        // Step 1: Collect current system metrics
        let system_metrics = self.collect_system_metrics().await?;
        
        // Step 2: Predict future storage needs
        let prediction = self.predict_storage_needs(&system_metrics).await?;
        
        // Step 3: Optimize cleanup strategy
        let optimized_strategy = self.optimize_cleanup_strategy(&system_metrics, &prediction).await?;
        
        // Step 4: Execute cleanup with optimized strategy
        let cleanup_result = self.execute_optimized_cleanup(&optimized_strategy).await?;
        
        // Step 5: Analyze cleanup effectiveness
        let effectiveness = self.analyze_cleanup_effectiveness(&cleanup_result, &prediction).await?;
        
        // Step 6: Update learning models
        self.update_learning_models(&system_metrics, &cleanup_result, &effectiveness).await?;
        
        let total_duration = start_time.elapsed();
        
        Ok(IntelligentCleanupResult {
            base_result: cleanup_result,
            system_metrics,
            prediction,
            optimized_strategy,
            effectiveness,
            analysis_duration: total_duration,
        })
    }

    /// Collect comprehensive system metrics
    async fn collect_system_metrics(&self) -> Result<SystemMetrics, SnapshotError> {
        debug!("Collecting system metrics for intelligent cleanup");
        
        // In a real implementation, these would collect actual system data
        let metrics = SystemMetrics {
            timestamp: SystemTime::now(),
            total_storage_used: 50_000_000_000, // 50GB
            available_storage: 100_000_000_000, // 100GB
            storage_growth_rate: 1_000_000_000, // 1GB per day
            snapshot_creation_rate: 24.0, // 24 snapshots per day
            read_access_patterns: self.analyze_access_patterns().await?,
            system_load: 0.6,
            network_bandwidth_usage: 0.4,
            estimated_future_load: 0.7,
        };
        
        Ok(metrics)
    }

    /// Analyze historical access patterns
    async fn analyze_access_patterns(&self) -> Result<HashMap<SnapshotType, AccessPattern>, SnapshotError> {
        let mut patterns = HashMap::new();
        
        use crate::types::snapshot::ComponentType;
        
        // Analyze different snapshot types
        patterns.insert(SnapshotType::Full { 
            include_history: true,
            compression_level: crate::types::snapshot::CompressionLevel::Medium,
        }, AccessPattern {
            avg_accesses_per_day: 5.0,
            last_access_age: Duration::from_secs(6 * 60 * 60), // 6 hours
            access_frequency_trend: -0.1, // Decreasing
            importance_score: 0.9,
        });
        
        patterns.insert(SnapshotType::Incremental { 
            base_snapshot: SnapshotId::new(),
            changed_components: vec![ComponentType::ObjectStore],
        }, AccessPattern {
            avg_accesses_per_day: 2.0,
            last_access_age: Duration::from_secs(12 * 60 * 60), // 12 hours
            access_frequency_trend: -0.3, // Decreasing faster
            importance_score: 0.6,
        });
        
        patterns.insert(SnapshotType::Checkpoint { 
            checkpoint_seq: 1000,
            include_transactions: true,
        }, AccessPattern {
            avg_accesses_per_day: 10.0,
            last_access_age: Duration::from_secs(2 * 60 * 60), // 2 hours
            access_frequency_trend: 0.1, // Slightly increasing
            importance_score: 0.8,
        });
        
        patterns.insert(SnapshotType::Epoch { 
            epoch: 0,
            include_committee_info: true,
        }, AccessPattern {
            avg_accesses_per_day: 1.0,
            last_access_age: Duration::from_secs(24 * 60 * 60), // 24 hours
            access_frequency_trend: -0.05, // Slowly decreasing
            importance_score: 0.95, // Very important
        });
        
        Ok(patterns)
    }

    /// Predict future storage needs using machine learning
    async fn predict_storage_needs(&self, metrics: &SystemMetrics) -> Result<StoragePrediction, SnapshotError> {
        debug!("Predicting future storage needs");
        
        let mut prediction_engine = self.prediction_engine.write().await;
        
        // Update the prediction model with current metrics
        prediction_engine.update_model(metrics);
        
        // Generate predictions for different time horizons
        let prediction = StoragePrediction {
            next_7_days: prediction_engine.predict_storage_usage(7),
            next_30_days: prediction_engine.predict_storage_usage(30),
            next_90_days: prediction_engine.predict_storage_usage(90),
            confidence_level: prediction_engine.get_confidence_level(),
            recommended_free_space: prediction_engine.calculate_recommended_free_space(metrics),
            critical_cleanup_threshold: prediction_engine.calculate_critical_threshold(metrics),
        };
        
        Ok(prediction)
    }

    /// Optimize cleanup strategy based on predictions and system state
    async fn optimize_cleanup_strategy(
        &self,
        metrics: &SystemMetrics,
        prediction: &StoragePrediction,
    ) -> Result<OptimizedCleanupStrategy, SnapshotError> {
        debug!("Optimizing cleanup strategy");
        
        let mut optimization_engine = self.optimization_engine.write().await;
        
        // Calculate urgency level
        let urgency = self.calculate_cleanup_urgency(metrics, prediction).await?;
        
        // Optimize strategy based on urgency and system constraints
        let strategy = optimization_engine.optimize_strategy(metrics, prediction, urgency);
        
        Ok(strategy)
    }

    /// Calculate how urgent cleanup is based on current state and predictions
    async fn calculate_cleanup_urgency(
        &self,
        metrics: &SystemMetrics,
        prediction: &StoragePrediction,
    ) -> Result<CleanupUrgency, SnapshotError> {
        let storage_ratio = metrics.total_storage_used as f64 / 
                          (metrics.total_storage_used + metrics.available_storage) as f64;
        
        let predicted_7day_ratio = prediction.next_7_days as f64 / 
                                 (metrics.total_storage_used + metrics.available_storage) as f64;
        
        let urgency_level = if storage_ratio > 0.95 || predicted_7day_ratio > 0.98 {
            IntelligentUrgencyLevel::Critical
        } else if storage_ratio > 0.85 || predicted_7day_ratio > 0.90 {
            IntelligentUrgencyLevel::High
        } else if storage_ratio > 0.70 || predicted_7day_ratio > 0.80 {
            IntelligentUrgencyLevel::Medium
        } else {
            IntelligentUrgencyLevel::Low
        };
        
        Ok(CleanupUrgency {
            level: urgency_level,
            storage_pressure: storage_ratio,
            predicted_pressure: predicted_7day_ratio,
            days_until_critical: self.calculate_days_until_critical(metrics, prediction).await?,
        })
    }

    /// Calculate days until storage becomes critical
    async fn calculate_days_until_critical(
        &self,
        metrics: &SystemMetrics,
        _prediction: &StoragePrediction,
    ) -> Result<f64, SnapshotError> {
        let daily_growth = metrics.storage_growth_rate;
        let available_space = metrics.available_storage;
        let critical_threshold = (available_space as f64 * 0.05) as u64; // 5% free space
        
        if daily_growth > 0 {
            Ok((available_space - critical_threshold) as f64 / daily_growth as f64)
        } else {
            Ok(f64::INFINITY) // No growth, never critical
        }
    }

    /// Execute cleanup with optimized strategy
    async fn execute_optimized_cleanup(
        &self,
        strategy: &OptimizedCleanupStrategy,
    ) -> Result<CleanupResult, SnapshotError> {
        debug!("Executing optimized cleanup strategy");
        
        // Apply dynamic configuration based on optimization
        self.apply_dynamic_config(&strategy.dynamic_config).await?;
        
        // Execute base cleanup with optimized settings
        let result = self.base_cleanup.execute_cleanup().await?;
        
        Ok(result)
    }

    /// Apply dynamic configuration adjustments
    async fn apply_dynamic_config(&self, dynamic_config: &DynamicCleanupConfig) -> Result<(), SnapshotError> {
        // In a real implementation, this would update the base cleanup manager's configuration
        debug!("Applying dynamic cleanup configuration: {:?}", dynamic_config);
        Ok(())
    }

    /// Analyze the effectiveness of the cleanup operation
    async fn analyze_cleanup_effectiveness(
        &self,
        cleanup_result: &CleanupResult,
        prediction: &StoragePrediction,
    ) -> Result<CleanupEffectiveness, SnapshotError> {
        debug!("Analyzing cleanup effectiveness");
        
        let space_freed_ratio = cleanup_result.space_freed as f64 / prediction.recommended_free_space as f64;
        let efficiency_score = self.calculate_efficiency_score(cleanup_result).await?;
        
        let effectiveness_rating = if space_freed_ratio >= 1.0 && efficiency_score >= 0.8 {
            EffectivenessRating::Excellent
        } else if space_freed_ratio >= 0.7 && efficiency_score >= 0.6 {
            EffectivenessRating::Good
        } else if space_freed_ratio >= 0.4 && efficiency_score >= 0.4 {
            EffectivenessRating::Fair
        } else {
            EffectivenessRating::Poor
        };
        
        Ok(CleanupEffectiveness {
            rating: effectiveness_rating.clone(),
            space_freed_ratio,
            efficiency_score,
            impact_on_system_performance: self.assess_performance_impact().await?,
            recommendations: self.generate_improvement_recommendations(&effectiveness_rating).await?,
        })
    }

    /// Calculate efficiency score based on time and resources used
    async fn calculate_efficiency_score(&self, cleanup_result: &CleanupResult) -> Result<f64, SnapshotError> {
        let time_factor = if cleanup_result.cleanup_duration <= Duration::from_secs(60) {
            1.0
        } else if cleanup_result.cleanup_duration <= Duration::from_secs(300) {
            0.8
        } else if cleanup_result.cleanup_duration <= Duration::from_secs(600) {
            0.6
        } else {
            0.4
        };
        
        let throughput_factor = cleanup_result.space_freed as f64 / cleanup_result.cleanup_duration.as_secs() as f64;
        let normalized_throughput = (throughput_factor / 1_000_000.0).min(1.0); // Normalize to MB/s
        
        Ok((time_factor + normalized_throughput) / 2.0)
    }

    /// Assess the impact of cleanup on system performance
    async fn assess_performance_impact(&self) -> Result<PerformanceImpact, SnapshotError> {
        // In a real implementation, this would measure actual system performance
        Ok(PerformanceImpact {
            cpu_usage_during_cleanup: 0.15,
            memory_usage_during_cleanup: 0.10,
            disk_io_impact: 0.25,
            network_impact: 0.05,
            overall_impact_score: 0.14, // Low impact
        })
    }

    /// Generate recommendations for improving cleanup effectiveness
    async fn generate_improvement_recommendations(
        &self,
        rating: &EffectivenessRating,
    ) -> Result<Vec<String>, SnapshotError> {
        let recommendations = match rating {
            EffectivenessRating::Excellent => vec![
                "Continue current cleanup strategy".to_string(),
                "Consider reducing cleanup frequency if consistently excellent".to_string(),
            ],
            EffectivenessRating::Good => vec![
                "Fine-tune age thresholds for incremental snapshots".to_string(),
                "Consider implementing more aggressive redundancy cleanup".to_string(),
            ],
            EffectivenessRating::Fair => vec![
                "Increase cleanup frequency".to_string(),
                "Review and adjust size-based cleanup thresholds".to_string(),
                "Implement more sophisticated protection rules".to_string(),
            ],
            EffectivenessRating::Poor => vec![
                "Significantly increase cleanup aggressiveness".to_string(),
                "Review snapshot retention policies".to_string(),
                "Consider implementing emergency cleanup procedures".to_string(),
                "Analyze snapshot access patterns for better optimization".to_string(),
            ],
        };
        
        Ok(recommendations)
    }

    /// Update machine learning models with cleanup results
    async fn update_learning_models(
        &self,
        metrics: &SystemMetrics,
        cleanup_result: &CleanupResult,
        effectiveness: &CleanupEffectiveness,
    ) -> Result<(), SnapshotError> {
        debug!("Updating learning models with cleanup results");
        
        let mut prediction_engine = self.prediction_engine.write().await;
        let mut optimization_engine = self.optimization_engine.write().await;
        
        // Update prediction accuracy
        prediction_engine.update_accuracy_metrics(metrics, cleanup_result);
        
        // Update optimization effectiveness
        optimization_engine.update_effectiveness_metrics(cleanup_result, effectiveness);
        
        // Store analytics for future analysis
        let mut analytics = self.analytics.write().await;
        analytics.record_cleanup_event(CleanupEvent {
            timestamp: SystemTime::now(),
            metrics: metrics.clone(),
            result: cleanup_result.clone(),
            effectiveness: effectiveness.clone(),
        });
        
        Ok(())
    }

    /// Get comprehensive cleanup analytics
    pub async fn get_cleanup_analytics(&self) -> CleanupAnalyticsReport {
        let analytics = self.analytics.read().await;
        let prediction_engine = self.prediction_engine.read().await;
        let optimization_engine = self.optimization_engine.read().await;
        
        CleanupAnalyticsReport {
            total_intelligent_cleanups: analytics.cleanup_history.len(),
            average_effectiveness_score: analytics.calculate_average_effectiveness(),
            prediction_accuracy: prediction_engine.get_accuracy_metrics(),
            optimization_improvements: optimization_engine.get_improvement_metrics(),
            trend_analysis: analytics.analyze_trends(),
            performance_insights: analytics.generate_performance_insights(),
        }
    }

    /// Recommend optimal cleanup schedule based on analysis
    pub async fn recommend_cleanup_schedule(&self) -> Result<CleanupScheduleRecommendation, SnapshotError> {
        let analytics = self.analytics.read().await;
        let config = self.config.read().await;
        
        let recommendation = analytics.analyze_optimal_schedule(&config);
        
        Ok(recommendation)
    }
}

/// Prediction engine for storage usage forecasting
struct PredictionEngine {
    historical_data: VecDeque<StorageDataPoint>,
    model_accuracy: f64,
    confidence_factor: f64,
}

impl PredictionEngine {
    fn new() -> Self {
        Self {
            historical_data: VecDeque::new(),
            model_accuracy: 0.7, // Start with moderate accuracy
            confidence_factor: 0.8,
        }
    }

    fn update_model(&mut self, metrics: &SystemMetrics) {
        let data_point = StorageDataPoint {
            timestamp: metrics.timestamp,
            storage_used: metrics.total_storage_used,
            growth_rate: metrics.storage_growth_rate,
            creation_rate: metrics.snapshot_creation_rate,
        };
        
        self.historical_data.push_back(data_point);
        
        // Keep only last 100 data points
        while self.historical_data.len() > 100 {
            self.historical_data.pop_front();
        }
    }

    fn predict_storage_usage(&self, days: u32) -> u64 {
        if self.historical_data.is_empty() {
            return 0;
        }
        
        // Simple linear prediction (in real implementation, use more sophisticated models)
        let latest = self.historical_data.back().unwrap();
        let daily_growth = latest.growth_rate;
        
        latest.storage_used + (daily_growth * days as u64)
    }

    fn get_confidence_level(&self) -> f64 {
        self.model_accuracy * self.confidence_factor
    }

    fn calculate_recommended_free_space(&self, metrics: &SystemMetrics) -> u64 {
        // Recommend keeping 7 days worth of growth + buffer
        let buffer_days = 7;
        let safety_factor = 1.5;
        
        metrics.storage_growth_rate * buffer_days * safety_factor as u64
    }

    fn calculate_critical_threshold(&self, metrics: &SystemMetrics) -> u64 {
        // Critical when less than 3 days of growth space remaining
        let critical_days = 3;
        metrics.storage_growth_rate * critical_days
    }

    fn update_accuracy_metrics(&mut self, _metrics: &SystemMetrics, _result: &CleanupResult) {
        // Update model accuracy based on prediction vs reality
        // This would involve comparing predictions with actual outcomes
        self.model_accuracy = (self.model_accuracy * 0.9) + (0.1 * 0.8); // Simulated improvement
    }

    fn get_accuracy_metrics(&self) -> f64 {
        self.model_accuracy
    }
}

/// Optimization engine for cleanup strategy optimization
struct OptimizationEngine {
    strategy_history: Vec<StrategyPerformance>,
    effectiveness_trends: HashMap<String, f64>,
}

impl OptimizationEngine {
    fn new() -> Self {
        Self {
            strategy_history: Vec::new(),
            effectiveness_trends: HashMap::new(),
        }
    }

    fn optimize_strategy(
        &mut self,
        metrics: &SystemMetrics,
        prediction: &StoragePrediction,
        urgency: CleanupUrgency,
    ) -> OptimizedCleanupStrategy {
        let aggressiveness = match urgency.level {
            IntelligentUrgencyLevel::Critical => 1.0,
            IntelligentUrgencyLevel::High => 0.8,
            IntelligentUrgencyLevel::Medium => 0.6,
            IntelligentUrgencyLevel::Low => 0.4,
        };

        let dynamic_config = DynamicCleanupConfig {
            age_multiplier: if urgency.level == IntelligentUrgencyLevel::Critical { 0.5 } else { 1.0 },
            count_multiplier: aggressiveness,
            size_threshold_multiplier: 1.0 / aggressiveness,
            protection_relaxation: aggressiveness > 0.8,
        };

        OptimizedCleanupStrategy {
            strategy_id: format!("strategy_{}", self.strategy_history.len()),
            urgency_level: urgency.level,
            aggressiveness_factor: aggressiveness,
            dynamic_config,
            expected_space_freed: self.estimate_space_freed(metrics, prediction, aggressiveness),
            confidence_score: self.calculate_strategy_confidence(metrics, &urgency),
        }
    }

    fn estimate_space_freed(&self, metrics: &SystemMetrics, _prediction: &StoragePrediction, aggressiveness: f64) -> u64 {
        // Estimate based on historical performance and aggressiveness
        let base_estimate = metrics.total_storage_used as f64 * 0.1; // 10% of total storage
        (base_estimate * aggressiveness) as u64
    }

    fn calculate_strategy_confidence(&self, _metrics: &SystemMetrics, urgency: &CleanupUrgency) -> f64 {
        // Higher confidence for more urgent situations (more aggressive strategies)
        match urgency.level {
            IntelligentUrgencyLevel::Critical => 0.9,
            IntelligentUrgencyLevel::High => 0.8,
            IntelligentUrgencyLevel::Medium => 0.7,
            IntelligentUrgencyLevel::Low => 0.6,
        }
    }

    fn update_effectiveness_metrics(&mut self, result: &CleanupResult, effectiveness: &CleanupEffectiveness) {
        let performance = StrategyPerformance {
            strategy_id: format!("strategy_{}", self.strategy_history.len()),
            space_freed: result.space_freed,
            execution_time: result.cleanup_duration,
            effectiveness_score: effectiveness.efficiency_score,
        };
        
        self.strategy_history.push(performance);
        
        // Keep only last 50 strategies
        while self.strategy_history.len() > 50 {
            self.strategy_history.remove(0);
        }
    }

    fn get_improvement_metrics(&self) -> f64 {
        if self.strategy_history.len() < 2 {
            return 0.0;
        }
        
        let recent_avg = self.strategy_history.iter()
            .rev()
            .take(5)
            .map(|s| s.effectiveness_score)
            .sum::<f64>() / 5.0;
            
        let older_avg = self.strategy_history.iter()
            .rev()
            .skip(5)
            .take(5)
            .map(|s| s.effectiveness_score)
            .sum::<f64>() / 5.0;
            
        recent_avg - older_avg
    }
}

/// Analytics engine for tracking cleanup performance
struct CleanupAnalytics {
    cleanup_history: VecDeque<CleanupEvent>,
    performance_trends: BTreeMap<SystemTime, f64>,
}

impl CleanupAnalytics {
    fn new() -> Self {
        Self {
            cleanup_history: VecDeque::new(),
            performance_trends: BTreeMap::new(),
        }
    }

    fn record_cleanup_event(&mut self, event: CleanupEvent) {
        self.cleanup_history.push_back(event.clone());
        self.performance_trends.insert(event.timestamp, event.effectiveness.efficiency_score);
        
        // Keep only last 100 events
        while self.cleanup_history.len() > 100 {
            self.cleanup_history.pop_front();
        }
        
        // Keep only last 100 trend points
        while self.performance_trends.len() > 100 {
            if let Some(oldest_key) = self.performance_trends.keys().next().cloned() {
                self.performance_trends.remove(&oldest_key);
            }
        }
    }

    fn calculate_average_effectiveness(&self) -> f64 {
        if self.cleanup_history.is_empty() {
            return 0.0;
        }
        
        self.cleanup_history.iter()
            .map(|event| event.effectiveness.efficiency_score)
            .sum::<f64>() / self.cleanup_history.len() as f64
    }

    fn analyze_trends(&self) -> TrendAnalysis {
        let mut trend_direction = TrendDirection::Stable;
        let mut trend_strength = 0.0;
        
        if self.performance_trends.len() >= 10 {
            let values: Vec<f64> = self.performance_trends.values().cloned().collect();
            let first_half = &values[..values.len()/2];
            let second_half = &values[values.len()/2..];
            
            let first_avg = first_half.iter().sum::<f64>() / first_half.len() as f64;
            let second_avg = second_half.iter().sum::<f64>() / second_half.len() as f64;
            
            let difference = second_avg - first_avg;
            trend_strength = difference.abs();
            
            trend_direction = if difference > 0.05 {
                TrendDirection::Improving
            } else if difference < -0.05 {
                TrendDirection::Declining
            } else {
                TrendDirection::Stable
            };
        }
        
        TrendAnalysis {
            direction: trend_direction,
            strength: trend_strength,
            confidence: if self.performance_trends.len() >= 20 { 0.8 } else { 0.5 },
        }
    }

    fn generate_performance_insights(&self) -> Vec<String> {
        let mut insights = Vec::new();
        
        if let Some(best_event) = self.cleanup_history.iter()
            .max_by(|a, b| a.effectiveness.efficiency_score.partial_cmp(&b.effectiveness.efficiency_score).unwrap()) {
            insights.push(format!("Best cleanup freed {:.2}GB in {:.1}s", 
                                best_event.result.space_freed as f64 / 1_000_000_000.0,
                                best_event.result.cleanup_duration.as_secs_f64()));
        }
        
        let avg_effectiveness = self.calculate_average_effectiveness();
        if avg_effectiveness > 0.8 {
            insights.push("Cleanup strategy is performing excellently".to_string());
        } else if avg_effectiveness < 0.5 {
            insights.push("Cleanup strategy needs optimization".to_string());
        }
        
        insights
    }

    fn analyze_optimal_schedule(&self, _config: &IntelligentCleanupConfig) -> CleanupScheduleRecommendation {
        // Analyze historical data to recommend optimal schedule
        let recommended_frequency = if self.calculate_average_effectiveness() > 0.8 {
            Duration::from_secs(8 * 60 * 60) // 8 hours if performing well
        } else {
            Duration::from_secs(4 * 60 * 60) // 4 hours if needs improvement
        };
        
        CleanupScheduleRecommendation {
            recommended_frequency,
            optimal_time_of_day: "02:00".to_string(), // Low usage time
            confidence: 0.7,
            reasoning: "Based on historical performance analysis".to_string(),
        }
    }
}

// Data structures for intelligent cleanup

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntelligentCleanupConfig {
    pub enable_predictive_analysis: bool,
    pub enable_dynamic_optimization: bool,
    pub enable_learning: bool,
    pub prediction_horizon_days: u32,
    pub optimization_aggressiveness: f64,
    pub confidence_threshold: f64,
}

impl Default for IntelligentCleanupConfig {
    fn default() -> Self {
        Self {
            enable_predictive_analysis: true,
            enable_dynamic_optimization: true,
            enable_learning: true,
            prediction_horizon_days: 30,
            optimization_aggressiveness: 0.7,
            confidence_threshold: 0.6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IntelligentCleanupResult {
    pub base_result: CleanupResult,
    pub system_metrics: SystemMetrics,
    pub prediction: StoragePrediction,
    pub optimized_strategy: OptimizedCleanupStrategy,
    pub effectiveness: CleanupEffectiveness,
    pub analysis_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct SystemMetrics {
    pub timestamp: SystemTime,
    pub total_storage_used: u64,
    pub available_storage: u64,
    pub storage_growth_rate: u64, // bytes per day
    pub snapshot_creation_rate: f64, // snapshots per day
    pub read_access_patterns: HashMap<SnapshotType, AccessPattern>,
    pub system_load: f64,
    pub network_bandwidth_usage: f64,
    pub estimated_future_load: f64,
}

#[derive(Debug, Clone)]
pub struct AccessPattern {
    pub avg_accesses_per_day: f64,
    pub last_access_age: Duration,
    pub access_frequency_trend: f64, // positive = increasing, negative = decreasing
    pub importance_score: f64, // 0.0 to 1.0
}

#[derive(Debug, Clone)]
pub struct StoragePrediction {
    pub next_7_days: u64,
    pub next_30_days: u64,
    pub next_90_days: u64,
    pub confidence_level: f64,
    pub recommended_free_space: u64,
    pub critical_cleanup_threshold: u64,
}

#[derive(Debug, Clone)]
pub struct CleanupUrgency {
    pub level: IntelligentUrgencyLevel,
    pub storage_pressure: f64,
    pub predicted_pressure: f64,
    pub days_until_critical: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IntelligentUrgencyLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct OptimizedCleanupStrategy {
    pub strategy_id: String,
    pub urgency_level: IntelligentUrgencyLevel,
    pub aggressiveness_factor: f64,
    pub dynamic_config: DynamicCleanupConfig,
    pub expected_space_freed: u64,
    pub confidence_score: f64,
}

#[derive(Debug, Clone)]
pub struct DynamicCleanupConfig {
    pub age_multiplier: f64,
    pub count_multiplier: f64,
    pub size_threshold_multiplier: f64,
    pub protection_relaxation: bool,
}

#[derive(Debug, Clone)]
pub struct CleanupEffectiveness {
    pub rating: EffectivenessRating,
    pub space_freed_ratio: f64,
    pub efficiency_score: f64,
    pub impact_on_system_performance: PerformanceImpact,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum EffectivenessRating {
    Excellent,
    Good,
    Fair,
    Poor,
}

#[derive(Debug, Clone)]
pub struct PerformanceImpact {
    pub cpu_usage_during_cleanup: f64,
    pub memory_usage_during_cleanup: f64,
    pub disk_io_impact: f64,
    pub network_impact: f64,
    pub overall_impact_score: f64,
}

#[derive(Debug, Clone)]
struct StorageDataPoint {
    timestamp: SystemTime,
    storage_used: u64,
    growth_rate: u64,
    creation_rate: f64,
}

#[derive(Debug, Clone)]
struct StrategyPerformance {
    strategy_id: String,
    space_freed: u64,
    execution_time: Duration,
    effectiveness_score: f64,
}

#[derive(Debug, Clone)]
struct CleanupEvent {
    timestamp: SystemTime,
    metrics: SystemMetrics,
    result: CleanupResult,
    effectiveness: CleanupEffectiveness,
}

#[derive(Debug, Clone)]
pub struct CleanupAnalyticsReport {
    pub total_intelligent_cleanups: usize,
    pub average_effectiveness_score: f64,
    pub prediction_accuracy: f64,
    pub optimization_improvements: f64,
    pub trend_analysis: TrendAnalysis,
    pub performance_insights: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TrendAnalysis {
    pub direction: TrendDirection,
    pub strength: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub enum TrendDirection {
    Improving,
    Stable,
    Declining,
}

#[derive(Debug, Clone)]
pub struct CleanupScheduleRecommendation {
    pub recommended_frequency: Duration,
    pub optimal_time_of_day: String,
    pub confidence: f64,
    pub reasoning: String,
}
