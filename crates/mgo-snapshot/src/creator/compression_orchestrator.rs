// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Compression orchestrator that coordinates all compression strategies

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use crate::creator::{
    CollectedStateData, SnapshotCompressor, 
    AdvancedCompressionManager, AdvancedCompressionResult,
};
use crate::types::{
    config::CompressionType,
    error::SnapshotResult,
};

/// Compression orchestrator that manages all compression strategies
pub struct CompressionOrchestrator {
    basic_compressor: Arc<SnapshotCompressor>,
    advanced_manager: Option<Arc<AdvancedCompressionManager>>,
    config: Arc<RwLock<CompressionOrchestratorConfig>>,
    performance_monitor: Arc<RwLock<CompressionPerformanceMonitor>>,
}

impl CompressionOrchestrator {
    /// Create a new compression orchestrator
    pub fn new(
        basic_compressor: Arc<SnapshotCompressor>,
        advanced_manager: Option<Arc<AdvancedCompressionManager>>,
        config: CompressionOrchestratorConfig,
    ) -> Self {
        Self {
            basic_compressor,
            advanced_manager,
            config: Arc::new(RwLock::new(config)),
            performance_monitor: Arc::new(RwLock::new(CompressionPerformanceMonitor::new())),
        }
    }

    /// Compress data using optimal strategy selection
    #[instrument(level = "info", skip(self, data))]
    pub async fn compress_optimal(&self, data: &CollectedStateData) -> SnapshotResult<OptimalCompressionResult> {
        let start_time = Instant::now();
        info!("Starting optimal compression for {} bytes", self.calculate_data_size(data));
        
        // Step 1: Analyze data and system conditions
        let analysis = self.analyze_compression_context(data).await?;
        
        // Step 2: Select optimal compression strategy
        let strategy = self.select_compression_strategy(&analysis).await?;
        
        // Step 3: Execute compression with selected strategy
        let result = self.execute_compression_strategy(&strategy, data).await?;
        
        // Step 4: Validate and optimize result
        let optimized_result = self.validate_and_optimize_result(&analysis, &result).await?;
        
        // Step 5: Update performance metrics
        let total_time = start_time.elapsed();
        self.update_performance_tracking(&analysis, &strategy, &optimized_result, total_time).await?;
        
        Ok(OptimalCompressionResult {
            compressed_data: match &optimized_result {
                CompressionResult::Advanced(ref r) => r.compressed_data.clone(),
                CompressionResult::Basic(ref data) => data.clone(),
            },
            strategy_used: strategy,
            original_size: analysis.data_size,
            compressed_size: optimized_result.compressed_size().unwrap_or(analysis.data_size),
            compression_ratio: optimized_result.compression_ratio().unwrap_or(1.0),
            compression_time: total_time,
            performance_metrics: CompressionPerformanceMetrics {
                cpu_usage: analysis.system_metrics.cpu_usage,
                memory_usage: analysis.system_metrics.memory_usage,
                disk_io_rate: analysis.system_metrics.disk_io_rate,
                compression_efficiency: self.calculate_efficiency(&optimized_result, total_time).await?,
            },
        })
    }

    /// Analyze compression context including data characteristics and system state
    async fn analyze_compression_context(&self, data: &CollectedStateData) -> SnapshotResult<CompressionContext> {
        debug!("Analyzing compression context");
        
        let data_size = self.calculate_data_size(data);
        let data_characteristics = self.analyze_data_characteristics(data).await?;
        let system_metrics = self.collect_system_metrics().await?;
        let historical_performance = self.get_historical_performance(&data_characteristics).await?;
        
        Ok(CompressionContext {
            data_size,
            data_characteristics,
            system_metrics,
            historical_performance,
        })
    }

    /// Calculate total data size
    fn calculate_data_size(&self, data: &CollectedStateData) -> usize {
        data.object_store.as_ref().map_or(0, |v| v.len()) + 
        data.authority_state.as_ref().map_or(0, |v| v.len()) + 
        data.epoch_store.as_ref().map_or(0, |v| v.len()) + 
        data.checkpoint_store.as_ref().map_or(0, |v| v.len()) + 
        data.transaction_store.as_ref().map_or(0, |v| v.len())
    }

    /// Analyze data characteristics for optimal compression
    async fn analyze_data_characteristics(&self, data: &CollectedStateData) -> SnapshotResult<DataCharacteristics> {
        let entropy = if let Some(ref object_store) = data.object_store {
            if !object_store.is_empty() {
                self.calculate_entropy(object_store).await?
            } else {
                EntropyLevel::Low
            }
        } else {
            EntropyLevel::Low
        };
        let compressibility = self.estimate_compressibility(data).await?;
        let data_type_distribution = self.analyze_data_type_distribution(data).await?;
        
        Ok(DataCharacteristics {
            entropy_level: entropy,
            estimated_compressibility: compressibility,
            data_type_distribution,
            repetition_patterns: self.detect_repetition_patterns(data).await?,
            structural_complexity: self.assess_structural_complexity(data).await?,
        })
    }

    /// Calculate entropy for compressibility estimation
    async fn calculate_entropy(&self, sample_data: &[u8]) -> SnapshotResult<EntropyLevel> {
        if sample_data.is_empty() {
            return Ok(EntropyLevel::Low);
        }
        
        let mut frequency = [0u32; 256];
        let sample_size = std::cmp::min(4096, sample_data.len());
        
        for &byte in &sample_data[..sample_size] {
            frequency[byte as usize] += 1;
        }
        
        let mut entropy = 0.0;
        for &freq in &frequency {
            if freq > 0 {
                let p = freq as f64 / sample_size as f64;
                entropy -= p * p.log2();
            }
        }
        
        entropy /= 8.0; // Normalize to 0-1 range
        
        if entropy > 0.8 {
            Ok(EntropyLevel::High)
        } else if entropy > 0.5 {
            Ok(EntropyLevel::Medium)
        } else {
            Ok(EntropyLevel::Low)
        }
    }

    /// Estimate compressibility potential
    async fn estimate_compressibility(&self, data: &CollectedStateData) -> SnapshotResult<CompressibilityLevel> {
        // Analyze small samples to estimate overall compressibility
        let sample_size = 1024;
        let mut total_compressibility = 0.0;
        let mut sample_count = 0;
        
        if let Some(ref object_store) = data.object_store {
            if !object_store.is_empty() {
                let end = std::cmp::min(sample_size, object_store.len());
                let compressibility = self.estimate_component_compressibility(&object_store[..end]).await?;
                total_compressibility += compressibility;
                sample_count += 1;
            }
        }
        
        if let Some(ref authority_state) = data.authority_state {
            if !authority_state.is_empty() {
                let end = std::cmp::min(sample_size, authority_state.len());
                let compressibility = self.estimate_component_compressibility(&authority_state[..end]).await?;
                total_compressibility += compressibility;
                sample_count += 1;
            }
        }
        
        if let Some(ref epoch_store) = data.epoch_store {
            if !epoch_store.is_empty() {
                let end = std::cmp::min(sample_size, epoch_store.len());
                let compressibility = self.estimate_component_compressibility(&epoch_store[..end]).await?;
                total_compressibility += compressibility;
                sample_count += 1;
            }
        }
        
        if sample_count == 0 {
            return Ok(CompressibilityLevel::Low);
        }
        
        let avg_compressibility = total_compressibility / sample_count as f64;
        
        if avg_compressibility > 0.7 {
            Ok(CompressibilityLevel::High)
        } else if avg_compressibility > 0.4 {
            Ok(CompressibilityLevel::Medium)
        } else {
            Ok(CompressibilityLevel::Low)
        }
    }

    /// Estimate compressibility for a data component
    async fn estimate_component_compressibility(&self, component: &[u8]) -> SnapshotResult<f64> {
        if component.len() < 64 {
            return Ok(0.0);
        }
        
        // Simple compressibility estimation using pattern detection
        let mut unique_patterns = std::collections::HashSet::new();
        let pattern_length = 4;
        
        for window in component.windows(pattern_length) {
            unique_patterns.insert(window);
        }
        
        let total_patterns = component.len().saturating_sub(pattern_length - 1);
        let compression_potential = 1.0 - (unique_patterns.len() as f64 / total_patterns as f64);
        
        Ok(compression_potential)
    }

    /// Analyze data type distribution
    async fn analyze_data_type_distribution(&self, data: &CollectedStateData) -> SnapshotResult<DataTypeDistribution> {
        let total_size = self.calculate_data_size(data);
        if total_size == 0 {
            return Ok(DataTypeDistribution::Empty);
        }
        
        let object_ratio = data.object_store.as_ref().map_or(0, |v| v.len()) as f64 / total_size as f64;
        let authority_ratio = data.authority_state.as_ref().map_or(0, |v| v.len()) as f64 / total_size as f64;
        let epoch_ratio = data.epoch_store.as_ref().map_or(0, |v| v.len()) as f64 / total_size as f64;
        
        if object_ratio > 0.6 {
            Ok(DataTypeDistribution::ObjectStoreDominated)
        } else if authority_ratio > 0.4 {
            Ok(DataTypeDistribution::AuthorityStateDominated)
        } else if epoch_ratio > 0.3 {
            Ok(DataTypeDistribution::EpochStoreDominated)
        } else {
            Ok(DataTypeDistribution::Balanced)
        }
    }

    /// Detect repetition patterns in data
    async fn detect_repetition_patterns(&self, data: &CollectedStateData) -> SnapshotResult<RepetitionLevel> {
        // Analyze the largest component for repetition patterns
        let largest_component = {
            let object_len = data.object_store.as_ref().map_or(0, |v| v.len());
            let authority_len = data.authority_state.as_ref().map_or(0, |v| v.len());
            
            if object_len > authority_len {
                data.object_store.as_ref()
            } else {
                data.authority_state.as_ref()
            }
        };
        
        let largest_component = match largest_component {
            Some(component) if !component.is_empty() => component,
            _ => return Ok(RepetitionLevel::Low),
        };
        
        let sample_size = std::cmp::min(2048_usize, largest_component.len());
        let mut pattern_counts = std::collections::HashMap::new();
        
        for window in largest_component[..sample_size].windows(8) {
            *pattern_counts.entry(window).or_insert(0) += 1;
        }
        
        let repeated_patterns = pattern_counts.values().filter(|&&count| count > 1).count();
        let total_patterns = sample_size.saturating_sub(7);
        let repetition_ratio = repeated_patterns as f64 / total_patterns as f64;
        
        if repetition_ratio > 0.4 {
            Ok(RepetitionLevel::High)
        } else if repetition_ratio > 0.2 {
            Ok(RepetitionLevel::Medium)
        } else {
            Ok(RepetitionLevel::Low)
        }
    }

    /// Assess structural complexity
    async fn assess_structural_complexity(&self, data: &CollectedStateData) -> SnapshotResult<StructuralComplexity> {
        // Assess based on data distribution and variability
        let mut component_sizes = Vec::new();
        
        if let Some(ref component) = data.object_store {
            component_sizes.push(component.len());
        }
        if let Some(ref component) = data.authority_state {
            component_sizes.push(component.len());
        }
        if let Some(ref component) = data.epoch_store {
            component_sizes.push(component.len());
        }
        if let Some(ref component) = data.checkpoint_store {
            component_sizes.push(component.len());
        }
        if let Some(ref component) = data.transaction_store {
            component_sizes.push(component.len());
        }
        
        let non_empty_components = component_sizes.len();
        let size_variance = self.calculate_size_variance(&component_sizes).await?;
        
        if non_empty_components >= 4 && size_variance > 0.5 {
            Ok(StructuralComplexity::High)
        } else if non_empty_components >= 3 || size_variance > 0.3 {
            Ok(StructuralComplexity::Medium)
        } else {
            Ok(StructuralComplexity::Low)
        }
    }

    /// Calculate variance in component sizes
    async fn calculate_size_variance(&self, sizes: &[usize]) -> SnapshotResult<f64> {
        if sizes.is_empty() {
            return Ok(0.0);
        }
        
        let mean = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
        let variance = sizes.iter()
            .map(|&size| (size as f64 - mean).powi(2))
            .sum::<f64>() / sizes.len() as f64;
        
        Ok(if mean > 0.0 { variance.sqrt() / mean } else { 0.0 }) // Coefficient of variation
    }

    /// Collect current system metrics
    async fn collect_system_metrics(&self) -> SnapshotResult<SystemMetrics> {
        // In a real implementation, this would collect actual system metrics
        Ok(SystemMetrics {
            cpu_usage: 0.4,
            memory_usage: 0.6,
            disk_io_rate: 0.3,
            available_memory: 8 * 1024 * 1024 * 1024, // 8GB
            cpu_cores: 8,
            load_average: 1.5,
        })
    }

    /// Get historical compression performance
    async fn get_historical_performance(&self, characteristics: &DataCharacteristics) -> SnapshotResult<HistoricalCompressionPerformance> {
        let monitor = self.performance_monitor.read().await;
        monitor.get_performance_for_characteristics(characteristics).await
    }

    /// Select optimal compression strategy based on analysis
    async fn select_compression_strategy(&self, context: &CompressionContext) -> SnapshotResult<CompressionStrategy> {
        debug!("Selecting compression strategy based on context analysis");
        
        let config = self.config.read().await;
        
        // Determine if advanced compression is beneficial
        let use_advanced = self.should_use_advanced_compression(context, &config).await?;
        
        if use_advanced && self.advanced_manager.is_some() {
            Ok(CompressionStrategy::Advanced {
                enable_preprocessing: context.data_characteristics.repetition_patterns != RepetitionLevel::Low,
                enable_multi_stage: context.data_characteristics.structural_complexity == StructuralComplexity::High,
                enable_parallel: context.data_size > 10 * 1024 * 1024 && context.system_metrics.cpu_cores >= 4,
                algorithm_preference: self.select_algorithm_preference(context).await?,
            })
        } else {
            Ok(CompressionStrategy::Basic {
                algorithm: self.select_basic_algorithm(context).await?,
                level: self.select_compression_level(context).await?,
            })
        }
    }

    /// Determine if advanced compression should be used
    async fn should_use_advanced_compression(
        &self,
        context: &CompressionContext,
        config: &CompressionOrchestratorConfig,
    ) -> SnapshotResult<bool> {
        if !config.enable_advanced_compression {
            return Ok(false);
        }
        
        // Use advanced compression for:
        // 1. Large data with high compressibility
        // 2. Complex structural data
        // 3. When system resources allow
        let size_threshold = context.data_size > 5 * 1024 * 1024; // > 5MB
        let complexity_threshold = context.data_characteristics.structural_complexity != StructuralComplexity::Low;
        let compressibility_threshold = context.data_characteristics.estimated_compressibility != CompressibilityLevel::Low;
        let resource_availability = context.system_metrics.cpu_usage < 0.7 && context.system_metrics.memory_usage < 0.8;
        
        Ok((size_threshold && compressibility_threshold) || (complexity_threshold && resource_availability))
    }

    /// Select algorithm preference for advanced compression
    async fn select_algorithm_preference(&self, context: &CompressionContext) -> SnapshotResult<AlgorithmPreference> {
        match (context.data_characteristics.entropy_level, context.data_characteristics.repetition_patterns) {
            (EntropyLevel::High, _) => Ok(AlgorithmPreference::Zstd),
            (_, RepetitionLevel::High) => Ok(AlgorithmPreference::Gzip),
            (EntropyLevel::Low, RepetitionLevel::Low) => Ok(AlgorithmPreference::Lz4),
            _ => Ok(AlgorithmPreference::Balanced),
        }
    }

    /// Select basic compression algorithm
    async fn select_basic_algorithm(&self, context: &CompressionContext) -> SnapshotResult<CompressionType> {
        if context.data_size < 1024 * 1024 { // < 1MB
            Ok(CompressionType::Lz4)
        } else if context.data_characteristics.estimated_compressibility == CompressibilityLevel::High {
            Ok(CompressionType::Zstd)
        } else if context.data_characteristics.repetition_patterns == RepetitionLevel::High {
            Ok(CompressionType::Gzip)
        } else {
            Ok(CompressionType::Zstd)
        }
    }

    /// Select compression level
    async fn select_compression_level(&self, context: &CompressionContext) -> SnapshotResult<i32> {
        let base_level = if context.system_metrics.cpu_usage > 0.8 {
            1 // Low CPU usage, prefer speed
        } else if context.data_characteristics.estimated_compressibility == CompressibilityLevel::High {
            6 // High compressibility, use higher level
        } else {
            3 // Default level
        };
        
        Ok(base_level)
    }

    /// Execute compression with selected strategy
    async fn execute_compression_strategy(
        &self,
        strategy: &CompressionStrategy,
        data: &CollectedStateData,
    ) -> SnapshotResult<CompressionResult> {
        match strategy {
            CompressionStrategy::Advanced { .. } => {
                if let Some(advanced_manager) = &self.advanced_manager {
                    let result = advanced_manager.compress_advanced(data).await?;
                    Ok(CompressionResult::Advanced(result))
                } else {
                    // Fallback to basic compression
                    let result = self.basic_compressor.compress(data).await?;
                    Ok(CompressionResult::Basic(result))
                }
            }
            CompressionStrategy::Basic { algorithm: _, level: _ } => {
                let result = self.basic_compressor.compress(data).await?;
                Ok(CompressionResult::Basic(result))
            }
        }
    }

    /// Validate and optimize compression result
    async fn validate_and_optimize_result(
        &self,
        context: &CompressionContext,
        result: &CompressionResult,
    ) -> SnapshotResult<CompressionResult> {
        // Validate compression quality
        let quality_ok = self.validate_compression_quality(context, result).await?;
        
        if !quality_ok {
            warn!("Compression quality below threshold, attempting optimization");
            return self.optimize_compression_result(result).await;
        }
        
        Ok(result.clone())
    }

    /// Validate compression quality
    async fn validate_compression_quality(
        &self,
        context: &CompressionContext,
        result: &CompressionResult,
    ) -> SnapshotResult<bool> {
        let compression_ratio = match result {
            CompressionResult::Advanced(ref r) => r.compression_ratio,
            CompressionResult::Basic(_) => {
                // Estimate compression ratio for basic result
                0.7 // Simplified estimation
            }
        };
        
        let expected_ratio = match context.data_characteristics.estimated_compressibility {
            CompressibilityLevel::High => 0.5,
            CompressibilityLevel::Medium => 0.7,
            CompressibilityLevel::Low => 0.9,
        };
        
        Ok(compression_ratio <= expected_ratio * 1.2) // Allow 20% tolerance
    }

    /// Optimize compression result if quality is insufficient
    async fn optimize_compression_result(&self, result: &CompressionResult) -> SnapshotResult<CompressionResult> {
        // In a real implementation, this might try alternative algorithms or settings
        Ok(result.clone())
    }

    /// Update performance tracking
    async fn update_performance_tracking(
        &self,
        context: &CompressionContext,
        strategy: &CompressionStrategy,
        result: &CompressionResult,
        compression_time: Duration,
    ) -> SnapshotResult<()> {
        let mut monitor = self.performance_monitor.write().await;
        monitor.record_compression_performance(CompressionPerformanceRecord {
            data_characteristics: context.data_characteristics.clone(),
            strategy: strategy.clone(),
            compression_time,
            compression_ratio: match result {
                CompressionResult::Advanced(ref r) => r.compression_ratio,
                CompressionResult::Basic(_) => 0.7, // Estimated
            },
            system_metrics: context.system_metrics.clone(),
        }).await?;
        
        Ok(())
    }

    /// Calculate compression efficiency
    async fn calculate_efficiency(&self, result: &CompressionResult, compression_time: Duration) -> SnapshotResult<f64> {
        let compression_ratio = match result {
            CompressionResult::Advanced(ref r) => r.compression_ratio,
            CompressionResult::Basic(_) => 0.7,
        };
        
        let time_factor = if compression_time.as_secs_f64() > 0.0 {
            1.0 / compression_time.as_secs_f64()
        } else {
            1.0
        };
        
        Ok(compression_ratio * time_factor)
    }

    /// Get comprehensive compression statistics
    pub async fn get_compression_statistics(&self) -> CompressionStatistics {
        let monitor = self.performance_monitor.read().await;
        
        CompressionStatistics {
            total_compressions: monitor.total_compressions,
            average_compression_ratio: monitor.calculate_average_compression_ratio(),
            average_compression_time: monitor.calculate_average_compression_time(),
            algorithm_performance: monitor.get_algorithm_performance(),
            strategy_effectiveness: monitor.get_strategy_effectiveness(),
            system_impact_analysis: monitor.get_system_impact_analysis(),
        }
    }
}

// Performance monitoring for compression operations
struct CompressionPerformanceMonitor {
    performance_history: std::collections::VecDeque<CompressionPerformanceRecord>,
    total_compressions: u64,
    algorithm_stats: std::collections::HashMap<CompressionType, AlgorithmPerformanceStats>,
}

impl CompressionPerformanceMonitor {
    fn new() -> Self {
        Self {
            performance_history: std::collections::VecDeque::new(),
            total_compressions: 0,
            algorithm_stats: std::collections::HashMap::new(),
        }
    }

    async fn record_compression_performance(&mut self, record: CompressionPerformanceRecord) -> SnapshotResult<()> {
        self.performance_history.push_back(record);
        self.total_compressions += 1;
        
        // Keep only recent history
        while self.performance_history.len() > 1000 {
            self.performance_history.pop_front();
        }
        
        Ok(())
    }

    async fn get_performance_for_characteristics(&self, characteristics: &DataCharacteristics) -> SnapshotResult<HistoricalCompressionPerformance> {
        // Find similar historical compressions
        let similar_records: Vec<_> = self.performance_history.iter()
            .filter(|record| self.characteristics_similar(&record.data_characteristics, characteristics))
            .collect();
        
        if similar_records.is_empty() {
            return Ok(HistoricalCompressionPerformance::default());
        }
        
        let avg_ratio = similar_records.iter()
            .map(|r| r.compression_ratio)
            .sum::<f64>() / similar_records.len() as f64;
        
        let avg_time = similar_records.iter()
            .map(|r| r.compression_time)
            .sum::<Duration>() / similar_records.len() as u32;
        
        Ok(HistoricalCompressionPerformance {
            average_compression_ratio: avg_ratio,
            average_compression_time: avg_time,
            sample_size: similar_records.len(),
            confidence: if similar_records.len() > 10 { 0.8 } else { 0.5 },
        })
    }

    fn characteristics_similar(&self, a: &DataCharacteristics, b: &DataCharacteristics) -> bool {
        a.entropy_level == b.entropy_level &&
        a.estimated_compressibility == b.estimated_compressibility &&
        a.repetition_patterns == b.repetition_patterns
    }

    fn calculate_average_compression_ratio(&self) -> f64 {
        if self.performance_history.is_empty() {
            return 1.0;
        }
        
        self.performance_history.iter()
            .map(|r| r.compression_ratio)
            .sum::<f64>() / self.performance_history.len() as f64
    }

    fn calculate_average_compression_time(&self) -> Duration {
        if self.performance_history.is_empty() {
            return Duration::default();
        }
        
        self.performance_history.iter()
            .map(|r| r.compression_time)
            .sum::<Duration>() / self.performance_history.len() as u32
    }

    fn get_algorithm_performance(&self) -> std::collections::HashMap<CompressionType, f64> {
        let mut performance = std::collections::HashMap::new();
        
        for algorithm in [CompressionType::None, CompressionType::Lz4, CompressionType::Gzip, CompressionType::Zstd] {
            let algo_records: Vec<_> = self.performance_history.iter()
                .filter(|r| match &r.strategy {
                    CompressionStrategy::Basic { algorithm: a, .. } => *a == algorithm,
                    CompressionStrategy::Advanced { .. } => false,
                })
                .collect();
            
            if !algo_records.is_empty() {
                let avg_efficiency = algo_records.iter()
                    .map(|r| r.compression_ratio / r.compression_time.as_secs_f64().max(0.001))
                    .sum::<f64>() / algo_records.len() as f64;
                performance.insert(algorithm, avg_efficiency);
            }
        }
        
        performance
    }

    fn get_strategy_effectiveness(&self) -> f64 {
        if self.performance_history.is_empty() {
            return 0.0;
        }
        
        let advanced_performance: Vec<_> = self.performance_history.iter()
            .filter(|r| matches!(r.strategy, CompressionStrategy::Advanced { .. }))
            .collect();
        
        let basic_performance: Vec<_> = self.performance_history.iter()
            .filter(|r| matches!(r.strategy, CompressionStrategy::Basic { .. }))
            .collect();
        
        if advanced_performance.is_empty() || basic_performance.is_empty() {
            return 0.0;
        }
        
        let advanced_avg = advanced_performance.iter()
            .map(|r| r.compression_ratio)
            .sum::<f64>() / advanced_performance.len() as f64;
        
        let basic_avg = basic_performance.iter()
            .map(|r| r.compression_ratio)
            .sum::<f64>() / basic_performance.len() as f64;
        
        (basic_avg - advanced_avg) / basic_avg // Improvement ratio
    }

    fn get_system_impact_analysis(&self) -> SystemImpactAnalysis {
        let recent_records: Vec<_> = self.performance_history.iter().rev().take(50).collect();
        
        if recent_records.is_empty() {
            return SystemImpactAnalysis::default();
        }
        
        let avg_cpu_during_compression = recent_records.iter()
            .map(|r| r.system_metrics.cpu_usage)
            .sum::<f64>() / recent_records.len() as f64;
        
        let avg_memory_during_compression = recent_records.iter()
            .map(|r| r.system_metrics.memory_usage)
            .sum::<f64>() / recent_records.len() as f64;
        
        SystemImpactAnalysis {
            average_cpu_usage_during_compression: avg_cpu_during_compression,
            average_memory_usage_during_compression: avg_memory_during_compression,
            compression_throughput_mbps: self.calculate_throughput(&recent_records),
            system_responsiveness_impact: if avg_cpu_during_compression > 0.8 { 
                SystemImpactLevel::High 
            } else if avg_cpu_during_compression > 0.6 { 
                SystemImpactLevel::Medium 
            } else { 
                SystemImpactLevel::Low 
            },
        }
    }

    fn calculate_throughput(&self, records: &[&CompressionPerformanceRecord]) -> f64 {
        if records.is_empty() {
            return 0.0;
        }
        
        let total_mb = records.len() as f64 * 10.0; // Assume ~10MB average
        let total_time_seconds: f64 = records.iter()
            .map(|r| r.compression_time.as_secs_f64())
            .sum();
        
        if total_time_seconds > 0.0 {
            total_mb / total_time_seconds
        } else {
            0.0
        }
    }
}

// Data structures for compression orchestration

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionOrchestratorConfig {
    pub enable_advanced_compression: bool,
    pub enable_adaptive_selection: bool,
    pub enable_performance_tracking: bool,
    pub performance_optimization_level: f64,
    pub resource_usage_threshold: ResourceUsageThreshold,
}

impl Default for CompressionOrchestratorConfig {
    fn default() -> Self {
        Self {
            enable_advanced_compression: true,
            enable_adaptive_selection: true,
            enable_performance_tracking: true,
            performance_optimization_level: 0.8,
            resource_usage_threshold: ResourceUsageThreshold::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageThreshold {
    pub max_cpu_usage: f64,
    pub max_memory_usage: f64,
    pub max_compression_time: Duration,
}

impl Default for ResourceUsageThreshold {
    fn default() -> Self {
        Self {
            max_cpu_usage: 0.8,
            max_memory_usage: 0.9,
            max_compression_time: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimalCompressionResult {
    pub compressed_data: CollectedStateData,
    pub strategy_used: CompressionStrategy,
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub compression_time: Duration,
    pub performance_metrics: CompressionPerformanceMetrics,
}

#[derive(Debug, Clone)]
pub struct CompressionPerformanceMetrics {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_io_rate: f64,
    pub compression_efficiency: f64,
}

#[derive(Debug, Clone)]
struct CompressionContext {
    data_size: usize,
    data_characteristics: DataCharacteristics,
    system_metrics: SystemMetrics,
    historical_performance: HistoricalCompressionPerformance,
}

#[derive(Debug, Clone, PartialEq)]
struct DataCharacteristics {
    entropy_level: EntropyLevel,
    estimated_compressibility: CompressibilityLevel,
    data_type_distribution: DataTypeDistribution,
    repetition_patterns: RepetitionLevel,
    structural_complexity: StructuralComplexity,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntropyLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CompressibilityLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DataTypeDistribution {
    Empty,
    ObjectStoreDominated,
    AuthorityStateDominated,
    EpochStoreDominated,
    Balanced,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RepetitionLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StructuralComplexity {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
struct SystemMetrics {
    cpu_usage: f64,
    memory_usage: f64,
    disk_io_rate: f64,
    available_memory: u64,
    cpu_cores: u32,
    load_average: f64,
}

#[derive(Debug, Clone)]
struct HistoricalCompressionPerformance {
    average_compression_ratio: f64,
    average_compression_time: Duration,
    sample_size: usize,
    confidence: f64,
}

impl Default for HistoricalCompressionPerformance {
    fn default() -> Self {
        Self {
            average_compression_ratio: 1.0,
            average_compression_time: Duration::default(),
            sample_size: 0,
            confidence: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
enum CompressionStrategy {
    Basic {
        algorithm: CompressionType,
        level: i32,
    },
    Advanced {
        enable_preprocessing: bool,
        enable_multi_stage: bool,
        enable_parallel: bool,
        algorithm_preference: AlgorithmPreference,
    },
}

#[derive(Debug, Clone)]
enum AlgorithmPreference {
    Lz4,
    Gzip,
    Zstd,
    Balanced,
}

#[derive(Debug, Clone)]
enum CompressionResult {
    Basic(CollectedStateData),
    Advanced(AdvancedCompressionResult),
}

impl CompressionResult {
    fn compressed_size(&self) -> Option<usize> {
        match self {
            CompressionResult::Advanced(r) => Some(r.compressed_size),
            CompressionResult::Basic(_) => None,
        }
    }

    fn compression_ratio(&self) -> Option<f64> {
        match self {
            CompressionResult::Advanced(r) => Some(r.compression_ratio),
            CompressionResult::Basic(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
struct CompressionPerformanceRecord {
    data_characteristics: DataCharacteristics,
    strategy: CompressionStrategy,
    compression_time: Duration,
    compression_ratio: f64,
    system_metrics: SystemMetrics,
}

#[derive(Debug, Clone)]
struct AlgorithmPerformanceStats {
    total_compressions: u64,
    average_compression_ratio: f64,
    average_compression_time: Duration,
    efficiency_score: f64,
}

#[derive(Debug, Clone)]
pub struct CompressionStatistics {
    pub total_compressions: u64,
    pub average_compression_ratio: f64,
    pub average_compression_time: Duration,
    pub algorithm_performance: std::collections::HashMap<CompressionType, f64>,
    pub strategy_effectiveness: f64,
    pub system_impact_analysis: SystemImpactAnalysis,
}

#[derive(Debug, Clone)]
pub struct SystemImpactAnalysis {
    pub average_cpu_usage_during_compression: f64,
    pub average_memory_usage_during_compression: f64,
    pub compression_throughput_mbps: f64,
    pub system_responsiveness_impact: SystemImpactLevel,
}

impl Default for SystemImpactAnalysis {
    fn default() -> Self {
        Self {
            average_cpu_usage_during_compression: 0.0,
            average_memory_usage_during_compression: 0.0,
            compression_throughput_mbps: 0.0,
            system_responsiveness_impact: SystemImpactLevel::Low,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SystemImpactLevel {
    Low,
    Medium,
    High,
}
