// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Advanced compression management system
//!
//! This module provides sophisticated compression management capabilities
//! including multi-stage compression, adaptive algorithm selection,
//! and performance optimization.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};


use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

use crate::creator::{CollectedStateData, SnapshotCompressor};
use crate::types::{
    config::CompressionType,
    error::SnapshotResult,
};

/// Advanced compression manager that orchestrates multiple compression strategies
pub struct AdvancedCompressionManager {
    compressors: HashMap<CompressionType, Arc<SnapshotCompressor>>,
    config: Arc<RwLock<AdvancedCompressionConfig>>,
    performance_tracker: Arc<RwLock<CompressionPerformanceTracker>>,
}

impl AdvancedCompressionManager {
    /// Create a new advanced compression manager
    pub async fn new() -> SnapshotResult<Self> {
        let mut compressors = HashMap::new();
        
        // Initialize compressors for each algorithm
        for algorithm in [CompressionType::None, CompressionType::Lz4, CompressionType::Gzip, CompressionType::Zstd] {
            let compressor = Arc::new(SnapshotCompressor::new(algorithm)?);
            compressors.insert(algorithm, compressor);
        }
        
        Ok(Self {
            compressors,
            config: Arc::new(RwLock::new(AdvancedCompressionConfig::default())),
            performance_tracker: Arc::new(RwLock::new(CompressionPerformanceTracker::new())),
        })
    }

    /// Perform advanced compression with multiple stages and optimization
    #[instrument(level = "info", skip(self, data))]
    pub async fn compress_advanced(&self, data: &CollectedStateData) -> SnapshotResult<AdvancedCompressionResult> {
        let start_time = Instant::now();
        info!("Starting advanced compression");
        
        // Stage 1: Analyze data characteristics
        let analysis = self.analyze_data_for_compression(data).await?;
        
        // Stage 2: Select optimal compression strategy
        let strategy = self.select_compression_strategy(&analysis).await?;
        
        // Stage 3: Pre-process data if beneficial
        let processed_data = if strategy.enable_preprocessing {
            self.preprocess_data(data).await?
        } else {
            data.clone()
        };
        
        // Stage 4: Apply compression using selected algorithm
        let compressed_data = self.apply_compression(&processed_data, &strategy).await?;
        
        // Stage 5: Post-process and optimize result
        let optimized_data = if strategy.enable_post_optimization {
            self.post_optimize_compression(&compressed_data, &strategy).await?
        } else {
            compressed_data
        };
        
        let compression_time = start_time.elapsed();
        
        // Calculate metrics
        let original_size = self.calculate_data_size(data);
        let compressed_size = self.calculate_data_size(&optimized_data);
        let compression_ratio = compressed_size as f64 / original_size as f64;
        
        // Record performance
        let performance_record = CompressionPerformanceRecord {
            original_size,
            compressed_size,
            compression_ratio,
            compression_time,
            algorithm_used: strategy.primary_algorithm,
            stages_used: strategy.get_stages_description(),
        };
        
        self.record_performance(performance_record).await?;
        
        Ok(AdvancedCompressionResult {
            compressed_data: optimized_data,
            original_size,
            compressed_size,
            compression_ratio,
            compression_time,
            algorithm_used: strategy.primary_algorithm,
            optimization_applied: strategy.enable_post_optimization,
            performance_metrics: self.get_current_performance_metrics().await?,
        })
    }

    /// Analyze data characteristics for optimal compression strategy selection
    async fn analyze_data_for_compression(&self, data: &CollectedStateData) -> SnapshotResult<CompressionAnalysis> {
        debug!("Analyzing data characteristics for compression");
        
        let total_size = self.calculate_data_size(data);
        let complexity_score = self.calculate_data_complexity(data).await?;
        let entropy_level = self.estimate_entropy_level(data).await?;
        let compressibility_estimate = self.estimate_compressibility(data).await?;
        
        Ok(CompressionAnalysis {
            total_size,
            complexity_score,
            entropy_level,
            compressibility_estimate,
            component_sizes: self.analyze_component_sizes(data),
        })
    }

    /// Calculate total data size
    fn calculate_data_size(&self, data: &CollectedStateData) -> usize {
        let mut total = 0;
        
        if let Some(ref object_store) = data.object_store {
            total += object_store.len();
        }
        if let Some(ref authority_state) = data.authority_state {
            total += authority_state.len();
        }
        if let Some(ref epoch_store) = data.epoch_store {
            total += epoch_store.len();
        }
        if let Some(ref checkpoint_store) = data.checkpoint_store {
            total += checkpoint_store.len();
        }
        if let Some(ref transaction_store) = data.transaction_store {
            total += transaction_store.len();
        }
        
        total
    }

    /// Calculate data complexity score
    async fn calculate_data_complexity(&self, data: &CollectedStateData) -> SnapshotResult<f64> {
        let mut complexity = 0.0;
        let mut component_count = 0;
        
        // Analyze each component
        if let Some(ref component) = data.object_store {
            complexity += self.analyze_component_complexity(component).await?;
            component_count += 1;
        }
        
        if let Some(ref component) = data.authority_state {
            complexity += self.analyze_component_complexity(component).await?;
            component_count += 1;
        }
        
        if let Some(ref component) = data.epoch_store {
            complexity += self.analyze_component_complexity(component).await?;
            component_count += 1;
        }
        
        Ok(if component_count > 0 {
            complexity / component_count as f64
        } else {
            0.0
        })
    }

    /// Analyze complexity of a single component
    async fn analyze_component_complexity(&self, component: &[u8]) -> SnapshotResult<f64> {
        if component.is_empty() {
            return Ok(0.0);
        }
        
        // Simple complexity analysis based on byte patterns
        let sample_size = std::cmp::min(1024, component.len());
        let mut unique_bytes = std::collections::HashSet::new();
        
        for &byte in &component[..sample_size] {
            unique_bytes.insert(byte);
        }
        
        // Higher unique byte ratio indicates higher complexity
        Ok(unique_bytes.len() as f64 / 256.0)
    }

    /// Estimate entropy level of the data
    async fn estimate_entropy_level(&self, data: &CollectedStateData) -> SnapshotResult<EntropyLevel> {
        let sample_data = if let Some(ref object_store) = data.object_store {
            if !object_store.is_empty() {
                &object_store[..std::cmp::min(4096, object_store.len())]
            } else {
                &[]
            }
        } else {
            &[]
        };
        
        if sample_data.is_empty() {
            return Ok(EntropyLevel::Low);
        }
        
        // Calculate Shannon entropy
        let mut frequency = [0u32; 256];
        for &byte in sample_data {
            frequency[byte as usize] += 1;
        }
        
        let mut entropy = 0.0;
        let total = sample_data.len() as f64;
        
        for &freq in &frequency {
            if freq > 0 {
                let p = freq as f64 / total;
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
    async fn estimate_compressibility(&self, data: &CollectedStateData) -> SnapshotResult<f64> {
        let mut total_compressibility = 0.0;
        let mut component_count = 0;
        
        if let Some(ref component) = data.object_store {
            total_compressibility += self.estimate_component_compressibility(component).await?;
            component_count += 1;
        }
        
        if let Some(ref component) = data.authority_state {
            total_compressibility += self.estimate_component_compressibility(component).await?;
            component_count += 1;
        }
        
        Ok(if component_count > 0 {
            total_compressibility / component_count as f64
        } else {
            0.0
        })
    }

    /// Estimate compressibility for a single component
    async fn estimate_component_compressibility(&self, component: &[u8]) -> SnapshotResult<f64> {
        if component.len() < 64 {
            return Ok(0.0);
        }
        
        // Simple compressibility estimation using pattern detection
        let sample_size = std::cmp::min(2048, component.len());
        let mut pattern_counts = std::collections::HashMap::new();
        let pattern_length = 4;
        
        for window in component[..sample_size].windows(pattern_length) {
            *pattern_counts.entry(window).or_insert(0) += 1;
        }
        
        let repeated_patterns = pattern_counts.values().filter(|&&count| count > 1).count();
        let total_patterns = sample_size.saturating_sub(pattern_length - 1);
        
        Ok(repeated_patterns as f64 / total_patterns as f64)
    }

    /// Analyze component sizes for strategy selection
    fn analyze_component_sizes(&self, data: &CollectedStateData) -> ComponentSizeAnalysis {
        let object_size = data.object_store.as_ref().map_or(0, |v| v.len());
        let authority_size = data.authority_state.as_ref().map_or(0, |v| v.len());
        let epoch_size = data.epoch_store.as_ref().map_or(0, |v| v.len());
        let checkpoint_size = data.checkpoint_store.as_ref().map_or(0, |v| v.len());
        let transaction_size = data.transaction_store.as_ref().map_or(0, |v| v.len());
        
        let total = object_size + authority_size + epoch_size + checkpoint_size + transaction_size;
        
        ComponentSizeAnalysis {
            object_store_ratio: if total > 0 { object_size as f64 / total as f64 } else { 0.0 },
            authority_state_ratio: if total > 0 { authority_size as f64 / total as f64 } else { 0.0 },
            epoch_store_ratio: if total > 0 { epoch_size as f64 / total as f64 } else { 0.0 },
            checkpoint_store_ratio: if total > 0 { checkpoint_size as f64 / total as f64 } else { 0.0 },
            transaction_store_ratio: if total > 0 { transaction_size as f64 / total as f64 } else { 0.0 },
            largest_component: self.identify_largest_component(object_size, authority_size, epoch_size, checkpoint_size, transaction_size),
        }
    }

    /// Identify the largest component
    fn identify_largest_component(&self, object: usize, authority: usize, epoch: usize, checkpoint: usize, transaction: usize) -> ComponentType {
        let sizes = [
            (object, ComponentType::ObjectStore),
            (authority, ComponentType::AuthorityState),
            (epoch, ComponentType::EpochStore),
            (checkpoint, ComponentType::CheckpointStore),
            (transaction, ComponentType::TransactionStore),
        ];
        
        sizes.iter()
            .max_by_key(|(size, _)| *size)
            .map(|(_, component_type)| *component_type)
            .unwrap_or(ComponentType::ObjectStore)
    }

    /// Select optimal compression strategy based on analysis
    async fn select_compression_strategy(&self, analysis: &CompressionAnalysis) -> SnapshotResult<CompressionStrategy> {
        let config = self.config.read().await;
        
        // Select primary algorithm based on data characteristics
        let primary_algorithm = match analysis.entropy_level {
            EntropyLevel::High => CompressionType::Zstd,
            EntropyLevel::Medium => {
                if analysis.compressibility_estimate > 0.6 {
                    CompressionType::Gzip
                } else {
                    CompressionType::Lz4
                }
            }
            EntropyLevel::Low => CompressionType::Gzip,
        };
        
        // Determine whether to enable preprocessing
        let enable_preprocessing = analysis.complexity_score > 0.5 && analysis.total_size > 1024 * 1024;
        
        // Determine whether to enable post-optimization
        let enable_post_optimization = config.enable_post_optimization && analysis.total_size > 5 * 1024 * 1024;
        
        Ok(CompressionStrategy {
            primary_algorithm,
            enable_preprocessing,
            enable_post_optimization,
            target_compression_ratio: config.target_compression_ratio,
        })
    }

    /// Preprocess data for better compression
    async fn preprocess_data(&self, data: &CollectedStateData) -> SnapshotResult<CollectedStateData> {
        debug!("Preprocessing data for better compression");
        
        // For now, return the data as-is
        // In a real implementation, this might include:
        // - Data deduplication
        // - Pattern optimization
        // - Component reordering
        Ok(data.clone())
    }

    /// Apply compression using the selected strategy
    async fn apply_compression(&self, data: &CollectedStateData, strategy: &CompressionStrategy) -> SnapshotResult<CollectedStateData> {
        debug!("Applying compression with algorithm: {:?}", strategy.primary_algorithm);
        
        let compressor = self.compressors.get(&strategy.primary_algorithm)
            .ok_or_else(|| {
                crate::types::error::SnapshotError::Compression(
                    format!("Compressor not found for algorithm: {:?}", strategy.primary_algorithm)
                )
            })?;
        
        compressor.compress(data).await
    }

    /// Post-optimize compression result
    async fn post_optimize_compression(&self, data: &CollectedStateData, _strategy: &CompressionStrategy) -> SnapshotResult<CollectedStateData> {
        debug!("Post-optimizing compression result");
        
        // For now, return the data as-is
        // In a real implementation, this might include:
        // - Additional compression passes
        // - Format optimization
        // - Metadata optimization
        Ok(data.clone())
    }

    /// Record compression performance
    async fn record_performance(&self, record: CompressionPerformanceRecord) -> SnapshotResult<()> {
        let mut tracker = self.performance_tracker.write().await;
        tracker.add_record(record);
        Ok(())
    }

    /// Get current performance metrics
    async fn get_current_performance_metrics(&self) -> SnapshotResult<AdvancedCompressionMetrics> {
        let tracker = self.performance_tracker.read().await;
        Ok(tracker.get_metrics())
    }
}

// Supporting data structures

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedCompressionConfig {
    pub enable_preprocessing: bool,
    pub enable_post_optimization: bool,
    pub target_compression_ratio: f64,
    pub max_compression_time: Duration,
}

impl Default for AdvancedCompressionConfig {
    fn default() -> Self {
        Self {
            enable_preprocessing: true,
            enable_post_optimization: true,
            target_compression_ratio: 0.5,
            max_compression_time: Duration::from_secs(300), // 5 minutes
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdvancedCompressionResult {
    pub compressed_data: CollectedStateData,
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
    pub compression_time: Duration,
    pub algorithm_used: CompressionType,
    pub optimization_applied: bool,
    pub performance_metrics: AdvancedCompressionMetrics,
}

#[derive(Debug, Clone)]
struct CompressionAnalysis {
    total_size: usize,
    complexity_score: f64,
    entropy_level: EntropyLevel,
    compressibility_estimate: f64,
    component_sizes: ComponentSizeAnalysis,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntropyLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
struct ComponentSizeAnalysis {
    object_store_ratio: f64,
    authority_state_ratio: f64,
    epoch_store_ratio: f64,
    checkpoint_store_ratio: f64,
    transaction_store_ratio: f64,
    largest_component: ComponentType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ComponentType {
    ObjectStore,
    AuthorityState,
    EpochStore,
    CheckpointStore,
    TransactionStore,
}

#[derive(Debug, Clone)]
struct CompressionStrategy {
    primary_algorithm: CompressionType,
    enable_preprocessing: bool,
    enable_post_optimization: bool,
    target_compression_ratio: f64,
}

impl CompressionStrategy {
    fn get_stages_description(&self) -> String {
        let mut stages = vec![format!("{:?}", self.primary_algorithm)];
        
        if self.enable_preprocessing {
            stages.insert(0, "preprocessing".to_string());
        }
        
        if self.enable_post_optimization {
            stages.push("post-optimization".to_string());
        }
        
        stages.join(" -> ")
    }
}

#[derive(Debug, Clone)]
struct CompressionPerformanceRecord {
    original_size: usize,
    compressed_size: usize,
    compression_ratio: f64,
    compression_time: Duration,
    algorithm_used: CompressionType,
    stages_used: String,
}

#[derive(Debug, Clone)]
pub struct AdvancedCompressionMetrics {
    pub total_compressions: u64,
    pub average_compression_ratio: f64,
    pub average_compression_time: Duration,
    pub best_compression_ratio: f64,
    pub throughput_mbps: f64,
}

struct CompressionPerformanceTracker {
    records: Vec<CompressionPerformanceRecord>,
    max_records: usize,
}

impl CompressionPerformanceTracker {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            max_records: 1000,
        }
    }

    fn add_record(&mut self, record: CompressionPerformanceRecord) {
        self.records.push(record);
        
        // Keep only recent records
        if self.records.len() > self.max_records {
            self.records.remove(0);
        }
    }

    fn get_metrics(&self) -> AdvancedCompressionMetrics {
        if self.records.is_empty() {
            return AdvancedCompressionMetrics {
                total_compressions: 0,
                average_compression_ratio: 1.0,
                average_compression_time: Duration::default(),
                best_compression_ratio: 1.0,
                throughput_mbps: 0.0,
            };
        }

        let total_compressions = self.records.len() as u64;
        
        let average_compression_ratio = self.records.iter()
            .map(|r| r.compression_ratio)
            .sum::<f64>() / self.records.len() as f64;
        
        let average_compression_time = self.records.iter()
            .map(|r| r.compression_time)
            .sum::<Duration>() / self.records.len() as u32;
        
        let best_compression_ratio = self.records.iter()
            .map(|r| r.compression_ratio)
            .fold(1.0, f64::min);
        
        let total_mb = self.records.iter()
            .map(|r| r.original_size as f64 / (1024.0 * 1024.0))
            .sum::<f64>();
        
        let total_time_seconds = self.records.iter()
            .map(|r| r.compression_time.as_secs_f64())
            .sum::<f64>();
        
        let throughput_mbps = if total_time_seconds > 0.0 {
            total_mb / total_time_seconds
        } else {
            0.0
        };

        AdvancedCompressionMetrics {
            total_compressions,
            average_compression_ratio,
            average_compression_time,
            best_compression_ratio,
            throughput_mbps,
        }
    }
}
