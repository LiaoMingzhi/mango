// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Batch processing utilities for efficient snapshot operations
//! Provides parallel processing, load balancing, and throughput optimization

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore, RwLock};
use tokio::time::timeout;
use futures::{stream::FuturesUnordered, StreamExt};
use tracing::{info, debug, warn, error, instrument};

use crate::types::error::{SnapshotResult, SnapshotError};
use crate::performance::memory_management::AutoMemoryManager;

/// Configuration for batch processing
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size for processing
    pub max_batch_size: usize,
    /// Maximum number of concurrent workers
    pub max_workers: usize,
    /// Timeout for individual batch operations
    pub batch_timeout: Duration,
    /// Backpressure threshold (number of pending batches)
    pub backpressure_threshold: usize,
    /// Enable adaptive batch sizing
    pub adaptive_sizing: bool,
    /// Target throughput (items per second)
    pub target_throughput: Option<f64>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            max_workers: num_cpus::get(),
            batch_timeout: Duration::from_secs(30),
            backpressure_threshold: 10,
            adaptive_sizing: true,
            target_throughput: None,
        }
    }
}

/// Batch processing statistics
#[derive(Debug, Clone)]
pub struct BatchStatistics {
    pub total_batches_processed: usize,
    pub total_items_processed: usize,
    pub average_batch_size: f64,
    pub average_processing_time: Duration,
    pub current_throughput: f64, // items per second
    pub peak_throughput: f64,
    pub errors_encountered: usize,
    pub backpressure_events: usize,
}

/// Batch processor for parallel processing of snapshot data
pub struct BatchProcessor<T, R> {
    config: BatchConfig,
    statistics: Arc<RwLock<BatchStatistics>>,
    memory_manager: AutoMemoryManager,
    semaphore: Arc<Semaphore>,
    _phantom: std::marker::PhantomData<(T, R)>,
}

impl<T, R> BatchProcessor<T, R> 
where
    T: Send + 'static + Clone,
    R: Send + 'static + Clone,
{
    pub fn new(config: BatchConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_workers));
        
        Self {
            statistics: Arc::new(RwLock::new(BatchStatistics {
                total_batches_processed: 0,
                total_items_processed: 0,
                average_batch_size: 0.0,
                average_processing_time: Duration::from_secs(0),
                current_throughput: 0.0,
                peak_throughput: 0.0,
                errors_encountered: 0,
                backpressure_events: 0,
            })),
            memory_manager: AutoMemoryManager::new(1000, 2000), // 1GB warning, 2GB error
            semaphore,
            config,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Process items in batches with parallel execution
    #[instrument(level = "info", skip(self, items, processor))]
    pub async fn process_batches<F, Fut>(
        &self,
        items: Vec<T>,
        processor: F,
    ) -> SnapshotResult<Vec<R>>
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = SnapshotResult<Vec<R>>> + Send,
    {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        info!("Starting batch processing of {} items", items.len());
        let start_time = Instant::now();

        // Create batches with adaptive sizing
        let batch_size = self.calculate_optimal_batch_size(items.len()).await;
        let batches = self.create_batches(items, batch_size);
        
        info!("Created {} batches with size ~{}", batches.len(), batch_size);

        // Process batches in parallel with backpressure control
        let results = self.process_batches_parallel(batches, processor).await?;

        // Update statistics
        let processing_time = start_time.elapsed();
        self.update_statistics(results.len(), processing_time).await;

        // Flatten results
        let flattened: Vec<R> = results.into_iter().flatten().collect();
        
        info!(
            "Batch processing completed: {} results in {:?}",
            flattened.len(),
            processing_time
        );

        Ok(flattened)
    }

    /// Process batches with streaming for memory efficiency
    #[instrument(level = "info", skip(self, items, processor, result_handler))]
    pub async fn process_streaming<F, Fut, H, HFut>(
        &self,
        items: Vec<T>,
        processor: F,
        result_handler: H,
    ) -> SnapshotResult<()>
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = SnapshotResult<Vec<R>>> + Send,
        H: Fn(Vec<R>) -> HFut + Send + Sync + Clone + 'static,
        HFut: std::future::Future<Output = SnapshotResult<()>> + Send,
    {
        info!("Starting streaming batch processing of {} items", items.len());
        
        let batch_size = self.calculate_optimal_batch_size(items.len()).await;
        let batches = self.create_batches(items, batch_size);

        // Create channels for streaming results
        let (result_tx, mut result_rx) = mpsc::channel::<Vec<R>>(self.config.backpressure_threshold);
        
        // Spawn result handler task
        let handler_task = {
            let result_handler = result_handler.clone();
            tokio::spawn(async move {
                let mut total_handled = 0;
                while let Some(batch_result) = result_rx.recv().await {
                    match result_handler(batch_result).await {
                        Ok(()) => {
                            total_handled += 1;
                            debug!("Handled batch result {}", total_handled);
                        }
                        Err(e) => {
                            error!("Failed to handle batch result: {}", e);
                            return Err(e);
                        }
                    }
                }
                Ok(())
            })
        };

        // Process batches and send results
        let processing_result = self.process_batches_streaming(batches, processor, result_tx).await;

        // Wait for both tasks to complete
        let handler_result = handler_task.await?;
        processing_result?;
        handler_result?;

        info!("Streaming batch processing completed");
        Ok(())
    }

    /// Calculate optimal batch size based on current conditions
    async fn calculate_optimal_batch_size(&self, total_items: usize) -> usize {
        if !self.config.adaptive_sizing {
            return self.config.max_batch_size.min(total_items);
        }

        let stats = self.statistics.read().await;
        
        // Base batch size
        let mut batch_size = self.config.max_batch_size;

        // Adjust based on memory usage
        let memory_stats = self.memory_manager.get_memory_statistics();
        let memory_pressure = memory_stats.estimated_current_memory as f64 / (1024.0 * 1024.0 * 1024.0); // GB
        
        if memory_pressure > 1.5 {
            batch_size = (batch_size as f64 * 0.7) as usize; // Reduce by 30%
            debug!("Reduced batch size due to memory pressure: {}", batch_size);
        }

        // Adjust based on throughput target
        if let Some(target_throughput) = self.config.target_throughput {
            if stats.current_throughput > target_throughput * 1.2 {
                batch_size = (batch_size as f64 * 1.1) as usize; // Increase by 10%
            } else if stats.current_throughput < target_throughput * 0.8 {
                batch_size = (batch_size as f64 * 0.9) as usize; // Decrease by 10%
            }
        }

        // Ensure reasonable bounds
        batch_size = batch_size.clamp(10, self.config.max_batch_size).min(total_items);
        
        debug!("Calculated optimal batch size: {}", batch_size);
        batch_size
    }

    /// Create batches from items
    fn create_batches(&self, items: Vec<T>, batch_size: usize) -> Vec<Vec<T>> {
        items
            .chunks(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    /// Process batches in parallel with concurrency control
    async fn process_batches_parallel<F, Fut>(
        &self,
        batches: Vec<Vec<T>>,
        processor: F,
    ) -> SnapshotResult<Vec<Vec<R>>>
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = SnapshotResult<Vec<R>>> + Send,
    {
        let mut futures = FuturesUnordered::new();
        let mut results = Vec::with_capacity(batches.len());
        let mut pending_batches = batches.into_iter().enumerate();

        // Start initial batch of tasks
        for _ in 0..self.config.max_workers {
            if let Some((index, batch)) = pending_batches.next() {
                let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
                    SnapshotError::InvalidOperation {
                        operation: "acquire_semaphore".to_string(),
                        reason: format!("Failed to acquire semaphore: {}", e),
                    }
                })?;
                
                let processor_clone = processor.clone();
                let timeout_duration = self.config.batch_timeout;
                
                let future = async move {
                    let _permit = permit; // Hold permit until task completes
                    let result = timeout(timeout_duration, processor_clone(batch)).await;
                    match result {
                        Ok(Ok(processed)) => Ok((index, processed)),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(SnapshotError::InvalidOperation {
                            operation: "batch_processing".to_string(),
                            reason: "Batch processing timeout".to_string(),
                        }),
                    }
                };
                
                futures.push(future);
            }
        }

        // Process results and start new tasks
        results.resize(pending_batches.len() + futures.len(), Vec::new());
        
        while let Some(result) = futures.next().await {
            match result {
                Ok((index, batch_result)) => {
                    results[index] = batch_result;
                    
                    // Start next batch if available
                    if let Some((index, batch)) = pending_batches.next() {
                        let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
                            SnapshotError::InvalidOperation {
                                operation: "acquire_semaphore".to_string(),
                                reason: format!("Failed to acquire semaphore: {}", e),
                            }
                        })?;
                        
                        let processor_clone = processor.clone();
                        let timeout_duration = self.config.batch_timeout;
                        
                        let future = async move {
                            let _permit = permit;
                            let result = timeout(timeout_duration, processor_clone(batch)).await;
                            match result {
                                Ok(Ok(processed)) => Ok((index, processed)),
                                Ok(Err(e)) => Err(e),
                                Err(_) => Err(SnapshotError::InvalidOperation {
                                    operation: "batch_processing".to_string(),
                                    reason: "Batch processing timeout".to_string(),
                                }),
                            }
                        };
                        
                        futures.push(future);
                    }
                }
                Err(e) => {
                    error!("Batch processing failed: {}", e);
                    // Update error statistics
                    {
                        let mut stats = self.statistics.write().await;
                        stats.errors_encountered += 1;
                    }
                    return Err(e);
                }
            }
        }

        Ok(results)
    }

    /// Process batches with streaming results
    async fn process_batches_streaming<F, Fut>(
        &self,
        batches: Vec<Vec<T>>,
        processor: F,
        result_tx: mpsc::Sender<Vec<R>>,
    ) -> SnapshotResult<()>
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = SnapshotResult<Vec<R>>> + Send,
    {
        let mut futures = FuturesUnordered::new();
        let mut pending_batches = batches.into_iter();

        // Start initial batch of tasks
        for _ in 0..self.config.max_workers {
            if let Some(batch) = pending_batches.next() {
                let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
                    SnapshotError::InvalidOperation {
                        operation: "acquire_semaphore".to_string(),
                        reason: format!("Failed to acquire semaphore: {}", e),
                    }
                })?;
                
                let processor_clone = processor.clone();
                let timeout_duration = self.config.batch_timeout;
                
                let future = async move {
                    let _permit = permit;
                    timeout(timeout_duration, processor_clone(batch)).await
                        .map_err(|_| SnapshotError::InvalidOperation {
                            operation: "batch_processing".to_string(),
                            reason: "Batch processing timeout".to_string(),
                        })?
                };
                
                futures.push(future);
            }
        }

        // Process results and start new tasks
        while let Some(result) = futures.next().await {
            match result {
                Ok(batch_result) => {
                    // Send result through channel
                    if result_tx.send(batch_result).await.is_err() {
                        warn!("Result receiver dropped, stopping processing");
                        break;
                    }
                    
                    // Start next batch if available
                    if let Some(batch) = pending_batches.next() {
                        let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
                            SnapshotError::InvalidOperation {
                                operation: "acquire_semaphore".to_string(),
                                reason: format!("Failed to acquire semaphore: {}", e),
                            }
                        })?;
                        
                        let processor_clone = processor.clone();
                        let timeout_duration = self.config.batch_timeout;
                        
                        let future = async move {
                            let _permit = permit;
                            timeout(timeout_duration, processor_clone(batch)).await
                                .map_err(|_| SnapshotError::InvalidOperation {
                                    operation: "batch_processing".to_string(),
                                    reason: "Batch processing timeout".to_string(),
                                })?
                        };
                        
                        futures.push(future);
                    }
                }
                Err(e) => {
                    error!("Streaming batch processing failed: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Update processing statistics
    async fn update_statistics(&self, processed_items: usize, processing_time: Duration) {
        let mut stats = self.statistics.write().await;
        
        stats.total_batches_processed += 1;
        stats.total_items_processed += processed_items;
        
        // Update averages
        let batch_count = stats.total_batches_processed as f64;
        stats.average_batch_size = stats.total_items_processed as f64 / batch_count;
        
        // Calculate throughput
        let seconds = processing_time.as_secs_f64();
        if seconds > 0.0 {
            stats.current_throughput = processed_items as f64 / seconds;
            if stats.current_throughput > stats.peak_throughput {
                stats.peak_throughput = stats.current_throughput;
            }
        }

        // Update average processing time (exponential moving average)
        let alpha = 0.1; // Smoothing factor
        let current_time_ms = processing_time.as_millis() as f64;
        let previous_time_ms = stats.average_processing_time.as_millis() as f64;
        let new_average_ms = alpha * current_time_ms + (1.0 - alpha) * previous_time_ms;
        stats.average_processing_time = Duration::from_millis(new_average_ms as u64);
    }

    /// Get current processing statistics
    pub async fn get_statistics(&self) -> BatchStatistics {
        self.statistics.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_statistics(&self) {
        let mut stats = self.statistics.write().await;
        *stats = BatchStatistics {
            total_batches_processed: 0,
            total_items_processed: 0,
            average_batch_size: 0.0,
            average_processing_time: Duration::from_secs(0),
            current_throughput: 0.0,
            peak_throughput: 0.0,
            errors_encountered: 0,
            backpressure_events: 0,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_processor() {
        let config = BatchConfig {
            max_batch_size: 10,
            max_workers: 2,
            ..Default::default()
        };
        
        let processor = BatchProcessor::new(config);
        
        // Test data
        let items: Vec<i32> = (0..100).collect();
        
        // Processor function that doubles each item
        let process_fn = |batch: Vec<i32>| async move {
            sleep(Duration::from_millis(10)).await; // Simulate work
            Ok(batch.into_iter().map(|x| x * 2).collect())
        };
        
        let results = processor.process_batches(items, process_fn).await.unwrap();
        
        assert_eq!(results.len(), 100);
        assert_eq!(results[0], 0);
        assert_eq!(results[99], 198);
        
        let stats = processor.get_statistics().await;
        assert!(stats.total_batches_processed > 0);
        assert_eq!(stats.total_items_processed, 100);
    }
}
