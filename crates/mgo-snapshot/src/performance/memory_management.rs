// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Memory management utilities for efficient snapshot operations
//! Includes memory pool, buffer management, and leak detection

use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};

use crate::types::error::{SnapshotResult, SnapshotError};

/// Enhanced memory pool for reusing large buffers during snapshot operations
pub struct MemoryPool {
    /// Available buffers of different sizes
    small_buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,    // ~64KB buffers
    medium_buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,   // ~1MB buffers  
    large_buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,    // ~16MB buffers
    xlarge_buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,   // ~64MB buffers
    /// Enhanced statistics
    total_allocated: AtomicUsize,
    total_reused: AtomicUsize,
    peak_memory: AtomicUsize,
    active_memory: AtomicUsize,
    fragmentation_ratio: AtomicUsize, // in basis points (0-10000)
    /// Enhanced configuration
    max_buffer_count: usize,
    small_buffer_size: usize,
    medium_buffer_size: usize,
    large_buffer_size: usize,
    xlarge_buffer_size: usize,
    memory_pressure_threshold: usize, // Memory pressure threshold in bytes
    gc_interval_ms: u64, // Garbage collection interval
}

impl MemoryPool {
    /// Create a new enhanced memory pool with optimized settings
    pub fn new() -> Self {
        Self {
            small_buffers: Arc::new(Mutex::new(VecDeque::new())),
            medium_buffers: Arc::new(Mutex::new(VecDeque::new())),
            large_buffers: Arc::new(Mutex::new(VecDeque::new())),
            xlarge_buffers: Arc::new(Mutex::new(VecDeque::new())),
            total_allocated: AtomicUsize::new(0),
            total_reused: AtomicUsize::new(0),
            peak_memory: AtomicUsize::new(0),
            active_memory: AtomicUsize::new(0),
            fragmentation_ratio: AtomicUsize::new(0),
            max_buffer_count: 150, // Increased for better performance
            small_buffer_size: 64 * 1024,        // 64KB
            medium_buffer_size: 1024 * 1024,     // 1MB
            large_buffer_size: 16 * 1024 * 1024, // 16MB
            xlarge_buffer_size: 64 * 1024 * 1024, // 64MB
            memory_pressure_threshold: 512 * 1024 * 1024, // 512MB
            gc_interval_ms: 30000, // 30 seconds
        }
    }

    /// Create a memory pool with custom configuration for different workloads
    pub fn new_with_config(
        max_buffer_count: usize,
        memory_pressure_threshold: usize,
        gc_interval_ms: u64,
    ) -> Self {
        Self {
            small_buffers: Arc::new(Mutex::new(VecDeque::new())),
            medium_buffers: Arc::new(Mutex::new(VecDeque::new())),
            large_buffers: Arc::new(Mutex::new(VecDeque::new())),
            xlarge_buffers: Arc::new(Mutex::new(VecDeque::new())),
            total_allocated: AtomicUsize::new(0),
            total_reused: AtomicUsize::new(0),
            peak_memory: AtomicUsize::new(0),
            active_memory: AtomicUsize::new(0),
            fragmentation_ratio: AtomicUsize::new(0),
            max_buffer_count,
            small_buffer_size: 64 * 1024,        // 64KB
            medium_buffer_size: 1024 * 1024,     // 1MB
            large_buffer_size: 16 * 1024 * 1024, // 16MB
            xlarge_buffer_size: 64 * 1024 * 1024, // 64MB
            memory_pressure_threshold,
            gc_interval_ms,
        }
    }

    /// Get a buffer suitable for the requested size with enhanced size selection
    pub async fn get_buffer(&self, min_size: usize) -> Vec<u8> {
        let (buffer_size, pool) = if min_size <= self.small_buffer_size {
            (self.small_buffer_size, &self.small_buffers)
        } else if min_size <= self.medium_buffer_size {
            (self.medium_buffer_size, &self.medium_buffers)
        } else if min_size <= self.large_buffer_size {
            (self.large_buffer_size, &self.large_buffers)
        } else if min_size <= self.xlarge_buffer_size {
            (self.xlarge_buffer_size, &self.xlarge_buffers)
        } else {
            // For extremely large requests, allocate exactly what's needed
            (min_size, &self.xlarge_buffers)
        };

        // Try to reuse existing buffer
        {
            let mut buffers = pool.lock().await;
            if let Some(mut buffer) = buffers.pop_front() {
                if buffer.capacity() >= min_size {
                    buffer.clear();
                    self.total_reused.fetch_add(1, Ordering::Relaxed);
                    debug!("Reused buffer of size {} for request {}", buffer.capacity(), min_size);
                    return buffer;
                }
            }
        }

        // Allocate new buffer
        let buffer = Vec::with_capacity(buffer_size);
        self.total_allocated.fetch_add(1, Ordering::Relaxed);
        
        // Update peak memory tracking
        let current_memory = self.get_estimated_memory_usage();
        let peak = self.peak_memory.load(Ordering::Relaxed);
        if current_memory > peak {
            self.peak_memory.store(current_memory, Ordering::Relaxed);
        }
        
        debug!("Allocated new buffer of size {} for request {}", buffer_size, min_size);
        buffer
    }

    /// Return a buffer to the pool for reuse
    pub async fn return_buffer(&self, buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        let pool = if capacity <= self.small_buffer_size {
            &self.small_buffers
        } else if capacity <= self.medium_buffer_size {
            &self.medium_buffers
        } else {
            &self.large_buffers
        };

        let mut buffers = pool.lock().await;
        if buffers.len() < self.max_buffer_count {
            buffers.push_back(buffer);
            debug!("Returned buffer of size {} to pool", capacity);
        } else {
            debug!("Buffer pool full, dropping buffer of size {}", capacity);
        }
    }

    /// Clear all buffers and free memory
    pub async fn clear(&self) {
        let mut small = self.small_buffers.lock().await;
        let mut medium = self.medium_buffers.lock().await;
        let mut large = self.large_buffers.lock().await;
        
        let freed_count = small.len() + medium.len() + large.len();
        
        small.clear();
        medium.clear();
        large.clear();
        
        info!("Cleared memory pool, freed {} buffers", freed_count);
    }

    /// Get memory usage statistics
    pub fn get_statistics(&self) -> MemoryStatistics {
        MemoryStatistics {
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            total_reused: self.total_reused.load(Ordering::Relaxed),
            peak_memory_bytes: self.peak_memory.load(Ordering::Relaxed),
            estimated_current_memory: self.get_estimated_memory_usage(),
        }
    }

    fn get_estimated_memory_usage(&self) -> usize {
        // This is a rough estimate - in practice you'd want more accurate tracking
        let allocated = self.total_allocated.load(Ordering::Relaxed);
        let reused = self.total_reused.load(Ordering::Relaxed);
        let active_buffers = allocated.saturating_sub(reused);
        
        // Assume average buffer size for estimation
        active_buffers * self.medium_buffer_size
    }
}

/// Memory usage statistics
#[derive(Debug, Clone)]
pub struct MemoryStatistics {
    pub total_allocated: usize,
    pub total_reused: usize,
    pub peak_memory_bytes: usize,
    pub estimated_current_memory: usize,
}

/// Memory monitor for tracking and alerting on high memory usage
pub struct MemoryMonitor {
    /// Weak reference to prevent circular dependency
    memory_pool: Weak<MemoryPool>,
    /// Memory threshold for warnings (in bytes)
    warning_threshold: usize,
    /// Memory threshold for errors (in bytes)  
    error_threshold: usize,
    /// Last reported memory usage
    last_reported: AtomicUsize,
}

impl MemoryMonitor {
    pub fn new(
        memory_pool: Weak<MemoryPool>,
        warning_threshold_mb: usize,
        error_threshold_mb: usize,
    ) -> Self {
        Self {
            memory_pool,
            warning_threshold: warning_threshold_mb * 1024 * 1024,
            error_threshold: error_threshold_mb * 1024 * 1024,
            last_reported: AtomicUsize::new(0),
        }
    }

    /// Check current memory usage and report if thresholds are exceeded
    pub async fn check_memory_usage(&self) -> SnapshotResult<()> {
        if let Some(pool) = self.memory_pool.upgrade() {
            let stats = pool.get_statistics();
            let current_memory = stats.estimated_current_memory;
            
            // Only report if memory usage has changed significantly
            let last = self.last_reported.load(Ordering::Relaxed);
            let change_threshold = 50 * 1024 * 1024; // 50MB
            
            if current_memory.abs_diff(last) > change_threshold {
                self.last_reported.store(current_memory, Ordering::Relaxed);
                
                debug!("Memory usage: {:.2}MB", current_memory as f64 / 1024.0 / 1024.0);
                
                if current_memory > self.error_threshold {
                    error!(
                        "Memory usage critical: {:.2}MB (threshold: {:.2}MB)",
                        current_memory as f64 / 1024.0 / 1024.0,
                        self.error_threshold as f64 / 1024.0 / 1024.0
                    );
                    return Err(SnapshotError::InvalidOperation {
                        operation: "memory_check".to_string(),
                        reason: format!("Memory usage exceeds error threshold: {:.2}MB", 
                                       current_memory as f64 / 1024.0 / 1024.0),
                    });
                } else if current_memory > self.warning_threshold {
                    warn!(
                        "Memory usage high: {:.2}MB (threshold: {:.2}MB)",
                        current_memory as f64 / 1024.0 / 1024.0,
                        self.warning_threshold as f64 / 1024.0 / 1024.0
                    );
                }
            }
        }
        
        Ok(())
    }

    /// Force garbage collection and memory cleanup
    pub async fn force_cleanup(&self) -> SnapshotResult<()> {
        if let Some(pool) = self.memory_pool.upgrade() {
            pool.clear().await;
            info!("Forced memory cleanup completed");
        }
        Ok(())
    }
}

/// Automatic memory management for snapshot operations
pub struct AutoMemoryManager {
    pool: Arc<MemoryPool>,
    monitor: MemoryMonitor,
    /// Enable automatic cleanup
    auto_cleanup: bool,
}

impl AutoMemoryManager {
    pub fn new(warning_threshold_mb: usize, error_threshold_mb: usize) -> Self {
        let pool = Arc::new(MemoryPool::new());
        let monitor = MemoryMonitor::new(Arc::downgrade(&pool), warning_threshold_mb, error_threshold_mb);
        
        Self {
            pool,
            monitor,
            auto_cleanup: true,
        }
    }

    /// Get a managed buffer that will be automatically returned to pool
    pub async fn get_managed_buffer(&self, min_size: usize) -> ManagedBuffer {
        let buffer = self.pool.get_buffer(min_size).await;
        ManagedBuffer::new(buffer, Arc::clone(&self.pool))
    }

    /// Check memory usage and optionally trigger cleanup
    pub async fn check_and_manage_memory(&self) -> SnapshotResult<()> {
        self.monitor.check_memory_usage().await?;
        
        if self.auto_cleanup {
            let stats = self.pool.get_statistics();
            // Trigger cleanup if we have many unused buffers
            if stats.total_allocated > stats.total_reused * 2 {
                warn!("Triggering automatic memory cleanup");
                self.monitor.force_cleanup().await?;
            }
        }
        
        Ok(())
    }

    /// Get memory statistics
    pub fn get_memory_statistics(&self) -> MemoryStatistics {
        self.pool.get_statistics()
    }
}

/// Managed buffer that automatically returns to pool when dropped
pub struct ManagedBuffer {
    buffer: Option<Vec<u8>>,
    pool: Arc<MemoryPool>,
}

impl ManagedBuffer {
    fn new(buffer: Vec<u8>, pool: Arc<MemoryPool>) -> Self {
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// Get mutable access to the buffer
    pub fn as_mut(&mut self) -> &mut Vec<u8> {
        self.buffer.as_mut().expect("Buffer already taken")
    }

    /// Get immutable access to the buffer  
    pub fn as_ref(&self) -> &Vec<u8> {
        self.buffer.as_ref().expect("Buffer already taken")
    }

    /// Take ownership of the buffer (prevents automatic return to pool)
    pub fn take(mut self) -> Vec<u8> {
        self.buffer.take().expect("Buffer already taken")
    }
}

impl Drop for ManagedBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            let pool = Arc::clone(&self.pool);
            // Return buffer to pool asynchronously
            tokio::spawn(async move {
                pool.return_buffer(buffer).await;
            });
        }
    }
}

impl MemoryPool {
    /// Enhanced memory pressure detection and optimization
    pub async fn check_memory_pressure(&self) -> MemoryPressureLevel {
        let active_memory = self.active_memory.load(Ordering::Relaxed);
        let fragmentation = self.fragmentation_ratio.load(Ordering::Relaxed);
        
        if active_memory > self.memory_pressure_threshold && fragmentation > 2000 {
            MemoryPressureLevel::Critical
        } else if active_memory > self.memory_pressure_threshold * 3 / 4 {
            MemoryPressureLevel::High
        } else if active_memory > self.memory_pressure_threshold / 2 {
            MemoryPressureLevel::Medium
        } else {
            MemoryPressureLevel::Low
        }
    }

    /// Perform intelligent garbage collection based on memory pressure
    pub async fn intelligent_gc(&self) -> SnapshotResult<GcStats> {
        debug!("Starting intelligent garbage collection");
        
        let pressure_level = self.check_memory_pressure().await;
        let mut stats = GcStats::default();
        
        match pressure_level {
            MemoryPressureLevel::Critical => {
                // Aggressive cleanup: remove 75% of buffers
                stats += self.cleanup_buffers(&self.small_buffers, 75).await?;
                stats += self.cleanup_buffers(&self.medium_buffers, 75).await?;
                stats += self.cleanup_buffers(&self.large_buffers, 75).await?;
                stats += self.cleanup_buffers(&self.xlarge_buffers, 90).await?;
            }
            MemoryPressureLevel::High => {
                // Moderate cleanup: remove 50% of buffers
                stats += self.cleanup_buffers(&self.small_buffers, 40).await?;
                stats += self.cleanup_buffers(&self.medium_buffers, 50).await?;
                stats += self.cleanup_buffers(&self.large_buffers, 60).await?;
                stats += self.cleanup_buffers(&self.xlarge_buffers, 70).await?;
            }
            MemoryPressureLevel::Medium => {
                // Light cleanup: remove older buffers
                stats += self.cleanup_buffers(&self.xlarge_buffers, 50).await?;
                stats += self.cleanup_buffers(&self.large_buffers, 30).await?;
            }
            MemoryPressureLevel::Low => {
                // Minimal cleanup: just prevent buffer count overflow
                if self.get_total_buffer_count().await > self.max_buffer_count {
                    stats += self.cleanup_buffers(&self.xlarge_buffers, 25).await?;
                }
            }
        }
        
        info!("GC completed: freed {} buffers, {} bytes", stats.buffers_freed, stats.memory_freed);
        Ok(stats)
    }

    /// Cleanup buffers with specified percentage
    async fn cleanup_buffers(
        &self,
        pool: &Arc<Mutex<VecDeque<Vec<u8>>>>,
        cleanup_percentage: usize,
    ) -> SnapshotResult<GcStats> {
        let mut stats = GcStats::default();
        let mut buffers = pool.lock().await;
        
        let target_removal = buffers.len() * cleanup_percentage / 100;
        for _ in 0..target_removal {
            if let Some(buffer) = buffers.pop_back() {
                stats.memory_freed += buffer.capacity();
                stats.buffers_freed += 1;
                self.active_memory.fetch_sub(buffer.capacity(), Ordering::Relaxed);
            }
        }
        
        Ok(stats)
    }

    /// Get total number of cached buffers across all pools
    async fn get_total_buffer_count(&self) -> usize {
        let small_count = self.small_buffers.lock().await.len();
        let medium_count = self.medium_buffers.lock().await.len();
        let large_count = self.large_buffers.lock().await.len();
        let xlarge_count = self.xlarge_buffers.lock().await.len();
        
        small_count + medium_count + large_count + xlarge_count
    }

    /// Get comprehensive memory statistics
    pub async fn get_enhanced_stats(&self) -> MemoryStats {
        let total_buffer_count = self.get_total_buffer_count().await;
        
        MemoryStats {
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            total_reused: self.total_reused.load(Ordering::Relaxed),
            peak_memory: self.peak_memory.load(Ordering::Relaxed),
            active_memory: self.active_memory.load(Ordering::Relaxed),
            fragmentation_ratio: self.fragmentation_ratio.load(Ordering::Relaxed),
            total_cached_buffers: total_buffer_count,
            memory_pressure_level: self.check_memory_pressure().await,
            reuse_efficiency: if self.total_allocated.load(Ordering::Relaxed) > 0 {
                (self.total_reused.load(Ordering::Relaxed) * 100) / self.total_allocated.load(Ordering::Relaxed)
            } else {
                0
            },
        }
    }

    /// Start automatic memory management background task
    pub fn start_auto_management(&self) -> tokio::task::JoinHandle<()> {
        let pool = Arc::new(self.clone());
        let gc_interval = std::time::Duration::from_millis(self.gc_interval_ms);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(gc_interval);
            
            loop {
                interval.tick().await;
                
                if let Err(e) = pool.intelligent_gc().await {
                    warn!("Auto GC failed: {}", e);
                } else {
                    debug!("Auto GC completed successfully");
                }
            }
        })
    }
}

/// Memory pressure levels for intelligent resource management
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryPressureLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// Garbage collection statistics
#[derive(Debug, Default, Clone)]
pub struct GcStats {
    pub buffers_freed: usize,
    pub memory_freed: usize,
}

impl std::ops::AddAssign for GcStats {
    fn add_assign(&mut self, other: GcStats) {
        self.buffers_freed += other.buffers_freed;
        self.memory_freed += other.memory_freed;
    }
}

/// Enhanced memory statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub total_allocated: usize,
    pub total_reused: usize,
    pub peak_memory: usize,
    pub active_memory: usize,
    pub fragmentation_ratio: usize,
    pub total_cached_buffers: usize,
    pub memory_pressure_level: MemoryPressureLevel,
    pub reuse_efficiency: usize, // Percentage
}

impl Clone for MemoryPool {
    fn clone(&self) -> Self {
        Self {
            small_buffers: Arc::clone(&self.small_buffers),
            medium_buffers: Arc::clone(&self.medium_buffers),
            large_buffers: Arc::clone(&self.large_buffers),
            xlarge_buffers: Arc::clone(&self.xlarge_buffers),
            total_allocated: AtomicUsize::new(self.total_allocated.load(Ordering::Relaxed)),
            total_reused: AtomicUsize::new(self.total_reused.load(Ordering::Relaxed)),
            peak_memory: AtomicUsize::new(self.peak_memory.load(Ordering::Relaxed)),
            active_memory: AtomicUsize::new(self.active_memory.load(Ordering::Relaxed)),
            fragmentation_ratio: AtomicUsize::new(self.fragmentation_ratio.load(Ordering::Relaxed)),
            max_buffer_count: self.max_buffer_count,
            small_buffer_size: self.small_buffer_size,
            medium_buffer_size: self.medium_buffer_size,
            large_buffer_size: self.large_buffer_size,
            xlarge_buffer_size: self.xlarge_buffer_size,
            memory_pressure_threshold: self.memory_pressure_threshold,
            gc_interval_ms: self.gc_interval_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_pool_basic() {
        let pool = MemoryPool::new();
        
        // Get a buffer
        let buffer = pool.get_buffer(1024).await;
        assert!(buffer.capacity() >= 1024);
        
        // Return it
        pool.return_buffer(buffer).await;
        
        // Get another buffer (should reuse)
        let buffer2 = pool.get_buffer(1024).await;
        assert!(buffer2.capacity() >= 1024);
        
        let stats = pool.get_statistics();
        assert_eq!(stats.total_allocated, 1);
        assert_eq!(stats.total_reused, 1);
    }

    #[tokio::test]
    async fn test_managed_buffer() {
        let manager = AutoMemoryManager::new(100, 200);
        
        {
            let mut managed = manager.get_managed_buffer(1024).await;
            managed.as_mut().extend_from_slice(b"test data");
            assert_eq!(managed.as_ref().len(), 9);
        } // Buffer should be returned to pool here
        
        let stats = manager.get_memory_statistics();
        assert_eq!(stats.total_allocated, 1);
    }
}

