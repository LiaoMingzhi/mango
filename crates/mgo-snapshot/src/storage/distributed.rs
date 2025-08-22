// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Distributed storage backend implementation

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::types::{
    error::StorageError,
    storage::{
        CleanupResult, SnapshotStorage, StorageStats, VerificationResult,
        StorageOpResult,
    },
    SnapshotData, SnapshotId, SnapshotMetadata,
};

/// Distributed storage backend using multiple replicas
pub struct DistributedSnapshotStorage {
    /// Primary storage backends
    primary_backends: Vec<Arc<dyn SnapshotStorage>>,
    /// Replication factor
    replication_factor: usize,
    /// Consistency level for operations
    consistency_level: ConsistencyLevel,
    /// Backend health status
    backend_health: Arc<RwLock<HashMap<String, BackendHealth>>>,
}

impl DistributedSnapshotStorage {
    /// Create new distributed storage instance
    pub fn new(
        backends: Vec<Arc<dyn SnapshotStorage>>,
        replication_factor: usize,
        consistency_level: ConsistencyLevel,
    ) -> Self {
        let backend_health = Arc::new(RwLock::new(HashMap::new()));
        
        Self {
            primary_backends: backends,
            replication_factor,
            consistency_level,
            backend_health,
        }
    }

    /// Get healthy backends
    async fn get_healthy_backends(&self) -> Vec<Arc<dyn SnapshotStorage>> {
        let health = self.backend_health.read().await;
        let mut healthy_backends = Vec::new();
        
        for (i, backend) in self.primary_backends.iter().enumerate() {
            let backend_id = format!("backend_{}", i);
            if let Some(health_status) = health.get(&backend_id) {
                if health_status.is_healthy {
                    healthy_backends.push(backend.clone());
                }
            } else {
                // If no health info, assume healthy
                healthy_backends.push(backend.clone());
            }
        }
        
        healthy_backends
    }

    /// Update backend health status
    async fn update_backend_health(&self, backend_index: usize, is_healthy: bool) {
        let mut health = self.backend_health.write().await;
        let backend_id = format!("backend_{}", backend_index);
        
        health.insert(backend_id, BackendHealth {
            is_healthy,
            last_check: chrono::Utc::now(),
            error_count: if is_healthy { 0 } else { 1 },
        });
    }

    /// Execute operation on multiple backends with consistency requirements
    async fn execute_with_consistency<F, T>(&self, operation: F) -> crate::types::error::StorageResult<T>
    where
        F: Fn(Arc<dyn SnapshotStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::types::error::StorageResult<T>> + Send>> + Send + Sync,
        T: Send + 'static,
    {
        let healthy_backends = self.get_healthy_backends().await;
        
        if healthy_backends.is_empty() {
            return Err(StorageError::BackendUnavailable {
                backend: "all_backends".to_string(),
            });
        }

        let required_success = match self.consistency_level {
            ConsistencyLevel::One => 1,
            ConsistencyLevel::Quorum => (healthy_backends.len() / 2) + 1,
            ConsistencyLevel::All => healthy_backends.len(),
        };

        let mut tasks = Vec::new();
        
        // Execute operation on multiple backends in parallel
        for backend in healthy_backends.iter().take(self.replication_factor) {
            let backend_clone = backend.clone();
            let op = operation(backend_clone);
            tasks.push(tokio::spawn(op));
        }

        let mut success_count = 0;
        let mut last_result = None;
        let mut errors = Vec::new();

        // Wait for results
        for task in tasks {
            match task.await {
                Ok(Ok(result)) => {
                    success_count += 1;
                    last_result = Some(result);
                    
                    if success_count >= required_success {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    errors.push(e);
                }
                Err(e) => {
                    errors.push(StorageError::Network(format!("Task failed: {}", e)));
                }
            }
        }

        if success_count >= required_success {
            Ok(last_result.unwrap())
        } else {
            Err(StorageError::Network(format!(
                "Insufficient successful operations: {}/{} required, errors: {:?}",
                success_count, required_success, errors
            )))
        }
    }

    /// Read from any healthy backend (eventual consistency)
    async fn read_from_any<F, T>(&self, operation: F) -> crate::types::error::StorageResult<T>
    where
        F: Fn(Arc<dyn SnapshotStorage>) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::types::error::StorageResult<T>> + Send>> + Send + Sync,
        T: Send + 'static,
    {
        let healthy_backends = self.get_healthy_backends().await;
        
        if healthy_backends.is_empty() {
            return Err(StorageError::BackendUnavailable {
                backend: "all_backends".to_string(),
            });
        }

        // Try backends in order until one succeeds
        for backend in healthy_backends {
            match operation(backend).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    debug!("Backend read failed, trying next: {}", e);
                    continue;
                }
            }
        }

        Err(StorageError::Network(
            "All backends failed for read operation".to_string()
        ))
    }
}

#[async_trait]
impl SnapshotStorage for DistributedSnapshotStorage {
    async fn store_snapshot(
        &self,
        snapshot_id: SnapshotId,
        data: SnapshotData,
        metadata: SnapshotMetadata,
    ) -> crate::types::error::StorageResult<StorageOpResult> {
        info!("Storing snapshot {} in distributed storage", snapshot_id);

        self.execute_with_consistency(|backend| {
            let snapshot_id = snapshot_id.clone();
            let data = data.clone();
            let metadata = metadata.clone();
            
            Box::pin(async move {
                backend.store_snapshot(snapshot_id, data, metadata).await
            })
        }).await
    }

    async fn retrieve_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotData> {
        info!("Retrieving snapshot {} from distributed storage", snapshot_id);

        self.read_from_any(|backend| {
            let snapshot_id = snapshot_id.clone();
            
            Box::pin(async move {
                backend.retrieve_snapshot(snapshot_id).await
            })
        }).await
    }

    async fn retrieve_metadata(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotMetadata> {
        self.read_from_any(|backend| {
            let snapshot_id = snapshot_id.clone();
            
            Box::pin(async move {
                backend.retrieve_metadata(snapshot_id).await
            })
        }).await
    }

    async fn delete_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<()> {
        info!("Deleting snapshot {} from distributed storage", snapshot_id);

        self.execute_with_consistency(|backend| {
            let snapshot_id = snapshot_id.clone();
            
            Box::pin(async move {
                backend.delete_snapshot(snapshot_id).await
            })
        }).await
    }

    async fn list_snapshots(
        &self,
        filter: Option<&HashMap<String, String>>,
    ) -> crate::types::error::StorageResult<Vec<SnapshotId>> {
        // For list operations, we can read from any backend
        let healthy_backends = self.get_healthy_backends().await;
        
        if healthy_backends.is_empty() {
            return Err(StorageError::BackendUnavailable {
                backend: "all_backends".to_string(),
            });
        }

        // Try first healthy backend
        if let Some(backend) = healthy_backends.first() {
            backend.list_snapshots(filter).await
        } else {
            Err(StorageError::BackendUnavailable {
                backend: "no_healthy_backends".to_string(),
            })
        }
    }

    async fn verify_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<VerificationResult> {
        info!("Verifying snapshot {} across distributed storage", snapshot_id);

        // Verify on multiple backends and combine results
        let healthy_backends = self.get_healthy_backends().await;
        let mut verification_tasks = Vec::new();

        for backend in healthy_backends.iter().take(std::cmp::min(3, healthy_backends.len())) {
            let backend_clone = backend.clone();
            let snapshot_id_clone = snapshot_id.clone();
            
            verification_tasks.push(tokio::spawn(async move {
                backend_clone.verify_snapshot(snapshot_id_clone).await
            }));
        }

        let mut successful_verifications = 0;
        let mut failed_verifications = 0;
        let mut combined_result = VerificationResult::valid();

        for task in verification_tasks {
            match task.await {
                Ok(Ok(result)) => {
                    if result.valid {
                        successful_verifications += 1;
                    } else {
                        failed_verifications += 1;
                        // Combine issues from all backends
                        for issue in result.issues {
                            combined_result.add_issue(issue);
                        }
                    }
                }
                Ok(Err(e)) => {
                    failed_verifications += 1;
                    combined_result.add_issue(format!("Backend verification failed: {}", e));
                }
                Err(e) => {
                    failed_verifications += 1;
                    combined_result.add_issue(format!("Verification task failed: {}", e));
                }
            }
        }

        // Require majority of verifications to succeed
        if successful_verifications > failed_verifications {
            combined_result.valid = true;
        } else {
            combined_result.valid = false;
        }

        Ok(combined_result)
    }

    async fn get_storage_stats(&self) -> crate::types::error::StorageResult<StorageStats> {
        // Aggregate stats from all backends
        let healthy_backends = self.get_healthy_backends().await;
        
        if healthy_backends.is_empty() {
            return Err(StorageError::BackendUnavailable {
                backend: "all_backends".to_string(),
            });
        }

        let mut total_capacity = 0u64;
        let mut total_used = 0u64;
        let mut total_available = 0u64;
        let mut total_snapshots = 0u64;
        let mut backend_count = 0u64;

        for backend in healthy_backends {
            if let Ok(stats) = backend.get_storage_stats().await {
                total_capacity += stats.total_capacity;
                total_used += stats.used_space;
                total_available += stats.available_space;
                total_snapshots += stats.snapshot_count;
                backend_count += 1;
            }
        }

        if backend_count == 0 {
            return Err(StorageError::BackendUnavailable {
                backend: "no_stats_available".to_string(),
            });
        }

        // Average the stats across backends
        Ok(StorageStats {
            total_capacity: total_capacity / backend_count,
            used_space: total_used / backend_count,
            available_space: total_available / backend_count,
            snapshot_count: total_snapshots / backend_count,
            total_snapshot_size: total_used / backend_count,
            average_snapshot_size: if total_snapshots > 0 {
                (total_used / backend_count) / (total_snapshots / backend_count)
            } else {
                0
            },
            backend_type: "distributed".to_string(),
            last_updated: chrono::Utc::now(),
        })
    }

    async fn snapshot_exists(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<bool> {
        // Check if snapshot exists on any backend
        self.read_from_any(|backend| {
            let snapshot_id = snapshot_id.clone();
            
            Box::pin(async move {
                backend.snapshot_exists(snapshot_id).await
            })
        }).await
    }

    async fn get_available_space(&self) -> crate::types::error::StorageResult<u64> {
        // Get minimum available space across all backends
        let healthy_backends = self.get_healthy_backends().await;
        
        if healthy_backends.is_empty() {
            return Err(StorageError::BackendUnavailable {
                backend: "all_backends".to_string(),
            });
        }

        let mut min_space = u64::MAX;
        
        for backend in healthy_backends {
            if let Ok(space) = backend.get_available_space().await {
                min_space = min_space.min(space);
            }
        }

        if min_space == u64::MAX {
            Err(StorageError::BackendUnavailable {
                backend: "no_space_info".to_string(),
            })
        } else {
            Ok(min_space)
        }
    }

    async fn cleanup_storage(&self) -> crate::types::error::StorageResult<CleanupResult> {
        info!("Running distributed storage cleanup");

        let healthy_backends = self.get_healthy_backends().await;
        let mut cleanup_tasks = Vec::new();

        for backend in healthy_backends {
            cleanup_tasks.push(tokio::spawn(async move {
                backend.cleanup_storage().await
            }));
        }

        let mut combined_result = CleanupResult::new();

        for task in cleanup_tasks {
            match task.await {
                Ok(Ok(result)) => {
                    combined_result.files_cleaned += result.files_cleaned;
                    combined_result.space_reclaimed += result.space_reclaimed;
                    combined_result.errors_encountered += result.errors_encountered;
                    combined_result.cleanup_log.extend(result.cleanup_log);
                }
                Ok(Err(e)) => {
                    combined_result.record_error(format!("Backend cleanup failed: {}", e));
                }
                Err(e) => {
                    combined_result.record_error(format!("Cleanup task failed: {}", e));
                }
            }
        }

        Ok(combined_result)
    }
}

/// Consistency levels for distributed operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    /// Only one replica needs to succeed
    One,
    /// Majority of replicas need to succeed
    Quorum,
    /// All replicas need to succeed
    All,
}

impl Default for ConsistencyLevel {
    fn default() -> Self {
        Self::Quorum
    }
}

/// Backend health status
#[derive(Debug, Clone)]
struct BackendHealth {
    is_healthy: bool,
    last_check: chrono::DateTime<chrono::Utc>,
    error_count: u32,
}

impl BackendHealth {
    /// Check if backend should be considered healthy
    fn is_healthy(&self) -> bool {
        self.is_healthy && self.error_count < 5
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::LocalSnapshotStorage;
    use crate::storage::compression::CompressionEngine;
    use tempfile::TempDir;

    async fn create_test_backend() -> (Arc<dyn SnapshotStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let compression = CompressionEngine::default();
        let storage = LocalSnapshotStorage::new(temp_dir.path(), compression, None)
            .await
            .unwrap();
        (Arc::new(storage), temp_dir)
    }

    #[tokio::test]
    async fn test_distributed_storage_basic() {
        let (backend1, _temp1) = create_test_backend().await;
        let (backend2, _temp2) = create_test_backend().await;
        
        let distributed = DistributedSnapshotStorage::new(
            vec![backend1, backend2],
            2,
            ConsistencyLevel::Quorum,
        );

        let snapshot_id = SnapshotId::new();
        let data = SnapshotData::new();
        let metadata = SnapshotMetadata::new(
            snapshot_id.clone(),
            crate::types::SnapshotType::Full {
                include_history: false,
                compression_level: crate::types::CompressionLevel::Medium,
            },
            0,
            0,
        );

        // Test store and retrieve
        let result = distributed
            .store_snapshot(snapshot_id.clone(), data.clone(), metadata)
            .await;
        assert!(result.is_ok());

        let retrieved = distributed.retrieve_snapshot(snapshot_id.clone()).await;
        assert!(retrieved.is_ok());

        let exists = distributed.snapshot_exists(snapshot_id.clone()).await;
        assert!(exists.unwrap());
    }
}
