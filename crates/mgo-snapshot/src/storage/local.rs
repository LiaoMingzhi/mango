// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Local file system storage backend implementation

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

use crate::types::{
    error::{SnapshotError, SnapshotResult, StorageError},
    storage::{
        CleanupResult, SnapshotStorage, StorageStats, VerificationResult,
        StorageOpResult,
    },
    SnapshotData, SnapshotId, SnapshotMetadata,
};
use super::{CompressionEngine, EncryptionEngine};

/// Local file system storage implementation
#[derive(Debug)]
#[allow(dead_code)]
pub struct LocalSnapshotStorage {
    /// Base directory for snapshot storage
    base_path: PathBuf,
    /// Compression engine
    compression: CompressionEngine,
    /// Encryption engine (optional)
    encryption: Option<EncryptionEngine>,
    /// Maximum file size before splitting
    max_file_size: u64,
    /// Number of backup copies to maintain
    backup_copies: u32,
}

impl LocalSnapshotStorage {
    /// Create new local storage instance
    pub async fn new<P: AsRef<Path>>(
        base_path: P,
        compression: CompressionEngine,
        encryption: Option<EncryptionEngine>,
    ) -> SnapshotResult<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        
        // Create base directory if it doesn't exist
        if !base_path.exists() {
            fs::create_dir_all(&base_path).await.map_err(|e| {
                SnapshotError::Io(e)
            })?;
        }

        // Create subdirectories
        let subdirs = ["snapshots", "metadata", "temp", "backups"];
        for subdir in &subdirs {
            let dir_path = base_path.join(subdir);
            if !dir_path.exists() {
                fs::create_dir_all(&dir_path).await.map_err(|e| {
                    SnapshotError::Io(e)
                })?;
            }
        }

        Ok(Self {
            base_path,
            compression,
            encryption,
            max_file_size: 10 * 1024 * 1024 * 1024, // 10GB default
            backup_copies: 2,
        })
    }

    /// Get snapshot file path
    fn get_snapshot_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.base_path
            .join("snapshots")
            .join(format!("{}.snapshot", snapshot_id))
    }

    /// Get metadata file path
    fn get_metadata_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.base_path
            .join("metadata")
            .join(format!("{}.meta", snapshot_id))
    }

    /// Get backup file path
    fn get_backup_path(&self, snapshot_id: &SnapshotId, backup_num: u32) -> PathBuf {
        self.base_path
            .join("backups")
            .join(format!("{}.backup.{}", snapshot_id, backup_num))
    }

    /// Get temporary file path
    #[allow(dead_code)]
    fn get_temp_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.base_path
            .join("temp")
            .join(format!("{}.tmp", snapshot_id))
    }

    /// Serialize and compress snapshot data
    async fn serialize_and_compress(&self, data: &SnapshotData) -> SnapshotResult<Vec<u8>> {
        // Serialize to binary format
        let serialized = bincode::serialize(data)
            .map_err(SnapshotError::Serialization)?;

        debug!("Serialized snapshot data: {} bytes", serialized.len());

        // Compress the data
        let compressed = self.compression.compress(&serialized)
            .map_err(|e| SnapshotError::Compression(e.to_string()))?;

        debug!(
            "Compressed snapshot data: {} bytes (ratio: {:.2}%)",
            compressed.len(),
            (compressed.len() as f64 / serialized.len() as f64) * 100.0
        );

        // Encrypt if encryption is enabled
        if let Some(ref encryption) = self.encryption {
            let encrypted = encryption.encrypt(&compressed)
                .map_err(|e| SnapshotError::generic(format!("Encryption failed: {}", e)))?;
            Ok(encrypted)
        } else {
            Ok(compressed)
        }
    }

    /// Decompress and deserialize snapshot data
    async fn decompress_and_deserialize(&self, data: &[u8]) -> SnapshotResult<SnapshotData> {
        // Decrypt if encryption is enabled
        let data = if let Some(ref encryption) = self.encryption {
            encryption.decrypt(data)
                .map_err(|e| SnapshotError::generic(format!("Decryption failed: {}", e)))?
        } else {
            data.to_vec()
        };

        // Decompress the data
        let decompressed = self.compression.decompress(&data)
            .map_err(|e| SnapshotError::Decompression(e.to_string()))?;

        // Deserialize from binary format
        let snapshot_data: SnapshotData = bincode::deserialize(&decompressed)
            .map_err(SnapshotError::Serialization)?;

        Ok(snapshot_data)
    }

    /// Write data to file atomically
    async fn write_file_atomic<P: AsRef<Path>>(
        &self,
        path: P,
        data: &[u8],
    ) -> SnapshotResult<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");

        // Write to temporary file first
        let mut temp_file = fs::File::create(&temp_path).await
            .map_err(SnapshotError::Io)?;
        
        temp_file.write_all(data).await
            .map_err(SnapshotError::Io)?;
        
        temp_file.flush().await
            .map_err(SnapshotError::Io)?;
        
        drop(temp_file);

        // Atomically rename to final location
        fs::rename(&temp_path, path).await
            .map_err(SnapshotError::Io)?;

        Ok(())
    }

    /// Read file content
    async fn read_file<P: AsRef<Path>>(&self, path: P) -> SnapshotResult<Vec<u8>> {
        let mut file = fs::File::open(path).await
            .map_err(SnapshotError::Io)?;
        
        let mut content = Vec::new();
        file.read_to_end(&mut content).await
            .map_err(SnapshotError::Io)?;
        
        Ok(content)
    }

    /// Calculate file checksum
    async fn calculate_checksum(&self, data: &[u8]) -> String {
        let hash = blake3::hash(data);
        hex::encode(hash.as_bytes())
    }

    /// Create backup copies
    async fn create_backups(&self, snapshot_id: &SnapshotId, data: &[u8]) -> SnapshotResult<()> {
        for i in 0..self.backup_copies {
            let backup_path = self.get_backup_path(snapshot_id, i);
            self.write_file_atomic(&backup_path, data).await?;
        }
        Ok(())
    }

    /// Get disk usage information (simplified non-recursive version)
    async fn get_disk_usage<P: AsRef<Path>>(&self, path: P) -> SnapshotResult<(u64, u64)> {
        let path = path.as_ref();
        let metadata = fs::metadata(path).await
            .map_err(SnapshotError::Io)?;
        
        if !metadata.is_dir() {
            return Ok((metadata.len(), 0));
        }

        let mut total_size = 0u64;
        let mut file_count = 0u64;
        
        // Stack for directories to process
        let mut dirs_to_process = vec![path.to_path_buf()];
        
        while let Some(current_dir) = dirs_to_process.pop() {
            let mut entries = fs::read_dir(&current_dir).await
                .map_err(SnapshotError::Io)?;
            
            while let Some(entry) = entries.next_entry().await
                .map_err(SnapshotError::Io)? {
                let metadata = entry.metadata().await
                    .map_err(SnapshotError::Io)?;
                
                if metadata.is_file() {
                    total_size += metadata.len();
                    file_count += 1;
                } else if metadata.is_dir() {
                    dirs_to_process.push(entry.path());
                }
            }
        }

        Ok((total_size, file_count))
    }
}

#[async_trait]
impl SnapshotStorage for LocalSnapshotStorage {
    async fn store_snapshot(
        &self,
        snapshot_id: SnapshotId,
        data: SnapshotData,
        mut metadata: SnapshotMetadata,
    ) -> crate::types::error::StorageResult<StorageOpResult> {
        let start_time = std::time::Instant::now();
        
        info!("Storing snapshot {} locally", snapshot_id);

        // Serialize and compress the data
        let processed_data = self.serialize_and_compress(&data).await?;
        
        // Update metadata with compression info
        metadata.uncompressed_size = data.total_size() as u64;
        metadata.compressed_size = processed_data.len() as u64;
        metadata.update_compression_metrics(metadata.uncompressed_size, metadata.compressed_size);
        
        // Calculate checksum
        metadata.checksum = self.calculate_checksum(&processed_data).await;

        // Get file paths
        let snapshot_path = self.get_snapshot_path(&snapshot_id);
        let metadata_path = self.get_metadata_path(&snapshot_id);

        // Write snapshot data
        self.write_file_atomic(&snapshot_path, &processed_data).await?;

        // Create backup copies
        self.create_backups(&snapshot_id, &processed_data).await?;

        // Write metadata
        let metadata_json = serde_json::to_vec_pretty(&metadata)
            .map_err(SnapshotError::Json)?;
        self.write_file_atomic(&metadata_path, &metadata_json).await?;

        let duration = start_time.elapsed();
        
        info!(
            "Successfully stored snapshot {} ({} bytes) in {:?}",
            snapshot_id,
            processed_data.len(),
            duration
        );

        Ok(StorageOpResult::success(
            snapshot_path.to_string_lossy().to_string(),
            processed_data.len() as u64,
            duration.as_millis() as u64,
        ))
    }

    async fn retrieve_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotData> {
        info!("Retrieving snapshot {} from local storage", snapshot_id);

        let snapshot_path = self.get_snapshot_path(&snapshot_id);
        
        if !snapshot_path.exists() {
            return Err(StorageError::Local(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Snapshot file not found: {}", snapshot_path.display()),
            )));
        }

        // Read snapshot data
        let processed_data = self.read_file(&snapshot_path).await?;

        // Decompress and deserialize
        let snapshot_data = self.decompress_and_deserialize(&processed_data).await?;

        info!("Successfully retrieved snapshot {}", snapshot_id);
        Ok(snapshot_data)
    }

    async fn retrieve_metadata(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<SnapshotMetadata> {
        let metadata_path = self.get_metadata_path(&snapshot_id);
        
        if !metadata_path.exists() {
            return Err(StorageError::Local(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Metadata file not found: {}", metadata_path.display()),
            )));
        }

        let metadata_content = self.read_file(&metadata_path).await?;
        let metadata: SnapshotMetadata = serde_json::from_slice(&metadata_content)
            .map_err(|e| StorageError::Local(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid metadata format: {}", e),
            )))?;

        Ok(metadata)
    }

    async fn delete_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<()> {
        info!("Deleting snapshot {} from local storage", snapshot_id);

        let snapshot_path = self.get_snapshot_path(&snapshot_id);
        let metadata_path = self.get_metadata_path(&snapshot_id);

        // Delete main files
        if snapshot_path.exists() {
            fs::remove_file(&snapshot_path).await
                .map_err(StorageError::Local)?;
        }

        if metadata_path.exists() {
            fs::remove_file(&metadata_path).await
                .map_err(StorageError::Local)?;
        }

        // Delete backup copies
        for i in 0..self.backup_copies {
            let backup_path = self.get_backup_path(&snapshot_id, i);
            if backup_path.exists() {
                if let Err(e) = fs::remove_file(&backup_path).await {
                    warn!("Failed to delete backup copy {}: {}", backup_path.display(), e);
                }
            }
        }

        info!("Successfully deleted snapshot {}", snapshot_id);
        Ok(())
    }

    async fn list_snapshots(
        &self,
        filter: Option<&HashMap<String, String>>,
    ) -> crate::types::error::StorageResult<Vec<SnapshotId>> {
        let snapshots_dir = self.base_path.join("snapshots");
        let mut snapshots = Vec::new();

        let mut entries = fs::read_dir(&snapshots_dir).await
            .map_err(StorageError::Local)?;

        while let Some(entry) = entries.next_entry().await
            .map_err(StorageError::Local)? {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            
            if file_name_str.ends_with(".snapshot") {
                let id_str = file_name_str.trim_end_matches(".snapshot");
                if let Ok(snapshot_id) = SnapshotId::from_string(id_str) {
                    // Apply filter if provided
                    if let Some(_filter) = filter {
                        // For now, just add all snapshots
                        // TODO: Implement filtering based on metadata
                        snapshots.push(snapshot_id);
                    } else {
                        snapshots.push(snapshot_id);
                    }
                }
            }
        }

        Ok(snapshots)
    }

    async fn verify_snapshot(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<VerificationResult> {
        let start_time = std::time::Instant::now();
        
        info!("Verifying snapshot {}", snapshot_id);

        let snapshot_path = self.get_snapshot_path(&snapshot_id);
        let metadata_path = self.get_metadata_path(&snapshot_id);

        // Check if files exist
        if !snapshot_path.exists() {
            return Ok(VerificationResult::invalid(vec![
                "Snapshot file not found".to_string()
            ]));
        }

        if !metadata_path.exists() {
            return Ok(VerificationResult::invalid(vec![
                "Metadata file not found".to_string()
            ]));
        }

        let mut result = VerificationResult::valid();

        // Read and verify metadata
        let metadata = match self.retrieve_metadata(snapshot_id.clone()).await {
            Ok(metadata) => metadata,
            Err(e) => {
                result.add_issue(format!("Failed to read metadata: {}", e));
                return Ok(result);
            }
        };

        // Read snapshot data
        let snapshot_data = match self.read_file(&snapshot_path).await {
            Ok(data) => data,
            Err(e) => {
                result.add_issue(format!("Failed to read snapshot data: {}", e));
                return Ok(result);
            }
        };

        // Verify checksum
        let calculated_checksum = self.calculate_checksum(&snapshot_data).await;
        result.checksum_valid = calculated_checksum == metadata.checksum;
        if !result.checksum_valid {
            result.add_issue(format!(
                "Checksum mismatch: expected {}, got {}",
                metadata.checksum,
                calculated_checksum
            ));
        }

        // Verify size
        result.size_valid = snapshot_data.len() as u64 == metadata.compressed_size;
        if !result.size_valid {
            result.add_issue(format!(
                "Size mismatch: expected {}, got {}",
                metadata.compressed_size,
                snapshot_data.len()
            ));
        }

        // Try to deserialize to verify format
        match self.decompress_and_deserialize(&snapshot_data).await {
            Ok(_) => {
                result.format_valid = true;
            }
            Err(e) => {
                result.format_valid = false;
                result.add_issue(format!("Format validation failed: {}", e));
            }
        }

        let duration = start_time.elapsed();
        result.duration_ms = duration.as_millis() as u64;

        if result.checksum_valid && result.size_valid && result.format_valid {
            info!("Snapshot {} verification passed", snapshot_id);
        } else {
            warn!("Snapshot {} verification failed: {}", snapshot_id, result.issues.join(", "));
        }

        Ok(result)
    }

    async fn get_storage_stats(&self) -> crate::types::error::StorageResult<StorageStats> {
        let (used_space, snapshot_count) = self.get_disk_usage(&self.base_path).await?;
        
        // Get available space (simplified implementation)
        let available_space = match fs::metadata(&self.base_path).await {
            Ok(_) => {
                // This is a simplified calculation
                // In a real implementation, you'd use statvfs or similar system calls
                1024 * 1024 * 1024 * 1024 // 1TB placeholder
            }
            Err(_) => 0,
        };

        let total_capacity = used_space + available_space;
        let average_snapshot_size = if snapshot_count > 0 {
            used_space / snapshot_count
        } else {
            0
        };

        Ok(StorageStats {
            total_capacity,
            used_space,
            available_space,
            snapshot_count,
            total_snapshot_size: used_space,
            average_snapshot_size,
            backend_type: "local".to_string(),
            last_updated: chrono::Utc::now(),
        })
    }

    async fn snapshot_exists(&self, snapshot_id: SnapshotId) -> crate::types::error::StorageResult<bool> {
        let snapshot_path = self.get_snapshot_path(&snapshot_id);
        let metadata_path = self.get_metadata_path(&snapshot_id);
        
        Ok(snapshot_path.exists() && metadata_path.exists())
    }

    async fn get_available_space(&self) -> crate::types::error::StorageResult<u64> {
        // Simplified implementation
        // In production, use platform-specific APIs to get actual available space
        Ok(1024 * 1024 * 1024 * 1024) // 1TB placeholder
    }

    async fn cleanup_storage(&self) -> crate::types::error::StorageResult<CleanupResult> {
        let start_time = std::time::Instant::now();
        let mut result = CleanupResult::new();
        
        info!("Starting storage cleanup");

        // Cleanup temporary files
        let temp_dir = self.base_path.join("temp");
        if temp_dir.exists() {
            let mut entries = fs::read_dir(&temp_dir).await
                .map_err(StorageError::Local)?;

            while let Some(entry) = entries.next_entry().await
                .map_err(StorageError::Local)? {
                if let Ok(metadata) = entry.metadata().await {
                    if metadata.is_file() {
                        if let Err(e) = fs::remove_file(entry.path()).await {
                            result.record_error(format!(
                                "Failed to remove temp file {}: {}",
                                entry.path().display(),
                                e
                            ));
                        } else {
                            result.record_cleaned_file(metadata.len());
                            result.log_entry(format!(
                                "Removed temp file: {}",
                                entry.path().display()
                            ));
                        }
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        result.duration_ms = duration.as_millis() as u64;

        info!(
            "Storage cleanup completed: {} files cleaned, {} bytes reclaimed",
            result.files_cleaned,
            result.space_reclaimed
        );

        Ok(result)
    }
}
