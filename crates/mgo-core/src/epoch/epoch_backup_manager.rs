// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Epoch Backup Manager for Mango Network
//! 
//! This module provides functionality to backup epoch data before removal,
//! enabling epoch rollback capabilities while maintaining storage efficiency.

use mango_metrics::spawn_monitored_task;
use narwhal_config::Epoch;
use std::{
    fs::{self, create_dir_all},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Default backup retention time in minutes (600 minutes = 10 hours)
const DEFAULT_BACKUP_RETENTION_MINUTES: u64 = 600;

/// Configuration for epoch backup management
#[derive(Debug, Clone)]
pub struct EpochBackupConfig {
    /// Base path for storing epoch backups
    pub backup_base_path: PathBuf,
    /// How long to retain backups in minutes
    pub backup_retention_minutes: u64,
    /// Whether to enable epoch backup (default: true)
    pub enable_backup: bool,
    /// Whether to compress backups (future enhancement)
    pub compress_backups: bool,
}

impl Default for EpochBackupConfig {
    fn default() -> Self {
        Self {
            backup_base_path: PathBuf::from("epoch_backups"),
            backup_retention_minutes: DEFAULT_BACKUP_RETENTION_MINUTES,
            enable_backup: true,
            compress_backups: false,
        }
    }
}

/// Manages epoch data backup and cleanup operations
pub struct EpochBackupManager {
    config: EpochBackupConfig,
    tx_backup: mpsc::Sender<BackupRequest>,
}

#[derive(Debug)]
struct BackupRequest {
    source_path: PathBuf,
    epoch: Epoch,
}

impl EpochBackupManager {
    /// Create a new EpochBackupManager with the given configuration
    pub fn new(config: EpochBackupConfig) -> Result<Self, anyhow::Error> {
        let (tx_backup, rx_backup) = mpsc::channel(10);

        // Create backup directory if it doesn't exist
        if config.enable_backup {
            create_dir_all(&config.backup_base_path)?;
            info!(
                "Epoch Backup Manager initialized with backup path: {:?}, retention: {} minutes",
                config.backup_base_path, config.backup_retention_minutes
            );
        } else {
            info!("Epoch Backup Manager initialized with backup disabled");
        }

        let manager = Self {
            config: config.clone(),
            tx_backup,
        };

        // Start the backup worker task
        manager.start_backup_worker(rx_backup);

        Ok(manager)
    }

    /// Start the background worker for handling backup requests
    fn start_backup_worker(&self, mut rx_backup: mpsc::Receiver<BackupRequest>) {
        let config = self.config.clone();
        
        spawn_monitored_task!(async move {
            info!("Starting Epoch Backup Manager worker");
            
            // Start periodic cleanup task
            let cleanup_config = config.clone();
            spawn_monitored_task!(async move {
                let mut cleanup_interval = tokio::time::interval(Duration::from_secs(1800)); // 30 minutes
                loop {
                    cleanup_interval.tick().await;
                    if cleanup_config.enable_backup {
                        Self::cleanup_old_backups(&cleanup_config).await;
                    }
                }
            });

            // Handle backup requests
            loop {
                match rx_backup.recv().await {
                    Some(request) => {
                        if config.enable_backup {
                            Self::process_backup_request(&config, request).await;
                        }
                    }
                    None => {
                        info!("Closing Epoch Backup Manager worker");
                        break;
                    }
                }
            }
        });
    }

    /// Request backup of epoch data before removal
    pub async fn backup_epoch_data(&self, source_path: PathBuf, epoch: Epoch) -> Result<(), anyhow::Error> {
        if !self.config.enable_backup {
            return Ok(());
        }

        let request = BackupRequest {
            source_path,
            epoch,
        };

        self.tx_backup.send(request).await.map_err(|e| {
            anyhow::anyhow!("Failed to send backup request for epoch {}: {}", epoch, e)
        })?;

        Ok(())
    }

    /// Process a single backup request
    async fn process_backup_request(config: &EpochBackupConfig, request: BackupRequest) {
        let backup_dir = Self::get_backup_path(config, request.epoch);
        
        match Self::backup_epoch_directory(&request.source_path, &backup_dir, request.epoch).await {
            Ok(backup_size) => {
                info!(
                    "Successfully backed up epoch {} to {:?} (size: {})",
                    request.epoch, backup_dir, format_size(backup_size)
                );
            }
            Err(e) => {
                error!(
                    "Failed to backup epoch {} from {:?} to {:?}: {}",
                    request.epoch, request.source_path, backup_dir, e
                );
            }
        }
    }

    /// Get the backup path for a specific epoch
    fn get_backup_path(config: &EpochBackupConfig, epoch: Epoch) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        config.backup_base_path
            .join(format!("epoch_{}_backup_{}", epoch, timestamp))
    }

    /// Backup an epoch directory to the backup location
    async fn backup_epoch_directory(
        source_path: &Path,
        backup_path: &Path,
        epoch: Epoch,
    ) -> Result<u64, anyhow::Error> {
        if !source_path.exists() {
            return Err(anyhow::anyhow!("Source path does not exist: {:?}", source_path));
        }

        // Create backup directory
        create_dir_all(backup_path)?;

        // Copy epoch data
        let backup_size = copy_directory_recursive(source_path, backup_path)?;

        // Create metadata file
        let metadata_path = backup_path.join("backup_metadata.json");
        let metadata = serde_json::json!({
            "epoch": epoch,
            "backup_timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "source_path": source_path.to_string_lossy(),
            "backup_size_bytes": backup_size,
            "backup_version": "1.0"
        });
        
        fs::write(metadata_path, serde_json::to_string_pretty(&metadata)?)?;

        Ok(backup_size)
    }

    /// Clean up old backups based on retention policy
    async fn cleanup_old_backups(config: &EpochBackupConfig) {
        let retention_duration = Duration::from_secs(config.backup_retention_minutes * 60);
        let cutoff_time = SystemTime::now() - retention_duration;

        info!(
            "Starting cleanup of epoch backups older than {} minutes",
            config.backup_retention_minutes
        );

        let backup_dirs = match fs::read_dir(&config.backup_base_path) {
            Ok(dirs) => dirs,
            Err(e) => {
                error!("Failed to read backup directory: {}", e);
                return;
            }
        };

        let mut cleaned_count = 0;
        let mut cleaned_size = 0u64;

        for entry in backup_dirs {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    warn!("Error reading backup directory entry: {}", e);
                    continue;
                }
            };

            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            // Check if this is a backup directory
            if !entry.file_name().to_string_lossy().contains("epoch_") {
                continue;
            }

            // Check creation time
            let metadata = match fs::metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Failed to get metadata for {:?}: {}", path, e);
                    continue;
                }
            };

            let created = match metadata.created().or_else(|_| metadata.modified()) {
                Ok(t) => t,
                Err(e) => {
                    warn!("Failed to get creation time for {:?}: {}", path, e);
                    continue;
                }
            };

            if created < cutoff_time {
                // Calculate directory size before removal
                let dir_size = calculate_directory_size(&path).unwrap_or(0);
                
                match fs::remove_dir_all(&path) {
                    Ok(_) => {
                        cleaned_count += 1;
                        cleaned_size += dir_size;
                        info!("Removed old backup: {:?}", path);
                    }
                    Err(e) => {
                        error!("Failed to remove old backup {:?}: {}", path, e);
                    }
                }
            }
        }

        if cleaned_count > 0 {
            info!(
                "Backup cleanup completed: removed {} backups, freed {}",
                cleaned_count,
                format_size(cleaned_size)
            );
        } else {
            info!("Backup cleanup completed: no old backups to remove");
        }
    }

    /// List available epoch backups
    pub fn list_available_backups(&self) -> Result<Vec<EpochBackupInfo>, anyhow::Error> {
        if !self.config.enable_backup {
            return Ok(Vec::new());
        }

        let mut backups = Vec::new();

        let backup_dirs = fs::read_dir(&self.config.backup_base_path)?;
        
        for entry in backup_dirs {
            let entry = entry?;
            let path = entry.path();
            
            if !path.is_dir() {
                continue;
            }

            // Try to read metadata
            let metadata_path = path.join("backup_metadata.json");
            if let Ok(metadata_content) = fs::read_to_string(metadata_path) {
                if let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&metadata_content) {
                    let backup_info = EpochBackupInfo {
                        epoch: metadata["epoch"].as_u64().unwrap_or(0),
                        backup_path: path.clone(),
                        backup_timestamp: metadata["backup_timestamp"].as_u64().unwrap_or(0),
                        backup_size: metadata["backup_size_bytes"].as_u64().unwrap_or(0),
                        source_path: metadata["source_path"].as_str().unwrap_or("unknown").to_string(),
                    };
                    backups.push(backup_info);
                }
            }
        }

        // Sort by epoch
        backups.sort_by_key(|b| b.epoch);
        Ok(backups)
    }

    /// Restore epoch data from backup
    pub async fn restore_epoch_from_backup(
        &self,
        epoch: Epoch,
        target_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let backups = self.list_available_backups()?;
        
        let backup = backups
            .iter()
            .find(|b| b.epoch == epoch)
            .ok_or_else(|| anyhow::anyhow!("No backup found for epoch {}", epoch))?;

        info!(
            "Restoring epoch {} from backup {:?} to {:?}",
            epoch, backup.backup_path, target_path
        );

        // Create target directory
        create_dir_all(target_path)?;

        // Copy backup data to target
        copy_directory_recursive(&backup.backup_path, target_path)?;

        // Remove metadata file from restored data
        let metadata_path = target_path.join("backup_metadata.json");
        if metadata_path.exists() {
            fs::remove_file(metadata_path)?;
        }

        info!("Successfully restored epoch {} to {:?}", epoch, target_path);
        Ok(())
    }
}

/// Information about an epoch backup
#[derive(Debug, Clone)]
pub struct EpochBackupInfo {
    pub epoch: Epoch,
    pub backup_path: PathBuf,
    pub backup_timestamp: u64,
    pub backup_size: u64,
    pub source_path: String,
}

/// Recursively copy a directory and return total size
fn copy_directory_recursive(source: &Path, target: &Path) -> Result<u64, anyhow::Error> {
    let mut total_size = 0u64;

    if source.is_dir() {
        create_dir_all(target)?;
        
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let target_path = target.join(entry.file_name());
            
            if source_path.is_dir() {
                total_size += copy_directory_recursive(&source_path, &target_path)?;
            } else {
                fs::copy(&source_path, &target_path)?;
                total_size += fs::metadata(&source_path)?.len();
            }
        }
    } else {
        fs::copy(source, target)?;
        total_size = fs::metadata(source)?.len();
    }

    Ok(total_size)
}

/// Calculate the total size of a directory
fn calculate_directory_size(path: &Path) -> Result<u64, anyhow::Error> {
    let mut total_size = 0u64;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            
            if entry_path.is_dir() {
                total_size += calculate_directory_size(&entry_path)?;
            } else {
                total_size += fs::metadata(&entry_path)?.len();
            }
        }
    } else {
        total_size = fs::metadata(path)?.len();
    }

    Ok(total_size)
}

/// Format byte size in human-readable format
fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_epoch_backup_manager() {
        let temp_dir = TempDir::new().unwrap();
        let backup_config = EpochBackupConfig {
            backup_base_path: temp_dir.path().join("backups"),
            backup_retention_minutes: 1, // 1 minute for testing
            enable_backup: true,
            compress_backups: false,
        };

        let manager = EpochBackupManager::new(backup_config).unwrap();

        // Create test epoch data
        let source_dir = temp_dir.path().join("epoch_2");
        create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("test_file.txt"), "test data").unwrap();

        // Test backup
        manager.backup_epoch_data(source_dir, 2).await.unwrap();

        // Wait a bit for async operations
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check if backup was created
        let backups = manager.list_available_backups().unwrap();
        assert!(!backups.is_empty());
        assert_eq!(backups[0].epoch, 2);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(512), "512.00 B");
        assert_eq!(format_size(1024), "1.00 KB");
        assert_eq!(format_size(1536), "1.50 KB");
        assert_eq!(format_size(1048576), "1.00 MB");
    }
}
