// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Snapshot metadata storage and management

use std::path::{Path, PathBuf};

use tokio::fs;
use serde_json;

use crate::types::{
    SnapshotId, SnapshotMetadata,
    error::{SnapshotError, SnapshotResult},
};

/// Snapshot metadata store
#[derive(Debug)]
pub struct SnapshotMetadataStore {
    /// Base directory for metadata storage
    base_path: PathBuf,
}

impl SnapshotMetadataStore {
    /// Create new metadata store
    pub async fn new<P: AsRef<Path>>(base_path: P) -> SnapshotResult<Self> {
        let base_path = base_path.as_ref().join("metadata");
        
        if !base_path.exists() {
            fs::create_dir_all(&base_path).await
                .map_err(SnapshotError::Io)?;
        }

        Ok(Self { base_path })
    }

    /// Store snapshot metadata
    pub async fn store_metadata(&self, metadata: &SnapshotMetadata) -> SnapshotResult<()> {
        let file_path = self.get_metadata_path(&metadata.id);
        let json_data = serde_json::to_vec_pretty(metadata)
            .map_err(SnapshotError::Json)?;

        fs::write(&file_path, json_data).await
            .map_err(SnapshotError::Io)?;

        Ok(())
    }

    /// Load snapshot metadata
    pub async fn load_metadata(&self, snapshot_id: &SnapshotId) -> SnapshotResult<SnapshotMetadata> {
        let file_path = self.get_metadata_path(snapshot_id);
        
        if !file_path.exists() {
            return Err(SnapshotError::SnapshotNotFound {
                id: snapshot_id.to_string(),
            });
        }

        let json_data = fs::read(&file_path).await
            .map_err(SnapshotError::Io)?;

        let metadata: SnapshotMetadata = serde_json::from_slice(&json_data)
            .map_err(SnapshotError::Json)?;

        Ok(metadata)
    }

    /// Delete snapshot metadata
    pub async fn delete_metadata(&self, snapshot_id: &SnapshotId) -> SnapshotResult<()> {
        let file_path = self.get_metadata_path(snapshot_id);
        
        if file_path.exists() {
            fs::remove_file(&file_path).await
                .map_err(SnapshotError::Io)?;
        }

        Ok(())
    }

    /// List all metadata files
    pub async fn list_metadata(&self) -> SnapshotResult<Vec<SnapshotId>> {
        let mut snapshot_ids = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await
            .map_err(SnapshotError::Io)?;

        while let Some(entry) = entries.next_entry().await
            .map_err(SnapshotError::Io)? {
            
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            
            if file_name_str.ends_with(".meta") {
                let id_str = file_name_str.trim_end_matches(".meta");
                if let Ok(snapshot_id) = SnapshotId::from_string(id_str) {
                    snapshot_ids.push(snapshot_id);
                }
            }
        }

        Ok(snapshot_ids)
    }

    /// Get metadata file path
    fn get_metadata_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.base_path.join(format!("{}.meta", snapshot_id))
    }
}
