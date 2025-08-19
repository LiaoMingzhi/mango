// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Snapshot registry for tracking available snapshots

use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::types::{SnapshotId, SnapshotInfo};

/// In-memory registry of available snapshots
#[derive(Debug)]
pub struct SnapshotRegistry {
    /// Map of snapshot ID to snapshot info
    snapshots: HashMap<SnapshotId, SnapshotInfo>,
}

impl SnapshotRegistry {
    /// Create new empty registry
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    /// Register a new snapshot
    pub fn register_snapshot(&mut self, snapshot_info: SnapshotInfo) {
        self.snapshots.insert(snapshot_info.metadata.id.clone(), snapshot_info);
    }

    /// Unregister a snapshot
    pub fn unregister_snapshot(&mut self, snapshot_id: &SnapshotId) {
        self.snapshots.remove(snapshot_id);
    }

    /// Get snapshot info by ID
    pub fn get_snapshot(&self, snapshot_id: &SnapshotId) -> Option<&SnapshotInfo> {
        self.snapshots.get(snapshot_id)
    }

    /// List all snapshots
    pub fn list_snapshots(&self) -> Vec<SnapshotInfo> {
        self.snapshots.values().cloned().collect()
    }

    /// Update verification time for a snapshot
    pub fn update_verification_time(&mut self, snapshot_id: &SnapshotId, timestamp: DateTime<Utc>) {
        if let Some(snapshot_info) = self.snapshots.get_mut(snapshot_id) {
            snapshot_info.last_verified = Some(timestamp);
        }
    }

    /// Get number of snapshots
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Check if snapshot exists
    pub fn contains_snapshot(&self, snapshot_id: &SnapshotId) -> bool {
        self.snapshots.contains_key(snapshot_id)
    }
}

impl Default for SnapshotRegistry {
    fn default() -> Self {
        Self::new()
    }
}
