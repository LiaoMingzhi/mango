// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Mango blockchain snapshot-based rollback system

#![deny(unused_imports)]
#![deny(unused_variables)]
#![warn(missing_docs)]

pub mod types;
pub mod storage;
pub mod manager;

// Re-export main types
pub use types::{
    SnapshotId, SnapshotData, SnapshotMetadata, SnapshotType,
    error::{SnapshotError, SnapshotResult},
    config::SnapshotConfig,
};

pub use storage::{SnapshotStorage, LocalSnapshotStorage};
pub use manager::SnapshotManager;