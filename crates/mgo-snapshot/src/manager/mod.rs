// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Snapshot manager and coordination logic

pub mod snapshot_manager;
pub mod metadata;
pub mod registry;

pub use snapshot_manager::SnapshotManager;
pub use metadata::SnapshotMetadataStore;
pub use registry::SnapshotRegistry;
