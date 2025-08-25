// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Mango blockchain snapshot-based rollback system

#![deny(unused_imports)]
#![deny(unused_variables)]
#![allow(missing_docs)] // TODO: Add comprehensive documentation
#![allow(dead_code)] // Allow unused code for development
#![allow(unused_assignments)] // Allow unused variable assignments during development
#![allow(unused_mut)] // Allow unnecessary mut during development

pub mod types;
pub mod storage;
pub mod manager;
pub mod creator;
pub mod restorer;
pub mod core_integration;
pub mod incremental;
pub mod strategy;
pub mod performance;
pub mod scheduler;
pub mod rollback;

// API and monitoring modules
#[cfg(feature = "api")]
pub mod api;

#[cfg(feature = "metrics")]
pub mod metrics;

// Existing modules for backward compatibility
pub mod uploader;
pub mod writer;
pub mod reader;

// Compatibility module with constants and types for existing modules
pub mod compat;

// Re-export compatibility items for existing code
pub use compat::*;

use anyhow::Result;
use fastcrypto::hash::MultisetHash;
use mgo_core::authority::authority_store_tables::AuthorityPerpetualTables;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::epoch::committee_store::CommitteeStore;
use mgo_types::accumulator::Accumulator;
use std::sync::Arc;

/// Setup database state after snapshot restoration
/// This function initializes the database state from the restored snapshot data
pub async fn setup_db_state(
    epoch: u64,
    root_accumulator: Accumulator,
    _perpetual_db: Arc<AuthorityPerpetualTables>,
    _checkpoint_store: Arc<CheckpointStore>,
    _committee_store: Arc<CommitteeStore>,
) -> Result<()> {
    // This is a compatibility function that ensures the database state is properly
    // initialized after snapshot restoration. In the new snapshot system, this would
    // be handled by the SnapshotRestorer, but for backward compatibility we provide
    // this function.
    
    // For now, this is a placeholder implementation
    // TODO: Implement proper state initialization logic
    tracing::info!(
        "Setting up database state for epoch {} with accumulator digest {:?}",
        epoch,
        root_accumulator.digest()
    );
    
    // The actual implementation would involve:
    // 1. Updating epoch stores with the restored state
    // 2. Ensuring consistency between different stores
    // 3. Setting up proper indexes and caches
    // 4. Validating the restored state

    Ok(())
}

// Re-export main types
pub use types::{
    SnapshotId, SnapshotData, SnapshotMetadata, SnapshotType,
    error::{SnapshotError, SnapshotResult},
    config::{SnapshotConfig, CompressionPriority},
};

pub use storage::{SnapshotStorage, LocalSnapshotStorage};
pub use manager::SnapshotManager;
pub use creator::compressor::CompressionStats;

// Re-export API and metrics types when features are enabled
#[cfg(feature = "api")]
pub use api::{SnapshotApiServer, ApiServerConfig};

#[cfg(feature = "metrics")]
pub use metrics::{PrometheusMetrics, MetricsCollector, MetricsRegistry};