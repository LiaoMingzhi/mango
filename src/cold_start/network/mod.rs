// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Network operations for cold start functionality
//! 
//! This module provides comprehensive network operations for cold starting nodes,
//! including node discovery, address resolution, connectivity testing, and state synchronization.

// Core network operations and types
pub mod types;
pub mod operations;

// Specialized functional modules
pub mod discovery;
pub mod connectivity;
pub mod address_resolution;
pub mod dns_resolution;
pub mod configuration;
pub mod checkpoint_sync;
pub mod responsiveness;

// Re-export main types and operations
pub use types::*;
pub use operations::NetworkOperations;

// Re-export key functionality from submodules
pub use discovery::{NodeDiscovery, DiscoveryStrategy};
pub use connectivity::{ConnectivityTester, ConnectivityMetrics};
pub use address_resolution::{AddressResolver, ResolutionStrategy};
pub use dns_resolution::{DnsResolver, DnsNamingConfig};
pub use configuration::{ConfigurationResolver, ConfigurationSource};
pub use checkpoint_sync::{CheckpointSynchronizer, SyncStrategy};
pub use responsiveness::{ResponsivenessChecker, ResponsivenessMetrics};
