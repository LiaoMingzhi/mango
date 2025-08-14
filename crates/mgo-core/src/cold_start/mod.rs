// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Cold start module for Mango Network
//! 
//! This module provides functionality for cold starting nodes in the network,
//! including node discovery, state synchronization, and consensus restart.

mod types;
mod metrics;
mod network;
mod state;
mod manager;

#[cfg(test)]
mod tests;

// Re-export public types and structs
pub use types::{
    ColdStartConfig, ColdStartResult, ColdStartPhase, ColdStartError,
    NetworkNode, NetworkState, ColdStartState,
};

pub use metrics::ColdStartMetrics;
pub use manager::ColdStartManager;

// Re-export internal modules for advanced usage
pub use network::NetworkOperations;
pub use state::StateOperations;
