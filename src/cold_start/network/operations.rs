// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Core network operations implementation

use std::sync::Arc;
use anyhow::Result;
use tracing::{info, debug};

use crate::authority::AuthorityState;
use crate::checkpoints::CheckpointStore;
use crate::authority_client::NetworkAuthorityClient;
use super::super::types::{ColdStartError, NetworkNode};
use super::super::metrics::ColdStartMetrics;
use super::types::*;
use super::discovery::NodeDiscovery;
use super::connectivity::ConnectivityTester;
use super::address_resolution::AddressResolver;
use super::dns_resolution::DnsResolver;
use super::configuration::ConfigurationResolver;
use super::checkpoint_sync::CheckpointSynchronizer;
use super::responsiveness::ResponsivenessChecker;

use mgo_types::base_types::AuthorityName;

/// NetworkOperations provides production-grade network operations for cold start
pub struct NetworkOperations {
    checkpoint_store: Arc<CheckpointStore>,
    authority_state: Arc<AuthorityState>,
    network_client: Arc<NetworkAuthorityClient>,
    metrics: Arc<ColdStartMetrics>,
    
    // Specialized components
    discovery: NodeDiscovery,
    connectivity: ConnectivityTester,
    address_resolver: AddressResolver,
    dns_resolver: DnsResolver,
    config_resolver: ConfigurationResolver,
    checkpoint_sync: CheckpointSynchronizer,
    responsiveness: ResponsivenessChecker,
}

impl NetworkOperations {
    /// Create new NetworkOperations instance
    pub fn new(
        checkpoint_store: Arc<CheckpointStore>,
        authority_state: Arc<AuthorityState>,
        network_client: Arc<NetworkAuthorityClient>,
        metrics: Arc<ColdStartMetrics>,
    ) -> Self {
        Self {
            checkpoint_store: checkpoint_store.clone(),
            authority_state: authority_state.clone(),
            network_client: network_client.clone(),
            metrics: metrics.clone(),
            
            // Initialize specialized components
            discovery: NodeDiscovery::new(
                checkpoint_store.clone(),
                authority_state.clone(),
                network_client.clone(),
                metrics.clone(),
            ),
            connectivity: ConnectivityTester::new(
                network_client.clone(),
                metrics.clone(),
            ),
            address_resolver: AddressResolver::new(
                authority_state.clone(),
                metrics.clone(),
            ),
            dns_resolver: DnsResolver::new(metrics.clone()),
            config_resolver: ConfigurationResolver::new(metrics.clone()),
            checkpoint_sync: CheckpointSynchronizer::new(
                checkpoint_store.clone(),
                authority_state.clone(),
                network_client.clone(),
                metrics.clone(),
            ),
            responsiveness: ResponsivenessChecker::new(
                network_client.clone(),
                metrics.clone(),
            ),
        }
    }

    /// Discover healthy nodes in the network
    pub async fn discover_healthy_nodes(&self) -> Result<Vec<NetworkNode>> {
        info!("Starting comprehensive node discovery");
        self.discovery.discover_healthy_nodes().await
    }

    /// Select best sync source from available nodes
    pub async fn select_best_sync_source(&self, nodes: &[NetworkNode]) -> Result<NetworkNode> {
        info!("Selecting best sync source from {} nodes", nodes.len());
        self.discovery.select_best_sync_source(nodes).await
    }

    /// Verify network connectivity
    pub async fn verify_network_connectivity(&self) -> Result<()> {
        info!("Verifying network connectivity");
        self.connectivity.verify_network_connectivity().await
    }

    /// Check node responsiveness
    pub async fn check_node_responsiveness(&self, name: &AuthorityName) -> String {
        debug!("Checking node responsiveness for {}", name);
        
        match self.responsiveness.check_responsiveness(name).await {
            Ok(result) => {
                info!("Node {} responsiveness check completed with score: {:.2}", 
                    name, result.overall_score);
                format!("Node {} is responsive (score: {:.2})", name, result.overall_score)
            }
            Err(e) => {
                debug!("Node {} responsiveness check failed: {}", name, e);
                format!("Node {} responsiveness check failed", name)
            }
        }
    }

    /// Get node network address
    pub async fn get_node_address(&self, name: &AuthorityName) -> String {
        debug!("Resolving network address for node {}", name);
        
        match self.address_resolver.resolve_address(name).await {
            Ok(address) => {
                debug!("Successfully resolved address for {}: {}", name, address);
                address
            }
            Err(e) => {
                debug!("Failed to resolve address for {}: {}", name, e);
                self.generate_fallback_address(name).await
            }
        }
    }

    /// Test TCP connectivity to a node
    pub async fn test_tcp_connectivity(&self, name: &AuthorityName, address: &str) -> Result<ConnectivityTestResult> {
        debug!("Testing TCP connectivity to {} at {}", name, address);
        self.connectivity.test_tcp_connectivity(name, address).await
    }

    /// Test RPC endpoint availability
    pub async fn test_rpc_endpoint_availability(&self, name: &AuthorityName, endpoint: &str) -> Result<bool> {
        debug!("Testing RPC endpoint availability for {} at {}", name, endpoint);
        self.connectivity.test_rpc_endpoint_availability(name, endpoint).await
    }

    /// Verify protocol compatibility
    pub async fn verify_protocol_compatibility(&self, name: &AuthorityName) -> Result<bool> {
        debug!("Verifying protocol compatibility for {}", name);
        self.connectivity.verify_protocol_compatibility(name).await
    }

    /// Test bidirectional communication
    pub async fn test_bidirectional_communication(&self, name: &AuthorityName) -> Result<bool> {
        debug!("Testing bidirectional communication with {}", name);
        self.connectivity.test_bidirectional_communication(name).await
    }

    /// Calculate dynamic sync threshold
    pub async fn calculate_dynamic_sync_threshold(&self, name: &AuthorityName) -> Result<f64> {
        debug!("Calculating dynamic sync threshold for {}", name);
        self.checkpoint_sync.calculate_dynamic_sync_threshold(name).await
    }

    /// Estimate sync velocity
    pub async fn estimate_sync_velocity(&self, name: &AuthorityName) -> Result<f64> {
        debug!("Estimating sync velocity for {}", name);
        self.checkpoint_sync.estimate_sync_velocity(name).await
    }

    /// Get latest checkpoint timestamp
    pub async fn get_latest_checkpoint_timestamp(&self, name: &AuthorityName) -> Result<u64> {
        debug!("Getting latest checkpoint timestamp for {}", name);
        self.checkpoint_sync.get_latest_checkpoint_timestamp(name).await
    }

    /// Resolve address from configuration files
    pub async fn resolve_from_configuration_files(&self, name: &AuthorityName) -> Result<String> {
        debug!("Resolving address from configuration files for {}", name);
        self.config_resolver.resolve_from_configuration_files(name).await
    }

    /// Resolve address from peer cache
    pub async fn resolve_from_peer_cache(&self, name: &AuthorityName) -> Result<String> {
        debug!("Resolving address from peer cache for {}", name);
        self.address_resolver.resolve_from_peer_cache(name).await
    }

    /// Generate DNS name for authority
    pub async fn generate_dns_name(&self, name: &AuthorityName) -> Result<Vec<String>> {
        debug!("Generating DNS names for {}", name);
        self.dns_resolver.generate_dns_name(name).await
    }

    /// Perform DNS lookup
    pub async fn perform_dns_lookup(&self, name: &AuthorityName) -> Result<String> {
        debug!("Performing DNS lookup for {}", name);
        self.dns_resolver.perform_dns_lookup(name).await
    }

    /// Get cached peer address
    pub async fn get_cached_peer_address(&self, name: &AuthorityName) -> Result<Option<String>> {
        debug!("Getting cached peer address for {}", name);
        self.address_resolver.get_cached_peer_address(name).await
    }

    /// Check if cache entry is fresh
    pub async fn is_cache_entry_fresh(&self, name: &AuthorityName, address: &str) -> Result<bool> {
        debug!("Checking cache entry freshness for {} at {}", name, address);
        self.address_resolver.is_cache_entry_fresh(name, address).await
    }

    /// Generate fallback address for a node
    async fn generate_fallback_address(&self, name: &AuthorityName) -> String {
        format!("{}@fallback-{}.mango.network:9000", name, name)
    }

    /// Get network metrics
    pub fn get_metrics(&self) -> NetworkMetrics {
        NetworkMetrics {
            operations_total: 0, // TODO: implement actual metrics tracking
            operations_successful: 0,
            operations_failed: 0,
            average_duration: std::time::Duration::from_millis(0),
            last_operation: std::time::SystemTime::now(),
        }
    }
}
