// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Node discovery functionality

use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow};
use tracing::{info, debug, warn};

use crate::authority::AuthorityState;
use crate::checkpoints::CheckpointStore;
use crate::authority_client::NetworkAuthorityClient;
use super::super::types::{ColdStartError, NetworkNode};
use super::super::metrics::ColdStartMetrics;
use super::types::*;

use mgo_types::base_types::AuthorityName;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

/// Discovery strategies
#[derive(Debug, Clone)]
pub enum DiscoveryStrategy {
    /// Committee-based discovery
    Committee,
    /// Service discovery
    ServiceDiscovery,
    /// Bootstrap nodes
    Bootstrap,
    /// Comprehensive (all methods)
    Comprehensive,
}

/// Node discovery implementation
pub struct NodeDiscovery {
    checkpoint_store: Arc<CheckpointStore>,
    authority_state: Arc<AuthorityState>,
    network_client: Arc<NetworkAuthorityClient>,
    metrics: Arc<ColdStartMetrics>,
}

impl NodeDiscovery {
    /// Create new node discovery instance
    pub fn new(
        checkpoint_store: Arc<CheckpointStore>,
        authority_state: Arc<AuthorityState>,
        network_client: Arc<NetworkAuthorityClient>,
        metrics: Arc<ColdStartMetrics>,
    ) -> Self {
        Self {
            checkpoint_store,
            authority_state,
            network_client,
            metrics,
        }
    }

    /// Discover healthy nodes in the network
    pub async fn discover_healthy_nodes(&self) -> Result<Vec<NetworkNode>> {
        info!("Starting comprehensive node discovery");
        
        let start_time = std::time::Instant::now();
        
        // Get committee members as potential nodes
        let committee_nodes = self.discover_committee_nodes().await?;
        info!("Discovered {} committee nodes", committee_nodes.len());
        
        // Perform health checks on discovered nodes
        let healthy_nodes = self.filter_healthy_nodes(committee_nodes).await?;
        
        if healthy_nodes.is_empty() {
            return Err(ColdStartError::NoHealthyNodesFound {
                scanned_nodes: 0,
            }.into());
        }
        
        let duration = start_time.elapsed();
        info!("Node discovery completed in {:?}, found {} healthy nodes", 
            duration, healthy_nodes.len());
        
        self.metrics.healthy_nodes_discovered.set(healthy_nodes.len() as i64);
        
        Ok(healthy_nodes)
    }

    /// Select best sync source from available nodes
    pub async fn select_best_sync_source(&self, nodes: &[NetworkNode]) -> Result<NetworkNode> {
        if nodes.is_empty() {
            return Err(anyhow!("No nodes available for sync source selection"));
        }

        info!("Selecting best sync source from {} candidates", nodes.len());

        // Filter healthy nodes
        let healthy_nodes: Vec<_> = nodes.iter()
            .filter(|node| node.is_healthy())
            .collect();

        if healthy_nodes.is_empty() {
            return Err(anyhow!("No healthy nodes available for sync"));
        }

        // Select node with best combination of health, latency, and checkpoint progress
        let best_node = healthy_nodes.iter()
            .min_by(|a, b| {
                // Primary: prefer nodes with higher checkpoint numbers
                let checkpoint_cmp = b.latest_checkpoint.unwrap_or(0)
                    .cmp(&a.latest_checkpoint.unwrap_or(0));
                
                if checkpoint_cmp != std::cmp::Ordering::Equal {
                    return checkpoint_cmp;
                }
                
                // Secondary: prefer nodes with lower latency
                a.latency().cmp(&b.latency())
            })
            .ok_or_else(|| anyhow!("Failed to select best node"))?;

        info!("Selected sync source: {} (latency: {:?}, checkpoint: {:?})", 
            best_node.name, best_node.latency(), best_node.latest_checkpoint);

        Ok((*best_node).clone())
    }

    /// Discover nodes from committee information
    async fn discover_committee_nodes(&self) -> Result<Vec<NetworkNode>> {
        debug!("Discovering nodes from committee information");
        
        // Get current committee from authority state
        let committee = self.authority_state.committee_store()
            .get_latest_committee()
            .map_err(|e| anyhow!("Failed to get committee: {}", e))?;

        let mut nodes = Vec::new();
        
        for (name, _) in committee.members() {
            // Generate address for committee member
            let address = self.generate_committee_member_address(name).await;
            
            let node = NetworkNode::new(
                *name,
                address,
                false, // Health will be checked later
                None,  // Checkpoint will be fetched later
                Duration::from_millis(0), // Latency will be measured later
            );
            
            nodes.push(node);
        }
        
        debug!("Generated {} nodes from committee", nodes.len());
        Ok(nodes)
    }

    /// Filter healthy nodes from candidate list
    async fn filter_healthy_nodes(&self, candidates: Vec<NetworkNode>) -> Result<Vec<NetworkNode>> {
        debug!("Filtering healthy nodes from {} candidates", candidates.len());
        
        let mut healthy_nodes = Vec::new();
        
        for mut node in candidates {
            match self.check_node_health(&node).await {
                Ok((is_healthy, latest_checkpoint, latency)) => {
                    node.is_healthy = is_healthy;
                    node.latest_checkpoint = latest_checkpoint;
                    node.latency = latency;
                    
                    if is_healthy {
                        healthy_nodes.push(node);
                    }
                }
                Err(e) => {
                    warn!("Failed to check health for node {}: {}", node.name, e);
                }
            }
        }
        
        debug!("Found {} healthy nodes", healthy_nodes.len());
        Ok(healthy_nodes)
    }

    /// Check individual node health
    async fn check_node_health(&self, node: &NetworkNode) -> Result<(bool, Option<CheckpointSequenceNumber>, Duration)> {
        let start = std::time::Instant::now();
        
        // Basic connectivity test
        let ping_result = self.ping_node(&node.address).await;
        let latency = start.elapsed();
        
        if ping_result.is_err() {
            debug!("Node {} failed ping test: {:?}", node.name, ping_result.err());
            return Ok((false, None, latency));
        }

        // Try to get latest checkpoint from node
        let checkpoint_result = self.get_node_latest_checkpoint(&node.name).await;
        
        match checkpoint_result {
            Ok(checkpoint) => {
                debug!("Node {} is healthy, latest checkpoint: {:?}", node.name, checkpoint);
                Ok((true, Some(checkpoint), latency))
            }
            Err(e) => {
                debug!("Node {} failed checkpoint query: {}", node.name, e);
                Ok((false, None, latency))
            }
        }
    }

    /// Generate address for committee member
    async fn generate_committee_member_address(&self, name: &AuthorityName) -> String {
        // For now, generate a predictable address based on authority name
        // In production, this would query actual network configuration
        format!("{}.validators.mango.network:9000", name)
    }

    /// Simple ping test to node
    async fn ping_node(&self, _address: &str) -> Result<()> {
        // Simulate basic connectivity test
        // In production, this would perform actual network connectivity test
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    /// Get latest checkpoint from specific node
    async fn get_node_latest_checkpoint(&self, name: &AuthorityName) -> Result<CheckpointSequenceNumber> {
        // Simulate getting checkpoint from node
        // In production, this would query the actual node
        debug!("Getting latest checkpoint from node {}", name);
        
        // For simulation, return a mock checkpoint number
        let checkpoint = CheckpointSequenceNumber::from(1000u64);
        Ok(checkpoint)
    }
}
