//! Simplified REST API server implementation for compatibility
//! 
//! This module implements a simplified HTTP server for the snapshot API
//! that is compatible with older Rust toolchain versions.

use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::broadcast;
use tracing::{info, instrument};

use crate::{
    manager::snapshot_manager::SnapshotManager,
    types::error::SnapshotResult,
};

/// Configuration for the API server
#[derive(Debug, Clone)]
pub struct ApiServerConfig {
    /// Server bind address
    pub bind_address: SocketAddr,
    /// Request timeout in seconds
    pub request_timeout: Duration,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Enable CORS
    pub enable_cors: bool,
    /// Enable request tracing
    pub enable_tracing: bool,
}

/// Simplified snapshot API server
pub struct SnapshotApiServer {
    /// Server configuration
    config: ApiServerConfig,
    /// Snapshot manager instance
    snapshot_manager: Arc<SnapshotManager>,
    /// Shutdown signal sender
    shutdown_tx: Option<broadcast::Sender<()>>,
}

impl SnapshotApiServer {
    /// Create a new API server instance
    pub fn new(
        config: ApiServerConfig,
        snapshot_manager: Arc<SnapshotManager>,
    ) -> Self {
        Self {
            config,
            snapshot_manager,
            shutdown_tx: None,
        }
    }

    /// Start the API server (simplified implementation)
    #[instrument(skip(self))]
    pub async fn start(&mut self) -> SnapshotResult<()> {
        info!(
            "Starting simplified snapshot API server on {}",
            self.config.bind_address
        );

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        info!("Simplified API server would listen on {}", self.config.bind_address);
        info!("Note: This is a placeholder implementation for compatibility");
        info!("In a production environment, you would implement the full HTTP server");

        // Simplified implementation - just wait for shutdown
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("API server received shutdown signal");
            }
            _ = tokio::signal::ctrl_c() => {
                info!("API server received Ctrl+C signal");
            }
        }

        info!("Simplified API server stopped");
        Ok(())
    }

    /// Stop the API server
    pub async fn stop(&self) {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }
    }

    /// Get server configuration
    pub fn config(&self) -> &ApiServerConfig {
        &self.config
    }

    /// Get snapshot manager
    pub fn snapshot_manager(&self) -> &Arc<SnapshotManager> {
        &self.snapshot_manager
    }
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".parse().unwrap(),
            request_timeout: Duration::from_secs(30),
            max_body_size: 10 * 1024 * 1024, // 10MB
            enable_cors: true,
            enable_tracing: true,
        }
    }
}

/// Builder for API server configuration
pub struct ApiServerConfigBuilder {
    config: ApiServerConfig,
}

impl ApiServerConfigBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: ApiServerConfig::default(),
        }
    }

    /// Set bind address
    pub fn bind_address(mut self, addr: SocketAddr) -> Self {
        self.config.bind_address = addr;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set maximum body size
    pub fn max_body_size(mut self, size: usize) -> Self {
        self.config.max_body_size = size;
        self
    }

    /// Enable or disable CORS
    pub fn cors(mut self, enable: bool) -> Self {
        self.config.enable_cors = enable;
        self
    }

    /// Enable or disable request tracing
    pub fn tracing(mut self, enable: bool) -> Self {
        self.config.enable_tracing = enable;
        self
    }

    /// Build the configuration
    pub fn build(self) -> ApiServerConfig {
        self.config
    }
}

impl Default for ApiServerConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
