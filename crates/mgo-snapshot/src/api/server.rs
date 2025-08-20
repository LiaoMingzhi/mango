//! REST API server implementation
//! 
//! This module implements the HTTP server for the snapshot API.

use axum::{
    extract::Request,
    middleware::{self, Next},
    response::Response,
    Router,
};
use hyper::{
    server::Server,
    service::{make_service_fn, service_fn},
    Body, Request, Response,
};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    signal,
    sync::broadcast,
    time::Instant,
};
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
    LatencyUnit, ServiceBuilderExt,
};
use tracing::{error, info, instrument, warn, Level};

use crate::{
    api::routes::{build_router, AppState},
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
    /// Rate limiting config
    pub rate_limit: Option<RateLimitConfig>,
}

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Time window in seconds
    pub window_seconds: u64,
}

/// Snapshot API server
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

    /// Start the API server
    #[instrument(skip(self))]
    pub async fn start(&mut self) -> SnapshotResult<()> {
        info!(
            "Starting snapshot API server on {}",
            self.config.bind_address
        );

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Build the router with middleware
        let app = self.build_app_with_middleware();

        info!("API server listening on {}", self.config.bind_address);

        // Create the service
        let make_svc = make_service_fn(move |_conn| {
            let app = app.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let app = app.clone();
                    async move {
                        app.oneshot(req).await.map_err(|err| {
                            error!("Service error: {}", err);
                            err
                        })
                    }
                }))
            }
        });

        // Start the server with graceful shutdown
        let server = Server::bind(&self.config.bind_address).serve(make_svc);
        
        tokio::select! {
            result = server => {
                match result {
                    Ok(_) => info!("API server stopped normally"),
                    Err(e) => error!("API server error: {}", e),
                }
            }
            _ = shutdown_rx.recv() => {
                info!("API server received shutdown signal");
            }
            _ = signal::ctrl_c() => {
                info!("API server received Ctrl+C signal");
            }
        }

        Ok(())
    }

    /// Stop the API server
    pub async fn stop(&self) {
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }
    }

    /// Build the application with middleware
    fn build_app_with_middleware(&self) -> Router {
        let mut app = build_router(self.snapshot_manager.clone());

        // Add middleware layers
        let service_builder = ServiceBuilder::new();

        // Add request timeout
        let service_builder = service_builder
            .timeout(self.config.request_timeout)
            .layer(middleware::from_fn(request_id_middleware));

        // Add CORS if enabled
        let service_builder = if self.config.enable_cors {
            service_builder.layer(
                CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(tower_http::cors::Any)
                    .allow_headers(tower_http::cors::Any),
            )
        } else {
            service_builder
        };

        // Add tracing if enabled
        let service_builder = if self.config.enable_tracing {
            service_builder.layer(
                TraceLayer::new_for_http()
                    .on_request(DefaultOnRequest::new().level(Level::INFO))
                    .on_response(
                        DefaultOnResponse::new()
                            .level(Level::INFO)
                            .latency_unit(LatencyUnit::Millis),
                    ),
            )
        } else {
            service_builder
        };

        // Add body size limit
        let service_builder = service_builder.layer(
            tower::limit::RequestBodyLimitLayer::new(self.config.max_body_size),
        );

        app.layer(service_builder)
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

/// Middleware to add request ID to each request
async fn request_id_middleware(
    mut request: Request,
    next: Next,
) -> Result<Response, Infallible> {
    let request_id = uuid::Uuid::new_v4().to_string();
    
    // Add request ID to headers
    request.headers_mut().insert(
        "X-Request-ID",
        request_id.parse().unwrap(),
    );

    // Add request start time
    let start_time = Instant::now();
    request.extensions_mut().insert(start_time);

    let mut response = next.run(request).await;

    // Add request ID to response headers
    response.headers_mut().insert(
        "X-Request-ID",
        request_id.parse().unwrap(),
    );

    Ok(response)
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".parse().unwrap(),
            request_timeout: Duration::from_secs(30),
            max_body_size: 10 * 1024 * 1024, // 10MB
            enable_cors: true,
            enable_tracing: true,
            rate_limit: Some(RateLimitConfig {
                max_requests: 100,
                window_seconds: 60,
            }),
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_seconds: 60,
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

    /// Set rate limiting configuration
    pub fn rate_limit(mut self, config: Option<RateLimitConfig>) -> Self {
        self.config.rate_limit = config;
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
