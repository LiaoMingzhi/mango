//! REST API route definitions
//! 
//! This module defines the HTTP routes for the snapshot API.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
    Router,
};

use crate::api::{handlers, types::*};
use crate::manager::snapshot_manager::SnapshotManager;
use crate::types::SnapshotValidationResult;
use std::sync::Arc;

/// Application state shared across handlers
pub type AppState = Arc<SnapshotManager>;

/// Build the complete router with all API routes
pub fn build_router(snapshot_manager: Arc<SnapshotManager>) -> Router {
    Router::new()
        // Health check
        .route("/health", get(health_check))
        
        // Snapshot management
        .route("/snapshots", post(create_snapshot))
        .route("/snapshots", get(list_snapshots))
        .route("/snapshots/:id", get(get_snapshot))
        .route("/snapshots/:id", delete(delete_snapshot))
        
        // Snapshot operations
        .route("/snapshots/:id/restore", post(restore_snapshot))
        .route("/snapshots/:id/verify", post(verify_snapshot))
        .route("/snapshots/:id/download", get(download_snapshot))
        
        // Operations status
        .route("/operations/:id", get(get_operation_status))
        .route("/operations/:id/cancel", post(cancel_operation))
        
        // Metrics and monitoring
        .route("/metrics", get(get_metrics))
        .route("/metrics/prometheus", get(get_prometheus_metrics))
        
        // System information
        .route("/info", get(get_system_info))
        .route("/storage/status", get(get_storage_status))
        
        .with_state(snapshot_manager)
}

/// Health check endpoint
pub async fn health_check(
    State(manager): State<AppState>,
) -> Result<Json<HealthResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::health_check_handler(manager).await
}

/// Create a new snapshot
pub async fn create_snapshot(
    State(manager): State<AppState>,
    Json(request): Json<CreateSnapshotRequest>,
) -> Result<Json<CreateSnapshotResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::create_snapshot_handler(manager, request).await
}

/// List snapshots with optional filtering
pub async fn list_snapshots(
    State(manager): State<AppState>,
    Query(request): Query<ListSnapshotsRequest>,
) -> Result<Json<ListSnapshotsResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::list_snapshots_handler(manager, request).await
}

/// Get details of a specific snapshot
pub async fn get_snapshot(
    State(manager): State<AppState>,
    Path(snapshot_id): Path<String>,
    Query(params): Query<GetSnapshotRequest>,
) -> Result<Json<GetSnapshotResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_snapshot_handler(manager, snapshot_id, params).await
}

/// Delete a snapshot
pub async fn delete_snapshot(
    State(manager): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(request): Json<DeleteSnapshotRequest>,
) -> Result<Json<DeleteSnapshotResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::delete_snapshot_handler(manager, snapshot_id, request).await
}

/// Restore from a snapshot
pub async fn restore_snapshot(
    State(manager): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(request): Json<RestoreSnapshotRequest>,
) -> Result<Json<RestoreSnapshotResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::restore_snapshot_handler(manager, snapshot_id, request).await
}

/// Verify a snapshot
pub async fn verify_snapshot(
    State(manager): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<SnapshotValidationResult>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::verify_snapshot_handler(manager, snapshot_id).await
}

/// Download snapshot data
pub async fn download_snapshot(
    State(manager): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<axum::response::Response, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::download_snapshot_handler(manager, snapshot_id).await
}

/// Get operation status
pub async fn get_operation_status(
    State(manager): State<AppState>,
    Path(operation_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_operation_status_handler(manager, operation_id).await
}

/// Cancel an operation
pub async fn cancel_operation(
    State(manager): State<AppState>,
    Path(operation_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::cancel_operation_handler(manager, operation_id).await
}

/// Get system metrics
pub async fn get_metrics(
    State(manager): State<AppState>,
    Query(request): Query<GetMetricsRequest>,
) -> Result<Json<GetMetricsResponse>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_metrics_handler(manager, request).await
}

/// Get Prometheus metrics
pub async fn get_prometheus_metrics(
    State(manager): State<AppState>,
) -> Result<String, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_prometheus_metrics_handler(manager).await
}

/// Get system information
pub async fn get_system_info(
    State(manager): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_system_info_handler(manager).await
}

/// Get storage status
pub async fn get_storage_status(
    State(manager): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiErrorResponse>)> {
    handlers::get_storage_status_handler(manager).await
}
