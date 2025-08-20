//! REST API handlers implementation
//! 
//! This module implements the actual request handling logic for the snapshot API.

use axum::{
    http::StatusCode,
    response::{Json, IntoResponse},
};
use chrono::Utc;
use std::sync::Arc;
use tracing::{error, info, instrument, warn};

use crate::{
    api::types::*,
    manager::snapshot_manager::SnapshotManager,
    types::{
        SnapshotId,
        validation::ValidationResult as SnapshotValidationResult,
    },
};

/// Type alias for API result
type ApiResult<T> = Result<T, (StatusCode, Json<ApiErrorResponse>)>;

/// Create API error response
fn api_error(status: StatusCode, code: &str, message: &str) -> (StatusCode, Json<ApiErrorResponse>) {
    let error = ApiErrorResponse {
        error_code: code.to_string(),
        message: message.to_string(),
        details: None,
        request_id: None,
        timestamp: Utc::now(),
    };
    (status, Json(error))
}

/// Health check handler
#[instrument(skip(manager))]
pub async fn health_check_handler(
    manager: Arc<SnapshotManager>,
) -> ApiResult<Json<HealthResponse>> {
    info!("Processing health check request");
    
    // Check component health
    let mut components = std::collections::HashMap::new();
    
    // Check storage backend
    let storage_health = match manager.get_storage_health().await {
        Ok(healthy) => {
            if healthy {
                ComponentHealth {
                    status: HealthStatus::Healthy,
                    message: Some("Storage backend operational".to_string()),
                    last_check: Utc::now(),
                    metrics: None,
                }
            } else {
                ComponentHealth {
                    status: HealthStatus::Critical,
                    message: Some("Storage backend issues detected".to_string()),
                    last_check: Utc::now(),
                    metrics: None,
                }
            }
        }
        Err(e) => {
            warn!("Storage health check failed: {}", e);
            ComponentHealth {
                status: HealthStatus::Critical,
                message: Some(format!("Storage health check failed: {}", e)),
                last_check: Utc::now(),
                metrics: None,
            }
        }
    };
    
    components.insert("storage".to_string(), storage_health);
    
    // Check snapshot manager
    let manager_health = ComponentHealth {
        status: HealthStatus::Healthy,
        message: Some("Snapshot manager operational".to_string()),
        last_check: Utc::now(),
        metrics: None,
    };
    components.insert("snapshot_manager".to_string(), manager_health);
    
    // Determine overall status
    let overall_status = if components.values().any(|h| matches!(h.status, HealthStatus::Critical)) {
        HealthStatus::Critical
    } else if components.values().any(|h| matches!(h.status, HealthStatus::Warning)) {
        HealthStatus::Warning
    } else {
        HealthStatus::Healthy
    };
    
    let response = HealthResponse {
        status: overall_status,
        version: "0.1.0".to_string(),
        uptime: 0, // TODO: Implement actual uptime tracking
        components,
    };
    
    Ok(Json(response))
}

/// Create snapshot handler
#[instrument(skip(manager, request))]
pub async fn create_snapshot_handler(
    manager: Arc<SnapshotManager>,
    request: CreateSnapshotRequest,
) -> ApiResult<Json<CreateSnapshotResponse>> {
    info!("Processing create snapshot request: {:?}", request.snapshot_type);
    
    // Convert API request to internal request
    let internal_request = crate::manager::snapshot_manager::CreateSnapshotRequest {
        snapshot_type: request.snapshot_type,
        checkpoint_seq: request.checkpoint_seq,
        epoch: request.epoch,
        components: request.components,
        compress: request.compress,
        description: request.description.unwrap_or_default(),
        tags: request.tags,
    };
    
    match manager.create_snapshot(internal_request).await {
        Ok(snapshot_id) => {
            let response = CreateSnapshotResponse {
                snapshot_id,
                status: CreationStatus::Queued,
                progress: 0,
                estimated_completion: None,
                error: None,
            };
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to create snapshot: {}", e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SNAPSHOT_CREATION_FAILED",
                &format!("Failed to create snapshot: {}", e),
            ))
        }
    }
}

/// List snapshots handler
#[instrument(skip(manager, request))]
pub async fn list_snapshots_handler(
    manager: Arc<SnapshotManager>,
    request: ListSnapshotsRequest,
) -> ApiResult<Json<ListSnapshotsResponse>> {
    info!("Processing list snapshots request");
    
    let filter = request.filter.unwrap_or_default();
    match manager.list_snapshots(filter).await {
        Ok(snapshots) => {
            let limit = request.limit.unwrap_or(50) as usize;
            let offset = request.offset.unwrap_or(0) as usize;
            
            let total_count = snapshots.len() as u64;
            let end_index = std::cmp::min(offset + limit, snapshots.len());
            let paginated_snapshots = if offset < snapshots.len() {
                snapshots[offset..end_index].to_vec()
            } else {
                Vec::new()
            };
            
            let response = ListSnapshotsResponse {
                snapshots: paginated_snapshots,
                total_count,
                has_more: end_index < snapshots.len(),
                next_token: if end_index < snapshots.len() {
                    Some(end_index.to_string())
                } else {
                    None
                },
            };
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to list snapshots: {}", e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "SNAPSHOT_LIST_FAILED",
                &format!("Failed to list snapshots: {}", e),
            ))
        }
    }
}

/// Get snapshot details handler
#[instrument(skip(manager, snapshot_id, _params))]
pub async fn get_snapshot_handler(
    manager: Arc<SnapshotManager>,
    snapshot_id: String,
    _params: GetSnapshotRequest,
) -> ApiResult<Json<GetSnapshotResponse>> {
    info!("Processing get snapshot request for ID: {}", snapshot_id);
    
    let snapshot_id_str = snapshot_id.clone();
    let snapshot_id = SnapshotId::from_string(&snapshot_id);
    
    match snapshot_id {
        Ok(id) => match manager.get_snapshot_data(&id).await {
            Ok(data) => {
                let response = GetSnapshotResponse {
                    metadata: data.metadata,
                    validation_result: None, // TODO: Add validation result if requested
                    storage_stats: None,     // TODO: Add storage stats
                };
                Ok(Json(response))
            }
            Err(e) => {
                error!("Failed to get snapshot {}: {}", id, e);
                Err(api_error(
                    StatusCode::NOT_FOUND,
                    "SNAPSHOT_NOT_FOUND",
                    &format!("Snapshot not found: {}", e),
                ))
            }
        },
        Err(e) => {
            error!("Invalid snapshot ID {}: {}", snapshot_id_str, e);
            Err(api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_SNAPSHOT_ID",
                &format!("Invalid snapshot ID: {}", e),
            ))
        }
    }
}

/// Delete snapshot handler
#[instrument(skip(manager, snapshot_id, _request))]
pub async fn delete_snapshot_handler(
    manager: Arc<SnapshotManager>,
    snapshot_id: String,
    _request: DeleteSnapshotRequest,
) -> ApiResult<Json<DeleteSnapshotResponse>> {
    info!("Processing delete snapshot request for ID: {}", snapshot_id);
    
    let snapshot_id_str = snapshot_id.clone();
    let snapshot_id = SnapshotId::from_string(&snapshot_id);
    
    match snapshot_id {
        Ok(id) => match manager.delete_snapshot(id.clone()).await {
            Ok(_) => {
                let response = DeleteSnapshotResponse {
                    success: true,
                    space_freed: None, // TODO: Calculate space freed
                    error: None,
                };
                Ok(Json(response))
            }
            Err(e) => {
                error!("Failed to delete snapshot {}: {}", id, e);
                Err(api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SNAPSHOT_DELETE_FAILED",
                    &format!("Failed to delete snapshot: {}", e),
                ))
            }
        },
        Err(e) => {
            error!("Invalid snapshot ID {}: {}", snapshot_id_str, e);
            Err(api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_SNAPSHOT_ID",
                &format!("Invalid snapshot ID: {}", e),
            ))
        }
    }
}

/// Restore snapshot handler
#[instrument(skip(manager, snapshot_id, request))]
pub async fn restore_snapshot_handler(
    manager: Arc<SnapshotManager>,
    snapshot_id: String,
    request: RestoreSnapshotRequest,
) -> ApiResult<Json<RestoreSnapshotResponse>> {
    info!("Processing restore snapshot request for ID: {}", snapshot_id);
    
    let snapshot_id_str = snapshot_id.clone();
    let snapshot_id = SnapshotId::from_string(&snapshot_id);
    
    match snapshot_id {
        Ok(id) => {
            // Convert API request to internal request
            let internal_request = crate::types::RestoreSnapshotRequest {
                snapshot_id: id.clone(),
                validation_level: request.validation_level,
                create_backup: request.create_backup,
                force_restore: request.force,
                max_retries: 3, // Default value
                timeout_seconds: 300, // Default value
            };
            
            match manager.restore_snapshot(internal_request).await {
                Ok(result) => {
                    let response = RestoreSnapshotResponse {
                        operation_id: result.operation_id,
                        status: RestoreStatus::Queued,
                        progress: 0,
                        backup_snapshot_id: result.backup_snapshot_id,
                        validation_result: None,
                        error: None,
                    };
                    Ok(Json(response))
                }
                Err(e) => {
                    error!("Failed to restore snapshot {}: {}", id, e);
                    Err(api_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "SNAPSHOT_RESTORE_FAILED",
                        &format!("Failed to restore snapshot: {}", e),
                    ))
                }
            }
        },
        Err(e) => {
            error!("Invalid snapshot ID {}: {}", snapshot_id_str, e);
            Err(api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_SNAPSHOT_ID",
                &format!("Invalid snapshot ID '{}': {}", snapshot_id_str, e),
            ))
        }
    }
}

/// Verify snapshot handler
#[instrument(skip(manager, snapshot_id))]
pub async fn verify_snapshot_handler(
    manager: Arc<SnapshotManager>,
    snapshot_id: String,
) -> ApiResult<Json<SnapshotValidationResult>> {
    info!("Processing verify snapshot request for ID: {}", snapshot_id);
    
    let snapshot_id_str = snapshot_id.clone();
    let snapshot_id = SnapshotId::from_string(&snapshot_id);
    
    match snapshot_id {
        Ok(id) => match manager.verify_snapshot(id.clone(), true).await {
            Ok(result) => Ok(Json(result)),
            Err(e) => {
                error!("Failed to verify snapshot {}: {}", id, e);
                Err(api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "SNAPSHOT_VERIFY_FAILED",
                    &format!("Failed to verify snapshot: {}", e),
                ))
            }
        },
        Err(e) => {
            error!("Invalid snapshot ID {}: {}", snapshot_id_str, e);
            Err(api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_SNAPSHOT_ID",
                &format!("Invalid snapshot ID: {}", e),
            ))
        }
    }
}

/// Download snapshot handler
#[instrument(skip(manager, snapshot_id))]
pub async fn download_snapshot_handler(
    manager: Arc<SnapshotManager>,
    snapshot_id: String,
) -> ApiResult<axum::response::Response> {
    info!("Processing download snapshot request for ID: {}", snapshot_id);
    
    let snapshot_id_str = snapshot_id.clone();
    let snapshot_id = SnapshotId::from_string(&snapshot_id);
    
    match snapshot_id {
        Ok(id) => match manager.get_snapshot_raw_data(&id).await {
            Ok(data) => {
                // Return binary data as response using axum Response
                let response = (
                    StatusCode::OK,
                    [
                        ("Content-Type", "application/octet-stream"),
                        ("Content-Disposition", &format!("attachment; filename=\"{}.snapshot\"", id)),
                    ],
                    data,
                ).into_response();
                Ok(response)
            }
            Err(e) => {
                error!("Failed to download snapshot {}: {}", id, e);
                Err(api_error(
                    StatusCode::NOT_FOUND,
                    "SNAPSHOT_DOWNLOAD_FAILED",
                    &format!("Failed to download snapshot: {}", e),
                ))
            }
        },
        Err(e) => {
            error!("Invalid snapshot ID {}: {}", snapshot_id_str, e);
            Err(api_error(
                StatusCode::BAD_REQUEST,
                "INVALID_SNAPSHOT_ID",
                &format!("Invalid snapshot ID: {}", e),
            ))
        }
    }
}

/// Get operation status handler
#[instrument(skip(manager, operation_id))]
pub async fn get_operation_status_handler(
    manager: Arc<SnapshotManager>,
    operation_id: String,
) -> ApiResult<Json<serde_json::Value>> {
    info!("Processing get operation status request for ID: {}", operation_id);
    
    match manager.get_operation_status(&operation_id).await {
        Ok(status) => {
            let response = serde_json::json!({
                "operation_id": operation_id,
                "status": status,
                "timestamp": Utc::now()
            });
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get operation status {}: {}", operation_id, e);
            Err(api_error(
                StatusCode::NOT_FOUND,
                "OPERATION_NOT_FOUND",
                &format!("Operation not found: {}", e),
            ))
        }
    }
}

/// Cancel operation handler
#[instrument(skip(manager, operation_id))]
pub async fn cancel_operation_handler(
    manager: Arc<SnapshotManager>,
    operation_id: String,
) -> ApiResult<Json<serde_json::Value>> {
    info!("Processing cancel operation request for ID: {}", operation_id);
    
    match manager.cancel_operation(&operation_id).await {
        Ok(_) => {
            let response = serde_json::json!({
                "operation_id": operation_id,
                "cancelled": true,
                "timestamp": Utc::now()
            });
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to cancel operation {}: {}", operation_id, e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "OPERATION_CANCEL_FAILED",
                &format!("Failed to cancel operation: {}", e),
            ))
        }
    }
}

/// Get metrics handler
#[instrument(skip(manager, _request))]
pub async fn get_metrics_handler(
    manager: Arc<SnapshotManager>,
    _request: GetMetricsRequest,
) -> ApiResult<Json<GetMetricsResponse>> {
    info!("Processing get metrics request");
    
    match manager.get_storage_info().await {
        Ok(storage_info) => {
            let response = GetMetricsResponse {
                total_snapshots: storage_info.total_snapshots,
                total_storage_used: storage_info.total_size_bytes,
                average_compression_ratio: storage_info.avg_compression_ratio,
                creation_rate: 0.0,    // TODO: Calculate rates
                restoration_rate: 0.0, // TODO: Calculate rates
                success_rate: 0.95,    // TODO: Calculate actual success rate
                active_operations: 0,  // TODO: Get active operations count
                storage_health: StorageHealthStatus::Healthy, // TODO: Map actual status
            };
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get metrics: {}", e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "METRICS_FAILED",
                &format!("Failed to get metrics: {}", e),
            ))
        }
    }
}

/// Get Prometheus metrics handler
#[instrument(skip(manager))]
pub async fn get_prometheus_metrics_handler(
    manager: Arc<SnapshotManager>,
) -> ApiResult<String> {
    info!("Processing get Prometheus metrics request");
    
    match manager.get_prometheus_metrics().await {
        Ok(metrics) => Ok(metrics),
        Err(e) => {
            error!("Failed to get Prometheus metrics: {}", e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "PROMETHEUS_METRICS_FAILED",
                &format!("Failed to get Prometheus metrics: {}", e),
            ))
        }
    }
}

/// Get system info handler
#[instrument(skip(_manager))]
pub async fn get_system_info_handler(
    _manager: Arc<SnapshotManager>,
) -> ApiResult<Json<serde_json::Value>> {
    info!("Processing get system info request");
    
    let info = serde_json::json!({
        "version": "0.1.0",
        "build_time": "2025-01-19T00:00:00Z",
        "commit_hash": "unknown",
        "snapshot_manager": {
            "active": true,
            "storage_backend": "local", // TODO: Get actual backend type
        },
        "system": {
            "uptime": 0, // TODO: Implement uptime tracking
            "memory_usage": 0, // TODO: Implement memory tracking
        }
    });
    
    Ok(Json(info))
}

/// Get storage status handler
#[instrument(skip(manager))]
pub async fn get_storage_status_handler(
    manager: Arc<SnapshotManager>,
) -> ApiResult<Json<serde_json::Value>> {
    info!("Processing get storage status request");
    
    match manager.get_storage_status().await {
        Ok(status) => {
            let response = serde_json::json!({
                "status": "healthy",
                "backends": status,
                "timestamp": Utc::now()
            });
            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get storage status: {}", e);
            Err(api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "STORAGE_STATUS_FAILED",
                &format!("Failed to get storage status: {}", e),
            ))
        }
    }
}
