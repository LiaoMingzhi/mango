//! REST API module for snapshot management
//! 
//! This module provides HTTP REST API endpoints for snapshot operations.

pub mod types;
pub mod handlers;
pub mod server_simple;
pub mod routes;

pub use types::*;
pub use server_simple::{SnapshotApiServer, ApiServerConfig};
