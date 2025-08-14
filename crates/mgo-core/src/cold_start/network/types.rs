// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Core types and data structures for network operations

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use mgo_types::base_types::AuthorityName;


/// Checkpoint synchronization data
#[derive(Debug, Clone)]
pub struct CheckpointSyncData {
    /// Highest executed checkpoint sequence number
    pub highest_executed: u64,
    /// Highest certified checkpoint sequence number
    pub highest_certified: u64,
    /// Timestamp of latest checkpoint
    pub latest_timestamp: u64,
}

/// Network address information with metadata
#[derive(Debug, Clone)]
pub struct NetworkAddressInfo {
    /// IP address
    pub ip: String,
    /// Port number
    pub port: u16,
    /// Network protocol
    pub protocol: NetworkProtocol,
    /// Address type/source
    pub address_type: AddressType,
    /// Geographic region
    pub region: String,
    /// Reliability score (0.0 to 1.0)
    pub reliability_score: f64,
    /// Last seen timestamp
    pub last_seen: SystemTime,
}

/// Network protocol types
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum NetworkProtocol {
    TCP,
    UDP,
    QUIC,
}

/// Address type classification
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum AddressType {
    Primary,
    Secondary,
    Metadata,
    Topology,
    Historical,
    LoadBalancer,
    ServiceDiscovery,
    Fallback,
}

/// Connection status
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
    Failed,
    Timeout,
}

/// Connectivity test result
#[derive(Debug, Clone)]
pub struct ConnectivityTestResult {
    /// Target authority
    pub authority: AuthorityName,
    /// Test success
    pub success: bool,
    /// Response time
    pub response_time: Duration,
    /// Error message if failed
    pub error: Option<String>,
    /// Test metadata
    pub metadata: HashMap<String, String>,
}

/// Address resolution source
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum AddressResolutionSource {
    CommitteeConfig,
    NetworkConfig,
    ServiceDiscovery,
    DnsResolution,
    CacheLookup,
    EnvironmentVariable,
    KubernetesConfigMap,
    ConsulKV,
    EtcdKV,
    DatabaseQuery,
    ManualOverride,
}

/// Validation status
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ValidationStatus {
    Pending,
    Passed,
    Failed,
    Skipped,
}

/// Configuration source types
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ConfigurationSourceType {
    LocalFile,
    NetworkUrl,
    ConsulKV,
    EtcdKV,
    KubernetesConfigMap,
    EnvironmentVariable,
    DatabaseQuery,
    GitRepository,
}

/// Configuration source information
#[derive(Debug, Clone)]
pub struct ConfigurationSource {
    /// Source type
    pub source_type: ConfigurationSourceType,
    /// Source location/path
    pub location: String,
    /// Authentication credentials if needed
    pub credentials: Option<String>,
    /// Source priority (higher = more preferred)
    pub priority: u32,
    /// Whether the source is currently available
    pub is_available: bool,
}
