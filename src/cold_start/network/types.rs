// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Core types and data structures for network operations

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use mgo_types::base_types::AuthorityName;
use mgo_types::messages_checkpoint::CheckpointSequenceNumber;

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

/// Comprehensive checkpoint synchronization analysis
#[derive(Debug, Clone)]
pub struct CheckpointSyncAnalysis {
    /// How many checkpoints behind network consensus
    pub lag_behind_consensus: u64,
    /// How many checkpoints behind local state
    pub lag_behind_local: u64,
    /// Gap between executed and certified checkpoints
    pub certified_gap: u64,
    /// Overall synchronization score (0.0 to 1.0)
    pub sync_score: f64,
    /// Estimated synchronization velocity (checkpoints per second)
    pub sync_velocity: f64,
    /// Whether the node is actively catching up
    pub is_catching_up: bool,
    /// Whether the node appears to be stalling
    pub is_stalling: bool,
    /// Data consistency score (0.0 to 1.0)
    pub consistency_score: f64,
}

/// Committee network data with metadata
#[derive(Debug, Clone)]
pub struct CommitteeNetworkData {
    /// Current epoch
    pub epoch: u64,
    /// Committee information
    pub committee: mgo_types::committee::Committee,
    /// Total stake in committee
    pub total_stake: u64,
    /// Number of committee members
    pub member_count: usize,
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
#[derive(Debug, Clone, PartialEq)]
pub enum NetworkProtocol {
    TCP,
    UDP,
    QUIC,
}

/// Address type classification
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

/// Node connection information
#[derive(Debug, Clone)]
pub struct NodeConnectionInfo {
    /// Node authority name
    pub authority: AuthorityName,
    /// Connection address
    pub address: String,
    /// Connection latency
    pub latency: Duration,
    /// Connection success rate
    pub success_rate: f64,
    /// Last connection attempt
    pub last_attempt: SystemTime,
    /// Connection status
    pub status: ConnectionStatus,
}

/// Connection status
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

/// Node responsiveness check result
#[derive(Debug, Clone)]
pub struct ResponsivenessCheckResult {
    pub check_type: String,
    pub success: bool,
    pub score: f64,
    pub latency: Duration,
    pub error_message: Option<String>,
    pub details: HashMap<String, String>,
    pub timestamp: SystemTime,
}

/// Address resolution result
#[derive(Debug, Clone)]
pub struct AddressResolutionResult {
    pub source: AddressResolutionSource,
    pub address: String,
    pub confidence: f64,
    pub resolution_time: Duration,
    pub validation_status: ValidationStatus,
    pub timestamp: SystemTime,
}

/// Address resolution source
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
#[derive(Debug, Clone)]
pub enum ValidationStatus {
    Pending,
    Passed,
    Failed,
    Skipped,
}

/// DNS resolution data
#[derive(Debug, Clone)]
pub struct DnsResolutionData {
    /// DNS query name
    pub query_name: String,
    /// Resolved addresses
    pub addresses: Vec<String>,
    /// Resolution time
    pub resolution_time: Duration,
    /// DNS server used
    pub dns_server: String,
    /// Record TTL
    pub ttl: Duration,
    /// Validation status
    pub is_valid: bool,
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

/// Configuration source types
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

/// Network operation metrics
#[derive(Debug, Clone)]
pub struct NetworkMetrics {
    /// Total operations performed
    pub operations_total: u64,
    /// Successful operations
    pub operations_successful: u64,
    /// Failed operations
    pub operations_failed: u64,
    /// Average operation duration
    pub average_duration: Duration,
    /// Last operation timestamp
    pub last_operation: SystemTime,
}

/// Anomaly severity levels
#[derive(Debug, Clone)]
pub enum AnomalySeverity {
    Low,
    Medium,
    High,
    Critical,
}
