# Rollback Module

## Overview

The Rollback Module provides production-grade rollback capabilities for the Mango blockchain system. It enables safe and efficient state recovery operations when network inconsistencies, consensus failures, or other critical issues are detected.

## Core Features

### 🔄 State Rollback Management
- **Consensus Index Analysis**: Comprehensive analysis of consensus state across multiple data sources
- **Multi-Source Data Queries**: Production-grade queries across epoch store, checkpoint store, authority state, and database
- **Intelligent Caching**: Advanced in-memory, persistent, backup, and distributed caching strategies
- **Performance Monitoring**: Real-time metrics collection and analysis for rollback operations

### 📊 Health Monitoring & Diagnostics
- **System State Assessment**: Comprehensive system health checks and stability analysis
- **Consistency Validation**: Multi-layer consistency verification across network components
- **Quick Health Checks**: Fast responsiveness and basic functionality validation
- **Automatic Recovery**: Intelligent cleanup and state repair mechanisms

### 🎯 Rollback Execution
- **Storage Management**: Dynamic storage space assessment and requirement estimation
- **Transaction Cleanup**: Safe cleanup of uncommitted transactions during rollback
- **Multi-Phase Rollback**: Structured rollback process with validation at each stage
- **Fallback Strategies**: Multiple recovery strategies for different failure scenarios

## Architecture

### Module Structure

```
rollback/
├── mod.rs              # Module exports and public interface
├── types.rs            # Core data structures and type definitions
├── analysis.rs         # Core analysis logic and data operations
├── consensus.rs        # Consensus-related rollback operations
├── health.rs           # Health monitoring and diagnostics
├── manager.rs          # Main rollback manager implementation
└── tests.rs           # Unit and integration tests
```

### Key Components

#### RollbackAnalysis
The core analysis engine that handles:
- Multi-source consensus index queries
- Message counting and analysis (processed, pending, consensus logs, authority queues)
- Advanced caching operations with LRU eviction and TTL management
- Data validation and integrity checks

#### RollbackHealth
Health monitoring system that provides:
- Comprehensive system state gathering
- Quick responsiveness checks
- Consistency validation across components
- Automated cleanup of inconsistent state

#### RollbackManager
The main coordinator that orchestrates:
- Storage space availability checks
- Rollback requirement estimation
- Transaction cleanup operations
- Integration with other system components

## Technical Specifications

### Data Sources Integration
- **Epoch Store**: Current epoch information and transitions
- **Checkpoint Store**: Verified checkpoint data and metadata
- **Authority State**: Validator state and consensus participation
- **Database**: Persistent storage layer with transaction logs

### Caching Strategy
```rust
// Multi-layer caching architecture
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   In-Memory     │ => │   Persistent     │ => │   Distributed   │
│   Cache (LRU)   │    │   Cache (Disk)   │    │   Cache (Redis) │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Performance Metrics
- **Cache Hit Ratios**: Memory, persistent, and distributed cache performance
- **Query Latencies**: Real-time monitoring of data source response times
- **Storage Utilization**: Dynamic tracking of storage requirements
- **Network Efficiency**: Bandwidth and connectivity analysis

## Usage Examples

### Basic Rollback Operation

```rust
use mgo_core::rollback::{RollbackManager, RollbackConfig};

// Initialize rollback manager
let manager = RollbackManager::new(
    checkpoint_store,
    authority_state,
    network_client,
).await?;

// Check rollback feasibility
let storage_available = manager.check_storage_space_availability().await?;
if !storage_available {
    return Err("Insufficient storage space for rollback".into());
}

// Estimate requirements
let requirements = manager.estimate_rollback_storage_requirements().await?;
println!("Rollback will require {} GB", requirements.total_size_gb);

// Execute rollback
manager.perform_rollback(target_checkpoint).await?;
```

### Health Monitoring

```rust
use mgo_core::rollback::RollbackHealth;

// Initialize health monitor
let health = RollbackHealth::new(analysis_engine);

// Perform comprehensive health check
let system_state = health.gather_comprehensive_system_state().await?;
println!("System health: {:?}", system_state.overall_health);

// Quick responsiveness check
let is_responsive = health.quick_health_check().await?;
if !is_responsive {
    // Trigger emergency procedures
    health.cleanup_inconsistent_state().await?;
}
```

### Advanced Analysis

```rust
use mgo_core::rollback::RollbackAnalysis;

// Initialize analysis engine
let analysis = RollbackAnalysis::new(config);

// Query multiple data sources
let epoch_data = analysis.query_epoch_store().await?;
let checkpoint_data = analysis.query_checkpoint_store().await?;
let authority_data = analysis.query_authority_state().await?;

// Analyze message counts
let processed_count = analysis.get_processed_message_count().await?;
let pending_count = analysis.get_pending_message_count().await?;

// Cache management
analysis.store_in_memory_cache(key, data).await?;
let cached_data = analysis.get_stored_cache_entry(key).await?;
```

## Configuration

### Environment Variables

```bash
# Rollback operation settings
ROLLBACK_MAX_RETRY_ATTEMPTS=3
ROLLBACK_TIMEOUT_SECONDS=300
ROLLBACK_STORAGE_THRESHOLD_GB=100

# Cache configuration
CACHE_TTL_SECONDS=3600
CACHE_MAX_SIZE_MB=512
CACHE_EVICTION_STRATEGY=LRU

# Health monitoring
HEALTH_CHECK_INTERVAL_SECONDS=30
HEALTH_ALERT_THRESHOLD=0.8
```

### Configuration Structures

```rust
pub struct RollbackConfig {
    pub max_retry_attempts: u32,
    pub operation_timeout: Duration,
    pub storage_threshold: u64,
    pub enable_distributed_cache: bool,
    pub cache_ttl: Duration,
}

pub struct HealthConfig {
    pub check_interval: Duration,
    pub alert_threshold: f64,
    pub enable_auto_cleanup: bool,
    pub consistency_validation: bool,
}
```

## Error Handling

The module provides comprehensive error handling with detailed error types:

```rust
pub enum RollbackError {
    InsufficientStorage { required: u64, available: u64 },
    ConsensusInconsistency { details: String },
    NetworkTimeout { operation: String, duration: Duration },
    CacheOperationFailed { cache_type: CacheType, operation: String },
    HealthCheckFailed { component: String, reason: String },
}
```

## Monitoring and Observability

### Metrics Collection
- Cache performance metrics (hit ratios, latencies)
- Storage utilization tracking
- Network connectivity analysis
- System health scores

### Logging Integration
All operations are thoroughly logged with appropriate levels:
- `DEBUG`: Detailed operation traces
- `INFO`: Major operation milestones
- `WARN`: Performance degradation alerts
- `ERROR`: Critical failures requiring intervention

## Performance Characteristics

### Scalability
- **Horizontal**: Supports distributed caching across multiple nodes
- **Vertical**: Efficient memory usage with intelligent eviction strategies
- **Storage**: Dynamic storage requirement assessment and optimization

### Latency Targets
- Cache operations: < 1ms
- Local queries: < 10ms
- Network queries: < 100ms
- Full rollback: < 5 minutes (depending on data size)

## Security Considerations

- **Data Integrity**: Cryptographic validation of all cached and stored data
- **Access Control**: Role-based access to rollback operations
- **Audit Trail**: Complete logging of all rollback operations
- **Safe Rollback**: Multi-phase validation to prevent data corruption

## Integration Points

### Dependencies
- `mgo-types`: Core blockchain type definitions
- `mgo-storage`: Storage layer integration
- `tokio`: Async runtime for concurrent operations
- `anyhow`: Error handling and propagation

### External Systems
- Checkpoint storage systems
- Authority consensus networks
- Distributed cache clusters (Redis, etc.)
- Monitoring and alerting systems

## Development and Testing

### Running Tests
```bash
# Unit tests
cargo test -p mgo-core --lib rollback

# Integration tests
cargo test -p mgo-core --test rollback_integration

# Performance benchmarks
cargo bench -p mgo-core rollback_bench
```

### Contributing
1. Ensure all tests pass
2. Add appropriate logging for new operations
3. Update metrics collection for new features
4. Follow the existing error handling patterns
5. Document any new configuration options

## Future Enhancements

- **Parallel Rollback**: Multi-threaded rollback operations
- **Predictive Analysis**: ML-based rollback requirement prediction
- **Cross-Chain Rollback**: Support for multi-chain rollback scenarios
- **Real-time Sync**: Live state synchronization during rollback operations
