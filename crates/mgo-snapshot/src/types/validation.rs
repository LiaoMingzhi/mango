// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Validation types and results for snapshot operations

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{ComponentType, SnapshotId};

/// Comprehensive validation result for snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Overall validation status
    pub valid: bool,
    /// Signature verification status
    pub signature_valid: bool,
    /// Data integrity verification status
    pub data_integrity: DataIntegrityResult,
    /// Business logic consistency status
    pub business_logic: BusinessLogicResult,
    /// Dependency verification status
    pub dependencies: DependencyResult,
    /// Validation timestamp
    pub validated_at: DateTime<Utc>,
    /// Validation duration in milliseconds
    pub duration_ms: u64,
    /// Validation warnings (non-critical issues)
    pub warnings: Vec<String>,
    /// Validation errors (critical issues)
    pub errors: Vec<String>,
}

impl ValidationResult {
    /// Create new validation result
    pub fn new() -> Self {
        Self {
            valid: true,
            signature_valid: true,
            data_integrity: DataIntegrityResult::new(),
            business_logic: BusinessLogicResult::new(),
            dependencies: DependencyResult::new(),
            validated_at: Utc::now(),
            duration_ms: 0,
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Add validation warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Add validation error
    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.valid = false;
    }

    /// Check if validation passed
    pub fn is_valid(&self) -> bool {
        self.valid
            && self.signature_valid
            && self.data_integrity.valid
            && self.business_logic.valid
            && self.dependencies.valid
    }

    /// Get total number of issues
    pub fn get_total_issues(&self) -> usize {
        self.errors.len() + self.warnings.len()
    }

    /// Get summary of validation issues
    pub fn get_issue_summary(&self) -> String {
        let mut summary = Vec::new();
        
        if !self.signature_valid {
            summary.push("Invalid signature".to_string());
        }
        
        if !self.data_integrity.valid {
            summary.push(format!("Data integrity issues: {}", self.data_integrity.issues.len()));
        }
        
        if !self.business_logic.valid {
            summary.push(format!("Business logic violations: {}", self.business_logic.violations.len()));
        }
        
        if !self.dependencies.valid {
            summary.push(format!("Missing dependencies: {}", self.dependencies.missing_dependencies.len()));
        }

        if summary.is_empty() {
            "All validations passed".to_string()
        } else {
            summary.join(", ")
        }
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Data integrity validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIntegrityResult {
    /// Overall integrity status
    pub valid: bool,
    /// Checksum verification results per component
    pub checksum_results: HashMap<ComponentType, bool>,
    /// Size verification results per component
    pub size_results: HashMap<ComponentType, bool>,
    /// Format verification results per component
    pub format_results: HashMap<ComponentType, bool>,
    /// Detected integrity issues
    pub issues: Vec<IntegrityIssue>,
}

impl DataIntegrityResult {
    /// Create new data integrity result
    pub fn new() -> Self {
        Self {
            valid: true,
            checksum_results: HashMap::new(),
            size_results: HashMap::new(),
            format_results: HashMap::new(),
            issues: Vec::new(),
        }
    }

    /// Add checksum result for component
    pub fn add_checksum_result(&mut self, component: ComponentType, valid: bool) {
        self.checksum_results.insert(component, valid);
        if !valid {
            self.valid = false;
            self.issues.push(IntegrityIssue {
                component,
                issue_type: IntegrityIssueType::ChecksumMismatch,
                description: format!("Checksum validation failed for {:?}", component),
                severity: IssueSeverity::Critical,
            });
        }
    }

    /// Add size result for component
    pub fn add_size_result(&mut self, component: ComponentType, valid: bool) {
        self.size_results.insert(component, valid);
        if !valid {
            self.valid = false;
            self.issues.push(IntegrityIssue {
                component,
                issue_type: IntegrityIssueType::SizeMismatch,
                description: format!("Size validation failed for {:?}", component),
                severity: IssueSeverity::Critical,
            });
        }
    }

    /// Add format result for component
    pub fn add_format_result(&mut self, component: ComponentType, valid: bool) {
        self.format_results.insert(component, valid);
        if !valid {
            self.valid = false;
            self.issues.push(IntegrityIssue {
                component,
                issue_type: IntegrityIssueType::FormatCorruption,
                description: format!("Format validation failed for {:?}", component),
                severity: IssueSeverity::Critical,
            });
        }
    }
}

impl Default for DataIntegrityResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Business logic consistency validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessLogicResult {
    /// Overall business logic validation status
    pub valid: bool,
    /// Epoch consistency check
    pub epoch_consistent: bool,
    /// Checkpoint sequence consistency check
    pub checkpoint_consistent: bool,
    /// Transaction consistency check
    pub transaction_consistent: bool,
    /// Object state consistency check
    pub object_state_consistent: bool,
    /// Detected business logic violations
    pub violations: Vec<BusinessLogicViolation>,
}

impl BusinessLogicResult {
    /// Create new business logic result
    pub fn new() -> Self {
        Self {
            valid: true,
            epoch_consistent: true,
            checkpoint_consistent: true,
            transaction_consistent: true,
            object_state_consistent: true,
            violations: Vec::new(),
        }
    }

    /// Add business logic violation
    pub fn add_violation(&mut self, violation: BusinessLogicViolation) {
        match violation.rule.as_str() {
            "epoch_consistency" => self.epoch_consistent = false,
            "checkpoint_consistency" => self.checkpoint_consistent = false,
            "transaction_consistency" => self.transaction_consistent = false,
            "object_state_consistency" => self.object_state_consistent = false,
            _ => {}
        }
        
        if violation.severity == IssueSeverity::Critical {
            self.valid = false;
        }
        
        self.violations.push(violation);
    }
}

impl Default for BusinessLogicResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Dependency validation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyResult {
    /// Overall dependency validation status
    pub valid: bool,
    /// List of missing dependencies
    pub missing_dependencies: Vec<String>,
    /// List of available dependencies
    pub available_dependencies: Vec<String>,
    /// Version compatibility results
    pub version_compatibility: HashMap<String, bool>,
}

impl DependencyResult {
    /// Create new dependency result
    pub fn new() -> Self {
        Self {
            valid: true,
            missing_dependencies: Vec::new(),
            available_dependencies: Vec::new(),
            version_compatibility: HashMap::new(),
        }
    }

    /// Add missing dependency
    pub fn add_missing_dependency(&mut self, dependency: String) {
        self.missing_dependencies.push(dependency);
        self.valid = false;
    }

    /// Add available dependency
    pub fn add_available_dependency(&mut self, dependency: String) {
        self.available_dependencies.push(dependency);
    }

    /// Add version compatibility result
    pub fn add_version_compatibility(&mut self, component: String, compatible: bool) {
        self.version_compatibility.insert(component, compatible);
        if !compatible {
            self.valid = false;
        }
    }
}

impl Default for DependencyResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Data integrity issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityIssue {
    /// Component where issue was detected
    pub component: ComponentType,
    /// Type of integrity issue
    pub issue_type: IntegrityIssueType,
    /// Human-readable description
    pub description: String,
    /// Severity level
    pub severity: IssueSeverity,
}

/// Types of data integrity issues
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityIssueType {
    ChecksumMismatch,
    SizeMismatch,
    FormatCorruption,
    MissingData,
    ExtraData,
    VersionMismatch,
}

/// Business logic violation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessLogicViolation {
    /// Business rule that was violated
    pub rule: String,
    /// Detailed description of violation
    pub details: String,
    /// Severity level
    pub severity: IssueSeverity,
    /// Suggested remediation
    pub remediation: Option<String>,
}

/// Issue severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl IssueSeverity {
    /// Check if severity is blocking
    pub fn is_blocking(&self) -> bool {
        matches!(self, Self::Error | Self::Critical)
    }
}

/// Repair operation results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairResult {
    /// Whether repair was successful
    pub success: bool,
    /// Type of repair performed
    pub repair_type: RepairType,
    /// Snapshot ID after repair
    pub repaired_snapshot_id: Option<SnapshotId>,
    /// Number of issues fixed
    pub issues_fixed: u32,
    /// Number of issues remaining
    pub issues_remaining: u32,
    /// Repair duration in milliseconds
    pub duration_ms: u64,
    /// Detailed repair log
    pub repair_log: Vec<String>,
}

impl RepairResult {
    /// Create successful repair result
    pub fn success(repair_type: RepairType, snapshot_id: SnapshotId) -> Self {
        Self {
            success: true,
            repair_type,
            repaired_snapshot_id: Some(snapshot_id),
            issues_fixed: 0,
            issues_remaining: 0,
            duration_ms: 0,
            repair_log: Vec::new(),
        }
    }

    /// Create failed repair result
    pub fn failure(repair_type: RepairType, reason: String) -> Self {
        Self {
            success: false,
            repair_type,
            repaired_snapshot_id: None,
            issues_fixed: 0,
            issues_remaining: 0,
            duration_ms: 0,
            repair_log: vec![format!("Repair failed: {}", reason)],
        }
    }

    /// Add repair log entry
    pub fn log_entry(&mut self, entry: String) {
        self.repair_log.push(entry);
    }
}

/// Types of repair operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairType {
    /// Repair from backup copies
    FromBackup,
    /// Reconstruct from adjacent snapshots
    Reconstruction,
    /// Partial repair of non-critical components
    Partial,
    /// Complete regeneration
    Regeneration,
}

/// Options for repair operations
#[derive(Debug, Clone, Default)]
pub struct RepairOptions {
    /// Available backup sources
    pub backup_sources: Option<Vec<SnapshotId>>,
    /// Allow reconstruction from adjacent snapshots
    pub allow_reconstruction: bool,
    /// Allow partial repair
    pub allow_partial_repair: bool,
    /// Maximum repair attempts
    pub max_attempts: u32,
    /// Repair timeout in seconds
    pub timeout_secs: u64,
    /// Custom repair parameters
    pub custom_params: HashMap<String, String>,
}

impl RepairOptions {
    /// Create new repair options with defaults
    pub fn new() -> Self {
        Self {
            backup_sources: None,
            allow_reconstruction: true,
            allow_partial_repair: true,
            max_attempts: 3,
            timeout_secs: 30 * 60, // 30 minutes
            custom_params: HashMap::new(),
        }
    }

    /// Set backup sources
    pub fn with_backup_sources(mut self, sources: Vec<SnapshotId>) -> Self {
        self.backup_sources = Some(sources);
        self
    }

    /// Set reconstruction allowance
    pub fn with_reconstruction(mut self, allow: bool) -> Self {
        self.allow_reconstruction = allow;
        self
    }

    /// Set partial repair allowance
    pub fn with_partial_repair(mut self, allow: bool) -> Self {
        self.allow_partial_repair = allow;
        self
    }

    /// Set maximum attempts
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }
}
