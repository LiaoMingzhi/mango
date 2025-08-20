// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Core types and data structures for the snapshot system

pub mod snapshot;
pub mod storage;
pub mod validation;
pub mod restore;
pub mod error;
pub mod config;

pub use snapshot::*;
pub use storage::*;
pub use validation::ValidationResult as SnapshotValidationResult;
pub use validation::{
    DataIntegrityResult, BusinessLogicResult, DependencyResult, IntegrityIssue,
    IntegrityIssueType, BusinessLogicViolation, IssueSeverity, RepairResult, RepairType, RepairOptions
};
pub use restore::{RestoreOptions, RestoreResult, RestoreSnapshotRequest};
pub use error::*;
pub use config::*;
