// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Snapshot strategy management
//!
//! This module implements various snapshot strategies including hybrid approaches
//! that combine full and incremental snapshots for optimal storage and performance.

pub mod hybrid_strategy;
pub mod auto_cleanup;
pub mod schedule_manager;
pub mod intelligent_cleanup;
pub mod smart_cleanup_orchestrator;

pub use hybrid_strategy::*;
pub use auto_cleanup::*;
pub use schedule_manager::*;
pub use intelligent_cleanup::*;
pub use smart_cleanup_orchestrator::*;
