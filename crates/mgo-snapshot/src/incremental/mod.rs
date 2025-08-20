// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Incremental snapshot functionality
//!
//! This module implements incremental snapshots that only store changes
//! relative to a base snapshot, reducing storage requirements and creation time.

pub mod delta_computer;
pub mod delta_applier;
pub mod incremental_manager;

pub use delta_computer::*;
pub use delta_applier::*;
pub use incremental_manager::*;
