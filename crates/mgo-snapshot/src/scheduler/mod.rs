// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Advanced automatic scheduler for snapshot operations
//!
//! This module provides intelligent scheduling capabilities that enhance the basic
//! schedule manager with adaptive algorithms, load-based scheduling, and smart
//! resource management.

pub mod smart_scheduler;
pub mod adaptive_scheduler;
pub mod resource_aware_scheduler;

pub use smart_scheduler::*;
pub use adaptive_scheduler::*;
pub use resource_aware_scheduler::*;
