// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Core integration module for mgo-snapshot with mgo-core
//! 
//! This module provides deep integration with mgo-core's internal storage systems,
//! enabling direct access to database tables and state management components.

pub mod database_accessor;
pub mod state_serializer;
pub mod state_applier;
pub mod validation;

pub use database_accessor::*;
pub use state_serializer::*;
pub use state_applier::*;
pub use validation::*;
