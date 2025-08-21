// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

//! Core integration module for mgo-snapshot with mgo-core
//! 
//! This module provides deep integration with mgo-core's internal storage systems,
//! enabling direct access to database tables and state management components.

pub mod atomic_operations;
pub mod data_adapters;
pub mod database_accessor;
pub mod state_serializer;
pub mod state_applier;
pub mod state_writer;
pub mod validation;

pub use atomic_operations::*;
pub use data_adapters::{EnhancedDatabaseAccessor, DatabaseStatistics, TransactionIterator};
pub use data_adapters::ObjectIterator as EnhancedObjectIterator;
pub use database_accessor::*;
pub use state_serializer::*;
pub use state_applier::*;
pub use state_writer::*;
pub use validation::*;
