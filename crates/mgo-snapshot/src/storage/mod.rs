// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Storage backend implementations for snapshot data

pub mod local;
pub mod distributed;
pub mod compression;
pub mod encryption;

pub use local::LocalSnapshotStorage;
pub use compression::{CompressionEngine, Compressor};
pub use encryption::{EncryptionEngine, Encryptor};

// Re-export storage traits from types
pub use crate::types::storage::{SnapshotStorage, DeltaStorage, DistributedConsensus};
