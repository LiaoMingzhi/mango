//! Compatibility module for existing snapshot functionality
//! 
//! This module provides constants and types needed by the existing
//! uploader, writer, and reader modules for backward compatibility.

use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use object_store::path::Path;

// Magic bytes and constants
/// Number of magic bytes
pub const MAGIC_BYTES: usize = 4;
/// Magic number for manifest files
pub const MANIFEST_FILE_MAGIC: u32 = 0x00C0FFEE;
/// Magic number for object files
pub const OBJECT_FILE_MAGIC: u32 = 0x0000FEED;
/// Magic number for reference files
pub const REFERENCE_FILE_MAGIC: u32 = 0x0000BEEF;
/// Maximum file size in bytes (128MB)
pub const FILE_MAX_BYTES: usize = 128 * 1024 * 1024;
/// Size of object reference in bytes
pub const OBJECT_REF_BYTES: usize = 32;
/// Size of object ID in bytes
pub const OBJECT_ID_BYTES: usize = 32;
/// Size of sequence number in bytes
pub const SEQUENCE_NUM_BYTES: usize = 8;
/// Size of SHA3 hash in bytes
pub const SHA3_BYTES: usize = 32;

// Re-export items from mgo-storage and mgo-core
pub use mgo_storage::FileCompression;
pub use mgo_core::db_checkpoint_handler::{SUCCESS_MARKER, STATE_SNAPSHOT_COMPLETED_MARKER};

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u8)]
/// Type of snapshot file
pub enum FileType {
    /// Object data file
    Object = 0,
    /// Reference data file
    Reference,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
/// Metadata for a snapshot file
pub struct FileMetadata {
    /// Type of file
    pub file_type: FileType,
    /// Bucket number
    pub bucket_num: u32,
    /// Part number
    pub part_num: u16,
    /// Number of objects in file
    pub num_objects: u64,
    /// Epoch number
    pub epoch_num: u64,
    /// SHA3 digest of file
    pub sha3_digest: [u8; 32],
    /// File compression type
    pub file_compression: mgo_storage::FileCompression,
}

impl FileMetadata {
    /// Get the file path for this metadata
    pub fn file_path(&self) -> Path {
        let dir_path = Path::from(format!("epoch_{}", self.epoch_num));
        match self.file_type {
            FileType::Object => dir_path.child(&*format!("{}_{}.obj", self.bucket_num, self.part_num)),
            FileType::Reference => dir_path.child(&*format!("{}_{}.ref", self.bucket_num, self.part_num)),
        }
    }

    /// Get the local file path
    pub fn local_file_path(&self, root_path: &std::path::Path, dir_path: &Path) -> anyhow::Result<std::path::PathBuf> {
        use mgo_storage::object_store::util::path_to_filesystem;
        let file_path_str = self.file_path().to_string();
        let full_path = dir_path.child(&*file_path_str);
        path_to_filesystem(root_path.to_path_buf(), &full_path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
/// Manifest version 1 structure
pub struct ManifestV1 {
    /// Snapshot format version
    pub snapshot_version: u8,
    /// Length of addresses
    pub address_length: u64,
    /// Metadata for all files in the snapshot
    pub file_metadata: Vec<FileMetadata>,
    /// Epoch number for this snapshot
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
/// Snapshot manifest enum
pub enum Manifest {
    /// Version 1 manifest
    V1(ManifestV1),
}

impl Manifest {
    /// Get the manifest version
    pub fn version(&self) -> u8 {
        match self {
            Manifest::V1(_) => 1,
        }
    }

    /// Get the snapshot version
    pub fn snapshot_version(&self) -> u8 {
        match self {
            Manifest::V1(v1) => v1.snapshot_version,
        }
    }

    /// Get the address length
    pub fn address_length(&self) -> u64 {
        match self {
            Manifest::V1(v1) => v1.address_length,
        }
    }

    /// Get the epoch number
    pub fn epoch(&self) -> u64 {
        match self {
            Manifest::V1(v1) => v1.epoch,
        }
    }

    /// Get the file metadata
    pub fn file_metadata(&self) -> &Vec<FileMetadata> {
        match self {
            Manifest::V1(v1) => &v1.file_metadata,
        }
    }
}

/// Compute SHA3 checksum of a file
pub fn compute_sha3_checksum(file_path: &std::path::Path) -> anyhow::Result<[u8; 32]> {
    use fastcrypto::hash::{HashFunction, Sha3_256};
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    
    let mut hasher = Sha3_256::default();
    hasher.update(&buffer);
    Ok(hasher.finalize().digest)
}

/// Create file metadata for a given file
pub fn create_file_metadata(
    file_path: &std::path::Path,
    file_compression: mgo_storage::FileCompression,
    file_type: FileType,
    bucket_num: u32,
    part_num: u16,
    epoch_num: u64,
) -> anyhow::Result<FileMetadata> {
    let sha3_digest = compute_sha3_checksum(file_path)?;
    Ok(FileMetadata {
        file_type,
        bucket_num,
        part_num,
        num_objects: 0, // This will be updated by caller
        epoch_num,
        sha3_digest,
        file_compression,
    })
}
