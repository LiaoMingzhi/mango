//! Snapshot compression functionality
//! 
//! This module implements compression for snapshot data.

use crate::types::{
    config::{CompressionType, CompressionPriority},
    error::{SnapshotError, SnapshotResult},
};
use crate::creator::CollectedStateData;


use std::io::{Read, Write};
use tracing::{debug, info, instrument};

/// Compression statistics
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub algorithm: CompressionType,
    pub level: i32,
    pub total_compressed_bytes: u64,
    pub total_original_bytes: u64,
    pub compression_ratio: f64,
}

/// Snapshot compressor for reducing snapshot size
/// 
/// Handles compression of collected state data using various algorithms.
pub struct SnapshotCompressor {
    /// Compression algorithm to use
    algorithm: CompressionType,
    
    /// Compression level (algorithm-specific)
    level: i32,
}

impl SnapshotCompressor {
    /// Create a new SnapshotCompressor
    pub fn new(compression_config: CompressionType) -> SnapshotResult<Self> {
        let level = match compression_config {
            CompressionType::None => 0,
            CompressionType::Zstd => 3,  // Default zstd level
            CompressionType::Lz4 => 0,  // LZ4 is fast by default
            CompressionType::Gzip => 6, // Default gzip level
        };
        
        Ok(Self {
            algorithm: compression_config,
            level,
        })
    }

    /// Create a new SnapshotCompressor with adaptive algorithm selection
    pub fn new_adaptive(data_size_hint: usize, priority: CompressionPriority) -> SnapshotResult<Self> {
        let algorithm = Self::select_optimal_algorithm(data_size_hint, priority);
        Self::new(algorithm)
    }

    /// Select optimal compression algorithm based on data characteristics and priorities
    pub fn select_optimal_algorithm(data_size: usize, priority: CompressionPriority) -> CompressionType {
        match priority {
            CompressionPriority::Speed => {
                if data_size < 1024 * 1024 {  // < 1MB, no compression overhead
                    CompressionType::None
                } else {
                    CompressionType::Lz4  // Fastest compression
                }
            }
            CompressionPriority::Balanced => {
                if data_size < 512 * 1024 {  // < 512KB
                    CompressionType::Lz4
                } else if data_size < 10 * 1024 * 1024 {  // < 10MB
                    CompressionType::Zstd
                } else {
                    CompressionType::Gzip  // Better ratio for large files
                }
            }
            CompressionPriority::Ratio => {
                if data_size < 256 * 1024 {  // < 256KB, not worth complex compression
                    CompressionType::Gzip
                } else {
                    CompressionType::Zstd  // Best ratio for large data
                }
            }
        }
    }

    /// Get compression statistics
    pub fn get_compression_stats(&self) -> CompressionStats {
        CompressionStats {
            algorithm: self.algorithm,
            level: self.level,
            total_compressed_bytes: 0, // Would be tracked in a real implementation
            total_original_bytes: 0,
            compression_ratio: 1.0,
        }
    }
    
    /// Compress collected state data
    #[instrument(level = "info", skip(self, data))]
    pub async fn compress(&self, data: &CollectedStateData) -> SnapshotResult<CollectedStateData> {
        if matches!(self.algorithm, CompressionType::None) {
            debug!("No compression requested, returning original data");
            return Ok(CollectedStateData {
                authority_state: data.authority_state.clone(),
                epoch_store: data.epoch_store.clone(),
                checkpoint_store: data.checkpoint_store.clone(),
                object_store: data.object_store.clone(),
                transaction_store: data.transaction_store.clone(),
                index_store: data.index_store.clone(),
                consensus_state: data.consensus_state.clone(),
                accumulator: data.accumulator.clone(),
                epoch: data.epoch,
                checkpoint_seq: data.checkpoint_seq,
                collection_time: data.collection_time,
            });
        }
        
        info!(algorithm = ?self.algorithm, level = self.level, "Starting data compression");
        
        let original_size = data.total_size();
        
        let compressed = CollectedStateData {
            authority_state: self.compress_optional_data(&data.authority_state).await?,
            epoch_store: self.compress_optional_data(&data.epoch_store).await?,
            checkpoint_store: self.compress_optional_data(&data.checkpoint_store).await?,
            object_store: self.compress_optional_data(&data.object_store).await?,
            transaction_store: self.compress_optional_data(&data.transaction_store).await?,
            index_store: self.compress_optional_data(&data.index_store).await?,
            consensus_state: self.compress_optional_data(&data.consensus_state).await?,
            accumulator: data.accumulator.clone(), // Don't compress accumulator
            epoch: data.epoch,
            checkpoint_seq: data.checkpoint_seq,
            collection_time: data.collection_time,
        };
        
        let compressed_size = compressed.total_size();
        let ratio = if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            1.0
        };
        
        info!(
            original_size = original_size,
            compressed_size = compressed_size,
            compression_ratio = ratio,
            "Data compression completed"
        );
        
        Ok(compressed)
    }
    
    /// Compress optional data field
    async fn compress_optional_data(&self, data: &Option<Vec<u8>>) -> SnapshotResult<Option<Vec<u8>>> {
        match data {
            Some(bytes) => {
                let compressed = self.compress_bytes(bytes).await?;
                Ok(Some(compressed))
            }
            None => Ok(None),
        }
    }
    
    /// Compress byte array using configured algorithm
    async fn compress_bytes(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        debug!(
            algorithm = ?self.algorithm,
            input_size = data.len(),
            "Compressing data chunk"
        );
        
        let result = match self.algorithm {
            CompressionType::None => data.to_vec(),
            CompressionType::Zstd => self.compress_zstd(data)?,
            CompressionType::Lz4 => self.compress_lz4(data)?,
            CompressionType::Gzip => self.compress_gzip(data)?,
        };
        
        debug!(
            input_size = data.len(),
            output_size = result.len(),
            ratio = result.len() as f64 / data.len() as f64,
            "Data chunk compression completed"
        );
        
        Ok(result)
    }
    
    /// Compress using ZSTD
    #[cfg(feature = "compression")]
    fn compress_zstd(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        zstd::bulk::compress(data, self.level)
            .map_err(|e| SnapshotError::Compression(format!("ZSTD compression failed: {}", e)))
    }
    
    /// Compress using ZSTD (fallback when feature disabled)
    #[cfg(not(feature = "compression"))]
    fn compress_zstd(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "ZSTD compression not available (feature disabled)".to_string()
        ))
    }
    
    /// Compress using LZ4
    #[cfg(feature = "compression")]
    fn compress_lz4(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }
    
    /// Compress using LZ4 (fallback when feature disabled)
    #[cfg(not(feature = "compression"))]
    fn compress_lz4(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "LZ4 compression not available (feature disabled)".to_string()
        ))
    }
    
    /// Compress using Gzip
    #[cfg(feature = "compression")]
    fn compress_gzip(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        use flate2::{Compression, write::GzEncoder};
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.level as u32));
        encoder.write_all(data)
            .map_err(|e| SnapshotError::Compression(format!("Gzip compression failed: {}", e)))?;
        
        encoder.finish()
            .map_err(|e| SnapshotError::Compression(format!("Gzip compression failed: {}", e)))
    }
    
    /// Compress using Gzip (fallback when feature disabled)
    #[cfg(not(feature = "compression"))]
    fn compress_gzip(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "Gzip compression not available (feature disabled)".to_string()
        ))
    }
    
    /// Decompress byte array (for validation)
    pub async fn decompress_bytes(&self, compressed_data: &[u8]) -> SnapshotResult<Vec<u8>> {
        if compressed_data.is_empty() {
            return Ok(Vec::new());
        }
        
        debug!(
            algorithm = ?self.algorithm,
            compressed_size = compressed_data.len(),
            "Decompressing data for validation"
        );
        
        let result = match self.algorithm {
            CompressionType::None => compressed_data.to_vec(),
            CompressionType::Zstd => self.decompress_zstd(compressed_data)?,
            CompressionType::Lz4 => self.decompress_lz4(compressed_data)?,
            CompressionType::Gzip => self.decompress_gzip(compressed_data)?,
        };
        
        debug!(
            compressed_size = compressed_data.len(),
            decompressed_size = result.len(),
            "Data decompression completed"
        );
        
        Ok(result)
    }
    
    /// Decompress using ZSTD
    #[cfg(feature = "compression")]
    fn decompress_zstd(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        zstd::bulk::decompress(data, 1024 * 1024 * 1024) // 1GB max
            .map_err(|e| SnapshotError::Compression(format!("ZSTD decompression failed: {}", e)))
    }
    
    /// Decompress using ZSTD (fallback)
    #[cfg(not(feature = "compression"))]
    fn decompress_zstd(&self, _data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "ZSTD decompression not available (feature disabled)".to_string()
        ))
    }
    
    /// Decompress using LZ4
    #[cfg(feature = "compression")]
    fn decompress_lz4(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(|e| SnapshotError::Compression(format!("LZ4 decompression failed: {}", e)))
    }
    
    /// Decompress using LZ4 (fallback)
    #[cfg(not(feature = "compression"))]
    fn decompress_lz4(&self, _data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "LZ4 decompression not available (feature disabled)".to_string()
        ))
    }
    
    /// Decompress using Gzip
    #[cfg(feature = "compression")]
    fn decompress_gzip(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        use flate2::read::GzDecoder;
        
        let mut decoder = GzDecoder::new(data);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result)
            .map_err(|e| SnapshotError::Compression(format!("Gzip decompression failed: {}", e)))?;
        
        Ok(result)
    }
    
    /// Decompress using Gzip (fallback)
    #[cfg(not(feature = "compression"))]
    fn decompress_gzip(&self, _data: &[u8]) -> SnapshotResult<Vec<u8>> {
        Err(SnapshotError::Compression(
            "Gzip decompression not available (feature disabled)".to_string()
        ))
    }
}
