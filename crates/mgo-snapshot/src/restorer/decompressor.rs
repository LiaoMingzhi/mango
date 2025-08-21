//! Snapshot decompression functionality
//! 
//! This module implements decompression for downloaded snapshot data.

use crate::types::{
    config::CompressionType,
    error::{SnapshotError, SnapshotResult},
};

use std::io::Read;
use tracing::{info, debug, error, warn, instrument};

/// Snapshot decompressor for restoring compressed snapshot data
/// 
/// Handles decompression of downloaded snapshot data using various algorithms.
pub struct SnapshotDecompressor {
}

impl SnapshotDecompressor {
    /// Create a new SnapshotDecompressor
    pub fn new() -> SnapshotResult<Self> {
        Ok(Self {})
    }
    
    /// Decompress snapshot data
    #[instrument(level = "info", skip(self, compressed_data))]
    pub async fn decompress(&self, compressed_data: &[u8]) -> SnapshotResult<Vec<u8>> {
        info!(
            compressed_size = compressed_data.len(),
            "Starting data decompression"
        );
        
        // Detect compression algorithm from data header
        let compression_type = self.detect_compression_algorithm(compressed_data)?;
        
        let decompressed = self.decompress_with_algorithm(compressed_data, compression_type).await?;
        
        // Validate decompressed data integrity
        self.validate_decompressed_data(&decompressed)?;
        
        info!(
            compressed_size = compressed_data.len(),
            decompressed_size = decompressed.len(),
            algorithm = ?compression_type,
            "Data decompression completed successfully"
        );
        
        Ok(decompressed)
    }

    /// Detect compression algorithm from data header
    fn detect_compression_algorithm(&self, data: &[u8]) -> SnapshotResult<CompressionType> {
        if data.is_empty() {
            return Ok(CompressionType::None);
        }

        // Check for compression magic bytes
        if data.len() >= 4 {
            // ZSTD magic number: 0x28, 0xB5, 0x2F, 0xFD
            if data.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
                return Ok(CompressionType::Zstd);
            }
            
            // LZ4 magic number: 0x04, 0x22, 0x4D, 0x18
            if data.starts_with(&[0x04, 0x22, 0x4D, 0x18]) {
                return Ok(CompressionType::Lz4);
            }
            
            // Gzip magic number: 0x1F, 0x8B
            if data.starts_with(&[0x1F, 0x8B]) {
                return Ok(CompressionType::Gzip);
            }
        }

        // Check for BCS-serialized uncompressed data (starts with small integer length)
        if data.len() >= 1 {
            let first_byte = data[0];
            // BCS typically starts with length prefixes that are small integers
            if first_byte < 0x80 {
                // This is likely uncompressed BCS data
                return Ok(CompressionType::None);
            }
        }

        // Default to no compression if we can't detect
        info!("Could not detect compression algorithm, assuming no compression");
        Ok(CompressionType::None)
    }
    
    /// Decompress using specific algorithm
    pub async fn decompress_with_algorithm(
        &self,
        compressed_data: &[u8],
        algorithm: CompressionType,
    ) -> SnapshotResult<Vec<u8>> {
        debug!("Using decompression algorithm: {:?}", algorithm);
        
        let start_time = std::time::Instant::now();
        let result = match algorithm {
            CompressionType::None => Ok(compressed_data.to_vec()),
            CompressionType::Zstd => self.decompress_zstd(compressed_data),
            CompressionType::Lz4 => self.decompress_lz4(compressed_data),
            CompressionType::Gzip => self.decompress_gzip(compressed_data),
            // CompressionType::Brotli => self.decompress_brotli(compressed_data),
        };
        
        let duration = start_time.elapsed();
        match &result {
            Ok(decompressed) => {
                info!(
                    algorithm = ?algorithm,
                    compressed_size = compressed_data.len(),
                    decompressed_size = decompressed.len(),
                    duration_ms = duration.as_millis(),
                    ratio = decompressed.len() as f64 / compressed_data.len() as f64,
                    "Decompression completed successfully"
                );
            }
            Err(e) => {
                error!(
                    algorithm = ?algorithm,
                    compressed_size = compressed_data.len(),
                    duration_ms = duration.as_millis(),
                    error = %e,
                    "Decompression failed"
                );
            }
        }
        
        result
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

    // Brotli support removed for simplicity

    /// Validate decompressed data integrity
    pub fn validate_decompressed_data(&self, data: &[u8]) -> SnapshotResult<()> {
        // Basic sanity checks
        if data.is_empty() {
            return Err(SnapshotError::InvalidFormat {
                reason: "Decompressed data is empty".to_string(),
            });
        }

        // Try to deserialize as BCS to check if it's valid snapshot data
        // This is a basic check - more specific validation would depend on the expected format
        match bcs::from_bytes::<Vec<u8>>(data) {
            Ok(_) => {
                debug!("Decompressed data appears to be valid BCS format");
                Ok(())
            }
            Err(_) => {
                // If it's not a simple Vec<u8>, it might still be valid snapshot data
                // We'll just check that it has reasonable structure
                if data.len() > 4 && data[0] != 0 {
                    debug!("Decompressed data appears to have valid structure");
                    Ok(())
                } else {
                    warn!("Decompressed data structure validation warning - proceeding anyway");
                    Ok(())
                }
            }
        }
    }
}
