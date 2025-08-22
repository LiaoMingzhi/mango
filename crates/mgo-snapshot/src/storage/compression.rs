// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Compression utilities for snapshot data

use std::io::{self, Read, Write};
use thiserror::Error;

use crate::types::{CompressionLevel, CompressionType};

/// Compression engine for snapshot data
#[derive(Debug, Clone)]
pub struct CompressionEngine {
    /// Compression algorithm to use
    compression_type: CompressionType,
    /// Compression level
    level: CompressionLevel,
}

impl CompressionEngine {
    /// Create new compression engine
    pub fn new(compression_type: CompressionType, level: CompressionLevel) -> Self {
        Self {
            compression_type,
            level,
        }
    }

    /// Compress data using configured algorithm
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match self.compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Zstd => self.compress_zstd(data),
            CompressionType::Lz4 => self.compress_lz4(data),
            CompressionType::Gzip => self.compress_gzip(data),
        }
    }

    /// Decompress data using configured algorithm
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        match self.compression_type {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Zstd => self.decompress_zstd(data),
            CompressionType::Lz4 => self.decompress_lz4(data),
            CompressionType::Gzip => self.decompress_gzip(data),
        }
    }

    /// Compress using ZSTD
    fn compress_zstd(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let level = self.zstd_level();
        zstd::bulk::compress(data, level)
            .map_err(|e| CompressionError::Zstd(e.to_string()))
    }

    /// Decompress using ZSTD
    fn decompress_zstd(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        zstd::bulk::decompress(data, 64 * 1024 * 1024) // 64MB max
            .map_err(|e| CompressionError::Zstd(e.to_string()))
    }

    /// Compress using LZ4
    fn compress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    /// Decompress using LZ4
    fn decompress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        lz4_flex::decompress_size_prepended(data)
            .map_err(CompressionError::Lz4Decompress)
    }

    /// Compress using Gzip
    fn compress_gzip(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        use flate2::{write::GzEncoder, Compression};
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)
            .map_err(CompressionError::Io)?;
        encoder.finish()
            .map_err(CompressionError::Io)
    }

    /// Decompress using Gzip
    fn decompress_gzip(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        use flate2::read::GzDecoder;
        
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(CompressionError::Io)?;
        Ok(decompressed)
    }

    /// Convert CompressionLevel to ZSTD level
    fn zstd_level(&self) -> i32 {
        match self.level {
            CompressionLevel::None => 0,
            CompressionLevel::Low => 1,
            CompressionLevel::Medium => 6,
            CompressionLevel::High => 15,
            CompressionLevel::Maximum => 22,
        }
    }

    /// Get compression type
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Get compression level
    pub fn compression_level(&self) -> CompressionLevel {
        self.level
    }
}

impl Default for CompressionEngine {
    fn default() -> Self {
        Self::new(CompressionType::Zstd, CompressionLevel::Medium)
    }
}

/// Compression trait for custom implementations
pub trait Compressor: Send + Sync {
    /// Compress data
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;
    
    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError>;
    
    /// Get compression algorithm name
    fn algorithm_name(&self) -> &'static str;
}

impl Compressor for CompressionEngine {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.compress(data)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        self.decompress(data)
    }

    fn algorithm_name(&self) -> &'static str {
        match self.compression_type {
            CompressionType::None => "none",
            CompressionType::Zstd => "zstd",
            CompressionType::Lz4 => "lz4",
            CompressionType::Gzip => "gzip",
        }
    }
}

/// Compression-related errors
#[derive(Error, Debug)]
pub enum CompressionError {
    /// ZSTD errors
    #[error("ZSTD error: {0}")]
    Zstd(String),

    /// LZ4 compression errors
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::block::CompressError),

    /// LZ4 decompression errors
    #[error("LZ4 decompression error: {0}")]
    Lz4Decompress(#[from] lz4_flex::block::DecompressError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Generic compression error
    #[error("Compression error: {0}")]
    Generic(String),
}

/// Streaming compressor for large data
pub struct StreamingCompressor {
    engine: CompressionEngine,
    chunk_size: usize,
}

impl StreamingCompressor {
    /// Create new streaming compressor
    pub fn new(engine: CompressionEngine, chunk_size: usize) -> Self {
        Self {
            engine,
            chunk_size,
        }
    }

    /// Compress data in streaming fashion
    pub fn compress_stream(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        if data.len() <= self.chunk_size {
            return self.engine.compress(data);
        }

        let mut compressed_chunks = Vec::new();
        
        for chunk in data.chunks(self.chunk_size) {
            let compressed_chunk = self.engine.compress(chunk)?;
            compressed_chunks.push(compressed_chunk);
        }

        // Serialize the chunks with length prefixes
        let mut result = Vec::new();
        for chunk in compressed_chunks {
            let len = chunk.len() as u32;
            result.extend_from_slice(&len.to_le_bytes());
            result.extend_from_slice(&chunk);
        }

        Ok(result)
    }

    /// Decompress streaming data
    pub fn decompress_stream(&self, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            if offset + 4 > data.len() {
                return Err(CompressionError::Generic(
                    "Invalid streaming format: incomplete length prefix".to_string()
                ));
            }

            let len_bytes: [u8; 4] = data[offset..offset + 4].try_into()
                .map_err(|_| CompressionError::Generic(
                    "Failed to read chunk length".to_string()
                ))?;
            
            let chunk_len = u32::from_le_bytes(len_bytes) as usize;
            offset += 4;

            if offset + chunk_len > data.len() {
                return Err(CompressionError::Generic(
                    "Invalid streaming format: incomplete chunk data".to_string()
                ));
            }

            let chunk_data = &data[offset..offset + chunk_len];
            let decompressed_chunk = self.engine.decompress(chunk_data)?;
            result.extend_from_slice(&decompressed_chunk);
            offset += chunk_len;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_roundtrip() {
        let engine = CompressionEngine::default();
        let test_data = b"Hello, world! This is test data for compression.".repeat(100);

        let compressed = engine.compress(&test_data).unwrap();
        let decompressed = engine.decompress(&compressed).unwrap();

        assert_eq!(test_data, decompressed);
        assert!(compressed.len() < test_data.len()); // Should be compressed
    }

    #[test]
    fn test_different_algorithms() {
        let test_data = b"Test data".repeat(1000);
        
        let algorithms = [
            CompressionType::None,
            CompressionType::Zstd,
            CompressionType::Lz4,
            CompressionType::Gzip,
        ];

        for algo in algorithms {
            let engine = CompressionEngine::new(algo, CompressionLevel::Medium);
            let compressed = engine.compress(&test_data).unwrap();
            let decompressed = engine.decompress(&compressed).unwrap();
            assert_eq!(test_data, decompressed, "Algorithm {:?} failed", algo);
        }
    }

    #[test]
    fn test_streaming_compression() {
        let engine = CompressionEngine::default();
        let streaming = StreamingCompressor::new(engine, 100);
        let test_data = b"A".repeat(500); // 5 chunks

        let compressed = streaming.compress_stream(&test_data).unwrap();
        let decompressed = streaming.decompress_stream(&compressed).unwrap();

        assert_eq!(test_data, decompressed);
    }
}
