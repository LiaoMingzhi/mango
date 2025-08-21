// Copyright (c) 2025 Mango Labs
// SPDX-License-Identifier: Apache-2.0

//! Streaming utilities for processing large snapshot data efficiently
//! Provides streaming compression, decompression, and serialization

use std::pin::Pin;
use std::task::{Context, Poll};
use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tracing::{info, debug};

use crate::types::error::{SnapshotResult, SnapshotError};
use crate::performance::memory_management::AutoMemoryManager;

/// Streaming chunk for processing large data
#[derive(Debug, Clone)]
pub struct DataChunk {
    pub data: Bytes,
    pub chunk_id: usize,
    pub total_chunks: Option<usize>,
    pub metadata: ChunkMetadata,
}

/// Metadata for data chunks
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub original_size: usize,
    pub compressed_size: usize,
    pub checksum: u32,
    pub compression_type: CompressionType,
}

/// Supported compression types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

/// Streaming snapshot codec for encoding/decoding chunks
pub struct SnapshotCodec {
    compression_type: CompressionType,
    chunk_size: usize,
    memory_manager: AutoMemoryManager,
}

impl SnapshotCodec {
    pub fn new(compression_type: CompressionType, chunk_size: usize) -> Self {
        Self {
            compression_type,
            chunk_size,
            memory_manager: AutoMemoryManager::new(500, 1000), // 500MB warning, 1GB error
        }
    }

    /// Create a streaming reader for large snapshot data
    pub fn create_reader<R: AsyncRead + Unpin>(
        &self,
        reader: R,
    ) -> SnapshotStreamReader<R> {
        SnapshotStreamReader::new(reader, self.chunk_size, self.compression_type)
    }

    /// Create a streaming writer for large snapshot data
    pub fn create_writer<W: AsyncWrite + Unpin>(
        &self,
        writer: W,
    ) -> SnapshotStreamWriter<W> {
        SnapshotStreamWriter::new(writer, self.chunk_size, self.compression_type)
    }
}

impl Decoder for SnapshotCodec {
    type Item = DataChunk;
    type Error = SnapshotError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 16 bytes for chunk header
        if src.len() < 16 {
            return Ok(None);
        }

        let chunk_id = src.get_u32() as usize;
        let total_chunks = {
            let val = src.get_u32();
            if val == u32::MAX { None } else { Some(val as usize) }
        };
        let original_size = src.get_u32() as usize;
        let compressed_size = src.get_u32() as usize;

        // Check if we have the full chunk data
        if src.len() < compressed_size + 8 { // +8 for checksum and compression type
            // Put back the header bytes
            let mut temp = BytesMut::with_capacity(16 + src.len());
            temp.put_u32(chunk_id as u32);
            temp.put_u32(total_chunks.map_or(u32::MAX, |c| c as u32));
            temp.put_u32(original_size as u32);
            temp.put_u32(compressed_size as u32);
            temp.put(src.split());
            *src = temp;
            return Ok(None);
        }

        let checksum = src.get_u32();
        let compression_type_byte = src.get_u8();
        let _reserved = src.get_u8(); // Reserved byte
        let _reserved2 = src.get_u16(); // Reserved bytes

        let compression_type = match compression_type_byte {
            0 => CompressionType::None,
            1 => CompressionType::Gzip,
            2 => CompressionType::Zstd,
            3 => CompressionType::Lz4,
            _ => return Err(SnapshotError::DataAccess {
                operation: "decode_chunk".to_string(),
                details: format!("Unknown compression type: {}", compression_type_byte),
            }),
        };

        let data = src.split_to(compressed_size).freeze();

        // Verify checksum
        let actual_checksum = crc32fast::hash(&data);
        if actual_checksum != checksum {
            return Err(SnapshotError::DataAccess {
                operation: "decode_chunk".to_string(),
                details: format!("Checksum mismatch: expected {}, got {}", checksum, actual_checksum),
            });
        }

        Ok(Some(DataChunk {
            data,
            chunk_id,
            total_chunks,
            metadata: ChunkMetadata {
                original_size,
                compressed_size,
                checksum,
                compression_type,
            },
        }))
    }
}

impl Encoder<DataChunk> for SnapshotCodec {
    type Error = SnapshotError;

    fn encode(&mut self, item: DataChunk, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Chunk header: chunk_id (4) + total_chunks (4) + original_size (4) + compressed_size (4)
        dst.reserve(16 + item.data.len() + 8);
        
        dst.put_u32(item.chunk_id as u32);
        dst.put_u32(item.total_chunks.map_or(u32::MAX, |c| c as u32));
        dst.put_u32(item.metadata.original_size as u32);
        dst.put_u32(item.metadata.compressed_size as u32);
        
        // Checksum and compression info
        dst.put_u32(item.metadata.checksum);
        dst.put_u8(match item.metadata.compression_type {
            CompressionType::None => 0,
            CompressionType::Gzip => 1,
            CompressionType::Zstd => 2,
            CompressionType::Lz4 => 3,
        });
        dst.put_u8(0); // Reserved
        dst.put_u16(0); // Reserved
        
        // Actual data
        dst.put(item.data);
        
        Ok(())
    }
}

/// Streaming reader for processing snapshot data in chunks
pub struct SnapshotStreamReader<R> {
    inner: R,
    chunk_size: usize,
    compression_type: CompressionType,
    current_chunk: usize,
    buffer: Vec<u8>,
}

impl<R: AsyncRead + Unpin> SnapshotStreamReader<R> {
    pub fn new(reader: R, chunk_size: usize, compression_type: CompressionType) -> Self {
        Self {
            inner: reader,
            chunk_size,
            compression_type,
            current_chunk: 0,
            buffer: Vec::with_capacity(chunk_size),
        }
    }

    /// Read next chunk from the stream
    pub async fn read_chunk(&mut self) -> SnapshotResult<Option<DataChunk>> {
        self.buffer.clear();
        self.buffer.resize(self.chunk_size, 0);

        match self.inner.read(&mut self.buffer).await {
            Ok(0) => Ok(None), // EOF
            Ok(bytes_read) => {
                self.buffer.truncate(bytes_read);
                let original_size = bytes_read;
                
                // Compress data if needed
                let (compressed_data, compression_type) = match self.compression_type {
                    CompressionType::None => (Bytes::copy_from_slice(&self.buffer), CompressionType::None),
                    CompressionType::Gzip => {
                        let compressed = self.compress_gzip(&self.buffer)?;
                        (compressed, CompressionType::Gzip)
                    }
                    CompressionType::Zstd => {
                        let compressed = self.compress_zstd(&self.buffer)?;
                        (compressed, CompressionType::Zstd)
                    }
                    CompressionType::Lz4 => {
                        let compressed = self.compress_lz4(&self.buffer)?;
                        (compressed, CompressionType::Lz4)
                    }
                };

                let checksum = crc32fast::hash(&compressed_data);
                let chunk_id = self.current_chunk;
                self.current_chunk += 1;

                let compressed_size = compressed_data.len();
                Ok(Some(DataChunk {
                    data: compressed_data,
                    chunk_id,
                    total_chunks: None, // Unknown for streaming
                    metadata: ChunkMetadata {
                        original_size,
                        compressed_size,
                        checksum,
                        compression_type,
                    },
                }))
            }
            Err(e) => Err(SnapshotError::DataAccess {
                operation: "read_chunk".to_string(),
                details: format!("Failed to read chunk: {}", e),
            }),
        }
    }

    fn compress_gzip(&self, data: &[u8]) -> SnapshotResult<Bytes> {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).map_err(|e| SnapshotError::DataAccess {
            operation: "gzip_compress".to_string(),
            details: format!("Gzip compression failed: {}", e),
        })?;
        
        let compressed = encoder.finish().map_err(|e| SnapshotError::DataAccess {
            operation: "gzip_compress".to_string(),
            details: format!("Gzip compression failed: {}", e),
        })?;
        
        Ok(Bytes::from(compressed))
    }

    fn compress_zstd(&self, data: &[u8]) -> SnapshotResult<Bytes> {
        let compressed = zstd::bulk::compress(data, 3).map_err(|e| SnapshotError::DataAccess {
            operation: "zstd_compress".to_string(),
            details: format!("Zstd compression failed: {}", e),
        })?;
        Ok(Bytes::from(compressed))
    }

    fn compress_lz4(&self, data: &[u8]) -> SnapshotResult<Bytes> {
        #[cfg(feature = "compression")]
        {
            let compressed = lz4::block::compress(data, None, false).map_err(|e| SnapshotError::DataAccess {
                operation: "lz4_compress".to_string(),
                details: format!("LZ4 compression failed: {}", e),
            })?;
            Ok(Bytes::from(compressed))
        }
        #[cfg(not(feature = "compression"))]
        {
            // If compression feature is not enabled, return data as-is
            Ok(Bytes::copy_from_slice(data))
        }
    }
}

/// Streaming writer for writing snapshot data in chunks
pub struct SnapshotStreamWriter<W> {
    inner: W,
    compression_type: CompressionType,
    chunks_written: usize,
}

impl<W: AsyncWrite + Unpin> SnapshotStreamWriter<W> {
    pub fn new(writer: W, _chunk_size: usize, compression_type: CompressionType) -> Self {
        Self {
            inner: writer,
            compression_type,
            chunks_written: 0,
        }
    }

    /// Write a chunk to the stream
    pub async fn write_chunk(&mut self, chunk: DataChunk) -> SnapshotResult<()> {
        // Decompress if needed
        let decompressed_data = match chunk.metadata.compression_type {
            CompressionType::None => chunk.data.to_vec(),
            CompressionType::Gzip => self.decompress_gzip(&chunk.data)?,
            CompressionType::Zstd => self.decompress_zstd(&chunk.data)?,
            CompressionType::Lz4 => self.decompress_lz4(&chunk.data)?,
        };

        self.inner.write_all(&decompressed_data).await.map_err(|e| SnapshotError::DataAccess {
            operation: "write_chunk".to_string(),
            details: format!("Failed to write chunk: {}", e),
        })?;

        self.chunks_written += 1;
        debug!("Wrote chunk {} ({} bytes)", chunk.chunk_id, decompressed_data.len());
        
        Ok(())
    }

    /// Flush the writer
    pub async fn flush(&mut self) -> SnapshotResult<()> {
        self.inner.flush().await.map_err(|e| SnapshotError::DataAccess {
            operation: "flush_writer".to_string(),
            details: format!("Failed to flush writer: {}", e),
        })?;
        
        info!("Flushed stream writer, {} chunks written", self.chunks_written);
        Ok(())
    }

    fn decompress_gzip(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| SnapshotError::DataAccess {
            operation: "gzip_decompress".to_string(),
            details: format!("Gzip decompression failed: {}", e),
        })?;
        
        Ok(decompressed)
    }

    fn decompress_zstd(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        zstd::bulk::decompress(data, 1024 * 1024 * 64).map_err(|e| SnapshotError::DataAccess {
            operation: "zstd_decompress".to_string(),
            details: format!("Zstd decompression failed: {}", e),
        })
    }

    fn decompress_lz4(&self, data: &[u8]) -> SnapshotResult<Vec<u8>> {
        #[cfg(feature = "compression")]
        {
            lz4::block::decompress(data, None).map_err(|e| SnapshotError::DataAccess {
                operation: "lz4_decompress".to_string(),
                details: format!("LZ4 decompression failed: {}", e),
            })
        }
        #[cfg(not(feature = "compression"))]
        {
            // If compression feature is not enabled, return data as-is
            Ok(data.to_vec())
        }
    }
}

/// Stream adapter for processing chunks with backpressure
pub struct ChunkStream<S> {
    inner: S,
    buffer_size: usize,
    processed_chunks: usize,
}

impl<S> ChunkStream<S> 
where 
    S: Stream<Item = SnapshotResult<DataChunk>> + Unpin,
{
    pub fn new(stream: S, buffer_size: usize) -> Self {
        Self {
            inner: stream,
            buffer_size,
            processed_chunks: 0,
        }
    }

    /// Process chunks with a given async function
    pub async fn process_chunks<F, Fut>(&mut self, mut processor: F) -> SnapshotResult<usize>
    where
        F: FnMut(DataChunk) -> Fut,
        Fut: std::future::Future<Output = SnapshotResult<()>>,
    {
        let mut buffer = Vec::with_capacity(self.buffer_size);
        
        while let Some(chunk_result) = self.inner.next().await {
            let chunk = chunk_result?;
            buffer.push(chunk);
            
            // Process buffer when full or at end of stream
            if buffer.len() >= self.buffer_size {
                for chunk in buffer.drain(..) {
                    processor(chunk).await?;
                    self.processed_chunks += 1;
                }
            }
        }
        
        // Process remaining chunks in buffer
        for chunk in buffer.drain(..) {
            processor(chunk).await?;
            self.processed_chunks += 1;
        }
        
        info!("Processed {} chunks", self.processed_chunks);
        Ok(self.processed_chunks)
    }
}

impl<S> Stream for ChunkStream<S>
where
    S: Stream<Item = SnapshotResult<DataChunk>> + Unpin,
{
    type Item = SnapshotResult<DataChunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
