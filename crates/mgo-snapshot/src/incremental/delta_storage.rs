//! 优化的增量数据存储格式

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;
use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, instrument};

use crate::types::CompressionType;
use crate::creator::compressor::SnapshotCompressor;

/// 增量存储后端类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageBackend {
    FileSystem { root_path: PathBuf },
    Memory,
    S3Compatible { bucket: String, prefix: String },
}

/// 增量数据块的元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaBlockMetadata {
    pub block_id: String,
    pub parent_snapshot_id: String,
    pub current_snapshot_id: String,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub compression: CompressionType,
    pub data_hash: String,
    pub created_at: SystemTime,
    pub block_type: DeltaBlockType,
}

/// 增量数据块类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaBlockType {
    ObjectDelta,
    TransactionDelta,
    CheckpointDelta,
    IndexDelta,
}

/// 增量存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaStorageConfig {
    pub backend: StorageBackend,
    pub default_compression: CompressionType,
    pub max_block_size: u64,
    pub block_cache_size: usize,
    pub auto_compression: bool,
    pub enable_deduplication: bool,
}

impl Default for DeltaStorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Memory,
            default_compression: CompressionType::Lz4,
            max_block_size: 64 * 1024 * 1024,
            block_cache_size: 1000,
            auto_compression: true,
            enable_deduplication: true,
        }
    }
}

/// 增量存储统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaStorageStats {
    pub total_blocks: u64,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
    pub avg_compression_ratio: f64,
    pub cache_hit_ratio: f64,
}

/// 优化的增量数据存储管理器
pub struct DeltaStorageManager {
    config: DeltaStorageConfig,
    compressor: SnapshotCompressor,
    block_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    metadata_index: Arc<RwLock<BTreeMap<String, DeltaBlockMetadata>>>,
    stats: Arc<Mutex<DeltaStorageStats>>,
}

impl DeltaStorageManager {
    /// 创建新的增量存储管理器
    pub fn new(config: DeltaStorageConfig) -> Result<Self> {
        let compressor = SnapshotCompressor::new(config.default_compression)?;
        
        Ok(Self {
            config,
            compressor,
            block_cache: Arc::new(RwLock::new(HashMap::new())),
            metadata_index: Arc::new(RwLock::new(BTreeMap::new())),
            stats: Arc::new(Mutex::new(DeltaStorageStats {
                total_blocks: 0,
                total_compressed_size: 0,
                total_uncompressed_size: 0,
                avg_compression_ratio: 1.0,
                cache_hit_ratio: 0.0,
            })),
        })
    }

    /// 存储增量数据块
    #[instrument(skip(self, data))]
    pub async fn store_delta_block(
        &self,
        block_id: &str,
        data: &[u8],
        metadata: DeltaBlockMetadata,
    ) -> Result<()> {
        // 压缩数据 (暂时简化实现)
        let compressed_data = if self.config.auto_compression {
            // TODO: Implement proper compression once public API is available
            data.to_vec()
        } else {
            data.to_vec()
        };

        // 存储到后端
        self.store_to_backend(block_id, &compressed_data).await?;

        // 更新元数据和统计信息
        let mut metadata_index = self.metadata_index.write().unwrap();
        metadata_index.insert(block_id.to_string(), metadata);

        let mut stats = self.stats.lock().unwrap();
        stats.total_blocks += 1;
        stats.total_uncompressed_size += data.len() as u64;
        stats.total_compressed_size += compressed_data.len() as u64;

        if stats.total_uncompressed_size > 0 {
            stats.avg_compression_ratio = stats.total_compressed_size as f64 / stats.total_uncompressed_size as f64;
        }

        info!("Stored delta block {} ({} -> {} bytes)", 
              block_id, data.len(), compressed_data.len());

        Ok(())
    }

    /// 读取增量数据块
    #[instrument(skip(self))]
    pub async fn read_delta_block(&self, block_id: &str) -> Result<(Vec<u8>, DeltaBlockMetadata)> {
        // 检查缓存
        let cache = self.block_cache.read().unwrap();
        if let Some(cached_data) = cache.get(block_id) {
            let metadata_index = self.metadata_index.read().unwrap();
            let metadata = metadata_index.get(block_id)
                .ok_or_else(|| anyhow!("Metadata not found: {}", block_id))?
                .clone();
            
            return Ok((cached_data.clone(), metadata));
        }
        drop(cache);

        // 从后端读取
        let compressed_data = self.read_from_backend(block_id).await?;
        
        let metadata_index = self.metadata_index.read().unwrap();
        let metadata = metadata_index.get(block_id)
            .ok_or_else(|| anyhow!("Metadata not found: {}", block_id))?
            .clone();

        // 解压缩数据 (暂时简化实现)
        let data = if metadata.compression != CompressionType::None {
            // TODO: Implement proper decompression once public API is available
            compressed_data
        } else {
            compressed_data
        };

        // 更新缓存
        let mut cache = self.block_cache.write().unwrap();
        cache.insert(block_id.to_string(), data.clone());

        debug!("Read delta block {} ({} bytes)", block_id, data.len());

        Ok((data, metadata))
    }

    /// 获取存储统计信息
    pub async fn get_stats(&self) -> DeltaStorageStats {
        let stats = self.stats.lock().unwrap();
        stats.clone()
    }

    // ===== 私有辅助方法 =====

    /// 存储到后端
    async fn store_to_backend(&self, block_id: &str, data: &[u8]) -> Result<()> {
        match &self.config.backend {
            StorageBackend::Memory => {
                let mut cache = self.block_cache.write().unwrap();
                cache.insert(block_id.to_string(), data.to_vec());
                Ok(())
            }
            StorageBackend::FileSystem { root_path } => {
                let file_path = root_path.join(format!("{}.delta", block_id));
                std::fs::create_dir_all(file_path.parent().unwrap())?;
                std::fs::write(&file_path, data)?;
                Ok(())
            }
            StorageBackend::S3Compatible { .. } => {
                warn!("S3 storage not yet implemented");
                Ok(())
            }
        }
    }

    /// 从后端读取
    async fn read_from_backend(&self, block_id: &str) -> Result<Vec<u8>> {
        match &self.config.backend {
            StorageBackend::Memory => {
                let cache = self.block_cache.read().unwrap();
                cache.get(block_id)
                    .ok_or_else(|| anyhow!("Block not found: {}", block_id))
                    .map(|data| data.clone())
            }
            StorageBackend::FileSystem { root_path } => {
                let file_path = root_path.join(format!("{}.delta", block_id));
                std::fs::read(&file_path).map_err(|e| anyhow!("Failed to read block {}: {}", block_id, e))
            }
            StorageBackend::S3Compatible { .. } => {
                Err(anyhow!("S3 storage not yet implemented"))
            }
        }
    }
}
