//! 增量链处理逻辑
//!
//! 这个模块实现了增量快照链的管理，包括链式应用、依赖跟踪、
//! 链完整性验证和智能链重组功能。

use std::collections::{HashMap, HashSet, VecDeque, BTreeMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime};
use anyhow::{Result, anyhow, bail};
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, error, instrument};

use crate::incremental::delta_storage::{DeltaStorageManager, DeltaBlockType};
use crate::incremental::delta_computer::ObjectDelta;
use crate::incremental::delta_applier::DeltaApplier;
use mgo_types::storage::ObjectKey;
use crate::core_integration::state_serializer::ObjectEntry;

/// 增量链节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaChainNode {
    /// 快照ID
    pub snapshot_id: String,
    /// 父快照ID
    pub parent_snapshot_id: Option<String>,
    /// 子快照ID列表
    pub children_snapshot_ids: Vec<String>,
    /// 节点深度（从基础快照开始计算）
    pub depth: u64,
    /// 创建时间
    pub created_at: SystemTime,
    /// 关联的增量块ID列表
    pub delta_block_ids: Vec<String>,
    /// 节点状态
    pub status: DeltaChainNodeStatus,
    /// 节点元数据
    pub metadata: HashMap<String, String>,
}

/// 增量链节点状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaChainNodeStatus {
    /// 活跃状态，可以正常使用
    Active,
    /// 已过期，待清理
    Expired,
    /// 损坏状态，需要修复
    Corrupted,
    /// 正在处理中
    Processing,
}

/// 增量链配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaChainConfig {
    /// 最大链长度
    pub max_chain_length: u64,
    /// 链压缩阈值
    pub compression_threshold: u64,
    /// 是否启用自动链重组
    pub auto_reorganization: bool,
    /// 链验证间隔
    pub validation_interval_secs: u64,
    /// 并行处理线程数
    pub worker_threads: usize,
    /// 链分支限制
    pub max_branch_factor: usize,
}

impl Default for DeltaChainConfig {
    fn default() -> Self {
        Self {
            max_chain_length: 100,
            compression_threshold: 50,
            auto_reorganization: true,
            validation_interval_secs: 3600, // 1 hour
            worker_threads: 4,
            max_branch_factor: 10,
        }
    }
}

/// 增量链统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaChainStats {
    /// 总链数
    pub total_chains: u64,
    /// 总节点数
    pub total_nodes: u64,
    /// 平均链长度
    pub avg_chain_length: f64,
    /// 最大链长度
    pub max_chain_length: u64,
    /// 活跃节点数
    pub active_nodes: u64,
    /// 损坏节点数
    pub corrupted_nodes: u64,
    /// 链重组次数
    pub reorganization_count: u64,
    /// 链应用成功次数
    pub successful_applications: u64,
    /// 链应用失败次数
    pub failed_applications: u64,
}

/// 链操作结果
#[derive(Debug, Clone)]
pub struct ChainOperationResult {
    /// 操作是否成功
    pub success: bool,
    /// 处理的节点数
    pub processed_nodes: u64,
    /// 操作耗时
    pub duration: std::time::Duration,
    /// 错误信息（如果有）
    pub error_message: Option<String>,
    /// 详细结果信息
    pub details: HashMap<String, String>,
}

/// 增量链管理器
pub struct DeltaChainManager {
    config: DeltaChainConfig,
    storage_manager: Arc<DeltaStorageManager>,
    delta_applier: Arc<DeltaApplier>,
    chain_index: Arc<RwLock<BTreeMap<String, DeltaChainNode>>>,
    root_chains: Arc<RwLock<HashSet<String>>>, // 基础快照ID集合
    stats: Arc<Mutex<DeltaChainStats>>,
    last_validation: Arc<Mutex<Instant>>,
}

impl DeltaChainManager {
    /// 创建新的增量链管理器
    pub fn new(
        config: DeltaChainConfig,
        storage_manager: Arc<DeltaStorageManager>,
        delta_applier: Arc<DeltaApplier>,
    ) -> Self {
        Self {
            config,
            storage_manager,
            delta_applier,
            chain_index: Arc::new(RwLock::new(BTreeMap::new())),
            root_chains: Arc::new(RwLock::new(HashSet::new())),
            stats: Arc::new(Mutex::new(DeltaChainStats {
                total_chains: 0,
                total_nodes: 0,
                avg_chain_length: 0.0,
                max_chain_length: 0,
                active_nodes: 0,
                corrupted_nodes: 0,
                reorganization_count: 0,
                successful_applications: 0,
                failed_applications: 0,
            })),
            last_validation: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// 添加新的增量链节点
    #[instrument(skip(self))]
    pub async fn add_chain_node(
        &self,
        snapshot_id: &str,
        parent_snapshot_id: Option<&str>,
        delta_block_ids: Vec<String>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let mut chain_index = self.chain_index.write().unwrap();
        
        // 检查节点是否已存在
        if chain_index.contains_key(snapshot_id) {
            bail!("Chain node already exists: {}", snapshot_id);
        }

        // 计算深度
        let depth = if let Some(parent_id) = parent_snapshot_id {
            if let Some(parent_node) = chain_index.get(parent_id) {
                parent_node.depth + 1
            } else {
                // 父节点不存在，创建为根节点
                warn!("Parent snapshot {} not found, creating as root", parent_id);
                0
            }
        } else {
            0
        };

        // 检查链长度限制
        if depth >= self.config.max_chain_length {
            warn!("Chain length limit reached for snapshot {}, triggering reorganization", snapshot_id);
            self.trigger_chain_reorganization(parent_snapshot_id.unwrap()).await?;
        }

        // 创建新节点
        let node = DeltaChainNode {
            snapshot_id: snapshot_id.to_string(),
            parent_snapshot_id: parent_snapshot_id.map(|s| s.to_string()),
            children_snapshot_ids: Vec::new(),
            depth,
            created_at: SystemTime::now(),
            delta_block_ids,
            status: DeltaChainNodeStatus::Active,
            metadata,
        };

        // 更新父节点的子节点列表
        if let Some(parent_id) = parent_snapshot_id {
            if let Some(parent_node) = chain_index.get_mut(parent_id) {
                parent_node.children_snapshot_ids.push(snapshot_id.to_string());
                
                // 检查分支因子限制
                if parent_node.children_snapshot_ids.len() > self.config.max_branch_factor {
                    warn!("Branch factor limit exceeded for snapshot {}", parent_id);
                }
            }
        } else {
            // 这是一个根节点
            let mut root_chains = self.root_chains.write().unwrap();
            root_chains.insert(snapshot_id.to_string());
        }

        // 插入新节点
        chain_index.insert(snapshot_id.to_string(), node);

        // 更新统计信息
        self.update_stats().await;

        info!("Added chain node {} with depth {}", snapshot_id, depth);
        Ok(())
    }

    /// 应用增量链到指定快照
    #[instrument(skip(self, target_state))]
    pub async fn apply_delta_chain(
        &self,
        from_snapshot_id: &str,
        to_snapshot_id: &str,
        target_state: &mut HashMap<ObjectKey, ObjectEntry>, // 使用正确的状态表示
    ) -> Result<ChainOperationResult> {
        let start_time = Instant::now();
        let mut processed_nodes = 0u64;

        // 构建从源到目标的路径
        let path = self.find_chain_path(from_snapshot_id, to_snapshot_id).await?;
        
        if path.is_empty() {
            return Ok(ChainOperationResult {
                success: false,
                processed_nodes: 0,
                duration: start_time.elapsed(),
                error_message: Some("No path found between snapshots".to_string()),
                details: HashMap::new(),
            });
        }

        // 验证路径完整性
        self.validate_chain_path(&path).await?;

        // 按顺序应用每个增量
        for (i, snapshot_id) in path.iter().enumerate() {
            if i == 0 {
                continue; // 跳过起始快照
            }

            match self.apply_single_delta(snapshot_id, target_state).await {
                Ok(_) => {
                    processed_nodes += 1;
                    debug!("Applied delta for snapshot {}", snapshot_id);
                }
                Err(e) => {
                    error!("Failed to apply delta for snapshot {}: {}", snapshot_id, e);
                    
                    // 更新统计信息
                    let mut stats = self.stats.lock().unwrap();
                    stats.failed_applications += 1;

                    return Ok(ChainOperationResult {
                        success: false,
                        processed_nodes,
                        duration: start_time.elapsed(),
                        error_message: Some(format!("Failed at snapshot {}: {}", snapshot_id, e)),
                        details: HashMap::new(),
                    });
                }
            }
        }

        // 更新统计信息
        let mut stats = self.stats.lock().unwrap();
        stats.successful_applications += 1;

        Ok(ChainOperationResult {
            success: true,
            processed_nodes,
            duration: start_time.elapsed(),
            error_message: None,
            details: {
                let mut details = HashMap::new();
                details.insert("path_length".to_string(), path.len().to_string());
                details.insert("target_snapshot".to_string(), to_snapshot_id.to_string());
                details
            },
        })
    }

    /// 验证增量链完整性
    #[instrument(skip(self))]
    pub async fn validate_chain_integrity(&self, root_snapshot_id: &str) -> Result<bool> {
        let chain_index = self.chain_index.read().unwrap();
        let mut visited = HashSet::new();
        let mut to_visit = VecDeque::new();
        
        to_visit.push_back(root_snapshot_id.to_string());

        while let Some(snapshot_id) = to_visit.pop_front() {
            if visited.contains(&snapshot_id) {
                continue;
            }
            visited.insert(snapshot_id.clone());

            if let Some(node) = chain_index.get(&snapshot_id) {
                // 验证节点状态
                if node.status == DeltaChainNodeStatus::Corrupted {
                    warn!("Found corrupted node in chain: {}", snapshot_id);
                    return Ok(false);
                }

                // 验证增量块是否存在
                for block_id in &node.delta_block_ids {
                    if self.storage_manager.read_delta_block(block_id).await.is_err() {
                        error!("Missing delta block {} for snapshot {}", block_id, snapshot_id);
                        return Ok(false);
                    }
                }

                // 添加子节点到待访问队列
                for child_id in &node.children_snapshot_ids {
                    to_visit.push_back(child_id.clone());
                }
            } else {
                error!("Missing chain node: {}", snapshot_id);
                return Ok(false);
            }
        }

        info!("Chain integrity validation passed for root {}, verified {} nodes", 
              root_snapshot_id, visited.len());
        Ok(true)
    }

    /// 重组增量链
    #[instrument(skip(self))]
    pub async fn reorganize_chain(&self, root_snapshot_id: &str) -> Result<ChainOperationResult> {
        let start_time = Instant::now();
        
        info!("Starting chain reorganization for root {}", root_snapshot_id);

        // 收集链中的所有节点
        let chain_nodes = self.collect_chain_nodes(root_snapshot_id).await?;
        
        if chain_nodes.len() < self.config.compression_threshold as usize {
            return Ok(ChainOperationResult {
                success: true,
                processed_nodes: 0,
                duration: start_time.elapsed(),
                error_message: None,
                details: {
                    let mut details = HashMap::new();
                    details.insert("reason".to_string(), "Chain too short for reorganization".to_string());
                    details
                },
            });
        }

        // 执行链压缩
        let compressed_nodes = self.compress_chain_segment(&chain_nodes).await?;
        
        // 更新链结构
        self.update_chain_structure(root_snapshot_id, &compressed_nodes).await?;

        // 更新统计信息
        let mut stats = self.stats.lock().unwrap();
        stats.reorganization_count += 1;

        info!("Chain reorganization completed for root {}, compressed {} nodes to {}", 
              root_snapshot_id, chain_nodes.len(), compressed_nodes.len());

        Ok(ChainOperationResult {
            success: true,
            processed_nodes: chain_nodes.len() as u64,
            duration: start_time.elapsed(),
            error_message: None,
            details: {
                let mut details = HashMap::new();
                details.insert("original_length".to_string(), chain_nodes.len().to_string());
                details.insert("compressed_length".to_string(), compressed_nodes.len().to_string());
                details
            },
        })
    }

    /// 获取链统计信息
    pub async fn get_stats(&self) -> DeltaChainStats {
        let stats = self.stats.lock().unwrap();
        stats.clone()
    }

    /// 清理过期的链节点
    #[instrument(skip(self))]
    pub async fn cleanup_expired_nodes(&self) -> Result<u64> {
        let mut chain_index = self.chain_index.write().unwrap();
        let mut removed_count = 0u64;
        let mut to_remove = Vec::new();

        for (snapshot_id, node) in chain_index.iter() {
            if node.status == DeltaChainNodeStatus::Expired {
                to_remove.push(snapshot_id.clone());
            }
        }

        for snapshot_id in to_remove {
            if chain_index.remove(&snapshot_id).is_some() {
                removed_count += 1;
                debug!("Removed expired chain node: {}", snapshot_id);
            }
        }

        if removed_count > 0 {
            self.update_stats().await;
            info!("Cleaned up {} expired chain nodes", removed_count);
        }

        Ok(removed_count)
    }

    // ===== 私有辅助方法 =====

    /// 查找增量链路径
    async fn find_chain_path(&self, from: &str, to: &str) -> Result<Vec<String>> {
        let chain_index = self.chain_index.read().unwrap();
        let mut path = Vec::new();
        let mut current = to;

        // 从目标向上回溯到源
        while current != from {
            path.push(current.to_string());
            
            if let Some(node) = chain_index.get(current) {
                if let Some(parent_id) = &node.parent_snapshot_id {
                    current = parent_id;
                } else {
                    bail!("No path found: reached root without finding source");
                }
            } else {
                bail!("Chain node not found: {}", current);
            }
        }

        path.push(from.to_string());
        path.reverse();
        Ok(path)
    }

    /// 验证链路径
    async fn validate_chain_path(&self, path: &[String]) -> Result<()> {
        let chain_index = self.chain_index.read().unwrap();
        
        for (i, snapshot_id) in path.iter().enumerate() {
            let node = chain_index.get(snapshot_id)
                .ok_or_else(|| anyhow!("Missing chain node: {}", snapshot_id))?;

            if node.status != DeltaChainNodeStatus::Active {
                bail!("Inactive chain node: {} (status: {:?})", snapshot_id, node.status);
            }

            // 验证父子关系
            if i > 0 {
                let parent_id = &path[i - 1];
                if node.parent_snapshot_id.as_ref() != Some(parent_id) {
                    bail!("Invalid parent-child relationship: {} -> {}", parent_id, snapshot_id);
                }
            }
        }

        Ok(())
    }

    /// 应用单个增量
    async fn apply_single_delta(
        &self,
        snapshot_id: &str,
        target_state: &mut HashMap<ObjectKey, ObjectEntry>,
    ) -> Result<()> {
        let chain_index = self.chain_index.read().unwrap();
        let node = chain_index.get(snapshot_id)
            .ok_or_else(|| anyhow!("Chain node not found: {}", snapshot_id))?;

        // 读取并应用所有相关的增量块
        for block_id in &node.delta_block_ids {
            let (delta_data, metadata) = self.storage_manager.read_delta_block(block_id).await?;
            
            // 根据块类型应用不同的增量
            match metadata.block_type {
                DeltaBlockType::ObjectDelta => {
                    let object_delta: ObjectDelta = bcs::from_bytes(&delta_data)?;
                    self.apply_object_delta(&object_delta, target_state).await?;
                }
                _ => {
                    // 其他类型的增量处理
                    debug!("Skipping non-object delta block: {}", block_id);
                }
            }
        }

        Ok(())
    }

    /// 应用对象增量
    async fn apply_object_delta(
        &self,
        object_delta: &ObjectDelta,
        target_state: &mut HashMap<ObjectKey, ObjectEntry>,
    ) -> Result<()> {
        // 应用新对象
        for (object_key, object_entry) in &object_delta.new_objects {
            target_state.insert(object_key.clone(), object_entry.clone());
        }

        // 应用修改的对象
        for (object_key, object_entry) in &object_delta.modified_objects {
            target_state.insert(object_key.clone(), object_entry.clone());
        }

        // 删除对象
        for (object_key, _) in &object_delta.deleted_objects {
            target_state.remove(object_key);
        }

        Ok(())
    }

    /// 触发链重组
    async fn trigger_chain_reorganization(&self, root_snapshot_id: &str) -> Result<()> {
        if self.config.auto_reorganization {
            info!("Auto-triggering chain reorganization for {}", root_snapshot_id);
            self.reorganize_chain(root_snapshot_id).await?;
        }
        Ok(())
    }

    /// 收集链节点
    async fn collect_chain_nodes(&self, root_snapshot_id: &str) -> Result<Vec<DeltaChainNode>> {
        let chain_index = self.chain_index.read().unwrap();
        let mut nodes = Vec::new();
        let mut to_visit = VecDeque::new();
        
        to_visit.push_back(root_snapshot_id.to_string());

        while let Some(snapshot_id) = to_visit.pop_front() {
            if let Some(node) = chain_index.get(&snapshot_id) {
                nodes.push(node.clone());
                
                // 按深度排序子节点
                let mut children = node.children_snapshot_ids.clone();
                children.sort_by(|a, b| {
                    let depth_a = chain_index.get(a).map(|n| n.depth).unwrap_or(0);
                    let depth_b = chain_index.get(b).map(|n| n.depth).unwrap_or(0);
                    depth_a.cmp(&depth_b)
                });
                
                for child_id in children {
                    to_visit.push_back(child_id);
                }
            }
        }

        Ok(nodes)
    }

    /// 压缩链段
    async fn compress_chain_segment(&self, _nodes: &[DeltaChainNode]) -> Result<Vec<DeltaChainNode>> {
        // 实际实现中会将多个连续的增量合并为一个
        // 这里返回占位符，表示压缩后的节点
        warn!("Chain compression not yet fully implemented");
        Ok(Vec::new())
    }

    /// 更新链结构
    async fn update_chain_structure(
        &self,
        _root_snapshot_id: &str,
        _compressed_nodes: &[DeltaChainNode],
    ) -> Result<()> {
        // 实际实现中会更新内存和持久化存储中的链结构
        warn!("Chain structure update not yet fully implemented");
        Ok(())
    }

    /// 更新统计信息
    async fn update_stats(&self) {
        let chain_index = self.chain_index.read().unwrap();
        let root_chains = self.root_chains.read().unwrap();
        let mut stats = self.stats.lock().unwrap();

        stats.total_chains = root_chains.len() as u64;
        stats.total_nodes = chain_index.len() as u64;
        
        // 计算平均链长度和最大链长度
        let mut total_depth = 0u64;
        let mut max_depth = 0u64;
        let mut active_count = 0u64;
        let mut corrupted_count = 0u64;

        for node in chain_index.values() {
            total_depth += node.depth;
            max_depth = max_depth.max(node.depth);
            
            match node.status {
                DeltaChainNodeStatus::Active => active_count += 1,
                DeltaChainNodeStatus::Corrupted => corrupted_count += 1,
                _ => {}
            }
        }

        if !chain_index.is_empty() {
            stats.avg_chain_length = total_depth as f64 / chain_index.len() as f64;
        }
        
        stats.max_chain_length = max_depth;
        stats.active_nodes = active_count;
        stats.corrupted_nodes = corrupted_count;
    }
}
