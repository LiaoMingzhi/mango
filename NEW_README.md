# Mango 4节点集群测试 - 新架构

## 📁 标准化目录结构

### 远程节点目录结构
```
/root/workspace/mango-cluster/
├── db_node/           # 数据库文件存储
│   ├── node_1/        # 节点1数据库
│   ├── node_2/        # 节点2数据库
│   ├── node_3/        # 节点3数据库
│   └── node_4/        # 节点4数据库
├── logs/              # 日志文件存储
│   ├── node_1/        # 节点1日志
│   ├── node_2/        # 节点2日志
│   ├── node_3/        # 节点3日志
│   └── node_4/        # 节点4日志
├── network/           # 网络配置
│   ├── genesis/       # 创世文件
│   └── keys/          # 密钥文件
├── config/            # 节点配置文件
│   └── node_config.yaml
├── bin/               # 可执行文件
│   ├── mgo
│   ├── mgo-node
│   └── mgo-test-validator
└── scripts/           # 运维脚本
    ├── start_node_X.sh
    ├── stop_node_X.sh
    └── check_node_X.sh
```

## 🚀 快速开始

### 方法1: 使用主控脚本 (推荐)
```bash
./new_master.sh
```
选择 `8) 完整部署流程` 一键完成所有步骤

### 方法2: 手动逐步执行
```bash
# 1. 创建标准化目录结构
./create_cluster_structure.sh

# 2. 部署可执行文件
./deploy_cluster_bins.sh

# 3. 更新节点配置
./update_node_config.sh

# 4. 创建运维脚本
./create_cluster_scripts.sh

# 5. 启动集群
./new_start_cluster.sh

# 6. 检查集群状态
./new_check_cluster.sh
```

## 📜 脚本说明

### 部署脚本
| 脚本 | 功能 | 描述 |
|------|------|------|
| `create_cluster_structure.sh` | 创建目录结构 | 在所有节点创建标准化目录 |
| `deploy_cluster_bins.sh` | 部署可执行文件 | 复制编译好的二进制文件 |
| `update_node_config.sh` | 更新配置 | 生成适应新结构的配置文件 |
| `create_cluster_scripts.sh` | 创建运维脚本 | 为每个节点生成专用脚本 |

### 运行脚本
| 脚本 | 功能 | 描述 |
|------|------|------|
| `new_start_cluster.sh` | 启动集群 | 使用新架构启动所有节点 |
| `new_check_cluster.sh` | 检查状态 | 全面的集群健康检查 |
| `new_stop_cluster.sh` | 停止集群 | 优雅停止所有节点 |
| `new_master.sh` | 主控脚本 | 交互式管理界面 |

## 🔧 配置特性

### 节点配置改进
- **分离式存储**: 数据库、日志、配置分目录存储
- **完整路径**: 所有路径使用绝对路径，避免相对路径问题
- **日志管理**: 每个节点独立日志目录，支持日志轮转
- **共识配置**: 完整的Narwhal共识配置
- **监控端口**: 每个节点使用不同的端口避免冲突

### 端口分配
| 节点 | RPC端口 | 网络端口 | P2P端口 | 管理端口 | WebSocket |
|------|---------|----------|---------|----------|-----------|
| 节点1 | 9544 | 9000 | 8080 | 1337 | 10544 |
| 节点2 | 9545 | 9001 | 8081 | 1338 | 10545 |
| 节点3 | 9546 | 9002 | 8082 | 1339 | 10546 |
| 节点4 | 9547 | 9003 | 8083 | 1340 | 10547 |

## 🛠️ 运维操作

### 单节点操作
在每个节点上，可以使用专用的运维脚本：
```bash
# 在远程节点上
cd /root/workspace/mango-cluster/scripts

# 启动特定节点
./start_node_1.sh

# 检查节点状态
./check_node_1.sh

# 停止节点
./stop_node_1.sh

# 清理重启
./start_node_1.sh --clean
```

### 集群操作
```bash
# 检查所有节点状态
./new_check_cluster.sh

# 重启整个集群
./new_master.sh  # 选择 9) 重新部署集群

# 停止集群
./new_stop_cluster.sh
```

## 📊 监控和日志

### 日志位置
- 节点日志: `/root/workspace/mango-cluster/logs/node_X/mgo-node.log`
- 每个节点有独立的日志目录
- 支持日志文件大小限制和轮转

### 状态检查
- 进程状态检查
- 端口监听检查
- 磁盘空间检查
- 内存使用检查
- RPC接口测试

## 🔧 故障排除

### 常见问题
1. **节点启动失败**
   ```bash
   # 检查日志
   ssh -i old_testnet.pem root@节点IP "tail -50 /root/workspace/mango-cluster/logs/node_X/mgo-node.log"
   ```

2. **配置文件问题**
   ```bash
   # 重新生成配置
   ./update_node_config.sh
   ```

3. **端口冲突**
   ```bash
   # 检查端口占用
   ./new_check_cluster.sh
   ```

4. **权限问题**
   ```bash
   # 修复权限
   ssh -i old_testnet.pem root@节点IP "cd /root/workspace/mango-cluster && chmod +x bin/* scripts/*"
   ```

## 💡 优势特性

### 新架构优势
- ✅ **标准化结构**: 统一的目录组织方式
- ✅ **文件分离**: 避免根目录混乱
- ✅ **独立运维**: 每个节点有专用脚本
- ✅ **完整配置**: 包含所有必要的Mango配置
- ✅ **易于维护**: 清晰的目录结构便于管理
- ✅ **可扩展性**: 易于添加新节点或功能

### 生产就绪
- 🔒 **安全配置**: 正确的文件权限和路径
- 📊 **完整监控**: 多维度的状态检查
- 📝 **详细日志**: 结构化的日志管理
- 🔄 **自动恢复**: 智能的错误处理和重试

## 📞 技术支持

如需帮助，请：
1. 查看节点日志文件
2. 运行 `./new_check_cluster.sh` 进行诊断
3. 使用 `./new_master.sh` 的故障排除选项
