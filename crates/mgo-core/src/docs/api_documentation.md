# MGO 高可用性管理 REST API 文档

## 概述

MGO 高可用性管理系统提供完整的REST API接口，用于管理和监控区块链节点的健康状态、恢复操作、安全检测等功能。

### 基础信息

- **API版本**: v1
- **基础URL**: `http://localhost:8080/api/v1`
- **内容类型**: `application/json`
- **认证**: 目前为演示版本，未启用认证

## API端点概览

### 健康检查
- `GET /health` - 获取基础健康状态
- `GET /health/detailed` - 获取详细健康状态

### 系统状态
- `GET /status` - 获取系统状态

### 恢复操作
- `POST /recovery` - 触发恢复操作
- `GET /recovery/status` - 获取恢复状态
- `POST /recovery/cancel` - 取消恢复操作

### 配置管理
- `GET /config` - 获取当前配置
- `PUT /config` - 更新配置

### 安全监控
- `GET /security/attacks` - 获取攻击检测结果
- `GET /security/threats` - 获取威胁分析

### 告警管理
- `GET /alerts` - 获取告警历史
- `POST /alerts/{id}/acknowledge` - 确认告警

### 回滚操作
- `POST /rollback` - 触发回滚操作
- `GET /rollback/status` - 获取回滚状态

### 冷启动操作
- `POST /cold-start` - 触发冷启动操作
- `GET /cold-start/status` - 获取冷启动状态

### 网络监控
- `GET /network/status` - 获取网络状态
- `GET /network/health` - 获取网络健康状态

---

## 详细API参考

### 1. 健康检查

#### GET /api/v1/health

获取系统基础健康状态。

**响应示例:**
```json
{
  "overall_healthy": true,
  "health_score": 95,
  "consensus_healthy": true,
  "network_healthy": true,
  "storage_healthy": true,
  "execution_healthy": true,
  "error_details": [],
  "timestamp": 1701234567
}
```

**字段说明:**
- `overall_healthy`: 整体健康状态
- `health_score`: 健康分数 (0-100)
- `consensus_healthy`: 共识系统健康状态
- `network_healthy`: 网络健康状态
- `storage_healthy`: 存储健康状态
- `execution_healthy`: 执行引擎健康状态
- `error_details`: 错误详情列表
- `timestamp`: Unix时间戳

#### GET /api/v1/health/detailed

获取详细健康状态，包含更多诊断信息。

**响应格式同基础健康检查，但包含更详细的 `error_details`。**

### 2. 系统状态

#### GET /api/v1/status

获取高可用性系统的整体状态。

**响应示例:**
```json
{
  "system_state": "Healthy",
  "recovery_attempts": 0,
  "last_recovery": null,
  "auto_recovery_enabled": true,
  "uptime_seconds": 3600
}
```

**字段说明:**
- `system_state`: 系统状态 (Healthy/Degraded/Critical/Failed)
- `recovery_attempts`: 恢复尝试次数
- `last_recovery`: 最后恢复时间 (Unix时间戳)
- `auto_recovery_enabled`: 是否启用自动恢复
- `uptime_seconds`: 系统运行时间（秒）

### 3. 恢复操作

#### POST /api/v1/recovery

触发手动恢复操作。

**请求体:**
```json
{
  "strategy": "auto",
  "checkpoint": 12345,
  "force": false,
  "reason": "手动恢复测试"
}
```

**字段说明:**
- `strategy`: 恢复策略 (`auto`, `rollback`, `cold_start`, `restart`)
- `checkpoint`: 目标检查点（rollback策略必需）
- `force`: 是否强制执行
- `reason`: 操作理由（可选）

**响应示例:**
```json
{
  "status": "started",
  "operation_id": "recovery-1701234567890",
  "strategy": "auto",
  "estimated_duration": 300,
  "started_at": 1701234567
}
```

#### GET /api/v1/recovery/status

获取当前恢复操作状态。

**响应示例:**
```json
{
  "status": "idle",
  "current_operation": null,
  "progress": 0,
  "estimated_remaining": 0
}
```

#### POST /api/v1/recovery/cancel

取消当前进行的恢复操作。

**响应示例:**
```json
{
  "status": "cancelled",
  "message": "恢复操作已取消"
}
```

### 4. 配置管理

#### GET /api/v1/config

获取当前高可用性配置。

**响应示例:**
```json
{
  "enable_auto_recovery": true,
  "auto_recovery_threshold": 3,
  "max_recovery_attempts": 3,
  "recovery_interval_seconds": 300,
  "monitor_interval_seconds": 30,
  "updated_at": 1701234567
}
```

#### PUT /api/v1/config

更新高可用性配置。

**请求体:**
```json
{
  "enable_auto_recovery": false,
  "auto_recovery_threshold": 5,
  "max_recovery_attempts": 2,
  "recovery_interval_seconds": 600,
  "monitor_interval_seconds": 60
}
```

**所有字段都是可选的，只需提供要更新的字段。**

### 5. 安全监控

#### GET /api/v1/security/attacks

获取攻击检测结果。

**响应示例:**
```json
{
  "indicators": [
    {
      "indicator_type": "unusual_transaction_pattern",
      "severity": "Medium",
      "description": "检测到异常交易模式",
      "detected_at": 1701234567
    }
  ],
  "threat_level": "Low",
  "attack_types": ["ddos_attempt"],
  "recommended_actions": ["增强监控", "启用速率限制"]
}
```

#### GET /api/v1/security/threats

获取威胁分析结果。

**响应示例:**
```json
{
  "threat_level": "Low",
  "active_threats": 0,
  "blocked_attacks": 0,
  "risk_score": 15,
  "recommendations": ["保持当前安全配置"]
}
```

### 6. 告警管理

#### GET /api/v1/alerts

获取告警历史。

**查询参数:**
- `page`: 页数 (默认: 1)
- `per_page`: 每页大小 (默认: 20)
- `filter`: 过滤条件 (可选)

**响应示例:**
```json
{
  "alerts": [
    {
      "id": "alert-123",
      "level": "Warning",
      "message": "CPU使用率过高",
      "component": "execution",
      "channel": "email",
      "created_at": 1701234567,
      "acknowledged": false
    }
  ],
  "total_count": 1,
  "pagination": {
    "page": 1,
    "per_page": 20,
    "total_pages": 1,
    "has_next": false
  }
}
```

#### POST /api/v1/alerts/{id}/acknowledge

确认指定的告警。

**响应示例:**
```json
{
  "status": "acknowledged",
  "alert_id": "alert-123",
  "acknowledged_at": 1701234567
}
```

### 7. 回滚操作

#### POST /api/v1/rollback

触发回滚操作。

**请求体:**
```json
{
  "checkpoint": 12345,
  "force": false,
  "reason": "回滚到稳定检查点"
}
```

**响应示例:**
```json
{
  "status": "started",
  "operation_id": "rollback-1701234567890",
  "checkpoint": 12345,
  "force": false,
  "estimated_duration": 300
}
```

#### GET /api/v1/rollback/status

获取回滚操作状态。

**响应示例:**
```json
{
  "status": "idle",
  "current_operation": null,
  "progress": 0,
  "last_checkpoint": null
}
```

### 8. 冷启动操作

#### POST /api/v1/cold-start

触发冷启动操作。

**请求体:**
```json
{
  "force": false,
  "discovery_timeout": 60,
  "sync_timeout": 300,
  "max_retries": 3
}
```

**响应示例:**
```json
{
  "status": "started",
  "operation_id": "cold-start-1701234567890",
  "force": false,
  "estimated_duration": 600
}
```

#### GET /api/v1/cold-start/status

获取冷启动状态。

**响应示例:**
```json
{
  "status": "idle",
  "phase": "none",
  "progress": 0,
  "discovery_timeout": 60,
  "sync_timeout": 300
}
```

### 9. 网络监控

#### GET /api/v1/network/status

获取网络状态信息。

**响应示例:**
```json
{
  "network_healthy": true,
  "connected_peers": 10,
  "network_latency": 50,
  "bandwidth_usage": "15%",
  "consensus_participation": "100%"
}
```

#### GET /api/v1/network/health

获取网络健康评估。

**响应示例:**
```json
{
  "overall_healthy": true,
  "health_score": 98,
  "network_issues": [],
  "recommendations": ["网络状态良好"],
  "last_check": 1701234567
}
```

---

## 错误处理

### HTTP状态码

- `200 OK`: 请求成功
- `400 Bad Request`: 请求参数错误
- `401 Unauthorized`: 未授权 (暂未实现)
- `404 Not Found`: 资源不存在
- `500 Internal Server Error`: 服务器内部错误

### 错误响应格式

```json
{
  "error": {
    "code": "INVALID_STRATEGY",
    "message": "不支持的恢复策略",
    "details": "策略必须是 auto, rollback, cold_start, restart 之一"
  }
}
```

---

## 使用示例

### 检查系统健康状态

```bash
curl -X GET http://localhost:8080/api/v1/health
```

### 触发自动恢复

```bash
curl -X POST http://localhost:8080/api/v1/recovery \
  -H "Content-Type: application/json" \
  -d '{
    "strategy": "auto",
    "force": false,
    "reason": "预防性恢复"
  }'
```

### 回滚到特定检查点

```bash
curl -X POST http://localhost:8080/api/v1/rollback \
  -H "Content-Type: application/json" \
  -d '{
    "checkpoint": 12345,
    "force": false,
    "reason": "回滚到稳定状态"
  }'
```

### 更新配置

```bash
curl -X PUT http://localhost:8080/api/v1/config \
  -H "Content-Type: application/json" \
  -d '{
    "enable_auto_recovery": true,
    "auto_recovery_threshold": 5
  }'
```

### 获取告警历史

```bash
curl -X GET "http://localhost:8080/api/v1/alerts?page=1&per_page=10"
```

---

## 集成指南

### 启动API服务器

```rust
use mgo_core::high_availability_api::create_api_router;
use mgo_core::integration::HighAvailabilityManager;
use axum::Server;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    // 创建高可用性管理器
    let ha_manager = Arc::new(HighAvailabilityManager::new(/* 参数 */));
    
    // 创建API路由
    let app = create_api_router(ha_manager);
    
    // 启动服务器
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    println!("API服务器启动在 http://{}", addr);
    
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

### 客户端SDK示例

```rust
use reqwest::Client;
use serde_json::json;

pub struct MgoHaClient {
    client: Client,
    base_url: String,
}

impl MgoHaClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }
    
    pub async fn health_check(&self) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let url = format!("{}/api/v1/health", self.base_url);
        let response = self.client.get(&url).send().await?;
        let json = response.json().await?;
        Ok(json)
    }
    
    pub async fn trigger_recovery(&self, strategy: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let url = format!("{}/api/v1/recovery", self.base_url);
        let payload = json!({
            "strategy": strategy,
            "force": false
        });
        
        let response = self.client
            .post(&url)
            .json(&payload)
            .send()
            .await?;
            
        let json = response.json().await?;
        Ok(json)
    }
}
```

---

## 监控和日志

### 日志格式

API服务器使用结构化日志格式，包含以下字段：

- `timestamp`: 时间戳
- `level`: 日志级别 (INFO, WARN, ERROR)
- `target`: 日志来源
- `message`: 日志消息
- `span`: 跟踪信息

### 指标收集

API服务器自动收集以下指标：

- HTTP请求计数和延迟
- 错误率
- 活跃连接数
- 恢复操作统计
- 健康检查结果

### 告警配置

建议监控以下指标并设置告警：

- API错误率 > 5%
- 响应时间 > 1秒
- 系统健康分数 < 80
- 连续恢复失败 >= 3次

---

## 安全考虑

### 认证和授权

虽然当前版本未实现认证，生产环境建议：

- 使用JWT或API密钥认证
- 实施基于角色的访问控制(RBAC)
- 限制敏感操作的访问权限

### 网络安全

- 在生产环境中使用HTTPS
- 配置防火墙规则
- 启用访问日志监控

### 数据保护

- 敏感信息不记录到日志中
- 使用加密存储配置数据
- 定期备份关键数据

---

## 故障排除

### 常见问题

**Q: API返回500错误**
A: 检查服务器日志，确认高可用性管理器初始化正确。

**Q: 恢复操作无响应**
A: 检查 `/recovery/status` 端点获取详细状态信息。

**Q: 健康检查返回异常**
A: 验证节点配置和网络连接状态。

### 调试技巧

1. 启用详细日志模式
2. 使用 `/health/detailed` 获取完整诊断信息
3. 检查系统资源使用情况
4. 验证网络连接和端口配置

---

## 更新日志

### v1.0.0 (当前版本)
- 初始API实现
- 健康检查和状态监控
- 恢复操作管理
- 安全监控接口
- 告警管理功能

---

## 联系支持

如需技术支持或报告问题，请联系：

- 邮箱: support@mangonetlabs.com
- 文档: https://docs.mangonetlabs.com
- GitHub: https://github.com/MangoNetworkLabs/mango