# Mango Network 高可用性系统使用指南

## 概述

Mango Network 高可用性系统提供了自动故障检测和恢复功能，包括：
- 健康监控和状态检查
- 攻击检测和安全防护
- 自动回滚和冷启动恢复
- 智能告警和通知系统

## 快速开始

### 1. 基本使用示例

```rust
use std::sync::Arc;
use prometheus::Registry;

use mgo_core::integration::{HighAvailabilityManager, HighAvailabilityConfig};
use mgo_core::authority::AuthorityState;
use mgo_core::checkpoints::CheckpointStore;
use mgo_core::authority_client::NetworkAuthorityClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 假设您已经有了这些组件
    let authority_state = get_authority_state().await;
    let checkpoint_store = get_checkpoint_store().await;
    let network_client = get_network_client().await;
    let registry = Registry::new();

    // 创建高可用性配置
    let config = HighAvailabilityConfig {
        enable_auto_recovery: true,
        auto_recovery_threshold: 3, // 连续3次健康检查失败后触发恢复
        max_recovery_attempts: 3,   // 最多尝试3次恢复
        recovery_interval: Duration::from_secs(300), // 5分钟恢复间隔
        ..Default::default()
    };

    // 创建高可用性管理器
    let ha_manager = HighAvailabilityManager::new(
        config,
        authority_state,
        checkpoint_store,
        network_client,
        &registry,
    )?;

    // 启动系统
    ha_manager.start().await?;
    
    println!("高可用性系统已启动");

    // 系统现在会自动监控和恢复
    // 您可以继续运行您的应用程序...

    // 在关闭时停止系统
    ha_manager.stop().await?;
    
    Ok(())
}
```

### 2. 与 MgoNode 集成

```rust
use mgo_node::high_availability::{
    MgoNodeHighAvailabilityBuilder, 
    HealthSummary
};
use mgo_core::integration::HighAvailabilityConfig;

async fn integrate_with_mgo_node() -> Result<(), Box<dyn std::error::Error>> {
    // 假设您有一个运行中的 MgoNode
    let node = get_mgo_node().await;
    
    // 创建高可用性配置
    let ha_config = HighAvailabilityConfig {
        enable_auto_recovery: true,
        auto_recovery_threshold: 2, // 更敏感的触发条件
        ..Default::default()
    };

    // 使用构建器模式创建高可用性扩展
    let ha_extension = MgoNodeHighAvailabilityBuilder::new()
        .with_node_name("validator-1".to_string())
        .with_config(ha_config)
        .enable_auto_recovery()
        .build(
            node.state.clone(),
            node.checkpoint_store.clone(),
            // 注意：需要根据实际实现提供网络客户端
            create_network_client(),
            node.registry_service.default_registry(),
        )?;

    // 启动高可用性管理
    ha_extension.start().await?;

    // 定期检查健康状态
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            
            match ha_extension.get_health_summary().await {
                Ok(summary) => {
                    println!("节点健康报告:\n{}", summary.generate_report());
                    
                    if !summary.is_healthy {
                        println!("⚠️  节点健康状态异常，自动恢复系统将处理");
                    }
                }
                Err(e) => {
                    println!("获取健康状态失败: {:?}", e);
                }
            }
        }
    });

    Ok(())
}
```

### 3. 手动操作示例

```rust
// 手动触发健康检查
let health_status = ha_manager.perform_health_check().await?;
println!("健康分数: {}/100", health_status.health_score());

// 手动执行回滚
let target_checkpoint = 12345;
match ha_manager.execute_rollback(target_checkpoint, false).await {
    Ok(result) => println!("回滚成功: {:?}", result),
    Err(e) => println!("回滚失败: {:?}", e),
}

// 手动执行冷启动
match ha_manager.execute_cold_start().await {
    Ok(result) => println!("冷启动成功: {:?}", result),
    Err(e) => println!("冷启动失败: {:?}", e),
}

// 获取系统状态
let system_state = ha_manager.get_system_state().await;
println!("当前系统状态: {:?}", system_state);
```

## 配置选项

### HighAvailabilityConfig

```rust
use mgo_core::integration::HighAvailabilityConfig;
use std::time::Duration;

let config = HighAvailabilityConfig {
    // 回滚配置
    rollback: RollbackConfig {
        force: false,                              // 是否强制回滚
        timeout: Duration::from_secs(300),         // 回滚超时
        auto_restart_consensus: true,              // 回滚后自动重启共识
        auto_sync_network: true,                   // 回滚后自动同步网络
    },
    
    // 冷启动配置
    cold_start: ColdStartConfig {
        discovery_timeout: Duration::from_secs(60),     // 节点发现超时
        sync_timeout: Duration::from_secs(300),         // 状态同步超时
        consensus_restart_timeout: Duration::from_secs(120), // 共识重启超时
        max_retry_attempts: 3,                          // 最大重试次数
        auto_cold_start: false,                         // 是否自动冷启动
    },
    
    // 监控配置
    monitor: MonitorConfig {
        monitor_interval: Duration::from_secs(30),      // 监控间隔
        enable_attack_detection: true,                  // 启用攻击检测
        health_check: HealthCheckConfig {
            timeout: Duration::from_secs(10),           // 健康检查超时
            enable_detailed_check: true,                // 启用详细检查
            ..Default::default()
        },
        alert: AlertConfig {
            min_alert_level: AlertLevel::Warning,       // 最小告警级别
            max_retry_attempts: 3,                      // 告警重试次数
            ..Default::default()
        },
    },
    
    // 自动恢复配置
    enable_auto_recovery: true,                         // 启用自动恢复
    auto_recovery_threshold: 3,                         // 触发恢复的失败次数
    recovery_interval: Duration::from_secs(300),        // 恢复间隔
    max_recovery_attempts: 3,                           // 最大恢复尝试次数
};
```

## 监控和告警

### 健康状态监控

```rust
// 实时监控健康状态
async fn monitor_health(ha_manager: &HighAvailabilityManager) {
    loop {
        match ha_manager.perform_health_check().await {
            Ok(status) => {
                println!("🏥 健康检查报告:");
                println!("  总分: {}/100", status.health_score());
                println!("  共识: {}", if status.consensus_healthy { "✅" } else { "❌" });
                println!("  网络: {}", if status.network_healthy { "✅" } else { "❌" });
                println!("  存储: {}", if status.storage_healthy { "✅" } else { "❌" });
                println!("  执行: {}", if status.execution_healthy { "✅" } else { "❌" });
                
                if !status.error_details.is_empty() {
                    println!("  错误详情:");
                    for error in &status.error_details {
                        println!("    - {}", error);
                    }
                }
            }
            Err(e) => {
                println!("❌ 健康检查失败: {:?}", e);
            }
        }
        
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
```

### 告警通知设置

```rust
use mgo_core::health_monitor::{AlertConfig, AlertChannel, AlertLevel};

let alert_config = AlertConfig {
    enabled_channels: vec![
        AlertChannel::Log,                    // 记录到日志
        AlertChannel::Console,                // 输出到控制台
        AlertChannel::File("/var/log/mgo/alerts.log".to_string()), // 写入文件
        // AlertChannel::Email("admin@example.com".to_string()),    // 邮件通知
        // AlertChannel::Webhook("https://hooks.slack.com/...".to_string()), // Slack通知
    ],
    min_alert_level: AlertLevel::Warning,    // 只发送警告级别以上的告警
    max_retry_attempts: 3,                   // 最多重试3次
    retry_interval: Duration::from_secs(30), // 重试间隔30秒
    deduplication_window: Duration::from_secs(300), // 5分钟内相同告警去重
    ..Default::default()
};
```

## 故障恢复策略

系统会根据健康状态自动选择合适的恢复策略：

### 1. 无需恢复 (健康分数 ≥ 75)
- 系统运行正常
- 继续监控

### 2. 重启服务 (健康分数 50-74)
- 服务降级但仍可运行
- 建议重启相关服务

### 3. 回滚恢复 (健康分数 < 50)
- 回滚到最近的安全检查点
- 适用于数据不一致问题

### 4. 冷启动恢复 (严重故障)
- 从网络重新同步状态
- 适用于严重的系统损坏

### 5. 混合策略 (检测到攻击)
- 先回滚再冷启动
- 最大程度保证安全性

## 性能和资源考虑

### 监控开销

- 健康检查：通常在 10-100ms 内完成
- 攻击检测：轻量级模式检测，开销最小
- 内存使用：约增加 5-10MB
- CPU 使用：正常情况下 < 1%

### 建议的监控间隔

```rust
// 生产环境推荐配置
let production_config = MonitorConfig {
    monitor_interval: Duration::from_secs(30),     // 30秒监控间隔
    enable_attack_detection: true,                 // 启用攻击检测
    ..Default::default()
};

// 开发环境可以更频繁
let development_config = MonitorConfig {
    monitor_interval: Duration::from_secs(10),     // 10秒监控间隔
    enable_attack_detection: false,                // 开发时可关闭
    ..Default::default()
};
```

## 故障排除

### 常见问题

1. **健康检查持续失败**
   ```rust
   // 检查具体的健康状态
   let status = ha_manager.perform_health_check().await?;
   for error in &status.error_details {
       println!("Error: {}", error);
   }
   ```

2. **自动恢复不工作**
   ```rust
   // 检查恢复尝试次数
   let attempts = ha_manager.get_recovery_attempts().await;
   if attempts >= ha_manager.get_config().max_recovery_attempts {
       println!("已达到最大恢复尝试次数，需要手动干预");
       ha_manager.reset_recovery_attempts().await;
   }
   ```

3. **网络连接问题**
   ```rust
   // 手动触发冷启动以重新发现网络
   match ha_manager.execute_cold_start().await {
       Ok(_) => println!("冷启动成功，网络应该已恢复"),
       Err(e) => println!("冷启动失败: {:?}", e),
   }
   ```

### 调试模式

```rust
// 启用详细日志
env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

// 禁用自动恢复以进行手动调试
let debug_config = HighAvailabilityConfig {
    enable_auto_recovery: false,
    ..Default::default()
};
```

## 最佳实践

1. **渐进式部署**：先在测试环境验证配置
2. **监控告警**：设置适当的告警通道和级别
3. **定期测试**：定期测试故障恢复功能
4. **备份策略**：确保有可靠的数据备份
5. **文档维护**：记录自定义配置和操作过程

## API 参考

完整的 API 文档请参考各模块的 rustdoc 文档：

- `mgo_core::integration` - 核心集成模块
- `mgo_core::health_monitor` - 健康监控
- `mgo_core::rollback` - 回滚管理
- `mgo_core::cold_start` - 冷启动管理
- `mgo_node::high_availability` - 节点集成

## 支持和贡献

如果您遇到问题或有改进建议，请：

1. 查看现有的 issue 和文档
2. 创建新的 issue 描述问题
3. 提交 Pull Request 贡献代码

---

*这个文档会持续更新以反映系统的最新功能和最佳实践。*