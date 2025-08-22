#!/bin/bash

# 快照回滚操作示例脚本
# 用法说明：checkpoint回滚和epoch回滚

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
check_mgo_command() {
    if ! command -v cargo &> /dev/null; then
        log_error "cargo命令未找到，请确保Rust已安装"
        exit 1
    fi
    log_info "检查mgo命令可用性..."
}

# 1. 查看可用快照
list_snapshots() {
    log_info "📋 查看可用快照..."
    cargo run --package mgo --bin mgo -- snapshot list --json
}

# 2. 创建checkpoint快照（用于回滚）
create_checkpoint_snapshot() {
    local checkpoint_num=$1
    log_info "📸 创建checkpoint $checkpoint_num 快照..."
    
    cargo run --package mgo --bin mgo -- snapshot create \
        --type checkpoint \
        --checkpoint $checkpoint_num \
        --include-transactions \
        --include-committee \
        --compression-level 6 \
        --json
}

# 3. 创建epoch快照（用于回滚）
create_epoch_snapshot() {
    local epoch_num=$1
    log_info "📸 创建epoch $epoch_num 快照..."
    
    cargo run --package mgo --bin mgo -- snapshot create \
        --type epoch \
        --epoch $epoch_num \
        --include-transactions \
        --include-committee \
        --compression-level 6 \
        --json
}

# 4. 执行checkpoint回滚
rollback_to_checkpoint() {
    local snapshot_id=$1
    local validation_level=${2:-"full"}
    
    log_warning "🔄 开始checkpoint回滚到快照: $snapshot_id"
    log_info "验证级别: $validation_level"
    
    # 创建当前状态备份
    log_info "💾 创建当前状态备份..."
    create_checkpoint_snapshot $(date +%s)
    
    # 执行回滚
    log_info "⚡ 执行回滚操作..."
    cargo run --package mgo --bin mgo -- snapshot restore $snapshot_id \
        --validation $validation_level \
        --backup-current \
        --max-retries 5 \
        --timeout 600 \
        --json
    
    # 验证回滚结果
    log_info "✅ 验证回滚结果..."
    cargo run --package mgo --bin mgo -- snapshot verify $snapshot_id --json
    
    log_success "Checkpoint回滚完成！"
}

# 5. 执行epoch回滚
rollback_to_epoch() {
    local snapshot_id=$1
    local validation_level=${2:-"full"}
    
    log_warning "🕐 开始epoch回滚到快照: $snapshot_id"
    log_info "验证级别: $validation_level"
    
    # 创建当前状态备份
    log_info "💾 创建当前状态备份..."
    create_epoch_snapshot $(date +%s)
    
    # 执行回滚
    log_info "⚡ 执行回滚操作..."
    cargo run --package mgo --bin mgo -- snapshot restore $snapshot_id \
        --validation $validation_level \
        --backup-current \
        --max-retries 3 \
        --timeout 900 \
        --json
    
    # 验证回滚结果
    log_info "✅ 验证回滚结果..."
    cargo run --package mgo --bin mgo -- snapshot verify $snapshot_id --json
    
    log_success "Epoch回滚完成！"
}

# 6. 监控回滚状态
monitor_rollback() {
    log_info "📊 监控回滚状态..."
    
    while true; do
        status=$(cargo run --package mgo --bin mgo -- snapshot restore-status --json 2>/dev/null | jq -r '.status' 2>/dev/null || echo "completed")
        
        case $status in
            "in_progress")
                log_info "回滚进行中..."
                sleep 5
                ;;
            "completed")
                log_success "回滚已完成！"
                break
                ;;
            "failed")
                log_error "回滚失败！"
                break
                ;;
            *)
                log_info "状态未知，停止监控"
                break
                ;;
        esac
    done
}

# 7. 紧急回滚（强制模式）
emergency_rollback() {
    local snapshot_id=$1
    
    log_error "🚨 执行紧急回滚到快照: $snapshot_id"
    log_warning "⚠️  使用强制模式，跳过安全检查！"
    
    cargo run --package mgo --bin mgo -- snapshot restore $snapshot_id \
        --force \
        --timeout 300 \
        --json
    
    log_warning "紧急回滚执行完毕，请立即验证系统状态！"
}

# 8. 系统健康检查
health_check() {
    log_info "🏥 执行系统健康检查..."
    
    cargo run --package mgo --bin mgo -- snapshot health --json
    cargo run --package mgo --bin mgo -- snapshot metrics --json
    cargo run --package mgo --bin mgo -- snapshot storage-info --json
}

# 主函数 - 演示用法
main() {
    log_info "🚀 快照回滚操作演示"
    
    check_mgo_command
    
    echo ""
    log_info "=== 1. 查看可用快照 ==="
    list_snapshots
    
    echo ""
    log_info "=== 2. 创建演示快照 ==="
    create_checkpoint_snapshot 12345
    create_epoch_snapshot 100
    
    echo ""
    log_info "=== 3. 演示checkpoint回滚 ==="
    # rollback_to_checkpoint "snap-checkpoint-12345" "basic"
    
    echo ""
    log_info "=== 4. 演示epoch回滚 ==="
    # rollback_to_epoch "snap-epoch-100" "full"
    
    echo ""
    log_info "=== 5. 系统健康检查 ==="
    health_check
    
    log_success "演示完成！"
}

# 使用说明
usage() {
    echo "用法: $0 [command] [args...]"
    echo ""
    echo "命令："
    echo "  list                     - 查看可用快照"
    echo "  create-checkpoint <num>  - 创建checkpoint快照"
    echo "  create-epoch <num>       - 创建epoch快照"
    echo "  rollback-checkpoint <id> - 回滚到checkpoint"
    echo "  rollback-epoch <id>      - 回滚到epoch"
    echo "  monitor                  - 监控回滚状态"
    echo "  emergency <id>           - 紧急回滚"
    echo "  health                   - 健康检查"
    echo "  demo                     - 运行演示"
    echo ""
    echo "示例："
    echo "  $0 create-checkpoint 12345"
    echo "  $0 rollback-checkpoint snap-checkpoint-12345"
    echo "  $0 rollback-epoch snap-epoch-100"
    echo "  $0 emergency snap-checkpoint-safe"
}

# 命令行参数处理
case "${1:-demo}" in
    "list")
        list_snapshots
        ;;
    "create-checkpoint")
        if [ -z "$2" ]; then
            log_error "请提供checkpoint号码"
            exit 1
        fi
        create_checkpoint_snapshot "$2"
        ;;
    "create-epoch")
        if [ -z "$2" ]; then
            log_error "请提供epoch号码"
            exit 1
        fi
        create_epoch_snapshot "$2"
        ;;
    "rollback-checkpoint")
        if [ -z "$2" ]; then
            log_error "请提供快照ID"
            exit 1
        fi
        rollback_to_checkpoint "$2" "${3:-full}"
        ;;
    "rollback-epoch")
        if [ -z "$2" ]; then
            log_error "请提供快照ID"
            exit 1
        fi
        rollback_to_epoch "$2" "${3:-full}"
        ;;
    "monitor")
        monitor_rollback
        ;;
    "emergency")
        if [ -z "$2" ]; then
            log_error "请提供快照ID"
            exit 1
        fi
        emergency_rollback "$2"
        ;;
    "health")
        health_check
        ;;
    "demo")
        main
        ;;
    "help"|"-h"|"--help")
        usage
        ;;
    *)
        log_error "未知命令: $1"
        usage
        exit 1
        ;;
esac
