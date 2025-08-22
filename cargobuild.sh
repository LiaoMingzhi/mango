echo "🔧 第1步：编译核心依赖..."
cargo build --package mgo-common

echo "🔧 第2步：编译核心模块..."
cargo build --package mgo-core --features test-utils

echo "🔧 第3步：编译网络模块..."
cargo build --package mgo-network

echo "🔧 第4步：编译测试验证模块..."
cargo build --package mgo-test-validator --jobs 1

echo "🔧 第5步：编译快照回滚模块..."
cargo build --package mgo-snapshot --jobs 1

echo "🔧 第6步：编译节点..."
cargo build --package mgo-node --jobs 1

echo "🔧 第7步：编译主程序..."
cargo build --package mgo  --jobs 1

echo "✅ 分步编译完成！"
