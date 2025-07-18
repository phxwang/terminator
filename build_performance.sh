#!/bin/bash

# 高性能 Cargo 构建脚本
# 使用方法: ./build_performance.sh [profile]

set -e

PROFILE=${1:-release}

echo "🚀 开始高性能构建..."
echo "使用 Profile: $PROFILE"

# 设置环境变量以获得最佳性能
export CARGO_PROFILE_RELEASE_LTO=fat
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_PANIC=abort

# 针对当前 CPU 架构优化 (移除 crt-static 以避免 proc-macro 冲突)
export RUSTFLAGS="-C target-cpu=native"

# 使用系统链接器优化
if command -v mold &> /dev/null; then
    echo "🔗 使用 mold 链接器"
    export RUSTFLAGS="$RUSTFLAGS -C link-arg=-fuse-ld=mold"
elif command -v lld &> /dev/null; then
    echo "🔗 使用 lld 链接器"
    export RUSTFLAGS="$RUSTFLAGS -C link-arg=-fuse-ld=lld"
fi

# 并行编译优化
export CARGO_BUILD_JOBS=$(nproc)

echo "🔧 环境变量设置:"
echo "  RUSTFLAGS: $RUSTFLAGS"
echo "  CARGO_BUILD_JOBS: $CARGO_BUILD_JOBS"

case $PROFILE in
    "release")
        echo "📦 构建 Release 版本..."
        cargo build --release
        ;;
    "performance")
        echo "⚡ 构建高性能版本..."
        cargo build --profile performance
        ;;
    "native")
        echo "🎯 构建 CPU 优化版本..."
        export RUSTFLAGS="$RUSTFLAGS -C target-cpu=native"
        cargo build --profile native
        ;;
    "size")
        echo "📏 构建最小体积版本..."
        export RUSTFLAGS="-C opt-level=z -C target-cpu=native -C strip=symbols"
        cargo build --release
        ;;
    "static")
        echo "🔒 构建静态链接版本..."
        # 只在最终二进制构建时使用 crt-static，避免 proc-macro 冲突
        export RUSTFLAGS="-C target-cpu=native -C target-feature=+crt-static"
        cargo build --release
        ;;
    *)
        echo "❌ 未知的 profile: $PROFILE"
        echo "可用选项: release, performance, native, size, static"
        exit 1
        ;;
esac

echo "✅ 构建完成!"

# 显示二进制文件信息
if [ -f "target/release/klend-terminator" ]; then
    echo "📊 二进制文件信息:"
    ls -lh target/release/klend-terminator
    file target/release/klend-terminator
fi
