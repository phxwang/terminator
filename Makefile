# 高性能 Rust 构建 Makefile

.PHONY: help build release performance native size clean benchmark install-tools

# 默认目标
help:
	@echo "🚀 高性能 Rust 构建工具"
	@echo ""
	@echo "可用命令:"
	@echo "  make build        - 标准 release 构建"
	@echo "  make performance  - 高性能优化构建"
	@echo "  make native       - CPU 特定优化构建"
	@echo "  make size         - 最小体积构建"
	@echo "  make benchmark    - 运行性能基准测试"
	@echo "  make clean        - 清理构建文件"
	@echo "  make install-tools - 安装性能工具"

# 设置通用环境变量
export CARGO_BUILD_JOBS := $(shell nproc)

# 标准 release 构建
build:
	@echo "📦 构建 Release 版本..."
	cargo build --release

# 高性能构建
performance:
	@echo "⚡ 构建高性能版本..."
	RUSTFLAGS="-C target-cpu=native" \
	cargo build --profile performance

# CPU 特定优化构建
native:
	@echo "🎯 构建 CPU 优化版本..."
	RUSTFLAGS="-C target-cpu=native" \
	cargo build --profile native

# 静态链接构建（避免 proc-macro 冲突）
static:
	@echo "🔒 构建静态链接版本..."
	RUSTFLAGS="-C target-cpu=native -C target-feature=+crt-static" \
	cargo build --release

# 最小体积构建
size:
	@echo "📏 构建最小体积版本..."
	RUSTFLAGS="-C opt-level=z -C target-cpu=native -C strip=symbols" \
	cargo build --release

# 使用 mold 链接器构建（如果可用）
mold:
	@echo "🔗 使用 mold 链接器构建..."
	@if command -v mold >/dev/null 2>&1; then \
		RUSTFLAGS="-C target-cpu=native -C link-arg=-fuse-ld=mold" \
		cargo build --profile performance; \
	else \
		echo "❌ mold 链接器未安装"; \
		exit 1; \
	fi

# 使用 lld 链接器构建
lld:
	@echo "🔗 使用 lld 链接器构建..."
	@if command -v lld >/dev/null 2>&1; then \
		RUSTFLAGS="-C target-cpu=native -C link-arg=-fuse-ld=lld" \
		cargo build --profile performance; \
	else \
		echo "❌ lld 链接器未安装"; \
		exit 1; \
	fi

# 运行基准测试
benchmark:
	@echo "🔬 运行性能基准测试..."
	./benchmark_builds.sh

# 清理构建文件
clean:
	@echo "🧹 清理构建文件..."
	cargo clean

# 安装性能工具
install-tools:
	@echo "🛠️ 安装性能优化工具..."
	@echo "安装 mold 链接器..."
	@if command -v apt >/dev/null 2>&1; then \
		sudo apt update && sudo apt install -y mold; \
	elif command -v yum >/dev/null 2>&1; then \
		sudo yum install -y mold; \
	elif command -v brew >/dev/null 2>&1; then \
		brew install mold; \
	else \
		echo "请手动安装 mold 链接器"; \
	fi
	@echo "安装 cargo-bloat (分析二进制大小)..."
	cargo install cargo-bloat
	@echo "安装 cargo-audit (安全审计)..."
	cargo install cargo-audit

# 分析二进制大小
analyze-size:
	@echo "📊 分析二进制大小..."
	@if command -v cargo-bloat >/dev/null 2>&1; then \
		cargo bloat --release; \
	else \
		echo "请先运行 'make install-tools' 安装 cargo-bloat"; \
	fi

# 显示构建信息
info:
	@echo "ℹ️ 构建环境信息:"
	@echo "Rust 版本: $$(rustc --version)"
	@echo "Cargo 版本: $$(cargo --version)"
	@echo "CPU 核心数: $(CARGO_BUILD_JOBS)"
	@echo "目标架构: $$(rustc --print target-list | grep $$(rustc -vV | grep host | cut -d' ' -f2))"
