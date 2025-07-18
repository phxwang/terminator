#!/bin/bash

# 性能基准测试脚本
# 比较不同构建配置的性能

set -e

echo "🔬 开始性能基准测试..."

# 清理之前的构建
cargo clean

profiles=("release" "performance" "native")
results_file="benchmark_results.txt"

echo "基准测试结果 - $(date)" > $results_file
echo "================================" >> $results_file

for profile in "${profiles[@]}"; do
    echo "🧪 测试 Profile: $profile"
    
    # 记录构建时间
    echo "Profile: $profile" >> $results_file
    echo "构建开始时间: $(date)" >> $results_file
    
    start_time=$(date +%s)
    
    case $profile in
        "release")
            cargo build --release
            binary_path="target/release/klend-terminator"
            ;;
        "performance")
            cargo build --profile performance
            binary_path="target/performance/klend-terminator"
            ;;
        "native")
            export RUSTFLAGS="-C target-cpu=native"
            cargo build --profile native
            binary_path="target/native/klend-terminator"
            unset RUSTFLAGS
            ;;
    esac
    
    end_time=$(date +%s)
    build_time=$((end_time - start_time))
    
    echo "构建时间: ${build_time}秒" >> $results_file
    
    if [ -f "$binary_path" ]; then
        file_size=$(stat -c%s "$binary_path")
        echo "二进制大小: $file_size 字节" >> $results_file
        echo "二进制大小 (人类可读): $(numfmt --to=iec-i --suffix=B $file_size)" >> $results_file
        
        # 检查二进制文件的优化信息
        echo "文件信息:" >> $results_file
        file "$binary_path" >> $results_file
        
        # 如果有 objdump，显示更多信息
        if command -v objdump &> /dev/null; then
            echo "段信息:" >> $results_file
            objdump -h "$binary_path" | grep -E '\.(text|data|bss)' >> $results_file 2>/dev/null || true
        fi
    fi
    
    echo "--------------------------------" >> $results_file
    echo ""
done

echo "✅ 基准测试完成!"
echo "📊 结果保存在: $results_file"
cat $results_file
