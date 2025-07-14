# Solana Instruction Data 解析和修改指南

## 概述

Solana instruction 的 `data` 字段是二进制数据，不是 JSON 格式。这个指南将展示如何正确地解析和修改 instruction data。

## 为什么不能用 JSON 解析？

```rust
// ❌ 错误的方式 - instruction data 不是 JSON
let data = serde_json::from_slice(&ix.data).unwrap(); // 这会失败！

// ✅ 正确的方式 - 解析二进制数据
let parsed_data = parse_instruction_data(&ix.data, &ix.program_id);
```

## Instruction Data 结构

不同程序的 instruction data 有不同的结构：

### 1. Anchor 程序（如 Jupiter）
```
[discriminator: 8 bytes] [instruction_data: variable length]
```

### 2. Native 程序（如 ComputeBudget, Token）
```
[instruction_type: 1 byte] [instruction_data: variable length]
```

### 3. 自定义程序
根据程序的具体实现而定。

## 使用方法

### 基本解析

```rust
use crate::instruction_parser::{parse_instruction_data, InstructionType};

// 解析 instruction data
let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

match parsed.instruction_type {
    InstructionType::Jupiter(jupiter_type) => {
        println!("这是 Jupiter 指令: {:?}", jupiter_type);
        // 处理 Jupiter 相关逻辑
    }
    InstructionType::ComputeBudget(compute_type) => {
        println!("这是 ComputeBudget 指令: {:?}", compute_type);
        // 处理 ComputeBudget 相关逻辑
    }
    InstructionType::Token(token_type) => {
        println!("这是 Token 指令: {:?}", token_type);
        // 处理 Token 相关逻辑
    }
    InstructionType::Unknown => {
        println!("未知指令类型");
        // 通用处理逻辑
    }
}

// 查看解析出的字段
for field in &parsed.parsed_fields {
    match &field.value {
        FieldValue::U64(val) => println!("{}: {}", field.name, val),
        FieldValue::U32(val) => println!("{}: {}", field.name, val),
        FieldValue::Pubkey(pubkey) => println!("{}: {}", field.name, pubkey),
        _ => println!("{}: {:?}", field.name, field.value),
    }
}
```

### 修改 Instruction Data

```rust
use crate::instruction_parser::{
    modify_jupiter_slippage, modify_jupiter_amount, modify_compute_unit_limit,
    FieldValue, modify_instruction_data
};

// 修改 Jupiter swap 的滑点
let mut jupiter_ix = /* your jupiter instruction */;
modify_jupiter_slippage(&mut jupiter_ix, 100).unwrap(); // 1% 滑点

// 修改 Jupiter swap 的金额
modify_jupiter_amount(&mut jupiter_ix, 1_000_000).unwrap();

// 修改 ComputeBudget 限制
let mut compute_ix = /* your compute budget instruction */;
modify_compute_unit_limit(&mut compute_ix, 200_000).unwrap();

// 通用字段修改
modify_instruction_data(
    &mut instruction,
    "field_name",
    FieldValue::U64(new_value)
).unwrap();
```

### 查看原始数据

```rust
// 以十六进制查看原始数据
let hex_data = hex::encode(&instruction.data);
println!("原始数据 (hex): {}", hex_data);

// 查看数据长度和前几个字节
println!("数据长度: {}", instruction.data.len());
println!("前8字节: {:?}", &instruction.data[0..8.min(instruction.data.len())]);
```

## 支持的程序类型

### 1. Jupiter

支持的指令类型：
- `Route` - 主要的 swap 指令
- `SharedAccountsRoute`
- `SetTokenLedger`

可解析的字段：
- `route_plan_length` - 路由计划长度
- `in_amount` - 输入金额
- `quoted_out_amount` - 预期输出金额
- `slippage_bps` - 滑点（基点）
- `platform_fee_bps` - 平台费用（基点）

### 2. ComputeBudget

支持的指令类型：
- `SetComputeUnitLimit` - 设置计算单位限制
- `SetComputeUnitPrice` - 设置计算单位价格
- `RequestUnits` - 请求计算单位（已废弃）
- `RequestHeapFrame` - 请求堆框架

可解析的字段：
- `compute_unit_limit` - 计算单位限制
- `compute_unit_price` - 计算单位价格（微 lamports）

### 3. Token

支持的指令类型：
- `Transfer` - 转账
- `Approve` - 授权
- `Close` - 关闭账户

可解析的字段：
- `amount` - 金额

## 实际应用示例

### 在 Jupiter Swap 中动态调整滑点

```rust
// 在清算过程中，如果检测到市场波动，动态增加滑点
for (i, ix) in jup_ixs.iter_mut().enumerate() {
    if is_jupiter_program(&ix.program_id) {
        let parsed = parse_instruction_data(&ix.data, &ix.program_id);

        // 检查当前滑点
        for field in &parsed.parsed_fields {
            if field.name == "slippage_bps" {
                if let FieldValue::U16(current_slippage) = &field.value {
                    if *current_slippage < 100 { // 如果滑点小于1%
                        // 增加到1%以提高成功率
                        modify_jupiter_slippage(ix, 100).unwrap();
                        info!("增加第{}个指令的滑点至1%", i);
                    }
                }
            }
        }
    }
}
```

### 批量修改 ComputeBudget

```rust
// 为所有 ComputeBudget 指令设置统一的计算单位限制
for ix in &mut instructions {
    if ix.program_id == compute_budget::ID {
        let parsed = parse_instruction_data(&ix.data, &ix.program_id);
        match parsed.instruction_type {
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit) => {
                modify_compute_unit_limit(ix, 300_000).unwrap();
                info!("设置计算单位限制为 300,000");
            }
            _ => {}
        }
    }
}
```

## 调试技巧

### 1. 比较修改前后的数据

```rust
let original_data = instruction.data.clone();
modify_jupiter_slippage(&mut instruction, new_slippage).unwrap();

println!("修改前: {}", hex::encode(&original_data));
println!("修改后: {}", hex::encode(&instruction.data));
```

### 2. 验证修改结果

```rust
// 修改后重新解析以验证
let modified_parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
for field in &modified_parsed.parsed_fields {
    if field.name == "slippage_bps" {
        if let FieldValue::U16(slippage) = &field.value {
            assert_eq!(*slippage, expected_slippage);
            println!("✅ 滑点修改成功: {}bps", slippage);
        }
    }
}
```

### 3. 处理解析错误

```rust
// 对于未知或复杂的指令，先查看原始数据
if matches!(parsed.instruction_type, InstructionType::Unknown) {
    println!("未知指令类型，原始数据:");
    println!("  程序ID: {}", instruction.program_id);
    println!("  数据长度: {}", instruction.data.len());
    println!("  前16字节: {:?}", &instruction.data[0..16.min(instruction.data.len())]);

    // 查看是否有 discriminator
    if let Some(disc) = parsed.discriminator {
        println!("  Discriminator: {:?}", disc);
    }
}
```

## 扩展支持

### 添加新的程序支持

1. 在 `InstructionType` 枚举中添加新的程序类型
2. 在 `parse_instruction_data` 中添加程序检测逻辑
3. 实现特定的解析函数
4. 添加修改函数（如果需要）

示例：
```rust
// 1. 添加新的程序类型
#[derive(Debug, Clone)]
pub enum InstructionType {
    // ... 现有类型
    Serum(SerumInstructionType),
}

// 2. 添加检测逻辑
fn is_serum_program(program_id: &Pubkey) -> bool {
    // 实现 Serum 程序检测逻辑
}

// 3. 实现解析函数
fn parse_serum_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    // 实现 Serum 指令解析逻辑
}
```

## 注意事项

1. **字节序**: Solana 使用小端字节序 (little-endian)
2. **数据对齐**: 某些程序可能有特殊的数据对齐要求
3. **版本差异**: 不同版本的程序可能有不同的指令格式
4. **安全性**: 修改 instruction data 前要充分验证，错误的数据可能导致交易失败
5. **测试**: 在主网使用前，务必在测试网或模拟环境中充分测试

## 运行示例

```bash
# 运行示例程序
cargo run --example instruction_parsing

# 运行测试
cargo test instruction_parser
```