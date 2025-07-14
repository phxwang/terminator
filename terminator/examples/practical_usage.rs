use solana_sdk::{instruction::Instruction, pubkey::Pubkey, compute_budget};
use klend_terminator::instruction_parser::*;

/// 实际应用示例：在交易构建过程中解析和修改指令
fn main() {
    println!("=== 实际应用：Jupiter Swap 指令处理 ===\n");

    // 模拟从 Jupiter API 获得的交易指令
    let mut instructions = create_mock_jupiter_transaction();

    println!("1. 分析原始交易");
    analyze_transaction(&instructions);

    println!("\n2. 动态优化交易");
    optimize_transaction(&mut instructions);

    println!("\n3. 验证修改结果");
    verify_modifications(&instructions);
}

/// 创建模拟的 Jupiter 交易（包含多种类型的指令）
fn create_mock_jupiter_transaction() -> Vec<Instruction> {
    let mut instructions = Vec::new();

    // 1. ComputeBudget 指令 - 设置计算单位限制
    let mut compute_data = vec![2u8]; // SetComputeUnitLimit
    compute_data.extend_from_slice(&150_000u32.to_le_bytes());
    instructions.push(Instruction {
        program_id: compute_budget::ID,
        accounts: vec![],
        data: compute_data,
    });

    // 2. ComputeBudget 指令 - 设置计算单位价格
    let mut price_data = vec![3u8]; // SetComputeUnitPrice
    price_data.extend_from_slice(&1000u64.to_le_bytes()); // 1000 micro-lamports
    instructions.push(Instruction {
        program_id: compute_budget::ID,
        accounts: vec![],
        data: price_data,
    });

    // 3. Jupiter Route 指令 - 主要的 swap 指令
    let jupiter_program_id = Pubkey::try_from("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();
    let mut jup_data = vec![0xd4, 0x96, 0x9b, 0x80, 0x7c, 0x81, 0x26, 0x77]; // Route discriminator
    jup_data.extend_from_slice(&2u32.to_le_bytes());        // route_plan_length: 2 hops
    jup_data.extend_from_slice(&5_000_000u64.to_le_bytes()); // in_amount: 5 USDC
    jup_data.extend_from_slice(&4_800_000u64.to_le_bytes()); // quoted_out_amount
    jup_data.extend_from_slice(&30u16.to_le_bytes());       // slippage_bps: 0.3%
    jup_data.push(10u8);                                    // platform_fee_bps: 0.1%

    instructions.push(Instruction {
        program_id: jupiter_program_id,
        accounts: vec![], // 在实际情况中会有很多账户
        data: jup_data,
    });

    // 4. Token Transfer 指令
    let mut token_data = vec![3u8]; // Transfer
    token_data.extend_from_slice(&1_000_000u64.to_le_bytes()); // amount
    instructions.push(Instruction {
        program_id: spl_token::ID,
        accounts: vec![],
        data: token_data,
    });

    instructions
}

/// 分析交易中的所有指令
fn analyze_transaction(instructions: &[Instruction]) {
    println!("交易包含 {} 个指令:", instructions.len());

    for (i, instruction) in instructions.iter().enumerate() {
        println!("\n指令 {}: Program ID: {}", i, instruction.program_id);

        let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

                match &parsed.instruction_type {
            InstructionType::ComputeBudget(compute_type) => {
                println!("  类型: ComputeBudget - {:?}", compute_type);
                for field in &parsed.parsed_fields {
                    print_field(field);
                }
            }
            InstructionType::Jupiter(jupiter_type) => {
                println!("  类型: Jupiter - {:?}", jupiter_type);
                for field in &parsed.parsed_fields {
                    print_field(field);
                }

                // 特别关注关键参数
                analyze_jupiter_params(&parsed);
            }
            InstructionType::Token(token_type) => {
                println!("  类型: Token - {:?}", token_type);
                for field in &parsed.parsed_fields {
                    print_field(field);
                }
            }
            InstructionType::Unknown => {
                println!("  类型: 未知");
                println!("  数据长度: {} bytes", instruction.data.len());
                if instruction.data.len() >= 8 {
                    println!("  前8字节: {:?}", &instruction.data[0..8]);
                }
            }
        }
    }
}

/// 优化交易性能和可靠性
fn optimize_transaction(instructions: &mut [Instruction]) {
    println!("开始优化交易...");

    for (i, instruction) in instructions.iter_mut().enumerate() {
        match instruction.program_id {
            // 优化 ComputeBudget 设置
            id if id == compute_budget::ID => {
                optimize_compute_budget(instruction, i);
            }
            // 优化 Jupiter 交换参数
            id if is_jupiter_program(&id) => {
                optimize_jupiter_swap(instruction, i);
            }
            _ => {}
        }
    }
}

/// 优化 ComputeBudget 设置
fn optimize_compute_budget(instruction: &mut Instruction, index: usize) {
    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

    match &parsed.instruction_type {
        InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit) => {
            // 对于复杂的 Jupiter 交换，增加计算单位限制
            if let Ok(()) = modify_compute_unit_limit(instruction, 300_000) {
                println!("  ✅ 指令 {}: 增加计算单位限制至 300,000", index);
            }
        }
        InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitPrice) => {
            // 在网络拥堵时提高优先费用
            let network_congestion_factor = 1.5; // 模拟网络拥堵
            if network_congestion_factor > 1.2 {
                if let Ok(()) = modify_instruction_data(
                    instruction,
                    "compute_unit_price",
                    FieldValue::U64(2000), // 提高至 2000 micro-lamports
                ) {
                    println!("  ✅ 指令 {}: 由于网络拥堵，提高计算单位价格至 2000", index);
                }
            }
        }
        _ => {}
    }
}

/// 优化 Jupiter 交换参数
fn optimize_jupiter_swap(instruction: &mut Instruction, index: usize) {
    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

    // 检查当前滑点设置
    for field in &parsed.parsed_fields {
        if field.name == "slippage_bps" {
            if let FieldValue::U16(current_slippage) = &field.value {
                println!("  当前滑点: {}bps ({}%)", current_slippage, *current_slippage as f64 / 100.0);

                // 基于市场条件动态调整滑点
                let market_volatility = 1.8; // 模拟市场波动性
                let recommended_slippage = calculate_optimal_slippage(*current_slippage, market_volatility);

                if recommended_slippage != *current_slippage {
                    if let Ok(()) = modify_jupiter_slippage(instruction, recommended_slippage) {
                        println!("  ✅ 指令 {}: 调整滑点从 {}bps 至 {}bps",
                               index, current_slippage, recommended_slippage);
                    }
                }
            }
        }

        if field.name == "in_amount" {
            if let FieldValue::U64(amount) = &field.value {
                println!("  交换金额: {} (约 {} USDC)", amount, *amount as f64 / 1_000_000.0);

                // 对于大额交易，可能需要分拆
                if *amount > 10_000_000 { // 超过 10 USDC
                    println!("  ⚠️  大额交易检测，建议考虑分拆以降低滑点影响");
                }
            }
        }
    }
}

/// 计算最优滑点
fn calculate_optimal_slippage(current: u16, volatility: f64) -> u16 {
    let base_slippage = current as f64;
    let volatility_adjustment = if volatility > 1.5 {
        base_slippage * 1.5 // 高波动性时增加50%滑点
    } else if volatility < 0.8 {
        base_slippage * 0.8 // 低波动性时减少20%滑点
    } else {
        base_slippage
    };

    // 确保滑点在合理范围内 (0.1% - 2%)
    (volatility_adjustment as u16).max(10).min(200)
}

/// 验证修改结果
fn verify_modifications(instructions: &[Instruction]) {
    println!("验证修改结果:");

    let mut total_compute_units = 0u32;
    let mut total_priority_fee = 0u64;
    let mut jupiter_count = 0;

    for (i, instruction) in instructions.iter().enumerate() {
        let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

        match &parsed.instruction_type {
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit) => {
                for field in &parsed.parsed_fields {
                    if field.name == "compute_unit_limit" {
                        if let FieldValue::U32(limit) = &field.value {
                            total_compute_units += limit;
                            println!("  指令 {}: 计算单位限制 = {}", i, limit);
                        }
                    }
                }
            }
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitPrice) => {
                for field in &parsed.parsed_fields {
                    if field.name == "compute_unit_price" {
                        if let FieldValue::U64(price) = &field.value {
                            total_priority_fee += price;
                            println!("  指令 {}: 计算单位价格 = {} micro-lamports", i, price);
                        }
                    }
                }
            }
            InstructionType::Jupiter(_) => {
                jupiter_count += 1;
                for field in &parsed.parsed_fields {
                    if field.name == "slippage_bps" {
                        if let FieldValue::U16(slippage) = &field.value {
                            println!("  指令 {}: Jupiter 滑点 = {}bps ({}%)",
                                   i, slippage, *slippage as f64 / 100.0);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    println!("\n交易摘要:");
    println!("  总计算单位: {}", total_compute_units);
    println!("  总优先费用: {} micro-lamports", total_priority_fee);
    println!("  Jupiter 交换数量: {}", jupiter_count);

    // 评估交易成功概率
    let success_probability = estimate_success_probability(total_compute_units, total_priority_fee, jupiter_count);
    println!("  预估成功概率: {:.1}%", success_probability * 100.0);
}

/// 估算交易成功概率（基于各种因素）
fn estimate_success_probability(compute_units: u32, priority_fee: u64, jupiter_swaps: usize) -> f64 {
    let mut probability = 0.95; // 基础概率

    // 计算单位因素
    if compute_units < 200_000 {
        probability -= 0.1; // 计算单位不足可能导致失败
    }

    // 优先费用因素
    if priority_fee < 1000 {
        probability -= 0.05; // 低优先费用在拥堵时可能被跳过
    }

    // Jupiter 交换复杂性
    probability -= jupiter_swaps as f64 * 0.02; // 每个交换增加2%失败概率

    probability.max(0.1).min(0.99) // 确保在合理范围内
}

/// 辅助函数：打印字段信息
fn print_field(field: &InstructionField) {
    match &field.value {
        FieldValue::U64(val) => println!("    {}: {} (offset: {})", field.name, val, field.offset),
        FieldValue::U32(val) => println!("    {}: {} (offset: {})", field.name, val, field.offset),
        FieldValue::U16(val) => println!("    {}: {} (offset: {})", field.name, val, field.offset),
        FieldValue::U8(val) => println!("    {}: {} (offset: {})", field.name, val, field.offset),
        FieldValue::Pubkey(pubkey) => println!("    {}: {} (offset: {})", field.name, pubkey, field.offset),
        _ => println!("    {}: {:?} (offset: {})", field.name, field.value, field.offset),
    }
}

/// 分析 Jupiter 参数的关键指标
fn analyze_jupiter_params(parsed: &ParsedInstructionData) {
    let mut in_amount = None;
    let mut quoted_out = None;
    let mut slippage = None;

    for field in &parsed.parsed_fields {
        match field.name.as_str() {
            "in_amount" => {
                if let FieldValue::U64(amount) = &field.value {
                    in_amount = Some(*amount);
                }
            }
            "quoted_out_amount" => {
                if let FieldValue::U64(amount) = &field.value {
                    quoted_out = Some(*amount);
                }
            }
            "slippage_bps" => {
                if let FieldValue::U16(bps) = &field.value {
                    slippage = Some(*bps);
                }
            }
            _ => {}
        }
    }

    if let (Some(input), Some(output), Some(slip)) = (in_amount, quoted_out, slippage) {
        let price_impact = ((input as f64 - output as f64) / input as f64) * 100.0;
        let slippage_pct = slip as f64 / 100.0;

        println!("  💱 交换分析:");
        println!("    价格影响: {:.3}%", price_impact);
        println!("    滑点保护: {:.3}%", slippage_pct);

        if price_impact > 1.0 {
            println!("    ⚠️  高价格影响警告");
        }
        if slippage_pct < price_impact * 1.5 {
            println!("    ⚠️  滑点可能不足");
        }
    }
}