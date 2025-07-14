use anchor_lang::prelude::*;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, compute_budget};
use klend_terminator::instruction_parser::{
    parse_instruction_data, FieldValue, modify_compute_unit_limit,
    InstructionType, JupiterInstructionType, ComputeBudgetInstructionType,
};

fn main() {
    println!("=== Solana Instruction Data 解析和修改示例 ===\n");

    // 示例1：解析 ComputeBudget 指令
    example_compute_budget_parsing();

    // 示例2：解析 Jupiter 指令（模拟数据）
    example_jupiter_parsing();

    // 示例3：修改指令数据
    example_instruction_modification();

    // 示例4：创建自定义指令数据
    example_custom_instruction_creation();
}

fn example_compute_budget_parsing() {
    println!("1. ComputeBudget 指令解析示例");
    println!("===============================");

    // 创建一个 SetComputeUnitLimit 指令
    let compute_unit_limit = 200_000u32;
    let mut data = vec![2u8]; // SetComputeUnitLimit discriminator
    data.extend_from_slice(&compute_unit_limit.to_le_bytes());

    let instruction = Instruction {
        program_id: compute_budget::ID,
        accounts: vec![],
        data,
    };

    println!("原始指令数据: {:?}", hex::encode(&instruction.data));

    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
    println!("解析结果: {:#?}", parsed);

    match parsed.instruction_type {
        InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit) => {
            println!("✅ 成功识别为 SetComputeUnitLimit 指令");
            for field in &parsed.parsed_fields {
                if field.name == "compute_unit_limit" {
                    if let FieldValue::U32(limit) = &field.value {
                        println!("   计算单位限制: {}", limit);
                    }
                }
            }
        }
        _ => println!("❌ 指令类型识别错误"),
    }
    println!();
}

fn example_jupiter_parsing() {
    println!("2. Jupiter 指令解析示例");
    println!("========================");

    // 模拟 Jupiter Route 指令数据
    let jupiter_program_id = Pubkey::try_from("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();

    // Jupiter Route discriminator + 模拟数据
    let mut data = vec![0xd4, 0x96, 0x9b, 0x80, 0x7c, 0x81, 0x26, 0x77]; // Route discriminator
    data.extend_from_slice(&1u32.to_le_bytes());        // route_plan_length
    data.extend_from_slice(&1000000u64.to_le_bytes());  // in_amount
    data.extend_from_slice(&950000u64.to_le_bytes());   // quoted_out_amount
    data.extend_from_slice(&50u16.to_le_bytes());       // slippage_bps (0.5%)
    data.push(0u8);                                     // platform_fee_bps

    let instruction = Instruction {
        program_id: jupiter_program_id,
        accounts: vec![],
        data,
    };

    println!("原始指令数据: {:?}", hex::encode(&instruction.data));

    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
    println!("解析结果: {:#?}", parsed);

    match parsed.instruction_type {
        InstructionType::Jupiter(JupiterInstructionType::Route) => {
            println!("✅ 成功识别为 Jupiter Route 指令");
            for field in &parsed.parsed_fields {
                match &field.value {
                    FieldValue::U64(val) => println!("   {}: {}", field.name, val),
                    FieldValue::U32(val) => println!("   {}: {}", field.name, val),
                    FieldValue::U16(val) => println!("   {}: {}", field.name, val),
                    FieldValue::U8(val) => println!("   {}: {}", field.name, val),
                    _ => println!("   {}: {:?}", field.name, field.value),
                }
            }
        }
        _ => println!("❌ 指令类型识别错误"),
    }
    println!();
}

fn example_instruction_modification() {
    println!("3. 指令数据修改示例");
    println!("===================");

    // 创建一个 ComputeBudget 指令并修改
    let mut data = vec![2u8]; // SetComputeUnitLimit
    data.extend_from_slice(&100_000u32.to_le_bytes());

    let mut instruction = Instruction {
        program_id: compute_budget::ID,
        accounts: vec![],
        data,
    };

    println!("修改前:");
    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
    for field in &parsed.parsed_fields {
        if field.name == "compute_unit_limit" {
            if let FieldValue::U32(limit) = &field.value {
                println!("   计算单位限制: {}", limit);
            }
        }
    }

    // 修改计算单位限制
    if let Err(e) = modify_compute_unit_limit(&mut instruction, 200_000) {
        println!("❌ 修改失败: {}", e);
    } else {
        println!("✅ 修改成功");

        println!("修改后:");
        let modified_parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
        for field in &modified_parsed.parsed_fields {
            if field.name == "compute_unit_limit" {
                if let FieldValue::U32(limit) = &field.value {
                    println!("   计算单位限制: {}", limit);
                }
            }
        }
    }
    println!();
}

fn example_custom_instruction_creation() {
    println!("4. 自定义指令数据创建示例");
    println!("=========================");

    // 使用 Anchor 框架创建指令数据
    #[derive(AnchorSerialize)]
    struct CustomInstruction {
        amount: u64,
        recipient: Pubkey,
        memo: String,
    }

    let custom_data = CustomInstruction {
        amount: 1_000_000,
        recipient: Pubkey::new_unique(),
        memo: "Test transfer".to_string(),
    };

    let instruction_data = {
        let mut data = Vec::new();
        custom_data.serialize(&mut data).unwrap();
        data
    };

    println!("自定义指令数据 (hex): {}", hex::encode(&instruction_data));
    println!("数据长度: {} bytes", instruction_data.len());

    // 解析自定义指令
    let custom_program_id = Pubkey::new_unique();
    let parsed = parse_instruction_data(&instruction_data, &custom_program_id);
    println!("解析结果: {:#?}", parsed);
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use klend_terminator::instruction_parser::*;

    #[test]
    fn test_compute_budget_modification() {
        let mut data = vec![2u8]; // SetComputeUnitLimit
        data.extend_from_slice(&100_000u32.to_le_bytes());

        let mut instruction = Instruction {
            program_id: compute_budget::ID,
            accounts: vec![],
            data,
        };

        // 修改前验证
        let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
        assert_eq!(parsed.parsed_fields.len(), 1);
        if let FieldValue::U32(limit) = &parsed.parsed_fields[0].value {
            assert_eq!(*limit, 100_000);
        }

        // 修改
        modify_compute_unit_limit(&mut instruction, 200_000).unwrap();

        // 修改后验证
        let modified_parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
        if let FieldValue::U32(limit) = &modified_parsed.parsed_fields[0].value {
            assert_eq!(*limit, 200_000);
        }
    }

    #[test]
    fn test_jupiter_program_detection() {
        let jupiter_v6 = Pubkey::try_from("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();
        let jupiter_v4 = Pubkey::try_from("JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB").unwrap();
        let other_program = Pubkey::new_unique();

        assert!(is_jupiter_program(&jupiter_v6));
        assert!(is_jupiter_program(&jupiter_v4));
        assert!(!is_jupiter_program(&other_program));
    }

    #[test]
    fn test_generic_instruction_parsing() {
        // 测试通用指令解析
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let program_id = Pubkey::new_unique();

        let parsed = parse_instruction_data(&data, &program_id);

        // 应该有discriminator（前8字节）
        assert!(parsed.discriminator.is_some());
        assert_eq!(parsed.discriminator.unwrap(), [1, 2, 3, 4, 5, 6, 7, 8]);

        // 应该有一个u64字段（后8字节）
        assert_eq!(parsed.parsed_fields.len(), 1);
        if let FieldValue::U64(value) = &parsed.parsed_fields[0].value {
            assert_eq!(*value, u64::from_le_bytes([9, 10, 11, 12, 13, 14, 15, 16]));
        }
    }
}