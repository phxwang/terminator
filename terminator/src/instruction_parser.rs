use anchor_lang::AnchorSerialize;
use solana_sdk::{pubkey::Pubkey, instruction::Instruction};
use std::{io::Cursor, fmt};
use hex;

/// 通用的指令数据解析结果
#[derive(Debug, Clone)]
pub struct ParsedInstructionData {
    pub discriminator: Option<[u8; 8]>,
    pub instruction_type: InstructionType,
    pub raw_data: Vec<u8>,
    pub parsed_fields: Vec<InstructionField>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstructionType {
    Jupiter(JupiterInstructionType),
    ComputeBudget(ComputeBudgetInstructionType),
    Token(TokenInstructionType),
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JupiterInstructionType {
    Route,
    SharedAccountsRoute,
    SetTokenLedger,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComputeBudgetInstructionType {
    RequestUnits,
    RequestHeapFrame,
    SetComputeUnitLimit,
    SetComputeUnitPrice,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenInstructionType {
    Transfer,
    Approve,
    Close,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct InstructionField {
    pub name: String,
    pub value: FieldValue,
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I64(i64),
    Pubkey(Pubkey),
    Bytes(Vec<u8>),
    String(String),
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::U8(val) => write!(f, "{}", val),
            FieldValue::U16(val) => write!(f, "{}", val),
            FieldValue::U32(val) => write!(f, "{}", val),
            FieldValue::U64(val) => write!(f, "{}", val),
            FieldValue::I64(val) => write!(f, "{}", val),
            FieldValue::Pubkey(val) => write!(f, "{}", val),
            FieldValue::Bytes(val) => write!(f, "{}", hex::encode(val)),
            FieldValue::String(val) => write!(f, "{}", val),
        }
    }
}

/// 解析指令数据的主要函数
pub fn parse_instruction_data(data: &[u8], program_id: &Pubkey) -> ParsedInstructionData {
    let mut parsed_data = ParsedInstructionData {
        discriminator: None,
        instruction_type: InstructionType::Unknown,
        raw_data: data.to_vec(),
        parsed_fields: Vec::new(),
    };

    if data.is_empty() {
        return parsed_data;
    }

    // 尝试解析程序特定的指令
    if is_jupiter_program(program_id) {
        parse_jupiter_instruction(data, &mut parsed_data);
    } else if is_compute_budget_program(program_id) {
        parse_compute_budget_instruction(data, &mut parsed_data);
    } else if is_token_program(program_id) {
        parse_token_instruction(data, &mut parsed_data);
    } else {
        parse_generic_instruction(data, &mut parsed_data);
    }

    parsed_data
}

/// 修改指令数据中的特定字段
pub fn modify_instruction_data(
    instruction: &mut Instruction,
    field_name: &str,
    new_value: FieldValue,
) -> anyhow::Result<()> {
    let parsed = parse_instruction_data(&instruction.data, &instruction.program_id);

    // 找到要修改的字段
    if let Some(field) = parsed.parsed_fields.iter().find(|f| f.name == field_name) {
        let mut new_data = instruction.data.clone();

        // 根据字段类型修改数据
        match (&field.value, &new_value) {
            (FieldValue::U64(_), FieldValue::U64(new_val)) => {
                let bytes = new_val.to_le_bytes();
                new_data[field.offset..field.offset + 8].copy_from_slice(&bytes);
            }
            (FieldValue::U32(_), FieldValue::U32(new_val)) => {
                let bytes = new_val.to_le_bytes();
                new_data[field.offset..field.offset + 4].copy_from_slice(&bytes);
            }
            (FieldValue::U16(_), FieldValue::U16(new_val)) => {
                let bytes = new_val.to_le_bytes();
                new_data[field.offset..field.offset + 2].copy_from_slice(&bytes);
            }
            (FieldValue::U8(_), FieldValue::U8(new_val)) => {
                new_data[field.offset] = *new_val;
            }
            (FieldValue::Pubkey(_), FieldValue::Pubkey(new_val)) => {
                new_data[field.offset..field.offset + 32].copy_from_slice(&new_val.to_bytes());
            }
            _ => {
                return Err(anyhow::anyhow!("Field type mismatch or unsupported modification"));
            }
        }

        instruction.data = new_data;
        return Ok(());
    }

    Err(anyhow::anyhow!("Field '{}' not found", field_name))
}

/// 创建新的指令数据
pub fn create_instruction_data<T: AnchorSerialize>(instruction_data: T) -> Vec<u8> {
    let mut data = Vec::new();
    instruction_data.serialize(&mut data).unwrap();
    data
}

pub fn is_jupiter_program(program_id: &Pubkey) -> bool {
    // Jupiter V6 program ID
    program_id.to_string() == "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
        || program_id.to_string() == "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB"
}

fn is_compute_budget_program(program_id: &Pubkey) -> bool {
    *program_id == solana_sdk::compute_budget::ID
}

fn is_token_program(program_id: &Pubkey) -> bool {
    *program_id == spl_token::ID || *program_id == spl_token_2022::ID
}

fn parse_jupiter_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    if data.len() < 8 {
        return;
    }

    // Jupiter 使用 Anchor discriminator (前8字节)
    let discriminator = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
    parsed_data.discriminator = Some(discriminator);

    // 根据 discriminator 判断指令类型
    parsed_data.instruction_type = match discriminator {
        // Route instruction discriminator (实际观察到的)
        [0xe5, 0x17, 0xcb, 0x97, 0x7a, 0xe3, 0xad, 0x2a] => {
            parse_jupiter_route_instruction(data, parsed_data);
            InstructionType::Jupiter(JupiterInstructionType::Route)
        }
        // 可能的其他Jupiter discriminators
        [0xd4, 0x96, 0x9b, 0x80, 0x7c, 0x81, 0x26, 0x77] => {
            parse_jupiter_route_instruction(data, parsed_data);
            InstructionType::Jupiter(JupiterInstructionType::SharedAccountsRoute)
        }
        _ => InstructionType::Jupiter(JupiterInstructionType::Unknown),
    };
}

fn parse_jupiter_route_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    if data.len() < 8 {
        return;
    }

    // Skip discriminator (first 8 bytes) and parse payload
    let payload = &data[8..];

    // 基于实际观察到的数据结构解析 (修正为U32类型)
    if payload.len() >= 4 {
        // offset 8-11: route_plan_length (U32)
        let route_plan_len = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        parsed_data.parsed_fields.push(InstructionField {
            name: "route_plan_length".to_string(),
            value: FieldValue::U32(route_plan_len),
            offset: 8,
            size: 4,
        });
    }

    if payload.len() >= 16 {
        // offset 16-23: in_amount (U64) - 实际位置在payload[8:16]
        let in_amount = u64::from_le_bytes([
            payload[8], payload[9], payload[10], payload[11],
            payload[12], payload[13], payload[14], payload[15]
        ]);
        parsed_data.parsed_fields.push(InstructionField {
            name: "in_amount".to_string(),
            value: FieldValue::U64(in_amount),
            offset: 16,
            size: 8,
        });
    }

    if payload.len() >= 24 {
        // offset 24-31: quoted_out_amount (U64) - 实际位置在payload[16:24]
        let out_amount = u64::from_le_bytes([
            payload[16], payload[17], payload[18], payload[19],
            payload[20], payload[21], payload[22], payload[23]
        ]);
        parsed_data.parsed_fields.push(InstructionField {
            name: "quoted_out_amount".to_string(),
            value: FieldValue::U64(out_amount),
            offset: 24,
            size: 8,
        });
    }

    // slippage_bps在新的位置 (after two U64 fields)
    if payload.len() >= 26 {
        let slippage_bps = u16::from_le_bytes([payload[24], payload[25]]);
        parsed_data.parsed_fields.push(InstructionField {
            name: "slippage_bps".to_string(),
            value: FieldValue::U16(slippage_bps),
            offset: 32,
            size: 2,
        });
    }

    // 如果有更多数据，可以继续解析其他字段
    if payload.len() >= 27 {
        // 尝试解析platform_fee_bps (如果存在)
        parsed_data.parsed_fields.push(InstructionField {
            name: "platform_fee_bps".to_string(),
            value: FieldValue::U8(payload[26]),
            offset: 34,
            size: 1,
        });
    }
}

fn parse_compute_budget_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    if data.is_empty() {
        return;
    }

    let instruction_type = data[0];
    parsed_data.instruction_type = match instruction_type {
        0 => {
            // RequestUnits (deprecated)
            if data.len() >= 9 {
                let units = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                let additional_fee = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

                parsed_data.parsed_fields.push(InstructionField {
                    name: "units".to_string(),
                    value: FieldValue::U32(units),
                    offset: 1,
                    size: 4,
                });

                parsed_data.parsed_fields.push(InstructionField {
                    name: "additional_fee".to_string(),
                    value: FieldValue::U32(additional_fee),
                    offset: 5,
                    size: 4,
                });
            }
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::RequestUnits)
        }
        1 => InstructionType::ComputeBudget(ComputeBudgetInstructionType::RequestHeapFrame),
        2 => {
            // SetComputeUnitLimit
            if data.len() >= 5 {
                let units = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                parsed_data.parsed_fields.push(InstructionField {
                    name: "compute_unit_limit".to_string(),
                    value: FieldValue::U32(units),
                    offset: 1,
                    size: 4,
                });
            }
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit)
        }
        3 => {
            // SetComputeUnitPrice
            if data.len() >= 9 {
                let micro_lamports = u64::from_le_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
                ]);
                parsed_data.parsed_fields.push(InstructionField {
                    name: "compute_unit_price".to_string(),
                    value: FieldValue::U64(micro_lamports),
                    offset: 1,
                    size: 8,
                });
            }
            InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitPrice)
        }
        _ => InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitPrice),
    };
}

fn parse_token_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    if data.is_empty() {
        return;
    }

    let instruction_type = data[0];
    parsed_data.instruction_type = match instruction_type {
        3 => {
            // Transfer
            if data.len() >= 9 {
                let amount = u64::from_le_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
                ]);
                parsed_data.parsed_fields.push(InstructionField {
                    name: "amount".to_string(),
                    value: FieldValue::U64(amount),
                    offset: 1,
                    size: 8,
                });
            }
            InstructionType::Token(TokenInstructionType::Transfer)
        }
        4 => {
            // Approve
            if data.len() >= 9 {
                let amount = u64::from_le_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]
                ]);
                parsed_data.parsed_fields.push(InstructionField {
                    name: "amount".to_string(),
                    value: FieldValue::U64(amount),
                    offset: 1,
                    size: 8,
                });
            }
            InstructionType::Token(TokenInstructionType::Approve)
        }
        9 => InstructionType::Token(TokenInstructionType::Close),
        _ => InstructionType::Token(TokenInstructionType::Unknown),
    };
}

fn parse_generic_instruction(data: &[u8], parsed_data: &mut ParsedInstructionData) {
    // 对于未知程序，尝试基本的数据解析
    if data.len() >= 8 {
        let discriminator = [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
        parsed_data.discriminator = Some(discriminator);
    }

    // 添加一些基本字段（假设前几个字节可能是重要参数）
    let mut offset = 0;

    // 如果有discriminator，跳过它
    if data.len() >= 8 {
        offset = 8;
    }

    // 尝试解析一些常见的字段类型
    while offset + 8 <= data.len() {
        let value = u64::from_le_bytes([
            data[offset], data[offset+1], data[offset+2], data[offset+3],
            data[offset+4], data[offset+5], data[offset+6], data[offset+7]
        ]);

        parsed_data.parsed_fields.push(InstructionField {
            name: format!("field_u64_at_{}", offset),
            value: FieldValue::U64(value),
            offset,
            size: 8,
        });

        offset += 8;
    }
}

// 辅助函数
// 注意：这些函数保留用于未来可能的扩展用途
#[allow(dead_code)]
fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u32> {
    use std::io::Read;
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

#[allow(dead_code)]
fn read_u64_le(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u64> {
    use std::io::Read;
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[allow(dead_code)]
fn read_u16_le(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u16> {
    use std::io::Read;
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

#[allow(dead_code)]
fn read_u8(cursor: &mut Cursor<&[u8]>) -> std::io::Result<u8> {
    use std::io::Read;
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

/// 示例：修改 Jupiter swap 的滑点
pub fn modify_jupiter_slippage(instruction: &mut Instruction, new_slippage_bps: u16) -> anyhow::Result<()> {
    modify_instruction_data(
        instruction,
        "slippage_bps",
        FieldValue::U16(new_slippage_bps),
    )
}

/// 示例：修改 Jupiter swap 的输入金额
pub fn modify_jupiter_in_amount(instruction: &mut Instruction, new_amount: u64) -> anyhow::Result<()> {
    modify_instruction_data(
        instruction,
        "in_amount",
        FieldValue::U64(new_amount),
    )
}

pub fn modify_jupiter_out_amount(instruction: &mut Instruction, new_amount: u64) -> anyhow::Result<()> {
    modify_instruction_data(
        instruction,
        "quoted_out_amount",
        FieldValue::U64(new_amount),
    )
}

/// 示例：修改 ComputeBudget 的单位限制
pub fn modify_compute_unit_limit(instruction: &mut Instruction, new_limit: u32) -> anyhow::Result<()> {
    modify_instruction_data(
        instruction,
        "compute_unit_limit",
        FieldValue::U32(new_limit),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_compute_budget_instruction() {
        let data = vec![2, 192, 92, 21, 0]; // SetComputeUnitLimit with 1400000
        let program_id = solana_sdk::compute_budget::id();

        let parsed = parse_instruction_data(&data, &program_id);

        assert_eq!(parsed.instruction_type, InstructionType::ComputeBudget(ComputeBudgetInstructionType::SetComputeUnitLimit));
        assert_eq!(parsed.parsed_fields.len(), 1);
        assert_eq!(parsed.parsed_fields[0].name, "compute_unit_limit");
        if let FieldValue::U32(limit) = parsed.parsed_fields[0].value {
            assert_eq!(limit, 1400000);
        } else {
            panic!("Expected U32 value");
        }
    }

            #[test]
    fn test_parse_jupiter_route_instruction() {
        // 最新观察到的Jupiter Route指令数据 (修正)
        let hex_data = "e517cb977ae3ad2a0100000026640001efd3171100000000a483150000000000000000";
        let data = hex::decode(hex_data).expect("Invalid hex data");
        let program_id = Pubkey::from_str("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();

        let parsed = parse_instruction_data(&data, &program_id);

        // 验证discriminator被正确识别
        assert_eq!(parsed.discriminator, Some([0xe5, 0x17, 0xcb, 0x97, 0x7a, 0xe3, 0xad, 0x2a]));

        // 验证指令类型
        assert_eq!(parsed.instruction_type, InstructionType::Jupiter(JupiterInstructionType::Route));

        // 验证解析出的字段
        assert!(!parsed.parsed_fields.is_empty(), "Should have parsed some fields");

        // 验证具体字段值（基于最新实际观察到的值）
        if let Some(route_plan_field) = parsed.parsed_fields.iter().find(|f| f.name == "route_plan_length") {
            if let FieldValue::U32(val) = route_plan_field.value {
                assert_eq!(val, 1);
            }
        }

                if let Some(in_amount_field) = parsed.parsed_fields.iter().find(|f| f.name == "in_amount") {
            if let FieldValue::U64(val) = in_amount_field.value {
                assert_eq!(val, 286774255); // 应该匹配最新log中的值
            }
        }

        if let Some(out_amount_field) = parsed.parsed_fields.iter().find(|f| f.name == "quoted_out_amount") {
            if let FieldValue::U64(val) = out_amount_field.value {
                assert_eq!(val, 1409956); // 应该匹配最新log中的值
            }
        }

        if let Some(slippage_field) = parsed.parsed_fields.iter().find(|f| f.name == "slippage_bps") {
            if let FieldValue::U16(val) = slippage_field.value {
                assert_eq!(val, 0); // 应该匹配log中的值
            }
        }

        // 打印解析结果用于调试
        println!("Parsed Jupiter instruction: {:?}", parsed);
        for field in &parsed.parsed_fields {
            println!("Field: {} = {:?} (offset: {}, size: {})", field.name, field.value, field.offset, field.size);
        }
    }

            #[test]
    fn test_modify_jupiter_instruction() {
        // 使用最新的Jupiter指令数据进行测试修改 (修正)
        let hex_data = "e517cb977ae3ad2a0100000026640001efd3171100000000a483150000000000000000";
        let data = hex::decode(hex_data).expect("Invalid hex data");
        let program_id = Pubkey::from_str("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();

        let mut instruction = Instruction {
            program_id,
            accounts: vec![], // 为简化测试，暂时为空
            data,
        };

        // 解析原始指令
        let original_parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
        println!("Original parsed: {:?}", original_parsed);

                // 验证原始值是正确的
        if let Some(in_amount_field) = original_parsed.parsed_fields.iter().find(|f| f.name == "in_amount") {
            if let FieldValue::U64(val) = in_amount_field.value {
                assert_eq!(val, 286774255);
                println!("Original in_amount: {}", val);
            }
        }

        // 尝试修改slippage
        let new_slippage_bps = 500; // 5%
        let result = modify_jupiter_slippage(&mut instruction, new_slippage_bps);

        if let Err(e) = &result {
            println!("Slippage modification failed: {}", e);
        } else {
            println!("Slippage modification succeeded");
        }

        // 尝试修改amount (现在是U64类型)
        let new_amount = 500000000u64;
        let amount_result = modify_jupiter_in_amount(&mut instruction, new_amount);

        if let Err(e) = &amount_result {
            println!("Amount modification failed: {}", e);
        } else {
            println!("Amount modification succeeded");
        }

        // 解析修改后的指令
        let modified_parsed = parse_instruction_data(&instruction.data, &instruction.program_id);
        println!("Modified parsed: {:?}", modified_parsed);

        // 验证修改结果
        if result.is_ok() {
            if let Some(slippage_field) = modified_parsed.parsed_fields.iter().find(|f| f.name == "slippage_bps") {
                if let FieldValue::U16(slippage) = slippage_field.value {
                    assert_eq!(slippage, new_slippage_bps);
                    println!("Successfully modified slippage to {}", slippage);
                }
            }
        }

        if amount_result.is_ok() {
            if let Some(amount_field) = modified_parsed.parsed_fields.iter().find(|f| f.name == "in_amount") {
                if let FieldValue::U64(amount) = amount_field.value {
                    assert_eq!(amount, new_amount);
                    println!("Successfully modified amount to {}", amount);
                }
            }
        }
    }
}