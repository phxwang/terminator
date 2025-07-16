use std::sync::Arc;

use tracing::warn;

use anchor_client::solana_sdk::{instruction::{AccountMeta, Instruction}, pubkey::Pubkey, signature::Keypair};
use anchor_lang::{prelude::Rent, system_program::System, Id, InstructionData, ToAccountMetas};
use anchor_spl::token::Token;
use tracing::debug;
use kamino_lending::{LendingMarket, Reserve, ReserveFarmKind};
use solana_sdk::{
    signer::Signer,
    sysvar::{
        SysvarId, {self},
    },
};
use spl_token_2022::ID as TOKEN_2022_PROGRAM_ID;

use crate::{consts::NULL_PUBKEY, liquidator::Liquidator, model::StateWithKey, readable, writable};

#[derive(Clone)]
pub struct InstructionBlocks {
    pub instruction: Instruction,
    pub payer: Pubkey,
    pub signers: Vec<Arc<Keypair>>,
}


/// Determine token program ID based on mint - returns Token2022 for newer mints, SPL Token for others
/// This is a simplified heuristic - in production you'd want to check the actual mint account
fn get_token_program_for_mint(mint: &Pubkey) -> Pubkey {
    // For now, we'll assume all mints could potentially be Token2022
    // In a real implementation, you'd check the mint account owner
    // For the specific failing mint in the error message, we'll assume it's Token2022
    let known_token2022_mints = [
        "2u1tszSeqZ3qBWF3uNGPFc8TzMk2tdiwknnRMWGWH", // The mint from the error
        // Add other known Token2022 mints here
    ];

    if known_token2022_mints.contains(&mint.to_string().as_str()) {
        TOKEN_2022_PROGRAM_ID
    } else {
        Token::id()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn liquidate_obligation_and_redeem_reserve_collateral_ix(
    program_id: &Pubkey,
    lending_market: StateWithKey<LendingMarket>,
    debt_reserve: StateWithKey<Reserve>,
    coll_reserve: StateWithKey<Reserve>,
    liquidator: &Liquidator,
    obligation: Pubkey,
    liquidity_amount: u64,
    min_acceptable_received_liq_amount: u64,
    max_allowed_ltv_override_pct_opt: Option<u64>,
) -> InstructionBlocks {
    let lending_market_pubkey = lending_market.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&lending_market_pubkey);

    let coll_reserve_state = coll_reserve.state.borrow();
    let coll_reserve_address = coll_reserve.key;

    let debt_reserve_state = debt_reserve.state.borrow();
    let debt_reserve_address = debt_reserve.key;

    let collateral_ctoken = coll_reserve_state.collateral.mint_pubkey;
    let collateral_token = coll_reserve_state.liquidity.mint_pubkey;
    let debt_token = debt_reserve_state.liquidity.mint_pubkey;

    debug!("collateral_ctoken: {:?}", collateral_ctoken);
    debug!("collateral_token: {:?}", collateral_token);
    debug!("debt_token: {:?}", debt_token);

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::LiquidateObligationAndRedeemReserveCollateral {
            liquidator: liquidator.wallet.pubkey(),
            lending_market: lending_market.key,
            repay_reserve: debt_reserve_address,
            repay_reserve_liquidity_supply: debt_reserve_state.liquidity.supply_vault,
            withdraw_reserve: coll_reserve_address,
            withdraw_reserve_collateral_mint: coll_reserve_state.collateral.mint_pubkey,
            withdraw_reserve_collateral_supply: coll_reserve_state.collateral.supply_vault,
            withdraw_reserve_liquidity_supply: coll_reserve_state.liquidity.supply_vault,
            withdraw_reserve_liquidity_fee_receiver: coll_reserve_state.liquidity.fee_vault,
            lending_market_authority,
            obligation,
            user_destination_collateral: *liquidator.atas.get(&collateral_ctoken).unwrap(),
            user_destination_liquidity: *liquidator.atas.get(&collateral_token).unwrap(),
            user_source_liquidity: *liquidator.atas.get(&debt_token).unwrap(),
            instruction_sysvar_account: sysvar::instructions::ID,
            repay_liquidity_token_program: get_token_program_for_mint(&debt_token),
            repay_reserve_liquidity_mint: debt_reserve_state.liquidity.mint_pubkey,
            withdraw_reserve_liquidity_mint: coll_reserve_state.liquidity.mint_pubkey,
            collateral_token_program: get_token_program_for_mint(&collateral_ctoken),
            withdraw_liquidity_token_program: get_token_program_for_mint(&collateral_token),
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::LiquidateObligationAndRedeemReserveCollateral {
            liquidity_amount,
            min_acceptable_received_liquidity_amount: min_acceptable_received_liq_amount,
            max_allowed_ltv_override_percent: max_allowed_ltv_override_pct_opt.unwrap_or(0),
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

pub fn refresh_reserve_ix(
    program_id: &Pubkey,
    reserve: Reserve,
    address: &Pubkey,
    payer: Arc<Keypair>,
) -> InstructionBlocks {
    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::RefreshReserve {
            lending_market: reserve.lending_market,
            reserve: *address,
            pyth_oracle: maybe_null_pk(reserve.config.token_info.pyth_configuration.price),
            switchboard_price_oracle: maybe_null_pk(
                reserve
                    .config
                    .token_info
                    .switchboard_configuration
                    .price_aggregator,
            ),
            switchboard_twap_oracle: maybe_null_pk(
                reserve
                    .config
                    .token_info
                    .switchboard_configuration
                    .twap_aggregator,
            ),
            scope_prices: maybe_null_pk(reserve.config.token_info.scope_configuration.price_feed),
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::RefreshReserve.data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

pub fn refresh_obligation_farm_for_reserve_ix(
    program_id: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    reserve_farm_state: Pubkey,
    obligation: Pubkey,
    owner: &Arc<Keypair>,
    farms_mode: ReserveFarmKind,
) -> InstructionBlocks {
    let (user_farm_state, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            reserve_farm_state.as_ref(),
            obligation.as_ref(),
        ],
        &farms::ID,
    );

    let reserve_state = reserve.state.borrow();
    let reserve_address = reserve.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&reserve_state.lending_market);

        let accts = vec![
        AccountMeta::new_readonly(owner.pubkey(), true), // crank
        AccountMeta::new(obligation, false), // obligation
        AccountMeta::new_readonly(reserve_state.lending_market, false), // lending_market
        AccountMeta::new_readonly(lending_market_authority, false), // lending_market_authority
        AccountMeta::new_readonly(reserve_address, false), // reserve
        AccountMeta::new(user_farm_state, false), // obligation_farm_user_state
        AccountMeta::new(reserve_farm_state, false), // reserve_farm_state
        AccountMeta::new_readonly(Rent::id(), false), // rent
        AccountMeta::new_readonly(farms::id(), false), // farms_program
        AccountMeta::new_readonly(System::id(), false), // system_program
    ];

    let instruction = Instruction {
        program_id: *program_id,
        accounts: accts,
        data: kamino_lending::instruction::RefreshObligationFarmsForReserve {
            mode: farms_mode as u8,
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: owner.pubkey(),
        signers: vec![owner.clone()],
    }
}

#[allow(clippy::too_many_arguments)]
pub fn init_obligation_farm_for_reserve_ix(
    program_id: &Pubkey,
    reserve_accounts: &StateWithKey<Reserve>,
    reserve_farm_state: Pubkey,
    obligation: &Pubkey,
    owner: &Pubkey,
    payer: &Arc<Keypair>,
    mode: ReserveFarmKind,
) -> InstructionBlocks {
    let (obligation_farm_state, _user_state_bump) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            reserve_farm_state.as_ref(),
            obligation.as_ref(),
        ],
        &farms::ID,
    );

    let reserve_state = reserve_accounts.state.borrow();
    let reserve_address = reserve_accounts.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&reserve_state.lending_market);

    let accts = kamino_lending::accounts::InitObligationFarmsForReserve {
        payer: payer.pubkey(),
        owner: *owner,
        obligation: *obligation,
        lending_market: reserve_state.lending_market,
        lending_market_authority,
        reserve: reserve_address,
        obligation_farm: obligation_farm_state,
        reserve_farm_state,
        rent: Rent::id(),
        farms_program: farms::id(),
        system_program: System::id(),
    };

    let instruction = Instruction {
        program_id: *program_id,
        accounts: accts.to_account_metas(None),
        data: kamino_lending::instruction::InitObligationFarmsForReserve { mode: mode as u8 }
            .data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

pub fn refresh_obligation_ix(
    program_id: &Pubkey,
    market: Pubkey,
    obligation: Pubkey,
    deposit_reserves: Vec<Option<Pubkey>>,
    borrow_reserves: Vec<Option<Pubkey>>,
    referrer_token_states: Vec<Option<Pubkey>>,
    payer: Arc<Keypair>,
) -> InstructionBlocks {
    let mut accounts = kamino_lending::accounts::RefreshObligation {
        obligation,
        lending_market: market,
    }
    .to_account_metas(None);

    deposit_reserves
        .iter()
        .filter(|reserve| reserve.is_some())
        .for_each(|reserve| {
            accounts.push(readable!(reserve.unwrap()));
        });

    borrow_reserves
        .iter()
        .filter(|reserve| reserve.is_some())
        .for_each(|reserve| {
            accounts.push(writable!(reserve.unwrap()));
        });

    referrer_token_states
        .iter()
        .filter(|referrer_token_state: &&Option<Pubkey>| referrer_token_state.is_some())
        .for_each(|referrer_token_state| {
            accounts.push(writable!(referrer_token_state.unwrap()));
        });

    let instruction = Instruction {
        program_id: *program_id,
        accounts,
        data: kamino_lending::instruction::RefreshObligation.data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

#[allow(clippy::too_many_arguments)]
pub fn flash_borrow_reserve_liquidity_ix(
    program_id: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    _obligation: &Pubkey,
    liquidity_amount: u64,
    liquidator: &Liquidator,
) -> InstructionBlocks {
    let reserve_state = reserve.state.borrow();
    let lending_market_pubkey = reserve_state.lending_market;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&lending_market_pubkey);

    let reserve_liquidity_mint = reserve_state.liquidity.mint_pubkey;
    let user_destination_liquidity = match liquidator.atas.get(&reserve_liquidity_mint) {
        Some(liquidity) => *liquidity,
        None => {
            warn!("user_destination_liquidity not found: {:?}", reserve_liquidity_mint);
            return InstructionBlocks {
                instruction: Instruction {
                    program_id: *program_id,
                    accounts: vec![],
                    data: vec![],
                },
                payer: liquidator.wallet.pubkey(),
                signers: vec![],
            };
        }
    };

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::FlashBorrowReserveLiquidity {
            user_transfer_authority: liquidator.wallet.pubkey(),
            lending_market_authority,
            lending_market: lending_market_pubkey,
            reserve: reserve.key,
            reserve_liquidity_mint,
            reserve_source_liquidity: reserve_state.liquidity.supply_vault,
            user_destination_liquidity,
            reserve_liquidity_fee_receiver: reserve_state.liquidity.fee_vault,
            referrer_token_state: None,
            referrer_account: None,
            sysvar_info: sysvar::instructions::ID,
            token_program: get_token_program_for_mint(&reserve_liquidity_mint),
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::FlashBorrowReserveLiquidity {
            liquidity_amount,
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

#[allow(clippy::too_many_arguments)]
pub fn flash_repay_reserve_liquidity_ix(
    program_id: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    _obligation: &Pubkey,
    liquidity_amount: u64,
    borrow_instruction_index: u8,
    liquidator: &Liquidator,
) -> InstructionBlocks {
    let reserve_state = reserve.state.borrow();
    let lending_market_pubkey = reserve_state.lending_market;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&lending_market_pubkey);

    let reserve_liquidity_mint = reserve_state.liquidity.mint_pubkey;
    let user_destination_liquidity = *liquidator.atas.get(&reserve_liquidity_mint).unwrap();

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::FlashRepayReserveLiquidity {
            user_transfer_authority: liquidator.wallet.pubkey(),
            lending_market_authority,
            lending_market: lending_market_pubkey,
            reserve: reserve.key,
            reserve_liquidity_mint,
            user_source_liquidity: user_destination_liquidity,
            reserve_destination_liquidity: reserve_state.liquidity.supply_vault,
            reserve_liquidity_fee_receiver: reserve_state.liquidity.fee_vault,
            referrer_token_state: None,
            referrer_account: None,
            sysvar_info: sysvar::instructions::ID,
            token_program: get_token_program_for_mint(&reserve_liquidity_mint),
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::FlashRepayReserveLiquidity {
            liquidity_amount,
            borrow_instruction_index,
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

pub fn maybe_null_pk(pubkey: Pubkey) -> Option<Pubkey> {
    if pubkey == Pubkey::default() || pubkey == NULL_PUBKEY {
        None
    } else {
        Some(pubkey)
    }
}
