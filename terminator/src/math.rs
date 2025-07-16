use anchor_lang::prelude::Pubkey;
use anyhow::Result;
use colored::Colorize;
use kamino_lending::{utils::Fraction, LiquidationParams, Obligation, ObligationCollateral, ObligationLiquidity, Reserve};
use std::collections::HashMap;
use tracing::{debug, info};

use crate::{liquidator::Holdings, model::StateWithKey};

#[derive(Debug)]
pub enum LiquidationStrategy {
    LiquidateAndRedeem(u64),
    SwapThenLiquidate(u64, u64),
}

pub struct ObligationInfo {
    pub borrowed_amount: Fraction,
    pub deposited_amount: Fraction,
    pub ltv: Fraction,
    pub unhealthy_ltv: Fraction,
}

pub fn obligation_info(address: &Pubkey, obligation: &Obligation) -> ObligationInfo {
    let borrowed_amount = Fraction::from_bits(obligation.borrow_factor_adjusted_debt_value_sf);
    let deposited_amount = Fraction::from_bits(obligation.deposited_value_sf);

    let (ltv, unhealthy_ltv) = if borrowed_amount > 0 && deposited_amount == 0 {
        info!("Obligation {address} has bad debt: {:?}", obligation);
        (Fraction::ZERO, Fraction::ZERO)
    } else if deposited_amount > 0 || borrowed_amount > 0 {
        (
            obligation.loan_to_value(),
            obligation.unhealthy_loan_to_value(),
        )
    } else {
        (Fraction::ZERO, Fraction::ZERO)
    };

    // info!("unhealthy_borrow_value_sf: {}", Fraction::from_bits(obligation.unhealthy_borrow_value_sf));

    ObligationInfo {
        borrowed_amount,
        deposited_amount,
        ltv,
        unhealthy_ltv,
    }
}

pub fn print_obligation_stats(
    obl_info: &ObligationInfo,
    address: &Pubkey,
    i: usize,
    num_obligations: usize,
) -> (bool, bool, bool, bool) {
    let ObligationInfo {
        borrowed_amount,
        deposited_amount,
        ltv,
        unhealthy_ltv,
    } = obl_info;

    let is_liquidatable = ltv > unhealthy_ltv;
    let big_near_liquidatable =  (ltv > &(unhealthy_ltv * &Fraction::from_num(0.94))) &&  borrowed_amount > &Fraction::from_num(10000);
    let medium_near_liquidatable = !big_near_liquidatable && (ltv > &(unhealthy_ltv * &Fraction::from_num(0.95))) &&  borrowed_amount > &Fraction::from_num(100);
    let small_near_liquidatable = !big_near_liquidatable && !medium_near_liquidatable && (ltv > &(unhealthy_ltv * &Fraction::from_num(0.95)));

    let mut msg = format!(
        "{}/{} obligation: {}, healthy: {}, LTV: {:?}%/{:?}%, deposited: {} borrowed: {}",
        i + 1,
        num_obligations,
        address.to_string().green(),
        if is_liquidatable {
            "NO".red()
        } else if small_near_liquidatable || medium_near_liquidatable || big_near_liquidatable {
            "NEAR".yellow()
        } else {
            "YES".green()
        },
        ltv * 100,
        unhealthy_ltv * 100,
        deposited_amount,
        borrowed_amount,
    );
    if big_near_liquidatable {
        msg.push_str(", BIG FISH");
    }

    if is_liquidatable || small_near_liquidatable || medium_near_liquidatable || big_near_liquidatable {
        info!("{}", msg);
    } else {
        debug!("{}", msg);
    }

    (is_liquidatable, small_near_liquidatable, medium_near_liquidatable, big_near_liquidatable)
}

pub fn get_liquidatable_amount(
    obligation: &StateWithKey<Obligation>,
    lending_market: &StateWithKey<kamino_lending::LendingMarket>,
    coll_reserve: &StateWithKey<kamino_lending::Reserve>,
    debt_reserve: &StateWithKey<kamino_lending::Reserve>,
    clock: &anchor_lang::prelude::Clock,
    max_allowed_ltv_override_pct_opt: Option<u64>,
    _liquidation_swap_slippage_pct: f64,
) -> Result<u64> {
    let debt_res_key = debt_reserve.key;

    // Calculate what is possible first
    let LiquidationParams { user_ltv, .. } =
        kamino_lending::liquidation_operations::get_liquidation_params(
            &lending_market.state.borrow(),
            &coll_reserve.state.borrow(),
            &debt_reserve.state.borrow(),
            &obligation.state.borrow(),
            clock.slot,
            true, // todo actually check if this is true
            true, // todo actually check if this is true
            max_allowed_ltv_override_pct_opt,
        )?;

    let obligation_state = obligation.state.borrow();
    let lending_market = lending_market.state.borrow();
    let (debt_liquidity, _) = obligation_state
        .find_liquidity_in_borrows(debt_res_key)
        .unwrap();

    let full_debt_amount_f = Fraction::from_bits(debt_liquidity.borrowed_amount_sf);
    let _full_debt_mv_f = Fraction::from_bits(debt_liquidity.market_value_sf);

    let liquidatable_debt =
        kamino_lending::liquidation_operations::max_liquidatable_borrowed_amount(
            &obligation_state,
            lending_market.liquidation_max_debt_close_factor_pct,
            lending_market.max_liquidatable_debt_market_value_at_once,
            debt_liquidity,
            user_ltv,
            lending_market.insolvency_risk_unhealthy_ltv_pct,
            kamino_lending::LiquidationReason::LtvExceeded,
        )
        .min(full_debt_amount_f);

    //let liquidation_ratio = liquidatable_debt / full_debt_amount_f;

    // This is what is possible, liquidatable/repayable debt in lamports and in $ terms
    let liqidatable_amount: u64 = liquidatable_debt.to_num();

    Ok(liqidatable_amount)
}

#[allow(clippy::too_many_arguments)]
pub fn decide_liquidation_strategy(
    base_mint: &Pubkey,
    obligation: &StateWithKey<Obligation>,
    lending_market: &StateWithKey<kamino_lending::LendingMarket>,
    coll_reserve: &StateWithKey<kamino_lending::Reserve>,
    debt_reserve: &StateWithKey<kamino_lending::Reserve>,
    clock: &anchor_lang::prelude::Clock,
    max_allowed_ltv_override_pct_opt: Option<u64>,
    liquidation_swap_slippage_pct: f64,
    holdings: Holdings,
) -> Result<Option<LiquidationStrategy>> {
    let debt_res_key = debt_reserve.key;

    // Calculate what is possible first
    let LiquidationParams { user_ltv, .. } =
        kamino_lending::liquidation_operations::get_liquidation_params(
            &lending_market.state.borrow(),
            &coll_reserve.state.borrow(),
            &debt_reserve.state.borrow(),
            &obligation.state.borrow(),
            clock.slot,
            true, // todo actually check if this is true
            true, // todo actually check if this is true
            max_allowed_ltv_override_pct_opt,
        )?;

    let obligation_state = obligation.state.borrow();
    let lending_market = lending_market.state.borrow();
    let (debt_liquidity, _) = obligation_state
        .find_liquidity_in_borrows(debt_res_key)
        .unwrap();

    let full_debt_amount_f = Fraction::from_bits(debt_liquidity.borrowed_amount_sf);
    let full_debt_mv_f = Fraction::from_bits(debt_liquidity.market_value_sf);

    let liquidatable_debt =
        kamino_lending::liquidation_operations::max_liquidatable_borrowed_amount(
            &obligation_state,
            lending_market.liquidation_max_debt_close_factor_pct,
            lending_market.max_liquidatable_debt_market_value_at_once,
            debt_liquidity,
            user_ltv,
            lending_market.insolvency_risk_unhealthy_ltv_pct,
            kamino_lending::LiquidationReason::LtvExceeded,
        )
        .min(full_debt_amount_f);

    // 添加除零检查
    let liquidation_ratio = if full_debt_amount_f > Fraction::ZERO {
        liquidatable_debt / full_debt_amount_f
    } else {
        Fraction::ZERO
    };

    // This is what is possible, liquidatable/repayable debt in lamports and in $ terms
    let liqidatable_amount: u64 = liquidatable_debt.to_num();
    let liquidable_mv = liquidation_ratio * full_debt_mv_f;

    // Compare to what we have already
    let debt_mint = &debt_reserve.state.borrow().liquidity.mint_pubkey;

    let debt_holding = holdings.holding_of(debt_mint).unwrap();
    let base_holding = holdings.holding_of(base_mint).unwrap();

    // Always assume we are fully rebalanced otherwise it's very hard to decide
    let decision = if debt_mint == base_mint {
        // liquidate as much as we have / is possible
        let debt_holding_balance = debt_holding.balance;
        let liquidate_amount = debt_holding_balance.min(liqidatable_amount);

        info!(
            "LiquidateAndRedeem math nums
            liquidate_amount: {liquidate_amount}
            debt_holding.balance: {debt_holding_balance}
            liquidatable_amount: {liqidatable_amount},"
        );

        Some(LiquidationStrategy::LiquidateAndRedeem(liquidate_amount))
    } else {
        // Swap base token to debt token then liquidate
        // compare market values, add slippage, then swap base token + slippage into debt token
        let holding_mv = base_holding.usd_value;
        let liquidable_mv: f64 = liquidable_mv.to_num();

        // Adjust by slippage how much we would have if we had to swap
        let holding_mv = holding_mv * (1.0 - liquidation_swap_slippage_pct / 100.0);

        // Liquidate as much as possible (the lowest of holding or liquidatable in $ terms)
        let liquidation_mv = holding_mv.min(liquidable_mv);

        // Decide on the final amount
        let ratio = if holding_mv != 0.0 {
            liquidation_mv / holding_mv
        } else {
            0.0 // 当分母为0时，使用默认值
        };
        let swap_amount = (base_holding.balance as f64 * ratio) as u64;
        let liquidate_amount = (liqidatable_amount as f64 * ratio) as u64;

        info!(
            "SwapAndLiquidate math nums
            holding_mv: {holding_mv},
            liquidable_mv: {liquidable_mv},
            liquidation_mv: {liquidation_mv},
            ratio: {ratio},
            swap_amount: {swap_amount},
            liquidate_amount: {liquidate_amount}"
        );

        Some(LiquidationStrategy::SwapThenLiquidate(
            swap_amount,
            liquidate_amount,
        ))

        // TODO: what if we have barely anything?
        // need to do flash loans
    };

    // Print everything such that we can debug later how the decision was made
    info!(
        "Liquidation decision: {decision:?},
        full_debt_amount: {full_debt_amount_f},
        full_debt_mv: {full_debt_mv_f},
        liquidatable_debt: {liquidatable_debt},
        liquidation_ratio: {liquidation_ratio},
        liqidatable_amount: {liqidatable_amount},
        liquidable_mv: {liquidable_mv},
        debt_holding: {debt_holding:?},
        base_holding: {base_holding:?},
        liquidation_swap_slippage_pct: {liquidation_swap_slippage_pct}
    "
    );

    Ok(decision)
}

pub fn find_best_collateral_reserve(
    deposits: &[ObligationCollateral],
    _reserves: &HashMap<Pubkey, Reserve>,
) -> Option<Pubkey> {
    // find the collateral reserve with the highest market value
    let mut best_reserve = None;
    let mut best_mv = Fraction::ZERO;

    for deposit in deposits {
        let mv = Fraction::from_bits(deposit.market_value_sf);
        if mv > best_mv {
            best_mv = mv;
            best_reserve = Some(deposit.deposit_reserve);
        }
    }

    best_reserve
}

pub fn find_best_debt_reserve(
    borrows: &[ObligationLiquidity],
    _reserves: &HashMap<Pubkey, Reserve>,
) -> Option<Pubkey> {
    // find the debt reserve with the highest market value
    let mut best_reserve = None;
    let mut best_mv = Fraction::ZERO;

    for borrow in borrows {
        let mv = Fraction::from_bits(borrow.market_value_sf);
        if mv > best_mv {
            best_mv = mv;
            best_reserve = Some(borrow.borrow_reserve);
        }
    }

    best_reserve
}
