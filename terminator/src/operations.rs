use std::collections::HashMap;

use anchor_client::solana_sdk::{account_info::AccountInfo, clock::Clock, pubkey::Pubkey};
use anyhow::Result;
use colored::Colorize;
use kamino_lending::{
    utils::seeds::BASE_SEED_REFERRER_TOKEN_STATE, LendingMarket, Obligation, ReferrerTokenState,
    Reserve,
};
use tracing::{debug, warn};

use crate::{
    accounts::{map_accounts_and_create_infos, oracle_accounts, OracleAccounts},
    client::KlendClient,
    model::StateWithKey,
};

pub fn refresh_reserve<'a>(
    key: &Pubkey,
    reserve: &mut Reserve,
    lending_market: &LendingMarket,
    clock: &Clock,
    pyth_account_infos: &HashMap<Pubkey, AccountInfo<'a>>,
    switchboard_feed_infos: &HashMap<Pubkey, AccountInfo<'a>>,
    scope_price_infos: &HashMap<Pubkey, AccountInfo<'a>>,
) -> Result<()> {
    if kamino_lending::lending_market::lending_operations::is_price_refresh_needed(
        reserve,
        lending_market,
        clock.unix_timestamp,
    ) {
        let pyth_oracle = if reserve.config.token_info.pyth_configuration.is_enabled() {
            Some(
                pyth_account_infos
                    .get(&reserve.config.token_info.pyth_configuration.price)
                    .unwrap(),
            )
        } else {
            None
        };

        let switchboard_price_oracle = if reserve
            .config
            .token_info
            .switchboard_configuration
            .is_enabled()
        {
            //println!("reserve {:?} with switchboard_price_oracle: {:?}", reserve.config.token_info.name, reserve.config.token_info.switchboard_configuration.price_aggregator);
            Some(
                switchboard_feed_infos
                    .get(
                        &reserve
                            .config
                            .token_info
                            .switchboard_configuration
                            .price_aggregator,
                    )
                    .unwrap(),
            )
        } else {
            None
        };

        let switchboard_twap_oracle = if reserve
            .config
            .token_info
            .switchboard_configuration
            .is_enabled()
        {
            Some(
                switchboard_feed_infos
                    .get(
                        &reserve
                            .config
                            .token_info
                            .switchboard_configuration
                            .twap_aggregator,
                    )
                    .unwrap(),
            )
        } else {
            None
        };

        let scope_prices = if reserve.config.token_info.scope_configuration.is_enabled() {
            Some(
                scope_price_infos
                    .get(&reserve.config.token_info.scope_configuration.price_feed)
                    .unwrap(),
            )
        } else {
            None
        };

        reserve.config.token_info.validate_token_info_config(
            pyth_oracle,
            switchboard_price_oracle,
            switchboard_twap_oracle,
            scope_prices,
        )?;

        let px = kamino_lending::utils::prices::get_price(
            &reserve.config.token_info,
            pyth_oracle,
            switchboard_price_oracle,
            switchboard_twap_oracle,
            scope_prices,
            &clock,
        )?;

        kamino_lending::lending_market::lending_operations::refresh_reserve(
            reserve,
            clock,
            px,
            lending_market.referral_fee_bps,
        )?;
    }

    debug!(
        "{} {} reserve refreshed",
        reserve.config.token_info.symbol().purple(),
        key.to_string().bright_blue()
    );

    Ok(())
}

pub struct ObligationReserves {
    pub borrow_reserves: Vec<StateWithKey<Reserve>>,
    pub deposit_reserves: Vec<StateWithKey<Reserve>>,
}

pub fn obligation_reserves(
    obligation: &Obligation,
    reserve_states: &HashMap<Pubkey, Reserve>,
) -> Result<ObligationReserves> {
    let mut deposit_reserves: Vec<StateWithKey<Reserve>> = vec![];
    for deposit in obligation.deposits.iter() {
        if deposit.deposit_reserve != Pubkey::default() {
            let res = reserve_states
                .get(&deposit.deposit_reserve)
                .ok_or(anyhow::anyhow!(
                    "Obligation deposit reserve {:?} not found from {:?}",
                    deposit.deposit_reserve,
                    reserve_states.keys()
                ))?;
            deposit_reserves.push(StateWithKey::new(*res, deposit.deposit_reserve));
        }
    }

    let mut borrow_reserves: Vec<StateWithKey<Reserve>> = vec![];
    for borrow in obligation.borrows.iter() {
        if borrow.borrow_reserve != Pubkey::default() {
            let res = reserve_states
                .get(&borrow.borrow_reserve)
                .ok_or(anyhow::anyhow!(
                    "Obligation borrow reserve {:?} not found from {:?}",
                    borrow.borrow_reserve,
                    reserve_states.keys()
                ))?;
            borrow_reserves.push(StateWithKey::new(*res, borrow.borrow_reserve));
        }
    }

    Ok(ObligationReserves {
        deposit_reserves,
        borrow_reserves,
    })
}

pub struct SplitObligations {
    pub zero_debt: Vec<(Pubkey, Obligation)>,
    pub risky: Vec<(Pubkey, Obligation)>,
}

pub fn split_obligations(obligations: &[(Pubkey, Obligation)]) -> SplitObligations {
    let (zero_debt, risky) = obligations
        .iter()
        .partition(|(_, obligation)| obligation.has_debt == 0);

    SplitObligations { risky, zero_debt }
}

#[allow(clippy::too_many_arguments)]
pub async fn refresh_reserves_and_obligation(
    klend_client: &KlendClient,
    obligation_addr: &Pubkey,
    obligation_state: &mut Obligation,
    reserves: &mut HashMap<Pubkey, Reserve>,
    referrer_token_states: &HashMap<Pubkey, ReferrerTokenState>,
    lending_market: &LendingMarket,
    clock: &Clock,
) -> Result<Vec<Pubkey>> {
    let OracleAccounts {
        mut pyth_accounts,
        mut switchboard_accounts,
        mut scope_price_accounts,
    } = oracle_accounts(&klend_client.client, reserves).await?;

    let all_oracle_keys = pyth_accounts.iter().map(|(k, _, _)| *k)
        .chain(switchboard_accounts.iter().map(|(k, _, _)| *k))
        .chain(scope_price_accounts.iter().map(|(k, _, _)| *k))
        .collect::<Vec<Pubkey>>();

    let pyth_account_infos = map_accounts_and_create_infos(&mut pyth_accounts);
    let switchboard_feed_infos = map_accounts_and_create_infos(&mut switchboard_accounts);
    let scope_price_infos = map_accounts_and_create_infos(&mut scope_price_accounts);

    let ObligationReserves {
        borrow_reserves,
        deposit_reserves,
    } = obligation_reserves(obligation_state, reserves)?;

    // Refresh reserves and obligation
    for reserve in borrow_reserves.iter() {
        refresh_reserve(
            &reserve.key,
            &mut reserve.state.borrow_mut(),
            lending_market,
            clock,
            &pyth_account_infos,
            &switchboard_feed_infos,
            &scope_price_infos,
        )?;

        // 同步更新的数据回原始的 reserves HashMap
        reserves.insert(reserve.key, *reserve.state.borrow());

        //info!("Borrow reserve: {:?}, last_update: {:?}", reserve.state.borrow().config.token_info.symbol(), reserve.state.borrow().last_update);
    }

    for reserve in deposit_reserves.iter() {
        refresh_reserve(
            &reserve.key,
            &mut reserve.state.borrow_mut(),
            lending_market,
            clock,
            &pyth_account_infos,
            &switchboard_feed_infos,
            &scope_price_infos,
        )?;

        // 同步更新的数据回原始的 reserves HashMap
        reserves.insert(reserve.key, *reserve.state.borrow());

        //info!("Deposit reserve: {:?}, last_update: {:?}", reserve.state.borrow().config.token_info.symbol(), reserve.state.borrow().last_update);
    }

    let referrer_states = referrer_token_states_of_obligation(
        obligation_addr,
        obligation_state,
        &borrow_reserves,
        referrer_token_states,
    )?;

    kamino_lending::lending_market::lending_operations::refresh_obligation(
        obligation_addr,
        obligation_state,
        lending_market,
        clock.slot,
        kamino_lending::MaxReservesAsCollateralCheck::Skip,
        borrow_reserves.into_iter(),
        deposit_reserves.into_iter(),
        referrer_states.into_iter(),
    )?;
    Ok(all_oracle_keys)
}

pub fn referrer_token_states_of_obligation(
    obligation_addr: &Pubkey,
    obligation: &Obligation,
    obligation_borrow_reserves: &Vec<StateWithKey<Reserve>>,
    referrer_token_states: &HashMap<Pubkey, ReferrerTokenState>,
) -> Result<Vec<StateWithKey<ReferrerTokenState>>> {
    let rts = if obligation.has_referrer() {
        let mut rts = Vec::new();
        let referrer = obligation.referrer;
        for reserve in obligation_borrow_reserves {
            let (key, _) = Pubkey::find_program_address(
                &[
                    BASE_SEED_REFERRER_TOKEN_STATE,
                    referrer.as_ref(),
                    reserve.state.borrow().liquidity.mint_pubkey.as_ref(),
                ],
                &kamino_lending::ID,
            );
            match referrer_token_states.get(&key) {
                Some(acc) => rts.push(StateWithKey::new(*acc, key)),
                None => {
                    warn!(
                        "Obligation {:?} referrer token state {:?} not found",
                        obligation_addr, key,
                    )
                }
            }
        }
        rts
    } else {
        vec![]
    };
    Ok(rts)
}
