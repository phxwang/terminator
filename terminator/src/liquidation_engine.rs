use std::{collections::{HashMap, HashSet}, sync::{Arc, RwLock}, time::Duration};

use anyhow::Result;
use colored::Colorize;
use kamino_lending::{Reserve, LendingMarket, ReferrerTokenState, Obligation, PriceStatusFlags, fraction::Fraction, utils::FractionExtra};
use solana_sdk::{
    clock::Clock,
    compute_budget,
    account::Account,
    sysvar::SysvarId,
    instruction::Instruction,
    address_lookup_table::AddressLookupTableAccount,
    pubkey::Pubkey,
    signer::Signer,
    bs58,
};
use tokio::time::sleep;
use tracing::{info, warn, debug, error};
use extra_proto::{Replace, SimulateTransactionRequest};
use futures::SinkExt;
use futures_util::stream::StreamExt;
use scope::OraclePrices as ScopePrices;
use anchor_lang::{AccountDeserialize, Discriminator};
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use bytemuck;

use crate::{
    accounts::{dump_accounts_to_file, refresh_oracle_keys, load_obligations_map, MarketAccounts, oracle_accounts, OracleAccounts, load_competitors_from_file, TimestampedPrice, get_price_usd},
    client::KlendClient,
    model::StateWithKey,
    operations::{
        obligation_reserves, referrer_token_states_of_obligation,
        ObligationReserves,
    },
    math,
    sysvars,
    swap,
    instruction_parser,
    yellowstone_transaction::create_yellowstone_client,
};

pub struct LiquidationEngine {
    /// Cache for obligations
    pub obligation_map: HashMap<Pubkey, Obligation>,
    /// Cache for market accounts (reserves, lending market, referrer token states)
    pub market_accounts_map: HashMap<Pubkey, (HashMap<Pubkey, Reserve>, LendingMarket, HashMap<Pubkey, ReferrerTokenState>)>,
    /// Cache for preloaded swap instructions
    pub obligation_swap_map: HashMap<Pubkey, (Vec<Instruction>, Option<Vec<AddressLookupTableAccount>>)>,
    /// List of obligation reserves that need refreshing
    pub obligation_reservers_to_refresh: Vec<Pubkey>,
    /// Cache for market obligations mapping
    pub market_obligations_map: HashMap<Pubkey, Vec<Pubkey>>,
    /// Cache for market pubkeys
    pub market_pubkeys: Vec<Pubkey>,
    /// Cache for scope price accounts
    pub all_scope_price_accounts: Vec<(Pubkey, bool, Account)>,
    /// Cache for switchboard accounts
    pub all_switchboard_accounts: Vec<(Pubkey, bool, Account)>,
    /// Cache for all reserves
    pub all_reserves: HashMap<Pubkey, Reserve>,
    /// Cache for all lending markets
    pub all_lending_market: HashMap<Pubkey, LendingMarket>,
    /// Cache for all referrer token states
    pub all_rts: HashMap<Pubkey, HashMap<Pubkey, ReferrerTokenState>>,
    /// Cache for reserve prices with timestamps
    pub reserves_prices: HashMap<Pubkey, TimestampedPrice>,
    /// Cache for current clock
    pub clock: Option<Clock>,
    /// Cache for subscribing pubkeys
    pub subscribing_pubkeys: Vec<Pubkey>,
    /// Cache for obligations with thread-safe access
    pub shared_obligation_map: Arc<RwLock<HashMap<Pubkey, Obligation>>>,
}

impl LiquidationEngine {
    pub fn new() -> Self {
        Self {
            obligation_map: HashMap::new(),
            market_accounts_map: HashMap::new(),
            obligation_swap_map: HashMap::new(),
            obligation_reservers_to_refresh: Vec::new(),
            market_obligations_map: HashMap::new(),
            market_pubkeys: Vec::new(),
            all_scope_price_accounts: Vec::new(),
            all_switchboard_accounts: Vec::new(),
            all_reserves: HashMap::new(),
            all_lending_market: HashMap::new(),
            all_rts: HashMap::new(),
            reserves_prices: HashMap::new(),
            clock: None,
            subscribing_pubkeys: Vec::new(),
            shared_obligation_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn liquidate(&mut self, klend_client: &Arc<KlendClient>, obligation: &Pubkey, slot: Option<u64>) -> Result<()> {
        info!("Liquidating obligation {}, slot: {:?}", obligation.to_string().green(), slot);
        debug!("Liquidator ATAs: {:?}", klend_client.liquidator.atas);

        info!("Performing full data refresh");

        let (obligation_data, clock, reserves, market, rts, loaded_accounts_data) = match slot {
            Some(slot) => {
                // load data from extra
                klend_client.load_data_from_file(obligation, slot).await?
            },
            None => {
                let clock = sysvars::clock(&klend_client.local_client.client).await?;

                // Reload accounts
                let mut ob = klend_client.fetch_obligation(obligation).await?;

                info!("Obligation before refresh: {:?}", ob);
                info!("Obligation summary before refresh: {:?}", ob.to_string());

                let market_accs = klend_client
                    .fetch_market_and_reserves(&ob.lending_market)
                    .await?;

                let mut reserves = market_accs.reserves;
                let market = market_accs.lending_market;
                // todo - don't load all
                let rts = klend_client.fetch_referrer_token_states().await?;

                let oracle_keys = crate::operations::refresh_reserves_and_obligation(
                    klend_client,
                    obligation,
                    &mut ob,
                    &mut reserves,
                    &rts,
                    &market,
                    &clock,
                )
                .await?;

                let ObligationReserves {
                    borrow_reserves,
                    deposit_reserves,
                } = obligation_reserves(&ob, &reserves)?;

                let mut obligation_reserve_keys = Vec::new();
                obligation_reserve_keys.extend(borrow_reserves.iter().map(|b| b.key).collect::<Vec<_>>());
                obligation_reserve_keys.extend(deposit_reserves.iter().map(|d| d.key).collect::<Vec<_>>());

                let mut to_dump_keys = Vec::new();
                to_dump_keys.extend(oracle_keys);
                to_dump_keys.push(Clock::id());
                to_dump_keys.push(*obligation);
                to_dump_keys.push(ob.lending_market);
                to_dump_keys.extend(obligation_reserve_keys.clone());

                dump_accounts_to_file(
                    &mut klend_client.extra_client.clone(),
                    &to_dump_keys,
                    clock.slot,
                    &obligation_reserve_keys,
                    *obligation,
                    ob.clone()).await?;

                (ob, clock, reserves, market, rts, None)
            }
        };

        self.liquidate_with_loaded_data(klend_client, obligation, clock, obligation_data, reserves, market, rts, loaded_accounts_data).await?;

        Ok(())
    }

    pub async fn liquidate_with_loaded_data(
        &mut self,
        klend_client: &Arc<KlendClient>,
        obligation: &Pubkey,
        clock: Clock,
        ob: Obligation,
        reserves: HashMap<Pubkey, Reserve>,
        market: LendingMarket,
        _rts: HashMap<Pubkey, ReferrerTokenState>,
        loaded_accounts_data: Option<HashMap<Pubkey, Account>>,
    ) -> Result<(), anyhow::Error> {
        info!("Liquidating: Obligation: {:?}", ob);
        info!("Liquidating: Obligation summary: {:?}", ob.to_string());
        let debt_res_key = match math::find_best_debt_reserve(&ob.borrows, &reserves) {
            Some(key) => key,
            None => {
                error!("No debt reserve found for obligation {}", obligation);
                return Err(anyhow::anyhow!("No debt reserve found for obligation"));
            }
        };
        let coll_res_key = match math::find_best_collateral_reserve(&ob.deposits, &reserves) {
            Some(key) => key,
            None => {
                error!("No collateral reserve found for obligation {}", obligation);
                return Err(anyhow::anyhow!("No collateral reserve found for obligation"));
            }
        };
        let debt_reserve_state = match reserves.get(&debt_res_key) {
            Some(reserve) => *reserve,
            None => {
                error!("Debt reserve {} not found in reserves", debt_res_key);
                return Err(anyhow::anyhow!("Debt reserve not found in reserves"));
            }
        };
        let coll_reserve_state = match reserves.get(&coll_res_key) {
            Some(reserve) => *reserve,
            None => {
                error!("Collateral reserve {} not found in reserves", coll_res_key);
                return Err(anyhow::anyhow!("Collateral reserve not found in reserves"));
            }
        };
        info!("Liquidating: debt_reserve_state: {:?}", debt_reserve_state);
        info!("Liquidating: coll_reserve_state: {:?}", coll_reserve_state);
        let debt_reserve = StateWithKey::new(debt_reserve_state, debt_res_key);
        let coll_reserve = StateWithKey::new(coll_reserve_state, coll_res_key);
        let lending_market = StateWithKey::new(market, ob.lending_market);
        let obligation = StateWithKey::new(ob, *obligation);
        info!("Liquidating: Clock: {:?}", clock);
        info!("Liquidating: Debt reserve: {:?}, last_update: {:?}, is_stale: {:?}", debt_reserve_state.config.token_info.symbol(), debt_reserve_state.last_update, debt_reserve_state.last_update.is_stale(clock.slot, PriceStatusFlags::LIQUIDATION_CHECKS));
        info!("Liquidating: Coll reserve: {:?}, last_update: {:?}, is_stale: {:?}", coll_reserve_state.config.token_info.symbol(), coll_reserve_state.last_update, coll_reserve_state.last_update.is_stale(clock.slot, PriceStatusFlags::LIQUIDATION_CHECKS));
        debug!("Liquidating: Reserves for {:?}: {:?}", ob.lending_market, reserves.keys());
        let deposit_reserves: Vec<StateWithKey<Reserve>> = ob
            .deposits
            .iter()
            .filter(|coll| coll.deposit_reserve != Pubkey::default())
            .filter_map(|coll| {
                match reserves.get(&coll.deposit_reserve) {
                    Some(reserve) => Some(StateWithKey::new(*reserve, coll.deposit_reserve)),
                    None => {
                        error!("Deposit reserve {} not found in reserves", coll.deposit_reserve);
                        None
                    }
                }
            })
            .collect();
        let max_allowed_ltv_override_pct_opt = Some(0);
        let liquidation_swap_slippage_pct = 0 as f64;
        let min_acceptable_received_collateral_amount = 0;
        let liquidate_amount = math::get_liquidatable_amount(
            &obligation,
            &lending_market,
            &coll_reserve,
            &debt_reserve,
            &clock,
            max_allowed_ltv_override_pct_opt,
            liquidation_swap_slippage_pct,
        )?;
        let borrowed_amount = Fraction::from_sf(obligation.state.borrow().borrows.iter().find(|b| b.borrow_reserve == debt_res_key).unwrap().borrowed_amount_sf);
        info!("Liquidating amount: {}, borrowed_amount: {}", liquidate_amount, borrowed_amount);
        let res = kamino_lending::lending_market::lending_operations::liquidate_and_redeem(
            &lending_market.state.borrow(),
            &debt_reserve,
            &coll_reserve,
            &mut obligation.state.borrow_mut().clone(),
            &clock,
            liquidate_amount,
            min_acceptable_received_collateral_amount,
            max_allowed_ltv_override_pct_opt,
            deposit_reserves.into_iter(),
        );
        info!("Simulating the Liquidating {:#?}", res);
        if res.is_ok() {
            let total_withdraw_liquidity_amount = match res {
                Ok(result) => result.total_withdraw_liquidity_amount,
                Err(_) => {
                    // This should not happen since we checked is_ok() above
                    error!("Unexpected error in liquidation simulation result");
                    return Ok(());
                }
            };
            let mut net_withdraw_liquidity_amount = 0;

            match total_withdraw_liquidity_amount {
                Some((withdraw_liquidity_amount, protocol_fee)) => {
                    net_withdraw_liquidity_amount = withdraw_liquidity_amount - protocol_fee;
                    info!("Liquidating: Net withdraw liquidity amount: {}", net_withdraw_liquidity_amount);
                }
                None => {
                    warn!("Total withdraw liquidity amount is None");
                }
            }

            let _user = klend_client.liquidator.wallet.pubkey();

            let mut ixns = vec![];
            let mut luts = vec![];

            // add flashloan ixns
            let flash_borrow_ixns = klend_client
                .flash_borrow_reserve_liquidity_ixns(
                    &debt_reserve,
                    &obligation.key,
                    liquidate_amount,
                )
                .await?;

            // Record the current instruction count to track flash borrow position
            let flash_borrow_instruction_index = ixns.len();
            ixns.extend_from_slice(&flash_borrow_ixns);

            info!("Liquidating: Flash borrow ixns count: {:?}", flash_borrow_ixns.len());

            // add liquidate ixns
            let liquidate_ixns = match klend_client
                .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                    lending_market,
                    debt_reserve.clone(),
                    coll_reserve.clone(),
                    obligation.clone(),
                    liquidate_amount,
                    min_acceptable_received_collateral_amount,
                    max_allowed_ltv_override_pct_opt,
                    &reserves
                )
                .await {
                    Ok(ixns) => ixns,
                    Err(e) => {
                        error!("Error creating liquidate instructions: {}", e);
                        return Err(e);
                    }
                };
            ixns.extend_from_slice(&liquidate_ixns);
            info!("Liquidating: Liquidate ixns count: {:?}", liquidate_ixns.len());

            let (jup_ixs, lookup_tables) = match self.obligation_swap_map.get(&obligation.key) {
                Some(swap_data) => {
                    // 有可能会有两个swap的instruction，需要适配
                    let mut modified_jup_ixs = swap_data.0.clone();
                    let inst1 = instruction_parser::parse_instruction_data(&modified_jup_ixs[1].data, &modified_jup_ixs[1].program_id);
                    info!("Liquidating: before modify jupiter in amount: {:?}", inst1);

                    if modified_jup_ixs.len() > 2 {
                        let in_amount = inst1.parsed_fields.iter().find(|f| f.name == "in_amount").unwrap().value.to_string();
                        let ratio = net_withdraw_liquidity_amount as f64 / in_amount.parse::<u64>().unwrap() as f64;

                        // multiply ratio to all in_amounts and out_amounts in modified_jup_ixs start from index 1
                        for ix in modified_jup_ixs.iter_mut().skip(1) {
                            let inst = instruction_parser::parse_instruction_data(&ix.data, &ix.program_id);
                            let in_amount = inst.parsed_fields.iter().find(|f| f.name == "in_amount").unwrap().value.to_string();
                            let out_amount = inst.parsed_fields.iter().find(|f| f.name == "quoted_out_amount").unwrap().value.to_string();
                            let in_amount_u64 = in_amount.parse::<u64>().unwrap();
                            let out_amount_u64 = out_amount.parse::<u64>().unwrap();
                            let new_in_amount = (in_amount_u64 as f64 * ratio) as u64;
                            let new_out_amount = (out_amount_u64 as f64 * ratio) as u64;
                            instruction_parser::modify_jupiter_in_amount(ix, new_in_amount);
                            instruction_parser::modify_jupiter_out_amount(ix, new_out_amount);

                            info!("Liquidating: Modified jupiter in amount: {:?}", instruction_parser::parse_instruction_data(&ix.data, &ix.program_id));
                        }
                    }

                    let last_ix = modified_jup_ixs.len() - 1;
                    instruction_parser::modify_jupiter_in_amount(&mut modified_jup_ixs[1], net_withdraw_liquidity_amount);
                    instruction_parser::modify_jupiter_out_amount(&mut modified_jup_ixs[last_ix], liquidate_amount);

                    info!("Liquidating: Modified jupiter in amount: {:?}", instruction_parser::parse_instruction_data(&modified_jup_ixs[1].data, &modified_jup_ixs[1].program_id));

                    (modified_jup_ixs, swap_data.1.clone())
                },
                None => {
                    info!("No swap data found for obligation {}, fetch swap instructions", obligation.key);
                    //add jupiter swap ixns
                    swap::swap_with_jupiter_ixns(
                        klend_client,
                        &coll_reserve.state.borrow().liquidity.mint_pubkey,
                        &debt_reserve.state.borrow().liquidity.mint_pubkey,
                        net_withdraw_liquidity_amount,
                        Some(liquidate_amount),
                        liquidation_swap_slippage_pct
                    ).await?
                }
            };

            info!("Liquidating: Jupiter swap ixns count: {:?}", jup_ixs.len());
            debug!("Liquidating: Jupiter swap ixns: {:?}", jup_ixs);
            debug!("Liquidating: Jupiter swap ixns lookup tables: {:?}", lookup_tables);

            ixns.extend_from_slice(&jup_ixs.into_iter().filter(|ix| ix.program_id != compute_budget::id()).collect::<Vec<_>>());
            if let Some(tables) = lookup_tables {
                luts.extend_from_slice(&tables);
            }

            // add flashloan repay ixns
            // Note: build_with_budget_and_fee() adds 2 ComputeBudget instructions at the beginning
            // So the actual flash borrow instruction will be at index: flash_borrow_instruction_index + 2
            let flash_repay_ixns = klend_client
                .flash_repay_reserve_liquidity_ixns(
                    &debt_reserve,
                    &obligation.key,
                    liquidate_amount,
                    (flash_borrow_instruction_index + 2) as u8,
                )
                .await?;

            ixns.extend_from_slice(&flash_repay_ixns);

            // TODO: add compute budget + prio fees
            let mut txn = klend_client.local_client.tx_builder().add_ixs(ixns.clone());

            // add custom lookup table
            if let Some(lut) = klend_client.custom_lookup_table.read().unwrap().clone() {
                txn = txn.add_lookup_table(lut);
            }

            // add ata lookup table
            if let Some(lut) = klend_client.ata_lookup_table.read().unwrap().clone() {
                txn = txn.add_lookup_table(lut);
            }

            for lut in luts {
                txn = txn.add_lookup_table(lut);
            }

            let txn_b64 = txn.to_base64();
            info!(
                "Liquidating: Simulation: https://explorer.solana.com/tx/inspector?message={}",
                urlencoding::encode(&txn_b64)
            );

            let txn = match txn.build_with_budget_and_fee(&[]).await {
                Ok(txn) => txn,
                Err(e) => {
                    error!("Error building transaction: {}", e);
                    return Err(e.into());
                }
            };

            info!("Liquidating: txn.message.address_table_lookups: {:?}", txn.message.address_table_lookups());
            info!("Liquidating: txn.message.static_account_keys({}): {:?}", txn.message.static_account_keys().len(), txn.message.static_account_keys());

            let static_account_keys = txn.message.static_account_keys();
            let mut missing_account_keys: HashSet<_> = static_account_keys.iter().cloned().collect();
            for ix in txn.message.instructions() {
                let program_id = ix.program_id(static_account_keys);
                missing_account_keys.remove(&program_id);
            }
            info!("Liquidating: missing account keys({}): {:?}", missing_account_keys.len(), missing_account_keys);

            match loaded_accounts_data {
                Some(loaded_accounts_data) => {
                    info!("Liquidating with extra client");

                    let mut replaces = vec![];
                    for (key, account) in loaded_accounts_data.iter() {
                        replaces.push(Replace {
                            address: key.to_bytes().to_vec(),
                            data: account.data.to_vec(),
                        });
                    }

                    info!("Liquidating: replaces count: {:?}", replaces.len());

                    let mut extra_client = klend_client.extra_client.clone();

                    let request = SimulateTransactionRequest {
                        data: serde_json::to_vec(&txn).unwrap(),
                        replaces: replaces,
                        commitment_or_slot: clock.slot,
                        addresses: vec![obligation.key.to_bytes().to_vec()],
                    };

                    info!("Liquidating: request data length: {:?}", request.data.len());

                    match extra_client.simulate_transaction(request).await {
                        Ok(response) => {
                            let response = response.into_inner();
                            if let Some(err) = response.err {
                                error!("Transaction simulation failed: {}", err);
                            } else {
                                info!("Transaction simulation succeeded");
                                if !response.datas.is_empty() {
                                    info!("Response data length: {}", response.datas[0].len());
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to simulate transaction: {}", e);
                        }
                    }
                }
                None => {
                    info!("Liquidating with normal client");

                    match klend_client
                        .local_client
                        .send_retry_and_confirm_transaction(txn.clone(), None, false)
                        .await
                        {
                            Ok(sig) => {
                                info!("Liquidating: tx sent: {:?}", sig.0);
                                info!("Liquidating: tx res: {:?}", sig.1);
                            }
                            Err(e) => {
                                error!("Liquidating: tx error: {:?}", e);

                                // fetch newest clock
                                let new_clock = match sysvars::get_clock(&klend_client.local_client.client).await {
                                    Ok(clock) => clock,
                                    Err(e) => {
                                        error!("Error getting clock: {}", e);
                                        return Err(e.into());
                                    }
                                };

                                info!("Liquidating: new clock after error: {:?}", new_clock);

                                let mut all_oracle_keys = HashSet::new();
                                let mut pyth_keys = HashSet::new();
                                let mut switchboard_keys = HashSet::new();
                                let mut scope_keys = HashSet::new();

                                refresh_oracle_keys(&reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

                                let ObligationReserves {
                                    borrow_reserves,
                                    deposit_reserves,
                                } = obligation_reserves(&ob, &reserves)?;

                                let mut obligation_reserve_keys = Vec::new();
                                obligation_reserve_keys.extend(borrow_reserves.iter().map(|b| b.key).collect::<Vec<_>>());
                                obligation_reserve_keys.extend(deposit_reserves.iter().map(|d| d.key).collect::<Vec<_>>());

                                // dump newest slot
                                let mut extra_client = klend_client.extra_client.clone();
                                let mut to_dump_keys = Vec::new();
                                to_dump_keys.push(Clock::id());
                                to_dump_keys.push(obligation.key);
                                to_dump_keys.push(obligation.state.borrow().lending_market);
                                to_dump_keys.extend(obligation_reserve_keys.clone());
                                to_dump_keys.extend(all_oracle_keys);

                                if let Err(e) = dump_accounts_to_file(
                                    &mut extra_client,
                                    &to_dump_keys,
                                    new_clock.slot,
                                    &obligation_reserve_keys,
                                    obligation.key,
                                    obligation.state.borrow().clone()
                                ).await {
                                    error!("Error dumping accounts to file: {}", e);
                                }

                                match klend_client
                                    .local_client
                                    .client
                                    .simulate_transaction(&txn)
                                    .await
                                    {
                                        Ok(res) => {
                                            info!("Liquidating: Simulation result: {:?}", res);
                                        }
                                        Err(e) => {
                                            error!("Liquidating: Simulation error: {:?}", e);
                                        }
                                    };
                            }
                        };
                }
            }
        }
        Ok(())
    }

    pub async fn check_and_liquidate(
        &mut self,
        klend_client: &Arc<KlendClient>,
        address: &Pubkey,
        obligation: &mut Obligation,
        lending_market: &LendingMarket,
        reserves: &HashMap<Pubkey, Reserve>,
        rts: &HashMap<Pubkey, ReferrerTokenState>
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let ObligationReserves {
            deposit_reserves,
            borrow_reserves,
        } = match obligation_reserves(&obligation, &reserves) {
            Ok(reserves) => reserves,
            Err(e) => {
                error!("[Liquidation Thread] Error getting obligation reserves for {}: {}", address, e);
                return Err(e);
            }
        };

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed obligation reserves time used: {} in {}s", address.to_string().green(), en);

        let referrer_states = match referrer_token_states_of_obligation(
            address,
            &obligation,
            &borrow_reserves,
            &rts,
        ) {
            Ok(states) => states,
            Err(e) => {
                error!("[Liquidation Thread] Error getting referrer token states for {}: {}", address, e);
                return Err(e);
            }
        };

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed token states time used: {} in {}s", address.to_string().green(), en);

        // Collect keys before moving the vectors
        let mut obligation_reserve_keys = Vec::new();
        obligation_reserve_keys.extend(borrow_reserves.iter().map(|b| b.key).collect::<Vec<_>>());
        obligation_reserve_keys.extend(deposit_reserves.iter().map(|d| d.key).collect::<Vec<_>>());

        if let Err(e) = kamino_lending::lending_market::lending_operations::refresh_obligation(
            &address,
            obligation,
            &lending_market,
            self.clock.as_ref().unwrap().slot,
            kamino_lending::MaxReservesAsCollateralCheck::Skip,
            deposit_reserves.into_iter(),
            borrow_reserves.into_iter(),
            referrer_states.into_iter(),
        ) {
            error!("[Liquidation Thread] Error refreshing obligation {}: {}", address, e);
            return Err(e.into());
        }

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed obligation time used: {} in {}s", address.to_string().green(), en);

        let obligation_stats = math::obligation_info(address, &obligation);
        if obligation_stats.ltv > obligation_stats.unhealthy_ltv {
            info!("[Liquidation Thread] Liquidating obligation start: pubkey: {}, obligation: {}, slot: {}", address.to_string().green(), obligation.to_string().green(), self.clock.as_ref().unwrap().slot);

            //dump accounts
            let mut extra_client = klend_client.extra_client.clone();
            let mut all_oracle_keys = HashSet::new();
            let mut pyth_keys = HashSet::new();
            let mut switchboard_keys = HashSet::new();
            let mut scope_keys = HashSet::new();

            refresh_oracle_keys(&reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

            let mut to_dump_keys = Vec::new();
            to_dump_keys.extend(all_oracle_keys);
            to_dump_keys.push(Clock::id());
            to_dump_keys.push(*address);
            to_dump_keys.push(obligation.lending_market);
            to_dump_keys.extend(obligation_reserve_keys.clone());

            if let Err(e) = dump_accounts_to_file(
                &mut extra_client,
                &to_dump_keys,
                self.clock.as_ref().unwrap().slot,
                &obligation_reserve_keys,
                *address,
                obligation.clone()
            ).await {
                error!("Error dumping accounts to file: {}", e);
            }

            let liquidate_start = std::time::Instant::now();
            match self.liquidate_with_loaded_data(
                klend_client,
                &address,
                self.clock.as_ref().unwrap().clone(),
                obligation.clone(),
                reserves.clone(),
                *lending_market,
                rts.clone(),
                None
            ).await {
                Ok(_) => {
                    info!("[Liquidation Thread] Liquidated obligation finished: {} success", address.to_string().green());
                }
                Err(e) => {
                    error!("[Liquidation Thread] Error liquidating obligation: {} {}", address.to_string().green(), e);
                }
            }
            let liquidate_en = liquidate_start.elapsed().as_secs_f64();
            info!("[Liquidation Thread] Liquidated obligation time used: {} in {}s", address.to_string().green(), liquidate_en);
        }
        else {
            debug!("[Liquidation Thread] Obligation is not liquidatable: {} {}", address.to_string().green(), obligation.to_string().green());
        }

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Check and liquidate time used: {} in {}s", address.to_string().green(), en);

        Ok(())
    }

    pub async fn liquidate_in_loop(&mut self, klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
        let start = std::time::Instant::now();

        let mut obligations_map = match load_obligations_map(scope.clone()).await {
            Ok(value) => value,
            Err(value) => return value,
        };

        let mut total_liquidatable_obligations = 0;

        // Initialize or update clock
        self.clock = Some(match sysvars::get_clock(&klend_client.local_client.client).await {
            Ok(clock) => clock,
            Err(e) => {
                error!("Error getting clock: {}", e);
                return Err(e.into());
            }
        });

        let en_clock = start.elapsed().as_secs_f64();
        debug!("Refreshing market clock time used: {}s", en_clock);

        for (market, liquidatable_obligations) in obligations_map.iter_mut() {
            info!("[Liquidation Thread]{}: {} liquidatable obligations found", market.to_string().green(), liquidatable_obligations.len());
            total_liquidatable_obligations += liquidatable_obligations.len();

            let market_pubkey = *market;

            if !self.market_accounts_map.contains_key(&market_pubkey) {
                let (market_accounts, rts) = match self.load_market_accounts_and_rts(klend_client, &market_pubkey).await {
                    Ok(result) => result,
                    Err(e) => {
                        error!("[Liquidation Thread] Error loading market accounts and rts {}: {}", market_pubkey, e);
                        continue;
                    }
                };
                self.market_accounts_map.insert(market_pubkey, (market_accounts.reserves, market_accounts.lending_market, rts));
            }

            //only refresh reserves in obligations
            let refresh_start = std::time::Instant::now();
            {
                let obligation_reservers_to_refresh = self.obligation_reservers_to_refresh.clone();
                let (reserves, lending_market, _rts) = self.market_accounts_map.get_mut(&market_pubkey).unwrap();
                match crate::refresh_market(klend_client,
                    &market_pubkey,
                    &obligation_reservers_to_refresh,
                    reserves,
                    lending_market,
                    self.clock.as_ref().unwrap(),
                    None,
                    None,
                    None).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[Liquidation Thread] Error refreshing market {}: {}", market_pubkey, e);
                        continue;
                    }
                };
            }
            let refresh_en = refresh_start.elapsed().as_secs_f64();
            debug!("[Liquidation Thread] Refreshed market {} in {}s", market_pubkey.to_string().green(), refresh_en);

            {
                let (reserves, lending_market, rts) = self.market_accounts_map.get(&market_pubkey).unwrap();
                let reserves = reserves.clone();
                let lending_market = lending_market.clone();
                let rts = rts.clone();
                self.scan_obligations(klend_client, liquidatable_obligations, &reserves, &lending_market, &rts, None).await;
            }
        }
        let en = start.elapsed().as_secs_f64();
        info!("[Liquidation Thread] Scanned {} obligations in {}s", total_liquidatable_obligations, en);

        Ok(())
    }

    pub async fn preload_swap_instructions(&mut self, klend_client: &Arc<KlendClient>, obligation_key: &Pubkey, obligation: &Obligation, reserves: &HashMap<Pubkey, Reserve>) -> Result<()> {
        let debt_res_key = match math::find_best_debt_reserve(&obligation.borrows, &reserves) {
            Some(key) => key,
            None => {
                error!("No debt reserve found for obligation {}", obligation_key);
                return Err(anyhow::anyhow!("No debt reserve found for obligation"));
            }
        };
        let coll_res_key = match math::find_best_collateral_reserve(&obligation.deposits, &reserves) {
            Some(key) => key,
            None => {
                error!("No collateral reserve found for obligation {}", obligation_key);
                return Err(anyhow::anyhow!("No collateral reserve found for obligation"));
            }
        };
        let debt_reserve_state = match reserves.get(&debt_res_key) {
            Some(reserve) => *reserve,
            None => {
                error!("Debt reserve {} not found in reserves", debt_res_key);
                return Err(anyhow::anyhow!("Debt reserve not found in reserves"));
            }
        };
        let coll_reserve_state = match reserves.get(&coll_res_key) {
            Some(reserve) => *reserve,
            None => {
                error!("Collateral reserve {} not found in reserves", coll_res_key);
                return Err(anyhow::anyhow!("Collateral reserve not found in reserves"));
            }
        };
        let (jup_ixs, lookup_tables) = match swap::swap_with_jupiter_ixns(
            klend_client,
            &coll_reserve_state.liquidity.mint_pubkey,
            &debt_reserve_state.liquidity.mint_pubkey,
            1000000,
            Some(1000000),
            0.0
        ).await {
            Ok(result) => result,
            Err(e) => {
                error!("Error getting swap instructions for obligation {}: {}", obligation_key, e);
                return Err(e.into());
            }
        };

        self.obligation_swap_map.insert(*obligation_key, (jup_ixs.clone(), lookup_tables));
        info!("Preloaded swap instructions for obligation {} {:?}", obligation_key, jup_ixs.len());

        sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    pub async fn scan_obligations(
        &mut self,
        klend_client: &Arc<KlendClient>,
        liquidatable_obligations: &mut Vec<Pubkey>,
        reserves: &HashMap<Pubkey, Reserve>,
        lending_market: &LendingMarket,
        rts: &HashMap<Pubkey, ReferrerTokenState>,
        price_changed_reserves: Option<&HashSet<Pubkey>>
    ) -> u32 {
        let mut checked_obligation_count = 0;

        for address in liquidatable_obligations.iter() {
            let start = std::time::Instant::now();

            let should_process = if let Some(obligation) = self.obligation_map.get(&address) {
                if let Some(price_changed_reserves) = price_changed_reserves {
                    //check if none of the reserves in the obligation are in the price_changed_reserves
                    if obligation.deposits.iter().all(|coll| !price_changed_reserves.contains(&coll.deposit_reserve)) &&
                        obligation.borrows.iter().all(|borrow| !price_changed_reserves.contains(&borrow.borrow_reserve)) {
                        debug!("[Liquidation Thread] Obligation reserves not changed, skip: {} {}", address.to_string().green(), obligation.to_string().green());
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            };

            if !should_process {
                continue;
            }

            if let Some(mut obligation) = self.obligation_map.remove(&address) {
                if let Err(e) = self.check_and_liquidate(klend_client, &address, &mut obligation, &lending_market, &reserves, &rts).await {
                    error!("[Liquidation Thread] Error checking/liquidating obligation {}: {}", address, e);
                }
                self.obligation_map.insert(*address, obligation);
                checked_obligation_count += 1;
            } else {
                match klend_client.fetch_obligation(&address).await {
                    Ok(mut obligation) => {
                        self.obligation_reservers_to_refresh.extend(obligation.deposits.iter().map(|coll| coll.deposit_reserve));
                        self.obligation_reservers_to_refresh.extend(obligation.borrows.iter().map(|borrow| borrow.borrow_reserve));
                        if let Err(e) = self.preload_swap_instructions(klend_client, &address, &obligation, &reserves).await {
                            error!("[Liquidation Thread] Error preloading swap instructions for obligation {}: {}", address, e);
                        }

                        if let Err(e) = self.check_and_liquidate(klend_client, &address, &mut obligation, &lending_market, &reserves, &rts).await {
                            error!("[Liquidation Thread] Error checking/liquidating obligation {}: {}", address, e);
                        }
                        self.obligation_map.insert(*address, obligation);
                        checked_obligation_count += 1;
                    }
                    Err(e) => {
                        error!("[Liquidation Thread] Error fetching obligation {}: {}", address, e);
                        continue;
                    }
                }
            }

            let en = start.elapsed().as_secs_f64();
            debug!("[Liquidation Thread] Processed obligation time used: {} in {}s", address.to_string().green(), en);
        }

        //根据obligation的borrow_factor_adjusted_debt_value_sf/unhealthy_borrow_value_sf对liquidatable_obligations进行排序, 值越大越靠前
        liquidatable_obligations.sort_by(|a, b| {
            let a_obligation = self.obligation_map.get(a);
            let b_obligation = self.obligation_map.get(b);
            if a_obligation.is_none() || b_obligation.is_none() {
                return std::cmp::Ordering::Equal;
            }
            let a_obligation = a_obligation.unwrap();
            let b_obligation = b_obligation.unwrap();
            let a_stats = math::obligation_info(a, a_obligation);
            let b_stats = math::obligation_info(b, b_obligation);

            // 添加除零检查
            let a_ratio = if a_stats.unhealthy_ltv > Fraction::ZERO {
                a_stats.ltv / a_stats.unhealthy_ltv
            } else {
                Fraction::ZERO
            };
            let b_ratio = if b_stats.unhealthy_ltv > Fraction::ZERO {
                b_stats.ltv / b_stats.unhealthy_ltv
            } else {
                Fraction::ZERO
            };

            debug!("{}: {}, {}: {}", a.to_string().green(), a_ratio, b.to_string().green(), b_ratio);
            b_ratio.partial_cmp(&a_ratio).unwrap_or(std::cmp::Ordering::Equal)
        });

        info!("sorted liquidatable_obligations: {:?}", liquidatable_obligations.iter().map(|obligation_key| {
            let obligation = self.obligation_map.get(obligation_key);
            match obligation {
                Some(obligation) => {
                    let obligation_stats = math::obligation_info(obligation_key, &obligation);

                    // 添加除零检查
                    let ratio = if obligation_stats.unhealthy_ltv > Fraction::ZERO {
                        obligation_stats.ltv / obligation_stats.unhealthy_ltv
                    } else {
                        Fraction::ZERO
                    };

                    let liquidatable: bool = obligation_stats.ltv > obligation_stats.unhealthy_ltv;
                    if liquidatable {
                        info!("Liquidatable obligation: {} {:?}", obligation_key.to_string().green(), obligation.to_string());
                    }
                    (*obligation_key, liquidatable, ratio.to_num::<f64>(), obligation_stats.borrowed_amount.to_num::<f64>(), obligation_stats.deposited_amount.to_num::<f64>())
                }
                None => {
                    (*obligation_key, false, 0.0, 0.0, 0.0)
                }
            }
        }).collect::<Vec<(Pubkey, bool, f64, f64, f64)>>());

        checked_obligation_count
    }

    pub async fn loop_liquidate(&mut self, klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
        loop {
            if let Err(e) = self.liquidate_in_loop(klend_client, scope.clone()).await {
                error!("[Liquidation Thread] Error: {}", e);
            }
        }
    }

    pub async fn stream_liquidate(&mut self, klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
        self.market_obligations_map = match load_obligations_map(scope.clone()).await {
            Ok(value) => value,
            Err(value) => return value,
        };

        for market_pubkey in self.market_obligations_map.keys() {
            let (market_accounts, rts) = match self.load_market_accounts_and_rts(klend_client, &market_pubkey).await {
                Ok(result) => result,
                Err(e) => {
                    error!("[Liquidation Thread] Error loading market accounts and rts {}: {}", market_pubkey, e);
                    continue;
                }
            };

            let OracleAccounts {
                pyth_accounts: _pyth_accounts,
                switchboard_accounts,
                scope_price_accounts,
            } = match oracle_accounts(&klend_client.local_client, &market_accounts.reserves).await {
                Ok(accounts) => accounts,
                Err(e) => {
                    error!("Error getting oracle accounts: {}", e);
                    continue;
                }
            };

            self.market_pubkeys.push(*market_pubkey);
            // Deduplicate scope price accounts by pubkey
            for account in scope_price_accounts {
                if !self.all_scope_price_accounts.iter().any(|(key, _, _)| *key == account.0) {
                    self.all_scope_price_accounts.push(account);
                }
            }

            // Deduplicate switchboard accounts by pubkey
            for account in switchboard_accounts {
                if !self.all_switchboard_accounts.iter().any(|(key, _, _)| *key == account.0) {
                    self.all_switchboard_accounts.push(account);
                }
            }

            // Insert individual reserves instead of nested structure
            self.all_reserves.extend(market_accounts.reserves);

            self.all_lending_market.insert(*market_pubkey, market_accounts.lending_market);
            self.all_rts.insert(*market_pubkey, rts);
        }

        println!("all_reserves: {:?}", self.all_reserves.keys());

        let _ = self.account_update_ws(
            klend_client,
            scope
        ).await;

        Ok(())
    }

    async fn load_market_accounts_and_rts(&self, klend_client: &Arc<KlendClient>, market: &Pubkey) -> Result<(MarketAccounts, HashMap<Pubkey, ReferrerTokenState>)> {
        let start = std::time::Instant::now();
        let market_accs = klend_client.fetch_market_and_reserves(market).await?;
        let rts = klend_client.fetch_referrer_token_states().await?;
        let en_accounts = start.elapsed().as_secs_f64();
        info!("Loading market accounts and rts {} time used: {}s", market.to_string().green(), en_accounts);
        Ok((market_accs, rts))
    }



    pub async fn account_update_ws(
        &mut self,
        klend_client: &Arc<KlendClient>,
        scope: String,
    ) -> anyhow::Result<()> {
        self.collect_subscription_keys().await?;

        let competitors = load_competitors_from_file()?;
        info!("competitors: {:?}", competitors);

        let mut accounts = HashMap::new();
        let account_filter = self.subscribing_pubkeys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
        accounts.insert(
            "client".to_string(),
            SubscribeRequestFilterAccounts {
                account: account_filter,
                owner: vec![],
                filters: vec![],
            },
        );

        let transactions_filter = competitors.iter().map(|key| key.to_string()).collect::<Vec<String>>();
        let mut transactions = HashMap::new();
        transactions.insert(
            "transactions".to_string(),
            SubscribeRequestFilterTransactions {
                account_required: transactions_filter,
                vote: Some(false),
                failed: Some(false),
                ..Default::default()
            },
        );

        let endpoint = "ws://198.244.253.218:10000".to_string();
        let x_token = None;

        let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;
        let (mut subscribe_tx, mut stream) = client.subscribe().await.map_err(|e| {
            anyhow::Error::msg(format!(
                "Failed to subscribe: {} ({})",
                endpoint, e
            ))
        })?;
        info!("Connected to the gRPC server");

        subscribe_tx
            .send(SubscribeRequest {
                slots: HashMap::new(),
                accounts,
                transactions: transactions.clone(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(CommitmentLevel::Processed.into()),
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            })
            .await
            .map_err(|e| {
                anyhow::Error::msg(format!(
                    "Failed to send: {} ({})",
                    endpoint, e
                ))
            })?;

        // Initialize clock if not already set
        if self.clock.is_none() {
            self.clock = Some(match sysvars::clock(&klend_client.local_client.client).await {
                Ok(clock) => clock,
                Err(_e) => {
                    error!("Failed to get clock");
                    return Err(anyhow::Error::msg("Failed to get clock"));
                }
            });
        }

        while let Some(message) = stream.next().await {
            if let Ok(msg) = message {
                match msg.update_oneof {
                    Some(UpdateOneof::Account(account)) => {
                        if let Some(account) = account.account {
                            let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();
                            let data = account.data;

                            if self.handle_clock_update(&pubkey, &data).await? {
                                continue;
                            }

                            if self.handle_obligation_update(
                                &pubkey,
                                &data,
                                klend_client,
                            ).await? {
                                continue;
                            }

                            if let Some(price_changed_reserves) = self.handle_price_update(
                                &pubkey,
                                &data,
                            ).await? {
                                self.process_price_changes(
                                    klend_client,
                                    &price_changed_reserves,
                                    &pubkey,
                                    &data,
                                ).await?;

                                // Refresh obligations map and update subscription if needed
                                self.refresh_obligations_subscription(
                                    &scope,
                                    &mut subscribe_tx,
                                ).await?;
                            }
                        }
                    }
                    Some(UpdateOneof::Transaction(transaction)) => {
                        self.handle_transaction_update(transaction).await?;
                    }
                    _ => {
                        debug!("Unknown update oneof: {:?}", msg.update_oneof);
                    }
                }
            } else {
                info!("Account Update error: {:?}", message);
                break;
            }
        }

        Ok(())
    }

    async fn collect_subscription_keys(
        &mut self,
    ) -> anyhow::Result<()> {
        let scope_price_pubkeys = self.all_scope_price_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
        let switchboard_pubkeys = self.all_switchboard_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
        let all_obligations_pubkeys = self.market_obligations_map.values().flatten().copied().collect::<Vec<Pubkey>>();
        self.subscribing_pubkeys = HashSet::<Pubkey>::from_iter([scope_price_pubkeys.clone(), switchboard_pubkeys, all_obligations_pubkeys.clone()].concat()).into_iter().collect();
        self.subscribing_pubkeys.push(Clock::id());
        info!("account update ws: {:?}", self.subscribing_pubkeys);

        Ok(())
    }



    async fn handle_clock_update(
        &mut self,
        pubkey: &Pubkey,
        data: &Vec<u8>,
    ) -> anyhow::Result<bool> {
        if *pubkey == Clock::id() {
            let account_data = Account {
                lamports: 0, // We don't have lamports from the update
                data: data.clone(),
                owner: Clock::id(), // Clock is owned by the system program
                executable: false,
                rent_epoch: 0,
            };
            if let Ok(updated_clock) = account_data.deserialize_data::<Clock>() {
                self.clock = Some(updated_clock);
                debug!("Clock updated: {:?}", self.clock);
            }
            return Ok(true);
        }
        Ok(false)
    }

    async fn handle_obligation_update(
        &mut self,
        pubkey: &Pubkey,
        data: &Vec<u8>,
        klend_client: &Arc<KlendClient>,
    ) -> anyhow::Result<bool> {
        let all_obligations_pubkeys: Vec<Pubkey> = self.market_obligations_map.values().flatten().copied().collect();

        if all_obligations_pubkeys.contains(pubkey) {
            let mut data_slice: &[u8] = data;
            let obligation = match Obligation::try_deserialize(&mut data_slice) {
                Ok(obligation) => obligation,
                Err(e) => {
                    error!("Failed to deserialize obligation: {:?}, pubkey: {:?}", e, pubkey);
                    return Ok(true);
                }
            };
            let mut obligation_map_write = self.shared_obligation_map.write().unwrap();
            obligation_map_write.insert(*pubkey, obligation.clone());
            drop(obligation_map_write); // Release the lock early
            info!("Obligation updated: {:?}, obligation: {:?}", pubkey, obligation);

            // Clone the reserves to avoid borrowing conflict
            let all_reserves_clone = self.all_reserves.clone();
            if let Err(e) = self.preload_swap_instructions(klend_client, pubkey, &obligation, &all_reserves_clone).await {
                error!("Failed to preload swap instructions for obligation {}: {}", pubkey, e);
            }
            return Ok(true);
        }
        Ok(false)
    }

    async fn handle_price_update(
        &mut self,
        pubkey: &Pubkey,
        data: &Vec<u8>,
    ) -> anyhow::Result<Option<HashSet<Pubkey>>> {
        if data.len() < 8 {
            debug!("Account: {:?} is not scope price account", pubkey);
            return Ok(None);
        }

        let disc_bytes = &data[0..8];
        if disc_bytes != ScopePrices::discriminator() {
            debug!("Account: {:?} is not scope price account", pubkey);
            return Ok(None);
        }

        let scope_prices = bytemuck::from_bytes::<ScopePrices>(&data[8..]);
        let mut price_changed_reserves: HashSet<Pubkey> = HashSet::new();

        for (reserve_pubkey, reserve) in self.all_reserves.iter() {
            if reserve.config.token_info.scope_configuration.price_feed == *pubkey {
                                    if let Some(price) = get_price_usd(&scope_prices, reserve.config.token_info.scope_configuration.price_chain) {
                        if let Some(old_price) = self.reserves_prices.get(reserve_pubkey) {
                            if old_price.price_value != price.price_value {
                                price_changed_reserves.insert(*reserve_pubkey);
                                self.reserves_prices.insert(*reserve_pubkey, price.clone());
                                info!("Price changed for reserve: {} new price: {:?}", reserve.config.token_info.symbol(), price);
                            }
                        } else {
                            price_changed_reserves.insert(*reserve_pubkey);
                            self.reserves_prices.insert(*reserve_pubkey, price.clone());
                            info!("Price changed for reserve: {} new price: {:?}", reserve.config.token_info.symbol(), price);
                        }
                    }
            }
        }

        if price_changed_reserves.is_empty() {
            info!("No price changed for reserves, skip");
            return Ok(None);
        }

        Ok(Some(price_changed_reserves))
    }

    async fn process_price_changes(
        &mut self,
        klend_client: &Arc<KlendClient>,
        price_changed_reserves: &HashSet<Pubkey>,
        updated_account_pubkey: &Pubkey,
        updated_account_data: &Vec<u8>,
    ) -> anyhow::Result<()> {
        // Clone market_pubkeys to avoid borrowing conflicts
        let market_pubkeys = self.market_pubkeys.clone();

        for market_pubkey in &market_pubkeys {
            let start = std::time::Instant::now();

            // Refresh market first - clone clock to avoid borrow conflicts
            let clock = self.clock.as_ref().unwrap().clone();
            let _ = crate::refresh_market(klend_client,
                market_pubkey,
                &Vec::new(),
                &mut self.all_reserves,
                self.all_lending_market.get_mut(market_pubkey).unwrap(),
                &clock,
                Some(&mut self.all_scope_price_accounts),
                Some(&mut self.all_switchboard_accounts),
                Some(&HashMap::from([(*updated_account_pubkey, updated_account_data.clone())]))
            ).await;

            // Now scan obligations - completely separate the borrows
            let obligations_len = match self.market_obligations_map.get(market_pubkey) {
                Some(obligations) => obligations.len(),
                None => {
                    info!("No obligations found for market: {:?}", market_pubkey);
                    continue;
                }
            };

            // Clone all necessary data to avoid borrow conflicts
            let reserves_clone = self.all_reserves.clone();
            let lending_market = self.all_lending_market.get(market_pubkey).cloned().unwrap();
            let rts = self.all_rts.get(market_pubkey).cloned().unwrap();

            // Scan obligations - need to extract obligations first to avoid borrow conflicts
            let checked_obligation_count = if let Some(obligations) = self.market_obligations_map.remove(market_pubkey) {
                // Put the obligations back after scanning
                let mut mutable_obligations = obligations;
                let count = self.scan_obligations(klend_client,
                    &mut mutable_obligations,
                    &reserves_clone,
                    &lending_market,
                    &rts,
                    Some(price_changed_reserves)
                ).await;

                // Put the obligations back
                self.market_obligations_map.insert(*market_pubkey, mutable_obligations);
                count
            } else {
                0
            };

            let duration = start.elapsed();
            info!("Scan {} obligations, time used: {:?} s, checked {} obligations", obligations_len, duration, checked_obligation_count);
        }

        Ok(())
    }



    async fn handle_transaction_update(
        &self,
        transaction: yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction,
    ) -> anyhow::Result<()> {
        if let Some(transaction) = transaction.transaction {
            let signature = bs58::encode(transaction.signature.as_slice()).into_string();
            info!("Transaction of competitor, signature: {:?}", signature);
        }
        Ok(())
    }

        async fn refresh_obligations_subscription<T>(
        &mut self,
        scope: &str,
        subscribe_tx: &mut T,
    ) -> anyhow::Result<()>
    where
        T: futures::SinkExt<SubscribeRequest> + Unpin,
        T::Error: std::fmt::Debug + std::fmt::Display,
    {
        match load_obligations_map(scope.to_string()).await {
            Ok(updated_obligations_map) => {
                let updated_obligations_pubkeys = updated_obligations_map.values().flatten().cloned().collect::<Vec<Pubkey>>();
                let obligations_map_pubkeys = self.market_obligations_map.values().flatten().cloned().collect::<Vec<Pubkey>>();
                let mut obligations_to_refresh = Vec::new();
                for pubkey in updated_obligations_pubkeys {
                    if !obligations_map_pubkeys.contains(&pubkey) {
                        obligations_to_refresh.push(pubkey);
                    }
                }

                if !obligations_to_refresh.is_empty() {
                    self.subscribing_pubkeys.extend(obligations_to_refresh.clone());

                    let mut accounts = HashMap::new();
                    let account_filter = self.subscribing_pubkeys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
                    accounts.insert(
                        "client".to_string(),
                        SubscribeRequestFilterAccounts {
                            account: account_filter,
                            owner: vec![],
                            filters: vec![],
                        },
                    );

                    let competitors = load_competitors_from_file()?;
                    let transactions_filter = competitors.iter().map(|key| key.to_string()).collect::<Vec<String>>();
                    let mut transactions = HashMap::new();
                    transactions.insert(
                        "transactions".to_string(),
                        SubscribeRequestFilterTransactions {
                            account_required: transactions_filter,
                            vote: Some(false),
                            failed: Some(false),
                            ..Default::default()
                        },
                    );

                    if let Err(e) = subscribe_tx.send(
                        SubscribeRequest {
                            slots: HashMap::new(),
                            accounts,
                            transactions,
                            blocks: HashMap::new(),
                            blocks_meta: HashMap::new(),
                            commitment: Some(CommitmentLevel::Processed.into()),
                            accounts_data_slice: vec![],
                            transactions_status: HashMap::new(),
                            ping: None,
                            entry: HashMap::new(),
                        }
                    ).await {
                        error!("Failed to subscribe: {}", e);
                    } else {
                        self.market_obligations_map.clear();
                        self.market_obligations_map.extend(updated_obligations_map);
                        info!("Successfully loaded {} markets from obligations map", self.market_obligations_map.len());
                    }
                } else {
                    info!("No new obligations to refresh");
                }
            }
            Err(e) => {
                error!("Failed to load obligations map: {:?}", e);
            }
        }
        Ok(())
    }
}

impl Default for LiquidationEngine {
    fn default() -> Self {
        Self::new()
    }
}