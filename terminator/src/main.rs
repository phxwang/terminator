use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::{Arc, RwLock}, time::Duration, fs, str::FromStr, path::Path};

use anchor_client::{solana_sdk::pubkey::Pubkey, Cluster};
use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use juno::DecompiledVersionedTx;
use kamino_lending::{Reserve, LendingMarket, ReferrerTokenState, Obligation};
use solana_sdk::{
    signer::Signer,
    clock::Clock,
    compute_budget,
    account::Account,
};
use tokio::time::sleep;
use tracing::{info, warn, debug, error};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt, fmt, Layer};
use solana_sdk::instruction::Instruction;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;

use crate::{
    accounts::{map_accounts_and_create_infos, oracle_accounts, OracleAccounts, MarketAccounts, account_update_ws},
    client::KlendClient,
    config::get_lending_markets,
    jupiter::get_best_swap_instructions,
    liquidator::{Holdings, Liquidator},
    model::StateWithKey,
    operations::{
        obligation_reserves, referrer_token_states_of_obligation, split_obligations,
        ObligationReserves, SplitObligations,
    },
    px::fetch_jup_prices,
    utils::get_all_reserve_mints,
    fs::File,
};

pub mod accounts;
pub mod client;
mod config;
pub mod consts;
pub mod instructions;
pub mod jupiter;
pub mod liquidator;
pub mod lookup_tables;
pub mod macros;
pub mod math;
mod model;
pub mod operations;
mod px;
pub mod sysvars;
mod utils;
pub mod yellowstone_transaction;

const USDC_MINT_STR: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Klend program id
    /// Default is mainnet: KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD
    /// If compiled with staging profile, default is: SLendK7ySfcEzyaFqy93gDnD3RtrpXJcnRwb6zFHJSh
    #[clap(long, env, parse(try_from_str))]
    klend_program_id: Option<Pubkey>,

    /// Connect to solana validator
    #[clap(long, env, parse(try_from_str), default_value = "localnet")]
    cluster: Cluster,

    /// Connect to solana validator
    #[clap(long, env, parse(try_from_str), default_value = "localnet")]
    local_cluster: Cluster,

    /// Account keypair to pay for the transactions
    #[clap(long, env, parse(from_os_str))]
    keypair: Option<PathBuf>,

    /// Markets to be considered
    /// Defaults to using api endpoint if not specified
    #[clap(long, env, parse(try_from_str))]
    markets: Option<Vec<Pubkey>>,

    /// Set flag to activate json log output
    #[clap(long, env = "JSON_LOGS")]
    json: bool,

    /// Print timestamps in logs (not needed on grafana)
    #[clap(long, env, default_value = "true")]
    log_timestamps: bool,

    /// Log file path (optional, if not provided logs only to console)
    #[clap(long, env)]
    log_file: Option<PathBuf>,

    /// Run with embedded webserver (default false)
    #[clap(short, env, long)]
    server: bool,

    /// Embedded webserver port
    /// Only valid if --server is also used
    #[clap(long, env, default_value = "8080")]
    server_port: u16,

    /// Subcommand to execute
    #[clap(subcommand)]
    action: Actions,
}

#[derive(Parser, Debug)]
pub struct RebalanceArgs {
    /// What to hold the balance in
    #[clap(long, env, parse(try_from_str), default_value = USDC_MINT_STR)]
    base_currency: Pubkey,

    /// Necessary for fees
    #[clap(long, env, parse(try_from_str), default_value = "0.5")]
    min_sol_balance: f64,

    /// Used for jup quote pxs etc.
    #[clap(long, env, parse(try_from_str), default_value = USDC_MINT_STR)]
    usdc_mint: Pubkey,

    /// From token
    #[clap(long, env, parse(try_from_str), default_value = "0.35")]
    rebalance_slippage_pct: f64,

    /// Threshold value to trigger a rebalance
    #[clap(long, env, parse(try_from_str), default_value = "5.0")]
    non_swappable_dust_usd_value: f64,
}

#[derive(Debug, Subcommand)]
pub enum Actions {
    /// Automatically refresh the prices
    #[clap()]
    Crank {
        /// Obligation to be cranked
        #[clap(long, env, parse(try_from_str))]
        obligation: Option<Pubkey>,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
    #[clap()]
    Liquidate {
        /// Obligation to be liquidated
        #[clap(long, env, parse(try_from_str))]
        obligation: Pubkey,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
    #[clap()]
    Swap {
        /// From token
        #[clap(long, env, parse(try_from_str))]
        from: Pubkey,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        to: Pubkey,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        amount: f64,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        slippage_pct: f64,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },

    #[clap()]
    LoopLiquidate {
        #[clap(long, env, parse(try_from_str))]
        scope: String,
        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
    #[clap()]
    StreamLiquidate {
        #[clap(long, env, parse(try_from_str))]
        scope: String,
        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Ok(e) = std::env::var("ENV") {
        dotenvy::from_filename(e)?;
    } else if PathBuf::from(".env").exists() {
        dotenvy::from_filename(".env")?;
    };
    let args: Args = Args::parse();

    let create_env_filter = || {
        let env_filter = EnvFilter::from_default_env();
        env_filter.add_directive("kamino_lending=warn".parse().unwrap())
    };

    // Create console layer
    let console_layer = fmt::layer()
        .compact()
        .with_filter(create_env_filter());

    let mut layers = vec![console_layer.boxed()];

    // Add file layer if log_file is specified
    if let Some(log_file) = &args.log_file {
        // Create logs directory if it doesn't exist
        if let Some(parent) = log_file.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)?;

        let file_layer = fmt::layer()
            .with_writer(file)
            .with_ansi(false) // No color codes in files
            .compact()
            .with_filter(create_env_filter());

        layers.push(file_layer.boxed());
    }

    tracing_subscriber::registry()
        .with(layers)
        .init();

    info!("Starting with {:#?}", args);

    info!("Initializing client..");
    let mut klend_client = config::get_client_for_action(&args)?;

    if klend_client.liquidator.atas.is_empty() {
        info!("Liquidator ATAs are empty, loading...");
        klend_client.liquidator = Liquidator::init(&klend_client).await?;
    }

    klend_client.load_default_lookup_table().await;

    let klend_client = Arc::new(klend_client);

    it_event!("klend_terminator::started");

    info!("Executing action..");
    match args.action {
        Actions::Crank {
            obligation: obligation_filter,
            rebalance_args: _,
        } => crank(&klend_client, obligation_filter).await,
        Actions::Liquidate {
            obligation,
            rebalance_args: _,
        } => liquidate(&klend_client, &obligation, false).await,
        Actions::Swap {
            from,
            to,
            amount,
            slippage_pct,
            rebalance_args: _,
        } => swap::swap_action(&klend_client, from, to, amount, slippage_pct).await,
        Actions::LoopLiquidate { scope, rebalance_args: _ } => loop_liquidate(&klend_client, scope).await,
        Actions::StreamLiquidate { scope, rebalance_args: _ } => stream_liquidate(&klend_client, scope).await,
    }
}


pub mod swap {
    use super::*;

    pub async fn swap_action(
        klend_client: &Arc<KlendClient>,
        from: Pubkey,
        to: Pubkey,
        amount: f64,
        slippage_pct: f64,
    ) -> Result<()> {
        let rebalance_config = match &klend_client.rebalance_config {
            None => Err(anyhow::anyhow!("Rebalance settings not found")),
            Some(c) => Ok(c),
        }?;

        let lending_markets = get_lending_markets(&klend_client.program_id).await?;
        let markets =
            client::utils::fetch_markets_and_reserves(klend_client, &lending_markets).await?;
        let (reserves, _, l_mints) = get_all_reserve_mints(&markets);
        let _pxs = fetch_jup_prices(&l_mints, &rebalance_config.usdc_mint, amount as f32).await?;
        let holdings = klend_client
            .liquidator
            .fetch_holdings(&klend_client.client.client, &reserves)
            .await?;
        swap(klend_client, &holdings, &from, &to, amount, slippage_pct).await
    }

    pub async fn swap_with_jupiter_ixns(
        klend_client: &KlendClient,
        from: &Pubkey,
        to: &Pubkey,
        amount: u64,
        output_amount: Option<u64>,
        slippage_pct: f64,
    ) -> Result<(Vec<Instruction>, Option<Vec<AddressLookupTableAccount>>)> {
        //let from_token = holdings.holding_of(from)?;
        //let _to_token = holdings.holding_of(to)?;
        let user = klend_client.liquidator.wallet.pubkey();

        //let amount_to_swap = (amount * 10f64.powf(from_token.decimals as f64)).floor() as u64;
        let slippage_bps = (slippage_pct * 100f64).floor() as u16;

        let jupiter_swap = get_best_swap_instructions(
            from,
            to,
            amount,
            output_amount,
            true,
            Some(slippage_bps),
            None,
            user,
            &klend_client.client.client,
            None,
            None,
        )
        .await?;

        let DecompiledVersionedTx {
            lookup_tables,
            instructions: jup_ixs,
        } = jupiter_swap;

        Ok((jup_ixs, lookup_tables))
    }

    pub async fn swap(
        _klend_client: &KlendClient,
        _holdings: &Holdings,
        _from: &Pubkey,
        _to: &Pubkey,
        _amount: f64,
        _slippage_pct: f64,
    ) -> Result<()> {
        // https://quote-api.jup.ag/v6/quote?inputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&outputMint=So11111111111111111111111111111111111111112&amount=94576524&slippageBps=50&swapMode=ExactIn&onlyDirectRoutes=true&asLegacyTransaction=false

        /*let (jup_ixs, lookup_tables) = swap_with_jupiter_ixns(klend_client, holdings, from, to, amount, slippage_pct).await?;

        let mut builder = klend_client.client.tx_builder().add_ixs(jup_ixs);

        if let Some(lookup_tables) = lookup_tables {
            for table in lookup_tables.into_iter() {
                builder = builder.add_lookup_table(table);
            }
        }

        let tx = builder.build(&[]).await?;

        info!("Sending transaction...");
        let (sig, _) = klend_client
            .client
            .send_retry_and_confirm_transaction(tx, None, false)
            .await?;
        info!("Executed transaction: {:?}", sig);*/

        Ok(())
    }
}

async fn liquidate(klend_client: &Arc<KlendClient>, obligation: &Pubkey, dont_refresh: bool) -> Result<()> {
    info!("Liquidating obligation {}", obligation.to_string().green());
    debug!("Liquidator ATAs: {:?}", klend_client.liquidator.atas);
    let rebalance_config = match &klend_client.rebalance_config {
        None => Err(anyhow::anyhow!("Rebalance settings not found")),
        Some(c) => Ok(c),
    }?;

    let (ob, reserves, market, _rts, clock) = if dont_refresh {
        // When dont_refresh is true, we avoid redundant data loading
        // The data should already be fresh from check_and_liquidate
        info!("Skipping data refresh as dont_refresh=true");

        // We still need to fetch the obligation to get basic info for liquidation
        let ob = klend_client.fetch_obligation(obligation).await?;
        let market_accs = klend_client
            .fetch_market_and_reserves(&ob.lending_market)
            .await?;

        (
            ob,
            market_accs.reserves,
            market_accs.lending_market,
            HashMap::new(), // Empty RTS as we skip refresh
            sysvars::clock(&klend_client.local_client.client).await?,
        )
    } else {
        // Original refresh logic when dont_refresh is false
        info!("Performing full data refresh");

        // Reload accounts
        let mut ob = klend_client.fetch_obligation(obligation).await?;
        let market_accs = klend_client
            .fetch_market_and_reserves(&ob.lending_market)
            .await?;

        let mut reserves = market_accs.reserves;
        let market = market_accs.lending_market;
        // todo - don't load all
        let rts = klend_client.fetch_referrer_token_states().await?;

        let clock = sysvars::clock(&klend_client.local_client.client).await?;

        //find first borrowed_amount > 0
        let debt_res_key = ob.borrows.iter().find(|b| b.borrowed_amount_sf > 0).unwrap().borrow_reserve;

        //find first deposited_amount > 0
        let coll_res_key = ob.deposits.iter().find(|d| d.deposited_amount > 0).unwrap().deposit_reserve;

        info!("Debt reserve key: {}", debt_res_key.to_string().green());
        info!("Coll reserve key: {}", coll_res_key.to_string().green());
        debug!("Reserves for {:?}: {:?}", ob.lending_market, reserves.keys());

        // Refresh reserves and obligation
        operations::refresh_reserves_and_obligation(
            klend_client,
            &debt_res_key,
            &coll_res_key,
            obligation,
            &mut ob,
            &mut reserves,
            &rts,
            &market,
            &clock,
        )
        .await?;

        (ob, reserves, market, rts, clock)
    };

    println!("ob: {:?}", ob);

    //find first borrowed_amount > 0
    let debt_res_key = match ob.borrows.iter().find(|b| b.borrowed_amount_sf > 0) {
        Some(borrow) => borrow.borrow_reserve,
        None => {
            error!("No borrowed amount found for obligation {}", obligation);
            return Err(anyhow::anyhow!("No borrowed amount found for obligation"));
        }
    };

    //find first deposited_amount > 0
    let coll_res_key = match ob.deposits.iter().find(|d| d.deposited_amount > 0) {
        Some(deposit) => deposit.deposit_reserve,
        None => {
            error!("No deposited amount found for obligation {}", obligation);
            return Err(anyhow::anyhow!("No deposited amount found for obligation"));
        }
    };

    info!("Debt reserve key: {}", debt_res_key.to_string().green());
    info!("Coll reserve key: {}", coll_res_key.to_string().green());
    debug!("Reserves for {:?}: {:?}", ob.lending_market, reserves.keys());

    // Now it's all fully refreshed and up to date
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
    let _debt_mint = debt_reserve_state.liquidity.mint_pubkey;
    let debt_reserve = StateWithKey::new(debt_reserve_state, debt_res_key);
    let coll_reserve = StateWithKey::new(coll_reserve_state, coll_res_key);
    let lending_market = StateWithKey::new(market, ob.lending_market);
    let obligation = StateWithKey::new(ob, *obligation);
    //let pxs = fetch_jup_prices(&[debt_mint], &rebalance_config.usdc_mint, 100.0).await?;
    //let holdings = klend_client
    //    .liquidator
    //    .fetch_holdings(&klend_client.client.client, &reserves)
    //    .await?;

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
    //let liquidation_strategy = math::decide_liquidation_strategy(
    //    &rebalance_config.base_token,
    //    &obligation,
    //    &lending_market,
    //    &coll_reserve,
    //    &debt_reserve,
    //    &clock,
    //    max_allowed_ltv_override_pct_opt,
    //    liquidation_swap_slippage_pct,
    //    holdings,
    //)?;

    /*let (swap_amount, liquidate_amount) = match liquidation_strategy {
        Some(LiquidationStrategy::LiquidateAndRedeem(liquidate_amount)) => (0, liquidate_amount),
        Some(LiquidationStrategy::SwapThenLiquidate(swap_amount, liquidate_amount)) => {
            (swap_amount, liquidate_amount)
        }
        None => (0, 0),
    };*/
    //let _swap_amount = 0;
    let liquidate_amount = math::get_liquidatable_amount(
        &obligation,
        &lending_market,
        &coll_reserve,
        &debt_reserve,
        &clock,
        max_allowed_ltv_override_pct_opt,
        liquidation_swap_slippage_pct,
    )?;

    info!("Liquidate amount: {}", liquidate_amount);

    // Simulate liquidation
    let res = kamino_lending::lending_market::lending_operations::liquidate_and_redeem(
        &lending_market.state.borrow(),
        &debt_reserve,
        &coll_reserve,
        &mut obligation.state.borrow_mut(),
        &clock,
        liquidate_amount,
        min_acceptable_received_collateral_amount,
        max_allowed_ltv_override_pct_opt,
        deposit_reserves.into_iter(),
    );

    println!("Simulating the liquidation {:#?}", res);

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
                info!("Net withdraw liquidity amount: {}", net_withdraw_liquidity_amount);
            }
            None => {
                warn!("Total withdraw liquidity amount is None");
            }
        }

        let _user = klend_client.liquidator.wallet.pubkey();
        let _base_mint = &rebalance_config.base_token;

        let mut ixns = vec![];
        let mut luts = vec![];

        /*if swap_amount > 0 {
            let jupiter_swap = get_best_swap_instructions(
                base_mint,
                &debt_mint,
                swap_amount,
                false,
                Some((liquidation_swap_slippage_pct * 100.0) as u16),
                None,
                user,
                &klend_client.client.client,
                None,
                None,
            )
            .await
            .unwrap();

            let DecompiledVersionedTx {
                lookup_tables,
                instructions: jup_ixs,
            } = jupiter_swap;

            // Filter compute budget ixns
            let jup_ixs = jup_ixs
                .into_iter()
                .filter(|ix| ix.program_id != compute_budget::id())
                .collect_vec();

            ixns.extend_from_slice(&jup_ixs);

            if let Some(lookup_tables) = lookup_tables {
                for table in lookup_tables.into_iter() {
                    luts.push(table);
                }
            }
        }*/

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
            )
            .await {
                Ok(ixns) => ixns,
                Err(e) => {
                    error!("Error creating liquidate instructions: {}", e);
                    return Err(e);
                }
            };
        ixns.extend_from_slice(&liquidate_ixns);

        //add jupiter swap ixns
        let (jup_ixs, lookup_tables) = swap::swap_with_jupiter_ixns(
            klend_client,
            &coll_reserve.state.borrow().liquidity.mint_pubkey,
            &debt_reserve.state.borrow().liquidity.mint_pubkey,
            net_withdraw_liquidity_amount,
            Some(liquidate_amount),
            liquidation_swap_slippage_pct
        ).await?;

        info!("Jupiter swap ixns count: {:?}", jup_ixs.len());
        debug!("Jupiter swap ixns: {:?}", jup_ixs);
        debug!("Jupiter swap ixns lookup tables: {:?}", lookup_tables);

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
        for lut in luts {
            txn = txn.add_lookup_table(lut);
        }


        let txn_b64 = txn.to_base64();
        println!(
            "Simulation: https://explorer.solana.com/tx/inspector?message={}",
            urlencoding::encode(&txn_b64)
        );

        let txn = match txn.build_with_budget_and_fee(&[]).await {
            Ok(txn) => txn,
            Err(e) => {
                error!("Error building transaction: {}", e);
                return Err(e.into());
            }
        };

        for ix in ixns {
            info!("Instruction: {:?} {:?}", ix.program_id, ix.data);
        }

         match klend_client
                .local_client
                .client
                .simulate_transaction(&txn)
                .await
                {
                    Ok(res) => {
                        info!("Simulation result: {:?}", res);

                        let simulate_only = false;

                        if !simulate_only {
                            match klend_client
                                .local_client
                                .send_retry_and_confirm_transaction(txn, None, false)
                                .await
                                {
                                    Ok(sig) => {
                                        info!("Liquidation tx sent: {:?}", sig.0);
                                        info!("Liquidation tx res: {:?}", sig.1);
                                    }
                                    Err(e) => {
                                        error!("Liquidation tx error: {:?}", e);
                                    }
                                }
                        }
                    }
                    Err(e) => {
                        error!("Simulation error: {:?}", e);
                    }
                }
    }
    Ok(())
}

async fn check_and_liquidate(klend_client: &Arc<KlendClient>, address: &Pubkey, mut obligation: Obligation, lending_market: &LendingMarket, clock: &Clock, reserves: &HashMap<Pubkey, Reserve>, rts: &HashMap<Pubkey, ReferrerTokenState>) -> Result<()> {
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

    if let Err(e) = kamino_lending::lending_market::lending_operations::refresh_obligation(
        &mut obligation,
        &lending_market,
        clock.slot,
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
        info!("[Liquidation Thread] Liquidating obligation start: {} {}", address.to_string().green(), obligation.to_string().green());

        let liquidate_start = std::time::Instant::now();
        match liquidate(klend_client, address, true).await {
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

async fn liquidate_in_loop(klend_client: &Arc<KlendClient>, scope: String, obligation_map: &mut HashMap<Pubkey, Obligation>, market_accounts_map: &mut HashMap<Pubkey, (HashMap<Pubkey, Reserve>, LendingMarket, HashMap<Pubkey, ReferrerTokenState>)>) -> Result<()> {
    let start = std::time::Instant::now();

    let obligations_map = match load_obligations_map(scope).await {
        Ok(value) => value,
        Err(value) => return value,
    };

    let mut total_liquidatable_obligations = 0;

    let mut obligation_reservers_to_refresh: Vec<Pubkey> = vec![];

    let clock = match sysvars::get_clock(&klend_client.local_client.client).await {
        Ok(clock) => clock,
        Err(e) => {
            error!("Error getting clock: {}", e);
            return Err(e.into());
        }
    };

    let en_clock = start.elapsed().as_secs_f64();
    debug!("Refreshing market clock time used: {}s", en_clock);


    for (market, liquidatable_obligations) in obligations_map.iter() {
        info!("[Liquidation Thread]{}: {} liquidatable obligations found", market.green(), liquidatable_obligations.len());
        total_liquidatable_obligations += liquidatable_obligations.len();

        let market_pubkey = match Pubkey::from_str(market) {
            Ok(pubkey) => pubkey,
            Err(e) => {
                error!("[Liquidation Thread] Invalid market pubkey {}: {}", market, e);
                continue;
            }
        };

        if !market_accounts_map.contains_key(&market_pubkey) {
            let (market_accounts, rts) = match load_market_accounts_and_rts(klend_client, &market_pubkey).await {
                Ok(result) => result,
                Err(e) => {
                    error!("[Liquidation Thread] Error loading market accounts and rts {}: {}", market_pubkey, e);
                    continue;
                }
            };
            market_accounts_map.insert(market_pubkey, (market_accounts.reserves, market_accounts.lending_market, rts));
        }

        let (reserves, lending_market, rts) = market_accounts_map.get_mut(&market_pubkey).unwrap();

        //only refresh reserves in obligations
        let refresh_start = std::time::Instant::now();
        match refresh_market(klend_client,
            &market_pubkey,
            &obligation_reservers_to_refresh,
            reserves,
            lending_market,
            &clock,
            None,
            None,
            None).await {
            Ok(_) => (),
            Err(e) => {
                error!("[Liquidation Thread] Error refreshing market {}: {}", market_pubkey, e);
                continue;
            }
        };
        let refresh_en = refresh_start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed market {} in {}s", market_pubkey.to_string().green(), refresh_en);

        scan_obligations(klend_client, obligation_map, &mut obligation_reservers_to_refresh, &clock, liquidatable_obligations, reserves, lending_market, rts).await;
    }
    let en = start.elapsed().as_secs_f64();
    info!("[Liquidation Thread] Scanned {} obligations in {}s", total_liquidatable_obligations, en);

    Ok(())
}

async fn scan_obligations(
    klend_client: &Arc<KlendClient>,
    obligation_map: &mut HashMap<Pubkey, Obligation>,
    obligation_reservers_to_refresh: &mut Vec<Pubkey>,
    clock: &Clock, liquidatable_obligations: &Vec<String>,
    reserves: &HashMap<Pubkey, Reserve>,
    lending_market: &LendingMarket,
    rts: &HashMap<Pubkey, ReferrerTokenState>) {
    for address_str in liquidatable_obligations.iter() {
        let address = match Pubkey::from_str(address_str) {
            Ok(pubkey) => pubkey,
            Err(e) => {
                error!("[Liquidation Thread] Invalid obligation address {}: {}", address_str, e);
                continue;
            }
        };

        let start = std::time::Instant::now();

        if let Some(obligation) = obligation_map.get(&address) {
            if let Err(e) = check_and_liquidate(klend_client, &address, *obligation, &lending_market, clock, &reserves, &rts).await {
                error!("[Liquidation Thread] Error checking/liquidating obligation {}: {}", address, e);
            }
        } else {
            match klend_client.fetch_obligation(&address).await {
                Ok(obligation) => {
                    obligation_map.insert(address, obligation);
                    obligation_reservers_to_refresh.extend(obligation.deposits.iter().map(|coll| coll.deposit_reserve));
                    obligation_reservers_to_refresh.extend(obligation.borrows.iter().map(|borrow| borrow.borrow_reserve));
                    if let Err(e) = check_and_liquidate(klend_client, &address, obligation, &lending_market, clock, &reserves, &rts).await {
                        error!("[Liquidation Thread] Error checking/liquidating obligation {}: {}", address, e);
                    }
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
}

async fn load_obligations_map(scope: String) -> Result<HashMap<String, Vec<String>>, std::result::Result<(), anyhow::Error>> {
    let file_path = format!("{}.json", scope);
    if !Path::new(&file_path).exists() {
        info!("[Liquidation Thread] File {} does not exist", file_path);
        sleep(Duration::from_secs(5)).await;
        return Err(Ok(()));
    }
    let file = match File::open(&file_path) {
        Ok(file) => file,
        Err(e) => {
            error!("[Liquidation Thread] Error opening file {}: {}", file_path, e);
            sleep(Duration::from_secs(5)).await;
            return Err(Ok(()));
        }
    };
    let obligations_map: HashMap<String, Vec<String>> = match serde_json::from_reader(file) {
        Ok(obligations_map) => {
            obligations_map
        }
        Err(e) => {
            error!("[Liquidation Thread] Error loading obligations map: {}", e);
            return Err(Err(e.into()));
        }
    };
    if obligations_map.is_empty() {
        info!("[Liquidation Thread] No liquidatable obligations found");
        sleep(Duration::from_secs(5)).await;
        return Err(Ok(()));
    }
    Ok(obligations_map)
}

async fn loop_liquidate(klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {

    let mut obligation_map: HashMap<Pubkey, Obligation> = HashMap::new();
    let mut market_accounts_map: HashMap<Pubkey, (HashMap<Pubkey, Reserve>, LendingMarket, HashMap<Pubkey, ReferrerTokenState>)> = HashMap::new();

    loop {
        if let Err(e) = liquidate_in_loop(klend_client, scope.clone(), &mut obligation_map, &mut market_accounts_map).await {
            error!("[Liquidation Thread] Error: {}", e);
        }
    }
}

async fn stream_liquidate(klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
    let obligations_map = match load_obligations_map(scope).await {
        Ok(value) => value,
        Err(value) => return value,
    };

    let mut market_pubkeys = Vec::new();
    let mut all_scope_price_accounts: Vec<(Pubkey, bool, Account)> = Vec::new();
    let mut all_switchboard_accounts: Vec<(Pubkey, bool, Account)> = Vec::new();
    let mut all_reserves: HashMap<Pubkey, Reserve> = HashMap::new();
    let mut all_lending_market: HashMap<Pubkey, LendingMarket> = HashMap::new();
    let mut all_rts: HashMap<Pubkey, HashMap<Pubkey, ReferrerTokenState>> = HashMap::new();

    // Convert obligations_map to the expected type
    let mut market_obligations_map: HashMap<Pubkey, Vec<String>> = HashMap::new();

    for (market, liquidatable_obligations) in obligations_map.iter() {
        let market_pubkey = match Pubkey::from_str(market) {
            Ok(pubkey) => pubkey,
            Err(e) => {
                error!("[Liquidation Thread] Invalid market pubkey {}: {}", market, e);
                continue;
            }
        };

        let (market_accounts, rts) = match load_market_accounts_and_rts(klend_client, &market_pubkey).await {
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

        // For now, create empty obligation list - this should be populated with actual obligations if needed
        market_obligations_map.insert(market_pubkey, liquidatable_obligations.clone());

        market_pubkeys.push(market_pubkey);
        // Deduplicate scope price accounts by pubkey
        for account in scope_price_accounts {
            if !all_scope_price_accounts.iter().any(|(key, _, _)| *key == account.0) {
                all_scope_price_accounts.push(account);
            }
        }

        // Deduplicate switchboard accounts by pubkey
        for account in switchboard_accounts {
            if !all_switchboard_accounts.iter().any(|(key, _, _)| *key == account.0) {
                all_switchboard_accounts.push(account);
            }
        }

        // Insert individual reserves instead of nested structure
        all_reserves.extend(market_accounts.reserves);

        all_lending_market.insert(market_pubkey, market_accounts.lending_market);
        all_rts.insert(market_pubkey, rts);
    }

    //println!("all_switchboard_accounts: {:?}", all_switchboard_accounts);
    println!("all_reserves: {:?}", all_reserves.keys());

    let _ = account_update_ws(
        klend_client,
        &market_pubkeys,
        &market_obligations_map,
        &mut all_scope_price_accounts,
        &mut all_switchboard_accounts,
        &mut all_reserves,
        &mut all_lending_market,
        &mut all_rts
    ).await;

    Ok(())
}

async fn crank(klend_client: &Arc<KlendClient>, obligation_filter: Option<Pubkey>) -> Result<()> {
    let sleep_duration = Duration::from_secs(10);

    // 保存所有near liquidatable obligations的数组
    let _big_fish_near_liquidatable_obligations_map: Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>> = Arc::new(RwLock::new(HashMap::new()));
    let _near_liquidatable_obligations_map: Arc<RwLock<HashMap<Pubkey, Vec<Pubkey>>>> = Arc::new(RwLock::new(HashMap::new()));

    // 启动一个新的线程，扫描big_fish_near_liquidatable_obligations
    //let big_fish_near_liquidatable_obligations_map_clone = Arc::clone(&big_fish_near_liquidatable_obligations_map);
    //let near_liquidatable_obligations_map_clone = Arc::clone(&near_liquidatable_obligations_map);
    //let klend_client_clone = Arc::clone(klend_client);
    //let _big_fish_near_liquidatable_obligations_thread = tokio::spawn(async move {
        //let _ = run_liquidation_thread(&klend_client_clone, big_fish_near_liquidatable_obligations_map_clone).await;
    //});

    //let klend_client_clone_2 = Arc::clone(klend_client);
    //let _near_liquidatable_obligations_thread = tokio::spawn(async move {
        //let _ = run_liquidation_thread(&klend_client_clone_2, near_liquidatable_obligations_map_clone).await;
    //});

    //sleep(Duration::from_secs(60)).await;

    let (markets, ob) = match obligation_filter {
        None => {
            let lending_markets = get_lending_markets(&klend_client.program_id).await?;
            info!("Cranking all markets {lending_markets:?}..");
            (lending_markets, None)
        }
        Some(filter) => {
            let ob = klend_client.fetch_obligation(&filter).await?;
            let market = ob.lending_market;
            (vec![market], Some(ob))
        }
    };

    loop {
        let mut near_liquidatable_obligations_new_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut big_fish_near_liquidatable_obligations_new_map: HashMap<String, Vec<String>> = HashMap::new();

        for market in &markets {
            info!("{} cranking market", market.to_string().green());
            let st = std::time::Instant::now();

            let start = std::time::Instant::now();

            //let mut market_near_liquidatable_obligations: Vec<Pubkey> = vec![];
            let mut market_big_fish_near_liquidatable_obligations: Vec<String> = vec![];
            let mut market_near_liquidatable_obligations: Vec<String> = vec![];

            // Reload accounts
            let obligations = match ob {
                None => {
                    match klend_client.fetch_obligations(market).await {
                        Ok(obs) => {
                            info!(
                                "Fetched {} obligations in {}s",
                                obs.len(),
                                start.elapsed().as_secs()
                            );
                            obs
                        }
                        Err(e) => {
                            error!("Error fetching obligations for market {}: {}", market, e);
                            continue; // Skip this market and continue with the next one
                        }
                    }
                }
                Some(o) => {
                    if let Some(filter) = obligation_filter {
                        vec![(filter, o)]
                    } else {
                        // This should not happen given the logic above, but we handle it safely
                        error!("Unexpected state: obligation_filter is None when ob is Some");
                        return Err(anyhow::anyhow!("Invalid state in obligation processing"));
                    }
                },
            };
            let (market_accounts, rts) = match load_market_accounts_and_rts(klend_client, market).await {
                Ok(result) => result,
                Err(e) => {
                    error!("Error loading market accounts and rts for {}: {}", market, e);
                    continue; // Skip this market and continue with the next one
                }
            };

            let mut reserves = market_accounts.reserves;
            let mut lending_market = market_accounts.lending_market;
            let clock = sysvars::clock(&klend_client.local_client.client).await?;

            match refresh_market(klend_client, market, &vec![], &mut reserves, &mut lending_market, &clock, None, None, None).await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error refreshing market {}: {}", market, e);
                    continue; // Skip this market and continue with the next one
                }
            };

            // Refresh all obligations second
            let SplitObligations {
                zero_debt,
                mut risky,
            } = split_obligations(&obligations);
            let num_obligations = risky.len();

            info!("Total obligations: {}", risky.len() + zero_debt.len());
            info!("Zero debt obligations: {}", zero_debt.len());
            info!("Risky obligations: {}", risky.len());

            let mut healthy_obligations = 0;
            let mut unhealthy_obligations = 0;
            for (i, (address, obligation)) in risky.iter_mut().enumerate() {
                // Apply the filter
                if let Some(obligation_filter) = obligation_filter {
                    if *address != obligation_filter {
                        continue;
                    }
                }
                // info!("Processing obligation {:?}", address);

                // Refresh the obligation
                let ObligationReserves {
                    deposit_reserves,
                    borrow_reserves,
                } = match obligation_reserves(obligation, &reserves) {
                    Ok(reserves) => reserves,
                    Err(e) => {
                        error!("Error getting obligation reserves for {}: {}", address, e);
                        continue; // Skip this obligation and continue with the next one
                    }
                };

                let referrer_states = match referrer_token_states_of_obligation(
                    address,
                    obligation,
                    &borrow_reserves,
                    &rts,
                ) {
                    Ok(states) => states,
                    Err(e) => {
                        error!("Error getting referrer token states for {}: {}", address, e);
                        continue; // Skip this obligation and continue with the next one
                    }
                };

                if let Err(e) = kamino_lending::lending_market::lending_operations::refresh_obligation(
                    obligation,
                    &lending_market,
                    clock.slot,
                    deposit_reserves.into_iter(),
                    borrow_reserves.into_iter(),
                    referrer_states.into_iter(),
                ) {
                    error!("Error refreshing obligation {}: {}", address, e);
                    continue; // Skip this obligation and continue with the next one
                }

                // info!("Refreshed obligation: {}", address.to_string().green());
                let obligation_stats = math::obligation_info(address, obligation);
                let (is_liquidatable, near_liquidatable, is_big_fish) = math::print_obligation_stats(&obligation_stats, address, i, num_obligations);


                if is_liquidatable {
                    unhealthy_obligations += 1;
                    // TODO: liquidate
                    info!("Liquidating obligation begin: {} {}", address.to_string().green(), obligation.to_string().green());
                    match liquidate(klend_client, address, true).await {
                        Ok(_) => {
                            info!("Liquidated obligation success: {} {}", address.to_string().green(), obligation.to_string().green());
                        }
                        Err(e) => {
                            error!("Liquidating obligation error: {} {}", address.to_string().green(), e);
                        }
                    }
                } else {
                    if near_liquidatable {
                        if is_big_fish {
                            market_big_fish_near_liquidatable_obligations.push(address.to_string());
                        } else {
                            //market_big_fish_near_liquidatable_obligations.push(address.clone());
                            market_near_liquidatable_obligations.push(address.to_string());
                        }
                    }
                    healthy_obligations += 1;
                }
            }

            //near_liquidatable_obligations_new_map.insert(*market, market_near_liquidatable_obligations);
            if !market_big_fish_near_liquidatable_obligations.is_empty() {
                big_fish_near_liquidatable_obligations_new_map.insert(market.to_string(), market_big_fish_near_liquidatable_obligations);
            }

            if !market_near_liquidatable_obligations.is_empty() {
                near_liquidatable_obligations_new_map.insert(market.to_string(), market_near_liquidatable_obligations);
            }

            let en = st.elapsed().as_secs_f64();
            info!(
                "{} evaluated {} total obligations {} with debt, {} healthy, {} unhealthy. Sleeping for {:?}, duration {:?}", market.to_string().green(), risky.len() + zero_debt.len(), num_obligations, healthy_obligations, unhealthy_obligations, sleep_duration, en
            );
        }

        //near_liquidatable_obligations_map = near_liquidatable_obligations_new_map;

        {
            //let mut map_write = big_fish_near_liquidatable_obligations_map.write().unwrap();

            //map_write.clear();
            //map_write.extend(big_fish_near_liquidatable_obligations_new_map);
            //for (market, obligations) in big_fish_near_liquidatable_obligations_new_map.iter() {
                //map_write.insert(*market, obligations.to_vec());
            //}

            info!("writing big_fish_near_liquidatable_obligations_map to file");

            // write big_fish_near_liquidatable_obligations_new_map to file
            match File::create("big_fish_near_liquidatable_obligations.json") {
                Ok(file) => {
                    if let Err(e) = serde_json::to_writer_pretty(file, &big_fish_near_liquidatable_obligations_new_map) {
                        error!("Error writing big_fish_near_liquidatable_obligations.json: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error creating big_fish_near_liquidatable_obligations.json: {}", e);
                }
            }
        }

        {
            //let mut map_write = near_liquidatable_obligations_map.write().unwrap();
            //map_write.clear();
            //for (market, obligations) in near_liquidatable_obligations_new_map.iter() {
                //map_write.insert(*market, obligations.to_vec());
            //}

            info!("writing near_liquidatable_obligations_map to file");

            // write near_liquidatable_obligations_new_map to file
            match File::create("near_liquidatable_obligations.json") {
                Ok(file) => {
                    if let Err(e) = serde_json::to_writer_pretty(file, &near_liquidatable_obligations_new_map) {
                        error!("Error writing near_liquidatable_obligations.json: {}", e);
                    }
                }
                Err(e) => {
                    error!("Error creating near_liquidatable_obligations.json: {}", e);
                }
            }
        }

        sleep(sleep_duration).await;
    }
}

async fn load_market_accounts_and_rts(klend_client: &Arc<KlendClient>, market: &Pubkey) -> Result<(MarketAccounts, HashMap<Pubkey, ReferrerTokenState>)> {
    let start = std::time::Instant::now();
    let market_accs = klend_client.fetch_market_and_reserves(market).await?;
    let rts = klend_client.fetch_referrer_token_states().await?;
    let en_accounts = start.elapsed().as_secs_f64();
    info!("Loading market accounts and rts {} time used: {}s", market.to_string().green(), en_accounts);
    Ok((market_accs, rts))
}

async fn refresh_market(klend_client: &Arc<KlendClient>, market: &Pubkey,  obligation_reservers_to_refresh: &Vec<Pubkey>,
    reserves: &mut HashMap<Pubkey, Reserve>, lending_market: &mut LendingMarket, clock: &Clock,
    mut scope_price_accounts: Option<&mut Vec<(Pubkey, bool, Account)>>,
    mut switchboard_accounts: Option<&mut Vec<(Pubkey, bool, Account)>>,
    updated_account_data: Option<&HashMap<Pubkey, Vec<u8>>>)
-> Result<()> {
    let start = std::time::Instant::now();
    //let market_accs = klend_client.fetch_market_and_reserves(market).await?;

    //let en_accounts = start.elapsed().as_secs_f64();
    //info!("Refreshing market accounts {} time used: {}s", market.to_string().green(), en_accounts);


    //let rts = klend_client.fetch_referrer_token_states().await?;
    //let mut reserves = market_accs.reserves.clone();
    // let mut lending_market = market_accs.lending_market;
    if lending_market.global_unhealthy_borrow_value == 0 {
        lending_market.global_unhealthy_borrow_value = lending_market.global_allowed_borrow_value;
    }

    //let en_rts = start.elapsed().as_secs_f64();
    //info!("Refreshing market referrer token states {} time used: {}s", market.to_string().green(), en_rts);

                // First get oracle accounts
    let (mut pyth_accounts, mut switchboard_accounts_vec, mut scope_price_accounts_vec) =
        if let Some(updated_account_data) = updated_account_data {
            // Use the provided accounts and update them with new data
            if scope_price_accounts.is_none() || switchboard_accounts.is_none() {
                return Err(anyhow::anyhow!("oracle accounts parameters are required when updated_account_data is provided"));
            }


            // Update accounts with new data
            for (key, data) in updated_account_data.iter() {
                if let Some(scope_accounts) = scope_price_accounts.as_mut() {
                    if let Some(scope_price_account) = scope_accounts.iter_mut().find(|(k, _, _)| *k == *key) {
                        let old_data_len = scope_price_account.2.data.len();
                        scope_price_account.2.data = data.clone();
                        debug!("updated scope_price_account: {:?} (data length: {} -> {})",
                        scope_price_account.0.to_string(), old_data_len, data.len());
                    }
                }
                if let Some(sb_accounts) = switchboard_accounts.as_mut() {
                    if let Some(switchboard_account) = sb_accounts.iter_mut().find(|(k, _, _)| *k == *key) {
                        let old_data_len = switchboard_account.2.data.len();
                        switchboard_account.2.data = data.clone();
                        debug!("updated switchboard_account: {:?} (data length: {} -> {})",
                        switchboard_account.0.to_string(), old_data_len, data.len());
                    }
                }
            }

            let updated_scope_accounts = scope_price_accounts.map(|s| s.clone()).unwrap_or_default();
            let updated_switchboard_accounts = switchboard_accounts.map(|s| s.clone()).unwrap_or_default();


            (Vec::new(), updated_switchboard_accounts, updated_scope_accounts)
        } else {
            // Fetch fresh oracle accounts
            let OracleAccounts {
                pyth_accounts,
                switchboard_accounts,
                scope_price_accounts: scope_accounts,
            } = match oracle_accounts(&klend_client.local_client, &reserves).await {
                Ok(accounts) => accounts,
                Err(e) => {
                    error!("Error getting oracle accounts: {}", e);
                    return Err(e.into());
                }
            };

            (pyth_accounts, switchboard_accounts, scope_accounts)
        };


    let en_oracle_accounts = start.elapsed().as_secs_f64();
    debug!("Refreshing market oracle accounts {} time used: {}s", market.to_string().green(), en_oracle_accounts);

    let pyth_account_infos = map_accounts_and_create_infos(&mut pyth_accounts);
    let switchboard_feed_infos = map_accounts_and_create_infos(&mut switchboard_accounts_vec);
    let scope_price_infos = map_accounts_and_create_infos(&mut scope_price_accounts_vec);

    let refresh_set: HashSet<&Pubkey> = obligation_reservers_to_refresh.iter().collect();
    if !refresh_set.is_empty() {
        reserves.retain(|key, _| refresh_set.contains(key));
    }

    // 预先过滤出真正需要refresh的reserves的keys
    let keys_needing_refresh: Vec<_> = reserves
        .iter()
        .filter(|(_key, reserve)| {
            // 检查是否需要refresh
            let ignore_tokens = ["CHAI"];
            if ignore_tokens.contains(&reserve.config.token_info.symbol()) {
                return false;
            }

            kamino_lending::lending_market::lending_operations::is_price_refresh_needed(
                reserve,
                &lending_market,
                clock.unix_timestamp,
            )
        })
        .map(|(key, _)| *key)
        .collect();

    // 只对需要refresh的reserves处理
    for key in keys_needing_refresh {
        let reserve = match reserves.get_mut(&key) {
            Some(reserve) => reserve,
            None => {
                error!("Reserve {} not found in reserves map", key);
                continue;
            }
        };
        debug!(
            "Refreshing reserve {} token {} with status {}",
            key.to_string().green(),
            reserve.config.token_info.symbol().purple(),
            reserve.config.status
        );
        // if reserve.config.status != ReserveStatus::Active as u8 {
        //     continue;
        // }
        if let Err(e) = reserve.last_update.slots_elapsed(clock.slot) {
            warn!(err = ?e,
                "RESERVE {:?} last updated slot is already ahead of the clock, skipping refresh",
                key,
            );
        } else {
            match operations::refresh_reserve(
                &key,
                reserve,
                &lending_market,
                &clock,
                &pyth_account_infos,
                &switchboard_feed_infos,
                &scope_price_infos,
            ) {
                Ok(_) => {
                    debug!("Refreshed reserve {} token {} with status {}", key.to_string().green(), reserve.config.token_info.symbol().purple(), reserve.config.status);
                }
                Err(e) => {
                    error!("Error refreshing reserve {} token {} with status {}: {}", key.to_string().green(), reserve.config.token_info.symbol().purple(), reserve.config.status, e);
                }
            }

            /*for reserve in reserves.values() {
                info!("reserve: {:?} {:?} {:?} {:?}",
                reserve.config.token_info.symbol(),
                Fraction::from_bits(reserve.liquidity.market_price_sf),
                reserve.liquidity.market_price_last_updated_ts,
                reserve.last_update.get_price_status());
            }*/
        }
    }

    let en_refresh_reserves = start.elapsed().as_secs_f64();
    debug!("Refreshing market reserves {} time used: {}s", market.to_string().green(), en_refresh_reserves);

    Ok(())
}
