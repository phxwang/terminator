use std::{collections::HashMap, path::PathBuf, sync::{Arc, RwLock}, time::Duration, fs, str::FromStr, path::Path};

use anchor_client::{solana_sdk::pubkey::Pubkey, Cluster};
use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use consts::WRAPPED_SOL_MINT;
use juno::DecompiledVersionedTx;
use kamino_lending::{Reserve, LendingMarket, ReferrerTokenState};
use solana_sdk::{
    signer::Signer,
    clock::Clock,
    compute_budget,
};
use tokio::time::sleep;
use tracing::{info, warn, debug, error};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt, fmt, Layer};
use solana_sdk::instruction::Instruction;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;

use crate::{
    accounts::{map_accounts_and_create_infos, oracle_accounts, OracleAccounts},
    client::{KlendClient, RebalanceConfig},
    config::get_lending_markets,
    jupiter::get_best_swap_instructions,
    liquidator::{Holding, Holdings, Liquidator},
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
    Rebalance {
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
        } => liquidate(&klend_client, &obligation).await,
        Actions::Swap {
            from,
            to,
            amount,
            slippage_pct,
            rebalance_args: _,
        } => swap::swap_action(&klend_client, from, to, amount, slippage_pct).await,
        Actions::Rebalance { rebalance_args: _ } => rebalance(&klend_client).await,
        Actions::LoopLiquidate { scope, rebalance_args: _ } => loop_liquidate(&klend_client, scope).await,
    }
}

async fn rebalance(klend_client: &Arc<KlendClient>) -> Result<()> {
    let lending_markets = get_lending_markets(&klend_client.program_id).await?;

    info!("Rebalancing...");
    let rebalance_config = match &klend_client.rebalance_config {
        None => Err(anyhow::anyhow!("Rebalance settings not found")),
        Some(c) => Ok(c),
    }?;
    let RebalanceConfig {
        base_token,
        min_sol_balance,
        rebalance_slippage_pct: slippage,
        ..
    } = rebalance_config;
    info!(
        "Loading markets and reserves for {} markets..",
        lending_markets.len()
    );
    let markets =
        crate::client::utils::fetch_markets_and_reserves(klend_client, &lending_markets).await?;
    let (all_reserves, _ctoken_mints, liquidity_mints) = get_all_reserve_mints(&markets);
    info!("Loading Jupiter prices..");
    let amount = 100.0;
    let pxs = fetch_jup_prices(&liquidity_mints, &rebalance_config.usdc_mint, amount).await?;
    info!("Loading holdings..");
    let mut holdings = klend_client
        .liquidator
        .fetch_holdings(&klend_client.client.client, &all_reserves)
        .await?;

    let base = holdings.holding_of(base_token).unwrap();
    info!(
        "Base {:?} {} holding {}",
        base.mint, base.label, base.ui_balance
    );
    let sol_holding = &holdings.sol;
    info!(
        "SOL holding {}, Min sol holding {}",
        sol_holding.ui_balance, min_sol_balance
    );

    // Rules:
    // - if sol_balance < min_sol -> base token swaps into min_sol balance at least
    // - if sol_balance > min_sol * 2 -> swap the diff from current_sol - min_sol * 2 -> base token
    // - every non base token goes into base token if > $1 -> swap it partially at most $20k at a time

    const SOL_BUFFER_FACTOR: f64 = 2.0;
    let sol_balance = sol_holding.ui_balance;

    if sol_balance < *min_sol_balance {
        // Swap base token into SOL to reach min sol balance
        let target = min_sol_balance * SOL_BUFFER_FACTOR;
        let missing = target - sol_balance;

        let px_sol_to_base = pxs.a_to_b(&WRAPPED_SOL_MINT, base_token);
        let base_to_swap = missing * px_sol_to_base * (1.0 + slippage / 100.0);

        info!("Sol balance {} is below min_balance {} so we are topping up to {}, therefore acquiring {} more SOL, sol_price_to_base {}, swapping base {}",
            sol_balance,
            min_sol_balance,
            target,
            missing,
            px_sol_to_base,
            base_to_swap
        );

        // TODO: make these ixns go together
        swap::swap(
            klend_client,
            &holdings,
            base_token,
            &sol_holding.mint,
            base_to_swap,
            *slippage,
        )
        .await?;

        let _ = accounts::unwrap_wsol_ata(klend_client).await?;

        // Reload holdings
        tokio::time::sleep(Duration::from_secs(5)).await;
        holdings = klend_client
            .liquidator
            .fetch_holdings(&klend_client.client.client, &all_reserves)
            .await?;
    }

    // TODO: If we have too much wsol and it's not the base asset
    // then just unwrap it
    // accounts::unwrap_wsol_ata(klend_client).await;
    if rebalance_config.base_token != WRAPPED_SOL_MINT {
        let wsol_holding = holdings.holding_of(&WRAPPED_SOL_MINT).unwrap();
        if wsol_holding.usd_value > 1.0 {
            info!("Unwrapping {} WSOL", wsol_holding.ui_balance);
            let _ = accounts::unwrap_wsol_ata(klend_client).await?;

            // Reload holdings
            tokio::time::sleep(Duration::from_secs(5)).await;
            klend_client
                .liquidator
                .fetch_holdings(&klend_client.client.client, &all_reserves)
                .await?;
        }
    }

    // Now swap the remaining
    for Holding {
        mint,
        ui_balance,
        usd_value,
        label,
        ..
    } in holdings.holdings.clone().into_iter()
    {
        if &mint == base_token {
            continue;
        }

        if usd_value < rebalance_config.non_swappable_dust_usd_value {
            // We don't swap it, too small
            continue;
        }

        // Swap the whole thing
        let px = pxs.a_to_b(&mint, base_token);
        let estimated_base = ui_balance * px * (1.0 + slippage / 100.0);
        info!(
            "Swapping non-base token {} amount: {} expecting back {} base",
            label, ui_balance, estimated_base
        );

        swap::swap(
            klend_client,
            &holdings,
            &mint,
            base_token,
            ui_balance,
            *slippage,
        )
        .await?;
    }
    Ok(())
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

async fn liquidate(klend_client: &Arc<KlendClient>, obligation: &Pubkey) -> Result<()> {
    info!("Liquidating obligation {}", obligation.to_string().green());
    debug!("Liquidator ATAs: {:?}", klend_client.liquidator.atas);
    let rebalance_config = match &klend_client.rebalance_config {
        None => Err(anyhow::anyhow!("Rebalance settings not found")),
        Some(c) => Ok(c),
    }?;

    // Reload accounts
    let mut ob = klend_client.fetch_obligation(obligation).await?;
    let market_accs = klend_client
        .fetch_market_and_reserves(&ob.lending_market)
        .await?;

    let mut reserves = market_accs.reserves;
    let market = &market_accs.lending_market;
    // todo - don't load all
    let rts = klend_client.fetch_referrer_token_states().await?;

    let clock = sysvars::clock(&klend_client.client.client).await;

    // Pick debt and coll reserves to liquidate
    let debt_res_key = ob.borrows[0].borrow_reserve;
    let coll_res_key = ob.deposits[0].deposit_reserve;

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
        market,
        &clock,
    )
    .await?;

    // Now it's all fully refreshed and up to date
    let debt_reserve_state = *reserves.get(&debt_res_key).unwrap();
    let coll_reserve_state = *reserves.get(&coll_res_key).unwrap();
    let _debt_mint = debt_reserve_state.liquidity.mint_pubkey;
    let debt_reserve = StateWithKey::new(debt_reserve_state, debt_res_key);
    let coll_reserve = StateWithKey::new(coll_reserve_state, coll_res_key);
    let lending_market = StateWithKey::new(*market, ob.lending_market);
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
        .map(|coll| {
            StateWithKey::new(
                *reserves.get(&coll.deposit_reserve).unwrap(),
                coll.deposit_reserve,
            )
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
        let total_withdraw_liquidity_amount = res.unwrap().total_withdraw_liquidity_amount;
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
        let liquidate_ixns = klend_client
            .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market,
                debt_reserve.clone(),
                coll_reserve.clone(),
                obligation.clone(),
                liquidate_amount,
                min_acceptable_received_collateral_amount,
                max_allowed_ltv_override_pct_opt,
            )
            .await
            .unwrap();
        ixns.extend_from_slice(&liquidate_ixns);

        //add jupiter swap ixns
        //TODO: liquidate_amount is not correct, need to calculate the amount of collateral to swap
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
        let mut txn = klend_client.client.tx_builder().add_ixs(ixns.clone());
        for lut in luts {
            txn = txn.add_lookup_table(lut);
        }


        let txn_b64 = txn.to_base64();
        println!(
            "Simulation: https://explorer.solana.com/tx/inspector?message={}",
            urlencoding::encode(&txn_b64)
        );

        let txn = txn.build_with_budget_and_fee(&[]).await.unwrap();

        let res = klend_client
            .client
            .client
            .simulate_transaction(&txn)
            .await
            .unwrap();
        info!("Simulation result: {:?}", res);

        for ix in ixns {
            println!("Instruction: {:?} {:?}", ix.program_id, ix.data);
        }

        let should_send = false;

        if should_send {
            let sig = klend_client
                .client
                .send_retry_and_confirm_transaction(txn, None, false)
                .await
                .unwrap();

            info!("Liquidation tx sent: {:?}", sig.0);
            info!("Liquidation tx res: {:?}", sig.1);
        }
    }
    Ok(())
}

async fn check_and_liquidate(klend_client: &Arc<KlendClient>, address: &Pubkey, lending_market: &LendingMarket, clock: &Clock, reserves: &HashMap<Pubkey, Reserve>, rts: &HashMap<Pubkey, ReferrerTokenState>) -> Result<()> {
    let mut obligation = klend_client.fetch_obligation(address).await?;

    let ObligationReserves {
        deposit_reserves,
        borrow_reserves,
    } = obligation_reserves(&obligation, &reserves)?;

    let referrer_states = referrer_token_states_of_obligation(
        address,
        &obligation,
        &borrow_reserves,
        &rts,
    )?;

    kamino_lending::lending_market::lending_operations::refresh_obligation(
        &mut obligation,
        &lending_market,
        clock.slot,
        deposit_reserves.into_iter(),
        borrow_reserves.into_iter(),
        referrer_states.into_iter(),
    )?;

    let obligation_stats = math::obligation_info(address, &obligation);
    if obligation_stats.ltv > obligation_stats.unhealthy_ltv {
        info!("[Liquidation Thread] Liquidating obligation: {} {}", address.to_string().green(), obligation.to_string().green());
        //liquidate(klend_client, address).await?;
    }
    else {
        debug!("[Liquidation Thread] Obligation is not liquidatable: {} {}", address.to_string().green(), obligation.to_string().green());
    }

    Ok(())
}

async fn liquidate_in_loop(klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
    // load hashmap from scope.json file, need to check if the file exists
    let file_path = format!("{}.json", scope);
    if !Path::new(&file_path).exists() {
        info!("[Liquidation Thread] File {} does not exist", file_path);
        sleep(Duration::from_secs(5)).await;
        return Ok(());
    }
    let file = File::open(file_path).unwrap();
    let obligations_map: HashMap<String, Vec<String>> = serde_json::from_reader(file).unwrap();

    let start = std::time::Instant::now();

    if obligations_map.is_empty() {
        info!("[Liquidation Thread] No liquidatable obligations found");
        sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    let mut total_liquidatable_obligations = 0;

    for (market, liquidatable_obligations) in obligations_map.iter() {
        info!("[Liquidation Thread]{}: {} liquidatable obligations found", market.green(), liquidatable_obligations.len());
        total_liquidatable_obligations += liquidatable_obligations.len();
        let market_pubkey = Pubkey::from_str(market).unwrap();
        let (rts, reserves, lending_market, clock) = refresh_market(klend_client, &market_pubkey).await?;
        for address in liquidatable_obligations.iter() {
            check_and_liquidate(klend_client, &Pubkey::from_str(address).unwrap(), &lending_market, &clock, &reserves, &rts).await?;
        }
    }
    let en = start.elapsed().as_secs_f64();
    info!("[Liquidation Thread] Scanned {} obligations in {}s", total_liquidatable_obligations, en);

    Ok(())
}

async fn loop_liquidate(klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {

    loop {
        if let Err(e) = liquidate_in_loop(klend_client, scope.clone()).await {
            error!("[Liquidation Thread] Error: {}", e);
        }
    }
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
                    let obs = klend_client.fetch_obligations(market).await?;
                    info!(
                        "Fetched {} obligations in {}s",
                        obs.len(),
                        start.elapsed().as_secs()
                    );
                    obs
                }
                Some(o) => vec![(obligation_filter.unwrap(), o)],
            };
            let (rts, reserves, lending_market, clock) = refresh_market(klend_client, market).await?;

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
                } = obligation_reserves(obligation, &reserves)?;
                let referrer_states = referrer_token_states_of_obligation(
                    address,
                    obligation,
                    &borrow_reserves,
                    &rts,
                )?;
                kamino_lending::lending_market::lending_operations::refresh_obligation(
                    obligation,
                    &lending_market,
                    clock.slot,
                    deposit_reserves.into_iter(),
                    borrow_reserves.into_iter(),
                    referrer_states.into_iter(),
                )?;

                // info!("Refreshed obligation: {}", address.to_string().green());
                let obligation_stats = math::obligation_info(address, obligation);
                let (is_liquidatable, near_liquidatable, is_big_fish) = math::print_obligation_stats(&obligation_stats, address, i, num_obligations);


                if is_liquidatable {
                    unhealthy_obligations += 1;
                    // TODO: liquidate
                    info!("Liquidating obligation: {} {}", address.to_string().green(), obligation.to_string().green());
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
            let file = File::create("big_fish_near_liquidatable_obligations.json").unwrap();
            serde_json::to_writer_pretty(file, &big_fish_near_liquidatable_obligations_new_map).unwrap();
        }

        {
            //let mut map_write = near_liquidatable_obligations_map.write().unwrap();
            //map_write.clear();
            //for (market, obligations) in near_liquidatable_obligations_new_map.iter() {
                //map_write.insert(*market, obligations.to_vec());
            //}

            info!("writing near_liquidatable_obligations_map to file");

            // write near_liquidatable_obligations_new_map to file
            let file = File::create("near_liquidatable_obligations.json").unwrap();
            serde_json::to_writer_pretty(file, &near_liquidatable_obligations_new_map).unwrap();
        }

        sleep(sleep_duration).await;
    }
}

async fn refresh_market(klend_client: &Arc<KlendClient>, market: &Pubkey)
-> Result<(HashMap<Pubkey, ReferrerTokenState>,
    HashMap<Pubkey, Reserve>,
    LendingMarket,
    Clock)> {
    let market_accs = klend_client.fetch_market_and_reserves(market).await?;
    let rts = klend_client.fetch_referrer_token_states().await?;
    let mut reserves = market_accs.reserves.clone();
    let mut lending_market = market_accs.lending_market;
    if lending_market.global_unhealthy_borrow_value == 0 {
        lending_market.global_unhealthy_borrow_value = lending_market.global_allowed_borrow_value;
    }
    let OracleAccounts {
        mut pyth_accounts,
        mut switchboard_accounts,
        mut scope_price_accounts,
    } = oracle_accounts(&klend_client.client, &reserves)
        .await
        .unwrap();
    let clock = sysvars::get_clock(&klend_client.client.client)
        .await
        .unwrap();
    let pyth_account_infos = map_accounts_and_create_infos(&mut pyth_accounts);
    let switchboard_feed_infos = map_accounts_and_create_infos(&mut switchboard_accounts);
    let scope_price_infos = map_accounts_and_create_infos(&mut scope_price_accounts);

    for (key, reserve) in reserves.iter_mut() {
        info!(
            "Refreshing reserve {} token {} with status {}",
            key.to_string().green(),
            reserve.config.token_info.symbol().purple(),
            reserve.config.status
        );
        // if reserve.config.status != ReserveStatus::Active as u8 {
        //     continue;
        // }
        let ignore_tokens = ["CHAI"];
        if ignore_tokens.contains(&reserve.config.token_info.symbol()) {
            continue;
        }
        if let Err(e) = reserve.last_update.slots_elapsed(clock.slot) {
            warn!(err = ?e,
                "RESERVE {:?} last updated slot is already ahead of the clock, skipping refresh",
                key,
            );
        } else {
            operations::refresh_reserve(
                key,
                reserve,
                &lending_market,
                &clock,
                &pyth_account_infos,
                &switchboard_feed_infos,
                &scope_price_infos,
            )?;
        }
    }
    Ok((rts, reserves, lending_market, clock))
}
