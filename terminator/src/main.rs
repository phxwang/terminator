use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::Arc, time::Duration, fs};

use anchor_client::{solana_sdk::pubkey::Pubkey, Cluster};
use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use juno::DecompiledVersionedTx;
use kamino_lending::{Reserve, LendingMarket};
use solana_sdk::{
    signer::Signer,
    clock::Clock,
    account::Account,
};
use tokio::time::sleep;
use tracing::{info, warn, debug, error};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt, fmt, Layer};
use solana_sdk::instruction::Instruction;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;



use crate::{
    accounts::{create_new_lookup_tables_account, oracle_accounts, OracleAccounts, map_accounts_and_create_infos},
    client::KlendClient,
    config::get_lending_markets,
    jupiter::get_best_swap_instructions,
    liquidator::{Holdings, Liquidator},
    liquidation_engine::LiquidationEngine,
    operations::{
        split_obligations,
        SplitObligations,
    },
    px::fetch_jup_prices,
    utils::get_all_reserve_mints,
};

pub mod accounts;
pub mod client;
mod config;
pub mod consts;
pub mod instructions;
pub mod jupiter;
pub mod liquidator;
pub mod liquidation_engine;
pub mod lookup_tables;
pub mod macros;
pub mod math;
mod model;
pub mod operations;
mod px;
pub mod sysvars;
mod utils;
pub mod yellowstone_transaction;

pub mod instruction_parser;

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

    /// Connect to custom extra service
    #[clap(long, env, parse(try_from_str), default_value = "localnet")]
    extra: String,

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

        /// Slot to simulate
        #[clap(long, env, parse(try_from_str))]
        slot: Option<u64>,

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
    #[clap()]
    CreateLookupTable {
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
    let mut klend_client = config::get_client_for_action(&args).await?;

    klend_client.load_default_lookup_table().await;

    if klend_client.liquidator.atas.is_empty() {
        info!("Liquidator ATAs are empty, loading...");
        klend_client.liquidator = Liquidator::init(&klend_client).await?;
    }

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
            slot,
            rebalance_args: _,
        } => {
            let mut liquidation_engine = LiquidationEngine::new();
            liquidation_engine.liquidate(&klend_client, &obligation, slot).await
        },
        Actions::Swap {
            from,
            to,
            amount,
            slippage_pct,
            rebalance_args: _,
        } => swap::swap_action(&klend_client, from, to, amount, slippage_pct).await,
        Actions::LoopLiquidate { scope, rebalance_args: _ } => {
            let mut liquidation_engine = LiquidationEngine::new();
            liquidation_engine.loop_liquidate(&klend_client, scope).await
        },
        Actions::StreamLiquidate { scope, rebalance_args: _ } => {
            let mut liquidation_engine = LiquidationEngine::new();
            liquidation_engine.stream_liquidate(&klend_client, scope).await
        },
        Actions::CreateLookupTable { rebalance_args: _ } => create_lookup_table(&klend_client).await,
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
            &klend_client.local_client.client,
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










async fn crank(klend_client: &Arc<KlendClient>, obligation_filter: Option<Pubkey>) -> Result<()> {
    let sleep_duration = Duration::from_secs(10);

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
        for market in &markets {
            info!("{} cranking market", market.to_string().green());
            let st = std::time::Instant::now();

            let start = std::time::Instant::now();

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

                let obligation_stats = crate::math::obligation_info(address, obligation);
                let (is_liquidatable, _small_near_liquidatable, _medium_near_liquidatable, _big_near_liquidatable) = crate::math::print_obligation_stats(&obligation_stats, address, i, num_obligations);

                if is_liquidatable {
                    unhealthy_obligations += 1;
                    // TODO: liquidate using LiquidationEngine
                    info!("Found liquidatable obligation: {} {:?}", address.to_string().green(), obligation.to_string());
                } else {
                    healthy_obligations += 1;
                }
            }

            let en = st.elapsed().as_secs_f64();
            info!(
                "{} evaluated {} total obligations {} with debt, {} healthy, {} unhealthy. Sleeping for {:?}, duration {:?}",
                market.to_string().green(),
                risky.len() + zero_debt.len(),
                num_obligations,
                healthy_obligations,
                unhealthy_obligations,
                sleep_duration,
                en
            );
        }

        sleep(sleep_duration).await;
    }
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
    // Note: global_unhealthy_borrow_value field was removed/renamed in kamino_lending update
    // if lending_market.global_allowed_borrow_value == 0 {
    //     lending_market.global_allowed_borrow_value = lending_market.global_allowed_borrow_value;
    // }

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

async fn create_lookup_table(klend_client: &Arc<KlendClient>) -> Result<()> {
    info!("Creating new lookup table account...");
    create_new_lookup_tables_account(klend_client).await?;
    info!("Successfully created new lookup table account");
    Ok(())
}
