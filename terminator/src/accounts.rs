use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anchor_client::{
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    },
    solana_sdk::{account::Account, account_info::AccountInfo, pubkey::Pubkey, signer::Signer},
};
use anchor_lang::Id;
use anchor_spl::token::Token;
use anyhow::Result;
use futures::SinkExt;
use futures_util::stream::StreamExt;
use kamino_lending::{LendingMarket, Reserve, Obligation, ReferrerTokenState};
//use scope::OraclePrices as ScopePrices;
use orbit_link::{async_client::AsyncClient, OrbitLink};
use spl_associated_token_account::instruction::create_associated_token_account;
use tracing::{info};
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterAccounts, subscribe_update::UpdateOneof};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use crate::{
    client::{rpc, KlendClient},
    consts::WRAPPED_SOL_MINT,
    yellowstone_transaction::create_yellowstone_client,
    refresh_market,
    scan_obligations,
    sysvars,
};

// Local types for scope price functionality
//type ScopePriceId = u16;
//type ScopeConversionChain = [ScopePriceId; 4];

/*#[derive(Debug, Clone)]
pub struct TimestampedPrice {
    pub price_value: u64,
    pub price_exp: u32,
    pub timestamp: u64,
}*/

// Local implementation of get_price_usd function
/*fn get_price_usd(
    scope_prices: &ScopePrices,
    tokens_chain: ScopeConversionChain,
) -> Option<TimestampedPrice> {
    if tokens_chain == [0, 0, 0, 0] {
        return None;
    }

    // Get the first price in the chain (simplified implementation)
    if let Some(price_info) = scope_prices.prices.get(tokens_chain[0] as usize) {
        Some(TimestampedPrice {
            price_value: price_info.price.value,
            price_exp: price_info.price.exp as u32,
            timestamp: price_info.unix_timestamp,
        })
    } else {
        None
    }
}*/

pub fn create_is_signer_account_infos<'a>(
    accounts: &'a mut [(Pubkey, bool, &'a mut Account)],
) -> HashMap<Pubkey, AccountInfo<'a>> {
    accounts
        .iter_mut()
        .map(|(key, is_signer, account)| {
            (
                *key,
                AccountInfo::new(
                    key,
                    *is_signer,
                    false,
                    &mut account.lamports,
                    &mut account.data,
                    &account.owner,
                    account.executable,
                    account.rent_epoch,
                ),
            )
        })
        .collect()
}

pub struct MarketAccounts {
    pub reserves: HashMap<Pubkey, Reserve>,
    pub lending_market: LendingMarket,
}

pub struct OracleAccounts {
    pub pyth_accounts: Vec<(Pubkey, bool, Account)>,
    pub switchboard_accounts: Vec<(Pubkey, bool, Account)>,
    pub scope_price_accounts: Vec<(Pubkey, bool, Account)>,
}

pub async fn market_and_reserve_accounts(
    client: &KlendClient,
    lending_market: &Pubkey,
) -> Result<MarketAccounts> {
    let market = client
        .client
        .get_anchor_account::<LendingMarket>(lending_market)
        .await?;

    let filter = RpcFilterType::Memcmp(Memcmp::new(
        32,
        MemcmpEncodedBytes::Bytes(lending_market.to_bytes().to_vec()),
    ));
    let filters = vec![filter];

    let res: Vec<(Pubkey, Reserve)> =
        rpc::get_zero_copy_pa(&client.client, &client.program_id, &filters).await?;

    let reserves: HashMap<Pubkey, Reserve> = res.into_iter().collect();

    Ok(MarketAccounts {
        reserves,
        lending_market: market,
    })
}

pub async fn oracle_accounts<T: AsyncClient, S: Signer>(
    client: &OrbitLink<T, S>,
    reserves: &HashMap<Pubkey, Reserve>,
) -> Result<OracleAccounts> {
    let mut all_oracle_keys = HashSet::new();
    let mut pyth_keys = HashSet::new();
    let mut switchboard_keys = HashSet::new();
    let mut scope_keys = HashSet::new();

    refresh_oracle_keys(reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

    let all_keys: Vec<Pubkey> = all_oracle_keys.into_iter().collect();
    let all_accounts = client.client.get_multiple_accounts(&all_keys).await?;

    let mut pyth_accounts = Vec::new();
    let mut switchboard_accounts = Vec::new();
    let mut scope_price_accounts = Vec::new();

    for (i, key) in all_keys.iter().enumerate() {
        if let Some(account) = &all_accounts[i] {
            let account_tuple = (*key, false, account.clone());

            if pyth_keys.contains(key) {
                pyth_accounts.push(account_tuple.clone());
            }
            if switchboard_keys.contains(key) {
                switchboard_accounts.push(account_tuple.clone());
            }
            if scope_keys.contains(key) {
                scope_price_accounts.push(account_tuple.clone());
            }
        }
    }

    Ok(OracleAccounts {
        pyth_accounts,
        switchboard_accounts,
        scope_price_accounts,
    })
}

pub fn refresh_oracle_keys(reserves: &HashMap<Pubkey, Reserve>, all_oracle_keys: &mut HashSet<Pubkey>, pyth_keys: &mut HashSet<Pubkey>, switchboard_keys: &mut HashSet<Pubkey>, scope_keys: &mut HashSet<Pubkey>) {
    for (_, reserve) in reserves.iter() {
        let pyth_key = reserve.config.token_info.pyth_configuration.price;
        let sb_price_key = reserve.config.token_info.switchboard_configuration.price_aggregator;
        let sb_twap_key = reserve.config.token_info.switchboard_configuration.twap_aggregator;
        let scope_key = reserve.config.token_info.scope_configuration.price_feed;

        //info!("reserve: {:?}, pyth_key: {:?}, sb_price_key: {:?}, sb_twap_key: {:?}, scope_key: {:?}", reserve.config.token_info.name, pyth_key, sb_price_key, sb_twap_key, scope_key);

        pyth_keys.insert(pyth_key);
        switchboard_keys.insert(sb_price_key);
        switchboard_keys.insert(sb_twap_key);
        scope_keys.insert(scope_key);

        all_oracle_keys.insert(pyth_key);
        all_oracle_keys.insert(sb_price_key);
        all_oracle_keys.insert(sb_twap_key);
        all_oracle_keys.insert(scope_key);
    }
}

#[macro_export]
macro_rules! map_and_collect_accounts {
    ($accounts:expr) => {{
        $accounts
            .iter_mut()
            .map(|(pk, writable, acc)| (*pk, *writable, acc))
            .collect::<Vec<_>>()
    }};
}

pub fn map_accounts_and_create_infos(
    accounts: &mut [(Pubkey, bool, Account)],
) -> HashMap<Pubkey, AccountInfo> {
    accounts
        .iter_mut()
        .map(|(key, is_signer, account)| {
            (
                *key,
                AccountInfo::new(
                    key,
                    *is_signer,
                    false,
                    &mut account.lamports,
                    &mut account.data,
                    &account.owner,
                    account.executable,
                    account.rent_epoch,
                ),
            )
        })
        .collect()
}

pub async fn unwrap_wsol_ata(klend_client: &KlendClient) -> Result<String> {
    info!("Unwrapping sol..");
    let user = klend_client.liquidator.wallet.pubkey();

    // Close the account
    let instructions = vec![spl_token::instruction::close_account(
        &Token::id(),
        klend_client.liquidator.atas.get(&WRAPPED_SOL_MINT).unwrap(),
        &user,
        &user,
        &[],
    )?];

    // // Sync remaining sol (no need to do this upon close, on open only)
    // info!("Sync native for wsol ata {}", wsol_ata);
    // let wsol_ata = klend_client.liquidator.atas.get(&WRAPPED_SOL_MINT).unwrap();
    // instructions.push(instruction::sync_native(&Token::id(), &wsol_ata).unwrap());

    // Then create it again so we have wsol ata existing
    let recreate_ix =
        create_associated_token_account(&user, &user, &WRAPPED_SOL_MINT, &Token::id());

    let tx = klend_client
        .client
        .tx_builder()
        .add_ixs(instructions)
        .add_ix(recreate_ix)
        .build(&[])
        .await?;

    let (sig, _) = klend_client
        .client
        .send_retry_and_confirm_transaction(tx, None, false)
        .await?;

    info!("Executed unwrap transaction: {:?}", sig);
    Ok(sig.to_string())
}

pub async fn find_account(
    client: &RpcClient,
    address: Pubkey,
) -> Result<Option<(Pubkey, Account)>> {
    let res = client.get_account(&address).await;
    if let Ok(account) = res {
        Ok(Some((address, account)))
    } else {
        println!("Ata not found: {}", address);
        Ok(None)
    }
}

pub async fn account_update_ws(
    klend_client: &Arc<KlendClient>,
    market_pubkeys: &Vec<Pubkey>,
    market_obligations_map: &HashMap<Pubkey, Vec<String>>,
    all_scope_price_accounts: &mut Vec<(Pubkey, bool, Account)>,
    all_switchboard_accounts: &mut Vec<(Pubkey, bool, Account)>,
    all_reserves: &mut HashMap<Pubkey, Reserve>,
    all_lending_market: &mut HashMap<Pubkey, LendingMarket>,
    all_rts: &mut HashMap<Pubkey, HashMap<Pubkey, ReferrerTokenState>>,
) -> anyhow::Result<()> {

    // Collect all scope price account keys
    let scope_price_pubkeys = all_scope_price_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
    let switchboard_pubkeys = all_switchboard_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
    let pubkeys: Vec<Pubkey> = HashSet::<Pubkey>::from_iter([scope_price_pubkeys, switchboard_pubkeys].concat()).into_iter().collect();
    info!("account update ws: {:?}", pubkeys);

    let mut accounts = HashMap::new();
    let mut obligation_map: HashMap<Pubkey, Obligation> = HashMap::new();
    let mut obligation_reservers_to_refresh: Vec<Pubkey> = vec![];
    let account_filter = pubkeys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
    accounts.insert(
        "client".to_string(),
        SubscribeRequestFilterAccounts {
            account: account_filter,
            owner: vec![],
            filters: vec![],
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
            transactions: HashMap::new(),
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

    while let Some(message) = stream.next().await {
        if let Ok(msg) = message {
            if let Some(UpdateOneof::Account(account)) = msg.update_oneof {
                let account = account.account;

                if let Some(account) = account {

                    let start = std::time::Instant::now();
                    let data = account.data;
                    let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();
                    //let scope_prices = bytemuck::from_bytes::<ScopePrices>(&data[8..]);
                    //info!("Account: {:?}, scope_prices updated: {:?}", pubkey, scope_prices.prices.len());

                    //update reserves
                    let clock = sysvars::clock(&klend_client.client.client).await;

                    //for price in scope_prices.prices {
                    //    let price_age_in_seconds = clock.unix_timestamp.saturating_sub(price.unix_timestamp as i64);
                    //    info!("Price age: {:?} second, price: value={}, exp={}", price_age_in_seconds, price.price.value, price.price.exp);
                    //}

                    //for reserve in all_reserves.values() {
                    //   debug!("reserve: {:?}", reserve.config.token_info.scope_configuration.price_feed);
                    //    if reserve.config.token_info.scope_configuration.price_feed == pubkey {
                    //        if let Some(price) = get_price_usd(&scope_prices, reserve.config.token_info.scope_configuration.price_chain) {
                    //            let price_age_in_seconds = clock.unix_timestamp.saturating_sub(price.timestamp as i64);
                    //            info!("WebSocket update - reserve: {} price: {:?} age: {:?} seconds", reserve.config.token_info.symbol(), price, price_age_in_seconds);
                    //        }
                    //    }
                    //}

                    for market_pubkey in market_pubkeys {
                        // Now call refresh_market without additional updated_account_data since we've already updated the arrays
                        let _ = refresh_market(klend_client,
                            &market_pubkey,
                            &Vec::new(),
                            all_reserves,
                            all_lending_market.get_mut(market_pubkey).unwrap(),
                            &clock,
                            Some(all_scope_price_accounts),
                            Some(all_switchboard_accounts),
                            Some(&HashMap::from([(pubkey, data.clone())]))
                            ).await;

                        //scan obligations
                        let obligations = market_obligations_map.get(market_pubkey).unwrap();
                        let _ = scan_obligations(klend_client,
                            &mut obligation_map,
                            &mut obligation_reservers_to_refresh,
                            &clock,
                            &obligations,
                            all_reserves,
                            all_lending_market.get(market_pubkey).unwrap(),
                            all_rts.get(market_pubkey).unwrap()).await;

                        let duration = start.elapsed();
                        info!("Scan {} obligations, time used: {:?} s", obligations.len(), duration);
                    }

                }
            }
        } else {
            info!("Account Update error: {:?}", message);
            break;
        }
    }

    Ok(())
}
