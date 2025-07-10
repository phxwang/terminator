use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::str::FromStr;
use std::path::Path;

use anchor_client::{
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    },
    solana_sdk::{account::Account, account_info::AccountInfo, pubkey::Pubkey, signer::Signer},
};
use anchor_lang::{Id, AccountDeserialize, Discriminator};
use solana_sdk::{clock::Clock, sysvar::SysvarId};
use anchor_spl::token::Token;
use anyhow::Result;
use futures::SinkExt;
use futures_util::stream::StreamExt;
use kamino_lending::{LendingMarket, Reserve, Obligation, ReferrerTokenState};
use scope::OraclePrices as ScopePrices;
use orbit_link::{async_client::AsyncClient, OrbitLink};
use spl_associated_token_account::instruction::create_associated_token_account;
use tracing::{info, debug, error};
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof};
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::tonic;
use tokio::time::sleep;
use extra_proto::GetMultipleAccountsRequest;

use crate::{
    client::{rpc, KlendClient},
    consts::WRAPPED_SOL_MINT,
    yellowstone_transaction::create_yellowstone_client,
    refresh_market,
    scan_obligations,
    sysvars,
};

// Local types for scope price functionality
type ScopePriceId = u16;
type ScopeConversionChain = [ScopePriceId; 4];

#[derive(Debug, Clone)]
pub struct TimestampedPrice {
    pub price_value: u64,
    pub price_exp: u32,
    pub timestamp: u64,
}

// Local implementation of get_price_usd function
fn get_price_usd(
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
}

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

pub async fn oracle_accounts_from_extra(
    extra_client: &mut extra_proto::extra_client::ExtraClient<tonic::transport::Channel>,
    reserves: &HashMap<Pubkey, Reserve>,
    slot: u64,
) -> Result<OracleAccounts> {
    let mut all_oracle_keys = HashSet::new();
    let mut pyth_keys = HashSet::new();
    let mut switchboard_keys = HashSet::new();
    let mut scope_keys = HashSet::new();

    refresh_oracle_keys(reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

    let all_keys: Vec<Pubkey> = all_oracle_keys.into_iter().collect();

    let response = extra_client.get_multiple_accounts(extra_proto::GetMultipleAccountsRequest {
        addresses: all_keys.iter()
            .map(|address| address.to_bytes().to_vec())
            .collect(),
        commitment_or_slot: slot,
    }).await?;

    let accounts = response.into_inner();

    let mut pyth_accounts = Vec::new();
    let mut switchboard_accounts = Vec::new();
    let mut scope_price_accounts = Vec::new();

    for (i, key) in all_keys.iter().enumerate() {
        if let Some(account_data) = accounts.datas.get(i) {
            if !account_data.is_empty() {
                let account = Account {
                    lamports: accounts.balances.get(i).copied().unwrap_or(0),
                    data: account_data.clone(),
                    owner: Pubkey::default(), // We don't have owner info from the response
                    executable: false, // We don't have executable info from the response
                    rent_epoch: 0, // We don't have rent_epoch info from the response
                };
                let account_tuple = (*key, false, account);

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
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Ok(None)
    }
}

pub async fn find_accounts(
    client: &RpcClient,
    addresses: &Vec<Pubkey>,
) -> Result<HashMap<Pubkey, Account>> {
    let chunks = addresses.chunks(100);
    let mut accounts = HashMap::new();
    for chunk in chunks {
        let res = client.get_multiple_accounts(chunk).await?;
        for (i, account_opt) in res.iter().enumerate() {
            if let Some(account) = account_opt {
                accounts.insert(chunk[i], account.clone());
            } else {
                println!("Account not found: {}", chunk[i]);
            }
        }
    }
    Ok(accounts)
}

pub async fn dump_accounts_to_file(
    extra_client: &mut extra_proto::extra_client::ExtraClient<tonic::transport::Channel>,
    dump_account_pubkeys: &Vec<Pubkey>,
    slot: u64,
    reserve_pubkeys: &Vec<Pubkey>,
) -> Result<()> {
    let request = GetMultipleAccountsRequest {
        addresses: dump_account_pubkeys.iter()
            .map(|address| address.to_bytes().to_vec())
            .collect(),
        commitment_or_slot: slot,
    };
    let response = extra_client.get_multiple_accounts(request).await?;
    let accounts = response.into_inner();

    let file_path = format!("./dump_data/{}.json", slot);
    let file = File::create(file_path.clone())?;
    let mut writer = BufWriter::new(file);


    let mut json_map = serde_json::Map::new();
    for (i, account_data) in accounts.datas.iter().enumerate() {
        let account_pubkey = dump_account_pubkeys[i];
        let account_data_array: Vec<u8> = account_data.iter().copied().collect();
        json_map.insert(account_pubkey.to_string(), serde_json::Value::Array(
            account_data_array.iter().map(|b| serde_json::Value::Number(serde_json::Number::from(*b))).collect()
        ));
    }

    let mut json_array = vec![];
    json_array.push(serde_json::Value::Object(json_map));
    json_array.push(serde_json::Value::Array(reserve_pubkeys.iter().map(|key| serde_json::Value::String(key.to_string())).collect::<Vec<serde_json::Value>>()));

    let json_array = serde_json::Value::Array(json_array);
    serde_json::to_writer_pretty(&mut writer, &json_array)?;

    info!("Dump accounts to file: {}", file_path);

    Ok(())
}

pub async fn load_accounts_from_file(
    slot: u64
) -> Result<(HashMap<Pubkey, Account>, Vec<Pubkey>)> {
    let file_path = format!("./dump_data/{}.json", slot);
    let file = File::open(file_path.clone())?;
    let reader = BufReader::new(file);
    let mut accounts = HashMap::new();
    let json_array: serde_json::Value = serde_json::from_reader(reader)?;
    let json_map = json_array.as_array().unwrap()[0].as_object().unwrap();
    let reserve_pubkeys = json_array.as_array().unwrap()[1].as_array().unwrap().iter().map(|b| Pubkey::from_str(b.as_str().unwrap()).unwrap()).collect::<Vec<Pubkey>>();

    for (pubkey, data) in json_map.iter() {
        let pubkey = Pubkey::from_str(pubkey)?;
        let data_array = data.as_array().unwrap().iter().map(|b| b.as_u64().unwrap()).collect::<Vec<u64>>();
        accounts.insert(pubkey, Account {
            lamports: 0,
            data: data_array.iter().map(|b| *b as u8).collect(),
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: 0,
        });
    }
    info!("Load accounts from file: {}, count: {}", file_path, accounts.len());
    Ok((accounts, reserve_pubkeys))
}

pub fn load_competitors_from_file() -> Result<Vec<Pubkey>> {
    let file = File::open(".competitors")?;
    let reader = BufReader::new(file);
    let mut competitors = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let pubkey = Pubkey::from_str(&line)?;
        competitors.push(pubkey);
    }
    Ok(competitors)
}

pub async fn load_obligations_map(scope: String) -> Result<HashMap<Pubkey, Vec<Pubkey>>, std::result::Result<(), anyhow::Error>> {
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
    //covert string to pubkey
    let obligations_map = obligations_map.iter().map(|(k, v)| (Pubkey::from_str(k).unwrap(), v.iter().map(|s| Pubkey::from_str(s).unwrap()).collect::<Vec<Pubkey>>())).collect::<HashMap<Pubkey, Vec<Pubkey>>>();
    Ok(obligations_map)
}

pub async fn account_update_ws(
    klend_client: &Arc<KlendClient>,
    scope: String,
    market_pubkeys: &Vec<Pubkey>,
    market_obligations_map: &mut HashMap<Pubkey, Vec<Pubkey>>,
    all_scope_price_accounts: &mut Vec<(Pubkey, bool, Account)>,
    all_switchboard_accounts: &mut Vec<(Pubkey, bool, Account)>,
    all_reserves: &mut HashMap<Pubkey, Reserve>,
    all_lending_market: &mut HashMap<Pubkey, LendingMarket>,
    all_rts: &mut HashMap<Pubkey, HashMap<Pubkey, ReferrerTokenState>>,
) -> anyhow::Result<()> {

    // Collect all scope price account keys
    let scope_price_pubkeys = all_scope_price_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
    let switchboard_pubkeys = all_switchboard_accounts.iter().map(|(key, _, _)| *key).collect::<Vec<Pubkey>>();
    let mut all_obligations_pubkeys = market_obligations_map.values().flatten().copied().collect::<Vec<Pubkey>>();
    let mut pubkeys: Vec<Pubkey> = HashSet::<Pubkey>::from_iter([scope_price_pubkeys.clone(), switchboard_pubkeys, all_obligations_pubkeys.clone()].concat()).into_iter().collect();
    pubkeys.push(Clock::id());
    info!("account update ws: {:?}", pubkeys);

    let competitors = load_competitors_from_file()?;
    info!("competitors: {:?}", competitors);


    let obligation_map: Arc<RwLock<HashMap<Pubkey, Obligation>>> = Arc::new(RwLock::new(HashMap::new()));
    let mut obligation_reservers_to_refresh: Vec<Pubkey> = vec![];

    let mut accounts = HashMap::new();
    let account_filter = pubkeys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
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

    let mut reserves_prices: HashMap<Pubkey, TimestampedPrice> = HashMap::new();

    let mut clock = match sysvars::clock(&klend_client.local_client.client).await {
        Ok(clock) => clock,
        Err(_e) => {
            error!("Failed to get clock");
            return Err(anyhow::Error::msg("Failed to get clock"));
        }
    };

    while let Some(message) = stream.next().await {
        if let Ok(msg) = message {
            match msg.update_oneof {
                Some(UpdateOneof::Account(account)) => {

                    let account = account.account;

                    if let Some(account) = account {
                        let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();
                        let data = account.data;
                        if pubkey == Clock::id() {
                            //update clock from account
                            let account_data = Account {
                                lamports: account.lamports,
                                data: data.clone(),
                                owner: account.owner.clone().try_into().unwrap(),
                                executable: account.executable,
                                rent_epoch: account.rent_epoch,
                            };
                            if let Ok(updated_clock) = account_data.deserialize_data::<Clock>() {
                                clock = updated_clock;
                                debug!("Clock updated: {:?}", clock);
                            }
                            continue;
                        }

                        if all_obligations_pubkeys.contains(&pubkey) {
                            let mut data_slice: &[u8] = &data;
                            let obligation = match Obligation::try_deserialize(&mut data_slice) {
                                Ok(obligation) => obligation,
                                Err(e) => {
                                    error!("Failed to deserialize obligation: {:?}, pubkey: {:?}", e, pubkey);
                                    continue;
                                }
                            };
                            let mut obligation_map_write = obligation_map.write().unwrap();
                            obligation_map_write.insert(pubkey, obligation);
                            info!("Obligation updated: {:?}, obligation: {:?}", pubkey, obligation);
                            continue;
                        }

                        if data.len() < 8 {
                            debug!("Account: {:?} is not scope price account", pubkey);
                            continue;
                        }

                        let disc_bytes = &data[0..8];
                        if disc_bytes != ScopePrices::discriminator() {
                            debug!("Account: {:?} is not scope price account", pubkey);
                            continue;
                        }

                        let scope_prices = bytemuck::from_bytes::<ScopePrices>(&data[8..]);
                        //info!("Account: {:?}, scope_prices updated: {:?}", pubkey, scope_prices.prices.len());

                        //for price in scope_prices.prices {
                        //    let price_age_in_seconds = clock.unix_timestamp.saturating_sub(price.unix_timestamp as i64);
                        //    info!("Price age: {:?} second, price: value={}, exp={}", price_age_in_seconds, price.price.value, price.price.exp);
                        //}

                        let mut price_changed_reserves: HashSet<Pubkey> = HashSet::new();

                        for (reserve_pubkey, reserve) in all_reserves.iter() {
                            if reserve.config.token_info.scope_configuration.price_feed == pubkey {
                                if let Some(price) = get_price_usd(&scope_prices, reserve.config.token_info.scope_configuration.price_chain) {
                                    //let price_age_in_seconds = clock.unix_timestamp.saturating_sub(price.timestamp as i64);
                                    //info!("WebSocket update - reserve: {} price: {:?} age: {:?} seconds", reserve.config.token_info.symbol(), price, price_age_in_seconds);
                                    if let Some(old_price) = reserves_prices.get(reserve_pubkey) {
                                        if old_price.price_value != price.price_value {
                                            price_changed_reserves.insert(*reserve_pubkey);
                                            reserves_prices.insert(*reserve_pubkey, price.clone());
                                            info!("Price changed for reserve: {} new price: {:?}", reserve.config.token_info.symbol(), price);
                                        }
                                    } else {
                                        price_changed_reserves.insert(*reserve_pubkey);
                                        reserves_prices.insert(*reserve_pubkey, price.clone());
                                        info!("Price changed for reserve: {} new price: {:?}", reserve.config.token_info.symbol(), price);
                                    }
                                }
                            }
                        }

                        if price_changed_reserves.is_empty() {
                            info!("No price changed for reserves, skip");
                            continue;
                        }

                        for market_pubkey in market_pubkeys {
                            let start = std::time::Instant::now();

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
                            let obligations = market_obligations_map.get_mut(market_pubkey).unwrap();

                            let mut obligation_map_write = obligation_map.write().unwrap();
                            let checked_obligation_count = scan_obligations(klend_client,
                                &mut obligation_map_write,
                                &mut obligation_reservers_to_refresh,
                                &clock,
                                obligations,
                                all_reserves,
                                all_lending_market.get(market_pubkey).unwrap(),
                                all_rts.get(market_pubkey).unwrap(),
                                Some(&price_changed_reserves)
                            ).await;

                            let duration = start.elapsed();
                            info!("Scan {} obligations, time used: {:?} s, checked {} obligations", obligations.len(), duration, checked_obligation_count);
                        }

                        //refresh market obligation map
                        match load_obligations_map(scope.clone()).await {
                            Ok(updated_obligations_map) => {
                                //find all obligations pubkeys in updated_obligations_map that are not in market_obligations_map
                                let updated_obligations_pubkeys = updated_obligations_map.values().flatten().cloned().collect::<Vec<Pubkey>>();
                                let obligations_map_pubkeys = market_obligations_map.values().flatten().cloned().collect::<Vec<Pubkey>>();
                                let mut obligations_to_refresh = Vec::new();
                                for pubkey in updated_obligations_pubkeys {
                                    if !obligations_map_pubkeys.contains(&pubkey) {
                                        obligations_to_refresh.push(pubkey);
                                    }
                                }

                                if obligations_to_refresh.is_empty() {
                                    info!("No new obligations to refresh");
                                    continue;
                                }

                                pubkeys.extend(obligations_to_refresh.clone());

                                let mut accounts = HashMap::new();
                                let account_filter = pubkeys.iter().map(|key| key.to_string()).collect::<Vec<String>>();
                                accounts.insert(
                                    "client".to_string(),
                                    SubscribeRequestFilterAccounts {
                                        account: account_filter,
                                        owner: vec![],
                                        filters: vec![],
                                    },
                                );
                                //send subscribe request to refresh obligations
                                subscribe_tx.send(
                                    SubscribeRequest {
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
                                    }
                                ).await.map_err(|e| {
                                    anyhow::Error::msg(format!(
                                        "Failed to subscribe: {} ({})",
                                        endpoint, e
                                    ))
                                })?;
                                all_obligations_pubkeys.extend(obligations_to_refresh);
                                market_obligations_map.clear();
                                market_obligations_map.extend(updated_obligations_map);
                                info!("Successfully loaded {} markets from obligations map", market_obligations_map.len());
                            }
                            Err(e) => {
                                error!("Failed to load obligations map: {:?}", e);
                            }
                        }
                    }
                }
                Some(UpdateOneof::Transaction(transaction)) => {
                    info!("Transaction of competitor: {:?}", transaction);
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
