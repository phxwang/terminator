use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
use anchor_lang::Id;
use solana_sdk::address_lookup_table::instruction;
use anchor_spl::token::Token;
use anyhow::Result;
use kamino_lending::{LendingMarket, Reserve, Obligation};
use scope::OraclePrices as ScopePrices;
use orbit_link::{async_client::AsyncClient, OrbitLink};
use spl_associated_token_account::instruction::create_associated_token_account;
use tracing::{info, error};
use tokio::time::sleep;
use extra_proto::GetMultipleAccountsRequest;
use yellowstone_grpc_proto::tonic;

use crate::{
    client::{rpc, KlendClient},
    consts::WRAPPED_SOL_MINT,
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
pub fn get_price_usd(
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

    info!("load {} reserves from market: {}", reserves.len(), lending_market);

    for reserve_key in reserves.keys() {
        client.check_and_add_to_custom_lookup_table(*reserve_key).await?;
    }

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
    obligation_pubkey: Pubkey,
    obligation: Obligation,
) -> Result<()> {
    let request = GetMultipleAccountsRequest {
        addresses: dump_account_pubkeys.iter()
            .map(|address| address.to_bytes().to_vec())
            .collect(),
        commitment_or_slot: 0,
    };
    info!("start dump accounts to file: {:?}, slot: {}", request, slot);
    let response = extra_client.get_multiple_accounts(request).await?;
    let accounts = response.into_inner();

    let mut file_path = format!("./dump_data/{}.json", slot);

    //check if file exists
    if Path::new(&file_path).exists() {
        info!("File {} already exists, append timestamp to file path", file_path);
        file_path = format!("./dump_data/{}_{}.json", slot, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    }

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
    json_array.push(serde_json::Value::String(format!("{}: {}", obligation_pubkey, obligation.to_string())));
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
    let start_index = if json_array.as_array().unwrap().len() == 2 { 0 } else { 1 };
    let json_map = json_array.as_array().unwrap()[start_index].as_object().unwrap();
    let reserve_pubkeys = json_array.as_array().unwrap()[start_index + 1].as_array().unwrap().iter().map(|b| Pubkey::from_str(b.as_str().unwrap()).unwrap()).collect::<Vec<Pubkey>>();

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

pub async fn create_new_lookup_tables_account(klend_client: &Arc<KlendClient>) -> Result<()> {
    use solana_sdk::commitment_config::CommitmentConfig;

    //在链上新建一个lookup table account
    let recent_slot = klend_client
        .client
        .client
        .get_slot_with_commitment(CommitmentConfig::finalized())
        .await?;

    let (create_lookup_table_ix, table_pubkey) = instruction::create_lookup_table(
        klend_client.client.payer_pubkey(),
        klend_client.client.payer_pubkey(),
        recent_slot,
    );

    let tx = klend_client
        .client
        .tx_builder()
        .add_ix(create_lookup_table_ix)
        .build(&[])
        .await?;

    let (sig, _) = klend_client
        .client
        .send_retry_and_confirm_transaction(tx, None, false)
        .await?;

    info!("Created new lookup table account: {} with signature: {:?}", table_pubkey, sig);
    Ok(())
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


