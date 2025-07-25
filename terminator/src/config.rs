use std::{collections::HashMap, path::PathBuf, str::FromStr, time::Duration};

use anchor_lang::{prelude::Pubkey, solana_program};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use orbit_link::OrbitLink;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Keypair},
};
use static_pubkey::static_pubkey;
use extra_proto::extra_client::ExtraClient;
use yellowstone_grpc_proto::tonic::transport;

use crate::{
    client::{KlendClient, RebalanceConfig},
    Actions, Args, RebalanceArgs,
};

lazy_static! {
    pub static ref LENDING_MARKETS: HashMap<Pubkey, Vec<Pubkey>> = {
        let mut m = HashMap::new();
        let mainnet_markets = vec![
            static_pubkey!("7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"),
            static_pubkey!("ByYiZxp8QrdN9qbdtaAiePN8AAr3qvTPppNJDpf5DVJ5"),
            static_pubkey!("DxXdAyU3kCjnyggvHmY5nAwg5cRbbmdyX3npfDMjjMek"),
        ];
        m.insert(
            static_pubkey!("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"),
            mainnet_markets,
        );
        let staging_markets = vec![static_pubkey!(
            "6WVSwDQXrBZeQVnu6hpnsRZhodaJTZBUaC334SiiBKdb"
        )];
        m.insert(
            static_pubkey!("SLendK7ySfcEzyaFqy93gDnD3RtrpXJcnRwb6zFHJSh"),
            staging_markets,
        );
        m
    };
}

pub async fn get_lending_markets(program_id: &Pubkey) -> Result<Vec<Pubkey>> {
    let env = std::env::var("MARKETS").ok();
    let markets = if let Some(markets) = env {
        let markets: Vec<Pubkey> = markets
            .split(',')
            .map(|s| Pubkey::from_str(s).unwrap())
            .collect();
        markets
    } else {
        // todo use API
        let markets = LENDING_MARKETS
            .get(program_id)
            .ok_or(anyhow!("No markets found for program {:?}", program_id))?;
        markets.clone()
    };
    Ok(markets)
}

// 根据entry生成tonic-channel
pub async fn generate_channel(
    entry: impl Into<String>, // 连接目标
    _ip: Option<std::net::Ipv4Addr>, // IP parameter kept for API compatibility but not used in simplified version
) -> Result<transport::Channel> {
    let entry = entry.into();

    // Simplified channel creation using basic tonic functionality
    let channel = transport::Channel::from_shared(entry)?
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::from_secs(1)))
        .http2_keep_alive_interval(Duration::from_secs(1))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .initial_connection_window_size(Some(8 * 1024 * 1024))
        .initial_stream_window_size(Some(4 * 1024 * 1024))
        .buffer_size(64 * 1024)
        .connect()
        .await?;

    Ok(channel)
}

pub async fn get_client_for_action(args: &Args) -> Result<KlendClient> {
    let (payer, placeholder) = get_keypair_for_action(&args.keypair)?;
    let commitment = CommitmentConfig::processed();
    let rpc = RpcClient::new_with_timeout_and_commitment(
        args.cluster.url().to_string(),
        Duration::from_secs(300),
        commitment,
    );
    let orbit_link: OrbitLink<RpcClient, Keypair> =
        OrbitLink::new(rpc, payer, None, commitment, placeholder)?;
    let local_rpc = RpcClient::new_with_timeout_and_commitment(
        args.local_cluster.url().to_string(),
        Duration::from_secs(300),
        commitment,
    );

    let (local_payer, placeholder) = get_keypair_for_action(&args.keypair)?;
    let local_orbit_link: OrbitLink<RpcClient, Keypair> =
        OrbitLink::new(local_rpc, local_payer, None, commitment, placeholder)?;

    // Create ExtraClient - for now create a dummy one, this should be configured properly based on requirements
    /*let extra_client = {
        // Try to get EXTRA environment variable, use a default if not present
        let extra_url = env::var("EXTRA").unwrap_or_else(|_| "http://localhost:50051".to_string());

        // Create a channel and ExtraClient
        // Note: This is a simplified version. In production, you might want proper error handling
        // and configuration for the channel
        let channel = tonic::transport::Channel::from_shared(extra_url)
            .map_err(|e| anyhow!("Failed to create channel: {}", e))?
            .connect()
            .await
            .map_err(|e| anyhow!("Failed to connect to extra service: {}", e))?;

        ExtraClient::new(channel)
    };*/

    // 自建服务，用于模拟交易
    let extra = args.extra.clone();
    log::info!("extra: {}", extra);
    let channel = generate_channel(extra, None)
        .await.expect("fail to generate channel");
    let extra_client = ExtraClient::new(channel);

    let rebalance_config = get_rebalance_config_for_action(&args.action);
    let klend_client = KlendClient::init(
        orbit_link,
        local_orbit_link,
        extra_client,
        args.klend_program_id.unwrap_or(kamino_lending::id()),
        rebalance_config,
    )?;
    Ok(klend_client)
}

pub fn get_keypair_for_action(
    keypair: &Option<PathBuf>,
) -> Result<(Option<Keypair>, Option<Pubkey>)> {
    let (keypair, pubkey) = client_keypair_and_pubkey(keypair)?;
    validate_keypair_for_action(&keypair)?;
    Ok((keypair, pubkey))
}

pub fn client_keypair_and_pubkey(
    keypair: &Option<PathBuf>,
) -> Result<(Option<Keypair>, Option<Pubkey>)> {
    Ok(if let Some(key) = keypair {
        (
            Some(
                read_keypair_file(key.clone())
                    .map_err(|e| anyhow!("Keypair file {:?} not found or invalid {:?}", key, e))?,
            ),
            None,
        )
    } else {
        (
            None,
            Some(Pubkey::from_str(
                "K1endProducer111111111111111111111111111111",
            )?),
        )
    })
}

fn validate_keypair_for_action(keypair: &Option<Keypair>) -> Result<()> {
    if keypair.is_none() {
        return Err(anyhow::anyhow!("Keypair is required for this action"));
    }

    Ok(())
}

pub fn get_rebalance_config_for_action(action: &Actions) -> Option<RebalanceConfig> {
    match action {
        Actions::Liquidate { rebalance_args, .. }
        | Actions::Crank { rebalance_args, .. }
        | Actions::Swap { rebalance_args, .. }
        | Actions::LoopLiquidate { rebalance_args, .. }
        | Actions::StreamLiquidate { rebalance_args, .. }
        | Actions::CreateLookupTable { rebalance_args, .. } => Some(parse_rebalance_args(rebalance_args)),
    }
}

fn parse_rebalance_args(args: &RebalanceArgs) -> RebalanceConfig {
    RebalanceConfig {
        base_token: args.base_currency,
        min_sol_balance: args.min_sol_balance,
        usdc_mint: args.usdc_mint,
        rebalance_slippage_pct: args.rebalance_slippage_pct,
        non_swappable_dust_usd_value: args.non_swappable_dust_usd_value,
    }
}
