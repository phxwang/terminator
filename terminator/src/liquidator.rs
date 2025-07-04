use std::{collections::HashMap, sync::Arc};

use anchor_client::solana_client::nonblocking::rpc_client::RpcClient;
use anchor_lang::{prelude::Pubkey, solana_program::program_pack::Pack, AccountDeserialize, Id};
use anchor_spl::token::{Mint, Token};
use anyhow::{anyhow, Result};
use kamino_lending::Reserve;
use solana_sdk::{signature::Keypair, signer::Signer};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account,
};
use spl_token::state::Account as TokenAccount;
use spl_token_2022::{
    extension::StateWithExtensions,
    state::{Account as Token2022Account, Mint as Token2022Mint},
    ID as TOKEN_2022_PROGRAM_ID,
};
use tracing::{info, warn, error};

use crate::{accounts::find_accounts, client::KlendClient, config::get_lending_markets};

#[derive(Debug, Clone, Default)]
pub struct Holdings {
    pub holdings: Vec<Holding>,
    pub sol: Holding,
}

#[derive(Debug, Clone, Default)]
pub struct Holding {
    pub mint: Pubkey,
    pub ata: Pubkey,
    pub decimals: u8,
    pub balance: u64,
    pub ui_balance: f64,
    pub label: String,
    pub usd_value: f64,
}

impl Holdings {
    pub fn holding_of(&self, mint: &Pubkey) -> Result<Holding> {
        for holding in self.holdings.iter() {
            if holding.mint == *mint {
                return Ok(holding.clone());
            }
        }
        Err(anyhow!("Holding not found for mint {}", mint))
    }
}

#[derive(Debug, Clone)]
pub struct Liquidator {
    pub wallet: Arc<Keypair>,
    pub atas: HashMap<Pubkey, Pubkey>,
}

fn label_of(mint: &Pubkey, reserves: &HashMap<Pubkey, Reserve>) -> String {
    for (_, reserve) in reserves.iter() {
        if &reserve.liquidity.mint_pubkey == mint {
            let symbol = reserve.config.token_info.symbol().to_string();
            if symbol == "SOL" {
                return "WSOL".to_string();
            } else {
                return symbol;
            }
        }
    }
    mint.to_string()
}

/// Determine if a mint uses Token2022 or SPL Token program
async fn get_token_program_ids(client: &RpcClient, mints: &Vec<Pubkey>) -> Result<HashMap<Pubkey, Pubkey>> {

    //分割成最多100个一组
    let chunks = mints.chunks(100);
    let mut token_program_ids = HashMap::new();
    for chunk in chunks {
        let mint_accounts = client.get_multiple_accounts(chunk).await?;
        for (i, mint_account) in mint_accounts.iter().enumerate() {
            if let Some(mint_account) = mint_account {
                if mint_account.owner == TOKEN_2022_PROGRAM_ID {
                    token_program_ids.insert(chunk[i], TOKEN_2022_PROGRAM_ID);
                } else {
                    token_program_ids.insert(chunk[i], Token::id());
                }
            } else {
                warn!("Mint account not found for mint {}", chunk[i]);
            }
        }
    }
    Ok(token_program_ids)
}

impl Liquidator {
    pub async fn init(
        client: &KlendClient
    ) -> Result<Liquidator> {
        // Load reserves mints

        let mut reserves = HashMap::new();

        let lending_markets = get_lending_markets(&client.program_id).await?;

        for market in lending_markets {
            let market_accs = client.fetch_market_and_reserves(&market).await?;
            reserves.extend(market_accs.reserves);
        }

        let mints: Vec<Pubkey> = reserves
            .iter()
            .flat_map(|(_, r)| [r.liquidity.mint_pubkey, r.collateral.mint_pubkey])
            .collect();
        // Load wallet
        let (wallet, atas) = match { client.client.payer().ok() } {
            Some(wallet) => {
                // Load or create atas
                info!("Loading atas...");
                let wallet = Arc::new(wallet.insecure_clone());
                let atas = get_or_create_atas(client, &wallet, &mints).await?;
                info!(
                    "Loaded liquidator {} with {} tokens",
                    wallet.pubkey(),
                    atas.len()
                );
                (wallet, atas)
            }
            None => (Arc::new(Keypair::new()), HashMap::new()),
        };

        let liquidator = Liquidator { wallet, atas };

        Ok(liquidator)
    }

    pub async fn fetch_holdings(
        &self,
        client: &RpcClient,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> Result<Holdings> {
        let mut holdings = Vec::new();
        // TODO: optimize this, get all accounts in batch to have 1 single rpc call
        let get_token_balance_futures = self
            .atas
            .iter()
            .map(|(mint, ata)| get_token_balance(client, mint, ata));
        let get_token_balance = futures::future::join_all(get_token_balance_futures).await;
        for (i, (mint, ata)) in self.atas.iter().enumerate() {
            match get_token_balance.get(i) {
                Some(Ok((balance, decimals))) => {
                    let ui_balance = *balance as f64 / 10u64.pow(*decimals as u32) as f64;
                    holdings.push(Holding {
                        mint: *mint,
                        ata: *ata,
                        decimals: *decimals,
                        balance: *balance,
                        ui_balance,
                        label: label_of(mint, reserves),
                        usd_value: 0.0,
                    });
                }
                Some(Err(e)) => {
                    warn!(
                        "Error getting balance for mint {:?} and ata {:?}: {:?}",
                        mint, ata, e
                    );
                }
                None => {
                    warn!("No result for mint {:?} and ata {:?}", mint, ata);
                }
            }
        }

        // Load SOL balance
        let balance = match client.get_balance(&self.wallet.pubkey()).await {
            Ok(balance) => balance,
            Err(e) => {
                error!("Error getting SOL balance for {}: {}", self.wallet.pubkey(), e);
                0 // Default to 0 balance if we can't fetch it
            }
        };
        let ui_balance = balance as f64 / 10u64.pow(9) as f64;
        let sol_holding = Holding {
            mint: Pubkey::default(), // No mint, this is the native balance
            ata: Pubkey::default(),  // Holding in the native account, not in the ata
            decimals: 9,
            balance,
            ui_balance,
            label: "SOL".to_string(),
            usd_value: 0.0,
        };
        info!("Holding {} SOL", sol_holding.ui_balance);

        for holding in holdings.iter() {
            if holding.balance > 0 {
                info!("Holding {} {}", holding.ui_balance, holding.label);
            }
        }

        let holding = Holdings {
            holdings,
            sol: sol_holding,
        };
        Ok(holding)
    }
}

async fn get_or_create_atas(
    client: &KlendClient,
    owner: &Arc<Keypair>,
    mints: &[Pubkey],
) -> Result<HashMap<Pubkey, Pubkey>> {
    let owner_pubkey = &owner.pubkey();

    // Determine the correct token program for these mints
    let token_program_ids = match get_token_program_ids(&client.client.client, &mints.to_vec()).await {
        Ok(token_program_ids) => token_program_ids,
        Err(e) => {
            warn!("Failed to determine token program for mints: {}", e);
            warn!("Skipping ATA creation due to unknown token program");
            return Ok(HashMap::new());
        }
    };

    // Calculate ATA address using the correct token program
    let mut atas = HashMap::new();
    for (mint, token_program_id) in token_program_ids.iter() {
        let ata = get_associated_token_address_with_program_id(owner_pubkey, mint, token_program_id);
        atas.insert(*mint, ata);
    }

    let ata_addresses: Vec<Pubkey> = atas.values().cloned().collect();
    let accounts = find_accounts(&client.local_client.client, &ata_addresses).await?;
    let mut existing_atas = HashMap::new();

    for (mint, ata) in atas.iter() {
        if accounts.get(ata).is_some() {
            existing_atas.insert(*mint, *ata);
        } else {
            let token_program_id = token_program_ids.get(mint).unwrap();
            info!("Creating ATA for mint {} using token program {} with address {}", mint, token_program_id, ata);

            let ix = create_associated_token_account(owner_pubkey, owner_pubkey, mint, token_program_id);
            let tx = match client
                .client
                .tx_builder()
                .add_ix(ix)
                .build(&[])
                .await {
                    Ok(tx) => tx,
                    Err(e) => {
                        warn!("Error building transaction for ATA creation: {}", e);
                        continue;
                    }
                };

            match client.send_and_confirm_transaction(tx).await {
                Ok((sig, _)) => {
                    info!(
                        "Created ata for liquidator: {}, mint: {}, ata: {}, sig: {:?}",
                        owner.pubkey(),
                        mint,
                        ata,
                        sig
                    );
                    existing_atas.insert(*mint, *ata);
                }
                Err(e) => {
                    warn!("Error creating ata for liquidator: {}, mint: {}, ata: {}, program_id: {:?}, error: {:?}", owner.pubkey(), mint, ata, token_program_id, e);
                }
            }
        }
    }

    Ok(existing_atas)
}

/// get the balance of a token account
pub async fn get_token_balance(
    client: &RpcClient,
    mint: &Pubkey,
    token_account: &Pubkey,
) -> Result<(u64, u8)> {
    let mint_account = client.get_account(mint).await?;
    let token_account = client.get_account(token_account).await?;

    // Check if it's a Token2022 mint
    if mint_account.owner == TOKEN_2022_PROGRAM_ID {
        let token_account = StateWithExtensions::<Token2022Account>::unpack(&token_account.data)?;
        let mint_account = StateWithExtensions::<Token2022Mint>::unpack(&mint_account.data)?;
        let amount = token_account.base.amount;
        let decimals = mint_account.base.decimals;
        Ok((amount, decimals))
    } else {
        // Standard SPL Token
        let token_account = TokenAccount::unpack(&token_account.data)?;
        let mint_account = Mint::try_deserialize_unchecked(&mut mint_account.data.as_ref())?;
        let amount = token_account.amount;
        let decimals = mint_account.decimals;
        Ok((amount, decimals))
    }
}
