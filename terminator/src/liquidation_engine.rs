use std::{collections::{HashMap, HashSet}, sync::{Arc, RwLock}, time::Duration, str::FromStr, env};

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
    bs58,
    hash::Hash,
};
use tokio::time::sleep;
use tracing::{info, warn, debug, error};
use extra_proto::{Replace, SimulateTransactionRequest};
use futures::SinkExt;

use scope::OraclePrices as ScopePrices;
use anchor_lang::{AccountDeserialize, Discriminator};
use yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions, SubscribeRequestFilterBlocksMeta, subscribe_update::UpdateOneof, SubscribeUpdate};
use yellowstone_grpc_proto::geyser::{CommitmentLevel};
use yellowstone_grpc_proto::tonic;

use bytemuck;

use crate::{
    accounts::{dump_accounts_to_file, refresh_oracle_keys, load_obligations_map, MarketAccounts, oracle_accounts, OracleAccounts, load_competitors_from_file, TimestampedPrice, get_price_usd},
    client::KlendClient,
    model::StateWithKey,
    operations::{
        obligation_reserves, referrer_token_states_of_obligation, refresh_reserves_and_obligation,
        ObligationReserves,
    },
    math,
    sysvars,
swap,
    instruction_parser,
    yellowstone_transaction::create_yellowstone_client,
};

// Helper struct to organize liquidation instructions
#[derive(Debug)]
struct LiquidationInstructions {
    instructions: Vec<Instruction>,
    lookup_tables: Vec<AddressLookupTableAccount>,
}

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
    /// Cache for latest blockhash and slot
    pub latest_blockhash: Option<Hash>,
    /// Cache for latest slot
    pub latest_slot: Option<u64>,
    /// ObligationCooldown
    pub obligation_cooldown: HashMap<Pubkey, u64>,
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
            latest_blockhash: None,
            latest_slot: None,
            obligation_cooldown: HashMap::new(),
        }
    }

    pub async fn liquidate(&mut self, klend_client: &Arc<KlendClient>, obligation: &Pubkey, slot: Option<u64>) -> Result<()> {
        info!("Liquidating obligation {}, slot: {:?}", obligation.to_string().green(), slot);
        debug!("Liquidator ATAs: {:?}", klend_client.liquidator.atas);

        info!("Performing full data refresh");

        let (obligation_data, market, loaded_accounts_data) = match slot {
            Some(slot) => {
                // load data from extra
                let (obligation_data, clock, reserves, market, rts, loaded_accounts_data) =klend_client.load_data_from_file(obligation, slot).await?;
                self.clock = Some(clock);
                self.all_reserves = reserves;
                self.all_rts.insert(obligation_data.lending_market, rts);
                (obligation_data, market, loaded_accounts_data)
            },
            None => {
                self.clock = Some(sysvars::clock(&klend_client.local_client.client).await?);

                // Reload accounts
                let mut ob = klend_client.fetch_obligation(obligation).await?;

                info!("Obligation before refresh: {:?}", ob);
                info!("Obligation summary before refresh: {:?}", ob.to_string());

                let market_accs = klend_client
                    .fetch_market_and_reserves(&ob.lending_market)
                    .await?;

                self.all_reserves = market_accs.reserves;
                let market = market_accs.lending_market;
                // todo - don't load all
                let referrer_token_states = klend_client.fetch_referrer_token_states().await?;
                self.all_rts.insert(ob.lending_market, referrer_token_states.clone());

                refresh_reserves_and_obligation(klend_client, &obligation, &mut ob, &mut self.all_reserves, &referrer_token_states, &market, self.clock.as_ref().unwrap()).await?;

                Self::dump_liquidation_accounts_static(klend_client, obligation, &ob, &self.all_reserves, self.clock.as_ref().unwrap()).await?;

                (ob, market, None)
            }
        };

        //fetch latest blockhash
        self.latest_blockhash = Some(klend_client.local_client.client.get_latest_blockhash().await?);

        self.preload_swap_instructions(klend_client, obligation, &obligation_data).await?;

        self.liquidate_with_loaded_data(klend_client, obligation, &mut obligation_data.clone(), market, loaded_accounts_data).await?;

        Ok(())
    }

pub async fn liquidate_with_loaded_data(
        &mut self,
        klend_client: &Arc<KlendClient>,
        obligation_key: &Pubkey,
        ob: &mut Obligation,
        market: LendingMarket,
        loaded_accounts_data: Option<HashMap<Pubkey, Account>>,
    ) -> Result<(), anyhow::Error> {
        info!("Liquidating: Obligation: {:?}", ob);
        info!("Liquidating: Obligation summary: {:?}", ob.to_string());

        let (debt_reserve, coll_reserve, lending_market, obligation_state) =
            self.find_best_reserves(ob, obligation_key, &market)?;

        let (liquidate_amount, net_withdraw_liquidity_amount) =
            self.calculate_liquidation_params(&obligation_state, &lending_market, &coll_reserve, &debt_reserve)?;

        let liquidation_instructions = match self.build_liquidation_instructions(
            klend_client,
            &debt_reserve,
            &coll_reserve,
            &lending_market,
            &obligation_state,
            liquidate_amount,
            net_withdraw_liquidity_amount,
        ).await {
            Ok(instructions) => instructions,
            Err(e) => {
                error!("Error building liquidation instructions: {}", e);
                self.obligation_cooldown.insert(*obligation_key, self.clock.as_ref().unwrap().slot);
                return Ok(());
            }
        };

        self.execute_liquidation_transaction(
            klend_client,
            liquidation_instructions,
            ob,
            *obligation_key,
            loaded_accounts_data,
        ).await?;

        Ok(())
    }

    fn find_best_reserves(
        &self,
        ob: &Obligation,
        obligation: &Pubkey,
        market: &LendingMarket,
    ) -> Result<(StateWithKey<Reserve>, StateWithKey<Reserve>, StateWithKey<LendingMarket>, StateWithKey<Obligation>)> {
        let debt_res_key = match math::find_best_debt_reserve(&ob.borrows, &self.all_reserves) {
            Some(key) => key,
            None => {
                error!("No debt reserve found for obligation {}", obligation);
                return Err(anyhow::anyhow!("No debt reserve found for obligation"));
            }
        };

        let coll_res_key = match math::find_best_collateral_reserve(&ob.deposits, &self.all_reserves) {
            Some(key) => key,
            None => {
                error!("No collateral reserve found for obligation {}", obligation);
                return Err(anyhow::anyhow!("No collateral reserve found for obligation"));
            }
        };

        let debt_reserve_state = match self.all_reserves.get(&debt_res_key) {
            Some(reserve) => *reserve,
            None => {
                error!("Debt reserve {} not found in reserves", debt_res_key);
                return Err(anyhow::anyhow!("Debt reserve not found in reserves"));
            }
        };

        let coll_reserve_state = match self.all_reserves.get(&coll_res_key) {
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
        let lending_market = StateWithKey::new(*market, ob.lending_market);
        let obligation_state = StateWithKey::new(ob.clone(), *obligation);

        Ok((debt_reserve, coll_reserve, lending_market, obligation_state))
    }

    fn calculate_liquidation_params(
        &self,
        obligation: &StateWithKey<Obligation>,
        lending_market: &StateWithKey<LendingMarket>,
        coll_reserve: &StateWithKey<Reserve>,
        debt_reserve: &StateWithKey<Reserve>,
    ) -> Result<(u64, u64)> {
        self.log_liquidation_info(debt_reserve, coll_reserve);

        let liquidate_amount = self.calculate_liquidate_amount(
            obligation, lending_market, coll_reserve, debt_reserve
        )?;

        let net_withdraw_liquidity_amount = self.simulate_liquidation_and_get_withdraw_amount(
            obligation, lending_market, coll_reserve, debt_reserve, liquidate_amount
        )?;

        Ok((liquidate_amount, net_withdraw_liquidity_amount))
    }

    fn log_liquidation_info(&self, debt_reserve: &StateWithKey<Reserve>, coll_reserve: &StateWithKey<Reserve>) {
        info!("Liquidating: Clock: {:?}", self.clock.as_ref().unwrap());
        info!("Liquidating: Debt reserve: {:?}, last_update: {:?}, is_stale: {:?}",
              debt_reserve.state.borrow().config.token_info.symbol(),
              debt_reserve.state.borrow().last_update,
              debt_reserve.state.borrow().last_update.is_stale(self.clock.as_ref().unwrap().slot, PriceStatusFlags::LIQUIDATION_CHECKS));
        info!("Liquidating: Coll reserve: {:?}, last_update: {:?}, is_stale: {:?}",
              coll_reserve.state.borrow().config.token_info.symbol(),
              coll_reserve.state.borrow().last_update,
              coll_reserve.state.borrow().last_update.is_stale(self.clock.as_ref().unwrap().slot, PriceStatusFlags::LIQUIDATION_CHECKS));
    }

    fn calculate_liquidate_amount(
        &self,
        obligation: &StateWithKey<Obligation>,
        lending_market: &StateWithKey<LendingMarket>,
        coll_reserve: &StateWithKey<Reserve>,
        debt_reserve: &StateWithKey<Reserve>,
    ) -> Result<u64> {
        let max_allowed_ltv_override_pct_opt = Some(0);
        let liquidation_swap_slippage_pct = 0_f64;

        let liquidate_amount = math::get_liquidatable_amount(
            obligation,
            lending_market,
            coll_reserve,
            debt_reserve,
            self.clock.as_ref().unwrap(),
            max_allowed_ltv_override_pct_opt,
            liquidation_swap_slippage_pct,
        )?;

        let borrowed_amount = Fraction::from_sf(
            obligation.state.borrow().borrows.iter()
                .find(|b| b.borrow_reserve == debt_reserve.key)
                .unwrap()
                .borrowed_amount_sf
        );

        info!("Liquidating amount: {}, borrowed_amount: {}", liquidate_amount, borrowed_amount);
        Ok(liquidate_amount)
    }

        fn simulate_liquidation_and_get_withdraw_amount(
        &self,
        obligation: &StateWithKey<Obligation>,
        lending_market: &StateWithKey<LendingMarket>,
        coll_reserve: &StateWithKey<Reserve>,
        debt_reserve: &StateWithKey<Reserve>,
        liquidate_amount: u64,
    ) -> Result<u64> {
        let deposit_reserves = self.prepare_deposit_reserves(obligation);
        let max_allowed_ltv_override_pct_opt = Some(0);
        let min_acceptable_received_collateral_amount = 0;

        let res = kamino_lending::lending_market::lending_operations::liquidate_and_redeem(
            &lending_market.state.borrow(),
            debt_reserve,
            coll_reserve,
            &mut obligation.state.borrow_mut().clone(),
            self.clock.as_ref().unwrap(),
            liquidate_amount,
            min_acceptable_received_collateral_amount,
            max_allowed_ltv_override_pct_opt,
            deposit_reserves.into_iter(),
        );

        info!("Simulating the Liquidating {:#?}", res);

        match res {
            Ok(result) => {
                match result.total_withdraw_liquidity_amount {
                    Some((withdraw_liquidity_amount, protocol_fee)) => {
                        let net_amount = withdraw_liquidity_amount - protocol_fee;
                        info!("Liquidating: Net withdraw liquidity amount: {}", net_amount);
                        Ok(net_amount)
                    }
                    None => {
                        warn!("Total withdraw liquidity amount is None");
                        Ok(0)
                    }
                }
            }
            Err(_) => Err(anyhow::anyhow!("Liquidation simulation failed"))
        }
    }

    fn prepare_deposit_reserves(&self, obligation: &StateWithKey<Obligation>) -> Vec<StateWithKey<Reserve>> {
        obligation.state.borrow()
            .deposits
            .iter()
            .filter(|coll| coll.deposit_reserve != Pubkey::default())
            .map(|coll| {
                let reserve = self.all_reserves.get(&coll.deposit_reserve).unwrap();
                StateWithKey::new(reserve.clone(), coll.deposit_reserve)
            })
            .collect()
    }

    async fn build_liquidation_instructions(
        &mut self,
        klend_client: &Arc<KlendClient>,
        debt_reserve: &StateWithKey<Reserve>,
        coll_reserve: &StateWithKey<Reserve>,
        lending_market: &StateWithKey<LendingMarket>,
        obligation: &StateWithKey<Obligation>,
        liquidate_amount: u64,
        net_withdraw_liquidity_amount: u64,
    ) -> Result<LiquidationInstructions> {
        let mut ixns = vec![];
        let mut luts = vec![];

        // Step 1: Add flash borrow instructions
        let flash_borrow_instruction_index = self.add_flash_borrow_instructions(
            klend_client, debt_reserve, obligation, liquidate_amount, &mut ixns
        ).await?;

        // Step 2: Add liquidation instructions
        self.add_liquidation_instructions(
            klend_client, lending_market, debt_reserve, coll_reserve,
            obligation, liquidate_amount, &mut ixns
        ).await?;

        // Step 3: Add Jupiter swap instructions
        self.add_jupiter_swap_instructions(
            klend_client, obligation, coll_reserve, debt_reserve,
            net_withdraw_liquidity_amount, liquidate_amount, &mut ixns, &mut luts
        ).await?;

        // Step 4: Add flash repay instructions
        self.add_flash_repay_instructions(
            klend_client, debt_reserve, obligation, liquidate_amount,
            flash_borrow_instruction_index, &mut ixns
        ).await?;

        Ok(LiquidationInstructions {
            instructions: ixns,
            lookup_tables: luts,
        })
    }

    async fn add_flash_borrow_instructions(
        &self,
        klend_client: &Arc<KlendClient>,
        debt_reserve: &StateWithKey<Reserve>,
        obligation: &StateWithKey<Obligation>,
        liquidate_amount: u64,
        ixns: &mut Vec<Instruction>,
    ) -> Result<usize> {
        let flash_borrow_ixns = klend_client
            .flash_borrow_reserve_liquidity_ixns(
                debt_reserve,
                &obligation.key,
                liquidate_amount,
            )
            .await?;

        let flash_borrow_instruction_index = ixns.len();
        ixns.extend_from_slice(&flash_borrow_ixns);
        info!("Liquidating: Flash borrow ixns count: {:?}", flash_borrow_ixns.len());
        Ok(flash_borrow_instruction_index)
    }

    async fn add_liquidation_instructions(
        &self,
        klend_client: &Arc<KlendClient>,
        lending_market: &StateWithKey<LendingMarket>,
        debt_reserve: &StateWithKey<Reserve>,
        coll_reserve: &StateWithKey<Reserve>,
        obligation: &StateWithKey<Obligation>,
        liquidate_amount: u64,
        ixns: &mut Vec<Instruction>,
    ) -> Result<()> {
        let liquidate_ixns = klend_client
            .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market.clone(),
                debt_reserve.clone(),
                coll_reserve.clone(),
                obligation.clone(),
                liquidate_amount,
                0, // min_acceptable_received_collateral_amount
                Some(0), // max_allowed_ltv_override_pct_opt
                &self.all_reserves
            )
            .await
            .map_err(|e| {
                error!("Error creating liquidate instructions: {}", e);
                e
            })?;

        ixns.extend_from_slice(&liquidate_ixns);
        info!("Liquidating: Liquidate ixns count: {:?}", liquidate_ixns.len());
        Ok(())
    }

    async fn add_jupiter_swap_instructions(
        &mut self,
        klend_client: &Arc<KlendClient>,
        obligation: &StateWithKey<Obligation>,
        coll_reserve: &StateWithKey<Reserve>,
        debt_reserve: &StateWithKey<Reserve>,
        net_withdraw_liquidity_amount: u64,
        liquidate_amount: u64,
        ixns: &mut Vec<Instruction>,
        luts: &mut Vec<AddressLookupTableAccount>,
    ) -> Result<()> {
        let (jup_ixs, lookup_tables) = self.get_jupiter_swap_instructions(
            klend_client,
            &obligation.key,
            &coll_reserve.state.borrow().liquidity.mint_pubkey,
            &debt_reserve.state.borrow().liquidity.mint_pubkey,
            net_withdraw_liquidity_amount,
            liquidate_amount,
        ).await?;

        info!("Liquidating: Jupiter swap ixns count: {:?}", jup_ixs.len());
        ixns.extend_from_slice(&jup_ixs.into_iter().filter(|ix| ix.program_id != compute_budget::id()).collect::<Vec<_>>());

        if let Some(tables) = lookup_tables {
            luts.extend_from_slice(&tables);
        }
        Ok(())
    }

    async fn add_flash_repay_instructions(
        &self,
        klend_client: &Arc<KlendClient>,
        debt_reserve: &StateWithKey<Reserve>,
        obligation: &StateWithKey<Obligation>,
        liquidate_amount: u64,
        flash_borrow_instruction_index: usize,
        ixns: &mut Vec<Instruction>,
    ) -> Result<()> {
        let flash_repay_ixns = klend_client
            .flash_repay_reserve_liquidity_ixns(
                debt_reserve,
                &obligation.key,
                liquidate_amount,
                (flash_borrow_instruction_index + 2) as u8,
            )
            .await?;
        ixns.extend_from_slice(&flash_repay_ixns);
        Ok(())
    }

    async fn get_jupiter_swap_instructions(
        &mut self,
        klend_client: &Arc<KlendClient>,
        obligation_key: &Pubkey,
        coll_mint: &Pubkey,
        debt_mint: &Pubkey,
        net_withdraw_liquidity_amount: u64,
        liquidate_amount: u64,
    ) -> Result<(Vec<Instruction>, Option<Vec<AddressLookupTableAccount>>)> {
        match self.obligation_swap_map.get(obligation_key) {
            Some(swap_data) => {
                let mut modified_jup_ixs = swap_data.0.clone();

                //find first non compute budget instruction
                let first_ix_id = modified_jup_ixs.iter().position(|ix| ix.program_id != compute_budget::id()).unwrap();
                let first_inst = instruction_parser::parse_instruction_data(&modified_jup_ixs[first_ix_id].data, &modified_jup_ixs[first_ix_id].program_id);
                info!("Liquidating: before modify jupiter in amount: {:?}", first_inst);

                if modified_jup_ixs.len() > 2 {
                    let in_amount = first_inst.parsed_fields.iter().find(|f| f.name == "in_amount").unwrap().value.to_string();
                    let ratio = net_withdraw_liquidity_amount as f64 / in_amount.parse::<u64>().unwrap() as f64;

                    // Multiply ratio to all in_amounts and out_amounts in modified_jup_ixs start from index 1
                    for ix in modified_jup_ixs.iter_mut().skip(first_ix_id) {
                        let inst = instruction_parser::parse_instruction_data(&ix.data, &ix.program_id);
                        info!("Liquidating: before modify jupiter in amount, parsed: {:?}", inst);
                        
                        let in_amount = inst.parsed_fields.iter().find(|f| f.name == "in_amount").unwrap().value.to_string();
                        let out_amount = inst.parsed_fields.iter().find(|f| f.name == "quoted_out_amount").unwrap().value.to_string();
                        let in_amount_u64 = in_amount.parse::<u64>().unwrap();
                        let out_amount_u64 = out_amount.parse::<u64>().unwrap();
                        let new_in_amount = (in_amount_u64 as f64 * ratio) as u64;
                        let new_out_amount = (out_amount_u64 as f64 * ratio) as u64;
                        let _ = instruction_parser::modify_jupiter_in_amount(ix, new_in_amount);
                        let _ = instruction_parser::modify_jupiter_out_amount(ix, new_out_amount);

                        info!("Liquidating: Modified jupiter in amount: {:?}", instruction_parser::parse_instruction_data(&ix.data, &ix.program_id));
                    }
                }

                let last_ix_id = modified_jup_ixs.len() - 1;
                let _ = instruction_parser::modify_jupiter_in_amount(&mut modified_jup_ixs[first_ix_id], net_withdraw_liquidity_amount);
                let _ = instruction_parser::modify_jupiter_out_amount(&mut modified_jup_ixs[last_ix_id], liquidate_amount);

                info!("Liquidating: Modified jupiter in amount: {:?}", instruction_parser::parse_instruction_data(&modified_jup_ixs[first_ix_id].data, &modified_jup_ixs[first_ix_id].program_id));

                Ok((modified_jup_ixs, swap_data.1.clone()))
            },
            None => {
                info!("No swap data found for obligation {}, fetch swap instructions", obligation_key);
                swap::swap_with_jupiter_ixns(
                    klend_client,
                    coll_mint,
                    debt_mint,
                    net_withdraw_liquidity_amount,
                    Some(liquidate_amount),
                    0.0 // liquidation_swap_slippage_pct
                ).await
            }
        }
    }

    async fn execute_liquidation_transaction(
        &mut self,
        klend_client: &Arc<KlendClient>,
        liquidation_instructions: LiquidationInstructions,
        ob: &mut Obligation,
        obligation_key: Pubkey,
        loaded_accounts_data: Option<HashMap<Pubkey, Account>>,
    ) -> Result<()> {
        let LiquidationInstructions {
            instructions: ixns,
            lookup_tables: luts,
        } = liquidation_instructions;

        let mut txn = klend_client.local_client.tx_builder().add_ixs(ixns.clone());

        // Add lookup tables
        if let Some(lut) = klend_client.custom_lookup_table.read().unwrap().clone() {
            txn = txn.add_lookup_table(lut);
        }
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

        let txn = match txn.build_with_budget_and_fee(&[], self.latest_blockhash.clone()).await {
            Ok(txn) => txn,
            Err(e) => {
                error!("Error building transaction: {}", e);
                return Err(e.into());
            }
        };

        self.log_transaction_details(&txn);

        match loaded_accounts_data {
            Some(loaded_accounts_data) => {
                self.execute_with_extra_client(klend_client, &txn, &loaded_accounts_data, &obligation_key).await
            }
            None => {
                self.execute_with_local_client(klend_client, &txn, ob, &obligation_key).await
            }
        }
    }

    fn log_transaction_details(&self, txn: &solana_sdk::transaction::VersionedTransaction) {
        info!("Liquidating: txn.message.address_table_lookups: {:?}", txn.message.address_table_lookups());
        info!("Liquidating: txn.message.static_account_keys({}): {:?}", txn.message.static_account_keys().len(), txn.message.static_account_keys());

        let static_account_keys = txn.message.static_account_keys();
        let mut missing_account_keys: HashSet<_> = static_account_keys.iter().cloned().collect();
        for ix in txn.message.instructions() {
            let program_id = ix.program_id(static_account_keys);
            missing_account_keys.remove(&program_id);
        }
        info!("Liquidating: missing account keys({}): {:?}", missing_account_keys.len(), missing_account_keys);
    }

    async fn execute_with_extra_client(
        &self,
        klend_client: &Arc<KlendClient>,
        txn: &solana_sdk::transaction::VersionedTransaction,
        loaded_accounts_data: &HashMap<Pubkey, Account>,
        obligation_key: &Pubkey,
    ) -> Result<()> {
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
            data: serde_json::to_vec(txn).unwrap(),
            replaces: replaces,
            commitment_or_slot: self.clock.as_ref().unwrap().slot,
            addresses: vec![obligation_key.to_bytes().to_vec()],
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

        Ok(())
    }

    async fn execute_with_local_client(
        &mut self,
        klend_client: &Arc<KlendClient>,
        txn: &solana_sdk::transaction::VersionedTransaction,
        ob: &mut Obligation,
        obligation_key: &Pubkey,
    ) -> Result<()> {
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
                    self.handle_transaction_error(klend_client, ob, obligation_key).await?;

                    match klend_client
                        .local_client
                        .client
                        .simulate_transaction(txn)
                        .await
                        {
                            Ok(res) => {
                                info!("Liquidating: Simulation result: {:?}", res);
                            }
                            Err(e) => {
                                error!("Liquidating: Simulation error: {:?}", e);
                            }
                        };

                    self.obligation_cooldown.insert(*obligation_key, self.clock.as_ref().unwrap().slot);
                }
            };

        Ok(())
    }

    async fn handle_transaction_error(
        &mut self,
        klend_client: &Arc<KlendClient>,
        ob: &mut Obligation,
        obligation_key: &Pubkey,
    ) -> Result<()> {
        // Fetch newest clock
        let new_clock = match sysvars::get_clock(&klend_client.local_client.client).await {
            Ok(clock) => clock,
            Err(e) => {
                error!("Error getting clock: {}", e);
                return Err(e.into());
            }
        };

        info!("Liquidating: new clock after error: {:?}", new_clock);

        Self::dump_liquidation_accounts_static(klend_client, obligation_key, ob, &self.all_reserves, &new_clock).await?;

        //copy data from new_obligation to ob
        let new_obligation = klend_client.fetch_obligation(obligation_key).await?;
        *ob = new_obligation;

        self.preload_swap_instructions(klend_client, obligation_key, ob).await?;

        info!("Liquidating: Refreshing obligation in obligation_map {:?}, {:?}", obligation_key, self.obligation_map.get(obligation_key).unwrap().to_string());
        
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

        let (deposit_reserves, borrow_reserves) = self.prepare_obligation_data(
            address, obligation, reserves, rts
        )?;

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Prepared obligation data time used: {} in {}s", address.to_string().green(), en);

        self.refresh_obligation_state(
            address, obligation, lending_market, deposit_reserves, borrow_reserves
        )?;

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed obligation time used: {} in {}s", address.to_string().green(), en);

        let obligation_stats = math::obligation_info(address, &obligation);
        if obligation_stats.ltv > obligation_stats.unhealthy_ltv {
            self.process_liquidatable_obligation(
                klend_client, address, obligation, reserves
            ).await?;
        } else {
            debug!("[Liquidation Thread] Obligation is not liquidatable: {} {}", address.to_string().green(), obligation.to_string().green());
        }

        let en = start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Check and liquidate time used: {} in {}s", address.to_string().green(), en);

        Ok(())
    }

    fn prepare_obligation_data(
        &self,
        address: &Pubkey,
        obligation: &Obligation,
        reserves: &HashMap<Pubkey, Reserve>,
        rts: &HashMap<Pubkey, ReferrerTokenState>
    ) -> Result<(Vec<StateWithKey<Reserve>>, Vec<StateWithKey<Reserve>>)> {
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

        let _referrer_states = match referrer_token_states_of_obligation(
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

        Ok((deposit_reserves, borrow_reserves))
    }

    fn refresh_obligation_state(
        &self,
        address: &Pubkey,
        obligation: &mut Obligation,
        lending_market: &LendingMarket,
        deposit_reserves: Vec<StateWithKey<Reserve>>,
        borrow_reserves: Vec<StateWithKey<Reserve>>
    ) -> Result<()> {
        // Collect keys before moving the vectors
        let mut obligation_reserve_keys = Vec::new();
        obligation_reserve_keys.extend(borrow_reserves.iter().map(|b| b.key).collect::<Vec<_>>());
        obligation_reserve_keys.extend(deposit_reserves.iter().map(|d| d.key).collect::<Vec<_>>());

        let referrer_states: Vec<StateWithKey<ReferrerTokenState>> = Vec::new();

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

        Ok(())
    }

    async fn process_liquidatable_obligation(
        &mut self,
        klend_client: &Arc<KlendClient>,
        address: &Pubkey,
        obligation: &mut Obligation,
        reserves: &HashMap<Pubkey, Reserve>
    ) -> Result<()> {
        // Check cooldown
        if let Some(cooldown) = self.obligation_cooldown.get(address) {
            if *cooldown > &self.clock.as_ref().unwrap().slot - 1200 {
                info!("[Liquidation Thread] Obligation is on cooldown: {} {}, cooldown start slot: {}", address.to_string().green(), obligation.to_string().green(), cooldown);
                return Ok(());
            }
        }

        info!("[Liquidation Thread] Liquidating obligation start: pubkey: {}, obligation: {}, slot: {}",
              address.to_string().green(), obligation.to_string().green(), self.clock.as_ref().unwrap().slot);

        // Dump accounts
        let klend_client_clone = klend_client.clone();
        let address_clone = *address;
        let obligation_clone = obligation.clone();
        let reserves_clone = reserves.clone();
        let clock_clone = self.clock.as_ref().unwrap().clone();
        tokio::spawn(async move {
            if let Err(e) = Self::dump_liquidation_accounts_static(&klend_client_clone, &address_clone, &obligation_clone, &reserves_clone, &clock_clone).await {
                error!("[Liquidation Thread] Error dumping liquidation accounts: {} {}", address_clone.to_string().green(), e);
            }
        });

        // Get the lending market for this obligation
        let lending_market = match self.all_lending_market.get(&obligation.lending_market) {
            Some(market) => *market,
            None => {
                error!("Lending market {} not found", obligation.lending_market);
                return Err(anyhow::anyhow!("Lending market not found"));
            }
        };

        // Get the referrer token states for this obligation
        let _rts = match self.all_rts.get(&obligation.lending_market) {
            Some(rts) => rts.clone(),
            None => {
                error!("Referrer token states for market {} not found", obligation.lending_market);
                return Err(anyhow::anyhow!("Referrer token states not found"));
            }
        };

        // Execute liquidation
        let liquidate_start = std::time::Instant::now();
        match self.liquidate_with_loaded_data(
            klend_client,
            address,
            obligation,
            lending_market,
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

        Ok(())
    }

    async fn dump_liquidation_accounts_static(
        klend_client: &Arc<KlendClient>,
        address: &Pubkey,
        obligation: &Obligation,
        reserves: &HashMap<Pubkey, Reserve>,
        clock: &Clock
    ) -> Result<()> {
        let mut extra_client = klend_client.extra_client.clone();
        let mut all_oracle_keys = HashSet::new();
        let mut pyth_keys = HashSet::new();
        let mut switchboard_keys = HashSet::new();
        let mut scope_keys = HashSet::new();

        refresh_oracle_keys(&reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

        // Collect obligation reserve keys
        let mut obligation_reserve_keys = Vec::new();
        obligation_reserve_keys.extend(
            obligation.borrows.iter()
                .filter(|b| b.borrow_reserve != Pubkey::default())
                .map(|b| b.borrow_reserve)
        );
        obligation_reserve_keys.extend(
            obligation.deposits.iter()
                .filter(|d| d.deposit_reserve != Pubkey::default())
                .map(|d| d.deposit_reserve)
        );

        let mut to_dump_keys = Vec::new();
        to_dump_keys.extend(all_oracle_keys);
        to_dump_keys.push(Clock::id());
        to_dump_keys.push(*address);
        to_dump_keys.push(obligation.lending_market);
        to_dump_keys.extend(obligation_reserve_keys.clone());

        if let Err(e) = dump_accounts_to_file(
            &mut extra_client,
            &to_dump_keys,
            clock.slot,
            &obligation_reserve_keys,
            *address,
            obligation.clone()
        ).await {
            error!("Error dumping accounts to file: {}", e);
            return Err(e.into());
        }

        Ok(())
    }

    pub async fn liquidate_in_loop(&mut self, klend_client: &Arc<KlendClient>, scope: String) -> Result<()> {
        let start = std::time::Instant::now();

        let mut obligations_map = self.load_obligations_map(scope).await?;
        let total_liquidatable_obligations = self.calculate_total_obligations(&obligations_map);

        self.update_clock(klend_client, &start).await?;

        for (market, liquidatable_obligations) in obligations_map.iter_mut() {
            self.process_market_liquidations(klend_client, *market, liquidatable_obligations).await;
        }

        let en = start.elapsed().as_secs_f64();
        info!("[Liquidation Thread] Scanned {} obligations in {}s", total_liquidatable_obligations, en);

        Ok(())
    }

    async fn load_obligations_map(&self, scope: String) -> Result<HashMap<Pubkey, Vec<Pubkey>>> {
        load_obligations_map(scope.clone()).await.map_err(|e| anyhow::anyhow!("Failed to load obligations map: {:?}", e))
    }

    fn calculate_total_obligations(&self, obligations_map: &HashMap<Pubkey, Vec<Pubkey>>) -> usize {
        obligations_map.values().map(|v| v.len()).sum()
    }

    async fn update_clock(&mut self, klend_client: &Arc<KlendClient>, start: &std::time::Instant) -> Result<()> {
        self.clock = Some(match sysvars::get_clock(&klend_client.local_client.client).await {
            Ok(clock) => clock,
            Err(e) => {
                error!("Error getting clock: {}", e);
                return Err(e.into());
            }
        });

        let en_clock = start.elapsed().as_secs_f64();
        debug!("Refreshing market clock time used: {}s", en_clock);
        Ok(())
    }

    async fn process_market_liquidations(
        &mut self,
        klend_client: &Arc<KlendClient>,
        market_pubkey: Pubkey,
        liquidatable_obligations: &mut Vec<Pubkey>
    ) {
        info!("[Liquidation Thread]{}: {} liquidatable obligations found", market_pubkey.to_string().green(), liquidatable_obligations.len());

        if let Err(e) = self.ensure_market_accounts_loaded(klend_client, &market_pubkey).await {
            error!("[Liquidation Thread] Error loading market accounts and rts {}: {}", market_pubkey, e);
            return;
        }

        if let Err(e) = self.refresh_market_data(klend_client, &market_pubkey).await {
            error!("[Liquidation Thread] Error refreshing market {}: {}", market_pubkey, e);
            return;
        }

        self.scan_market_obligations(klend_client, &market_pubkey, liquidatable_obligations).await;
    }

    async fn ensure_market_accounts_loaded(&mut self, klend_client: &Arc<KlendClient>, market_pubkey: &Pubkey) -> Result<()> {
        if !self.market_accounts_map.contains_key(market_pubkey) {
            let (market_accounts, rts) = self.load_market_accounts_and_rts(klend_client, market_pubkey).await?;
            self.market_accounts_map.insert(*market_pubkey, (market_accounts.reserves, market_accounts.lending_market, rts));
        }
        Ok(())
    }

    async fn refresh_market_data(&mut self, klend_client: &Arc<KlendClient>, market_pubkey: &Pubkey) -> Result<()> {
        let refresh_start = std::time::Instant::now();

        let obligation_reservers_to_refresh = self.obligation_reservers_to_refresh.clone();
        let (reserves, lending_market, _rts) = self.market_accounts_map.get_mut(market_pubkey).unwrap();

        crate::refresh_market(klend_client,
            market_pubkey,
            &obligation_reservers_to_refresh,
            reserves,
            lending_market,
            self.clock.as_ref().unwrap(),
            None,
            None,
            None).await?;

        let refresh_en = refresh_start.elapsed().as_secs_f64();
        debug!("[Liquidation Thread] Refreshed market {} in {}s", market_pubkey.to_string().green(), refresh_en);
        Ok(())
    }

    async fn scan_market_obligations(&mut self, klend_client: &Arc<KlendClient>, market_pubkey: &Pubkey, liquidatable_obligations: &mut Vec<Pubkey>) {
        let (reserves, lending_market, rts) = self.market_accounts_map.get(market_pubkey).unwrap();
        let reserves = reserves.clone();
        let lending_market = lending_market.clone();
        let rts = rts.clone();
        self.scan_obligations(klend_client, liquidatable_obligations, &reserves, &lending_market, &rts, None).await;
    }

pub async fn preload_swap_instructions(&mut self, klend_client: &Arc<KlendClient>, obligation_key: &Pubkey, obligation: &Obligation) -> Result<()> {
        let debt_res_key = match math::find_best_debt_reserve(&obligation.borrows, &self.all_reserves) {
            Some(key) => key,
            None => {
                error!("No debt reserve found for obligation {}", obligation_key);
                return Err(anyhow::anyhow!("No debt reserve found for obligation"));
            }
        };
        let coll_res_key = match math::find_best_collateral_reserve(&obligation.deposits, &self.all_reserves) {
            Some(key) => key,
            None => {
                error!("No collateral reserve found for obligation {}", obligation_key);
                return Err(anyhow::anyhow!("No collateral reserve found for obligation"));
            }
        };
        let debt_reserve_state = match self.all_reserves.get(&debt_res_key) {    
            Some(reserve) => *reserve,
            None => {
                error!("Debt reserve {} not found in reserves", debt_res_key);
                return Err(anyhow::anyhow!("Debt reserve not found in reserves"));
            }
        };
        let coll_reserve_state = match self.all_reserves.get(&coll_res_key) {
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
                return Ok(());
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

            if !self.should_process_obligation(address, price_changed_reserves) {
                continue;
            }

            if let Err(e) = self.process_single_obligation(
                klend_client,
                address,
                reserves,
                lending_market,
                rts,
            ).await {
                error!("[Liquidation Thread] Error processing obligation {}: {}", address, e);
                continue;
            }

            checked_obligation_count += 1;
            let en = start.elapsed().as_secs_f64();
            debug!("[Liquidation Thread] Processed obligation time used: {} in {}s", address.to_string().green(), en);
        }

        self.sort_liquidatable_obligations(liquidatable_obligations);
        checked_obligation_count
    }

    fn should_process_obligation(
        &self,
        address: &Pubkey,
        price_changed_reserves: Option<&HashSet<Pubkey>>
    ) -> bool {
        if let Some(obligation) = self.obligation_map.get(address) {
            if let Some(price_changed_reserves) = price_changed_reserves {
                // Check if none of the reserves in the obligation are in the price_changed_reserves
                if obligation.deposits.iter().all(|coll| !price_changed_reserves.contains(&coll.deposit_reserve)) &&
                    obligation.borrows.iter().all(|borrow| !price_changed_reserves.contains(&borrow.borrow_reserve)) {
                    debug!("[Liquidation Thread] Obligation reserves not changed, skip: {} {}", address.to_string().green(), obligation.to_string().green());
                    return false;
                }
            }
            true
        } else {
            true
        }
    }

    async fn process_single_obligation(
        &mut self,
        klend_client: &Arc<KlendClient>,
        address: &Pubkey,
        reserves: &HashMap<Pubkey, Reserve>,
        lending_market: &LendingMarket,
        rts: &HashMap<Pubkey, ReferrerTokenState>,
    ) -> Result<()> {
        if let Some(mut obligation) = self.obligation_map.remove(address) {
            self.check_and_liquidate(klend_client, address, &mut obligation, lending_market, reserves, rts).await?;
            self.obligation_map.insert(*address, obligation);
        } else {
            let mut obligation = klend_client.fetch_obligation(address).await?;
            self.add_obligation_reserves_to_refresh(&obligation);

            if let Err(e) = self.preload_swap_instructions(klend_client, address, &obligation).await {
                error!("[Liquidation Thread] Error preloading swap instructions for obligation {}: {}", address, e);
            }

            self.check_and_liquidate(klend_client, address, &mut obligation, lending_market, reserves, rts).await?;
            self.obligation_map.insert(*address, obligation);
        }
        Ok(())
    }

    fn add_obligation_reserves_to_refresh(&mut self, obligation: &Obligation) {
        self.obligation_reservers_to_refresh.extend(
            obligation.deposits.iter().map(|coll| coll.deposit_reserve)
        );
        self.obligation_reservers_to_refresh.extend(
            obligation.borrows.iter().map(|borrow| borrow.borrow_reserve)
        );
    }

    fn sort_liquidatable_obligations(&self, liquidatable_obligations: &mut Vec<Pubkey>) {
        // Sort by borrow_factor_adjusted_debt_value_sf/unhealthy_borrow_value_sf ratio, higher values first
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

            // Add zero division check
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

        self.log_sorted_obligations(liquidatable_obligations);
    }

    fn log_sorted_obligations(&self, liquidatable_obligations: &[Pubkey]) {
        info!("sorted liquidatable_obligations: {:?}", liquidatable_obligations.iter().map(|obligation_key| {
            let obligation = self.obligation_map.get(obligation_key);
            match obligation {
                Some(obligation) => {
                    let obligation_stats = math::obligation_info(obligation_key, obligation);

                    // Add zero division check
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
        self.initialize_clock_if_needed(klend_client).await?;

        let (accounts, transactions) = self.prepare_subscription_filters().await?;
        let (mut subscribe_tx, mut stream) = self.establish_websocket_connection(accounts, transactions).await?;

        self.process_websocket_message_stream(klend_client, scope, &mut subscribe_tx, &mut stream).await
    }

    async fn initialize_clock_if_needed(&mut self, klend_client: &Arc<KlendClient>) -> anyhow::Result<()> {
        if self.clock.is_none() {
            self.clock = Some(match sysvars::clock(&klend_client.local_client.client).await {
                Ok(clock) => clock,
                Err(_e) => {
                    error!("Failed to get clock");
                    return Err(anyhow::Error::msg("Failed to get clock"));
                }
            });
        }
        Ok(())
    }

    async fn prepare_subscription_filters(&self) -> anyhow::Result<(
        HashMap<String, SubscribeRequestFilterAccounts>,
        HashMap<String, SubscribeRequestFilterTransactions>
    )> {
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

        Ok((accounts, transactions))
    }

    async fn establish_websocket_connection(
        &self,
        accounts: HashMap<String, SubscribeRequestFilterAccounts>,
        transactions: HashMap<String, SubscribeRequestFilterTransactions>
    ) -> anyhow::Result<(
        impl futures::SinkExt<SubscribeRequest> + Send + Unpin,
        impl futures::StreamExt<Item = Result<SubscribeUpdate, tonic::Status>> + Send + Unpin
    )> {
        let endpoint = env::var("YELLOWSTONE_RPC")?.to_string();
        let x_token = None;

        let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;
        let (mut subscribe_tx, stream) = client.subscribe().await.map_err(|e| {
            anyhow::Error::msg(format!(
                "Failed to subscribe: {} ({})",
                endpoint, e
            ))
        })?;
        info!("Connected to the gRPC server");

        let mut request_filter = HashMap::new();
        request_filter.insert(
            "client".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        );

        let blocks_meta = request_filter.clone();

        subscribe_tx
            .send(SubscribeRequest {
                slots: HashMap::new(),
                accounts,
                transactions,
                blocks: HashMap::new(),
                blocks_meta,
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

        Ok((subscribe_tx, stream))
    }

    async fn process_websocket_message_stream<T>(
        &mut self,
        klend_client: &Arc<KlendClient>,
        scope: String,
        subscribe_tx: &mut T,
        stream: &mut (impl futures::StreamExt<Item = Result<SubscribeUpdate, tonic::Status>> + Send + Unpin)
    ) -> anyhow::Result<()>
    where
        T: futures::SinkExt<SubscribeRequest> + Send + Unpin,
    {
        while let Some(message) = stream.next().await {
            if let Ok(msg) = message {
                match msg.update_oneof {
                    Some(UpdateOneof::Account(account)) => {
                        self.handle_account_update(klend_client, &scope, subscribe_tx, account).await?;
                    }
                    Some(UpdateOneof::Transaction(transaction)) => {
                        self.handle_transaction_update(transaction).await?;
                    }
                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                        self.handle_block_meta_update(block_meta).await?;
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

    async fn handle_block_meta_update(
        &mut self,
        block_meta: yellowstone_grpc_proto::prelude::SubscribeUpdateBlockMeta
    ) -> anyhow::Result<()> {
        debug!("Block meta: {:?}", block_meta);
        //save to class buffer
        //covert string to Hash
        self.latest_blockhash = Some(Hash::from_str(&block_meta.blockhash)?);
        self.latest_slot = Some(block_meta.slot);
        Ok(())
    }

    async fn handle_account_update<T>(
        &mut self,
        klend_client: &Arc<KlendClient>,
        scope: &str,
        subscribe_tx: &mut T,
        account_update: yellowstone_grpc_proto::prelude::SubscribeUpdateAccount
    ) -> anyhow::Result<()>
        where
        T: futures::SinkExt<SubscribeRequest> + Unpin,
        {
        if let Some(account) = account_update.account {
            let pubkey = Pubkey::try_from(account.pubkey.as_slice()).unwrap();
            let data = account.data;

            if self.handle_clock_update(&pubkey, &data).await? {
                return Ok(());
            }

            if self.handle_obligation_update(&pubkey, &data, klend_client).await? {
                return Ok(());
            }

            if let Some(price_changed_reserves) = self.handle_price_update(&pubkey, &data).await? {
                self.process_price_changes(
                    klend_client,
                    &price_changed_reserves,
                    &pubkey,
                    &data,
                ).await?;

                // Refresh obligations map and update subscription if needed
                self.refresh_obligations_subscription(scope, subscribe_tx).await?;
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
        let all_obligations_pubkeys: Vec<Pubkey> = self.market_obligations_map.values().flatten().copied().collect::<Vec<Pubkey>>();

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
            if let Err(e) = self.preload_swap_instructions(klend_client, pubkey, &obligation).await {
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
        let market_pubkeys = self.market_pubkeys.clone();

        for market_pubkey in &market_pubkeys {
            self.process_single_market_price_change(
                klend_client,
                market_pubkey,
                price_changed_reserves,
                updated_account_pubkey,
                updated_account_data,
            ).await;
        }

        Ok(())
    }

    async fn process_single_market_price_change(
        &mut self,
        klend_client: &Arc<KlendClient>,
        market_pubkey: &Pubkey,
        price_changed_reserves: &HashSet<Pubkey>,
        updated_account_pubkey: &Pubkey,
        updated_account_data: &Vec<u8>,
    ) {
        let start = std::time::Instant::now();

        self.refresh_market_with_price_update(
            klend_client, market_pubkey, updated_account_pubkey, updated_account_data
        ).await;

        let obligations_count = self.get_market_obligations_count(market_pubkey);
        if obligations_count == 0 {
            info!("No obligations found for market: {:?}", market_pubkey);
            return;
        }

        let checked_obligation_count = self.scan_market_obligations_with_price_changes(
            klend_client, market_pubkey, price_changed_reserves
        ).await;

        let duration = start.elapsed();
        info!("Scan {} obligations, time used: {:?} s, checked {} obligations",
              obligations_count, duration, checked_obligation_count);
    }

    async fn refresh_market_with_price_update(
        &mut self,
        klend_client: &Arc<KlendClient>,
        market_pubkey: &Pubkey,
        updated_account_pubkey: &Pubkey,
        updated_account_data: &Vec<u8>,
    ) {
        let clock = self.clock.as_ref().unwrap().clone();
        let updated_accounts = HashMap::from([(*updated_account_pubkey, updated_account_data.clone())]);

        let _ = crate::refresh_market(
            klend_client,
            market_pubkey,
            &Vec::new(),
            &mut self.all_reserves,
            self.all_lending_market.get_mut(market_pubkey).unwrap(),
            &clock,
            Some(&mut self.all_scope_price_accounts),
            Some(&mut self.all_switchboard_accounts),
            Some(&updated_accounts)
        ).await;
    }

    fn get_market_obligations_count(&self, market_pubkey: &Pubkey) -> usize {
        self.market_obligations_map.get(market_pubkey)
            .map(|obligations| obligations.len())
            .unwrap_or(0)
    }

    async fn scan_market_obligations_with_price_changes(
        &mut self,
        klend_client: &Arc<KlendClient>,
        market_pubkey: &Pubkey,
        price_changed_reserves: &HashSet<Pubkey>,
    ) -> u32 {
        // Clone all necessary data to avoid borrow conflicts
        let reserves_clone = self.all_reserves.clone();
        let lending_market = self.all_lending_market.get(market_pubkey).cloned().unwrap();
        let rts = self.all_rts.get(market_pubkey).cloned().unwrap();

        // Scan obligations - need to extract obligations first to avoid borrow conflicts
        if let Some(obligations) = self.market_obligations_map.remove(market_pubkey) {
            let mut mutable_obligations = obligations;
            let count = self.scan_obligations(
                klend_client,
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
        }
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

                    if let Err(_e) = subscribe_tx.send(
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
                        error!("Failed to subscribe to updated obligations");
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