use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Write},
    str::FromStr,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

type TransactionResult = std::result::Result<(), TransactionError>;

use anchor_client::{
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    },
    solana_sdk::pubkey::Pubkey,
};
use solana_sdk::account::Account;
use anyhow::{anyhow, Result};
use kamino_lending::{
    utils::seeds, LendingMarket, Obligation, ReferrerTokenState, Reserve, ReserveFarmKind,
};
use orbit_link::OrbitLink;
use solana_account_decoder::parse_address_lookup_table::UiLookupTable;
use solana_sdk::{
    address_lookup_table::{instruction, state::AddressLookupTable, AddressLookupTableAccount},
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    signature::{Keypair, Signature},
    transaction::{TransactionError, VersionedTransaction},
    sysvar::{clock::Clock, SysvarId},
};
use tracing::info;
use anchor_lang::AccountDeserialize;

use crate::{
    accounts::{find_account, load_accounts_from_file, market_and_reserve_accounts, map_accounts_and_create_infos, MarketAccounts, refresh_oracle_keys},
    consts::{NULL_PUBKEY, WRAPPED_SOL_MINT},
    instructions::{self, InstructionBlocks},
    liquidator::Liquidator,
    lookup_tables::collect_keys,
    model::StateWithKey,
    operations::{refresh_reserve, obligation_reserves, ObligationReserves, referrer_token_states_of_obligation},
    px,
    px::Prices,
};
use extra_proto::extra_client::ExtraClient;
use yellowstone_grpc_proto::tonic;

pub struct KlendClient {
    pub program_id: Pubkey,

    pub client: OrbitLink<RpcClient, Keypair>,

    pub local_client: OrbitLink<RpcClient, Keypair>,

    pub extra_client: ExtraClient<tonic::transport::Channel>,

    // Txn data
    pub lookup_table: Option<AddressLookupTableAccount>,

    pub custom_lookup_table: RwLock<Option<AddressLookupTableAccount>>,

    pub ata_lookup_table: RwLock<Option<AddressLookupTableAccount>>,

    // Rebalance settings
    pub rebalance_config: Option<RebalanceConfig>,

    // Liquidator
    // TODO: move all the fields of the liquidator out of this struct and flatten it
    pub liquidator: Liquidator,
}

#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    pub base_token: Pubkey,
    pub min_sol_balance: f64,
    pub usdc_mint: Pubkey,
    pub rebalance_slippage_pct: f64,
    pub non_swappable_dust_usd_value: f64,
}

impl KlendClient {
    pub fn init(
        client: OrbitLink<RpcClient, Keypair>,
        local_client: OrbitLink<RpcClient, Keypair>,
        extra_client: ExtraClient<tonic::transport::Channel>,
        program_id: Pubkey,
        rebalance_config: Option<RebalanceConfig>,
    ) -> Result<Self> {
        // Create a dumb one first
        let liquidator = Liquidator {
            wallet: Arc::new(Keypair::new()),
            atas: HashMap::new(),
        };

        Ok(Self {
            program_id,
            client,
            local_client,
            extra_client,
            lookup_table: None,
            custom_lookup_table: RwLock::new(None),
            ata_lookup_table: RwLock::new(None),
            liquidator,
            rebalance_config,
        })
    }

    pub async fn fetch_market_and_reserves(&self, market: &Pubkey) -> Result<MarketAccounts> {
        market_and_reserve_accounts(self, market).await
    }

    pub async fn fetch_obligations(&self, market: &Pubkey) -> Result<Vec<(Pubkey, Obligation)>> {
        info!("Fetching obligations for market: {}", market);
        let filter = RpcFilterType::Memcmp(Memcmp::new(
            32,
            MemcmpEncodedBytes::Bytes(market.to_bytes().to_vec()),
        ));
        let filters = vec![filter];
        let obligations = rpc::get_zero_copy_pa(&self.client, &self.program_id, &filters).await?;
        Ok(obligations)
    }

    pub async fn fetch_obligation(&self, obligation_address: &Pubkey) -> Result<Obligation> {
        info!("Fetching obligation: {}", obligation_address);
        let obligation = self
            .local_client
            .get_anchor_account::<Obligation>(obligation_address)
            .await?;
        Ok(obligation)
    }

    pub async fn fetch_obligations_by_pubkey(&self, pubkeys: &[Pubkey]) -> Result<HashMap<Pubkey, Obligation>> {
        info!("Fetching obligations: {:?}", pubkeys);
        // 按照最多100个一组进行分组
        let chunks = pubkeys.chunks(100);
        let mut obligations_map = HashMap::new();
        for chunk in chunks {
            let obligations = self
                .local_client
                .get_anchor_accounts::<Obligation>(chunk)
                .await?;
            for (i, pubkey) in chunk.iter().enumerate() {
                if let Some(obligation) = obligations[i] {
                    obligations_map.insert(*pubkey, obligation);
                }
            }
        }
        Ok(obligations_map)
    }

    pub async fn fetch_referrer_token_states(&self) -> Result<HashMap<Pubkey, ReferrerTokenState>> {
        let states = self
            .client
            .get_all_zero_copy_accounts::<ReferrerTokenState>()
            .await?;
        let map = states.into_iter().collect();
        Ok(map)
    }

    pub async fn check_and_add_to_ata_lookup_table(&self, account_key: Pubkey) -> Result<()> {
        let ata_lookup_table = {
            let guard = self.ata_lookup_table.read().unwrap();
            guard.clone().unwrap()
        };

        if ata_lookup_table.addresses.contains(&account_key) {
            //info!("Account already in ata lookup table: {:?}", account_key);
            return Ok(());
        }

        info!("Adding account to ata lookup table: {:?}", account_key);

        let mut updated_ata_lookup_table = ata_lookup_table.clone();
        updated_ata_lookup_table.addresses.push(account_key);

        self.send_extend_lookup_table_transaction(updated_ata_lookup_table.key, account_key).await?;

        // Update the in-memory lookup table after successful transaction
        {
            let mut guard = self.ata_lookup_table.write().unwrap();
            *guard = Some(updated_ata_lookup_table);
        }

        info!("Added account to ata lookup table: {:?}", account_key);
        Ok(())
    }

    pub async fn check_and_add_to_custom_lookup_table(&self, account_key: Pubkey) -> Result<()> {

        if let Some(ref lookup_table) = self.lookup_table {
            if lookup_table.addresses.contains(&account_key) {
                //info!("Account already in lookup table: {:?}", account_key);
                return Ok(());
            }
        }

        let custom_lookup_table = {
            let guard = self.custom_lookup_table.read().unwrap();
            guard.clone().unwrap()
        };

        if custom_lookup_table.addresses.contains(&account_key) {
            //info!("Account already in custom lookup table: {:?}", account_key);
            return Ok(());
        }

        info!("Adding account to custom lookup table: {:?}", account_key);

        //add account to custom lookup table
        let mut updated_custom_lookup_table = custom_lookup_table.clone();
        updated_custom_lookup_table.addresses.push(account_key);

        self.send_extend_lookup_table_transaction(updated_custom_lookup_table.key, account_key).await?;


        // Update the in-memory lookup table after successful transaction
        {
            let mut guard = self.custom_lookup_table.write().unwrap();
            *guard = Some(updated_custom_lookup_table);
        }

        info!("Added account to custom lookup table: {:?}", account_key);
        Ok(())
    }

    pub async fn send_extend_lookup_table_transaction(&self, lookup_table_key: Pubkey, account_key: Pubkey) -> Result<()> {
        let tx = self.local_client.tx_builder().add_ix(instruction::extend_lookup_table(
            lookup_table_key,
            self.client.payer_pubkey(),
            Some(self.client.payer_pubkey()),
            vec![account_key],
        )).build(&[]).await?;

        let (sig, _) = self.local_client
            .send_retry_and_confirm_transaction(tx, None, false)
            .await?;

        info!("Sent extend lookup table transaction: {:?}, signature: {:?}", account_key, sig);
        Ok(())
    }


    pub async fn load_lookup_table(&mut self, market_accounts: MarketAccounts) {
        self.load_liquidator_lookup_table().await;
        self.update_liquidator_lookup_table(collect_keys(
            &market_accounts.reserves,
            &self.liquidator,
            &market_accounts.lending_market,
        ))
        .await;
        self.client
            .add_lookup_table(self.lookup_table.clone().unwrap());
    }

    pub async fn load_default_lookup_table(&mut self) {
        self.load_liquidator_lookup_table().await;
        self.load_custom_lookup_table().await;
        self.load_ata_lookup_table().await;
        self.client
            .add_lookup_table(self.lookup_table.clone().unwrap());
        self.local_client
            .add_lookup_table(self.lookup_table.clone().unwrap());
    }

    async fn load_liquidator_lookup_table(&mut self) {
        // The liquidator has one static lookup table associated with it
        // and is stored on a local file
        // Here we load it or create it and save it
        // we do not manage the addresses, that is done in a separate stage
        let filename = std::env::var("LIQUIDATOR_LOOKUP_TABLE_FILE").unwrap();
        self.lookup_table = self.load_lookup_table_from_file(&filename).await.unwrap();
    }

    async fn load_custom_lookup_table(&self) {
        let filename = std::env::var("CUSTOM_LOOKUP_TABLE_FILE").unwrap();
        let lookup_table = self.load_lookup_table_from_file(&filename).await.unwrap();
        {
            let mut guard = self.custom_lookup_table.write().unwrap();
            *guard = lookup_table;
        }
    }

    async fn load_ata_lookup_table(&self) {
        let filename = std::env::var("ATA_LOOKUP_TABLE_FILE").unwrap();
        let lookup_table = self.load_lookup_table_from_file(&filename).await.unwrap();
        {
            let mut guard = self.ata_lookup_table.write().unwrap();
            *guard = lookup_table;
        }
    }


    async fn load_lookup_table_from_file(&self, filename: &str) -> Result<Option<AddressLookupTableAccount>> {

        if !std::path::Path::new(&filename).exists() {
            File::create(&filename).unwrap();
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&filename)
            .unwrap();
        let mut current_content = String::new();
        file.read_to_string(&mut current_content).unwrap();

        let lookup_table = if current_content.is_empty() {
            let lut = self
                .create_init_reserve_lookup_table(&[], || {
                    thread::sleep(Duration::from_secs(12));
                })
                .await
                .unwrap();
            file.set_len(0).unwrap();
            file.write_all(lut.key.to_string().as_bytes()).unwrap();
            info!("Created new empty lookup table {}", lut.key);
            Some(lut)
        } else {
            let lut_key = Pubkey::from_str(&current_content).unwrap();
            info!("Liquidator lookuptable {:?}", lut_key);
            let lookup_table_data = self.client.client.get_account(&lut_key).await.unwrap();
            let ui_lookup_table: UiLookupTable = UiLookupTable::from(
                AddressLookupTable::deserialize(&lookup_table_data.data).unwrap(),
            );
            let lookup_table = AddressLookupTableAccount {
                key: lut_key,
                addresses: ui_lookup_table
                    .addresses
                    .iter()
                    .map(|x| Pubkey::from_str(x).unwrap())
                    .collect::<Vec<Pubkey>>(),
            };
            info!(
                "Loaded lookup table {} with {} keys",
                lut_key,
                ui_lookup_table.addresses.len()
            );
            Some(lookup_table)
        };

        Ok(lookup_table)
    }

    async fn update_liquidator_lookup_table(&mut self, expected: HashSet<Pubkey>) {
        if self.lookup_table.is_none() {
            self.load_liquidator_lookup_table().await;
        }

        // TODO: Maybe sleep
        let lut = self.lookup_table.as_ref().unwrap();
        let already_in_lut: HashSet<Pubkey> = HashSet::from_iter(lut.addresses.iter().copied());
        let expected_in_lut: HashSet<Pubkey> = expected;

        let missing_keys = expected_in_lut
            .iter()
            .filter(|x| !already_in_lut.contains(x))
            .copied()
            .collect::<Vec<Pubkey>>();

        let extra_keys = lut
            .addresses
            .iter()
            .filter(|x| !expected_in_lut.contains(*x))
            .copied()
            .collect::<Vec<Pubkey>>();

        info!("Missing keys: {:?}", missing_keys.len());
        info!("Extra keys: {:?}", extra_keys.len());

        if !missing_keys.is_empty() {
            info!("Extending lookup table");
            self.extend_lut_with_keys(lut.key, &missing_keys, || {
                thread::sleep(Duration::from_secs(12));
            })
            .await
            .unwrap();

            // Reload it
            self.load_liquidator_lookup_table().await;
        }
    }

    async fn create_init_reserve_lookup_table(
        &self,
        keys: &[Pubkey],
        delay_fn: impl Fn(),
    ) -> Result<AddressLookupTableAccount> {


        // Create lookup table
        let recent_slot = self
            .client
            .client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;

        let (create_lookup_table, table_pk) = instruction::create_lookup_table(
            self.client.payer_pubkey(),
            self.client.payer_pubkey(),
            recent_slot,
        );

        let txn = self.client.create_tx(&[create_lookup_table], &[]).await?;

        self.client
            .send_retry_and_confirm_transaction(txn, None, false)
            .await?;

        let keys = keys
            .iter()
            .filter(|x| **x != NULL_PUBKEY)
            .copied()
            .collect::<Vec<Pubkey>>();

        self.extend_lut_with_keys(table_pk, &keys, delay_fn).await?;

        Ok(AddressLookupTableAccount {
            key: table_pk,
            addresses: keys,
        })
    }

    async fn extend_lut_with_keys(
        &self,
        table_pk: Pubkey,
        keys: &[Pubkey],
        delay_fn: impl Fn(),
    ) -> Result<()> {

        for selected_keys in keys.chunks(20) {
            info!("Extending lookup table with {} keys", selected_keys.len());
            let extend_ix = instruction::extend_lookup_table(
                table_pk,
                self.client.payer_pubkey(),
                Some(self.client.payer_pubkey()),
                selected_keys.to_vec(),
            );

            let tx = self.client.create_tx(&[extend_ix], &[]).await?;
            self.send_and_confirm_transaction(tx).await.unwrap();
            // wait until lookup table is active
            delay_fn();
        }

        Ok(())
    }

    // TODO: move this to orbitlink
    pub async fn send_and_confirm_transaction(
        &self,
        tx: VersionedTransaction,
    ) -> Result<(Signature, Option<TransactionResult>)> {
        let mut num_retries = 0;
        let max_retries = 5;
        loop {
            num_retries += 1;
            if num_retries > max_retries {
                return Err(anyhow!("Max retries reached"));
            }
            let (sig, res) = self
                .local_client
                .send_retry_and_confirm_transaction(tx.clone(), None, false)
                .await?;
            if let Some(Err(TransactionError::BlockhashNotFound)) = res {
                continue;
            } else {
                return Ok((sig, res));
            }
        }
    }

    pub async fn fetch_all_prices(
        &mut self,
        reserves: &[Reserve],
        usd_mint: &Pubkey,
    ) -> Result<Prices> {
        let mut mints = reserves
            .iter()
            .map(|x| x.liquidity.mint_pubkey)
            .collect::<HashSet<Pubkey>>();

        if let Some(c) = &self.rebalance_config {
            mints.insert(c.base_token);
            mints.insert(c.usdc_mint);
        };
        mints.insert(WRAPPED_SOL_MINT);

        // Convert mints to vec
        let mints = mints.into_iter().collect::<Vec<Pubkey>>();

        // TOOD: fix amount to be per token
        let amount = 100.0;
        px::fetch_jup_prices(&mints, usd_mint, amount).await
    }

    pub async fn flash_borrow_reserve_liquidity_ixns(
        &self,
        reserve: &StateWithKey<Reserve>,
        obligation: &Pubkey,
        liquidity_amount: u64,
    ) -> Result<Vec<Instruction>> {
        let flash_borrow_ix = instructions::flash_borrow_reserve_liquidity_ix(
            &self.program_id,
            reserve,
            obligation,
            liquidity_amount,
            &self.liquidator,
        );

        Ok(vec![flash_borrow_ix.instruction])
    }

    pub async fn flash_repay_reserve_liquidity_ixns(
        &self,
        reserve: &StateWithKey<Reserve>,
        obligation: &Pubkey,
        liquidity_amount: u64,
        borrow_instruction_index: u8,
    ) -> Result<Vec<Instruction>> {
        let flash_repay_ix = instructions::flash_repay_reserve_liquidity_ix(
            &self.program_id,
            reserve,
            obligation,
            liquidity_amount,
            borrow_instruction_index,
            &self.liquidator,
        );

        Ok(vec![flash_repay_ix.instruction])
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn liquidate_obligation_and_redeem_reserve_collateral_ixns(
        &self,
        lending_market: StateWithKey<LendingMarket>,
        debt_reserve: StateWithKey<Reserve>,
        coll_reserve: StateWithKey<Reserve>,
        obligation: StateWithKey<Obligation>,
        liquidity_amount: u64,
        min_acceptable_received_coll_amount: u64,
        max_allowed_ltv_override_pct_opt: Option<u64>,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> Result<Vec<Instruction>> {
        let liquidate_ix = instructions::liquidate_obligation_and_redeem_reserve_collateral_ix(
            &self.program_id,
            lending_market,
            debt_reserve.clone(),
            coll_reserve.clone(),
            &self.liquidator,
            obligation.key,
            liquidity_amount,
            min_acceptable_received_coll_amount,
            max_allowed_ltv_override_pct_opt,
        );

        let (pre_instructions, post_instructions) = self
            .wrap_obligation_instruction_with_farms(
                &[&coll_reserve, &debt_reserve],
                &[ReserveFarmKind::Collateral, ReserveFarmKind::Debt],
                &obligation,
                &self.liquidator.wallet.clone(),
                reserves,
            )
            .await;

        let mut instructions = vec![];
        for ix in pre_instructions {
            instructions.push(ix.instruction);
        }
        instructions.push(liquidate_ix.instruction);
        for ix in post_instructions {
            instructions.push(ix.instruction);
        }

        Ok(instructions)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn liquidate_obligation_and_redeem_reserve_collateral(
        &mut self,
        lending_market: StateWithKey<LendingMarket>,
        debt_reserve: StateWithKey<Reserve>,
        coll_reserve: StateWithKey<Reserve>,
        obligation: StateWithKey<Obligation>,
        liquidity_amount: u64,
        min_acceptable_received_coll_amount: u64,
        max_allowed_ltv_override_pct_opt: Option<u64>,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> Result<VersionedTransaction> {
        let instructions = self
            .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market,
                debt_reserve,
                coll_reserve,
                obligation,
                liquidity_amount,
                min_acceptable_received_coll_amount,
                max_allowed_ltv_override_pct_opt,
                reserves,
            )
            .await?;

        let txn = self.client.tx_builder().add_ixs(instructions);
        let txn_b64 = txn.to_base64();
        println!(
            "Simulation: https://explorer.solana.com/tx/inspector?message={}",
            urlencoding::encode(&txn_b64)
        );

        txn.build_with_budget_and_fee(&[], None).await.map_err(Into::into)
    }

    pub async fn wrap_obligation_instruction_with_farms(
        &self,
        reserve_accts: &[&StateWithKey<Reserve>],
        farm_modes: &[ReserveFarmKind],
        obligation: &StateWithKey<Obligation>,
        payer: &Arc<Keypair>,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> (Vec<InstructionBlocks>, Vec<InstructionBlocks>) {
        // If has farms, also do init farm obligations
        // Always do refresh_reserve
        // Always do refresh_obligation
        // If has farms, also do refresh farms
        // Then this ix
        // If has farms, also do refresh farms

        let mut pre_instructions = vec![];
        let mut post_instructions = vec![];

        let obligation_state = *(obligation.state.borrow());
        let obligation_address = obligation.key;

        let (deposit_reserves, borrow_reserves, referrer_token_states) = self
            .get_obligation_reserves_and_referrer_token_states(&obligation_state, reserves)
            .await;

        let mut unique_reserves = deposit_reserves
            .iter()
            .chain(borrow_reserves.iter())
            .filter_map(|x| *x)
            .collect::<Vec<Pubkey>>();
        unique_reserves.sort();
        unique_reserves.dedup();

        let instruction_reserves = reserve_accts.iter().map(|x| x.key).collect::<Vec<Pubkey>>();

        // 1. Build init_obligation_farm if necessary
        /*for reserve in reserve_accts {
            let (farm_debt, farm_collateral) = {
                let reserve_state = reserve.state.borrow();
                (
                    reserve_state.get_farm(ReserveFarmKind::Debt),
                    reserve_state.get_farm(ReserveFarmKind::Collateral),
                )
            };
            let (obligation_farm_debt, obligation_farm_coll) =
                obligation_farms(&self.local_client, farm_debt, farm_collateral, obligation_address)
                    .await;

            if farm_debt != Pubkey::default() && obligation_farm_debt.is_none() {
                let init_obligation_farm_ix = instructions::init_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve,
                    farm_debt,
                    &obligation_address,
                    &obligation_state.owner,
                    payer,
                    ReserveFarmKind::Debt,
                );
                info!(
                    "Adding pre-ixn init_obligation_farm_ix current {:?} debt ",
                    farm_debt
                );
                pre_instructions.push(init_obligation_farm_ix.clone());
            }

            if farm_collateral != Pubkey::default() && obligation_farm_coll.is_none() {
                let init_obligation_farm_ix = instructions::init_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve,
                    farm_collateral,
                    &obligation_address,
                    &obligation_state.owner,
                    payer,
                    ReserveFarmKind::Collateral,
                );
                info!(
                    "Adding pre-ixn init_obligation_farm_ix current {:?} coll",
                    farm_collateral
                );
                pre_instructions.push(init_obligation_farm_ix.clone());
            }
        }*/

        // 2. Build Refresh Reserve (for the non-instruction reserves - i.e. deposit, borrow)
        for reserve_acc in unique_reserves {
            if instruction_reserves.contains(&reserve_acc) {
                continue;
            }
            let reserve: Reserve = *reserves.get(&reserve_acc).unwrap();
            let refresh_reserve_ix = instructions::refresh_reserve_ix(
                &self.program_id,
                reserve,
                &reserve_acc,
                payer.clone(),
            );
            info!("Adding pre-ixn refresh_reserve unique {:?}", reserve_acc);
            pre_instructions.push(refresh_reserve_ix);
        }

        // 3. Build Refresh Reserve (for the current instruction - i.e. deposit, borrow)
        for reserve_acc in instruction_reserves {
            let reserve: Reserve = *reserves.get(&reserve_acc).unwrap();
            let refresh_reserve_ix = instructions::refresh_reserve_ix(
                &self.program_id,
                reserve,
                &reserve_acc,
                payer.clone(),
            );
            info!("Adding pre-ixn refresh_reserve current {:?}", reserve_acc);
            pre_instructions.push(refresh_reserve_ix);
        }

        // 4. Build Refresh Obligation
        let refresh_obligation_ix = instructions::refresh_obligation_ix(
            &self.program_id,
            obligation_state.lending_market,
            obligation_address,
            deposit_reserves,
            borrow_reserves,
            referrer_token_states,
            payer.clone(),
        );

        info!("Adding pre-ixn refresh_obligation");
        pre_instructions.push(refresh_obligation_ix);

        for (reserve_acc, farm_mode) in reserve_accts.iter().zip(farm_modes.iter()) {
            let reserve: Reserve = *reserves.get(&reserve_acc.key).unwrap();

            let farm = reserve.get_farm(*farm_mode);

            // 5.1 Build Refresh Obligation Farms
            if farm != Pubkey::default() {
                let refresh_farms_ix = instructions::refresh_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve_acc,
                    farm,
                    obligation_address,
                    payer,
                    *farm_mode,
                );

                info!("pre_ixs refresh_obligation_farms {:?}", farm);

                pre_instructions.push(refresh_farms_ix.clone());
                post_instructions.push(refresh_farms_ix);
            }
        }

        (pre_instructions, post_instructions)
    }

    pub async fn get_obligation_reserves_and_referrer_token_states(
        &self,
        obligation: &Obligation,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> (
        Vec<Option<Pubkey>>,
        Vec<Option<Pubkey>>,
        Vec<Option<Pubkey>>,
    ) {
        let deposit_reserves: Vec<Option<Pubkey>> = obligation
            .deposits
            .iter()
            .filter(|x| x.deposit_reserve != Pubkey::default())
            .map(|x| Some(x.deposit_reserve))
            .collect();

        let borrow_reserves: Vec<Option<Pubkey>> = obligation
            .borrows
            .iter()
            .filter(|x| x.borrow_reserve != Pubkey::default())
            .map(|x| Some(x.borrow_reserve))
            .collect();

        let referrer_token_states: Vec<Option<Pubkey>> = if obligation.has_referrer() {
            let mut vec = Vec::with_capacity(borrow_reserves.len());

            for borrow_reserve in borrow_reserves.iter() {
                match borrow_reserve {
                    Some(borrow_reserve) => {
                        let reserve_account: Reserve = *reserves.get(borrow_reserve).unwrap();

                        vec.push(Some(get_referrer_token_state_key(
                            &obligation.referrer,
                            &reserve_account.liquidity.mint_pubkey,
                        )));
                    }
                    None => {}
                }
            }
            vec
        } else {
            Vec::new()
        };

        (deposit_reserves, borrow_reserves, referrer_token_states)
    }

    pub async fn load_data_from_file(&self, obligation: &Pubkey, slot: u64)
    -> anyhow::Result<(Obligation, Clock, HashMap<Pubkey, Reserve>, LendingMarket, HashMap<Pubkey, ReferrerTokenState>, Option<HashMap<Pubkey, Account>>)> {
        // load data from extra
        info!("Loading data from extra for obligation {:?}, slot {:?}", obligation, slot);

        let (accounts, reserve_pubkeys) = load_accounts_from_file(slot).await?;

        let obligation_account = accounts.get(obligation).unwrap();
        let mut obligation_state = Obligation::try_deserialize(&mut obligation_account.data.as_slice())?;
        let clock_account = accounts.get(&Clock::id()).unwrap();
        let clock = clock_account.deserialize_data::<Clock>()?;

        info!("Loaded clock data from extra for obligation {:?}, clock {:?}", obligation_state, clock);

        let market_account = accounts.get(&obligation_state.lending_market).unwrap();
        let market = LendingMarket::try_deserialize(&mut market_account.data.as_slice())?;

        info!("Loaded market data from extra for market {:?}", market);

        let mut reserves = HashMap::new();
        for reserve_pubkey in reserve_pubkeys {
            let reserve_account = accounts.get(&reserve_pubkey).unwrap();
            let reserve = Reserve::try_deserialize(&mut reserve_account.data.as_slice())?;
            reserves.insert(reserve_pubkey, reserve);
        }

        // todo - don't load all
        let rts = self.fetch_referrer_token_states().await?;

        let mut all_oracle_keys = HashSet::new();
        let mut pyth_keys = HashSet::new();
        let mut switchboard_keys = HashSet::new();
        let mut scope_keys = HashSet::new();

        refresh_oracle_keys(&reserves, &mut all_oracle_keys, &mut pyth_keys, &mut switchboard_keys, &mut scope_keys);

        let mut pyth_accounts = Vec::new();
        let mut switchboard_accounts = Vec::new();
        let mut scope_price_accounts = Vec::new();

        for pyth_key in pyth_keys {
            let pyth_account = accounts.get(&pyth_key).unwrap();
            pyth_accounts.push((pyth_key, false, pyth_account.clone()));
        }
        for switchboard_key in switchboard_keys {
            let switchboard_account = accounts.get(&switchboard_key).unwrap();
            switchboard_accounts.push((switchboard_key, false, switchboard_account.clone()));
        }
        for scope_key in scope_keys {
            let scope_account = accounts.get(&scope_key).unwrap();
            scope_price_accounts.push((scope_key, false, scope_account.clone()));
        }

        let pyth_account_infos = map_accounts_and_create_infos(&mut pyth_accounts);
        let switchboard_feed_infos = map_accounts_and_create_infos(&mut switchboard_accounts);
        let scope_price_infos = map_accounts_and_create_infos(&mut scope_price_accounts);

        let ObligationReserves {
            borrow_reserves,
            deposit_reserves,
        } = obligation_reserves(&obligation_state, &reserves)?;

        // Refresh reserves and obligation
        for reserve in borrow_reserves.iter() {
            refresh_reserve(
                &reserve.key,
                &mut reserve.state.borrow_mut(),
                &market,
                &clock,
                &pyth_account_infos,
                &switchboard_feed_infos,
                &scope_price_infos,
            )?;

            // 同步更新的数据回原始的 reserves HashMap
            reserves.insert(reserve.key, *reserve.state.borrow());

            //info!("Borrow reserve: {:?}, last_update: {:?}", reserve.state.borrow().config.token_info.symbol(), reserve.state.borrow().last_update);
        }

        for reserve in deposit_reserves.iter() {
            refresh_reserve(
                &reserve.key,
                &mut reserve.state.borrow_mut(),
                &market,
                &clock,
                &pyth_account_infos,
                &switchboard_feed_infos,
                &scope_price_infos,
            )?;

            // 同步更新的数据回原始的 reserves HashMap
            reserves.insert(reserve.key, *reserve.state.borrow());

            //info!("Deposit reserve: {:?}, last_update: {:?}", reserve.state.borrow().config.token_info.symbol(), reserve.state.borrow().last_update);
        }

        let referrer_states = referrer_token_states_of_obligation(
            obligation,
            &obligation_state,
            &borrow_reserves,
            &rts,
        )?;

        kamino_lending::lending_market::lending_operations::refresh_obligation(
            &obligation,
            &mut obligation_state,
            &market,
            clock.slot,
            kamino_lending::MaxReservesAsCollateralCheck::Perform,
            deposit_reserves.into_iter(),
            borrow_reserves.into_iter(),
            referrer_states.into_iter(),
        )?;

        Ok((obligation_state, clock, reserves, market, rts, Some(accounts)))
    }
}

pub fn get_referrer_token_state_key(referrer: &Pubkey, mint: &Pubkey) -> Pubkey {
    let (referrer_token_state_key, _referrer_token_state_bump) = Pubkey::find_program_address(
        &[
            seeds::BASE_SEED_REFERRER_TOKEN_STATE,
            referrer.as_ref(),
            mint.as_ref(),
        ],
        &kamino_lending::id(),
    );

    referrer_token_state_key
}

pub async fn obligation_farms(
    client: &OrbitLink<RpcClient, Keypair>,
    farm_debt: Pubkey,
    farm_collateral: Pubkey,
    obligation_address: Pubkey,
) -> (
    Option<StateWithKey<farms::state::UserState>>,
    Option<StateWithKey<farms::state::UserState>>,
) {
    let (obligation_farm_debt, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            farm_debt.as_ref(),
            obligation_address.as_ref(),
        ],
        &farms::ID,
    );
    let (obligation_farm_coll, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            farm_collateral.as_ref(),
            obligation_address.as_ref(),
        ],
        &farms::ID,
    );

    let obligation_farm_debt_account = if farm_debt == Pubkey::default() {
        None
    } else if let Ok(None) = find_account(&client.client, obligation_farm_debt).await {
        None
    } else {
        let acc = client
            .get_anchor_account::<farms::state::UserState>(&obligation_farm_debt)
            .await
            .unwrap();

        Some(StateWithKey::new(acc, obligation_farm_debt))
    };

    let obligation_farm_coll_account = if farm_collateral == Pubkey::default() {
        None
    } else if let Ok(None) = find_account(&client.client, obligation_farm_coll).await {
        None
    } else {
        let acc = client
            .get_anchor_account::<farms::state::UserState>(&obligation_farm_coll)
            .await
            .unwrap();

        Some(StateWithKey::new(acc, obligation_farm_coll))
    };

    (obligation_farm_debt_account, obligation_farm_coll_account)
}

pub mod utils {
    use super::*;

    pub async fn fetch_markets_and_reserves(
        client: &Arc<KlendClient>,
        markets: &[Pubkey],
    ) -> anyhow::Result<HashMap<Pubkey, MarketAccounts>> {
        let futures = markets
            .iter()
            .map(|market| {
                let client = client.clone();
                let market = *market;
                async move { client.fetch_market_and_reserves(&market).await }
            })
            .collect::<Vec<_>>();

        let results = futures::future::join_all(futures).await;

        let mut map = HashMap::new();
        for (i, result) in results.into_iter().enumerate() {
            let r = result?;
            map.insert(markets[i], r);
        }
        Ok(map)
    }
}

pub mod rpc {
    use anchor_client::solana_client::{
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    };
    use anchor_lang::{Discriminator, Owner};
    use bytemuck::{from_bytes, AnyBitPattern};
    use orbit_link::OrbitLink;
    use solana_account_decoder::UiAccountEncoding;
    use solana_rpc_client::nonblocking::rpc_client::RpcClient;

    use super::*;

    pub async fn get_zero_copy_pa<Acc>(
        client: &OrbitLink<RpcClient, Keypair>,
        program_id: &Pubkey,
        filters: &[RpcFilterType],
    ) -> Result<Vec<(Pubkey, Acc)>>
    where
        Acc: AnyBitPattern + Owner + Discriminator,
    {
        let size = u64::try_from(std::mem::size_of::<Acc>() + 8).unwrap();
        let discrim_memcmp = RpcFilterType::Memcmp(Memcmp::new(
            0,
            MemcmpEncodedBytes::Bytes(Acc::discriminator().to_vec()),
        ));
        let mut all_filters = vec![RpcFilterType::DataSize(size), discrim_memcmp];
        for f in filters {
            all_filters.push(f.clone());
        }
        let config = RpcProgramAccountsConfig {
            filters: Some(all_filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64Zstd),
                commitment: Some(CommitmentConfig::confirmed()),
                ..RpcAccountInfoConfig::default()
            },
            ..RpcProgramAccountsConfig::default()
        };

        let accs = client
            .client
            .get_program_accounts_with_config(program_id, config)
            .await?;

        let parsed_accounts = accs
            .into_iter()
            .map(|(pubkey, account)| {
                let data: &[u8] = &account.data;
                let acc: &Acc = from_bytes(&data[8..]);
                Ok((pubkey, *acc))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(parsed_accounts)
    }
}
