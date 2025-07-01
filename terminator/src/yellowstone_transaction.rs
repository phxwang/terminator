use backoff::{backoff::Constant, future::retry};
use itertools::Itertools;
use solana_sdk::{signature::Signature, pubkey::Pubkey};
use anchor_lang::AnchorDeserialize;
use tokio::sync::mpsc::Sender;
use yellowstone_grpc_client::Interceptor;
use yellowstone_grpc_proto::{geyser::{CommitmentLevel, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeRequestPing}, prelude::{Message, TransactionStatusMeta}};

use {
    futures::stream::StreamExt,
    std::collections::HashMap,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
    },
};
use core::time::Duration;
use std::sync::{atomic::AtomicU64, Arc};
use futures_util::SinkExt;

// Local definitions since ellipsis_transaction_utils is not available
#[derive(Debug, Clone)]
pub struct ParsedInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ParsedInnerInstruction {
    pub parent_index: usize,
    pub instruction: ParsedInstruction,
}

#[derive(Debug, Clone)]
pub struct ParsedTransaction {
    pub fee_payer: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub instructions: Vec<ParsedInstruction>,
    pub inner_instructions: Vec<Vec<ParsedInnerInstruction>>,
    pub logs: Vec<String>,
    pub is_err: bool,
}

pub struct YellowstoneTransaction {
    pub slot: u64,
    pub meta: TransactionStatusMeta,
    pub signature: Signature,
    pub message: Message,
}

impl YellowstoneTransaction {
    pub fn parse_message(
        &self,
        loaded_addresses: &[String],
    ) -> (Vec<String>, Vec<ParsedInstruction>) {
        let mut keys = self
            .message
            .account_keys
            .iter()
            .map(|pk| Pubkey::try_from_slice(pk).unwrap().to_string())
            .collect_vec();
        keys.extend_from_slice(loaded_addresses);
        let instructions = self
            .message
            .instructions
            .iter()
            .map(|instruction| ParsedInstruction {
                program_id: keys[instruction.program_id_index as usize].clone(),
                accounts: instruction
                    .accounts
                    .iter()
                    .map(|i| keys[*i as usize].clone())
                    .collect(),
                data: instruction.data.clone(),
            })
            .collect_vec();
        (keys, instructions)
    }

    pub fn to_parsed_transaction(&self) -> ParsedTransaction {
        let loaded_addresses = [
            self.meta
                .loaded_writable_addresses
                .iter()
                .map(|x| Pubkey::try_from_slice(x).unwrap().to_string())
                .collect_vec(),
            self.meta
                .loaded_readonly_addresses
                .iter()
                .map(|x| Pubkey::try_from_slice(x).unwrap().to_string())
                .collect_vec(),
        ]
        .concat();

        let (keys, instructions) = self.parse_message(&loaded_addresses);
        let is_err = self.meta.err.is_some();
        let logs = self.meta.log_messages.clone();
        let fee_payer = keys[0].clone();
        let inner_instructions = self
            .meta
            .inner_instructions
            .iter()
            .map(|ii| {
                ii.instructions
                    .iter()
                    .map(|i| ParsedInnerInstruction {
                        parent_index: ii.index as usize,
                        instruction: ParsedInstruction {
                            program_id: keys[i.program_id_index as usize].clone(),
                            accounts: i
                                .accounts
                                .iter()
                                .map(|i| keys[*i as usize].clone())
                                .collect(),
                            data: i.data.clone(),
                        },
                    })
                    .collect::<Vec<ParsedInnerInstruction>>()
            })
            .collect::<Vec<Vec<ParsedInnerInstruction>>>();

        ParsedTransaction {
            fee_payer,
            slot: self.slot,
            block_time: None,
            signature: self.signature.to_string(),
            instructions,
            inner_instructions,
            logs,
            is_err,
        }
    }
}



pub async fn transaction_subscribe(
    endpoint: String,
    x_token: Option<String>,
    sender: Sender<(CommitmentLevel, YellowstoneTransaction)>,
    accounts_to_include: Vec<Pubkey>,
    accounts_to_exclude: Vec<Pubkey>,
    commitment: CommitmentLevel,
    received_failed_tx: Option<bool>,
) -> anyhow::Result<()> {
    let mut transactions = HashMap::new();
    transactions.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: received_failed_tx, // disable failed tx, 对于failed的tx，怎么办？
            signature: None,
            account_include: accounts_to_include
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            account_exclude: accounts_to_exclude
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            account_required: vec![]
        },
    );

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(Constant::new(Duration::from_secs(1)), move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let transactions = transactions.clone();
        let sender = sender.clone();
        // // random ping id
        // let ping_id = rand::random::<i32>();
        async move {
            log::info!("Reconnecting to the gRPC server: {}, {:?}, {:?}", endpoint, x_token, commitment);
            let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;

            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: HashMap::new(),
                transactions,
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(commitment.into()), // 1 是 confirmed
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            };

            let (mut subscribe_tx, mut stream) = client.subscribe().await.map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;
            subscribe_tx.send(subscribe_request).await.map_err(|e| anyhow::Error::msg(format!("Failed to send subscribe request: {}", e)))?;
            // let (mut subscribe_tx, mut stream) = client
            //     .subscribe_with_request(Some(subscribe_request))
            //     .await
            //     .map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;

            while let Some(message) = stream.next().await {

                match message {
                    Ok(msg) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Transaction(transaction)) => {
                                let slot = transaction.slot;
                                match transaction.transaction {
                                    Some(tx) => {
                                        if tx.meta.is_none() {
                                            log::warn!("Transaction meta is empty");
                                            continue;
                                        }
                                        if tx.transaction.is_none() {
                                            log::warn!("Transaction is empty");
                                            continue;
                                        }
                                        let message = tx.transaction.and_then(|x| x.message);
                                        if message.is_none() {
                                            log::warn!("Transaction message is empty");
                                            continue;
                                        }
                                        // log::info!("transacton: {:?}", Signature::try_from(tx.signature.clone()).ok()?);
                                        let ys = YellowstoneTransaction {
                                            slot,
                                            meta: tx.meta.unwrap(),
                                            signature: Signature::try_from(tx.signature).unwrap(),
                                            message: message.unwrap(),
                                        };
                                        if sender.send((commitment, ys)).await.is_err() {
                                            log::error!("Failed to send transaction update");
                                        };
                                    },
                                    None => {
                                        log::info!("transaction is none");
                                    },
                                }
                            },
                            Some(UpdateOneof::Ping(_ping)) => {
                                // log::info!("Received ping: {:?}", ping);
                                let r = subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await;
                                if let Err(e) = r {
                                    log::error!("Failed to send ping: {}", e);
                                    continue;
                                }
                            },
                            Some(UpdateOneof::Pong(_pong)) => {
                                // log::info!("Received pong: {:?}", pong);
                            },
                            None => {
                                log::info!("received none update one of")
                            },
                            _ => {
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("Failed to receive transaction update, break the stream loop and retry, {}", e);
                        break;
                    }
                };
            }
            Err(anyhow::Error::msg("Stream closed").into())
        }
    })
    .await
}


pub async fn create_yellowstone_client(endpoint: &str,
    x_token: &Option<String>, timeout_secs: u64) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
    GeyserGrpcClient::build_from_shared(endpoint.to_string())
                .and_then(|builder| builder.x_token(x_token.clone()))
                .map(|builder| builder.connect_timeout(Duration::from_secs(timeout_secs)))
                .map(|builder| builder.timeout(Duration::from_secs(timeout_secs)))
                .map_err(|e| anyhow::Error::msg(format!("Failed to create builder: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    anyhow::Error::msg(format!(
                        "Failed to connected to endpoint: {} ({})",
                        endpoint, e
                    ))
                })
}

pub async fn slot_subscribe(
    endpoint: String,
    x_token: Option<String>,
    slot_container: Arc<AtomicU64>,
    commitment: CommitmentLevel,
) -> anyhow::Result<()> {
    let mut slots = HashMap::new();
    slots.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots { filter_by_commitment: Some(true) },
    );

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(Constant::new(Duration::from_secs(1)), move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let slot_container = slot_container.clone();
        let slots = slots.clone();
        async move {
            log::info!("Reconnecting to the slot gRPC server");
            let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;
            let subscribe_request = SubscribeRequest {
                slots,
                accounts: HashMap::new(),
                transactions: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(commitment.into()), // 1 是 confirmed
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            };
            let (mut subscribe_tx, mut stream) = client
                .subscribe_with_request(Some(subscribe_request))
                .await
                .map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;

            while let Some(message) = stream.next().await {
                if message.is_err() {
                    log::error!("Failed to receive slot update, break the stream loop and retry");
                    break;
                }
                let msg = message.unwrap();
                match msg.update_oneof {
                    Some(UpdateOneof::Slot(slot)) => {
                        // log::info!("slot updated to {}", slot.slot);
                        slot_container.store(slot.slot, std::sync::atomic::Ordering::Relaxed);
                    },
                    Some(UpdateOneof::Ping(_ping)) => {
                        // log::info!("Received slot subscribe ping: {:?}", ping);
                        let r = subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                        if let Err(e) = r {
                            log::error!("Failed to send ping: {}", e);
                            continue;
                        }
                    },
                    Some(UpdateOneof::Pong(_pong)) => {
                        // log::info!("Received slot subscribe pong: {:?}", pong);
                    }
                    None => {
                        log::info!("received none update one of")
                    },
                    _ => {
                    }
                }

            }
            Err(anyhow::Error::msg("Stream closed").into())
        }
    })
    .await
}


pub async fn slot_subscribe_to_channel(
    endpoint: String,
    x_token: Option<String>,
    slot_channel: Arc<Sender<u64>>,
    commitment: CommitmentLevel,
) -> anyhow::Result<()> {
    let mut slots = HashMap::new();
    slots.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots { filter_by_commitment: Some(true) },
    );

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(Constant::new(Duration::from_secs(1)), move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let slot_channel = slot_channel.clone();
        let slots = slots.clone();
        async move {
            log::info!("Reconnecting to the slot gRPC server");
            let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;
            let subscribe_request = SubscribeRequest {
                slots,
                accounts: HashMap::new(),
                transactions: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(commitment.into()), // 1 是 confirmed
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            };
            let (mut subscribe_tx, mut stream) = client
                .subscribe_with_request(Some(subscribe_request))
                .await
                .map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;

            while let Some(message) = stream.next().await {
                if message.is_err() {
                    log::error!("Failed to receive slot update, break the stream loop and retry");
                    break;
                }
                let msg = message.unwrap();
                match msg.update_oneof {
                    Some(UpdateOneof::Slot(slot)) => {
                        slot_channel.send(slot.slot).await.unwrap();
                    },
                    Some(UpdateOneof::Ping(_ping)) => {
                        // log::info!("Received slot subscribe ping: {:?}", ping);
                        let r = subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                        if let Err(e) = r {
                            log::error!("Failed to send ping: {}", e);
                            continue;
                        }
                    },
                    Some(UpdateOneof::Pong(_pong)) => {
                        // log::info!("Received slot subscribe pong: {:?}", pong);
                    }
                    None => {
                        log::info!("received none update one of");
                        break;
                    },
                    _ => {
                    }
                }

            }
            Err(anyhow::Error::msg("Stream closed").into())
        }
    })
    .await
}

#[derive(
    Clone, // 方便比较和赋值
    Debug,
)]
pub struct BlockMeta {
    pub blockhash: String,
    pub slot: u64,
}

pub async fn blockmeta_subscribe_to_channel(
    endpoint: String,
    x_token: Option<String>,
    blockmeta_channel: Sender<BlockMeta>,
    commitment: CommitmentLevel,
) -> anyhow::Result<()> {
    let mut request_filter = HashMap::new();
    request_filter.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(Constant::new(Duration::from_secs(1)), move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let slot_channel = blockmeta_channel.clone();
        let blocks_meta = request_filter.clone();
        async move {
            log::info!("Reconnecting to the slot gRPC server");
            let mut client = create_yellowstone_client(&endpoint, &x_token, 10).await?;
            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: HashMap::new(),
                transactions: HashMap::new(),
                blocks: HashMap::new(),
                blocks_meta,
                commitment: Some(commitment.into()), // 1 是 confirmed
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            };
            let (mut subscribe_tx, mut stream) = client
                .subscribe_with_request(Some(subscribe_request))
                .await
                .map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;

            while let Some(message) = stream.next().await {
                if message.is_err() {
                    log::error!("Failed to receive slot update, break the stream loop and retry");
                    break;
                }
                let msg = message.unwrap();
                match msg.update_oneof {
                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                        slot_channel.send(BlockMeta { blockhash: block_meta.blockhash, slot: block_meta.slot }).await.unwrap();
                    },
                    Some(UpdateOneof::Ping(_ping)) => {
                        // log::info!("Received slot subscribe ping: {:?}", ping);
                        let r = subscribe_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                        if let Err(e) = r {
                            log::error!("Failed to send ping: {}", e);
                            continue;
                        }
                    },
                    Some(UpdateOneof::Pong(_pong)) => {
                        // log::info!("Received slot subscribe pong: {:?}", pong);
                    }
                    None => {
                        log::info!("received none update one of");
                        break;
                    },
                    _ => {
                    }
                }

            }
            Err(anyhow::Error::msg("Stream closed").into())
        }
    })
    .await
}


