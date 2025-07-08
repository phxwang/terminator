// 针对所有支持的amm，检查SOL-USDC本地报价和模拟报价是否一致
use std::{collections::HashMap, env, sync::LazyLock};
use dotenv::dotenv;
use bytes::Bytes;
use itertools::Either;
use solana_sdk::{
    pubkey::Pubkey, hash::Hash, signature::Signature,
    program_pack::Pack, commitment_config::CommitmentConfig,
    message::{VersionedMessage, v0::Message as V0Message},
    transaction::VersionedTransaction, compute_budget::ComputeBudgetInstruction,
    sysvar, clock::Clock,
};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config};
use spl_token::state::Account as TokenAcccount;
use extra_proto::{
    extra_client::ExtraClient, Commitment,
    GetMultipleAccountsRequest, SimulateTransactionRequest,
};
use kubi2::{
    common::{net_utils::tonic::generate_channel, utils::init_env_logger},
    sol::base::{generate_amm, Token},
};

// 用于测试的amm，要求至少一段是SOL或者USDC，这样测试账户上有足够的
static AMM_IDS: LazyLock<Vec<Pubkey>> = LazyLock::new(|| [
    // -------- token-swap --------
    // legacy: SOL-USDC；无差异
    "AmHUjHKfSFP34D4VgPsviFNjWrvTN761Yazvv2eKAsSz",
    "2QvejGqx1yAx5CwTPczaC5XRC7YPGtRM7ApzWNCTCZ9Q",
    // orca-v1: SOL-USDC；无差异
    "6fTRDD7sYxCN7oyoSQaN1AWC3P2m8A6gVZzGrpej9DvL",
    // orca-v2: SOL-USDC；无差异
    "EGZ7tiLeH62TPV1gL8WwbXGzEPa9zmcpVnnkPKKnrE2U",
    // orca-v2: USDT-USDC；无差异
    "F13xvvx45jVGd84ynK3c8T89UejQVxjCLtmHfPmAXAHP",
    // orca-v2: mSOL-SOL；无差异
    "9EQMEzJdE2LDAY1hw1RytpufdwAXzatYfQ3M2UuT9b88",
    // saros: SOL-USDC；无差异
    "Djxfn7zWFxFqYgwesXfq8BeAirfXwfhQmNcCousXh7G7",
    // dooar: SOL-USDC；无差异
    "5GGvkcqQ1554ibdc18JXiPqR8aJz6WV3JSNShoj32ufT",
    // fluxbeam: SOL-USDC；🔴尾数存在差异
    "CZEZDGDkzsn4zTfdw6XRm4U1o6GatotMhhRmVEzdwGS3",

    // -------- orac-whirlpool --------
    // SOL-USDC，早期池子可使用swap和swpv2，无差异
    // "FpCMFDFGYotvufJ7HrFHsWEiiQCGbkLCtwHiDnh7o28Q",
    // "83v8iPyZihDEjDdY8RdZddyZNyUtXngz69Lgo9Kt5d6d",
    // "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
    // Zeta-SOL，后期池子仅支持swpv2，无差异
    // "EY8Nqmqdf8wqnv4DRqnqmpbA7cxjXdgKFay63Mq7wkto",

    // -------- raydium-clmm --------
    // SOL-USDC，无差异
    // "2QdhepnKRTLjjSqPL1PtKNwqrUkoLee5Gqs8bvZhRdMv",
    // USDC-USDT，无差异
    // "3NeUgARDmFgnKtkJLqUcEUNCfknFCcGsFfMJCtx6bAgx",
    // TODO 存在问题 看起来是tick-array不存在导致的，为什么中心不存在呢？
    // "3nNg189ewAxiNeRt3hHuMzTrL6hpwUZ7zXUWTcQhTojq",
    // 测试用
    // "DTzrS1bx935fisL7toqrmTfM8tLy5Rq1AqLn2qLvgZxJ",

    // obric-amm: SOL-USDC TODO 还没有完成，主要是oracle部分
    // "AvBSC1KmFNceHpD6jyyXBV6gMXFxZ8BJJ3HVUN8kCurJ",

    // sol-fi: SOL-USDC；TODO 未完成
    // "AHhiY6GAKfBkvseQDQbBC7qp3fTRNpyZccuEdYSdPFEf",

    // meteora-damm: SOL-USDC；无差异
    // "Ft8FD9gg1TdawhWijNzdkpYTiVL8ETfY6gAwS24bwYio",

    // meteora-dlmm: SOL-USDC；无差异
    // "5XRqv7LCoC5FhWKk5JN8n4kCrJs3e4KH1XsYzKeMd5Nt",

    // saber-stable: USDT-USDC，saber没有SOL-USDC；🔴尾数存在差异
    // "YAkoNb6HKmSxQN9L8hiBE5tPJRsniSSMzND1boHmZxe",
    // saber-decimals: USDC-sUSDC8；无差异
    // "G4gRGymKo7MGzGZup12JS39YVCvy8YMM6KY9AmcKi5iw",

    // mercurial-stable: USDT-USDC-PAI 无差异
    // "SWABtvDnJwWwAb9CbSA3nv7nTnrtYjrACAVtuP3gyBB",
    // mercurial-stable: 其他；部分🔴尾数存在差异，js同样
    // "SoLw5ovBPNfodtAbxqEKHLGppyrdB4aZthdGwpfpQgi",
    // "USD42Jvem43aBSLqT83GZmvRbzAjpKBonQYBQhni7Cv",
    // "LiDoU8ymvYptqxenJ4YpcURBchn4ef63tcbdznBCKJh",
    // "MAR1zHjHaQcniE2gXsDptkyKUnNfMEsLBVcfP7vLyv7",
    // "BUSDXyZeFXrcEkETHfgGh5wfqavmfC8ZJ8BbRP33ctaG",
    // "UXD3M3N6Hn1JjbxugKguhJVHbYm8zHvdF5pNf7dumd5",
    // "USDHyeagFLn8Ct6JfQ7CAV9aoecKjwWmWXL6oNNoL7W",
    // "USD4WhBLkQsNm8bXMfxoKuMRtE281CYrPGcfJXZxQL9",
    // "aUSNkg1M8YoHEpAAf3XX4FXUUCNKRA16HhCFEYq39Jv",
    // "abUDCvrNgN9snpRKo2x73pSxaWP1m7gysnBeVmi2XPW",
    // "abUTXJ1KTSaKocx3gr6UQDn5kaN666c9x7UXZSXepbh",
    // "ABuSDa3bRxMQ9nFUoGtuFqTGujGebzbNuwozD1iqRFbe",
    // "HNXa7Lkmep3ChcBRwo6hbYgCooGRaVQkAM9VFjmRFbYE",

    // raydium-amm: SOL-USDC；无差异
    // "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",

    // pump-amm: SOL-USDC；🔴尾数存在差异
    // "Gf7sXMoP8iRw4iiXmJ1nq4vxcRycbGXy5RL8a8LnTd3v",
    // "J2MYQ15QbTAmMrGcqbPgWaWsH9fgmzN8GBJ5SC7DzU5A",
    // pump-amm: Figure-SOL，首次观察到账户长度300字节
    // "GseMAnNDvntR5uFePZ51yZBXzNSn7GdFPkfHwfr6d77J",
    // 带有coin-creator，#test-SOL
    // "8F7PAcVMDidJScLyx33iCaNbuL8s5pQVqAz4JSnqywiK",
    // 没有coin-creator，且没有占位的amm
    // "qLKm71JzWUwcubngWGBdVzUyrAW6DkEyGEinmyz2xmJ",

    // meteora-amm: SOL-USDC: 无差异
    // "6SWtsTzXrurtVWZdEHvnQdE9oM8tTtyg8rfEo3b4nM93",
    // meteora-amm: VIRTUAL-SOL；无差异
    // "B1AdQ85N2mJ2xtMg9bgThhsPoA6T3M26rt4TChWSiPpr",
    // meteora-amm: $ZMBI-SOL，activation-type=slot, activation-type=301345498；无差异
    // "FHZFQ6neZugr4coZNZPEmSBchS2uEyF8e3YBE6qAN19b",

    // stable-weighted: SOL-USDC；无差异
    // "JV4MkRFn58xpyrhF2oDxQYwnq5jFVzTQUKcUzce1FQA",
    // stabble-stable: USDT-USDC；无差异
    // "5K7CHUbBYAh6wrantyJvDDqwT4VoKuZTi73CN1DTUUer", // 流动性正常
    // stabble-stable: FDUSD-USDT-USDC；无差异
    // "DqePNza3Hg7wSfkMp2d84HAwNsPrZ5w18LgTRLoYyzfE", // 流动性充裕
    // stabble-stable: JupSOL-JitoSOL-mSOL-wSOL；无差异
    // "CakowQHNvku2CDVDprpvdsQGDKv35TEagjEUNoKwvoQH", // 流动性正常
].into_iter().map(|s| s.parse().unwrap()).collect());

#[tokio::main]
async fn main() {
    // 读入配置
    dotenv().expect("fail to load .env");
    // 初始化logger
    init_env_logger();

    // 交易账户，要求有足够的SOL/USDC用于模拟
    let owner = env::var("OWNER_PUBLIC")
        .expect("fail to read OWNER")
        .parse::<Pubkey>()
        .expect("fail to parse OWNER");
    log::info!("owner: {}", owner);
    let another = env::var("ANOTHER_PUBLIC")
        .expect("fail to read ANOTHER")
        .parse::<Pubkey>()
        .expect("fail to parse ANOTHER");
    log::info!("another: {}", another);
    // 外部RPC，用于获取账户数据和模拟交易
    let rpc = env::var("EXTERNAL_RPC")
        .expect("EXTERNAL_RPC missed in .env");
    log::info!("rpc: {}", rpc);
    let rpc_client = RpcClient::new_with_commitment(
        rpc, CommitmentConfig::confirmed(),
    );
    // 自建服务，用于模拟交易
    let extra = env::var("EXTRA")
        .expect("EXTRA missed in .env");
    log::info!("extra: {}", extra);
    let channel = generate_channel(extra, None)
        .await.expect("fail to generate channel");
    let mut extra_client = ExtraClient::new(channel);

    // 逐个AMM
    for amm_id in AMM_IDS.iter() {
        log::info!("🔵 processing amm: {}", amm_id);

        // Part 1: 生成amm
        let mut amm = {
            let account = rpc_client.get_account(amm_id)
                .await.expect("fail to get account");
            // dbg!(account.data.len());
            // 账户数据测试代码
            // log::info!("data: {}", base64::encode(&account.data));
            generate_amm(*amm_id, Bytes::from(account.data), account.owner)
                .expect("fail to generate amm") // 外层result
                .expect("unsupported amm") // 内层option
        };
        log::info!("kind: {}, mint: {:?}", amm.kind(), amm.mints());

        // Part 2: 更新amm，使用extra确保和模拟同步
        let mut update_slot;
        let mut update_ts;
        loop {
            // 读入账户数据，特别注意：此处使用confirmed确保内容即时且不会发生变化
            let mut addresses = amm.accounts_for_update();
            addresses.push(sysvar::clock::ID);
            // dbg!(&addresses);
            let respnose = extra_client.get_multiple_accounts(
                GetMultipleAccountsRequest {
                    addresses: addresses.iter()
                        .map(|address| address.to_bytes().to_vec())
                        .collect(),
                    commitment_or_slot: Commitment::Confirmed as _,
                }
            ).await.expect("fail to get accounts")
            .into_inner();
            assert!(addresses.len() == respnose.datas.len());
            // rpc_client.get_multiple_accounts_with_commitment(
            //     &addresses, CommitmentConfig::confirmed(),
            // ).await.expect("fail to get accounts");
            let infos = addresses.into_iter() // 地址
                .zip(respnose.datas.into_iter()) // 数据
                .filter_map(|(address, data)| { // 转换
                    if data.len() > 0 {
                        Some((address, Bytes::from(data)))
                    } else { None}
                })
                .collect::<HashMap<_, _>>(); // 过滤掉空数据
            // 实际更新
            let stable = amm.update(&infos)
                .expect("fail to update amm");
            // 从clock变量中获取slot和timestamp
            let clock = bincode::deserialize::<Clock>(
                infos.get(&sysvar::clock::ID)
                    .unwrap()
                    .as_ref()
            ).unwrap();
            update_slot = clock.slot;
            update_ts = clock.unix_timestamp as u64;
            // update_slot = respnose.slot;
            // update_ts = mock_block_timestamp();
            // 数据编码后写入文件
            // if stable {
            //     use std::{fs::File, io::Write};
            //     let mut file = File::create("../kubi/detect-amm.txt")
            //         .expect("fail to create file");
            //     infos.into_iter().for_each(|(address, data)| {
            //         write!(file, "{}\n{}\n", address, base64::encode(data)).unwrap();
            //     });
            //     break;
            // }
            // 完备则退出
            if stable { break }
        }
        log::info!("update slot: {}", update_slot);

        // Part 3: 比较报价和模拟交易，区分类型
        let unknown; // 额外申请确保引用有效
        for (from, to, nums) in
            if [Token::SOL, Token::USDC].into_iter()
                .all(|token| amm.mints().contains(&token.mint))
            { vec![ // 常见amm都存在SOL-USDC池子
                (Token::SOL, Token::USDC, vec![0.05, 0.1, 1.0, 10.0, 100.0, 1_000.0]),
                (Token::USDC, Token::SOL, vec![5.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0]),
            ] } else if [Token::USDT, Token::USDC].into_iter()
                .all(|token| amm.mints().contains(&token.mint))
            { vec![ // 部分stable没有SOL-USDC池子，此处尝试USDT-USDC
                (Token::USDC, Token::USDT, vec![5.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0]),
                (Token::USDT, Token::USDC, vec![5.0, 10.0, 100.0, 1_000.0, 10_000.0, 25_000.0]),
            ] } else if amm.mints().contains(&Token::SOL.mint) { // 存在SOL，尝试单向到另一币种
                unknown = Token {
                    symbol: "UNKNOWN",
                    mint: amm.mints().to_vec().into_iter()
                        .find(|mint| *mint != Token::SOL.mint)
                        .unwrap(), // 一定存在
                    multi: 1_000_000_000,
                };
                vec![(Token::SOL, &unknown, vec![0.05, 0.1, 1.0, 10.0, 100.0, 1_000.0])]
            } else if amm.mints().contains(&Token::USDC.mint) { // 存在USDC，尝试单向到另一币种
                unknown = Token {
                    symbol: "UNKNOWN",
                    mint: amm.mints().to_vec().into_iter()
                        .find(|mint| *mint != Token::USDC.mint)
                        .unwrap(), // 一定存在
                    multi: 1_000_000,
                };
                vec![(Token::USDC, &unknown, vec![5.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0])]
            } else if amm.mints().contains(&Token::USDT.mint) { // 存在USDT，尝试单向到另一币种
                unknown = Token {
                    symbol: "UNKNOWN",
                    mint: amm.mints().to_vec().into_iter()
                        .find(|mint| *mint != Token::USDT.mint)
                        .unwrap(), // 一定存在
                    multi: 1_000_000,
                };
                vec![(Token::USDT, &unknown, vec![5.0, 10.0, 100.0, 1_000.0, 10_000.0, 25_000.0])]
            } else { unreachable!() }
        {
            log::info!("from {} to {}: {:?}", from.symbol, to.symbol, nums);

            // 对应token_account地址
            let from_address = spl_associated_token_account::get_associated_token_address(
                &owner, &from.mint);
            let to_address = spl_associated_token_account::get_associated_token_address(
                &owner, &to.mint);

            // 不同点位开始测算
            let first_number = nums[0];
            // let first_number = nums[nums.len() - 1];
            for num in nums {
                // 本地报价
                let local = amm.quote(
                    from.mint, to.mint,
                    (num * from.multi as f64) as _,
                    Some((update_slot, update_ts)), // 设定时间
                    // None,
                );
                // dbg!(&local);
                // let local_2 = amm.quote(
                //     from.mint, to.mint,
                //     (num * from.multi as f64) as _,
                //     Some((update_slot + 2, update_ts + 1)),
                // );
                // let local_10 = amm.quote(
                //     from.mint, to.mint,
                //     (num * from.multi as f64) as _,
                //     Some((update_slot + 10, update_ts + 4)),
                // );
                // let local_100 = amm.quote(
                //     from.mint, to.mint,
                //     (num * from.multi as f64) as _,
                //     Some((update_slot + 100, update_ts + 40)),
                // );
                // let local_1000 = amm.quote(
                //     from.mint, to.mint,
                //     (num * from.multi as f64) as _,
                //     Some((update_slot + 1000, update_ts + 400)),
                // );
                // dbg!(local_2, local_10, local_100, local_1000);

                // 模拟报价
                let simulate = {
                    // 生成tx
                    let tx = {
                        let (swap, metas) = amm.routie(
                            owner,
                            (from.mint, from_address),
                            (to.mint, to_address),
                            // controller_interface::ID,
                            jupiter_interface::ID,
                        );
                        // dbg!(&metas);
                        // if 1 > 0 { std::process::exit(1) }
                        // let mut ix = controller_interface::chain_ix(
                        //     controller_interface::ChainIxArgs {
                        //         swaps: vec![swap.into()],
                        //         input: (num * from.multi as f64) as _,
                        //         minimal_output: 0,
                        //     },
                        // ).unwrap(); // 一定成功
                        let mut ix = jupiter_interface::route_ix(
                            jupiter_interface::RouteKeys {
                                token_program: spl_token::id(),
                                user_transfer_authority: owner,
                                user_source_token_account: from_address,
                                user_destination_token_account: to_address,
                                destination_token_account: jupiter_interface::ID, // program_id表示空，原因未知
                                destination_mint: to.mint,
                                platform_fee_account: jupiter_interface::ID, // program_id表示空
                                event_authority: jupiter_interface::EVENT_AUTHORITY,
                                program: jupiter_interface::ID,
                            },
                            jupiter_interface::RouteIxArgs {
                                route_plan: vec![
                                    jupiter_interface::RoutePlanStep {
                                        swap: swap.into(),
                                        percent: 100,
                                        input_index: 0,
                                        output_index: 1,
                                    },
                                ],
                                in_amount: (num * from.multi as f64) as _,
                                quoted_out_amount: 0, // 期望输出，此处不做限制
                                slippage_bps: 0, // slippage和platform-fee都为空
                                platform_fee_bps: 0,
                            },
                        ).unwrap(); // 一定成功
                        // dbg!(&metas);
                        ix.accounts.extend(metas);
                        // 测试代码看，看后面的try_new是否出错
                        // 特别注意：无论指令多长，后面的try_new和try_conmpile都不会报错
                        // for _ in 0..50 {
                        //     ix.accounts.push(solana_sdk::instruction::AccountMeta {
                        //         pubkey: Pubkey::new_unique(),
                        //         is_signer: false,
                        //         is_writable: false,
                        //     });
                        // }

                        VersionedTransaction {
                            signatures: vec![
                                Signature::new_unique(),
                                Signature::new_unique(),
                            ], // 模拟无需签名
                            message: VersionedMessage::V0(
                                V0Message::try_compile(
                                    &another, // 支付者，特别注意：不是owner
                                    &[
                                        ComputeBudgetInstruction::set_compute_unit_limit(1_000_000),
                                        spl_associated_token_account::instruction
                                            ::create_associated_token_account_idempotent(
                                                &owner, // 费用支付者
                                                &owner, // 生成账户的所有者
                                                &to.mint, // 币种
                                                &spl_token::ID,
                                            ), // 确保目标账户存在
                                        ix, // 兑换指令
                                    ],
                                    &[],
                                    Hash::default(), // 模拟无需blockhash
                                ).unwrap(),
                            ),
                        }
                    };

                    // 记录账户余额，此处固定slot
                    let pre = {
                        let response = extra_client.get_multiple_accounts(GetMultipleAccountsRequest {
                            addresses: vec![to_address.to_bytes().to_vec()],
                            commitment_or_slot: update_slot, // 固定slot
                        }).await.expect("fail to get multiple accounts")
                        .into_inner();
                        assert!(response.datas.len() == 1);
                        if response.datas[0].len() > 0 { // 可能不存在
                            TokenAcccount::unpack_from_slice(
                                &response.datas[0]
                            ).unwrap().amount // 一定成功
                        } else { 0 }
                    };

                    // 通过extra模拟交易，此处固定slot
                    let response = extra_client.simulate_transaction(SimulateTransactionRequest {
                        data: bincode::serialize(&tx).unwrap(), // 一定成功
                        replaces: vec![], // 不做替换
                        commitment_or_slot: update_slot, // 固定slot
                        addresses: vec![to_address.to_bytes().to_vec()], // 目标token-acount
                    }).await.expect("fail to simulate transaction")
                    .into_inner();
                    // dbg!(&response.logs);

                    // 区分结果
                    if let Some(_err) = response.err { // tx失败
                        Either::Right(response.logs)
                    } else { // tx成功
                        let post = TokenAcccount::unpack_from_slice(
                            &response.datas[0]
                        ).unwrap().amount;
                        Either::Left((pre, post, response.consumed))
                    }
                };

                // 提示结果
                match (local, simulate) {
                    // 本地和模拟都成功，比较结果
                    (Ok(local), Either::Left((pre, post, consumed))) => {
                        if local == post - pre {
                            log::info!("{} SAME: local {}, simulate {}, consumed: {}",
                                num, local, post - pre, consumed);
                        } else if local >= post - pre - 1 && local <= post - pre + 1 {
                            // 特别注意：相差1也认为是正确的，只是在计算过程中取整存在差异
                            log::warn!("{} CLOSE: local {}, simulate {}, consumed: {}",
                                num, local, post - pre, consumed);
                        } else {
                            log::error!("{} DIFF: local {}, simulate {}, consumed: {}",
                                num, local, post - pre, consumed);
                        }
                    },
                    // 本地和模拟都失败，也认为是正确的
                    (Err(err), Either::Right(logs)) => {
                        log::warn!("{} FAILED: local {}, simulate: {:?}",
                            num, err, logs);
                    },
                    // 其他情况都认为不一致，此时认为出错
                    (local, simulate) => {
                        log::error!("{} UNEXPECTED: local {:?}, simulate {:?}",
                            num, local, simulate);
                    },
                }
            }

            { // 模拟from -> to -> from，检查账户填写是否正确
                // 兑换指令
                let (forward_swap, forward_metas) = amm.routie(
                    owner,
                    (from.mint, from_address),
                    (to.mint, to_address),
                    jupiter_interface::ID,
                );
                let (backward_swap, backward_metas) = amm.routie(
                    owner,
                    (to.mint, to_address),
                    (from.mint, from_address),
                    jupiter_interface::ID,
                );
                let mut ix = jupiter_interface::route_ix(
                    jupiter_interface::RouteKeys {
                        token_program: spl_token::id(),
                        user_transfer_authority: owner,
                        user_source_token_account: from_address,
                        user_destination_token_account: to_address,
                        destination_token_account: jupiter_interface::ID, // program_id表示空，原因未知
                        destination_mint: to.mint,
                        platform_fee_account: jupiter_interface::ID, // program_id表示空
                        event_authority: jupiter_interface::EVENT_AUTHORITY,
                        program: jupiter_interface::ID,
                    },
                    jupiter_interface::RouteIxArgs {
                        route_plan: vec![
                            jupiter_interface::RoutePlanStep {
                                swap: forward_swap.into(),
                                percent: 100,
                                input_index: 0,
                                output_index: 1,
                            },
                            jupiter_interface::RoutePlanStep {
                                swap: backward_swap.into(),
                                percent: 100,
                                input_index: 1,
                                output_index: 0,
                            },
                        ],
                        in_amount: (first_number * from.multi as f64) as _,
                        quoted_out_amount: 0, // 期望输出，此处不做限制
                        slippage_bps: 0, // slippage和platform-fee都为空
                        platform_fee_bps: 0,
                    },
                ).unwrap(); // 一定成功
                // dbg!(&metas);
                ix.accounts.extend(forward_metas);
                ix.accounts.extend(backward_metas);

                let tx = VersionedTransaction {
                    signatures: vec![
                        Signature::new_unique(),
                        Signature::new_unique(),
                    ], // 模拟无需签名
                    message: VersionedMessage::V0(
                        V0Message::try_compile(
                            &another, // 支付者，特别注意：不是owner
                            &[
                                spl_associated_token_account::instruction
                                ::create_associated_token_account_idempotent(
                                    &owner, // 费用支付者
                                    &owner, // 生成账户的所有者
                                    &to.mint, // 币种
                                    &spl_token::ID,
                                ), // 确保目标账户存在
                                ix, // 兑换指令
                            ],
                            &[],
                            Hash::default(), // 模拟无需blockhash
                        ).unwrap(),
                    ),
                };

                let response = rpc_client.simulate_transaction_with_config(
                    &tx,
                    rpc_config::RpcSimulateTransactionConfig {
                        sig_verify: false,
                        replace_recent_blockhash: true,
                        ..Default::default()
                    }
                ).await.expect("fail to simulate from->to->from");
                if response.value.err.is_some() {
                    log::error!("{} FAILED from->to->from: {:?} {:#?}",
                        first_number, response.value.err, response.value.logs);
                    // std::process::exit(1);
                } else {
                    log::info!("{} GOOD from->to->from", first_number);
                    // log::info!("simulate from->to->from: {:#?}",
                    //     response.value.logs);
                }
            }
        } // for (from, to, nums)
    } // for amm_id
}