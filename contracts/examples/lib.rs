//! Sirius SuperContract V2 — Milestone 1 & Milestone 2 Comprehensive Contract Suite
//! Target: wasm32-unknown-unknown for Sirius Catapult gRPC WASM VM
//!
//! Exports:
//!   1. `return_two()`         -> Milestone 1 baseline benchmark (returns 2, 1 gas)
//!   2. `storage_test()`       -> Milestone 2 DFMS Drive Storage & Filesystem overlay test
//!   3. `run()`                -> Milestone 2 Full Stack (Blockchain Host API + Drive State Persistence + Embedded TransferTx)
//!   4. `defi_stake_pool()`    -> Advanced Test 1: Parameterized Multi-User DeFi Staking Pool (Bincode ABI + Multi-Tx Batch Payout)
//!   5. `dir_iterator_audit()` -> Advanced Test 2: Hierarchical Directory Iterator & Sharded Ledger Audit
//!   6. `large_stream_io()`    -> Advanced Test 3: Multi-Chunk 48KB Streaming File I/O & Download Gas Metering
//!   7. `atomic_swap_audit()`  -> Advanced Test 4: Full Blockchain Context Audit & Multi-Mosaic Atomic Swap
//!   8. `corrupt_and_trap()`   -> Advanced Test 5: Sandbox Corruption + Out-Of-Gas Trap (Verifies Atomic Rollback)

use sdk::blockchain::{self, AggregateTransaction, EmbeddedTransaction};
use sdk::dir_iterator::DirIterator;
use sdk::file::{FileReader, FileWriter};
use sdk::filesystem::{
    create_dir, file_size, is_file, move_filesystem_entry, path_exists, remove_filesystem_entry,
};
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Read, Write};

/// Mosaic represents a token on Sirius Chain (e.g., XPX or custom token mosaic).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mosaic {
    pub mosaic_id: u64,
    pub amount: u64,
}

/// Native Sirius XPX Mosaic ID: 0x402B2F579FAEBC59
pub const XPX_MOSAIC_ID: u64 = 0x402B2F579FAEBC59;
/// Secondary DeFi Reward Mosaic ID: 0x778899AABBCCDDEE
pub const DEFI_MOSAIC_ID: u64 = 0x778899AABBCCDDEE;

/// Helper to construct an Embedded TransferTransaction (Entity_Type_Transfer = 0x4154, v3)
fn build_transfer_tx(receiver: [u8; 32], mosaic_id: u64, amount: u64, message: &[u8]) -> Option<EmbeddedTransaction> {
    let mut embedded_tx = EmbeddedTransaction::default();
    embedded_tx.set_entity_type(0x4154);
    embedded_tx.set_version(3);

    let mosaic_count = 1u64.to_le_bytes();
    let msg_size = (message.len() as u64).to_le_bytes();

    let token = Mosaic { mosaic_id, amount };
    let token_bytes = bincode::serialize(&token).ok()?;

    let mut payload = Vec::with_capacity(32 + 8 + 8 + token_bytes.len() + message.len());
    payload.extend_from_slice(&receiver);
    payload.extend_from_slice(&mosaic_count);
    payload.extend_from_slice(&msg_size);
    payload.extend_from_slice(&token_bytes);
    payload.extend_from_slice(message);

    embedded_tx.set_payload(payload);
    Some(embedded_tx)
}

// =============================================================================
// 1. Milestone 1 Baseline: Pure WASM execution with zero host calls
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn return_two() -> u32 {
    2
}

// =============================================================================
// 2. Milestone 2 Storage Overlay Test
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn storage_test() -> u32 {
    let test_case = b"Hi Sirius SuperContract V2.\nPersistent Drive Overlay Verified.\n";

    {
        let file = match FileWriter::new("test.txt") {
            Ok(f) => f,
            Err(_) => return 101,
        };
        let mut writer = BufWriter::new(file);
        if writer.write(test_case).is_err() {
            return 102;
        }
        if writer.flush().is_err() {
            return 103;
        }
    }

    let mut read_back = Vec::new();
    {
        let file = match FileReader::new("test.txt") {
            Ok(f) => f,
            Err(_) => return 104,
        };
        let mut reader = BufReader::new(file);
        if reader.read_to_end(&mut read_back).is_err() {
            return 105;
        }
    }

    if test_case.as_slice() != read_back.as_slice() {
        return 106;
    }

    if create_dir("move").is_err() {
        return 107;
    }

    if move_filesystem_entry("test.txt", "move/moved.txt").is_err() {
        return 108;
    }

    if !path_exists("move/moved.txt") {
        return 888;
    }

    match is_file("move/moved.txt") {
        Ok(true) => {}
        _ => return 999,
    }

    if remove_filesystem_entry("move").is_err() {
        return 109;
    }

    0
}

// =============================================================================
// 3. Milestone 2 Full-Stack Vault Execution
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn run() -> u32 {
    let caller = blockchain::get_caller_public_key();
    let height = blockchain::get_block_height();

    let service_payments = blockchain::get_service_payments();
    let deposit_amount = service_payments
        .iter()
        .filter(|p| p.mosaic_id == XPX_MOSAIC_ID)
        .map(|p| p.amount)
        .sum::<u64>();

    let transfer_amount = if deposit_amount > 0 {
        deposit_amount + 50_000_000
    } else {
        500_000_000
    };

    let mut prev_calls: u64 = 0;
    let mut prev_volume: u64 = 0;
    if path_exists("vault_state.bin") {
        if let Ok(file) = FileReader::new("vault_state.bin") {
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            if reader.read_to_end(&mut buf).is_ok() && buf.len() >= 16 {
                prev_calls = u64::from_le_bytes(buf[0..8].try_into().unwrap_or([0; 8]));
                prev_volume = u64::from_le_bytes(buf[8..16].try_into().unwrap_or([0; 8]));
            }
        }
    }

    let new_calls = prev_calls + 1;
    let new_volume = prev_volume + transfer_amount;

    {
        let file = match FileWriter::new("vault_state.bin") {
            Ok(f) => f,
            Err(_) => return 201,
        };
        let mut writer = BufWriter::new(file);
        let mut state_bytes = Vec::with_capacity(24);
        state_bytes.extend_from_slice(&new_calls.to_le_bytes());
        state_bytes.extend_from_slice(&new_volume.to_le_bytes());
        state_bytes.extend_from_slice(&height.to_le_bytes());
        if writer.write(&state_bytes).is_err() || writer.flush().is_err() {
            return 202;
        }
    }

    if file_size("vault_state.bin").unwrap_or(0) != 24 {
        return 203;
    }

    let embedded_tx = match build_transfer_tx(caller, XPX_MOSAIC_ID, transfer_amount, b"Payout: Supercontract Token Transfer") {
        Some(tx) => tx,
        None => return 1,
    };

    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);
    aggregate_tx.add_embedded_transaction(embedded_tx);

    blockchain::set_transaction(&aggregate_tx);

    0
}

// =============================================================================
// 4. Advanced Test 1: Parameterized Multi-User DeFi Staking Pool (`defi_stake_pool`)
// =============================================================================
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StakerPosition {
    pub public_key: [u8; 32],
    pub staked_xpx: u64,
    pub claimed_rewards: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct DeFiPoolLedger {
    pub total_staked: u64,
    pub total_rewards_distributed: u64,
    pub last_updated_height: u64,
    pub positions: Vec<StakerPosition>,
}

/// Binary Call Parameter ABI for `defi_stake_pool`:
///   byte[0] = op_code:
///     1 => DepositStake:  byte[1..33] = staker_pubkey (32B), byte[33..41] = stake_amount (u64 LE)
///     2 => DistributeAndSettle: byte[1..9] = reward_pool_amount (u64 LE)
#[no_mangle]
pub unsafe extern "C" fn defi_stake_pool() -> u32 {
    let params = blockchain::get_call_params();
    if params.is_empty() {
        return 301;
    }

    let height = blockchain::get_block_height();
    let _ = create_dir("defi_pool");
    let ledger_path = "defi_pool/ledger.bin";

    let mut ledger: DeFiPoolLedger = if path_exists(ledger_path) {
        if let Ok(file) = FileReader::new(ledger_path) {
            let mut reader = BufReader::new(file);
            let mut raw = Vec::new();
            if reader.read_to_end(&mut raw).is_ok() {
                bincode::deserialize(&raw).unwrap_or_default()
            } else {
                DeFiPoolLedger::default()
            }
        } else {
            DeFiPoolLedger::default()
        }
    } else {
        DeFiPoolLedger::default()
    };

    let op_code = params[0];
    match op_code {
        // Op 1: Register or increase stake for a user
        1 => {
            if params.len() < 41 {
                return 302;
            }
            let mut staker_pk = [0u8; 32];
            staker_pk.copy_from_slice(&params[1..33]);
            let amount = u64::from_le_bytes(params[33..41].try_into().unwrap_or([0; 8]));
            if amount == 0 {
                return 303;
            }

            if let Some(pos) = ledger.positions.iter_mut().find(|p| p.public_key == staker_pk) {
                pos.staked_xpx = pos.staked_xpx.saturating_add(amount);
            } else {
                ledger.positions.push(StakerPosition {
                    public_key: staker_pk,
                    staked_xpx: amount,
                    claimed_rewards: 0,
                });
            }
            ledger.total_staked = ledger.total_staked.saturating_add(amount);
            ledger.last_updated_height = height;
        }
        // Op 2: Distribute yield proportionally across all stakers & emit multi-tx batch payout
        2 => {
            if params.len() < 9 || ledger.total_staked == 0 || ledger.positions.is_empty() {
                return 304;
            }
            let reward_pool = u64::from_le_bytes(params[1..9].try_into().unwrap_or([0; 8]));
            let total_staked = ledger.total_staked;

            let mut aggregate_tx = AggregateTransaction::default();
            aggregate_tx.set_max_fee(250_000);

            for pos in ledger.positions.iter_mut() {
                // Proportional reward calculation: (staked_xpx * reward_pool) / total_staked
                let reward = ((pos.staked_xpx as u128 * reward_pool as u128) / (total_staked as u128)) as u64;
                pos.claimed_rewards = pos.claimed_rewards.saturating_add(reward);
                let total_payout = pos.staked_xpx.saturating_add(reward);

                if let Some(tx) = build_transfer_tx(
                    pos.public_key,
                    XPX_MOSAIC_ID,
                    total_payout,
                    b"SiriusStake: Proportional Pool Settlement",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                } else {
                    return 305;
                }
            }

            ledger.total_rewards_distributed = ledger.total_rewards_distributed.saturating_add(reward_pool);
            ledger.last_updated_height = height;
            blockchain::set_transaction(&aggregate_tx);
        }
        _ => return 306,
    }

    // Persist updated Bincode ledger back to Drive
    let encoded = match bincode::serialize(&ledger) {
        Ok(b) => b,
        Err(_) => return 307,
    };
    {
        let file = match FileWriter::new(ledger_path) {
            Ok(f) => f,
            Err(_) => return 308,
        };
        let mut writer = BufWriter::new(file);
        if writer.write(&encoded).is_err() || writer.flush().is_err() {
            return 309;
        }
    }

    // Return total number of active stakers in pool
    ledger.positions.len() as u32
}

// =============================================================================
// 5. Advanced Test 2: Hierarchical Directory Iterator & Sharded Ledger Audit
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn dir_iterator_audit() -> u32 {
    if create_dir("shards/eu").is_err()
        || create_dir("shards/us").is_err()
        || create_dir("shards/ap/tier2").is_err()
    {
        return 401;
    }

    let shard_files: [(&str, u64); 3] = [
        ("shards/eu/validator_1.bin", 125_000_000),
        ("shards/us/validator_2.bin", 250_000_000),
        ("shards/ap/tier2/validator_3.bin", 625_000_000),
    ];

    for (path, balance) in shard_files.iter() {
        let file = match FileWriter::new(path) {
            Ok(f) => f,
            Err(_) => return 402,
        };
        let mut writer = BufWriter::new(file);
        if writer.write(&balance.to_le_bytes()).is_err() || writer.flush().is_err() {
            return 403;
        }
    }

    // Traverse `shards` recursively via DirIterator (`create_dir_iterator`, `has_next_dir_iterator`, `next_dir_iterator`, `destroy_dir_iterator`)
    let mut discovered_entries = 0u32;
    let mut max_depth_seen = 0u32;
    let mut name_checksum = 0u32;

    {
        let iter = DirIterator::new("shards", true);
        for entry in iter {
            discovered_entries += 1;
            if entry.depth > max_depth_seen {
                max_depth_seen = entry.depth;
            }
            for b in entry.name.as_bytes() {
                name_checksum = name_checksum.wrapping_add(*b as u32);
            }
        }
    } // `Drop` automatically invokes `destroy_dir_iterator`

    // Verify all 3 shard balances sum to 1,000.000000 XPX
    let mut total_shard_balance = 0u64;
    for (path, _) in shard_files.iter() {
        let file = match FileReader::new(path) {
            Ok(f) => f,
            Err(_) => return 404,
        };
        let mut reader = BufReader::new(file);
        let mut buf = [0u8; 8];
        if reader.read_exact(&mut buf).is_err() {
            return 405;
        }
        total_shard_balance += u64::from_le_bytes(buf);
    }

    if total_shard_balance != 1_000_000_000 {
        return 406;
    }

    // Expected: 7 entries (`ap`, `tier2`, `validator_3.bin`, `eu`, `validator_1.bin`, `us`, `validator_2.bin`), max_depth = 2
    if discovered_entries != 7 || max_depth_seen != 2 {
        return 407;
    }

    discovered_entries * 1000 + max_depth_seen * 100 + (name_checksum % 100)
}

// =============================================================================
// 6. Advanced Test 3: Large Multi-Chunk Streaming I/O (48 KB across 3 chunks)
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn large_stream_io() -> u32 {
    // 48 KB = 49,152 bytes (3x the 16,384-byte STREAM_BUFFER_SIZE)
    const TOTAL_BYTES: usize = 49_152;
    let mut payload = vec![0u8; TOTAL_BYTES];
    for i in 0..TOTAL_BYTES {
        payload[i] = ((i * 31 + 17) & 0xFF) as u8;
    }

    {
        let file = match FileWriter::new("large_dataset.bin") {
            Ok(f) => f,
            Err(_) => return 501,
        };
        let mut writer = BufWriter::with_capacity(32_768, file);
        if writer.write_all(&payload).is_err() || writer.flush().is_err() {
            return 502;
        }
    }

    if file_size("large_dataset.bin").unwrap_or(0) != TOTAL_BYTES as u64 {
        return 503;
    }

    let mut read_back = Vec::with_capacity(TOTAL_BYTES);
    {
        let file = match FileReader::new("large_dataset.bin") {
            Ok(f) => f,
            Err(_) => return 504,
        };
        let mut reader = BufReader::new(file);
        if reader.read_to_end(&mut read_back).is_err() {
            return 505;
        }
    }

    if read_back.len() != TOTAL_BYTES || read_back != payload {
        return 506;
    }

    // Compute FNV-1a 32-bit digest of the 48 KB stream to prove byte-exact integrity
    let mut hash: u32 = 0x811c9dc5;
    for b in read_back.iter() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }

    // Clean up temporary 48KB file after verification so Drive remains compact
    if remove_filesystem_entry("large_dataset.bin").is_err() {
        return 507;
    }

    hash
}

// =============================================================================
// 7. Advanced Test 4: Full Blockchain Context Audit & Multi-Mosaic Atomic Swap
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn atomic_swap_audit() -> u32 {
    let height = blockchain::get_block_height();
    let block_hash = blockchain::get_block_hash();
    let block_time = blockchain::get_block_time();
    let gen_time = blockchain::get_block_generation_time();
    let tx_hash = blockchain::get_transaction_hash();
    let caller_pk = blockchain::get_caller_public_key();
    let contract_pk = blockchain::get_contract_public_key();
    let exec_payment = blockchain::get_execution_payment();
    let dl_payment = blockchain::get_download_payment();
    let service_payments = blockchain::get_service_payments();

    // Validate all blockchain context invariants
    if height == 0 || block_time == 0 || gen_time == 0 {
        return 601;
    }
    if block_hash == [0u8; 32] || tx_hash == [0u8; 32] || caller_pk == [0u8; 32] || contract_pk == [0u8; 32] {
        return 602;
    }
    if exec_payment == 0 || dl_payment == 0 {
        return 603;
    }

    let xpx_in = service_payments
        .iter()
        .filter(|p| p.mosaic_id == XPX_MOSAIC_ID)
        .map(|p| p.amount)
        .sum::<u64>();

    if xpx_in == 0 {
        return 604;
    }

    // Atomic Swap Rate: 1 XPX -> 10 DEFI_MOSAIC + 10% XPX cashback rebate
    let defi_out = xpx_in * 10;
    let xpx_rebate = xpx_in / 10;

    let tx_leg_1 = match build_transfer_tx(caller_pk, DEFI_MOSAIC_ID, defi_out, b"AtomicSwap: DEFI Token Delivery") {
        Some(tx) => tx,
        None => return 605,
    };
    let tx_leg_2 = match build_transfer_tx(caller_pk, XPX_MOSAIC_ID, xpx_rebate, b"AtomicSwap: 10% XPX Rebate") {
        Some(tx) => tx,
        None => return 606,
    };

    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(150_000);
    aggregate_tx.add_embedded_transaction(tx_leg_1);
    aggregate_tx.add_embedded_transaction(tx_leg_2);
    blockchain::set_transaction(&aggregate_tx);

    // Return deterministic digest of block_hash[0..4] ^ tx_hash[0..4]
    let bh_prefix = u32::from_le_bytes(block_hash[0..4].try_into().unwrap_or([0; 4]));
    let th_prefix = u32::from_le_bytes(tx_hash[0..4].try_into().unwrap_or([0; 4]));
    bh_prefix ^ th_prefix
}

// =============================================================================
// 8. Advanced Test 5: Sandbox Corruption + Out-Of-Gas Trap (Verifies Rollback)
// =============================================================================
#[no_mangle]
pub unsafe extern "C" fn corrupt_and_trap() -> u32 {
    // 1. Overwrite critical persistent files inside the uncommitted sandbox overlay
    if let Ok(mut file) = FileWriter::new("vault_state.bin") {
        let _ = file.write_all(b"CORRUPTED_BY_FAULTY_CONTRACT_EXECUTION");
        let _ = file.flush();
    }
    let _ = remove_filesystem_entry("defi_pool");

    // 2. Deliberately exhaust gas limit in an infinite host-call loop so execution fails
    loop {
        let _ = blockchain::get_block_height();
    }
}
