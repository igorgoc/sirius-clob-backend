//! SiriusCLOB — Autonomous On-Chain Central Limit Order Book DEX for Sirius SuperContract V2
//! Target: wasm32-unknown-unknown for Sirius Catapult gRPC WASM VM
//!
//! Exports:
//!   - `run()`               -> Universal dispatcher handling binary call parameters
//!   - `clob_place_order()`  -> Places a Limit, IOC, or PostOnly order and executes matching
//!   - `clob_cancel_order()` -> Cancels a resting limit order and refunds escrow
//!   - `clob_get_depth()`    -> Generates orderbook snapshot and writes to disk

pub mod orderbook;
pub mod storage;
pub mod types;

use orderbook::MarketPair;
use sdk::blockchain::{self, AggregateTransaction, EmbeddedTransaction};
use serde::{Deserialize, Serialize};
use storage::{
    canonical_to_network_mosaic, canonicalize_mosaic_id, load_balances, load_or_init_market,
    record_trades, save_balances, save_market_state, BNB_MOSAIC_ID, USDT_MOSAIC_ID, XPX_MOSAIC_ID,
};
use types::*;

/// Mosaic represents a token on Sirius Chain (XPX, USDT, etc.)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mosaic {
    pub mosaic_id: u64,
    pub amount: u64,
}

/// Constructs an Embedded TransferTransaction (Entity_Type_Transfer = 0x4154, version 3)
pub fn build_transfer_tx(
    receiver: [u8; 32],
    mosaic_id: u64,
    amount: u64,
    message: &[u8],
) -> Option<EmbeddedTransaction> {
    if amount == 0 {
        return None;
    }
    let mut embedded_tx = EmbeddedTransaction::default();
    embedded_tx.set_entity_type(0x4154);
    embedded_tx.set_version(3);

    // Translate canonical or raw mosaic to live on-chain Testnet 2 / Network Mosaic ID
    let network_mosaic = canonical_to_network_mosaic(mosaic_id);
    let mosaic_count = 1u64.to_le_bytes();
    let msg_size = (message.len() as u64).to_le_bytes();

    let token = Mosaic { mosaic_id: network_mosaic, amount };
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

/// Operation Codes for binary call params
pub const OP_PLACE_ORDER: u8 = 1;
pub const OP_CANCEL_ORDER: u8 = 2;
pub const OP_GET_DEPTH: u8 = 3;
pub const OP_DEPOSIT: u8 = 4;
pub const OP_WITHDRAW: u8 = 5;
pub const OP_CANCEL_ALL: u8 = 6;
pub const OP_BATCH_CANCEL: u8 = 7;
pub const OP_EMERGENCY_UNLOCK: u8 = 8;

/// Core Order Placement Handler
pub fn handle_place_order(params: &[u8]) -> u32 {
    // ABI Resiliency: only strip leading OP_PLACE_ORDER if total length matches prefixed frame (19 or 27 bytes)
    let p = if (params.len() == 19 || params.len() == 27) && params[0] == OP_PLACE_ORDER {
        &params[1..]
    } else {
        params
    };

    if p.len() < 18 {
        return 101; // Invalid param length
    }

    let side = match p[0] {
        0 => OrderSide::Buy,
        1 => OrderSide::Sell,
        _ => return 102, // Invalid side
    };

    let order_type = match p[1] {
        0 => OrderType::Limit,
        1 => OrderType::IOC,
        2 => OrderType::PostOnly,
        _ => return 103, // Invalid order type
    };

    let price = u64::from_le_bytes(p[2..10].try_into().unwrap_or([0; 8]));
    let amount = u64::from_le_bytes(p[10..18].try_into().unwrap_or([0; 8]));

    let explicit_quote = if p.len() >= 26 {
        u64::from_le_bytes(p[18..26].try_into().unwrap_or([0; 8]))
    } else {
        0
    };

    let caller = blockchain::get_caller_public_key();
    let timestamp = blockchain::get_block_time();

    let payments = blockchain::get_service_payments();

    let quote_mosaic_id = if explicit_quote != 0 {
        explicit_quote
    } else if payments.iter().any(|pm| pm.mosaic_id == BNB_MOSAIC_ID) {
        BNB_MOSAIC_ID
    } else {
        USDT_MOSAIC_ID
    };

    // Whitelist check: strictly only USDT and BNB quote mosaics are supported
    if quote_mosaic_id != USDT_MOSAIC_ID && quote_mosaic_id != BNB_MOSAIC_ID {
        return 105; // Unsupported quote mosaic ID
    }

    let (required_mosaic, required_amount) = match side {
        OrderSide::Buy => (quote_mosaic_id, MarketPair::calculate_quote_amount(amount, price)),
        OrderSide::Sell => (XPX_MOSAIC_ID, amount),
    };

    if price == 0 || amount == 0 || required_amount == 0 || required_amount == u64::MAX {
        return 104; // Zero price, amount, sub-atomic truncation, or saturated quote overflow
    }

    // Verify service payments and free trading balance in on-chain BalanceLedger (saturating sum to prevent u64 wrap)
    let paid = payments
        .iter()
        .filter(|p| p.mosaic_id == required_mosaic)
        .fold(0u64, |acc, p| acc.saturating_add(p.amount));

    let mut ledger = load_balances();
    if paid > 0 {
        ledger.deposit(caller, required_mosaic, paid);
    }

    let (free_bal, _) = ledger.get_balance(&caller, required_mosaic);

    if free_bal < required_amount {
        // If partial tokens were sent, refund them immediately so they are never lost
        if paid > 0 {
            let _ = ledger.withdraw(&caller, required_mosaic, paid);
            let mut refund_agg = AggregateTransaction::default();
            refund_agg.set_max_fee(100_000);
            if let Some(tx) = build_transfer_tx(
                caller,
                required_mosaic,
                paid,
                b"SiriusCLOB: Insufficient Escrow Refund",
            ) {
                refund_agg.add_embedded_transaction(tx);
                blockchain::set_transaction(&refund_agg);
            }
        }
        let _ = save_balances(&ledger);
        return 301; // Insufficient escrow deposit
    }

    // Lock required amount from free balance into order escrow
    let _ = ledger.lock(&caller, required_mosaic, required_amount);
    let _ = save_balances(&ledger);

    // Load market state from Replicator Drive
    let mut market = match load_or_init_market(quote_mosaic_id) {
        Ok(m) => m,
        Err(_) => {
            let _ = ledger.unlock(&caller, required_mosaic, required_amount);
            let _ = save_balances(&ledger);
            return 105;
        }
    };

    let result = match market.place_order(caller, side, order_type, price, amount, timestamp) {
        Ok(res) => res,
        Err(_) => {
            let _ = ledger.unlock(&caller, required_mosaic, required_amount);
            let _ = save_balances(&ledger);
            // If the matching engine rejects (e.g. lot size, tick size), refund 100% of deposited tokens
            if paid > 0 {
                let mut refund_agg = AggregateTransaction::default();
                refund_agg.set_max_fee(100_000);
                if let Some(tx) = build_transfer_tx(
                    caller,
                    required_mosaic,
                    paid,
                    b"SiriusCLOB: Order Rejected Refund",
                ) {
                    refund_agg.add_embedded_transaction(tx);
                    blockchain::set_transaction(&refund_agg);
                }
            }
            return 201; // Engine rejected order
        }
    };

    // Save updated orderbook state
    if save_market_state(&market).is_err() {
        let _ = ledger.unlock(&caller, required_mosaic, required_amount);
        let _ = save_balances(&ledger);
        return 105; // Failed to persist state
    }

    // Record trade history
    if !result.fills.is_empty() {
        let _ = record_trades(market.quote_mosaic_id, &result.fills);
    }

    // Prepare settlement transfers
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);

    for fill in &result.fills {
        match fill.taker_side {
            OrderSide::Buy => {
                let _ = ledger.release_locked(&fill.taker_pubkey, market.quote_mosaic_id, fill.quote_amount);
                let _ = ledger.release_locked(&fill.maker_pubkey, market.base_mosaic_id, fill.base_amount);

                // Taker (Buyer) receives Base (XPX minus taker fee)
                let net_base = match fill.base_amount.checked_sub(fill.taker_fee) {
                    Some(val) => val,
                    None => return 305, // Fee exceeds base amount underflow error
                };
                let _ = ledger.deposit(fill.taker_pubkey, market.base_mosaic_id, net_base);
                let base_memo = format!("SiriusCLOB: Trade Fill (Base):FEE={}:TAKER_FEE={}", fill.taker_fee, fill.taker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.taker_pubkey,
                    market.base_mosaic_id,
                    net_base,
                    base_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
                // Maker (Seller) receives Quote (USDT minus maker fee)
                let net_quote = match fill.quote_amount.checked_sub(fill.maker_fee) {
                    Some(val) => val,
                    None => return 305, // Fee exceeds quote amount underflow error
                };
                let _ = ledger.deposit(fill.maker_pubkey, market.quote_mosaic_id, net_quote);
                let quote_memo = format!("SiriusCLOB: Trade Fill (Quote):FEE={}:MAKER_FEE={}", fill.maker_fee, fill.maker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.maker_pubkey,
                    market.quote_mosaic_id,
                    net_quote,
                    quote_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }
            OrderSide::Sell => {
                let _ = ledger.release_locked(&fill.taker_pubkey, market.base_mosaic_id, fill.base_amount);
                let _ = ledger.release_locked(&fill.maker_pubkey, market.quote_mosaic_id, fill.quote_amount);

                // Maker (Buyer) receives Base (XPX minus maker fee)
                let net_base = match fill.base_amount.checked_sub(fill.maker_fee) {
                    Some(val) => val,
                    None => return 305, // Fee exceeds base amount underflow error
                };
                let _ = ledger.deposit(fill.maker_pubkey, market.base_mosaic_id, net_base);
                let base_memo = format!("SiriusCLOB: Trade Fill (Base):FEE={}:MAKER_FEE={}", fill.maker_fee, fill.maker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.maker_pubkey,
                    market.base_mosaic_id,
                    net_base,
                    base_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
                // Taker (Seller) receives Quote (USDT minus taker fee)
                let net_quote = match fill.quote_amount.checked_sub(fill.taker_fee) {
                    Some(val) => val,
                    None => return 305, // Fee exceeds quote amount underflow error
                };
                let _ = ledger.deposit(fill.taker_pubkey, market.quote_mosaic_id, net_quote);
                let quote_memo = format!("SiriusCLOB: Trade Fill (Quote):FEE={}:TAKER_FEE={}", fill.taker_fee, fill.taker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.taker_pubkey,
                    market.quote_mosaic_id,
                    net_quote,
                    quote_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }
        }
    }

    // Overpayment protection: if caller attached more than required, refund excess immediately
    let excess_deposit = match paid.checked_sub(required_amount) {
        Some(val) => val,
        None => 0,
    };

    // Exact Escrow Refunds (IOC unspent, price improvement, STP, and excess deposit)
    let total_refund_base = result.refund_base + if side == OrderSide::Sell { excess_deposit } else { 0 };
    let total_refund_quote = result.refund_quote + if side == OrderSide::Buy { excess_deposit } else { 0 };

    if total_refund_base > 0 {
        let _ = ledger.unlock(&caller, market.base_mosaic_id, total_refund_base);
        if let Some(tx) = build_transfer_tx(
            caller,
            market.base_mosaic_id,
            total_refund_base,
            b"SiriusCLOB: Base Escrow Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
        }
    }

    if total_refund_quote > 0 {
        let _ = ledger.unlock(&caller, market.quote_mosaic_id, total_refund_quote);
        if let Some(tx) = build_transfer_tx(
            caller,
            market.quote_mosaic_id,
            total_refund_quote,
            b"SiriusCLOB: Quote Escrow Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
        }
    }

    if save_balances(&ledger).is_err() {
        return 105; // Do not emit settlement transactions if balance ledger failed to persist
    }

    // Emit settlement transactions if any were generated
    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0 // Success
}

/// Core Order Cancellation Handler
pub fn handle_cancel_order(params: &[u8]) -> u32 {
    let p = if (params.len() == 9 || params.len() == 17) && params[0] == OP_CANCEL_ORDER {
        &params[1..]
    } else {
        params
    };

    if p.len() < 8 {
        return 101; // Invalid param length
    }

    let order_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
    let explicit_quote = if p.len() >= 16 {
        u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]))
    } else {
        0
    };
    let caller = blockchain::get_caller_public_key();

    let mut market = if explicit_quote != 0 {
        match load_or_init_market(explicit_quote) {
            Ok(m) => m,
            Err(_) => return 105,
        }
    } else {
        // Search in USDT market first, then BNB market
        let m_usdt = match load_or_init_market(USDT_MOSAIC_ID) {
            Ok(m) => m,
            Err(_) => return 105,
        };
        if m_usdt.bids.values().any(|lvl| lvl.orders.iter().any(|o| o.order_id == order_id))
            || m_usdt.asks.values().any(|lvl| lvl.orders.iter().any(|o| o.order_id == order_id))
        {
            m_usdt
        } else {
            match load_or_init_market(BNB_MOSAIC_ID) {
                Ok(m) => m,
                Err(_) => return 105,
            }
        }
    };

    let (side, refund_amount, _price) = match market.cancel_order(order_id, caller) {
        Ok(res) => res,
        Err(_) => return 202, // Order not found or unauthorized
    };

    if save_market_state(&market).is_err() {
        return 105;
    }

    // Emit refund transaction for unspent escrow and release in ledger
    if refund_amount > 0 {
        let refund_mosaic = match side {
            OrderSide::Buy => market.quote_mosaic_id,
            OrderSide::Sell => market.base_mosaic_id,
        };

        let mut ledger = load_balances();
        let _ = ledger.unlock(&caller, refund_mosaic, refund_amount);
        if save_balances(&ledger).is_err() {
            return 105;
        }

        let mut aggregate_tx = AggregateTransaction::default();
        aggregate_tx.set_max_fee(100_000);

        if let Some(tx) = build_transfer_tx(
            caller,
            refund_mosaic,
            refund_amount,
            b"SiriusCLOB: Order Cancellation Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
            blockchain::set_transaction(&aggregate_tx);
        }
    }

    0
}

/// Core Deposit Handler
pub fn handle_deposit(params: &[u8]) -> u32 {
    let caller = blockchain::get_caller_public_key();
    let payments = blockchain::get_service_payments();

    let mut ledger = load_balances();
    let mut credited = 0usize;

    // 1. Process servicePayments attached to the invocation
    if !payments.is_empty() {
        for pm in &payments {
            if pm.amount > 0 {
                if let Some(canonical_id) = canonicalize_mosaic_id(pm.mosaic_id) {
                    ledger.deposit(caller, canonical_id, pm.amount);
                    credited += 1;
                }
            }
        }
    }

    // 2. Fallback: Parse binary deposit parameters if service payments array is empty
    // Format: [OP_DEPOSIT (1 byte), mosaic_id (8 bytes LE), amount (8 bytes LE)]
    if credited == 0 && params.len() >= 16 {
        let p = if !params.is_empty() && params[0] == OP_DEPOSIT {
            &params[1..]
        } else {
            params
        };
        if p.len() >= 16 {
            let mosaic_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
            let amount = u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]));
            if amount > 0 {
                if let Some(canonical_id) = canonicalize_mosaic_id(mosaic_id) {
                    ledger.deposit(caller, canonical_id, amount);
                    credited += 1;
                }
            }
        }
    }

    if credited == 0 {
        // Autonomous Trustless Safety: If uncredited/unrecognized payments were attached, refund immediately to caller
        if !payments.is_empty() {
            let mut refund_agg = AggregateTransaction::default();
            refund_agg.set_max_fee(100_000);
            for pm in &payments {
                if pm.amount > 0 {
                    if let Some(tx) = build_transfer_tx(
                        caller,
                        pm.mosaic_id,
                        pm.amount,
                        b"SiriusCLOB: Deposit Rejected Refund",
                    ) {
                        refund_agg.add_embedded_transaction(tx);
                    }
                }
            }
            if !refund_agg.get_embedded_transactions().is_empty() {
                blockchain::set_transaction(&refund_agg);
            }
        }
        return 303; // Zero deposit amount or unrecognized mosaic
    }

    if save_balances(&ledger).is_err() {
        return 105; // Failed to persist state
    }

    0
}

/// Core Withdrawal Handler
pub fn handle_withdraw(params: &[u8]) -> u32 {
    let p = if !params.is_empty() && params[0] == OP_WITHDRAW {
        &params[1..]
    } else {
        params
    };

    if p.len() < 16 {
        return 101; // Invalid param length
    }

    let raw_mosaic_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
    let amount = u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]));

    if amount == 0 {
        return 104; // Zero amount
    }

    // Whitelist check: strictly only XPX, USDT, BNB mosaics are allowed
    let mosaic_id = match canonicalize_mosaic_id(raw_mosaic_id) {
        Some(id) => id,
        None => return 105, // Unsupported mosaic ID
    };

    let caller = blockchain::get_caller_public_key();
    let mut ledger = load_balances();

    if ledger.withdraw(&caller, mosaic_id, amount).is_err() {
        return 302; // Insufficient free balance
    }

    if save_balances(&ledger).is_err() {
        return 105;
    }

    // Emit on-chain transfer from contract vault to caller
    let mut withdraw_agg = AggregateTransaction::default();
    withdraw_agg.set_max_fee(100_000);
    if let Some(tx) = build_transfer_tx(
        caller,
        mosaic_id,
        amount,
        b"SiriusCLOB: Vault Withdrawal",
    ) {
        withdraw_agg.add_embedded_transaction(tx);
        blockchain::set_transaction(&withdraw_agg);
    }

    0
}

/// Core Depth Query Handler
pub fn handle_get_depth(params: &[u8]) -> u32 {
    let p = if (params.len() == 9 || params.len() == 17) && params[0] == OP_GET_DEPTH {
        &params[1..]
    } else {
        params
    };

    let max_levels = if p.len() >= 8 {
        u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8])) as usize
    } else {
        50 // default to 50 levels
    };

    let explicit_quote = if p.len() >= 16 {
        u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]))
    } else {
        0
    };

    let quotes_to_sync = if explicit_quote != 0 {
        vec![explicit_quote]
    } else {
        vec![USDT_MOSAIC_ID, BNB_MOSAIC_ID]
    };

    for q in quotes_to_sync {
        let market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let depth = market.get_depth(max_levels);

        // Save JSON depth snapshot for public UI consumption
        if let Ok(json_depth) = serde_json::to_vec(&depth) {
            use sdk::file::FileWriter;
            use sdk::filesystem::{create_dir, path_exists};
            use std::io::Write;

            if let Ok((market_dir, _, _, _, depth_json)) = storage::get_market_paths(q) {
                if !path_exists(market_dir) {
                    let _ = create_dir(market_dir);
                }
                if let Ok(mut writer) = FileWriter::new(depth_json) {
                    let _ = writer.write_all(&json_depth);
                    let _ = writer.flush();
                }
            }
        }
    }

    0
}

/// Core Cancel All Handler
pub fn handle_cancel_all(params: &[u8]) -> u32 {
    let p = if !params.is_empty() && params[0] == OP_CANCEL_ALL {
        &params[1..]
    } else {
        params
    };

    let explicit_quote = if p.len() >= 8 {
        u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]))
    } else {
        0
    };

    let caller = blockchain::get_caller_public_key();
    let markets_to_process = if explicit_quote != 0 {
        vec![explicit_quote]
    } else {
        vec![USDT_MOSAIC_ID, BNB_MOSAIC_ID]
    };

    let mut ledger = load_balances();
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);
    let mut _total_cancelled = 0usize;

    for q in markets_to_process {
        let mut market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let res = market.cancel_all(caller);
        if !res.cancelled_order_ids.is_empty() {
            _total_cancelled += res.cancelled_order_ids.len();
            let _ = save_market_state(&market);

            if res.total_refund_base > 0 {
                let _ = ledger.unlock(&caller, market.base_mosaic_id, res.total_refund_base);
                if let Some(tx) = build_transfer_tx(
                    caller,
                    market.base_mosaic_id,
                    res.total_refund_base,
                    b"SiriusCLOB: Cancel All Base Refund",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }

            if res.total_refund_quote > 0 {
                let _ = ledger.unlock(&caller, market.quote_mosaic_id, res.total_refund_quote);
                if let Some(tx) = build_transfer_tx(
                    caller,
                    market.quote_mosaic_id,
                    res.total_refund_quote,
                    b"SiriusCLOB: Cancel All Quote Refund",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }
        }
    }

    let _ = save_balances(&ledger);

    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0
}

/// Core Batch Cancel Handler
pub fn handle_batch_cancel(params: &[u8]) -> u32 {
    let p = if !params.is_empty() && params[0] == OP_BATCH_CANCEL {
        &params[1..]
    } else {
        params
    };

    if p.len() < 8 {
        return 101; // Invalid param length
    }

    let caller = blockchain::get_caller_public_key();

    // Parse payload: supports [quote_mosaic_id: u64, order_count: u64, order_ids...]
    // or [quote_mosaic_id: u64, order_ids...] or [order_ids...]
    let mut explicit_quote = 0u64;
    let mut order_ids = Vec::new();

    if p.len() >= 16 {
        let first_u64 = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
        let second_u64 = u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]));

        if p.len() == 16 + (second_u64 as usize) * 8 {
            // Format: [quote_mosaic_id: 8 bytes, count: 8 bytes, order_ids...]
            explicit_quote = first_u64;
            let count = second_u64 as usize;
            for i in 0..count {
                let start = 16 + i * 8;
                let oid = u64::from_le_bytes(p[start..start + 8].try_into().unwrap_or([0; 8]));
                order_ids.push(oid);
            }
        } else if (p.len() - 8) % 8 == 0 && (first_u64 == USDT_MOSAIC_ID || first_u64 == BNB_MOSAIC_ID || first_u64 == 0) {
            // Format: [quote_mosaic_id: 8 bytes, order_ids...]
            explicit_quote = first_u64;
            let mut offset = 8;
            while offset + 8 <= p.len() {
                let oid = u64::from_le_bytes(p[offset..offset + 8].try_into().unwrap_or([0; 8]));
                order_ids.push(oid);
                offset += 8;
            }
        } else if p.len() % 8 == 0 {
            // Format: [order_ids...]
            let mut offset = 0;
            while offset + 8 <= p.len() {
                let oid = u64::from_le_bytes(p[offset..offset + 8].try_into().unwrap_or([0; 8]));
                order_ids.push(oid);
                offset += 8;
            }
        }
    } else if p.len() == 8 {
        let oid = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
        order_ids.push(oid);
    }

    if order_ids.is_empty() {
        return 101; // No orders provided
    }

    let markets_to_process = if explicit_quote != 0 {
        vec![explicit_quote]
    } else {
        vec![USDT_MOSAIC_ID, BNB_MOSAIC_ID]
    };

    let mut ledger = load_balances();
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);
    let mut total_cancelled = 0usize;

    for q in markets_to_process {
        let mut market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let res = market.cancel_batch(&order_ids, caller);
        if !res.cancelled_order_ids.is_empty() {
            total_cancelled += res.cancelled_order_ids.len();
            let _ = save_market_state(&market);

            if res.total_refund_base > 0 {
                let _ = ledger.unlock(&caller, market.base_mosaic_id, res.total_refund_base);
                if let Some(tx) = build_transfer_tx(
                    caller,
                    market.base_mosaic_id,
                    res.total_refund_base,
                    b"SiriusCLOB: Batch Cancel Base Refund",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }

            if res.total_refund_quote > 0 {
                let _ = ledger.unlock(&caller, market.quote_mosaic_id, res.total_refund_quote);
                if let Some(tx) = build_transfer_tx(
                    caller,
                    market.quote_mosaic_id,
                    res.total_refund_quote,
                    b"SiriusCLOB: Batch Cancel Quote Refund",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }
        }
    }

    if total_cancelled == 0 {
        return 202; // Order not found or unauthorized
    }

    let _ = save_balances(&ledger);

    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0
}

/// Core Emergency Direct On-Chain Unlock Handler
/// Cancels all resting orders and refunds 100% of caller's free balances directly on-chain
pub fn handle_emergency_unlock() -> u32 {
    let caller = blockchain::get_caller_public_key();
    let markets_to_process = [USDT_MOSAIC_ID, BNB_MOSAIC_ID];

    let mut ledger = load_balances();
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(200_000);

    // 1. Cancel all resting orders across all markets and unlock escrow
    for q in markets_to_process {
        if let Ok(mut market) = load_or_init_market(q) {
            let res = market.cancel_all(caller);
            if !res.cancelled_order_ids.is_empty() {
                let _ = save_market_state(&market);
                if res.total_refund_base > 0 {
                    let _ = ledger.unlock(&caller, market.base_mosaic_id, res.total_refund_base);
                }
                if res.total_refund_quote > 0 {
                    let _ = ledger.unlock(&caller, market.quote_mosaic_id, res.total_refund_quote);
                }
            }
        }
    }

    // 2. Refund all free balances for caller across XPX, USDT, BNB
    let supported_mosaics = [XPX_MOSAIC_ID, USDT_MOSAIC_ID, BNB_MOSAIC_ID];
    for &m in &supported_mosaics {
        let (free, _) = ledger.get_balance(&caller, m);
        if free > 0 {
            if ledger.withdraw(&caller, m, free).is_ok() {
                if let Some(tx) = build_transfer_tx(
                    caller,
                    m,
                    free,
                    b"SiriusCLOB: Emergency Direct On-Chain Unlock",
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
            }
        }
    }

    let _ = save_balances(&ledger);

    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0
}

// =============================================================================
// Exported WASM Entrypoints
// =============================================================================

#[no_mangle]
pub unsafe extern "C" fn clob_place_order() -> u32 {
    let params = blockchain::get_call_params();
    handle_place_order(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_cancel_order() -> u32 {
    let params = blockchain::get_call_params();
    handle_cancel_order(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_get_depth() -> u32 {
    let params = blockchain::get_call_params();
    handle_get_depth(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_deposit() -> u32 {
    let params = blockchain::get_call_params();
    handle_deposit(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_withdraw() -> u32 {
    let params = blockchain::get_call_params();
    handle_withdraw(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_cancel_all() -> u32 {
    let params = blockchain::get_call_params();
    handle_cancel_all(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_batch_cancel() -> u32 {
    let params = blockchain::get_call_params();
    handle_batch_cancel(&params)
}

#[no_mangle]
pub unsafe extern "C" fn clob_emergency_unlock() -> u32 {
    handle_emergency_unlock()
}

/// Universal Dispatcher Entrypoint (`run`)
#[no_mangle]
pub unsafe extern "C" fn run() -> u32 {
    let params = blockchain::get_call_params();
    if params.is_empty() {
        return 100; // Empty params
    }

    let op_code = params[0];
    let payload = &params[1..];

    match op_code {
        OP_PLACE_ORDER => handle_place_order(payload),
        OP_CANCEL_ORDER => handle_cancel_order(payload),
        OP_GET_DEPTH => handle_get_depth(payload),
        OP_DEPOSIT => handle_deposit(payload),
        OP_WITHDRAW => handle_withdraw(payload),
        OP_CANCEL_ALL => handle_cancel_all(payload),
        OP_BATCH_CANCEL => handle_batch_cancel(payload),
        OP_EMERGENCY_UNLOCK => handle_emergency_unlock(),
        _ => 101, // Unknown op code
    }
}
