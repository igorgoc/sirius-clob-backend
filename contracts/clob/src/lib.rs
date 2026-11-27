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

/// Anti-DoS parameter bounds
pub const MAX_DEPTH_LEVELS: usize = 500;
pub const MAX_BATCH_CANCEL_ORDERS: usize = 500;

/// Helper: emit an on-chain refund for an attached service payment without touching the balance ledger
fn emit_payment_refund(caller: [u8; 32], mosaic_id: u64, paid: u64, memo: &[u8]) {
    if paid == 0 {
        return;
    }
    let mut refund_agg = AggregateTransaction::default();
    refund_agg.set_max_fee(100_000);
    if let Some(tx) = build_transfer_tx(caller, mosaic_id, paid, memo) {
        refund_agg.add_embedded_transaction(tx);
        blockchain::set_transaction(&refund_agg);
    }
}

/// Pure, panic-free parser for OP_BATCH_CANCEL payloads (safe on 32-bit wasm32 usize)
pub fn parse_batch_cancel_params(params: &[u8]) -> Result<(u64, Vec<OrderId>), u32> {
    // Strip leading OP_BATCH_CANCEL ONLY when framed length is 8*k + 1 (prevents double-stripping order_id == 7!)
    let p = if !params.is_empty() && params.len() % 8 == 1 && params[0] == OP_BATCH_CANCEL {
        &params[1..]
    } else {
        params
    };

    if p.len() < 8 || p.len() % 8 != 0 {
        return Err(101);
    }

    let mut explicit_quote = 0u64;
    let mut order_ids = Vec::new();

    if p.len() >= 16 {
        let first_u64 = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
        let second_u64 = u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]));

        // Checked arithmetic prevents 32-bit wasm32 usize truncation and overflow abort
        let counted_frame_match = usize::try_from(second_u64)
            .ok()
            .and_then(|c| c.checked_mul(8))
            .and_then(|b| b.checked_add(16))
            .map(|expected_len| expected_len == p.len())
            .unwrap_or(false);

        if counted_frame_match {
            let count = second_u64 as usize;
            if count == 0 || count > MAX_BATCH_CANCEL_ORDERS {
                return Err(101);
            }
            explicit_quote = first_u64;
            order_ids.reserve(count);
            for i in 0..count {
                let start = 16 + i * 8;
                let oid = u64::from_le_bytes(p[start..start + 8].try_into().unwrap_or([0; 8]));
                order_ids.push(oid);
            }
        } else if first_u64 == 0
            || canonicalize_mosaic_id(first_u64)
                .map(|m| m == USDT_MOSAIC_ID || m == BNB_MOSAIC_ID)
                .unwrap_or(false)
        {
            let count = (p.len() - 8) / 8;
            if count == 0 || count > MAX_BATCH_CANCEL_ORDERS {
                return Err(101);
            }
            explicit_quote = first_u64;
            order_ids.reserve(count);
            let mut offset = 8;
            while offset + 8 <= p.len() {
                let oid = u64::from_le_bytes(p[offset..offset + 8].try_into().unwrap_or([0; 8]));
                order_ids.push(oid);
                offset += 8;
            }
        } else {
            let count = p.len() / 8;
            if count > MAX_BATCH_CANCEL_ORDERS {
                return Err(101);
            }
            order_ids.reserve(count);
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

    if order_ids.is_empty() || order_ids.len() > MAX_BATCH_CANCEL_ORDERS {
        return Err(101);
    }

    if explicit_quote != 0 {
        match canonicalize_mosaic_id(explicit_quote) {
            Some(q) if q == USDT_MOSAIC_ID || q == BNB_MOSAIC_ID => explicit_quote = q,
            _ => return Err(105),
        }
    }

    Ok((explicit_quote, order_ids))
}

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
        match canonicalize_mosaic_id(explicit_quote) {
            Some(q) if q == USDT_MOSAIC_ID || q == BNB_MOSAIC_ID => q,
            _ => return 105, // Unsupported quote mosaic ID
        }
    } else if payments
        .iter()
        .any(|pm| canonicalize_mosaic_id(pm.mosaic_id) == Some(BNB_MOSAIC_ID))
    {
        BNB_MOSAIC_ID
    } else {
        USDT_MOSAIC_ID
    };

    let (required_mosaic, required_amount) = match side {
        OrderSide::Buy => (quote_mosaic_id, MarketPair::calculate_quote_amount(amount, price)),
        OrderSide::Sell => (XPX_MOSAIC_ID, amount),
    };

    if price == 0 || amount == 0 || required_amount == 0 || required_amount == u64::MAX {
        return 104; // Zero price, amount, sub-atomic truncation, or saturated quote overflow
    }

    // Verify service payments matching the required mosaic (canonicalized, saturating sum)
    let paid = payments
        .iter()
        .filter(|pm| canonicalize_mosaic_id(pm.mosaic_id) == Some(required_mosaic))
        .fold(0u64, |acc, pm| acc.saturating_add(pm.amount));

    // Fail-closed load: never wipe balances on corrupt balances.bin
    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => {
            emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Ledger Load Error Refund");
            return 105;
        }
    };

    if paid > 0 {
        if ledger.deposit(caller, required_mosaic, paid).is_err() {
            emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Deposit Overflow Refund");
            return 305;
        }
    }

    let (free_bal, _) = ledger.get_balance(&caller, required_mosaic);

    if free_bal < required_amount {
        // Ledger has NOT been persisted yet, so refunding attached `paid` leaves ledger untouched & consistent
        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Insufficient Escrow Refund");
        return 301; // Insufficient escrow deposit
    }

    // Lock required amount in memory (do NOT persist until market matching & all accounting checks succeed!)
    if ledger.lock(&caller, required_mosaic, required_amount).is_err() {
        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Lock Error Refund");
        return 301;
    }

    // Load market state from Replicator Drive
    let mut market = match load_or_init_market(quote_mosaic_id) {
        Ok(m) => m,
        Err(_) => {
            emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Market Load Error Refund");
            return 105;
        }
    };

    let result = match market.place_order(caller, side, order_type, price, amount, timestamp) {
        Ok(res) => res,
        Err(_) => {
            // Because `ledger` was NEVER persisted with `paid` or the lock, refunding `paid` cannot double-refund!
            emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Order Rejected Refund");
            return 201; // Engine rejected order
        }
    };

    // Prepare settlement transfers & apply ALL ledger mutations in memory BEFORE writing ANY file to disk!
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
                    None => {
                        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Fee Error Refund");
                        return 305;
                    }
                };
                if ledger.deposit(fill.taker_pubkey, market.base_mosaic_id, net_base).is_err() {
                    emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Balance Overflow Refund");
                    return 305;
                }
                let base_memo = format!("SiriusCLOB: Trade Fill (Base):FEE={}:TAKER_FEE={}", fill.taker_fee, fill.taker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.taker_pubkey,
                    market.base_mosaic_id,
                    net_base,
                    base_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
                // Maker (Seller) receives Quote (USDT/BNB minus maker fee)
                let net_quote = match fill.quote_amount.checked_sub(fill.maker_fee) {
                    Some(val) => val,
                    None => {
                        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Fee Error Refund");
                        return 305;
                    }
                };
                if ledger.deposit(fill.maker_pubkey, market.quote_mosaic_id, net_quote).is_err() {
                    emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Balance Overflow Refund");
                    return 305;
                }
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
                    None => {
                        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Fee Error Refund");
                        return 305;
                    }
                };
                if ledger.deposit(fill.maker_pubkey, market.base_mosaic_id, net_base).is_err() {
                    emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Balance Overflow Refund");
                    return 305;
                }
                let base_memo = format!("SiriusCLOB: Trade Fill (Base):FEE={}:MAKER_FEE={}", fill.maker_fee, fill.maker_fee);
                if let Some(tx) = build_transfer_tx(
                    fill.maker_pubkey,
                    market.base_mosaic_id,
                    net_base,
                    base_memo.as_bytes(),
                ) {
                    aggregate_tx.add_embedded_transaction(tx);
                }
                // Taker (Seller) receives Quote (USDT/BNB minus taker fee)
                let net_quote = match fill.quote_amount.checked_sub(fill.taker_fee) {
                    Some(val) => val,
                    None => {
                        emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Fee Error Refund");
                        return 305;
                    }
                };
                if ledger.deposit(fill.taker_pubkey, market.quote_mosaic_id, net_quote).is_err() {
                    emit_payment_refund(caller, required_mosaic, paid, b"SiriusCLOB: Balance Overflow Refund");
                    return 305;
                }
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

    // Unlock any evicted unfillable dust remainders back to their resting order owners
    for &(dust_owner, dust_side, dust_amount) in &result.dust_refunds {
        if dust_amount > 0 {
            let dust_mosaic = match dust_side {
                OrderSide::Buy => market.quote_mosaic_id,
                OrderSide::Sell => market.base_mosaic_id,
            };
            let _ = ledger.unlock(&dust_owner, dust_mosaic, dust_amount);
        }
    }

    // Exact Escrow Refunds (IOC unspent, PostOnly rejection, price improvement, unfillable dust, and STP)
    // NOTE: Unlocked strictly from locked escrow (never mixed with free `excess_deposit`!)
    if result.refund_base > 0 {
        let _ = ledger.unlock(&caller, market.base_mosaic_id, result.refund_base);
        if let Some(tx) = build_transfer_tx(
            caller,
            market.base_mosaic_id,
            result.refund_base,
            b"SiriusCLOB: Base Escrow Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
        }
    }

    if result.refund_quote > 0 {
        let _ = ledger.unlock(&caller, market.quote_mosaic_id, result.refund_quote);
        if let Some(tx) = build_transfer_tx(
            caller,
            market.quote_mosaic_id,
            result.refund_quote,
            b"SiriusCLOB: Quote Escrow Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
        }
    }

    // Overpayment protection: excess `paid > required_amount` sat in `free_amount` (never locked),
    // so withdraw it from `free_amount` BEFORE emitting its on-chain refund transfer!
    let excess_deposit = paid.saturating_sub(required_amount);
    if excess_deposit > 0 {
        if ledger.withdraw(&caller, required_mosaic, excess_deposit).is_ok() {
            if let Some(tx) = build_transfer_tx(
                caller,
                required_mosaic,
                excess_deposit,
                b"SiriusCLOB: Excess Deposit Refund",
            ) {
                aggregate_tx.add_embedded_transaction(tx);
            }
        }
    }

    // Persist market state (skip redundant write on rejected PostOnly) & balance ledger together
    if result.status != OrderStatus::Rejected {
        if save_market_state(&market).is_err() {
            return 105; // Failed to persist market state; balances untouched
        }
    }

    if save_balances(&ledger).is_err() {
        return 105; // Do not emit settlement transactions if balance ledger failed to persist
    }

    // Record trade history
    if !result.fills.is_empty() {
        let _ = record_trades(market.quote_mosaic_id, &result.fills);
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
        let canon_q = match canonicalize_mosaic_id(explicit_quote) {
            Some(q) if q == USDT_MOSAIC_ID || q == BNB_MOSAIC_ID => q,
            _ => return 105,
        };
        match load_or_init_market(canon_q) {
            Ok(m) => m,
            Err(_) => return 105,
        }
    } else {
        // Search in USDT market first, then BNB market
        let m_usdt = match load_or_init_market(USDT_MOSAIC_ID) {
            Ok(m) => m,
            Err(_) => return 105,
        };
        if m_usdt.order_index.contains_key(&order_id) {
            m_usdt
        } else {
            match load_or_init_market(BNB_MOSAIC_ID) {
                Ok(m) => m,
                Err(_) => return 105,
            }
        }
    };

    // Load balance ledger fail-closed BEFORE mutating/saving market state
    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };

    let (side, refund_amount, _price) = match market.cancel_order(order_id, caller) {
        Ok(res) => res,
        Err(_) => return 202, // Order not found or unauthorized
    };

    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);

    if refund_amount > 0 {
        let refund_mosaic = match side {
            OrderSide::Buy => market.quote_mosaic_id,
            OrderSide::Sell => market.base_mosaic_id,
        };
        let _ = ledger.unlock(&caller, refund_mosaic, refund_amount);
        if let Some(tx) = build_transfer_tx(
            caller,
            refund_mosaic,
            refund_amount,
            b"SiriusCLOB: Order Cancellation Refund",
        ) {
            aggregate_tx.add_embedded_transaction(tx);
        }
    }

    if save_market_state(&market).is_err() {
        return 105;
    }

    if refund_amount > 0 && save_balances(&ledger).is_err() {
        return 105;
    }

    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0
}

/// Core Deposit Handler
pub fn handle_deposit(params: &[u8]) -> u32 {
    let caller = blockchain::get_caller_public_key();
    let payments = blockchain::get_service_payments();

    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };
    let mut credited = 0usize;

    // 1. Process servicePayments attached to the invocation
    if !payments.is_empty() {
        for pm in &payments {
            if pm.amount > 0 {
                if let Some(canonical_id) = canonicalize_mosaic_id(pm.mosaic_id) {
                    if ledger.deposit(caller, canonical_id, pm.amount).is_err() {
                        return 305;
                    }
                    credited += 1;
                }
            }
        }
    }

    // 2. Fallback: Parse binary deposit parameters if service payments array is empty
    // Format: [OP_DEPOSIT (1 byte, optional), mosaic_id (8 bytes LE), amount (8 bytes LE)]
    if credited == 0 && params.len() >= 16 {
        let p = if params.len() == 17 && params[0] == OP_DEPOSIT {
            &params[1..]
        } else {
            params
        };
        if p.len() >= 16 {
            let mosaic_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
            let amount = u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]));
            if amount > 0 {
                if let Some(canonical_id) = canonicalize_mosaic_id(mosaic_id) {
                    if ledger.deposit(caller, canonical_id, amount).is_err() {
                        return 305;
                    }
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
    let p = if params.len() == 17 && params[0] == OP_WITHDRAW {
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
    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };

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

    // Safe clamp before usize cast (prevents 32-bit wasm32 truncation and huge allocations)
    let max_levels = if p.len() >= 8 {
        let raw = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
        u64::min(raw, MAX_DEPTH_LEVELS as u64) as usize
    } else {
        50 // default to 50 levels
    };

    let explicit_quote = if p.len() >= 16 {
        u64::from_le_bytes(p[8..16].try_into().unwrap_or([0; 8]))
    } else {
        0
    };

    let quotes_to_sync = if explicit_quote != 0 {
        match canonicalize_mosaic_id(explicit_quote) {
            Some(q) if q == USDT_MOSAIC_ID || q == BNB_MOSAIC_ID => vec![q],
            _ => return 105,
        }
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
    let p = if (params.len() == 1 || params.len() == 9) && params[0] == OP_CANCEL_ALL {
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
        match canonicalize_mosaic_id(explicit_quote) {
            Some(q) if q == USDT_MOSAIC_ID || q == BNB_MOSAIC_ID => vec![q],
            _ => return 105,
        }
    } else {
        vec![USDT_MOSAIC_ID, BNB_MOSAIC_ID]
    };

    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);
    let mut total_cancelled = 0usize;

    for q in markets_to_process {
        let mut market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => return 105,
        };
        let res = market.cancel_all(caller);
        if !res.cancelled_order_ids.is_empty() {
            total_cancelled += res.cancelled_order_ids.len();
            if save_market_state(&market).is_err() {
                return 105;
            }

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

    if total_cancelled > 0 && save_balances(&ledger).is_err() {
        return 105;
    }

    if !aggregate_tx.get_embedded_transactions().is_empty() {
        blockchain::set_transaction(&aggregate_tx);
    }

    0
}

/// Core Batch Cancel Handler
pub fn handle_batch_cancel(params: &[u8]) -> u32 {
    let (explicit_quote, order_ids) = match parse_batch_cancel_params(params) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };

    let caller = blockchain::get_caller_public_key();
    let markets_to_process = if explicit_quote != 0 {
        vec![explicit_quote]
    } else {
        vec![USDT_MOSAIC_ID, BNB_MOSAIC_ID]
    };

    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(100_000);
    let mut total_cancelled = 0usize;

    for q in markets_to_process {
        let mut market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => return 105,
        };
        let res = market.cancel_batch(&order_ids, caller);
        if !res.cancelled_order_ids.is_empty() {
            total_cancelled += res.cancelled_order_ids.len();
            if save_market_state(&market).is_err() {
                return 105;
            }

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

    if save_balances(&ledger).is_err() {
        return 105;
    }

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

    let mut ledger = match load_balances() {
        Ok(l) => l,
        Err(_) => return 105,
    };
    let mut aggregate_tx = AggregateTransaction::default();
    aggregate_tx.set_max_fee(200_000);

    // 1. Cancel all resting orders across all markets and unlock escrow
    for q in markets_to_process {
        let mut market = match load_or_init_market(q) {
            Ok(m) => m,
            Err(_) => return 105,
        };
        let res = market.cancel_all(caller);
        if !res.cancelled_order_ids.is_empty() {
            if save_market_state(&market).is_err() {
                return 105;
            }
            if res.total_refund_base > 0 {
                let _ = ledger.unlock(&caller, market.base_mosaic_id, res.total_refund_base);
            }
            if res.total_refund_quote > 0 {
                let _ = ledger.unlock(&caller, market.quote_mosaic_id, res.total_refund_quote);
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

    if save_balances(&ledger).is_err() {
        return 105;
    }

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

#[cfg(test)]
mod security_tests {
    use super::*;
    use crate::storage::BalanceLedger;

    #[test]
    fn test_batch_cancel_order_id_7_not_double_stripped() {
        assert_eq!(parse_batch_cancel_params(&[]), Err(101));
        assert_eq!(parse_batch_cancel_params(&[OP_BATCH_CANCEL]), Err(101));

        // When run() already stripped OP_BATCH_CANCEL, payload for cancelling Order #7 is 8 bytes starting with 0x07
        let order_7_payload = 7u64.to_le_bytes();
        let (q, ids) = parse_batch_cancel_params(&order_7_payload).expect("Order #7 must NOT be double-stripped");
        assert_eq!(q, 0);
        assert_eq!(ids, vec![7]);

        // Direct call WITH leading OP_BATCH_CANCEL (9 bytes = 8*1 + 1) must strip opcode cleanly
        let mut framed = vec![OP_BATCH_CANCEL];
        framed.extend_from_slice(&7u64.to_le_bytes());
        let (q2, ids2) = parse_batch_cancel_params(&framed).expect("Framed Order #7 must parse cleanly");
        assert_eq!(q2, 0);
        assert_eq!(ids2, vec![7]);
    }

    #[test]
    fn test_batch_cancel_overflow_and_dos_rejected_without_panic() {
        // Malicious count = u64::MAX must NOT overflow 32-bit usize or panic
        let mut evil = Vec::new();
        evil.extend_from_slice(&USDT_MOSAIC_ID.to_le_bytes());
        evil.extend_from_slice(&u64::MAX.to_le_bytes());
        // Does not match counted frame; falls through to [quote_mosaic_id, order_ids...] with 1 id (u64::MAX)
        let parsed = parse_batch_cancel_params(&evil);
        assert!(parsed.is_ok());

        // Malicious huge counted frame > MAX_BATCH_CANCEL_ORDERS must be rejected with 101
        let huge_count = (MAX_BATCH_CANCEL_ORDERS + 5) as u64;
        let mut huge = Vec::with_capacity(16 + (huge_count as usize) * 8);
        huge.extend_from_slice(&USDT_MOSAIC_ID.to_le_bytes());
        huge.extend_from_slice(&huge_count.to_le_bytes());
        for i in 0..huge_count {
            huge.extend_from_slice(&(i + 1).to_le_bytes());
        }
        assert_eq!(parse_batch_cancel_params(&huge), Err(101));

        // Unsupported explicit quote mosaic must be rejected with 105
        let mut bad_quote = Vec::new();
        bad_quote.extend_from_slice(&0xDEADBEEFu64.to_le_bytes());
        bad_quote.extend_from_slice(&1u64.to_le_bytes());
        bad_quote.extend_from_slice(&42u64.to_le_bytes());
        assert_eq!(parse_batch_cancel_params(&bad_quote), Err(105));
    }

    #[test]
    fn test_balance_ledger_checked_overflow_protection() {
        let mut ledger = BalanceLedger::default();
        let alice = [0xAA; 32];
        assert!(ledger.deposit(alice, XPX_MOSAIC_ID, u64::MAX - 5).is_ok());
        // Overflowing deposit must fail closed instead of silently saturating/dropping balance
        assert!(ledger.deposit(alice, XPX_MOSAIC_ID, 10).is_err());
        assert_eq!(ledger.get_balance(&alice, XPX_MOSAIC_ID), (u64::MAX - 5, 0));

        // Normal lock/unlock/withdraw work cleanly
        assert!(ledger.lock(&alice, XPX_MOSAIC_ID, 1000).is_ok());
        assert_eq!(ledger.get_balance(&alice, XPX_MOSAIC_ID), (u64::MAX - 1005, 1000));
        assert!(ledger.unlock(&alice, XPX_MOSAIC_ID, 1000).is_ok());
        assert_eq!(ledger.get_balance(&alice, XPX_MOSAIC_ID), (u64::MAX - 5, 0));
    }

    struct SplitMix64(u64);
    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn range(&mut self, lo: u64, hi: u64) -> u64 {
            if lo >= hi { return lo; }
            lo + (self.next() % (hi - lo + 1))
        }
    }

    fn assert_market_and_ledger_invariants(
        market: &MarketPair,
        ledger: &BalanceLedger,
        traders: &[[u8; 32]],
        total_deposited_base: u128,
        total_deposited_quote: u128,
        total_fees_base: u128,
        total_fees_quote: u128,
        step: usize,
    ) {
        // 1. Never crossed book
        if let (Some(bb), Some(ba)) = (market.best_bid(), market.best_ask()) {
            assert!(bb < ba, "Step {}: Crossed book! best_bid={} >= best_ask={}", step, bb, ba);
        }

        // 2. Exact total_depth, zero empty levels, zero dust resting orders, and 1-to-1 order_index bijection
        let mut resting_count = 0usize;
        let mut book_locked_base: u128 = 0;
        for (&price, lvl) in &market.bids {
            assert!(!lvl.orders.is_empty(), "Step {}: Empty bid level @ {}", step, price);
            let mut sum_depth = 0u64;
            for o in &lvl.orders {
                assert!(o.remaining_amount > 0, "Step {}: Zero remaining bid #{}", step, o.order_id);
                assert!(
                    MarketPair::calculate_quote_amount(o.remaining_amount, price) > 0,
                    "Step {}: Unfillable zero-quote dust bid #{} resting @ {}",
                    step, o.order_id, price
                );
                assert_eq!(
                    market.order_index.get(&o.order_id),
                    Some(&(OrderSide::Buy, price)),
                    "Step {}: order_index mismatch for bid #{}",
                    step, o.order_id
                );
                sum_depth = sum_depth.checked_add(o.remaining_amount).expect("depth sum overflow");
                resting_count += 1;
            }
            assert_eq!(lvl.total_depth, sum_depth, "Step {}: Bid total_depth mismatch @ {}", step, price);
        }

        for (&price, lvl) in &market.asks {
            assert!(!lvl.orders.is_empty(), "Step {}: Empty ask level @ {}", step, price);
            let mut sum_depth = 0u64;
            for o in &lvl.orders {
                assert!(o.remaining_amount > 0, "Step {}: Zero remaining ask #{}", step, o.order_id);
                assert!(
                    MarketPair::calculate_quote_amount(o.remaining_amount, price) > 0,
                    "Step {}: Unfillable zero-quote dust ask #{} resting @ {}",
                    step, o.order_id, price
                );
                assert_eq!(
                    market.order_index.get(&o.order_id),
                    Some(&(OrderSide::Sell, price)),
                    "Step {}: order_index mismatch for ask #{}",
                    step, o.order_id
                );
                sum_depth = sum_depth.checked_add(o.remaining_amount).expect("depth sum overflow");
                book_locked_base += o.remaining_amount as u128;
                resting_count += 1;
            }
            assert_eq!(lvl.total_depth, sum_depth, "Step {}: Ask total_depth mismatch @ {}", step, price);
        }

        assert_eq!(
            market.order_index.len(),
            resting_count,
            "Step {}: order_index len ({}) != resting orders ({})",
            step, market.order_index.len(), resting_count
        );

        // 3. Exact Base & Quote conservation across all traders + protocol fees
        let mut ledger_base_free: u128 = 0;
        let mut ledger_base_locked: u128 = 0;
        let mut ledger_quote_free: u128 = 0;
        let mut ledger_quote_locked: u128 = 0;
        for t in traders {
            let (fb, lb) = ledger.get_balance(t, market.base_mosaic_id);
            let (fq, lq) = ledger.get_balance(t, market.quote_mosaic_id);
            ledger_base_free += fb as u128;
            ledger_base_locked += lb as u128;
            ledger_quote_free += fq as u128;
            ledger_quote_locked += lq as u128;
        }

        assert_eq!(
            ledger_base_free + ledger_base_locked + total_fees_base,
            total_deposited_base,
            "Step {}: BASE CONSERVATION BREACH! free={} locked={} fees={} != deposited={}",
            step, ledger_base_free, ledger_base_locked, total_fees_base, total_deposited_base
        );
        assert_eq!(
            ledger_base_locked,
            book_locked_base,
            "Step {}: LOCKED BASE DRIFT! ledger_locked={} != book_asks={}",
            step, ledger_base_locked, book_locked_base
        );
        assert_eq!(
            ledger_quote_free + ledger_quote_locked + total_fees_quote,
            total_deposited_quote,
            "Step {}: QUOTE CONSERVATION BREACH! free={} locked={} fees={} != deposited={}",
            step, ledger_quote_free, ledger_quote_locked, total_fees_quote, total_deposited_quote
        );
    }

    fn run_market_fuzz_campaign(
        seed: u64,
        iterations: usize,
        quote_mosaic_id: u64,
        tick_size: u64,
        min_order_size: u64,
        maker_fee_bps: u64,
        taker_fee_bps: u64,
        price_lo_ticks: u64,
        price_hi_ticks: u64,
    ) {
        let mut rng = SplitMix64(seed);
        let mut market = MarketPair::new(
            XPX_MOSAIC_ID,
            quote_mosaic_id,
            tick_size,
            min_order_size,
            maker_fee_bps,
            taker_fee_bps,
        );
        let mut ledger = BalanceLedger::default();
        let traders: [[u8; 32]; 6] = [
            [1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32], [5u8; 32], [6u8; 32],
        ];
        let mut total_deposited_base: u128 = 0;
        let mut total_deposited_quote: u128 = 0;
        let mut total_fees_base: u128 = 0;
        let mut total_fees_quote: u128 = 0;

        for t in &traders {
            let init_base = 500_000_000_000u64; // 500k XPX
            let init_quote = 500_000_000_000u64;
            ledger.deposit(*t, XPX_MOSAIC_ID, init_base).unwrap();
            ledger.deposit(*t, quote_mosaic_id, init_quote).unwrap();
            total_deposited_base += init_base as u128;
            total_deposited_quote += init_quote as u128;
        }

        let mut known_order_ids: Vec<OrderId> = Vec::new();
        let mut total_fills: usize = 0;

        for step in 0..iterations {
            let action = rng.range(0, 99);
            let trader = traders[rng.range(0, (traders.len() - 1) as u64) as usize];

            if action < 58 {
                // Place random Limit / IOC / PostOnly Buy or Sell order
                let side = if rng.range(0, 1) == 0 { OrderSide::Buy } else { OrderSide::Sell };
                let order_type = match rng.range(0, 9) {
                    0..=5 => OrderType::Limit,
                    6..=7 => OrderType::IOC,
                    _ => OrderType::PostOnly,
                };
                let raw_tick_idx = rng.range(price_lo_ticks, price_hi_ticks);
                let price = raw_tick_idx * tick_size;
                let amount = rng.range(min_order_size, min_order_size.saturating_mul(250));

                let (req_mosaic, req_amount) = match side {
                    OrderSide::Buy => (quote_mosaic_id, MarketPair::calculate_quote_amount(amount, price)),
                    OrderSide::Sell => (XPX_MOSAIC_ID, amount),
                };
                if price == 0 || amount == 0 || req_amount == 0 || req_amount == u64::MAX {
                    continue;
                }
                let (free_bal, _) = ledger.get_balance(&trader, req_mosaic);
                if free_bal < req_amount {
                    continue;
                }
                ledger.lock(&trader, req_mosaic, req_amount).unwrap();

                match market.place_order(trader, side, order_type, price, amount, step as u64 + 1) {
                    Ok(res) => {
                        if res.order_id > 0 && res.status != OrderStatus::Rejected {
                            known_order_ids.push(res.order_id);
                            if known_order_ids.len() > 256 {
                                known_order_ids.remove(0);
                            }
                        }
                        for fill in &res.fills {
                            assert!(fill.base_amount > 0, "Step {}: zero base fill", step);
                            assert!(fill.quote_amount > 0, "Step {}: ZERO-QUOTE FILL EXPLOIT!", step);
                            total_fills += 1;
                            match fill.taker_side {
                                OrderSide::Buy => {
                                    ledger.release_locked(&fill.taker_pubkey, market.quote_mosaic_id, fill.quote_amount).unwrap();
                                    ledger.release_locked(&fill.maker_pubkey, market.base_mosaic_id, fill.base_amount).unwrap();
                                    let net_base = fill.base_amount.checked_sub(fill.taker_fee).unwrap();
                                    let net_quote = fill.quote_amount.checked_sub(fill.maker_fee).unwrap();
                                    ledger.deposit(fill.taker_pubkey, market.base_mosaic_id, net_base).unwrap();
                                    ledger.deposit(fill.maker_pubkey, market.quote_mosaic_id, net_quote).unwrap();
                                    total_fees_base += fill.taker_fee as u128;
                                    total_fees_quote += fill.maker_fee as u128;
                                }
                                OrderSide::Sell => {
                                    ledger.release_locked(&fill.taker_pubkey, market.base_mosaic_id, fill.base_amount).unwrap();
                                    ledger.release_locked(&fill.maker_pubkey, market.quote_mosaic_id, fill.quote_amount).unwrap();
                                    let net_base = fill.base_amount.checked_sub(fill.maker_fee).unwrap();
                                    let net_quote = fill.quote_amount.checked_sub(fill.taker_fee).unwrap();
                                    ledger.deposit(fill.maker_pubkey, market.base_mosaic_id, net_base).unwrap();
                                    ledger.deposit(fill.taker_pubkey, market.quote_mosaic_id, net_quote).unwrap();
                                    total_fees_base += fill.maker_fee as u128;
                                    total_fees_quote += fill.taker_fee as u128;
                                }
                            }
                        }
                        for &(dust_owner, dust_side, dust_amount) in &res.dust_refunds {
                            if dust_amount > 0 {
                                let m = match dust_side {
                                    OrderSide::Buy => market.quote_mosaic_id,
                                    OrderSide::Sell => market.base_mosaic_id,
                                };
                                ledger.unlock(&dust_owner, m, dust_amount).unwrap();
                            }
                        }
                        if res.refund_base > 0 {
                            ledger.unlock(&trader, market.base_mosaic_id, res.refund_base).unwrap();
                        }
                        if res.refund_quote > 0 {
                            ledger.unlock(&trader, market.quote_mosaic_id, res.refund_quote).unwrap();
                        }
                    }
                    Err(_) => {
                        ledger.unlock(&trader, req_mosaic, req_amount).unwrap();
                    }
                }
            } else if action < 73 && !known_order_ids.is_empty() {
                // Single cancel (authorized or unauthorized)
                let oid = known_order_ids[rng.range(0, (known_order_ids.len() - 1) as u64) as usize];
                if let Ok((side, refund, _)) = market.cancel_order(oid, trader) {
                    if refund > 0 {
                        let m = match side {
                            OrderSide::Buy => market.quote_mosaic_id,
                            OrderSide::Sell => market.base_mosaic_id,
                        };
                        ledger.unlock(&trader, m, refund).unwrap();
                    }
                }
            } else if action < 83 && !known_order_ids.is_empty() {
                // Batch cancel (including duplicates & random IDs)
                let cnt = rng.range(1, 5) as usize;
                let mut batch = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    batch.push(known_order_ids[rng.range(0, (known_order_ids.len() - 1) as u64) as usize]);
                }
                let res = market.cancel_batch(&batch, trader);
                if res.total_refund_base > 0 {
                    ledger.unlock(&trader, market.base_mosaic_id, res.total_refund_base).unwrap();
                }
                if res.total_refund_quote > 0 {
                    ledger.unlock(&trader, market.quote_mosaic_id, res.total_refund_quote).unwrap();
                }
            } else if action < 88 {
                // Cancel all for trader
                let res = market.cancel_all(trader);
                if res.total_refund_base > 0 {
                    ledger.unlock(&trader, market.base_mosaic_id, res.total_refund_base).unwrap();
                }
                if res.total_refund_quote > 0 {
                    ledger.unlock(&trader, market.quote_mosaic_id, res.total_refund_quote).unwrap();
                }
            } else if action < 95 {
                // Fuzz binary batch_cancel parser with arbitrary bytes (must NEVER panic!)
                let len = rng.range(0, 72) as usize;
                let mut raw = vec![0u8; len];
                for b in &mut raw {
                    *b = (rng.next() & 0xFF) as u8;
                }
                let _ = parse_batch_cancel_params(&raw);
            } else {
                // Depth snapshot query
                let max_lvl = rng.range(0, 50) as usize;
                let d = market.get_depth(max_lvl);
                assert!(d.bids.len() <= max_lvl && d.asks.len() <= max_lvl);
            }

            // Check all invariants frequently (every step for first 2k, every 25 steps thereafter + final step)
            if step < 2_000 || step % 25 == 0 || step + 1 == iterations {
                assert_market_and_ledger_invariants(
                    &market,
                    &ledger,
                    &traders,
                    total_deposited_base,
                    total_deposited_quote,
                    total_fees_base,
                    total_fees_quote,
                    step,
                );
            }
        }

        // Final drain: cancel_all for every trader and verify book is 100% empty & base locked == 0
        for t in &traders {
            let res = market.cancel_all(*t);
            if res.total_refund_base > 0 {
                ledger.unlock(t, market.base_mosaic_id, res.total_refund_base).unwrap();
            }
            if res.total_refund_quote > 0 {
                ledger.unlock(t, market.quote_mosaic_id, res.total_refund_quote).unwrap();
            }
        }
        assert!(market.bids.is_empty() && market.asks.is_empty() && market.order_index.is_empty());
        assert!(total_fills > 100, "Fuzzer must exercise real matching (got {} fills)", total_fills);
        assert_market_and_ledger_invariants(
            &market,
            &ledger,
            &traders,
            total_deposited_base,
            total_deposited_quote,
            total_fees_base,
            total_fees_quote,
            iterations,
        );
    }

    #[test]
    fn fuzz_100k_orderbook_and_ledger_invariants() {
        // Campaign 1: 50,000 random ops on XPX/USDT (tick=100, maker=5bps, taker=10bps)
        run_market_fuzz_campaign(
            0x51A1_05C1_0B00_0001,
            50_000,
            USDT_MOSAIC_ID,
            100,
            100_000,
            5,
            10,
            70,   // 0.000070 USDT
            110,  // 0.000110 USDT
        );

        // Campaign 2: 50,000 random ops on ultra-low-tick XPX/BNB (tick=1, maker=0bps, taker=10bps, extreme dust ticks 10..30!)
        run_market_fuzz_campaign(
            0x51A1_05C1_0B00_0002,
            50_000,
            BNB_MOSAIC_ID,
            1,
            100_000,
            0,
            10,
            10,   // 0.00000010 BNB
            30,   // 0.00000030 BNB
        );
    }
}
