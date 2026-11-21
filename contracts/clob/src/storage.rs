//! SiriusCLOB — Persistent Storage Manager for Sirius Replicator Drive

use crate::orderbook::MarketPair;
use crate::types::TradeFill;
use sdk::file::{FileReader, FileWriter};
use sdk::filesystem::{create_dir, path_exists};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

pub const DEFAULT_MARKET_DIR: &str = "markets/xpx_usdt";
pub const DEFAULT_STATE_FILE: &str = "markets/xpx_usdt/market_state.bin";
pub const DEFAULT_TRADES_FILE: &str = "markets/xpx_usdt/trades.bin";
pub const DEFAULT_TRADES_JSON: &str = "markets/xpx_usdt/trades.json";

pub const XPX_MOSAIC_ID: u64 = 0x0DC67FBE1CAD29E3;
pub const USDT_MOSAIC_ID: u64 = 0x10112CBA1E7B9037;
pub const BNB_MOSAIC_ID: u64 = 0x1E5C5FADEC7B34DD;

pub const TESTNET2_XPX_MOSAIC_ID: u64 = 0x13BFC518E40549D7;
pub const TESTNET2_USDT_MOSAIC_ID: u64 = 0x4430AEFEF7E15D0C;
pub const TESTNET2_BNB_MOSAIC_ID: u64 = 0x3CC1D3CE7447B6F9;

pub const MAINNET_XPX_MOSAIC_ID: u64 = 0x402B2F579FAEBC59; // Official Sirius Mainnet Genesis XPX Mosaic ID
pub const MAINNET_USDT_MOSAIC_ID: u64 = 0x7525381A171D1E44;
pub const MAINNET_BNB_MOSAIC_ID: u64 = 0x1E5C5FADEC7B34DD;
pub const XPX_NAMESPACE_ID: u64 = 0xBFFB42A19116BDF6;

pub fn canonicalize_mosaic_id(id: u64) -> Option<u64> {
    if id == XPX_MOSAIC_ID || id == TESTNET2_XPX_MOSAIC_ID || id == MAINNET_XPX_MOSAIC_ID || id == XPX_NAMESPACE_ID {
        Some(XPX_MOSAIC_ID)
    } else if id == USDT_MOSAIC_ID || id == TESTNET2_USDT_MOSAIC_ID || id == MAINNET_USDT_MOSAIC_ID {
        Some(USDT_MOSAIC_ID)
    } else if id == BNB_MOSAIC_ID || id == TESTNET2_BNB_MOSAIC_ID || id == MAINNET_BNB_MOSAIC_ID {
        Some(BNB_MOSAIC_ID)
    } else {
        None
    }
}

pub fn is_mainnet_active() -> bool {
    path_exists("config/mainnet.flag")
}

/// Converts internal canonical mosaic IDs to live on-chain mosaic IDs for payout transactions (supports Mainnet via config/mainnet.flag)
pub fn canonical_to_network_mosaic(id: u64) -> u64 {
    let mainnet = is_mainnet_active();
    if id == XPX_MOSAIC_ID || id == TESTNET2_XPX_MOSAIC_ID || id == MAINNET_XPX_MOSAIC_ID || id == XPX_NAMESPACE_ID {
        if mainnet { MAINNET_XPX_MOSAIC_ID } else { TESTNET2_XPX_MOSAIC_ID }
    } else if id == USDT_MOSAIC_ID || id == TESTNET2_USDT_MOSAIC_ID || id == MAINNET_USDT_MOSAIC_ID {
        if mainnet { MAINNET_USDT_MOSAIC_ID } else { TESTNET2_USDT_MOSAIC_ID }
    } else if id == BNB_MOSAIC_ID || id == TESTNET2_BNB_MOSAIC_ID || id == MAINNET_BNB_MOSAIC_ID {
        if mainnet { MAINNET_BNB_MOSAIC_ID } else { TESTNET2_BNB_MOSAIC_ID }
    } else {
        id
    }
}

pub fn is_supported_quote_mosaic(quote_mosaic_id: u64) -> bool {
    quote_mosaic_id == USDT_MOSAIC_ID || quote_mosaic_id == BNB_MOSAIC_ID
}

pub fn get_market_paths(quote_mosaic_id: u64) -> Result<(&'static str, &'static str, &'static str, &'static str, &'static str), &'static str> {
    if quote_mosaic_id == BNB_MOSAIC_ID {
        Ok((
            "markets/xpx_bnb",
            "markets/xpx_bnb/market_state.bin",
            "markets/xpx_bnb/trades.bin",
            "markets/xpx_bnb/trades.json",
            "markets/xpx_bnb/depth.json",
        ))
    } else if quote_mosaic_id == USDT_MOSAIC_ID {
        Ok((
            "markets/xpx_usdt",
            "markets/xpx_usdt/market_state.bin",
            "markets/xpx_usdt/trades.bin",
            "markets/xpx_usdt/trades.json",
            "markets/xpx_usdt/depth.json",
        ))
    } else {
        Err("Unsupported quote mosaic ID: only USDT and BNB markets are permitted")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MarketPairV1 {
    pub base_mosaic_id: u64,
    pub quote_mosaic_id: u64,
    pub tick_size: u64,
    pub min_order_size: u64,
    pub next_order_id: crate::types::OrderId,
    pub maker_fee_bps: u64,
    pub taker_fee_bps: u64,
    pub bids: std::collections::BTreeMap<crate::types::PriceTick, crate::types::PriceLevel>,
    pub asks: std::collections::BTreeMap<crate::types::PriceTick, crate::types::PriceLevel>,
}

pub fn load_or_init_market(quote_mosaic_id: u64) -> Result<MarketPair, &'static str> {
    let (_, state_file, _, _, _) = get_market_paths(quote_mosaic_id)?;
    if path_exists(state_file) {
        if let Ok(mut reader) = FileReader::new(state_file) {
            let mut buf = Vec::new();
            if reader.read_to_end(&mut buf).is_ok() && !buf.is_empty() {
                if let Ok(mut market) = bincode::deserialize::<MarketPair>(&buf) {
                    if market.order_index.is_empty() && (!market.bids.is_empty() || !market.asks.is_empty()) {
                        market.rebuild_order_index();
                    }
                    return Ok(market);
                }
                if let Ok(v1) = bincode::deserialize::<MarketPairV1>(&buf) {
                    let mut market = MarketPair {
                        base_mosaic_id: v1.base_mosaic_id,
                        quote_mosaic_id: v1.quote_mosaic_id,
                        tick_size: v1.tick_size,
                        min_order_size: v1.min_order_size,
                        next_order_id: v1.next_order_id,
                        maker_fee_bps: v1.maker_fee_bps,
                        taker_fee_bps: v1.taker_fee_bps,
                        bids: v1.bids,
                        asks: v1.asks,
                        order_index: std::collections::BTreeMap::new(),
                    };
                    market.rebuild_order_index();
                    return Ok(market);
                }
            }
        }
    }

    // Default Market initialized on first boot
    let tick_size = if quote_mosaic_id == BNB_MOSAIC_ID { 1 } else { 100 };
    Ok(MarketPair::new(
        XPX_MOSAIC_ID,
        quote_mosaic_id,
        tick_size,
        100_000,     // min order size = 0.100000 XPX
        0,           // 0.00% maker fee (0 bps)
        10,          // 0.10% taker fee (10 bps)
    ))
}

pub fn save_market_state(market: &MarketPair) -> Result<(), &'static str> {
    let (market_dir, state_file, _, _, _) = get_market_paths(market.quote_mosaic_id)?;
    if !path_exists(market_dir) {
        let _ = create_dir(market_dir);
    }

    let encoded = bincode::serialize(market).map_err(|_| "Failed to serialize market state")?;
    let mut writer = FileWriter::new(state_file).map_err(|_| "Failed to open market state file for writing")?;
    writer.write_all(&encoded).map_err(|_| "Failed to write market state")?;
    writer.flush().map_err(|_| "Failed to flush market state")?;
    Ok(())
}

pub fn record_trades(quote_mosaic_id: u64, new_trades: &[TradeFill]) -> Result<(), &'static str> {
    if new_trades.is_empty() {
        return Ok(());
    }

    let (market_dir, _, trades_file, trades_json, _) = get_market_paths(quote_mosaic_id)?;
    if !path_exists(market_dir) {
        let _ = create_dir(market_dir);
    }

    let mut all_trades: Vec<TradeFill> = Vec::new();
    if path_exists(trades_file) {
        if let Ok(mut reader) = FileReader::new(trades_file) {
            let mut buf = Vec::new();
            if reader.read_to_end(&mut buf).is_ok() && !buf.is_empty() {
                if let Ok(existing) = bincode::deserialize::<Vec<TradeFill>>(&buf) {
                    all_trades = existing;
                }
            }
        }
    }

    all_trades.extend_from_slice(new_trades);
    // Keep rolling history of last 500 trades
    if all_trades.len() > 500 {
        let trim_count = all_trades.len() - 500;
        all_trades.drain(0..trim_count);
    }

    let encoded = bincode::serialize(&all_trades).map_err(|_| "Failed to serialize trades")?;
    let mut writer = FileWriter::new(trades_file).map_err(|_| "Failed to open trades file for writing")?;
    writer.write_all(&encoded).map_err(|_| "Failed to write trades")?;
    writer.flush().map_err(|_| "Failed to flush trades")?;

    // Export human-readable JSON trades for UI consumption and REST APIs
    if let Ok(json_trades) = serde_json::to_vec(&all_trades) {
        if let Ok(mut json_writer) = FileWriter::new(trades_json) {
            let _ = json_writer.write_all(&json_trades);
            let _ = json_writer.flush();
        }
    }

    Ok(())
}

pub const BALANCES_DIR: &str = "markets";
pub const BALANCES_FILE: &str = "markets/balances.bin";
pub const BALANCES_JSON: &str = "markets/balances.json";

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct AccountBalanceEntry {
    pub pubkey_hex: String,
    pub pubkey: [u8; 32],
    pub mosaic_id: u64,
    pub free_amount: u64,
    pub locked_amount: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct BalanceLedger {
    pub entries: Vec<AccountBalanceEntry>,
}

impl BalanceLedger {
    pub fn get_balance(&self, pubkey: &[u8; 32], mosaic_id: u64) -> (u64, u64) {
        for e in &self.entries {
            if &e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                return (e.free_amount, e.locked_amount);
            }
        }
        (0, 0)
    }

    pub fn deposit(&mut self, pubkey: [u8; 32], mosaic_id: u64, amount: u64) {
        for e in &mut self.entries {
            if e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                e.free_amount = e.free_amount.saturating_add(amount);
                return;
            }
        }
        const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
        let mut hex = String::with_capacity(64);
        for &b in &pubkey {
            hex.push(HEX_CHARS[(b >> 4) as usize] as char);
            hex.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        self.entries.push(AccountBalanceEntry {
            pubkey_hex: hex,
            pubkey,
            mosaic_id,
            free_amount: amount,
            locked_amount: 0,
        });
        self.entries.sort_by(|a, b| a.pubkey.cmp(&b.pubkey).then(a.mosaic_id.cmp(&b.mosaic_id)));
    }

    pub fn withdraw(&mut self, pubkey: &[u8; 32], mosaic_id: u64, amount: u64) -> Result<(), &'static str> {
        for e in &mut self.entries {
            if &e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                if e.free_amount < amount {
                    return Err("Insufficient free balance");
                }
                e.free_amount -= amount;
                return Ok(());
            }
        }
        Err("Account or mosaic not found")
    }

    pub fn lock(&mut self, pubkey: &[u8; 32], mosaic_id: u64, amount: u64) -> Result<(), &'static str> {
        for e in &mut self.entries {
            if &e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                if e.free_amount < amount {
                    return Err("Insufficient free balance to lock");
                }
                e.free_amount -= amount;
                e.locked_amount = e.locked_amount.saturating_add(amount);
                return Ok(());
            }
        }
        Err("Account or mosaic not found")
    }

    pub fn unlock(&mut self, pubkey: &[u8; 32], mosaic_id: u64, amount: u64) -> Result<(), &'static str> {
        for e in &mut self.entries {
            if &e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                let actual = e.locked_amount.min(amount);
                e.locked_amount -= actual;
                e.free_amount = e.free_amount.saturating_add(actual);
                return Ok(());
            }
        }
        Err("Account or mosaic not found")
    }

    pub fn release_locked(&mut self, pubkey: &[u8; 32], mosaic_id: u64, amount: u64) -> Result<(), &'static str> {
        for e in &mut self.entries {
            if &e.pubkey == pubkey && e.mosaic_id == mosaic_id {
                let actual = e.locked_amount.min(amount);
                e.locked_amount -= actual;
                return Ok(());
            }
        }
        Err("Account or mosaic not found")
    }
}

pub fn load_balances() -> BalanceLedger {
    if path_exists(BALANCES_FILE) {
        if let Ok(mut reader) = FileReader::new(BALANCES_FILE) {
            let mut buf = Vec::new();
            if reader.read_to_end(&mut buf).is_ok() && !buf.is_empty() {
                if let Ok(ledger) = bincode::deserialize::<BalanceLedger>(&buf) {
                    return ledger;
                }
            }
        }
    }
    BalanceLedger::default()
}

pub fn save_balances(ledger: &BalanceLedger) -> Result<(), &'static str> {
    if !path_exists(BALANCES_DIR) {
        let _ = create_dir(BALANCES_DIR);
    }

    let encoded = bincode::serialize(ledger).map_err(|_| "Failed to serialize balances")?;
    let mut writer = FileWriter::new(BALANCES_FILE).map_err(|_| "Failed to open balances file for writing")?;
    writer.write_all(&encoded).map_err(|_| "Failed to write balances")?;
    writer.flush().map_err(|_| "Failed to flush balances")?;

    if let Ok(json_bytes) = serde_json::to_vec(ledger) {
        if let Ok(mut json_writer) = FileWriter::new(BALANCES_JSON) {
            let _ = json_writer.write_all(&json_bytes);
            let _ = json_writer.flush();
        }
    }

    Ok(())
}

