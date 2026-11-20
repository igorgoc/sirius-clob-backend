//! SiriusCLOB — Core Type Definitions and Data Structures

use serde::{Deserialize, Serialize};

/// Unique identifier for each order (monotonically increasing 64-bit integer)
pub type OrderId = u64;

/// Fixed-point price tick (scaled by 10^8 to ensure zero floating point non-determinism)
pub type PriceTick = u64;

/// Price precision scale (10^8: e.g. price $0.00500000 = 500,000 ticks)
pub const PRICE_SCALE: u64 = 100_000_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderSide {
    Buy = 0,  // Bid: Paying Quote token to receive Base token
    Sell = 1, // Ask: Paying Base token to receive Quote token
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,    // Good 'Til Cancelled (rests on book if unmatched)
    IOC = 1,      // Immediate Or Cancel (fills available depth, cancels remainder)
    PostOnly = 2, // Maker Only (aborts if order would cross book immediately as taker)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum OrderStatus {
    Open = 0,
    PartiallyFilled = 1,
    Filled = 2,
    Cancelled = 3,
    Rejected = 4,
}

/// Compact Limit Order structure
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LimitOrder {
    pub order_id: OrderId,
    pub owner_pubkey: [u8; 32],
    pub side: OrderSide,
    pub price: PriceTick,
    pub initial_amount: u64,
    pub remaining_amount: u64,
    pub timestamp: u64,
}

/// A collection of orders at an identical price tick (FIFO Price-Time Priority Queue)
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: PriceTick,
    pub total_depth: u64,
    pub orders: Vec<LimitOrder>,
}

impl PriceLevel {
    pub fn new(price: PriceTick) -> Self {
        Self {
            price,
            total_depth: 0,
            orders: Vec::new(),
        }
    }

    pub fn push_order(&mut self, order: LimitOrder) {
        self.total_depth += order.remaining_amount;
        self.orders.push(order);
    }
}

/// An individual trade execution event emitted by the matching engine
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TradeFill {
    pub maker_order_id: OrderId,
    pub maker_pubkey: [u8; 32],
    pub taker_pubkey: [u8; 32],
    pub taker_side: OrderSide,
    pub price: PriceTick,
    pub base_amount: u64,
    pub quote_amount: u64,
    pub maker_fee: u64,
    pub taker_fee: u64,
}

/// Return result of placing an order into the matching engine
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct OrderResult {
    pub order_id: OrderId,
    pub status: OrderStatus,
    pub fills: Vec<TradeFill>,
    pub remaining_amount: u64,
    pub total_base_filled: u64,
    pub total_quote_filled: u64,
    pub refund_base: u64,
    pub refund_quote: u64,
}

/// Public Order Book depth snapshot entry (for UI and queries)
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price: PriceTick,
    pub total_depth: u64,
    pub order_count: u32,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct OrderBookDepth {
    pub base_mosaic_id: u64,
    pub base_mosaic_hex: String,
    pub quote_mosaic_id: u64,
    pub quote_mosaic_hex: String,
    pub best_bid: Option<PriceTick>,
    pub best_ask: Option<PriceTick>,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

/// Result of batch cancel or cancel all operations
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub struct BatchCancelResult {
    pub cancelled_order_ids: Vec<OrderId>,
    pub total_refund_base: u64,
    pub total_refund_quote: u64,
}
