//! SiriusCLOB — High-Performance Matching Engine & Order Book

use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketPair {
    pub base_mosaic_id: u64,
    pub quote_mosaic_id: u64,
    pub tick_size: u64,
    pub min_order_size: u64,
    pub next_order_id: OrderId,
    pub maker_fee_bps: u64, // e.g. 0 = 0.00%
    pub taker_fee_bps: u64, // e.g. 10 = 0.10%

    // Bids stored by PriceTick (scanned in descending order)
    pub bids: BTreeMap<PriceTick, PriceLevel>,
    // Asks stored by PriceTick (scanned in ascending order)
    pub asks: BTreeMap<PriceTick, PriceLevel>,
    // Fast O(log N) secondary index mapping OrderId -> (OrderSide, PriceTick)
    pub order_index: BTreeMap<OrderId, (OrderSide, PriceTick)>,
}

impl MarketPair {
    pub fn new(
        base_mosaic_id: u64,
        quote_mosaic_id: u64,
        tick_size: u64,
        min_order_size: u64,
        maker_fee_bps: u64,
        taker_fee_bps: u64,
    ) -> Self {
        Self {
            base_mosaic_id,
            quote_mosaic_id,
            tick_size: if tick_size == 0 { 1 } else { tick_size },
            min_order_size: if min_order_size == 0 { 1 } else { min_order_size },
            next_order_id: 1,
            maker_fee_bps,
            taker_fee_bps,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_index: BTreeMap::new(),
        }
    }

    /// Reconstructs the secondary order index from resting bids and asks
    pub fn rebuild_order_index(&mut self) {
        self.order_index.clear();
        for (&price, level) in self.bids.iter() {
            for order in &level.orders {
                self.order_index.insert(order.order_id, (OrderSide::Buy, price));
            }
        }
        for (&price, level) in self.asks.iter() {
            for order in &level.orders {
                self.order_index.insert(order.order_id, (OrderSide::Sell, price));
            }
        }
    }

    pub fn best_bid(&self) -> Option<PriceTick> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<PriceTick> {
        self.asks.keys().next().copied()
    }

    /// Calculates quote asset quantity from base asset quantity and price tick
    pub fn calculate_quote_amount(base_amount: u64, price: PriceTick) -> u64 {
        let prod = (base_amount as u128) * (price as u128);
        let quote = prod / (PRICE_SCALE as u128);
        u64::try_from(quote).unwrap_or(u64::MAX)
    }

    /// Calculates trading fee with round-up (ceiling division) rule
    /// Fee formula: (quote_amt * fee_bps + 9_999) / 10_000
    /// This prevents fee evasion through small-lot fractional truncation.
    pub fn calculate_fee(quote_amt: u64, fee_bps: u64) -> u64 {
        if quote_amt == 0 || fee_bps == 0 {
            return 0;
        }
        if fee_bps >= 10_000 {
            return quote_amt;
        }
        let prod = (quote_amt as u128) * (fee_bps as u128);
        let fee = ((prod + 9_999) / 10_000) as u64;
        std::cmp::min(fee, quote_amt)
    }

    /// Places a Limit or IOC or PostOnly order into the matching engine.
    /// Executes immediately against crossing liquidity, resting any unfilled remainder.
    pub fn place_order(
        &mut self,
        trader: [u8; 32],
        side: OrderSide,
        order_type: OrderType,
        price: PriceTick,
        amount: u64,
        timestamp: u64,
    ) -> Result<OrderResult, &'static str> {
        if amount < self.min_order_size {
            return Err("Order amount below minimum lot size");
        }
        if price == 0 {
            return Err("Price must be strictly positive");
        }
        if price % self.tick_size != 0 {
            return Err("Price must be a multiple of tick size");
        }
        if ((amount as u128 * price as u128) / (PRICE_SCALE as u128)) > u64::MAX as u128 {
            return Err("Order value overflows u64 quote limit");
        }
        if Self::calculate_quote_amount(amount, price) == 0 {
            return Err("Order quote amount rounds to zero");
        }

        // Post-Only check: if order crosses liquidity immediately, reject before consuming an order ID
        if order_type == OrderType::PostOnly {
            match side {
                OrderSide::Buy => {
                    if let Some(best_ask) = self.best_ask() {
                        if price >= best_ask {
                            return Ok(OrderResult {
                                order_id: 0,
                                status: OrderStatus::Rejected,
                                fills: Vec::new(),
                                remaining_amount: 0,
                                total_base_filled: 0,
                                total_quote_filled: 0,
                                refund_base: 0,
                                refund_quote: Self::calculate_quote_amount(amount, price),
                            });
                        }
                    }
                }
                OrderSide::Sell => {
                    if let Some(best_bid) = self.best_bid() {
                        if price <= best_bid {
                            return Ok(OrderResult {
                                order_id: 0,
                                status: OrderStatus::Rejected,
                                fills: Vec::new(),
                                remaining_amount: 0,
                                total_base_filled: 0,
                                total_quote_filled: 0,
                                refund_base: amount,
                                refund_quote: 0,
                            });
                        }
                    }
                }
            }
        }

        let order_id = self.next_order_id;
        self.next_order_id = self.next_order_id.checked_add(1).ok_or("Order ID counter overflow")?;

        let mut remaining = amount;
        let mut fills = Vec::new();
        let mut total_base_filled = 0u64;
        let mut total_quote_filled = 0u64;
        let mut stp_refund_base = 0u64;
        let mut stp_refund_quote = 0u64;

        match side {
            OrderSide::Buy => {
                // Post-Only check: if order crosses best ask immediately, reject
                if order_type == OrderType::PostOnly {
                    if let Some(best_ask) = self.best_ask() {
                        if price >= best_ask {
                            return Ok(OrderResult {
                                order_id,
                                status: OrderStatus::Rejected,
                                fills: Vec::new(),
                                remaining_amount: 0,
                                total_base_filled: 0,
                                total_quote_filled: 0,
                                refund_base: 0,
                                refund_quote: Self::calculate_quote_amount(amount, price),
                            });
                        }
                    }
                }

                // Match against resting Asks (Lowest Ask first)
                while remaining > 0 {
                    let best_ask_price = match self.best_ask() {
                        Some(p) if p <= price => p,
                        _ => break, // No matching asks left
                    };

                    let mut level_empty = false;
                    if let Some(level) = self.asks.get_mut(&best_ask_price) {
                        while remaining > 0 && !level.orders.is_empty() {
                            let resting = &mut level.orders[0];

                            // Self-Trade Prevention (STP): Decrement and Cancel
                            if resting.owner_pubkey == trader {
                                let cancel_qty = std::cmp::min(remaining, resting.remaining_amount);
                                level.total_depth -= cancel_qty;
                                resting.remaining_amount -= cancel_qty;
                                remaining -= cancel_qty;
                                // The resting Sell order was backed by Base asset (XPX), which is now cancelled
                                stp_refund_base += cancel_qty;
                                if resting.remaining_amount == 0 {
                                    let removed = level.orders.remove(0);
                                    self.order_index.remove(&removed.order_id);
                                }
                                continue;
                            }

                            let fill_qty = std::cmp::min(remaining, resting.remaining_amount);
                            let quote_amt = Self::calculate_quote_amount(fill_qty, best_ask_price);
                            let maker_fee = Self::calculate_fee(quote_amt, self.maker_fee_bps);
                            let taker_fee = Self::calculate_fee(fill_qty, self.taker_fee_bps);

                            fills.push(TradeFill {
                                maker_order_id: resting.order_id,
                                maker_pubkey: resting.owner_pubkey,
                                taker_pubkey: trader,
                                taker_side: OrderSide::Buy,
                                price: best_ask_price,
                                base_amount: fill_qty,
                                quote_amount: quote_amt,
                                maker_fee,
                                taker_fee,
                            });

                            total_base_filled += fill_qty;
                            total_quote_filled += quote_amt;
                            remaining -= fill_qty;
                            level.total_depth -= fill_qty;
                            resting.remaining_amount -= fill_qty;

                            if resting.remaining_amount == 0 {
                                let removed = level.orders.remove(0);
                                self.order_index.remove(&removed.order_id);
                            }
                        }
                        if level.orders.is_empty() {
                            level_empty = true;
                        }
                    }

                    if level_empty {
                        self.asks.remove(&best_ask_price);
                    }
                }

                // Handle remaining unfilled quantity
                if remaining > 0 && order_type != OrderType::IOC {
                    let resting_order = LimitOrder {
                        order_id,
                        owner_pubkey: trader,
                        side: OrderSide::Buy,
                        price,
                        initial_amount: amount,
                        remaining_amount: remaining,
                        timestamp,
                    };
                    self.bids
                        .entry(price)
                        .or_insert_with(|| PriceLevel::new(price))
                        .push_order(resting_order);
                    self.order_index.insert(order_id, (OrderSide::Buy, price));
                }
            }

            OrderSide::Sell => {
                // Post-Only check: if order crosses best bid immediately, reject
                if order_type == OrderType::PostOnly {
                    if let Some(best_bid) = self.best_bid() {
                        if price <= best_bid {
                            return Ok(OrderResult {
                                order_id,
                                status: OrderStatus::Rejected,
                                fills: Vec::new(),
                                remaining_amount: 0,
                                total_base_filled: 0,
                                total_quote_filled: 0,
                                refund_base: amount,
                                refund_quote: 0,
                            });
                        }
                    }
                }

                // Match against resting Bids (Highest Bid first)
                while remaining > 0 {
                    let best_bid_price = match self.best_bid() {
                        Some(p) if p >= price => p,
                        _ => break, // No matching bids left
                    };

                    let mut level_empty = false;
                    if let Some(level) = self.bids.get_mut(&best_bid_price) {
                        while remaining > 0 && !level.orders.is_empty() {
                            let resting = &mut level.orders[0];

                            // Self-Trade Prevention (STP): Decrement and Cancel
                            if resting.owner_pubkey == trader {
                                let cancel_qty = std::cmp::min(remaining, resting.remaining_amount);
                                level.total_depth -= cancel_qty;
                                resting.remaining_amount -= cancel_qty;
                                remaining -= cancel_qty;
                                // The resting Buy order was backed by Quote asset (USDT), which is now cancelled
                                stp_refund_quote += Self::calculate_quote_amount(cancel_qty, best_bid_price);
                                if resting.remaining_amount == 0 {
                                    let removed = level.orders.remove(0);
                                    self.order_index.remove(&removed.order_id);
                                }
                                continue;
                            }

                            let fill_qty = std::cmp::min(remaining, resting.remaining_amount);
                            let quote_amt = Self::calculate_quote_amount(fill_qty, best_bid_price);
                            let maker_fee = Self::calculate_fee(fill_qty, self.maker_fee_bps);
                            let taker_fee = Self::calculate_fee(quote_amt, self.taker_fee_bps);

                            fills.push(TradeFill {
                                maker_order_id: resting.order_id,
                                maker_pubkey: resting.owner_pubkey,
                                taker_pubkey: trader,
                                taker_side: OrderSide::Sell,
                                price: best_bid_price,
                                base_amount: fill_qty,
                                quote_amount: quote_amt,
                                maker_fee,
                                taker_fee,
                            });

                            total_base_filled += fill_qty;
                            total_quote_filled += quote_amt;
                            remaining -= fill_qty;
                            level.total_depth -= fill_qty;
                            resting.remaining_amount -= fill_qty;

                            if resting.remaining_amount == 0 {
                                let removed = level.orders.remove(0);
                                self.order_index.remove(&removed.order_id);
                            }
                        }
                        if level.orders.is_empty() {
                            level_empty = true;
                        }
                    }

                    if level_empty {
                        self.bids.remove(&best_bid_price);
                    }
                }

                // Handle remaining unfilled quantity
                if remaining > 0 && order_type != OrderType::IOC {
                    let resting_order = LimitOrder {
                        order_id,
                        owner_pubkey: trader,
                        side: OrderSide::Sell,
                        price,
                        initial_amount: amount,
                        remaining_amount: remaining,
                        timestamp,
                    };
                    self.asks
                        .entry(price)
                        .or_insert_with(|| PriceLevel::new(price))
                        .push_order(resting_order);
                    self.order_index.insert(order_id, (OrderSide::Sell, price));
                }
            }
        }

        let status = if remaining == 0 {
            OrderStatus::Filled
        } else if total_base_filled > 0 {
            OrderStatus::PartiallyFilled
        } else if order_type == OrderType::IOC {
            OrderStatus::Cancelled
        } else {
            OrderStatus::Open
        };

        // Exact Escrow Accounting:
        // Returns any unspent escrow (from IOC cancellations, price improvement, or STP)
        let (refund_base, refund_quote) = match side {
            OrderSide::Buy => {
                let initial_deposit_quote = Self::calculate_quote_amount(amount, price);
                let resting_quote = if order_type != OrderType::IOC && remaining > 0 {
                    Self::calculate_quote_amount(remaining, price)
                } else {
                    0
                };
                let needed_quote = total_quote_filled.saturating_add(resting_quote);
                let price_improvement_refund = match initial_deposit_quote.checked_sub(needed_quote) {
                    Some(val) => val,
                    None => 0,
                };
                (stp_refund_base, price_improvement_refund.saturating_add(stp_refund_quote))
            }
            OrderSide::Sell => {
                let initial_deposit_base = amount;
                let resting_base = if order_type != OrderType::IOC && remaining > 0 {
                    remaining
                } else {
                    0
                };
                let needed_base = total_base_filled.saturating_add(resting_base);
                let unspent_base_refund = match initial_deposit_base.checked_sub(needed_base) {
                    Some(val) => val,
                    None => 0,
                };
                (unspent_base_refund.saturating_add(stp_refund_base), stp_refund_quote)
            }
        };

        Ok(OrderResult {
            order_id,
            status,
            fills,
            remaining_amount: remaining,
            total_base_filled,
            total_quote_filled,
            refund_base,
            refund_quote,
        })
    }

    /// Cancels an open resting order and refunds remaining escrow to the owner.
    /// Uses strict O(log N) secondary order index lookup with saturating depth protection.
    pub fn cancel_order(
        &mut self,
        order_id: OrderId,
        caller_pubkey: [u8; 32],
    ) -> Result<(OrderSide, u64, PriceTick), &'static str> {
        let &(side, price) = match self.order_index.get(&order_id) {
            Some(entry) => entry,
            None => return Err("Order not found or already filled/cancelled"),
        };

        match side {
            OrderSide::Buy => {
                let level = match self.bids.get_mut(&price) {
                    Some(lvl) => lvl,
                    None => return Err("Order not found or already filled/cancelled"),
                };
                let pos = match level.orders.iter().position(|o| o.order_id == order_id) {
                    Some(idx) => idx,
                    None => return Err("Order not found or already filled/cancelled"),
                };
                if level.orders[pos].owner_pubkey != caller_pubkey {
                    return Err("Unauthorized: order does not belong to caller");
                }
                let removed = level.orders.remove(pos);
                level.total_depth = level.total_depth.saturating_sub(removed.remaining_amount);
                let refund_quote = Self::calculate_quote_amount(removed.remaining_amount, price);
                if level.orders.is_empty() {
                    self.bids.remove(&price);
                }
                self.order_index.remove(&order_id);
                Ok((OrderSide::Buy, refund_quote, price))
            }
            OrderSide::Sell => {
                let level = match self.asks.get_mut(&price) {
                    Some(lvl) => lvl,
                    None => return Err("Order not found or already filled/cancelled"),
                };
                let pos = match level.orders.iter().position(|o| o.order_id == order_id) {
                    Some(idx) => idx,
                    None => return Err("Order not found or already filled/cancelled"),
                };
                if level.orders[pos].owner_pubkey != caller_pubkey {
                    return Err("Unauthorized: order does not belong to caller");
                }
                let removed = level.orders.remove(pos);
                level.total_depth = level.total_depth.saturating_sub(removed.remaining_amount);
                let refund_base = removed.remaining_amount;
                if level.orders.is_empty() {
                    self.asks.remove(&price);
                }
                self.order_index.remove(&order_id);
                Ok((OrderSide::Sell, refund_base, price))
            }
        }
    }

    /// Cancels a batch of specific order IDs belonging to caller in a single atomic instruction
    pub fn cancel_batch(
        &mut self,
        order_ids: &[OrderId],
        caller_pubkey: [u8; 32],
    ) -> BatchCancelResult {
        let mut result = BatchCancelResult {
            cancelled_order_ids: Vec::new(),
            total_refund_base: 0,
            total_refund_quote: 0,
        };

        for &order_id in order_ids {
            if let Ok((side, refund, _price)) = self.cancel_order(order_id, caller_pubkey) {
                result.cancelled_order_ids.push(order_id);
                match side {
                    OrderSide::Buy => result.total_refund_quote = result.total_refund_quote.saturating_add(refund),
                    OrderSide::Sell => result.total_refund_base = result.total_refund_base.saturating_add(refund),
                }
            }
        }

        result
    }

    /// Cancels all open resting orders belonging to caller across both bids and asks on this market
    pub fn cancel_all(
        &mut self,
        caller_pubkey: [u8; 32],
    ) -> BatchCancelResult {
        let mut result = BatchCancelResult {
            cancelled_order_ids: Vec::new(),
            total_refund_base: 0,
            total_refund_quote: 0,
        };

        // Collect matching bid order IDs and prices
        let mut empty_bids = Vec::new();
        for (&price, level) in self.bids.iter_mut() {
            let mut i = 0;
            while i < level.orders.len() {
                if level.orders[i].owner_pubkey == caller_pubkey {
                    let order = level.orders.remove(i);
                    level.total_depth -= order.remaining_amount;
                    let refund = Self::calculate_quote_amount(order.remaining_amount, price);
                    result.total_refund_quote = result.total_refund_quote.saturating_add(refund);
                    result.cancelled_order_ids.push(order.order_id);
                    self.order_index.remove(&order.order_id);
                } else {
                    i += 1;
                }
            }
            if level.orders.is_empty() {
                empty_bids.push(price);
            }
        }
        for price in empty_bids {
            self.bids.remove(&price);
        }

        // Collect matching ask order IDs and prices
        let mut empty_asks = Vec::new();
        for (&price, level) in self.asks.iter_mut() {
            let mut i = 0;
            while i < level.orders.len() {
                if level.orders[i].owner_pubkey == caller_pubkey {
                    let order = level.orders.remove(i);
                    level.total_depth -= order.remaining_amount;
                    result.total_refund_base = result.total_refund_base.saturating_add(order.remaining_amount);
                    result.cancelled_order_ids.push(order.order_id);
                    self.order_index.remove(&order.order_id);
                } else {
                    i += 1;
                }
            }
            if level.orders.is_empty() {
                empty_asks.push(price);
            }
        }
        for price in empty_asks {
            self.asks.remove(&price);
        }

        result
    }

    /// Returns the top N depth levels on bids and asks for public display
    pub fn get_depth(&self, max_levels: usize) -> OrderBookDepth {
        let bids: Vec<DepthLevel> = self
            .bids
            .iter()
            .rev() // Highest bid price first
            .take(max_levels)
            .map(|(&price, level)| DepthLevel {
                price,
                total_depth: level.total_depth,
                order_count: level.orders.len() as u32,
            })
            .collect();

        let asks: Vec<DepthLevel> = self
            .asks
            .iter() // Lowest ask price first
            .take(max_levels)
            .map(|(&price, level)| DepthLevel {
                price,
                total_depth: level.total_depth,
                order_count: level.orders.len() as u32,
            })
            .collect();

        OrderBookDepth {
            base_mosaic_id: self.base_mosaic_id,
            base_mosaic_hex: format!("0x{:016X}", self.base_mosaic_id),
            quote_mosaic_id: self.quote_mosaic_id,
            quote_mosaic_hex: format!("0x{:016X}", self.quote_mosaic_id),
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            bids,
            asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_market() -> MarketPair {
        MarketPair::new(
            0x402B2F579FAEBC59, // XPX
            0x0DC67FBE1CAD29E3, // USDT
            100,                 // tick size = 100 ticks
            1_000,              // min order size = 1,000 units
            0,                  // 0.00% maker fee
            10,                 // 0.10% taker fee
        )
    }

    const ALICE: [u8; 32] = [1u8; 32];
    const BOB: [u8; 32] = [2u8; 32];
    const CHARLIE: [u8; 32] = [3u8; 32];

    #[test]
    fn test_resting_limit_orders() {
        let mut market = sample_market();

        // Alice places Buy @ 500,000 (0.005 USDT) for 10,000 XPX
        let res1 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        assert_eq!(res1.status, OrderStatus::Open);
        assert_eq!(res1.order_id, 1);
        assert_eq!(market.best_bid(), Some(500_000));

        // Bob places Buy @ 501,000 for 5,000 XPX (becomes new best bid)
        let res2 = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 501_000, 5_000, 101).unwrap();
        assert_eq!(res2.status, OrderStatus::Open);
        assert_eq!(market.best_bid(), Some(501_000));

        // Charlie places Sell @ 505,000 for 20,000 XPX (best ask)
        let res3 = market.place_order(CHARLIE, OrderSide::Sell, OrderType::Limit, 505_000, 20_000, 102).unwrap();
        assert_eq!(res3.status, OrderStatus::Open);
        assert_eq!(market.best_ask(), Some(505_000));
    }

    #[test]
    fn test_fifo_price_time_priority() {
        let mut market = sample_market();

        // Alice places Sell @ 500,000 for 10,000 XPX at t=100
        let r1 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        // Bob places Sell @ 500,000 for 10,000 XPX at t=101
        let r2 = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 101).unwrap();

        assert_eq!(r1.order_id, 1);
        assert_eq!(r2.order_id, 2);

        // Charlie places matching Buy @ 500,000 for 15,000 XPX
        let taker_res = market.place_order(CHARLIE, OrderSide::Buy, OrderType::Limit, 500_000, 15_000, 105).unwrap();
        assert_eq!(taker_res.status, OrderStatus::Filled);
        assert_eq!(taker_res.fills.len(), 2);

        // Fill 1 must be Alice (FIFO) for 10,000
        assert_eq!(taker_res.fills[0].maker_order_id, 1);
        assert_eq!(taker_res.fills[0].maker_pubkey, ALICE);
        assert_eq!(taker_res.fills[0].base_amount, 10_000);

        // Fill 2 must be Bob for remaining 5,000
        assert_eq!(taker_res.fills[1].maker_order_id, 2);
        assert_eq!(taker_res.fills[1].maker_pubkey, BOB);
        assert_eq!(taker_res.fills[1].base_amount, 5_000);

        // Bob should have 5,000 remaining on book
        let depth = market.get_depth(5);
        assert_eq!(depth.asks.len(), 1);
        assert_eq!(depth.asks[0].total_depth, 5_000);
    }

    #[test]
    fn test_post_only_order() {
        let mut market = sample_market();

        // Alice places Sell @ 500,000
        market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 100).unwrap();

        // Bob tries PostOnly Buy @ 500,000 (would cross book immediately) -> must reject
        let res = market.place_order(BOB, OrderSide::Buy, OrderType::PostOnly, 500_000, 5_000, 101).unwrap();
        assert_eq!(res.status, OrderStatus::Rejected);
        assert_eq!(res.refund_quote, MarketPair::calculate_quote_amount(5_000, 500_000));
        assert_eq!(res.refund_base, 0);

        // Bob places PostOnly Buy @ 499,000 (does not cross) -> must succeed as maker
        let res_ok = market.place_order(BOB, OrderSide::Buy, OrderType::PostOnly, 499_000, 5_000, 102).unwrap();
        assert_eq!(res_ok.status, OrderStatus::Open);
    }

    #[test]
    fn test_ioc_order() {
        let mut market = sample_market();

        // Alice places Sell @ 500,000 for 6,000 XPX
        market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 6_000, 100).unwrap();

        // Bob places IOC Buy @ 500,000 for 10,000 XPX
        let res = market.place_order(BOB, OrderSide::Buy, OrderType::IOC, 500_000, 10_000, 101).unwrap();
        assert_eq!(res.status, OrderStatus::PartiallyFilled);
        assert_eq!(res.total_base_filled, 6_000);
        assert_eq!(res.remaining_amount, 4_000);
        // Refund unspent quote escrow for 4,000 XPX
        assert_eq!(res.refund_quote, MarketPair::calculate_quote_amount(4_000, 500_000));
        assert_eq!(res.refund_base, 0);

        // Book should be empty now
        assert_eq!(market.best_ask(), None);
        assert_eq!(market.best_bid(), None);
    }

    #[test]
    fn test_self_trade_prevention() {
        let mut market = sample_market();

        // Alice places Sell @ 500,000 for 10,000 XPX
        market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 100).unwrap();

        // Alice places Buy @ 500,000 for 4,000 XPX (same trader!)
        let res = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 4_000, 101).unwrap();
        // STP (Decrement & Cancel) prevents self-trade: 0 fills emitted
        assert_eq!(res.fills.len(), 0);
        assert_eq!(res.total_base_filled, 0);

        // Incoming Buy cancelled quote escrow refunded
        assert_eq!(res.refund_quote, MarketPair::calculate_quote_amount(4_000, 500_000));
        // Resting Sell cancelled base escrow refunded
        assert_eq!(res.refund_base, 4_000);

        // Resting order should be reduced from 10,000 to 6,000
        let depth = market.get_depth(5);
        assert_eq!(depth.asks[0].total_depth, 6_000);
    }

    #[test]
    fn test_price_improvement_refund() {
        let mut market = sample_market();

        // Alice places resting Ask @ 500,000 ($0.0050) for 10,000 XPX
        market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 100).unwrap();

        // Bob places Limit Buy @ 600,000 ($0.0060) for 10,000 XPX
        // Bob deposited 10,000 * 600,000 / 1e8 = 60,000 quote units ($0.06)
        // Matches at Alice's resting price: 10,000 * 500,000 / 1e8 = 50,000 quote units ($0.05)
        let res = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 600_000, 10_000, 101).unwrap();
        assert_eq!(res.status, OrderStatus::Filled);
        assert_eq!(res.total_base_filled, 10_000);
        assert_eq!(res.total_quote_filled, 50);

        // Bob receives 10 quote units price improvement refund (60 - 50 = 10)
        assert_eq!(res.refund_quote, 10);
        assert_eq!(res.refund_base, 0);
    }

    #[test]
    fn test_cancel_order() {
        let mut market = sample_market();

        let res = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        assert_eq!(market.best_bid(), Some(500_000));

        // Bob cannot cancel Alice's order
        let unauthorized = market.cancel_order(res.order_id, BOB);
        assert!(unauthorized.is_err());

        // Alice cancels her order
        let (side, refund, price) = market.cancel_order(res.order_id, ALICE).unwrap();
        assert_eq!(side, OrderSide::Buy);
        assert_eq!(price, 500_000);
        assert_eq!(refund, MarketPair::calculate_quote_amount(10_000, 500_000));

        // Book should be clear
        assert_eq!(market.best_bid(), None);
    }

    #[test]
    fn test_order_indexing_consistency() {
        let mut market = sample_market();

        let o1 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        let o2 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 501_000, 15_000, 101).unwrap();
        let o3 = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 505_000, 20_000, 102).unwrap();

        assert_eq!(market.order_index.len(), 3);
        assert_eq!(market.order_index.get(&o1.order_id), Some(&(OrderSide::Buy, 500_000)));
        assert_eq!(market.order_index.get(&o2.order_id), Some(&(OrderSide::Buy, 501_000)));
        assert_eq!(market.order_index.get(&o3.order_id), Some(&(OrderSide::Sell, 505_000)));

        // Charlie fills o3 completely
        let _ = market.place_order(CHARLIE, OrderSide::Buy, OrderType::Limit, 505_000, 20_000, 103).unwrap();
        assert_eq!(market.order_index.get(&o3.order_id), None);
        assert_eq!(market.order_index.len(), 2);

        // Cancel o1
        let _ = market.cancel_order(o1.order_id, ALICE).unwrap();
        assert_eq!(market.order_index.get(&o1.order_id), None);
        assert_eq!(market.order_index.len(), 1);

        // Test rebuild_order_index
        market.order_index.clear();
        assert!(market.order_index.is_empty());
        market.rebuild_order_index();
        assert_eq!(market.order_index.len(), 1);
        assert_eq!(market.order_index.get(&o2.order_id), Some(&(OrderSide::Buy, 501_000)));
    }

    #[test]
    fn test_cancel_batch() {
        let mut market = sample_market();

        let o1 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        let _o2 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 501_000, 10_000, 101).unwrap();
        let o3 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 505_000, 5_000, 102).unwrap();
        let _o4 = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 506_000, 5_000, 103).unwrap();

        // Alice batch cancels o1 and o3 (one buy, one sell)
        let res = market.cancel_batch(&[o1.order_id, o3.order_id], ALICE);
        assert_eq!(res.cancelled_order_ids.len(), 2);
        assert!(res.cancelled_order_ids.contains(&o1.order_id));
        assert!(res.cancelled_order_ids.contains(&o3.order_id));
        assert_eq!(res.total_refund_base, 5_000); // o3 sell XPX
        assert_eq!(res.total_refund_quote, MarketPair::calculate_quote_amount(10_000, 500_000)); // o1 buy USDT

        // o2 (Alice) and o4 (Bob) must still be on book
        assert_eq!(market.best_bid(), Some(501_000));
        assert_eq!(market.best_ask(), Some(506_000));
        assert_eq!(market.order_index.len(), 2);
    }

    #[test]
    fn test_cancel_all() {
        let mut market = sample_market();

        let o1 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        let o2 = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 501_000, 10_000, 101).unwrap();
        let o3 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 505_000, 5_000, 102).unwrap();
        let o4 = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 499_000, 8_000, 103).unwrap();
        let o5 = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 506_000, 8_000, 104).unwrap();

        // Alice calls cancel_all
        let res = market.cancel_all(ALICE);
        assert_eq!(res.cancelled_order_ids.len(), 3);
        assert!(res.cancelled_order_ids.contains(&o1.order_id));
        assert!(res.cancelled_order_ids.contains(&o2.order_id));
        assert!(res.cancelled_order_ids.contains(&o3.order_id));
        assert_eq!(res.total_refund_base, 5_000);
        let exp_quote = MarketPair::calculate_quote_amount(10_000, 500_000) + MarketPair::calculate_quote_amount(10_000, 501_000);
        assert_eq!(res.total_refund_quote, exp_quote);

        // Bob's orders must still be on book
        assert_eq!(market.best_bid(), Some(499_000));
        assert_eq!(market.best_ask(), Some(506_000));
        assert_eq!(market.order_index.len(), 2);
        assert_eq!(market.order_index.get(&o4.order_id), Some(&(OrderSide::Buy, 499_000)));
        assert_eq!(market.order_index.get(&o5.order_id), Some(&(OrderSide::Sell, 506_000)));
    }

    #[test]
    fn test_fee_conservation_every_match() {
        let mut market = sample_market();

        // 1. Maker Buy (Alice) + Taker Sell (Bob): 10 XPX @ 0.005000 USDT
        // Alice deposits 50,000 quote escrow (10,000,000 base * 500,000 ticks / 1e8)
        let r_buy = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 500_000, 10_000_000, 100).unwrap();
        assert_eq!(r_buy.status, OrderStatus::Open);

        // Bob sells 10,000,000 base (10 XPX)
        let r_sell = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 500_000, 10_000_000, 101).unwrap();
        assert_eq!(r_sell.status, OrderStatus::Filled);
        assert_eq!(r_sell.fills.len(), 1);

        let fill = &r_sell.fills[0];
        // Conservation check for Base:
        // Bob deposited 10,000,000 base escrow. Alice receives fill.base_amount.
        assert_eq!(fill.base_amount, 10_000_000);

        // Conservation check for Quote:
        // Alice deposited 50,000 quote escrow.
        // Bob (taker seller) receives net_quote = fill.quote_amount - fill.taker_fee
        // Protocol receives fill.taker_fee.
        let bob_payout = fill.quote_amount - fill.taker_fee;
        let fee = fill.taker_fee;
        let escrow_consumed = fill.quote_amount;
        assert_eq!(bob_payout, 49_950);
        assert_eq!(fee, 50);
        assert_eq!(bob_payout + fee, escrow_consumed, "Payout + fee must exactly equal escrow consumed");
        assert_eq!(bob_payout + fee, 50_000);

        // 2. Maker Sell (Alice) + Taker Buy (Bob): 20 XPX @ 0.005000 USDT
        let r_ask = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 20_000_000, 102).unwrap();
        assert_eq!(r_ask.status, OrderStatus::Open);

        let r_bid = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 500_000, 20_000_000, 103).unwrap();
        assert_eq!(r_bid.status, OrderStatus::Filled);
        assert_eq!(r_bid.fills.len(), 1);

        let fill2 = &r_bid.fills[0];
        // Symmetric Conservation check for Base:
        // Alice deposited 20,000,000 base escrow.
        // Bob (taker buyer) receives net_base = fill2.base_amount - fill2.taker_fee (10 bps)
        // Protocol receives fill2.taker_fee in Base.
        let bob_payout_base = fill2.base_amount - fill2.taker_fee;
        let fee_base = fill2.taker_fee;
        let base_escrow_consumed = fill2.base_amount;
        assert_eq!(bob_payout_base, 19_980_000);
        assert_eq!(fee_base, 20_000);
        assert_eq!(bob_payout_base + fee_base, base_escrow_consumed, "Base payout + fee must exactly equal base escrow consumed");

        // Conservation check for Quote:
        // Bob deposited 100,000 quote escrow.
        // Alice (maker seller) receives quote_amount - maker_fee (0 bps)
        let alice_payout_quote = fill2.quote_amount - fill2.maker_fee;
        let fee_quote = fill2.maker_fee;
        let quote_escrow_consumed = fill2.quote_amount;
        assert_eq!(alice_payout_quote + fee_quote, quote_escrow_consumed, "Quote payout + fee must exactly equal quote escrow consumed");
        assert_eq!(alice_payout_quote, 100_000);
        assert_eq!(fee_quote, 0);
    }

    #[test]
    fn test_symmetric_fee_schedule_taker_buyer_and_taker_seller() {
        // Market with 5 bps maker fee and 15 bps taker fee
        let mut market = MarketPair::new(
            0x402B2F579FAEBC59, // XPX
            0x0DC67FBE1CAD29E3, // USDT
            100,
            1_000,
            5,  // 0.05% maker fee
            15, // 0.15% taker fee
        );

        // Scenario A: Taker is Buyer (Bob crosses Alice's resting Ask)
        // Alice resting Sell: 10,000,000 base (10 XPX) @ 500,000 ($0.0050)
        let _r_ask = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000_000, 100).unwrap();
        // Bob incoming Buy: 10,000,000 base (10 XPX) @ 500,000 ($0.0050)
        let r_bid = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 500_000, 10_000_000, 101).unwrap();
        assert_eq!(r_bid.fills.len(), 1);
        let fill_a = &r_bid.fills[0];

        // Taker (Bob) bought base XPX: pays 15 bps taker fee on received base
        // 10,000,000 * 15 / 10,000 = 15,000 units
        assert_eq!(fill_a.taker_fee, 15_000);
        let bob_net_base = fill_a.base_amount - fill_a.taker_fee;
        assert_eq!(bob_net_base, 9_985_000);
        assert_eq!(bob_net_base + fill_a.taker_fee, fill_a.base_amount);

        // Maker (Alice) sold base XPX and received quote USDT: pays 5 bps maker fee on quote proceeds
        // Quote amount = 50,000 units ($0.05)
        // 50,000 * 5 / 10,000 = 25 units
        assert_eq!(fill_a.maker_fee, 25);
        let alice_net_quote = fill_a.quote_amount - fill_a.maker_fee;
        assert_eq!(alice_net_quote, 49_975);
        assert_eq!(alice_net_quote + fill_a.maker_fee, fill_a.quote_amount);

        // Scenario B: Taker is Seller (Charlie crosses David's resting Bid)
        const DAVID: [u8; 32] = [4u8; 32];
        const CHARLIE2: [u8; 32] = [5u8; 32];
        // David resting Buy: 10,000,000 base (10 XPX) @ 500,000 ($0.0050)
        let _r_bid2 = market.place_order(DAVID, OrderSide::Buy, OrderType::Limit, 500_000, 10_000_000, 102).unwrap();
        // Charlie incoming Sell: 10,000,000 base (10 XPX) @ 500,000 ($0.0050)
        let r_sell2 = market.place_order(CHARLIE2, OrderSide::Sell, OrderType::Limit, 500_000, 10_000_000, 103).unwrap();
        assert_eq!(r_sell2.fills.len(), 1);
        let fill_b = &r_sell2.fills[0];

        // Taker (Charlie) sold base XPX and received quote USDT: pays 15 bps taker fee on quote proceeds
        // 50,000 * 15 / 10,000 = 75 units
        assert_eq!(fill_b.taker_fee, 75);
        let charlie_net_quote = fill_b.quote_amount - fill_b.taker_fee;
        assert_eq!(charlie_net_quote, 49_925);
        assert_eq!(charlie_net_quote + fill_b.taker_fee, fill_b.quote_amount);

        // Maker (David) bought base XPX and received base proceeds: pays 5 bps maker fee on received base
        // 10,000,000 * 5 / 10,000 = 5,000 units
        assert_eq!(fill_b.maker_fee, 5_000);
        let david_net_base = fill_b.base_amount - fill_b.maker_fee;
        assert_eq!(david_net_base, 9_995_000);
        assert_eq!(david_net_base + fill_b.maker_fee, fill_b.base_amount);
    }

    #[test]
    fn test_ioc_unfilled_status_cancelled() {
        let mut market = sample_market();

        // Bob places IOC Buy @ 500,000 for 10,000 XPX into an empty book
        let res = market.place_order(BOB, OrderSide::Buy, OrderType::IOC, 500_000, 10_000, 100).unwrap();
        assert_eq!(res.status, OrderStatus::Cancelled);
        assert_eq!(res.fills.len(), 0);
        assert_eq!(res.total_base_filled, 0);
        assert_eq!(res.remaining_amount, 10_000);
        assert_eq!(res.refund_quote, MarketPair::calculate_quote_amount(10_000, 500_000));
        assert_eq!(market.best_bid(), None);
        assert_eq!(market.best_ask(), None);

        // Alice places IOC Sell @ 500,000 for 10,000 XPX into an empty book
        let res_sell = market.place_order(ALICE, OrderSide::Sell, OrderType::IOC, 500_000, 10_000, 101).unwrap();
        assert_eq!(res_sell.status, OrderStatus::Cancelled);
        assert_eq!(res_sell.fills.len(), 0);
        assert_eq!(res_sell.total_base_filled, 0);
        assert_eq!(res_sell.remaining_amount, 10_000);
        assert_eq!(res_sell.refund_base, 10_000);
        assert_eq!(market.best_bid(), None);
        assert_eq!(market.best_ask(), None);
    }

    #[test]
    fn test_quote_overflow_and_fee_bounds() {
        let mut market = sample_market();

        // Excessive price / amount producing nominal quote > u64::MAX must be rejected
        let tick_aligned_large_price = (u64::MAX / 100) * 100;
        let res = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, tick_aligned_large_price, u64::MAX, 100);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), "Order value overflows u64 quote limit");

        // Fee cap: fee_bps >= 10,000 caps fee to 100% of quote_amt without truncating
        assert_eq!(MarketPair::calculate_fee(1_000_000, 10_000), 1_000_000);
        assert_eq!(MarketPair::calculate_fee(1_000_000, 25_000), 1_000_000);

        // Quote calculation truncation safety: saturated to u64::MAX if overflow
        let saturated_quote = MarketPair::calculate_quote_amount(u64::MAX, u64::MAX);
        assert_eq!(saturated_quote, u64::MAX);
    }

    #[test]
    fn test_extreme_tick_boundaries_and_zero_rejection() {
        // Market with minimum tick size = 1 and min order size = 1
        let mut market = MarketPair::new(
            0x402B2F579FAEBC59, // XPX
            0x0DC67FBE1CAD29E3, // USDT
            1,                  // tick size = 1 unit
            1,                  // min order size = 1 unit
            0,                  // 0.00% maker fee
            10,                 // 0.10% taker fee
        );

        // Price = 0 must be rejected
        let res_zero_price = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 0, 100, 100);
        assert_eq!(res_zero_price.err().unwrap(), "Price must be strictly positive");

        // Amount = 0 must be rejected
        let res_zero_amount = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 100, 0, 101);
        assert_eq!(res_zero_amount.err().unwrap(), "Order amount below minimum lot size");

        // Minimum tick price = 1 is accepted
        let res_min_tick = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 1, 100_000_000, 102).unwrap();
        assert_eq!(res_min_tick.status, OrderStatus::Open);
        assert_eq!(market.best_ask(), Some(1));

        // High valid price within safe bounds
        let high_price = 10_000_000_000u64; // $100.00 per unit in micro-ticks
        let res_high = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, high_price, 1_000_000, 103).unwrap();
        assert_eq!(res_high.status, OrderStatus::Open);
        assert_eq!(market.best_ask(), Some(1)); // Min tick 1 is still best ask
    }

    #[test]
    fn test_sub_micro_unit_rounding_and_fee_ceiling() {
        // 1 micro-unit order with ceiling fee calculation
        let mut market = MarketPair::new(
            0x402B2F579FAEBC59,
            0x0DC67FBE1CAD29E3,
            1,
            1,
            0,  // maker fee = 0
            10, // taker fee = 10 bps
        );

        // Alice rests Sell for 1 unit of XPX @ $1.00 (100,000,000 ticks)
        market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 100_000_000, 1, 100).unwrap();

        // Bob crosses with Buy for 1 unit of XPX @ $1.00
        let res = market.place_order(BOB, OrderSide::Buy, OrderType::Limit, 100_000_000, 1, 101).unwrap();
        assert_eq!(res.status, OrderStatus::Filled);
        assert_eq!(res.fills.len(), 1);

        let fill = &res.fills[0];
        assert_eq!(fill.base_amount, 1);
        assert_eq!(fill.quote_amount, 1);

        // Ceiling fee on 1 unit @ 10 bps: (1 * 10 + 9,999) / 10,000 = 10,009 / 10,000 = 1 unit
        assert_eq!(fill.taker_fee, 1);
        // Net delivery to Bob: 1 - 1 = 0 units (never underflows to negative)
        let net_bob_base = fill.base_amount.checked_sub(fill.taker_fee).unwrap();
        assert_eq!(net_bob_base, 0);

        // Zero amount or zero fee_bps strictly produces 0 fee
        assert_eq!(MarketPair::calculate_fee(0, 10), 0);
        assert_eq!(MarketPair::calculate_fee(100, 0), 0);
    }

    #[test]
    fn test_consecutive_multi_order_stp_ladder() {
        let mut market = sample_market(); // tick size = 100, min order size = 1,000

        // Alice places 3 consecutive resting Ask orders across different price levels
        let r1 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 500_000, 10_000, 100).unwrap();
        let r2 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 510_000, 20_000, 101).unwrap();
        let r3 = market.place_order(ALICE, OrderSide::Sell, OrderType::Limit, 520_000, 30_000, 102).unwrap();

        // Bob places a higher resting Ask order
        let r4 = market.place_order(BOB, OrderSide::Sell, OrderType::Limit, 530_000, 50_000, 103).unwrap();

        assert_eq!(market.best_ask(), Some(500_000));
        assert_eq!(market.order_index.len(), 4);

        // Alice submits a large crossing Buy for 70,000 XPX @ 530,000
        // Expected outcome:
        // - Level 500,000 (10,000 XPX): Alice's order -> STP decrements & cancels 10,000 XPX (60,000 remaining)
        // - Level 510,000 (20,000 XPX): Alice's order -> STP decrements & cancels 20,000 XPX (40,000 remaining)
        // - Level 520,000 (30,000 XPX): Alice's order -> STP decrements & cancels 30,000 XPX (10,000 remaining)
        // - Level 530,000 (10,000 XPX): Bob's order   -> Fills 10,000 XPX against Bob! (0 remaining)
        let res = market.place_order(ALICE, OrderSide::Buy, OrderType::Limit, 530_000, 70_000, 104).unwrap();

        assert_eq!(res.status, OrderStatus::Filled);
        assert_eq!(res.remaining_amount, 0);
        assert_eq!(res.total_base_filled, 10_000);

        // Exactly 1 trade fill against Bob
        assert_eq!(res.fills.len(), 1);
        assert_eq!(res.fills[0].maker_pubkey, BOB);
        assert_eq!(res.fills[0].base_amount, 10_000);
        assert_eq!(res.fills[0].price, 530_000);

        // STP Base Refund: Alice receives back the 60,000 XPX from her 3 cancelled asks
        assert_eq!(res.refund_base, 60_000);

        // Quote Refund:
        // Initial deposit = (70,000 * 530,000) / 100,000,000 = 371
        // Filled with Bob = (10,000 * 530,000) / 100,000,000 = 53
        // Unspent quote refunded to Alice = 371 - 53 = 318
        assert_eq!(res.refund_quote, 318);

        // Verify Order Book State:
        // Alice's 3 orders (r1, r2, r3) must be completely purged from the book and index
        assert_eq!(market.order_index.get(&r1.order_id), None);
        assert_eq!(market.order_index.get(&r2.order_id), None);
        assert_eq!(market.order_index.get(&r3.order_id), None);

        // Bob's order remains with 40,000 XPX
        assert_eq!(market.best_ask(), Some(530_000));
        assert_eq!(market.order_index.get(&r4.order_id), Some(&(OrderSide::Sell, 530_000)));
        assert_eq!(market.asks.get(&530_000).unwrap().total_depth, 40_000);
        assert_eq!(market.asks.get(&530_000).unwrap().orders[0].remaining_amount, 40_000);
    }
}
