# ⚡ SiriusCLOB: High-Performance On-Chain Central Limit Order Book DEX

**SiriusCLOB** is an autonomous, on-chain **Central Limit Order Book (CLOB) Decentralized Exchange** engineered natively for **ProximaX Sirius SuperContract V2**.

Inspired by battle-tested CLOB architectures such as **OpenBook** (Solana), **Hyperliquid** (L1), and **Kujira FIN** (Cosmos), SiriusCLOB pairs the deterministic execution speed of pure WebAssembly with the persistent, zero-copy I/O throughput of **Sirius Replicator Drives** (DFMS) and Catapult's native **Aggregate Complete Transaction** settlement.

---

## 🏛️ Legal & Governance Model: Pathway 1 (Pure Public Good)

SiriusCLOB is published strictly under the **MIT Open-Source License** following **Pathway 1: Open-Source Contributor / Public Good**:
1. **Escrow Architecture**: Escrow tokens are transferred to and held by an operator-controlled contract account on chain, with settlement payouts governed by 3-node WASM consensus.
2. **Zero Centralized Infrastructure**: There is no off-chain matching server, no centralized order routing API, and no proprietary database. Order matching occurs inside the 3-node BFT WASM virtual machine.
3. **No Private Fee Skimming**: All protocol parameters (lot sizes, tick sizes, maker rebates) are governed by on-chain consensus.

---

## 🚀 Key Architectural Advantages

| Metric / Dimension | EVM DEX (Uniswap / dYdX V3) | SiriusCLOB on SuperContract V2 |
| :--- | :--- | :--- |
| **Execution Engine** | 256-bit EVM Bytecode | **wasm32-unknown-unknown (133 KB Binary)** |
| **Price Precision** | Floating point / EVM BigNumber | **Fixed-Point $10^8$ Integer Price Ticks (`PriceTick`)** |
| **Priority Queue** | Merkle Trie traversal ($O(N)$ storage gas) | **$O(\log N)$ In-Memory BTreeMap + FIFO Price-Time Queue** |
| **Persistence Layer** | EVM State Storage ($$$ prohibitive) | **Zero-Copy Binary State on Sirius Replicator Drive (< 2ms I/O)** |
| **Settlement Protocol**| Multi-step approvals & internal balances | **Native Catapult Aggregate Complete Transactions (`0x4154`)** |
| **MEV Protection** | Susceptible to sandwich attacks / frontrunning| **Deterministic FIFO Price-Time Batching with STP** |

---

## ⚙️ Matching Engine Capabilities

### 1. Supported Order Types
* **Limit Order (GTC)**: Rests in the order book at the designated price tick if uncrossed.
* **Immediate Or Cancel (IOC)**: Fills all available liquidity across crossing price levels and immediately refunds any unfilled remainder.
* **Post-Only (Maker-Only)**: Protects liquidity providers by rejecting any order that would cross the book as a taker, guaranteeing maker status.

### 2. Self-Trade Prevention (STP)
Enforces **Decrement & Cancel (DC)**: If a trader's order crosses their own resting order on the book, the matching engine decrements the matched quantity without transferring tokens or charging fees, eliminating accidental self-execution and wash trading.

### 3. Replicator Drive Persistence
The complete order book state is serialized into an ultra-compact binary format:
```
drive/
└── markets/
    └── xpx_usdt/
        ├── market_state.bin    # BTreeMap price levels & active orders (~60 KB)
        ├── trades.bin          # Rolling circular history of last 500 fills
        └── depth.json          # Real-time JSON snapshot for client UIs
```

---

## 🧪 Quickstart & Testing

### 1. Run Unit Test Suite
```bash
cd contracts/clob
cargo test --lib orderbook
```
Output:
```
running 6 tests
test orderbook::tests::test_cancel_order ... ok
test orderbook::tests::test_fifo_price_time_priority ... ok
test orderbook::tests::test_ioc_order ... ok
test orderbook::tests::test_post_only_order ... ok
test orderbook::tests::test_resting_limit_orders ... ok
test orderbook::tests::test_self_trade_prevention ... ok
test result: ok. 6 passed; 0 failed
```

### 2. Compile Release WASM
```bash
cargo build --target wasm32-unknown-unknown --release
# Generates: target/wasm32-unknown-unknown/release/sirius_clob.wasm (133 KB)
```

### 3. Run Multi-Node Cluster E2E Suite
From the repository root:
```bash
node scripts/test_clob_e2e.js
```
Validates:
* 3/3 BFT WASM VM state consensus across `node1 (:5051)`, `node2 (:5052)`, `node3 (:5053)`.
* Parallel order matching and trade settlement.
* Real-time block commitment in live Catapult ledger.

### 4. Launch Trading Cockpit Dashboard
```bash
# Dashboard runs on port 8088
open http://localhost:8088/clob
```
Features live dual-column depth ladder, interactive candlestick chart, real-time trade feed, and 1-click order cancellation with automated escrow refunds.
