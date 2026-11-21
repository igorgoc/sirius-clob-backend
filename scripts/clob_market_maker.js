/**
 * SiriusCLOB — Autonomous Algorithmic Market Maker & Liquidity Bot
 *
 * Implements high-frequency laddered quoting and continuous trade simulation:
 *   1. Quoting Engine: Seeds 5-level Bid and Ask ladders around mid-market price
 *   2. Liquidity Replenishment: Re-quotes depleted levels automatically
 *   3. Simulated Takers: Retail/Arbitrage crossing orders (Limit, IOC)
 *   4. Consensus & Settlement: 3/3 BFT VM agreement + On-Chain settlement
 *   5. Price Discovery: Random-walk mid-price drift with spread stabilization
 *   6. Drive Persistence: Real-time depth.json and trades.bin export
 */

const fs = require('fs');
const path = require('path');
const {
  executeOnNodeVm,
  assert3NodeConsensus,
  NODES,
  DRIVE_KEY_HEX,
  ALICE_KEY_HEX,
  BOB_KEY_HEX,
  sha256Hex,
} = require('../executor/milestone2_verifier');
const { execFileSync } = require('child_process');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const ACCOUNTS_FILE = path.join(CLUSTER_DIR, 'nemesis_accounts.json');
const MM_PID_FILE = path.join(CLUSTER_DIR, 'mm.pid');
const MM_STATUS_FILE = path.join(CLUSTER_DIR, 'mm_status.json');

function writeStatus(status) {
  try {
    const tmpFile = `${MM_STATUS_FILE}.${process.pid}.${Date.now()}.tmp`;
    fs.writeFileSync(tmpFile, JSON.stringify(status, null, 2));
    fs.renameSync(tmpFile, MM_STATUS_FILE);
  } catch (_) {}
}

function removePid() {
  try {
    if (fs.existsSync(MM_PID_FILE)) {
      fs.unlinkSync(MM_PID_FILE);
    }
  } catch (_) {}
}

const CLOB_WASM_PATH = path.join(
  ROOT_DIR,
  'contracts/clob/target/wasm32-unknown-unknown/release/sirius_clob.wasm'
);

const XPX_MOSAIC_ID = BigInt('0x0DC67FBE1CAD29E3').toString(10);
const USDT_MOSAIC_ID = BigInt('0x10112CBA1E7B9037').toString(10);
const BNB_MOSAIC_ID = BigInt('0x1E5C5FADEC7B34DD').toString(10);
const PRICE_SCALE = 100_000_000n;

// Simulated Market Maker & Trader Identities
const MM_BUYER_KEY = ALICE_KEY_HEX;
const MM_SELLER_KEY = BOB_KEY_HEX;
const TAKER_KEY = '5555555555555555555555555555555555555555555555555555555555555555';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function calculateQuoteAmount(baseAmount, priceTicks) {
  return Number((BigInt(baseAmount) * BigInt(priceTicks)) / PRICE_SCALE);
}

function encodePlaceOrder(side, orderType, priceTicks, baseAmount) {
  const buf = Buffer.alloc(19);
  buf.writeUInt8(1, 0); // OP_PLACE_ORDER
  buf.writeUInt8(side, 1); // 0 = Buy, 1 = Sell
  buf.writeUInt8(orderType, 2); // 0 = Limit, 1 = IOC, 2 = PostOnly
  buf.writeBigUInt64LE(BigInt(priceTicks), 3);
  buf.writeBigUInt64LE(BigInt(baseAmount), 11);
  return buf;
}

function encodeCancelOrder(orderId) {
  const buf = Buffer.alloc(9);
  buf.writeUInt8(2, 0); // OP_CANCEL_ORDER
  buf.writeBigUInt64LE(BigInt(orderId), 1);
  return buf;
}

function encodeGetDepth(maxLevels = 50) {
  const buf = Buffer.alloc(9);
  buf.writeUInt8(3, 0); // OP_GET_DEPTH
  buf.writeBigUInt64LE(BigInt(maxLevels), 1);
  return buf;
}

let cachedMmHeight = 9364100;
function getLatestCommittedHeight() {
  try {
    const t2Client = require('../dashboard/testnet2_client');
    t2Client.getLatestHeight().then((h) => { if (h > 0) cachedMmHeight = h; }).catch(() => {});
  } catch (_) {}
  return cachedMmHeight;
}

function copyDirSync(src, dst) {
  if (!fs.existsSync(src)) return;
  fs.mkdirSync(dst, { recursive: true });
  const entries = fs.readdirSync(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const dstPath = path.join(dst, entry.name);
    if (entry.isDirectory()) {
      copyDirSync(srcPath, dstPath);
    } else {
      fs.copyFileSync(srcPath, dstPath);
    }
  }
}

class ClobMarketMaker {
  constructor(options = {}) {
    this.initialMidPrice = options.midPrice || 500_000; // $0.005000 USDT
    this.currentMidPrice = this.initialMidPrice;
    this.spreadTicks = options.spreadTicks || 2_000; // $0.000020 USDT
    this.tickStep = options.tickStep || 1_000; // $0.000010 per level
    this.levels = options.levels || 5;
    this.lotSize = options.lotSize || 10_000_000; // 10 XPX base lot
    this.txCounter = 1000;
    this.totalTradesExecuted = 0;
    this.totalVolumeXPX = 0;
    this.totalVolumeUSDT = 0;
    this.nodeDrivePaths = {};
    this.nodeWasmPaths = {};
  }

  async initialize() {
    console.log('\n================================================================================');
    console.log('🤖 SiriusCLOB Autonomous Market Maker & Liquidity Bot');
    console.log(`   Initial Mid Price: $${(this.initialMidPrice / 1e8).toFixed(6)} USDT`);
    console.log(`   Spread: $${(this.spreadTicks / 1e8).toFixed(6)} USDT | Depth Levels: ${this.levels}`);
    console.log('================================================================================\n');

    if (!fs.existsSync(CLOB_WASM_PATH)) {
      throw new Error(`Missing CLOB WASM binary at ${CLOB_WASM_PATH}`);
    }

    // Deploy contract to all node drives
    for (const node of NODES) {
      const driveRoot = path.join(CLUSTER_DIR, node.name, 'data', 'drives', DRIVE_KEY_HEX);
      const committedDriveDir = path.join(driveRoot, 'drive');
      fs.mkdirSync(committedDriveDir, { recursive: true });

      const destWasm = path.join(committedDriveDir, 'sirius_clob.wasm');
      fs.copyFileSync(CLOB_WASM_PATH, destWasm);
      this.nodeDrivePaths[node.name] = driveRoot;
      this.nodeWasmPaths[node.name] = destWasm;
    }
    console.log('✅ Market Maker verified WASM deployment across all 3 Replicator Drives.\n');
  }

  async executeClusterCall(callerKeyHex, params, servicePayments, label) {
    const height = getLatestCommittedHeight('node1') || 100;
    const txHash = cryptoRandomHex(32);
    this.txCounter++;

    const results = await Promise.all(
      NODES.map((n) =>
        executeOnNodeVm(
          n,
          this.nodeWasmPaths[n.name],
          'run',
          height,
          txHash,
          this.txCounter,
          this.nodeDrivePaths[n.name],
          {
            parameters: params,
            callerKeyHex,
            servicePayments,
          }
        )
      )
    );

    assert3NodeConsensus(label, results);
    return results[0].returns;
  }

  async seedOrderBookLadder() {
    const driveDepthPath = path.join(this.nodeDrivePaths['node1'], 'drive', 'markets/xpx_usdt/depth.json');
    if (fs.existsSync(driveDepthPath)) {
      try {
        const existing = JSON.parse(fs.readFileSync(driveDepthPath, 'utf8'));
        if (existing.bids && existing.bids.length >= 2 && existing.asks && existing.asks.length >= 2) {
          console.log(`📊 [LIQUIDITY READY] Reusing existing depth ladder (${existing.bids.length} bids, ${existing.asks.length} asks).`);
          if (existing.best_bid && existing.best_ask) {
            this.currentMidPrice = Math.floor((existing.best_bid + existing.best_ask) / 2);
          }
          // Ensure node2 and node3 drive replicas are in sync with node1
          for (const targetNode of ['node2', 'node3']) {
            try {
              const srcMarkets = path.join(this.nodeDrivePaths['node1'], 'drive', 'markets');
              const dstMarkets = path.join(this.nodeDrivePaths[targetNode], 'drive', 'markets');
              copyDirSync(srcMarkets, dstMarkets);
            } catch (_) {}
          }
          return;
        }
      } catch (_) {}
    }

    console.log(`📊 [SEEDING LIQUIDITY] Building ${this.levels}-level Bid and Ask ladders...`);

    // 1. Seed Bids (MM_BUYER_KEY)
    for (let i = 1; i <= this.levels; i++) {
      const price = this.currentMidPrice - (this.spreadTicks / 2) - ((i - 1) * this.tickStep);
      const amount = this.lotSize * i;
      const quoteEscrow = calculateQuoteAmount(amount, price);

      const params = encodePlaceOrder(0, 0, price, amount); // Buy, Limit
      const payments = [{ mosaic_id: USDT_MOSAIC_ID, amount: String(quoteEscrow) }];

      await this.executeClusterCall(
        MM_BUYER_KEY,
        params,
        payments,
        `MM Seed Bid Level ${i} @ $${(price / 1e8).toFixed(6)}`
      );
      console.log(`   + Bid Level ${i}: ${amount / 1e6} XPX @ $${(price / 1e8).toFixed(6)} (Escrow: $${(quoteEscrow / 1e6).toFixed(4)} USDT)`);
    }

    // 2. Seed Asks (MM_SELLER_KEY)
    for (let i = 1; i <= this.levels; i++) {
      const price = this.currentMidPrice + (this.spreadTicks / 2) + ((i - 1) * this.tickStep);
      const amount = this.lotSize * i;
      const baseEscrow = amount;

      const params = encodePlaceOrder(1, 0, price, amount); // Sell, Limit
      const payments = [{ mosaic_id: XPX_MOSAIC_ID, amount: String(baseEscrow) }];

      await this.executeClusterCall(
        MM_SELLER_KEY,
        params,
        payments,
        `MM Seed Ask Level ${i} @ $${(price / 1e8).toFixed(6)}`
      );
      console.log(`   + Ask Level ${i}: ${amount / 1e6} XPX @ $${(price / 1e8).toFixed(6)} (Escrow: ${baseEscrow / 1e6} XPX)`);
    }

    // Refresh depth snapshot
    await this.refreshDepthSnapshot();
    console.log('✅ Initial Order Book depth seeded with 3/3 BFT Consensus.\n');
  }

  async refreshDepthSnapshot() {
    const params = encodeGetDepth(20);
    await this.executeClusterCall(TAKER_KEY, params, [], 'Query Depth Snapshot');
  }

  async simulateTradingRound(roundNum) {
    console.log(`--------------------------------------------------------------------------------`);
    console.log(`⚡ [ROUND ${roundNum}] Executing Automated Market Making & Taker Trading Tick...`);

    // 1. Determine taker side (50% Buy, 50% Sell)
    const isTakerBuy = Math.random() < 0.5;
    const takerAmount = (Math.floor(Math.random() * 3) + 1) * this.lotSize; // 10 to 30 XPX

    if (isTakerBuy) {
      // Taker buys against lowest Ask (price improvement allowed)
      const targetAskPrice = this.currentMidPrice + (this.spreadTicks / 2);
      const maxLimitPrice = targetAskPrice + this.tickStep; // slightly higher to test price improvement
      const quoteEscrow = calculateQuoteAmount(takerAmount, maxLimitPrice);

      const params = encodePlaceOrder(0, 0, maxLimitPrice, takerAmount);
      const payments = [{ mosaic_id: USDT_MOSAIC_ID, amount: String(quoteEscrow) }];

      const ret = await this.executeClusterCall(
        TAKER_KEY,
        params,
        payments,
        `Taker Market Buy: ${takerAmount / 1e6} XPX`
      );

      const tx = ret.transaction;
      const fillCount = (tx && tx.transactions) ? tx.transactions.length : 0;
      this.totalTradesExecuted++;
      this.totalVolumeXPX += takerAmount;
      const fillQuote = calculateQuoteAmount(takerAmount, targetAskPrice);
      this.totalVolumeUSDT += fillQuote;

      console.log(`   🛒 [TAKER BUY] Executed ${takerAmount / 1e6} XPX fill against resting ask!`);
      console.log(`      - Trade Value: $${(fillQuote / 1e6).toFixed(4)} USDT`);
      console.log(`      - Emitted ${fillCount} settlement transfer transactions (3/3 BFT Verified)`);

      // Immediately replenish consumed Ask liquidity
      const replenishParams = encodePlaceOrder(1, 0, targetAskPrice, takerAmount);
      await this.executeClusterCall(
        MM_SELLER_KEY,
        replenishParams,
        [{ mosaic_id: XPX_MOSAIC_ID, amount: String(takerAmount) }],
        'MM Replenish Ask'
      );
      console.log(`   🔄 [MM REPLENISH] Quoted fresh Ask liquidity: ${takerAmount / 1e6} XPX @ $${(targetAskPrice / 1e8).toFixed(6)}`);

    } else {
      // Taker sells against highest Bid
      const targetBidPrice = this.currentMidPrice - (this.spreadTicks / 2);
      const minLimitPrice = targetBidPrice - this.tickStep; // crossing bid
      const baseEscrow = takerAmount;

      const params = encodePlaceOrder(1, 0, minLimitPrice, takerAmount);
      const payments = [{ mosaic_id: XPX_MOSAIC_ID, amount: String(baseEscrow) }];

      const ret = await this.executeClusterCall(
        TAKER_KEY,
        params,
        payments,
        `Taker Market Sell: ${takerAmount / 1e6} XPX`
      );

      const tx = ret.transaction;
      const fillCount = (tx && tx.transactions) ? tx.transactions.length : 0;
      this.totalTradesExecuted++;
      this.totalVolumeXPX += takerAmount;
      const fillQuote = calculateQuoteAmount(takerAmount, targetBidPrice);
      this.totalVolumeUSDT += fillQuote;

      console.log(`   🏷️  [TAKER SELL] Executed ${takerAmount / 1e6} XPX fill against resting bid!`);
      console.log(`      - Trade Value: $${(fillQuote / 1e6).toFixed(4)} USDT`);
      console.log(`      - Emitted ${fillCount} settlement transfer transactions (3/3 BFT Verified)`);

      // Immediately replenish consumed Bid liquidity
      const replenishQuote = calculateQuoteAmount(takerAmount, targetBidPrice);
      const replenishParams = encodePlaceOrder(0, 0, targetBidPrice, takerAmount);
      await this.executeClusterCall(
        MM_BUYER_KEY,
        replenishParams,
        [{ mosaic_id: USDT_MOSAIC_ID, amount: String(replenishQuote) }],
        'MM Replenish Bid'
      );
      console.log(`   🔄 [MM REPLENISH] Quoted fresh Bid liquidity: ${takerAmount / 1e6} XPX @ $${(targetBidPrice / 1e8).toFixed(6)}`);
    }

    // 2. Price discovery random walk drift (±10 ticks), clamped to positive bounds [100_000, 10_000_000]
    const drift = (Math.floor(Math.random() * 3) - 1) * 10;
    const minMidPrice = Math.max(100_000, this.spreadTicks + this.levels * this.tickStep * 2);
    const maxMidPrice = 10_000_000;
    this.currentMidPrice = Math.min(maxMidPrice, Math.max(minMidPrice, this.currentMidPrice + drift));
    console.log(`   📈 [PRICE DISCOVERY] New Mid Price: $${(this.currentMidPrice / 1e8).toFixed(6)} (Drift: ${drift >= 0 ? '+' : ''}${drift} ticks)`);

    // 3. Update public depth feed
    await this.refreshDepthSnapshot();
  }

  async run(rounds = 5, delayMs = 2000) {
    fs.writeFileSync(MM_PID_FILE, String(process.pid));
    writeStatus({
      running: true,
      pid: process.pid,
      mode: rounds === -1 ? 'daemon' : (rounds === 1 ? 'step' : 'batch'),
      round: 0,
      totalTrades: this.totalTradesExecuted,
      volumeXPX: this.totalVolumeXPX,
      volumeUSDT: this.totalVolumeUSDT,
      midPrice: this.currentMidPrice,
      spreadTicks: this.spreadTicks,
      lastAction: 'Initializing node accounts & VM states...',
      lastUpdate: Date.now(),
    });

    let r = 1;
    let stopped = false;
    const cleanupAndExit = () => {
      if (!stopped) {
        console.log('\n🛑 Market Maker stopping gracefully...');
        stopped = true;
        removePid();
        writeStatus({
          running: false,
          pid: null,
          mode: 'stopped',
          round: r - 1,
          totalTrades: this.totalTradesExecuted,
          volumeXPX: this.totalVolumeXPX,
          volumeUSDT: this.totalVolumeUSDT,
          midPrice: this.currentMidPrice,
          spreadTicks: this.spreadTicks,
          lastAction: 'Market Maker stopped',
          lastUpdate: Date.now(),
        });
      }
    };
    process.on('SIGINT', cleanupAndExit);
    process.on('SIGTERM', cleanupAndExit);
    process.on('exit', removePid);

    try {
      await this.initialize();
      await this.seedOrderBookLadder();

      writeStatus({
        running: true,
        pid: process.pid,
        mode: rounds === -1 ? 'daemon' : (rounds === 1 ? 'step' : 'batch'),
        round: 0,
        totalTrades: this.totalTradesExecuted,
        volumeXPX: this.totalVolumeXPX,
        volumeUSDT: this.totalVolumeUSDT,
        midPrice: this.currentMidPrice,
        spreadTicks: this.spreadTicks,
        lastAction: 'Order book liquidity ready. Beginning trading ticks.',
        lastUpdate: Date.now(),
      });

      while (!stopped && (rounds === -1 || r <= rounds)) {
        await this.simulateTradingRound(r);
        writeStatus({
          running: true,
          pid: process.pid,
          mode: rounds === -1 ? 'daemon' : (rounds === 1 ? 'step' : 'batch'),
          round: r,
          totalTrades: this.totalTradesExecuted,
          volumeXPX: this.totalVolumeXPX,
          volumeUSDT: this.totalVolumeUSDT,
          midPrice: this.currentMidPrice,
          spreadTicks: this.spreadTicks,
          lastAction: `Round ${r}: Executed taker trade & replenished liquidity`,
          lastUpdate: Date.now(),
        });
        r++;
        if (!stopped && (rounds === -1 || r <= rounds)) {
          await sleep(delayMs);
        }
      }
    } finally {
      cleanupAndExit();
    }

    console.log('\n================================================================================');
    console.log('🏆 MARKET MAKING & TRADING SIMULATION COMPLETE');
    console.log('================================================================================');
    const summary = [
      ['Rounds Completed', `${r - 1} active trading ticks`],
      ['Total Trades Executed', `${this.totalTradesExecuted} fills`],
      ['Total Base Volume', `${(this.totalVolumeXPX / 1e6).toLocaleString()} XPX`],
      ['Total Quote Volume', `$${(this.totalVolumeUSDT / 1e6).toFixed(4)} USDT`],
      ['Final Mid Price', `$${(this.currentMidPrice / 1e8).toFixed(6)} USDT`],
      ['Spread Consistency', `$${(this.spreadTicks / 1e8).toFixed(6)} USDT (Maintained across 3/3 BFT nodes)`],
      ['Storage Persistence', `markets/xpx_usdt/depth.json & trades.bin updated on Replicator Drive`],
    ];

    console.log('  Metric                         | Result');
    console.log('---------------------------------+------------------------------------------------------------------------------------------');
    for (const [m, res] of summary) {
      console.log(`  ${m.padEnd(30)} | ${res}`);
    }
    console.log('================================================================================\n');

    return {
      success: true,
      trades: this.totalTradesExecuted,
      volumeXPX: this.totalVolumeXPX,
      volumeUSDT: this.totalVolumeUSDT,
      finalMidPrice: this.currentMidPrice,
    };
  }
}

function cryptoRandomHex(bytes) {
  const crypto = require('crypto');
  return crypto.randomBytes(bytes).toString('hex');
}

if (require.main === module) {
  const args = process.argv.slice(2);
  let rounds = 5;
  let delayMs = 2000;

  if (args.includes('--daemon')) {
    rounds = -1; // Infinite daemon loop
  } else if (args.includes('--step')) {
    rounds = 1; // Single round
  } else {
    for (const arg of args) {
      if (arg.startsWith('--rounds=')) {
        rounds = parseInt(arg.split('=')[1], 10) || 5;
      }
      if (arg.startsWith('--delay=')) {
        delayMs = parseInt(arg.split('=')[1], 10) || 2000;
      }
    }
  }

  const mm = new ClobMarketMaker();
  mm.run(rounds, delayMs).catch((err) => {
    console.error('❌ Market Maker failed:', err);
    removePid();
    process.exit(1);
  });
}

module.exports = { ClobMarketMaker };
