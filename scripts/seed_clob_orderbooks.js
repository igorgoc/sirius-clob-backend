/**
 * SiriusCLOB — Multi-Pair Order Book Seeder
 * Populates deep, realistic 8-level Bid and Ask ladders for both XPX/USDT and XPX/BNB.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const {
  executeOnNodeVm,
  assert3NodeConsensus,
  NODES,
  DRIVE_KEY_HEX,
  ALICE_KEY_HEX,
  BOB_KEY_HEX,
} = require('../executor/milestone2_verifier');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const CLOB_WASM_PATH = path.join(
  ROOT_DIR,
  'contracts/clob/target/wasm32-unknown-unknown/release/sirius_clob.wasm'
);

const XPX_MOSAIC_ID = BigInt('0x0DC67FBE1CAD29E3').toString(10);
const USDT_MOSAIC_ID = BigInt('0x10112CBA1E7B9037').toString(10);
const BNB_MOSAIC_ID = BigInt('0x1E5C5FADEC7B34DD').toString(10);
const PRICE_SCALE = 100_000_000n;

const BUYER_KEY = ALICE_KEY_HEX;
const SELLER_KEY = BOB_KEY_HEX;
const TAKER_KEY = '5555555555555555555555555555555555555555555555555555555555555555';

function calculateQuoteAmount(baseAmount, priceTicks) {
  return Number((BigInt(baseAmount) * BigInt(priceTicks)) / PRICE_SCALE);
}

function encodePlaceOrder(side, orderType, priceTicks, baseAmount, quoteMosaicId = 0n) {
  if (!quoteMosaicId || quoteMosaicId === 0n) {
    const buf = Buffer.alloc(19);
    buf.writeUInt8(1, 0); // OP_PLACE_ORDER
    buf.writeUInt8(side, 1);
    buf.writeUInt8(orderType, 2);
    buf.writeBigUInt64LE(BigInt(priceTicks), 3);
    buf.writeBigUInt64LE(BigInt(baseAmount), 11);
    return buf;
  }
  const buf = Buffer.alloc(27);
  buf.writeUInt8(1, 0); // OP_PLACE_ORDER
  buf.writeUInt8(side, 1);
  buf.writeUInt8(orderType, 2);
  buf.writeBigUInt64LE(BigInt(priceTicks), 3);
  buf.writeBigUInt64LE(BigInt(baseAmount), 11);
  buf.writeBigUInt64LE(BigInt(quoteMosaicId), 19);
  return buf;
}

function encodeGetDepth(maxLevels = 50, quoteMosaicId = 0n) {
  if (!quoteMosaicId || quoteMosaicId === 0n) {
    const buf = Buffer.alloc(9);
    buf.writeUInt8(3, 0); // OP_GET_DEPTH
    buf.writeBigUInt64LE(BigInt(maxLevels), 1);
    return buf;
  }
  const buf = Buffer.alloc(17);
  buf.writeUInt8(3, 0);
  buf.writeBigUInt64LE(BigInt(maxLevels), 1);
  buf.writeBigUInt64LE(BigInt(quoteMosaicId), 9);
  return buf;
}

function getLatestCommittedHeight(nodeName = 'node1') {
  const indexDat = path.join(CLUSTER_DIR, nodeName, 'data', 'index.dat');
  if (fs.existsSync(indexDat)) {
    try {
      const buf = fs.readFileSync(indexDat);
      if (buf.length >= 8) {
        return Number(buf.readBigUInt64LE(0));
      }
    } catch (_) {}
  }
  return 100;
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

async function seedOrderBooks() {
  console.log('\n================================================================================');
  console.log('🌊 SiriusCLOB — Multi-Pair Order Book Seeder');
  console.log('   Populating XPX/USDT and XPX/BNB with Deep Liquidity Ladders');
  console.log('================================================================================\n');

  // 1. Deploy WASM to all drives
  const nodeDrivePaths = {};
  const nodeWasmPaths = {};
  for (const node of NODES) {
    const driveRoot = path.join(CLUSTER_DIR, node.name, 'data', 'drives', DRIVE_KEY_HEX);
    const driveDir = path.join(driveRoot, 'drive');
    fs.mkdirSync(driveDir, { recursive: true });

    const destWasm = path.join(driveDir, 'sirius_clob.wasm');
    fs.copyFileSync(CLOB_WASM_PATH, destWasm);
    nodeDrivePaths[node.name] = driveRoot;
    nodeWasmPaths[node.name] = destWasm;
  }
  console.log('📦 Synchronized sirius_clob.wasm to all 3 node Replicator Drives.');

  let txCounter = 2000;
  async function callCluster(callerKeyHex, params, payments, label) {
    const height = getLatestCommittedHeight('node1');
    const txHash = crypto.randomBytes(32).toString('hex');
    txCounter++;

    const results = await Promise.all(
      NODES.map((n) =>
        executeOnNodeVm(
          n,
          nodeWasmPaths[n.name],
          'run',
          height,
          txHash,
          txCounter,
          nodeDrivePaths[n.name],
          {
            parameters: params,
            callerKeyHex,
            servicePayments: payments,
          }
        )
      )
    );

    assert3NodeConsensus(label, results);
    return results[0].returns;
  }

  // 2. Clear stale market states to ensure crisp, clean fresh books
  for (const node of NODES) {
    for (const m of ['xpx_usdt', 'xpx_bnb']) {
      const mDir = path.join(CLUSTER_DIR, node.name, 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', m);
      fs.mkdirSync(mDir, { recursive: true });
      try { fs.unlinkSync(path.join(mDir, 'market_state.bin')); } catch (_) {}
      try { fs.unlinkSync(path.join(mDir, 'depth.json')); } catch (_) {}
    }
  }

  // ---------------------------------------------------------------------------
  // A. Seed XPX / USDT ($0.005000 Mid Price)
  // ---------------------------------------------------------------------------
  console.log('\n🟢 [PAIR 1: XPX/USDT] Seeding 8-Level Bid & Ask Ladder around $0.005000...');
  const usdtMidPrice = 500_000; // $0.005000
  const usdtSpreadTicks = 2_000; // $0.000020
  const usdtTickStep = 1_000; // $0.000010 per level
  const usdtLotBase = 25_000_000; // 25 XPX step

  // Bids (8 levels)
  for (let i = 1; i <= 8; i++) {
    const price = usdtMidPrice - (usdtSpreadTicks / 2) - ((i - 1) * usdtTickStep);
    const amount = usdtLotBase * i;
    const quoteEscrow = calculateQuoteAmount(amount, price);

    const params = encodePlaceOrder(0, 0, price, amount, BigInt(USDT_MOSAIC_ID));
    const payments = [{ mosaic_id: USDT_MOSAIC_ID, amount: String(quoteEscrow) }];
    await callCluster(BUYER_KEY, params, payments, `XPX/USDT Seed Bid ${i}`);
    console.log(`   + Bid #${i}: ${(amount / 1e6).toFixed(0).padStart(4)} XPX @ $${(price / 1e8).toFixed(6)} (Escrow: $${(quoteEscrow / 1e6).toFixed(4)} USDT)`);
  }

  // Asks (8 levels)
  for (let i = 1; i <= 8; i++) {
    const price = usdtMidPrice + (usdtSpreadTicks / 2) + ((i - 1) * usdtTickStep);
    const amount = usdtLotBase * i;
    const baseEscrow = amount;

    const params = encodePlaceOrder(1, 0, price, amount, BigInt(USDT_MOSAIC_ID));
    const payments = [{ mosaic_id: XPX_MOSAIC_ID, amount: String(baseEscrow) }];
    await callCluster(SELLER_KEY, params, payments, `XPX/USDT Seed Ask ${i}`);
    console.log(`   + Ask #${i}: ${(amount / 1e6).toFixed(0).padStart(4)} XPX @ $${(price / 1e8).toFixed(6)} (Escrow: ${(baseEscrow / 1e6).toFixed(0)} XPX)`);
  }

  // Execute 2 sample crossing fills on XPX/USDT to create live trade history
  console.log('   ⚡ Executing sample trade fills on XPX/USDT...');
  // Taker buy 15 XPX @ best ask ($0.005010)
  const takerBuyParams = encodePlaceOrder(0, 0, 501_000, 15_000_000, BigInt(USDT_MOSAIC_ID));
  const takerBuyEscrow = calculateQuoteAmount(15_000_000, 501_000);
  await callCluster(TAKER_KEY, takerBuyParams, [{ mosaic_id: USDT_MOSAIC_ID, amount: String(takerBuyEscrow) }], 'Taker Buy 15 XPX');

  // Taker sell 10 XPX @ best bid ($0.004990)
  const takerSellParams = encodePlaceOrder(1, 0, 499_000, 10_000_000, BigInt(USDT_MOSAIC_ID));
  await callCluster(TAKER_KEY, takerSellParams, [{ mosaic_id: XPX_MOSAIC_ID, amount: '10000000' }], 'Taker Sell 10 XPX');

  // Re-quote top of book to keep ladder full
  await callCluster(BUYER_KEY, encodePlaceOrder(0, 0, 499_000, 25_000_000, BigInt(USDT_MOSAIC_ID)), [{ mosaic_id: USDT_MOSAIC_ID, amount: String(calculateQuoteAmount(25_000_000, 499_000)) }], 'Top Bid Replenish');
  await callCluster(SELLER_KEY, encodePlaceOrder(1, 0, 501_000, 25_000_000, BigInt(USDT_MOSAIC_ID)), [{ mosaic_id: XPX_MOSAIC_ID, amount: '25000000' }], 'Top Ask Replenish');

  // ---------------------------------------------------------------------------
  // B. Seed XPX / BNB (0.00000833 BNB Mid Price = 833 ticks)
  // ---------------------------------------------------------------------------
  console.log('\n🟡 [PAIR 2: XPX/BNB] Seeding 6-Level Bid & Ask Ladder around 0.00000833 BNB...');
  const bnbMidPrice = 833; // 833 ticks = 0.00000833 BNB
  const bnbSpreadTicks = 4; // 4 ticks
  const bnbTickStep = 2; // 2 ticks per level
  const bnbLotBase = 50_000_000; // 50 XPX step

  // Bids (6 levels)
  for (let i = 1; i <= 6; i++) {
    const price = bnbMidPrice - (bnbSpreadTicks / 2) - ((i - 1) * bnbTickStep);
    const amount = bnbLotBase * i;
    const quoteEscrow = calculateQuoteAmount(amount, price);

    const params = encodePlaceOrder(0, 0, price, amount, BigInt(BNB_MOSAIC_ID));
    const payments = [{ mosaic_id: BNB_MOSAIC_ID, amount: String(quoteEscrow) }];
    await callCluster(BUYER_KEY, params, payments, `XPX/BNB Seed Bid ${i}`);
    console.log(`   + Bid #${i}: ${(amount / 1e6).toFixed(0).padStart(4)} XPX @ ${(price / 1e8).toFixed(8)} BNB (Escrow: ${(quoteEscrow / 1e6).toFixed(4)} BNB)`);
  }

  // Asks (6 levels)
  for (let i = 1; i <= 6; i++) {
    const price = bnbMidPrice + (bnbSpreadTicks / 2) + ((i - 1) * bnbTickStep);
    const amount = bnbLotBase * i;
    const baseEscrow = amount;

    const params = encodePlaceOrder(1, 0, price, amount, BigInt(BNB_MOSAIC_ID));
    const payments = [{ mosaic_id: XPX_MOSAIC_ID, amount: String(baseEscrow) }];
    await callCluster(SELLER_KEY, params, payments, `XPX/BNB Seed Ask ${i}`);
    console.log(`   + Ask #${i}: ${(amount / 1e6).toFixed(0).padStart(4)} XPX @ ${(price / 1e8).toFixed(8)} BNB (Escrow: ${(baseEscrow / 1e6).toFixed(0)} XPX)`);
  }

  // 3. Trigger Global Depth Snapshots (Writes depth.json for both USDT and BNB)
  console.log('\n📸 Triggering Replicator Drive depth snapshots for both pairs...');
  await callCluster(TAKER_KEY, encodeGetDepth(50, 0n), [], 'Global Depth Snapshot Export');

  // Replicate markets directory from node1 to node2 and node3
  const srcMarkets = path.join(nodeDrivePaths['node1'], 'drive', 'markets');
  for (const targetNode of ['node2', 'node3']) {
    const dstMarkets = path.join(nodeDrivePaths[targetNode], 'drive', 'markets');
    copyDirSync(srcMarkets, dstMarkets);
  }

  console.log('✅ Order books successfully populated and replicated across all 3 nodes!\n');
}

if (require.main === module) {
  seedOrderBooks().catch((err) => {
    console.error('❌ Seeder failed:', err);
    process.exit(1);
  });
}

module.exports = { seedOrderBooks };
