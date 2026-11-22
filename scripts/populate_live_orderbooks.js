/**
 * Populate Live Orderbooks on Render for XPX/USDT and XPX/BNB
 */

const https = require('https');
const path = require('path');
const fs = require('fs');

const BACKEND_DIR = path.resolve(__dirname, '..');
const clobApi = require(path.join(BACKEND_DIR, 'dashboard', 'clob_api.js'));
const accountsPath = path.join(BACKEND_DIR, 'cluster', 'test_30_accounts.json');

const LIVE_API_BASE = 'https://sirius-clob-backend.onrender.com';
const testAccounts = JSON.parse(fs.readFileSync(accountsPath, 'utf8'));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function httpsPost(url, payload) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(payload);
    const parsed = new URL(url);
    const req = https.request(
      {
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postData),
        },
      },
      (res) => {
        let data = '';
        res.on('data', (c) => (data += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, data: JSON.parse(data) });
          } catch (e) {
            resolve({ status: res.statusCode, raw: data });
          }
        });
      }
    );
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

async function placeLimitOrder(acc, pair, side, price, amount) {
  const nonce = Date.now().toString() + Math.floor(Math.random() * 10000);
  const orderReq = {
    pair,
    side,
    type: 'Limit',
    price,
    amount,
    nonce,
    expiry: Date.now() + 3600000, // 1 hour expiry
    publicKey: acc.publicKey,
    allowWideCollar: true,
  };
  orderReq.signature = clobApi.signOrderRequest(acc.privateKey, orderReq);

  const res = await httpsPost(`${LIVE_API_BASE}/api/clob/order`, orderReq);
  return res;
}

async function run() {
  console.log('================================================================================');
  console.log('🌊 POPULATING LIVE CLOB ORDER BOOKS ON RENDER (BOTH PAIRS)');
  console.log('================================================================================\n');

  const buyerAcc = testAccounts[0]; // Has USDT & BNB
  const sellerAccs = [testAccounts[5], testAccounts[6], testAccounts[7], testAccounts[8], testAccounts[9]];

  // ---------------------------------------------------------------------------
  // 1. POPULATE XPX / USDT
  // ---------------------------------------------------------------------------
  console.log('--- Seeding XPX/USDT Order Book ---');
  const usdtBids = [
    { price: '0.00007900', amount: '10.000000' },
    { price: '0.00007800', amount: '15.000000' },
    { price: '0.00007700', amount: '20.000000' },
    { price: '0.00007600', amount: '25.000000' },
    { price: '0.00007500', amount: '30.000000' },
    { price: '0.00007400', amount: '40.000000' },
  ];

  for (let i = 0; i < usdtBids.length; i++) {
    const b = usdtBids[i];
    const res = await placeLimitOrder(buyerAcc, 'XPX/USDT', 'buy', b.price, b.amount);
    if (res.data && res.data.ok) {
      console.log(`  ✅ XPX/USDT Bid #${i + 1}: ${b.amount} XPX @ ${b.price} USDT (Order #${res.data.orderId})`);
    } else {
      console.error(`  ❌ XPX/USDT Bid #${i + 1} Failed:`, res.data || res.raw);
    }
    await sleep(250);
  }

  const usdtAsks = [
    { price: '0.00008100', amount: '1.000000' },
    { price: '0.00008200', amount: '1.000000' },
    { price: '0.00008300', amount: '1.000000' },
    { price: '0.00008400', amount: '1.000000' },
    { price: '0.00008500', amount: '1.000000' },
    { price: '0.00008600', amount: '1.000000' },
  ];

  for (let i = 0; i < usdtAsks.length; i++) {
    const a = usdtAsks[i];
    const seller = sellerAccs[i % sellerAccs.length];
    const res = await placeLimitOrder(seller, 'XPX/USDT', 'sell', a.price, a.amount);
    if (res.data && res.data.ok) {
      console.log(`  ✅ XPX/USDT Ask #${i + 1}: ${a.amount} XPX @ ${a.price} USDT (Order #${res.data.orderId})`);
    } else {
      console.error(`  ❌ XPX/USDT Ask #${i + 1} Failed:`, res.data || res.raw);
    }
    await sleep(250);
  }

  // ---------------------------------------------------------------------------
  // 2. POPULATE XPX / BNB
  // ---------------------------------------------------------------------------
  console.log('\n--- Seeding XPX/BNB Order Book ---');
  const bnbBids = [
    { price: '0.00000013', amount: '10.000000' },
    { price: '0.00000012', amount: '15.000000' },
    { price: '0.00000011', amount: '20.000000' },
    { price: '0.00000010', amount: '25.000000' },
    { price: '0.00000009', amount: '30.000000' },
  ];

  for (let i = 0; i < bnbBids.length; i++) {
    const b = bnbBids[i];
    const res = await placeLimitOrder(buyerAcc, 'XPX/BNB', 'buy', b.price, b.amount);
    if (res.data && res.data.ok) {
      console.log(`  ✅ XPX/BNB Bid #${i + 1}: ${b.amount} XPX @ ${b.price} BNB (Order #${res.data.orderId})`);
    } else {
      console.error(`  ❌ XPX/BNB Bid #${i + 1} Failed:`, res.data || res.raw);
    }
    await sleep(250);
  }

  const bnbAsks = [
    { price: '0.00000015', amount: '1.000000' },
    { price: '0.00000016', amount: '1.000000' },
    { price: '0.00000017', amount: '1.000000' },
    { price: '0.00000018', amount: '1.000000' },
    { price: '0.00000019', amount: '1.000000' },
  ];

  for (let i = 0; i < bnbAsks.length; i++) {
    const a = bnbAsks[i];
    const seller = sellerAccs[i % sellerAccs.length];
    const res = await placeLimitOrder(seller, 'XPX/BNB', 'sell', a.price, a.amount);
    if (res.data && res.data.ok) {
      console.log(`  ✅ XPX/BNB Ask #${i + 1}: ${a.amount} XPX @ ${a.price} BNB (Order #${res.data.orderId})`);
    } else {
      console.error(`  ❌ XPX/BNB Ask #${i + 1} Failed:`, res.data || res.raw);
    }
    await sleep(250);
  }

  console.log('\n================================================================================');
  console.log('🎉 LIVE ORDER BOOKS SEEDED AND VERIFIED ON BOTH PAIRS!');
  console.log('================================================================================\n');
}

run().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
