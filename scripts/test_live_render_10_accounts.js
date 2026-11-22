/**
 * Sirius CLOB 10-Account Live Trading Drill
 * Runs directly against live production services:
 * - Live Relayer API: https://sirius-clob-backend.onrender.com
 * - Live Catapult RPC: https://api-2.testnet2.xpxsirius.io
 * - Sirius Testnet 2 (Network 168)
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const https = require('https');

const BACKEND_DIR = path.resolve(__dirname, '..');
const clobApi = require(path.join(BACKEND_DIR, 'dashboard', 'clob_api.js'));
const accountsPath = path.join(BACKEND_DIR, 'cluster', 'test_30_accounts.json');

const LIVE_API_BASE = 'https://sirius-clob-backend.onrender.com';
const L1_RPC_BASE = 'https://api-2.testnet2.xpxsirius.io';

const testAccounts = JSON.parse(fs.readFileSync(accountsPath, 'utf8')).slice(0, 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data) });
        } catch (e) {
          resolve({ status: res.statusCode, raw: data });
        }
      });
    }).on('error', reject);
  });
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

async function fetchL1Account(address) {
  const res = await httpsGet(`${L1_RPC_BASE}/account/${address}`);
  if (!res.data || !res.data.account) {
    return { address, mosaics: [], xpx: 0n, usdt: 0n, bnb: 0n };
  }
  let xpx = 0n;
  let usdt = 0n;
  let bnb = 0n;
  for (const m of res.data.account.mosaics) {
    const low = BigInt(m.id[0] >>> 0);
    const high = BigInt(m.id[1] >>> 0);
    const u64 = (high << 32n) | low;
    const amtLow = BigInt(m.amount[0] >>> 0);
    const amtHigh = BigInt(m.amount[1] >>> 0);
    const amt = (amtHigh << 32n) | amtLow;
    const idHex = u64.toString(16).toUpperCase().padStart(16, '0');

    if (idHex === '13BFC518E40549D7' || idHex === '13BF761E0DC5F437') xpx = amt;
    else if (idHex === '4430AEFEF7E15D0C' || idHex === '10103E5D4B2A8C0C') usdt = amt;
    else if (idHex === '3CC1D3CE7447B6F9' || idHex === '0DFDC9D17448B699') bnb = amt;
  }
  return { address, xpx, usdt, bnb };
}

async function runLive10AccountDrill() {
  console.log('================================================================================');
  console.log('🚀 SIRIUS CLOB DEX: 10-ACCOUNT LIVE REAL MARKET DRILL');
  console.log('Target: ' + LIVE_API_BASE);
  console.log('Network: Sirius Testnet 2 (Catapult RPC: ' + L1_RPC_BASE + ')');
  console.log('================================================================================\n');

  // Step 1: Verify Live Solvency Sentinel
  console.log('[Step 1] Auditing Live Solvency Sentinel on Render...');
  const solvency = await httpsGet(`${LIVE_API_BASE}/api/clob/solvency/audit`);
  console.log(`  Live Audit Status: ${solvency.status}`);
  console.log(`  Healthy: ${solvency.data.audit.healthy}, CircuitBreaker: ${solvency.data.audit.circuitBreakerTripped}`);
  for (const [asset, info] of Object.entries(solvency.data.audit.assets)) {
    console.log(`  Asset [${asset}]: Reserve Ratio = ${info.reserveRatioPct}%, Solvent = ${info.isSolvent}, Surplus = ${info.surplusAtomic}`);
    assert.strictEqual(info.isSolvent, true, `Asset ${asset} must be 100% solvent`);
  }
  assert.strictEqual(solvency.data.audit.breaches.length, 0, 'Zero solvency breaches allowed');
  console.log('  ✅ Solvency Audit confirmed 100% solvent on live production backend.\n');

  // Step 2: Query Initial State for 10 Accounts
  console.log('[Step 2] Querying Initial State for 10 on-chain accounts...');
  const initialAccounts = [];
  for (let i = 0; i < 10; i++) {
    const acc = testAccounts[i];
    const l1 = await fetchL1Account(acc.address);
    const clobRes = await httpsGet(`${LIVE_API_BASE}/api/clob/balance?publicKey=${acc.publicKey}`);
    const clobBal = clobRes.data && clobRes.data.balances ? clobRes.data.balances : {};
    initialAccounts.push({ acc, l1, clobBal });
    console.log(
      `  Acc #${acc.index} (${i < 5 ? 'BUYER ' : 'SELLER'} | ${acc.address.slice(0, 10)}...): ` +
      `L1 XPX: ${(Number(l1.xpx) / 1e6).toFixed(4)} | ` +
      `L1 USDT: ${(Number(l1.usdt) / 1e6).toFixed(2)} | ` +
      `CLOB Avail XPX: ${(Number(clobBal.xpx || 0) / 1e6).toFixed(4)} | ` +
      `CLOB Avail USDT: ${(Number(clobBal.usdt || 0) / 1e6).toFixed(2)}`
    );
  }

  // Step 3: Accounts 1 to 5 execute Market Buys (1.000000 XPX each)
  console.log('\n[Step 3] Executing Market BUYS for Accounts 1 to 5 on Live Relayer...');
  const buyResults = [];
  for (let i = 0; i < 5; i++) {
    const acc = testAccounts[i];
    const nonce = Date.now().toString() + Math.floor(Math.random() * 1000);
    const orderReq = {
      pair: 'XPX/USDT',
      side: 'buy',
      type: 'Market',
      price: '0.00008500',
      amount: '1.000000',
      nonce,
      expiry: Date.now() + 180000,
      publicKey: acc.publicKey,
      allowWideCollar: true,
      maxSlippageBps: 2000,
    };
    orderReq.signature = clobApi.signOrderRequest(acc.privateKey, orderReq);

    const res = await httpsPost(`${LIVE_API_BASE}/api/clob/order`, orderReq);
    const success = res.status === 200 && res.data && res.data.ok;
    buyResults.push({ acc, success, res });

    if (success) {
      console.log(
        `  ✅ Acc #${acc.index} Buy Order #${res.data.orderId}: ` +
        `status=${res.data.status}, fills=${res.data.fillsCount}, ` +
        `remaining=${res.data.remainingAmount}, Avail XPX=${(Number(res.data.balances.xpx) / 1e6).toFixed(4)}, ` +
        `USDT=${(Number(res.data.balances.usdt) / 1e6).toFixed(2)}`
      );
    } else {
      console.error(`  ❌ Acc #${acc.index} Buy Order Failed: Status ${res.status}`, res.data || res.raw);
    }
    await sleep(350);
  }

  // Step 4: Accounts 6 to 10 execute Market Sells (1.000000 XPX each)
  console.log('\n[Step 4] Executing Market SELLS for Accounts 6 to 10 on Live Relayer...');
  const sellResults = [];
  for (let i = 5; i < 10; i++) {
    const acc = testAccounts[i];
    const nonce = Date.now().toString() + Math.floor(Math.random() * 1000);
    const orderReq = {
      pair: 'XPX/USDT',
      side: 'sell',
      type: 'Market',
      price: '0.00007500',
      amount: '1.000000',
      nonce,
      expiry: Date.now() + 180000,
      publicKey: acc.publicKey,
      allowWideCollar: true,
      maxSlippageBps: 2000,
    };
    orderReq.signature = clobApi.signOrderRequest(acc.privateKey, orderReq);

    const res = await httpsPost(`${LIVE_API_BASE}/api/clob/order`, orderReq);
    const success = res.status === 200 && res.data && res.data.ok;
    sellResults.push({ acc, success, res });

    if (success) {
      console.log(
        `  ✅ Acc #${acc.index} Sell Order #${res.data.orderId}: ` +
        `status=${res.data.status}, fills=${res.data.fillsCount}, ` +
        `remaining=${res.data.remainingAmount}, Avail XPX=${(Number(res.data.balances.xpx) / 1e6).toFixed(4)}, ` +
        `USDT=${(Number(res.data.balances.usdt) / 1e6).toFixed(2)}`
      );
    } else {
      console.error(`  ❌ Acc #${acc.index} Sell Order Failed: Status ${res.status}`, res.data || res.raw);
    }
    await sleep(350);
  }

  // Step 5: Post-Trade Reconciliation & Zero Ghost Balance Verification
  console.log('\n[Step 5] Post-Trade Account Reconciliation across all 10 Accounts...');
  let zeroWipes = 0;
  for (let i = 0; i < 10; i++) {
    const acc = testAccounts[i];
    const initial = initialAccounts[i];
    const postRes = await httpsGet(`${LIVE_API_BASE}/api/clob/balance?publicKey=${acc.publicKey}`);
    const postBal = postRes.data && postRes.data.balances ? postRes.data.balances : {};

    const initialXpx = BigInt(initial.clobBal.xpx || '0');
    const postXpx = BigInt(postBal.xpx || '0');

    // Rule: An account that started with funds must never be wiped to 0
    if (initialXpx > 0n && postXpx === 0n) {
      console.error(`  🚨 VIOLATION: Acc #${acc.index} balance was wiped to 0!`);
      zeroWipes++;
    }

    const initXpxFmt = (Number(initialXpx) / 1e6).toFixed(4);
    const postXpxFmt = (Number(postXpx) / 1e6).toFixed(4);
    const initUsdtFmt = (Number(initial.clobBal.usdt || '0') / 1e6).toFixed(2);
    const postUsdtFmt = (Number(postBal.usdt || '0') / 1e6).toFixed(2);

    console.log(
      `  Acc #${acc.index} (${i < 5 ? 'BUYER ' : 'SELLER'}): ` +
      `XPX: ${initXpxFmt} -> ${postXpxFmt} | USDT: ${initUsdtFmt} -> ${postUsdtFmt}`
    );
  }

  const buySuccessCount = buyResults.filter((r) => r.success).length;
  const sellSuccessCount = sellResults.filter((r) => r.success).length;

  console.log('\n================================================================================');
  console.log('📊 10-ACCOUNT DRILL RESULTS SUMMARY:');
  console.log(`  Live Market Buys Successful: ${buySuccessCount}/5`);
  console.log(`  Live Market Sells Successful: ${sellSuccessCount}/5`);
  console.log(`  Zero-Balance Wipeouts: ${zeroWipes}`);
  console.log('================================================================================\n');

  assert.strictEqual(zeroWipes, 0, 'Zero accounts may have their balances wiped out');
  assert.strictEqual(buySuccessCount, 5, 'All 5 market buys must succeed');
  assert.strictEqual(sellSuccessCount, 5, 'All 5 market sells must succeed');
}

runLive10AccountDrill()
  .then(() => {
    console.log('🎉 10-ACCOUNT LIVE DRILL COMPLETED WITH 100% SUCCESS ON PRODUCTION BACKEND!');
    process.exit(0);
  })
  .catch((err) => {
    console.error('💥 DRILL FAILED:', err);
    process.exit(1);
  });
