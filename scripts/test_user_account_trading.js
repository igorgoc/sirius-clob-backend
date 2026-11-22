/**
 * Test User Account Trading & Live Verification
 * Account: VAYGA4RVFRXFW4G6IYYEX7A7ZFQKQF5525FEQHY5
 * Private Key: E8A4B4CED34D6BAA397091A84376451D27D5030A63E95EDBC5139C4055B5D505
 */
const https = require('https');
const sdk = require('/Users/igorgoc/Projects/web-wallet-vuejs/node_modules/tsjs-xpx-chain-sdk');
const config = require('../dashboard/testnet2_config');
const client = require('../dashboard/testnet2_client');

const LIVE_API_BASE = 'https://sirius-clob-backend.onrender.com';
const USER_PRIV_KEY = 'E8A4B4CED34D6BAA397091A84376451D27D5030A63E95EDBC5139C4055B5D505';

const NETWORK_ID = Number(config.NETWORK_TYPE || 168);
const DOMAIN_SEPARATOR = `${NETWORK_ID}:SiriusCLOB-v1`;

function canonicalizeDecimalString(val, maxDecimals = 8) {
  if (val === undefined || val === null) return '0';
  if (typeof val === 'string') return val.trim();
  if (typeof val === 'number' && Number.isFinite(val)) {
    const raw = String(val);
    if (raw.includes('e') || raw.includes('E')) {
      return val.toFixed(maxDecimals).replace(/\.?0+$/, '') || '0';
    }
    return raw;
  }
  return String(val);
}

function constructOrderMessage(pair, side, type, price, amount, nonce, expiry) {
  const normPair = (pair || 'XPX/USDT').toUpperCase().replace('_', '/');
  const normSide = side === 1 || String(side).toLowerCase() === 'sell' ? 'sell' : 'buy';
  const normType = type === 1 || String(type).toLowerCase() === 'market' ? 'Market' : 'Limit';
  const priceStr = canonicalizeDecimalString(price, 8);
  const amountStr = canonicalizeDecimalString(amount, 6);
  return `${DOMAIN_SEPARATOR}:ORDER:${normPair}:${normSide}:${normType}:${priceStr}:${amountStr}:${nonce}:${expiry}`;
}

function signOrder(privKey, order) {
  const account = sdk.Account.createFromPrivateKey(privKey, NETWORK_ID, 1);
  const msgStr = constructOrderMessage(
    order.pair,
    order.side,
    order.type,
    order.price,
    order.amount,
    order.nonce,
    order.expiry
  );
  return account.signData(msgStr);
}

function httpsPost(urlStr, data) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(data);
    const parsed = new URL(urlStr);
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
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, data: JSON.parse(body) });
          } catch (e) {
            resolve({ status: res.statusCode, raw: body });
          }
        });
      }
    );
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

function httpsGet(urlStr) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(urlStr);
    const req = https.request(
      {
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: parsed.pathname + parsed.search,
        method: 'GET',
      },
      (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => {
          try {
            resolve({ status: res.statusCode, data: JSON.parse(body) });
          } catch (e) {
            resolve({ status: res.statusCode, raw: body });
          }
        });
      }
    );
    req.on('error', reject);
    req.end();
  });
}

async function placeOrder(acc, pair, side, type, price, amount) {
  const nonce = Date.now().toString() + Math.floor(Math.random() * 10000);
  const orderReq = {
    pair,
    side,
    type,
    price,
    amount,
    nonce,
    expiry: Date.now() + 3600000,
    publicKey: acc.publicKey,
    allowWideCollar: true,
  };
  orderReq.signature = signOrder(USER_PRIV_KEY, orderReq);
  return httpsPost(`${LIVE_API_BASE}/api/clob/order`, orderReq);
}

async function run() {
  console.log('================================================================================');
  console.log('🧪 LIVE USER TRADING TEST: MARKET SELL & MARKET BUY');
  console.log('================================================================================\n');

  const userAcc = sdk.Account.createFromPrivateKey(USER_PRIV_KEY, config.NETWORK_TYPE, 1);
  const userAddr = userAcc.address.plain();
  const userPub = userAcc.publicKey;

  console.log(`👤 User Account:`);
  console.log(`   Address:    ${userAddr}`);
  console.log(`   Public Key: ${userPub}\n`);

  // 1. Initial Balances
  console.log('--- 1. Checking Initial Balances ---');
  const onChainBal = await client.getAccountOnChainBalances(userAddr, true);
  const xpxOnChain = Number(onChainBal.get('13bfc518e40549d7') || 0n) / 1e6;
  const usdtOnChain = Number(onChainBal.get('4430aefef7e15d0c') || 0n) / 1e6;
  const bnbOnChain = Number(onChainBal.get('3cc1d3ce7447b6f9') || 0n) / 1e6;
  console.log(`   Layer 1 On-Chain Balances:`);
  console.log(`     XPX:  ${xpxOnChain.toFixed(6)}`);
  console.log(`     USDT: ${usdtOnChain.toFixed(6)}`);
  console.log(`     BNB:  ${bnbOnChain.toFixed(6)}`);

  const initialClobRes = await httpsGet(`${LIVE_API_BASE}/api/clob/balance?publicKey=${userPub}`);
  console.log(`   CLOB Trading Balances:`, initialClobRes.data.balances, '\n');

  // 2. Check Order Book Depth
  console.log('--- 2. Checking Live Order Book Depth ---');
  let depthRes = await httpsGet(`${LIVE_API_BASE}/api/clob/depth?pair=XPX/USDT`);
  console.log(`   XPX/USDT Best Bid: ${depthRes.data?.best_bid}, Best Ask: ${depthRes.data?.best_ask}`);
  console.log(`   Bids: ${depthRes.data?.bids?.length || 0} levels, Asks: ${depthRes.data?.asks?.length || 0} levels`);

  // If orderbook is depleted, wait for market maker daemon
  while (!depthRes.data?.bids?.length || !depthRes.data?.asks?.length) {
    console.log('   ⚠️ Waiting for live market maker liquidity ladder...');
    await new Promise((r) => setTimeout(r, 4000));
    depthRes = await httpsGet(`${LIVE_API_BASE}/api/clob/depth?pair=XPX/USDT`);
  }

  // 3. Execute Market Sell 1 XPX
  console.log('\n--- 3. Executing MARKET SELL: 1 XPX for USDT ---');
  const bestBidPrice = depthRes.data?.best_bid ? (depthRes.data.best_bid / 1e8).toFixed(8) : '0.00007900';
  console.log(`   Sending Market Sell: 1.000000 XPX (targeting bid price ~${bestBidPrice})...`);
  const sellRes = await placeOrder(userAcc, 'XPX/USDT', 'sell', 'Market', bestBidPrice, '1.000000');
  console.log(`   Sell Order Result (HTTP ${sellRes.status}):`, JSON.stringify(sellRes.data, null, 2));

  // 4. Wait for Settlement & Check Balances
  console.log('\n--- 4. Post-Sell Balances & Settlement Verification ---');
  await new Promise((r) => setTimeout(r, 4000));
  const postSellClob = await httpsGet(`${LIVE_API_BASE}/api/clob/balance?publicKey=${userPub}`);
  console.log(`   CLOB Balances after Sell:`, postSellClob.data.balances);

  // 5. Execute Market Buy 1 XPX
  console.log('\n--- 5. Executing MARKET BUY: 1 XPX with USDT ---');
  depthRes = await httpsGet(`${LIVE_API_BASE}/api/clob/depth?pair=XPX/USDT`);
  const bestAskPrice = depthRes.data?.best_ask ? (depthRes.data.best_ask / 1e8).toFixed(8) : '0.00008100';
  console.log(`   Sending Market Buy: 1.000000 XPX (targeting ask price ~${bestAskPrice})...`);
  const buyRes = await placeOrder(userAcc, 'XPX/USDT', 'buy', 'Market', bestAskPrice, '1.000000');
  console.log(`   Buy Order Result (HTTP ${buyRes.status}):`, JSON.stringify(buyRes.data, null, 2));

  // 6. Final Balances
  console.log('\n--- 6. Final Balance Audit ---');
  await new Promise((r) => setTimeout(r, 4000));
  const finalClob = await httpsGet(`${LIVE_API_BASE}/api/clob/balance?publicKey=${userPub}`);
  console.log(`   Final CLOB Balances:`, finalClob.data.balances);

  const finalOnChain = await client.getAccountOnChainBalances(userAddr, true);
  console.log(`   Final L1 On-Chain Balances:`);
  console.log(`     XPX:  ${(Number(finalOnChain.get('13bfc518e40549d7') || 0n) / 1e6).toFixed(6)}`);
  console.log(`     USDT: ${(Number(finalOnChain.get('4430aefef7e15d0c') || 0n) / 1e6).toFixed(6)}`);
  console.log(`     BNB:  ${(Number(finalOnChain.get('3cc1d3ce7447b6f9') || 0n) / 1e6).toFixed(6)}`);

  console.log('\n================================================================================');
  console.log('✅ TRADING TEST COMPLETE');
  console.log('================================================================================');
}

run().catch((err) => {
  console.error('Fatal error during trading test:', err);
  process.exit(1);
});
