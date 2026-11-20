const { Account, NetworkType } = require('/Users/igorgoc/Projects/web-wallet-exchange/node_modules/tsjs-xpx-chain-sdk');

const CLOB_API_URL = 'https://sirius-clob-backend.onrender.com';
const NETWORK_TYPE = 168;

function signOrder(account, pair, side, type, priceStr, amountStr, nonce, expiry) {
  const normPair = pair.toUpperCase().replace('_', '/');
  const normSide = side === 'sell' || side === 1 ? 'sell' : 'buy';
  const normType = type === 'Market' || type === 1 ? 'Market' : 'Limit';
  const orderMsg = `${NETWORK_TYPE}:SiriusCLOB-v1:ORDER:${normPair}:${normSide}:${normType}:${priceStr}:${amountStr}:${nonce}:${expiry}`;
  return account.signData(orderMsg);
}

function signCancel(account, pair, orderId, nonce, expiry) {
  const normPair = pair.toUpperCase().replace('_', '/');
  const cancelMsg = `${NETWORK_TYPE}:SiriusCLOB-v1:CANCEL:${normPair}:${orderId}:${nonce}:${expiry}`;
  return account.signData(cancelMsg);
}

async function run() {
  console.log("=== Benchmarking Order Execution Latency on Render Cloud ===");
  const bobPriv = "15687BE4BB90C54728B78BAFA47CEC09959812B3A521D1D98033563185F6B000";
  const bob = Account.createFromPrivateKey(bobPriv, NetworkType.TEST_NET, 1);

  // 1. Warm-up / balance check
  const tBal0 = Date.now();
  const balRes = await fetch(`${CLOB_API_URL}/api/clob/balance?publicKey=${bob.publicKey}`);
  const balData = await balRes.json();
  const balMs = Date.now() - tBal0;
  console.log(`[Balance Query] took ${balMs}ms | USDT: ${balData.balances?.usdt || 0}`);

  // 2. Measure Limit Order Placement
  const now = Date.now();
  const payload = {
    pair: 'XPX/USDT',
    side: 'buy',
    type: 'Limit',
    price: '0.005',
    amount: '10',
    publicKey: bob.publicKey,
    nonce: String(now),
    expiry: now + 3600000,
    allowWideCollar: true
  };
  payload.signature = signOrder(bob, payload.pair, payload.side, payload.type, payload.price, payload.amount, payload.nonce, payload.expiry);

  console.log("Submitting Limit Order...");
  const tOrder0 = Date.now();
  const res = await fetch(`${CLOB_API_URL}/api/clob/order`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const orderLatencyMs = Date.now() - tOrder0;
  const result = await res.json();
  console.log(`[Limit Order] Result in ${orderLatencyMs}ms:`, {
    ok: result.ok,
    orderId: result.orderId,
    status: result.status,
    gasConsumed: result.gasConsumed
  });

  // 3. Measure Order Cancellation
  if (result.success && result.orderId) {
    const cancelNow = Date.now();
    const cancelPayload = {
      pair: 'XPX/USDT',
      orderId: result.orderId,
      publicKey: bob.publicKey,
      nonce: String(cancelNow),
      expiry: cancelNow + 3600000
    };
    cancelPayload.signature = signCancel(bob, cancelPayload.pair, cancelPayload.orderId, cancelPayload.nonce, cancelPayload.expiry);
    
    console.log("Cancelling Order...");
    const tCancel0 = Date.now();
    const cancelRes = await fetch(`${CLOB_API_URL}/api/clob/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cancelPayload)
    });
    const cancelLatencyMs = Date.now() - tCancel0;
    const cancelResult = await cancelRes.json();
    console.log(`[Cancel Order] Result in ${cancelLatencyMs}ms:`, {
      ok: cancelResult.ok,
      orderId: cancelResult.orderId
    });
  }
}

run().catch(console.error);
