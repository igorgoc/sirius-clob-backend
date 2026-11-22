/**
 * Verification Test: Unfilled IOC Market Order Escrow Refund & Zero Balance Loss
 * Tests:
 *  1. Universal mosaic matchers (isXpxMosaic, isUsdtMosaic, isBnbMosaic) across network & canonical IDs.
 *  2. Unfilled Market Sell: Verifies 100% of escrow is refunded, 0 XPX debited, 0 locked.
 *  3. Unfilled Market Buy: Verifies 100% of quote escrow is refunded, 0 USDT debited, 0 locked.
 *  4. Fallback reconciliation: Even if emittedTx has no refund tx, unspent escrow is unconditionally refunded.
 */

const assert = require('assert');
const crypto = require('crypto');
const { Account } = require('tsjs-xpx-chain-sdk');
const clobApi = require('../dashboard/clob_api');

async function runTests() {
  console.log('================================================================');
  console.log('🧪 TESTING UNFILLED IOC REFUND & ESCROW RECONCILIATION');
  console.log('================================================================\n');

  // Test 1: Universal mosaic matchers
  console.log('Test 1: Testing universal mosaic matchers...');
  const testMosaics = {
    xpx: [
      '0dc67fbe1cad29e3',
      '13bfc518e40549d7',
      '402b2f579faebc59',
      'bffb42a19116bdf6',
      '992621222383397347',
      '1423067181077744087',
      BigInt('0x13BFC518E40549D7').toString(10),
      'xpx',
      'XPX'
    ],
    usdt: [
      '10112cba1e7b9037',
      '4430aefef7e15d0c',
      '7525381a171d1e44',
      '1157755757097881655',
      '4913615467364637964',
      BigInt('0x4430AEFEF7E15D0C').toString(10),
      'usdt',
      'USDT'
    ],
    bnb: [
      '1e5c5fadec7b34dd',
      '3cc1d3ce7447b6f9',
      '2187728719596696797',
      '4377983656976694009',
      BigInt('0x3CC1D3CE7447B6F9').toString(10),
      'bnb',
      'BNB'
    ]
  };

  for (const x of testMosaics.xpx) {
    assert.strictEqual(clobApi.isXpxMosaic(x), true, `Failed matching XPX mosaic: ${x}`);
    assert.strictEqual(clobApi.isUsdtMosaic(x), false, `False positive USDT for XPX mosaic: ${x}`);
  }
  for (const u of testMosaics.usdt) {
    assert.strictEqual(clobApi.isUsdtMosaic(u), true, `Failed matching USDT mosaic: ${u}`);
    assert.strictEqual(clobApi.isXpxMosaic(u), false, `False positive XPX for USDT mosaic: ${u}`);
  }
  for (const b of testMosaics.bnb) {
    assert.strictEqual(clobApi.isBnbMosaic(b), true, `Failed matching BNB mosaic: ${b}`);
    assert.strictEqual(clobApi.isXpxMosaic(b), false, `False positive XPX for BNB mosaic: ${b}`);
  }
  console.log('   ✅ All mosaic IDs and representations successfully matched without false positives.\n');

  // Test 2: Unfilled Market Sell refund simulation
  console.log('Test 2: Simulating Unfilled Market Sell of 1 XPX...');
  const privSell = crypto.randomBytes(32).toString('hex');
  const accSell = Account.createFromPrivateKey(privSell, 168, 1);
  const testKeySell = accSell.publicKey.toLowerCase();
  
  // Set initial balance
  clobApi.updateAccountBalance(testKeySell, (b) => {
    b.xpx = 10000000n; // 10 XPX
    b.usdt = 50000000n; // 50 USDT
    b.lockedXpx = 0n;
    b.lockedUsdt = 0n;
    b.hasTraded = false;
  }, { source: 'VERIFIED_DEPOSIT' });

  const balBeforeSell = clobApi.getAccountBalance(testKeySell);
  console.log('   Balance before order:', balBeforeSell);

  // Setup custom executor to simulate SuperContract returning an unfilled IOC order with Escrow Refund
  const mockRefundTx = {
    transactions: [
      {
        payload: (() => {
          const buf = Buffer.alloc(32 + 8 + 8 + 16 + 30);
          buf.fill(0);
          Buffer.from(testKeySell, 'hex').copy(buf, 0); // recipient
          buf.writeBigUInt64LE(1n, 32); // mosaic count
          const msg = Buffer.from('SiriusCLOB: Base Escrow Refund', 'utf8');
          buf.writeBigUInt64LE(BigInt(msg.length), 40); // msg_size
          buf.writeBigUInt64LE(BigInt('0x13BFC518E40549D7'), 48); // Testnet 2 XPX mosaic ID!
          buf.writeBigUInt64LE(1000000n, 56); // 1 XPX refund amount
          msg.copy(buf, 64);
          return buf;
        })()
      }
    ]
  };

  clobApi.setCustomConsensusExecutor(async () => {
    return [{ returns: { return_val: 0, execution_gas_consumed: 1500, transaction: mockRefundTx } }];
  });

  const now = Date.now();
  const nonceSell = now + 1;
  const expirySell = now + 3600000;
  const signMsgSell = clobApi.constructOrderMessage('XPX/USDT', 1, 1, '0.00008500', '1.000000', nonceSell, expirySell);
  const sigSell = accSell.signData(signMsgSell);

  const sellResult = await clobApi._placeOrderUnlocked({
    pair: 'XPX/USDT',
    side: 'Sell',
    type: 'Market',
    price: '0.00008500',
    amount: '1.000000',
    publicKey: testKeySell,
    nonce: nonceSell,
    expiry: expirySell,
    signature: sigSell,
    allowWideCollar: true,
  });

  console.log('   Order execution status:', sellResult.status, 'Fills count:', sellResult.fillsCount);
  const balAfterSell = clobApi.getAccountBalance(testKeySell);
  console.log('   Balance after unfilled order:', balAfterSell);

  assert.strictEqual(BigInt(balAfterSell.xpx), BigInt(balBeforeSell.xpx), 'CRITICAL ERROR: XPX balance was debited on unfilled order!');
  assert.strictEqual(BigInt(balAfterSell.lockedXpx), 0n, 'CRITICAL ERROR: Locked XPX remained non-zero on unfilled IOC order!');
  console.log('   ✅ Unfilled Market Sell properly refunded 100% of escrow: 0 drift, 0 locked!\n');

  // Test 3: Unfilled Market Buy refund simulation
  console.log('Test 3: Simulating Unfilled Market Buy of XPX with USDT...');
  const privBuy = crypto.randomBytes(32).toString('hex');
  const accBuy = Account.createFromPrivateKey(privBuy, 168, 1);
  const testKeyBuy = accBuy.publicKey.toLowerCase();
  
  clobApi.updateAccountBalance(testKeyBuy, (b) => {
    b.xpx = 10000000n;
    b.usdt = 50000000n;
    b.lockedXpx = 0n;
    b.lockedUsdt = 0n;
    b.hasTraded = false;
  }, { source: 'VERIFIED_DEPOSIT' });

  const balBeforeBuy = clobApi.getAccountBalance(testKeyBuy);

  const mockQuoteRefundTx = {
    transactions: [
      {
        payload: (() => {
          const buf = Buffer.alloc(32 + 8 + 8 + 16 + 31);
          buf.fill(0);
          Buffer.from(testKeyBuy, 'hex').copy(buf, 0);
          buf.writeBigUInt64LE(1n, 32);
          const msg = Buffer.from('SiriusCLOB: Quote Escrow Refund', 'utf8');
          buf.writeBigUInt64LE(BigInt(msg.length), 40);
          buf.writeBigUInt64LE(BigInt('0x4430AEFEF7E15D0C'), 48); // Testnet 2 USDT mosaic ID!
          buf.writeBigUInt64LE(82n, 56); // 82 quote units locked by slippage ceiling
          msg.copy(buf, 64);
          return buf;
        })()
      }
    ]
  };

  clobApi.setCustomConsensusExecutor(async () => {
    return [{ returns: { return_val: 0, execution_gas_consumed: 1500, transaction: mockQuoteRefundTx } }];
  });

  const nonceBuy = now + 2;
  const expiryBuy = now + 3600000;
  const signMsgBuy = clobApi.constructOrderMessage('XPX/USDT', 0, 1, '0.00008500', '1.000000', nonceBuy, expiryBuy);
  const sigBuy = accBuy.signData(signMsgBuy);

  const buyResult = await clobApi._placeOrderUnlocked({
    pair: 'XPX/USDT',
    side: 'Buy',
    type: 'Market',
    price: '0.00008500',
    amount: '1.000000',
    publicKey: testKeyBuy,
    nonce: nonceBuy,
    expiry: expiryBuy,
    signature: sigBuy,
    allowWideCollar: true,
  });

  console.log('   Order execution status:', buyResult.status, 'Fills count:', buyResult.fillsCount);
  const balAfterBuy = clobApi.getAccountBalance(testKeyBuy);
  console.log('   Balance after unfilled order:', balAfterBuy);

  assert.strictEqual(BigInt(balAfterBuy.usdt), BigInt(balBeforeBuy.usdt), 'CRITICAL ERROR: USDT balance was debited on unfilled order!');
  assert.strictEqual(BigInt(balAfterBuy.lockedUsdt), 0n, 'CRITICAL ERROR: Locked USDT remained non-zero on unfilled IOC order!');
  console.log('   ✅ Unfilled Market Buy properly refunded 100% of quote escrow: 0 drift, 0 locked!\n');

  // Test 4: Unfilled Market order with NO emitted transactions (Fallback safety reconciliation)
  console.log('Test 4: Simulating Unfilled Market Sell with NO emitted transactions from VM...');
  const privFallback = crypto.randomBytes(32).toString('hex');
  const accFallback = Account.createFromPrivateKey(privFallback, 168, 1);
  const testKeyFallback = accFallback.publicKey.toLowerCase();
  
  clobApi.updateAccountBalance(testKeyFallback, (b) => {
    b.xpx = 5000000n;
    b.usdt = 10000000n;
    b.lockedXpx = 0n;
    b.lockedUsdt = 0n;
    b.hasTraded = false;
  }, { source: 'VERIFIED_DEPOSIT' });

  const balBeforeFallback = clobApi.getAccountBalance(testKeyFallback);

  clobApi.setCustomConsensusExecutor(async () => {
    return [{ returns: { return_val: 0, execution_gas_consumed: 1200, transaction: { transactions: [] } } }];
  });

  const nonceFallback = now + 3;
  const expiryFallback = now + 3600000;
  const signMsgFallback = clobApi.constructOrderMessage('XPX/USDT', 1, 1, '0.00008500', '1.000000', nonceFallback, expiryFallback);
  const sigFallback = accFallback.signData(signMsgFallback);

  const fallbackResult = await clobApi._placeOrderUnlocked({
    pair: 'XPX/USDT',
    side: 'Sell',
    type: 'Market',
    price: '0.00008500',
    amount: '1.000000',
    publicKey: testKeyFallback,
    nonce: nonceFallback,
    expiry: expiryFallback,
    signature: sigFallback,
    allowWideCollar: true,
  });

  const balAfterFallback = clobApi.getAccountBalance(testKeyFallback);
  assert.strictEqual(BigInt(balAfterFallback.xpx), BigInt(balBeforeFallback.xpx), 'Fallback failed to restore XPX balance!');
  assert.strictEqual(BigInt(balAfterFallback.lockedXpx), 0n, 'Fallback failed to clear lockedXpx!');
  console.log('   ✅ Safety fallback unconditionally restored locked escrow even with 0 emitted txs!\n');

  // Reset custom executor
  clobApi.setCustomConsensusExecutor(null);

  console.log('================================================================');
  console.log('🎉 ALL REFUND & RECONCILIATION TESTS PASSED PERFECTLY!');
  console.log('================================================================');
}

runTests().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
