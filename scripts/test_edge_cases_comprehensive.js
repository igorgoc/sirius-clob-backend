#!/usr/bin/env node
/**
 * Comprehensive Isolated Edge-Case & Security Regression Test Suite
 *
 * Tests all critical findings discovered during the autonomous multi-wave audit:
 *   1. P2P Counterparty pending outflow deduction in computeTradingAccountBalances
 *   2. Concurrent settlement cosignature serialization (no lost updates)
 *   3. Expired pending_cosign settlement auto-rebuild (on return & cosign race)
 *   4. Validator fail-closed SETTLEMENT & WITHDRAWAL enforcement (including quoteSym fix)
 *   5. BFT assert3NodeConsensus quorum math & null guards
 *
 * Runs against an isolated temporary SQLite DB; NEVER mutates cluster/ or production DB.
 */

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const tempDbPath = path.join(os.tmpdir(), `clob_edge_cases_${process.pid}_${Date.now()}.db`);
process.env.CLOB_DB_FILE = tempDbPath;

const sdk = require('tsjs-xpx-chain-sdk');
const {
  Account,
  AggregateTransaction,
  TransferTransaction,
  Deadline,
  Mosaic,
  MosaicId,
  UInt64,
  PlainMessage,
  PublicAccount,
  CosignatureTransaction,
  KeyPair,
  DerivationScheme,
} = sdk;

const config = require('../dashboard/testnet2_config');
const clobDb = require('../dashboard/clob_db');
const cosigner = require('../dashboard/trading_account_cosigner');
const testnetClient = require('../dashboard/testnet2_client');
const { assert3NodeConsensus } = require('../executor/milestone2_verifier');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { startVmServer } = require('../executor/grpc_vm_server');

async function runTests() {
  const userA = Account.generateNewAccount(config.NETWORK_TYPE);
  const userB = Account.generateNewAccount(config.NETWORK_TYPE);
  const tradeA = Account.generateNewAccount(config.NETWORK_TYPE);
  const tradeB = Account.generateNewAccount(config.NETWORK_TYPE);
  const vault = testnetClient.getOrInitVaultAccount();

  clobDb.saveTradingAccount({
    tradingPublicKey: tradeA.publicKey,
    tradingAddress: tradeA.address.plain(),
    ownerPublicKey: userA.publicKey,
    ownerAddress: userA.address.plain(),
    validatorClusterPublicKey: vault.publicKey,
    validatorClusterAddress: vault.address.plain(),
    minApproval: 2,
    status: 'active',
  });
  clobDb.saveTradingAccount({
    tradingPublicKey: tradeB.publicKey,
    tradingAddress: tradeB.address.plain(),
    ownerPublicKey: userB.publicKey,
    ownerAddress: userB.address.plain(),
    validatorClusterPublicKey: vault.publicKey,
    validatorClusterAddress: vault.address.plain(),
    minApproval: 2,
    status: 'active',
  });

  // ============================================================================
  // TEST 1: Counterparty pending outflow deduction in computeTradingAccountBalances
  // ============================================================================
  const transfers = [
    {
      fromTradingPub: tradeA.publicKey,
      fromType: 'TRADING',
      ownerPublicKey: userA.publicKey.toLowerCase(),
      toAddress: tradeB.address.plain(),
      mosaicHex: config.XPX_MOSAIC_ID,
      amountAtomic: '2000000', // 2 XPX from A -> B
      symbol: 'XPX',
    },
    {
      fromTradingPub: tradeB.publicKey,
      fromType: 'TRADING',
      ownerPublicKey: userB.publicKey.toLowerCase(),
      toAddress: tradeA.address.plain(),
      mosaicHex: config.USDT_MOSAIC_ID,
      amountAtomic: '5000000', // 5 USDT from B -> A
      symbol: 'USDT',
    },
  ];

  const settlement = await cosigner.createTradeSettlement({
    orderId: 'ORD_EDGE_1',
    ownerPublicKey: userA.publicKey,
    counterpartyKey: userB.publicKey,
    transfers,
    delta: { xpx: '-2000000', usdt: '5000000', bnb: '0' },
  });

  // Stub on-chain balances: both accounts hold 10 XPX & 10 USDT on Sirius L1
  const origGetOnChain = testnetClient.getAccountOnChainBalances;
  testnetClient.getAccountOnChainBalances = async () => new Map([
    [config.XPX_MOSAIC_ID.toLowerCase(), 10_000_000n],
    [config.USDT_MOSAIC_ID.toLowerCase(), 10_000_000n],
    [config.BNB_MOSAIC_ID.toLowerCase(), 0n],
  ]);

  try {
    const recA = clobDb.getTradingAccountByOwner(userA.publicKey);
    const recB = clobDb.getTradingAccountByOwner(userB.publicKey);
    const balA = await cosigner.computeTradingAccountBalances(recA);
    const balB = await cosigner.computeTradingAccountBalances(recB);

    // User A owes 2 XPX (deducted immediately), receives 5 USDT (NOT credited until L1 confirm)
    assert.strictEqual(balA.withdrawable.xpx, '8000000', 'User A withdrawable XPX must deduct 2 XPX pending outflow');
    assert.strictEqual(balA.withdrawable.usdt, '10000000', 'User A withdrawable USDT must NOT include unconfirmed inflow');

    // User B (counterparty!) owes 5 USDT (MUST be deducted immediately!), receives 2 XPX (NOT credited until confirm)
    assert.strictEqual(balB.withdrawable.usdt, '5000000', 'Counterparty User B withdrawable USDT must deduct 5 USDT pending outflow');
    assert.strictEqual(balB.tradeable.usdt, '5000000', 'Counterparty User B tradeable USDT must deduct 5 USDT pending outflow');
    assert.strictEqual(balB.withdrawable.xpx, '10000000', 'Counterparty User B withdrawable XPX must NOT include unconfirmed inflow');
    console.log('✅ Test 1 passed: P2P counterparty pending outflows strictly deducted');
  } finally {
    testnetClient.getAccountOnChainBalances = origGetOnChain;
  }

  // ============================================================================
  // TEST 2: Concurrent cosignature serialization (no lost updates)
  // ============================================================================
  const cosigA = CosignatureTransaction.signTransactionPayload(userA, settlement.aggregatePayload, config.GENERATION_HASH);
  const cosigB = CosignatureTransaction.signTransactionPayload(userB, settlement.aggregatePayload, config.GENERATION_HASH);
  // Submit A twice concurrently -> idempotent, serialized
  const [r1, r2] = await Promise.all([
    cosigner.submitSettlementCosignature({
      settlementId: settlement.settlementId,
      cosignature: { parentHash: cosigA.parentHash, signer: cosigA.signer, signature: cosigA.signature },
    }),
    cosigner.submitSettlementCosignature({
      settlementId: settlement.settlementId,
      cosignature: { parentHash: cosigA.parentHash, signer: cosigA.signer, signature: cosigA.signature },
    }),
  ]);
  assert.strictEqual(r1.ok, true);
  assert.strictEqual(r2.ok, true);
  const afterA = clobDb.getTradingSettlement(settlement.settlementId);
  assert.strictEqual(afterA.collectedCosigs.length, 1, 'Duplicate concurrent cosign from same user must dedupe cleanly');
  console.log('✅ Test 2 passed: Concurrent settlement cosignature serialization');

  // ============================================================================
  // TEST 3: Expired pending_cosign auto-rebuild on counterparty return
  // ============================================================================
  const pastMs = Date.now() - 15 * 60 * 1000 - Deadline.timestampNemesisBlock * 1000;
  const pastDeadline = new Deadline(Deadline.create().value, UInt64.fromUint(pastMs));
  const expInner = TransferTransaction.create(
    pastDeadline,
    tradeB.address,
    [new Mosaic(new MosaicId(config.XPX_MOSAIC_ID), UInt64.fromUint(2000000))],
    PlainMessage.create('EXP'),
    config.NETWORK_TYPE
  ).toAggregate(PublicAccount.createFromPublicKey(tradeA.publicKey, config.NETWORK_TYPE));
  const expAgg = AggregateTransaction.createCompleteV1(pastDeadline, [expInner], config.NETWORK_TYPE, [], UInt64.fromUint(160000));
  const expSigned = vault.sign(expAgg, config.GENERATION_HASH);
  clobDb.updateTradingSettlementPayload(settlement.settlementId, {
    txHash: expSigned.hash,
    aggregatePayload: expSigned.payload,
    collectedCosigs: [{ parentHash: cosigA.parentHash, signer: cosigA.signer, signature: cosigA.signature }],
    status: 'pending_cosign',
  });

  const rebuiltList = cosigner.getPendingSettlementsWithAutoRebuild(userB.publicKey);
  assert.strictEqual(rebuiltList.length, 1);
  assert.strictEqual(rebuiltList[0].rebuilt, true);
  assert.notStrictEqual(rebuiltList[0].txHash, expSigned.hash);
  assert.strictEqual(rebuiltList[0].collectedCosigs.length, 0);
  assert.strictEqual(cosigner.isSettlementPayloadExpired(rebuiltList[0].aggregatePayload), false);
  console.log('✅ Test 3 passed: Expired pending_cosign auto-rebuild for offline counterparty');

  // ============================================================================
  // TEST 4: Validator gRPC fail-closed checks (live ephemeral server)
  // ============================================================================
  const testPort = 15991;
  const vmServer = await startVmServer(testPort, 'node1');
  const protoDef = protoLoader.loadSync(path.join(__dirname, '../executor/proto/supercontract_server.proto'), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const scProto = grpc.loadPackageDefinition(protoDef).supercontractserver;
  const client = new scProto.SupercontractServer(`127.0.0.1:${testPort}`, grpc.credentials.createInsecure());

  const cosignRpc = (req) => new Promise((resolve, reject) => {
    client.CosignTransaction(req, (err, res) => (err ? reject(err) : resolve(res)));
  });

  try {
    // Build a SELL settlement that sends XPX out of tradeA WITHOUT delivering any USDT
    const sellBadInner = TransferTransaction.create(
      Deadline.create(),
      tradeB.address,
      [new Mosaic(new MosaicId(config.XPX_MOSAIC_ID), UInt64.fromUint(1000000))],
      PlainMessage.create('CLOB_P2P_SETTLE:BAD'),
      config.NETWORK_TYPE
    ).toAggregate(PublicAccount.createFromPublicKey(tradeA.publicKey, config.NETWORK_TYPE));
    const sellBadAgg = AggregateTransaction.createCompleteV1(Deadline.create(), [sellBadInner], config.NETWORK_TYPE, [], UInt64.fromUint(160000));
    const sellBadSigned = vault.sign(sellBadAgg, config.GENERATION_HASH);

    // 4a. Missing orderContext must be REJECTED (previously silently bypassed!)
    const resNoCtx = await cosignRpc({
      action: 'SETTLEMENT',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({ allowedSigners: [tradeA.publicKey] }),
    });
    assert.strictEqual(resNoCtx.approved, false, 'SETTLEMENT without orderContext must be rejected');
    assert(resNoCtx.error.includes('missing signed orderContext'), `Unexpected error: ${resNoCtx.error}`);

    // 4b. Missing allowedSigners must be REJECTED
    const resNoAllowed = await cosignRpc({
      action: 'SETTLEMENT',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({}),
    });
    assert.strictEqual(resNoAllowed.approved, false, 'SETTLEMENT without allowedSigners must be rejected');
    assert(resNoAllowed.error.includes('missing allowedSigners'), `Unexpected error: ${resNoAllowed.error}`);

    // 4c. SELL sending XPX with 0 USDT received must reject cleanly with "without delivering USDT" (NOT ReferenceError!)
    const domainSep = `${config.NETWORK_TYPE}:SiriusCLOB-v1`;
    const nonce = '999991';
    const expiry = String(Date.now() + 3600_000);
    const msg = `${domainSep}:ORDER:XPX/USDT:sell:Market:0:1:${nonce}:${expiry}`;
    const kpA = KeyPair.createKeyPairFromPrivateKeyString(userA.privateKey, DerivationScheme.Ed25519Sha3);
    const sigHex = Buffer.from(KeyPair.sign(kpA, Buffer.from(msg, 'utf8'), DerivationScheme.Ed25519Sha3)).toString('hex');

    const resZeroPayout = await cosignRpc({
      action: 'SETTLEMENT',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({
        allowedSigners: [tradeA.publicKey],
        orderContext: {
          pair: 'XPX/USDT',
          side: 'sell',
          type: 'Market',
          price: '0',
          amount: '1',
          nonce,
          expiry,
          signature: sigHex,
          publicKey: userA.publicKey,
          domainSeparator: domainSep,
        },
      }),
    });
    assert.strictEqual(resZeroPayout.approved, false, 'Zero-payout SELL must be rejected');
    assert(resZeroPayout.error.includes('without delivering USDT'), `Expected clean quoteSym message, got: ${resZeroPayout.error}`);

    // 4d. Negative decimal in orderContext must be rejected by strict parseDecimalToBigIntValidator
    const negMsg = `${domainSep}:ORDER:XPX/USDT:sell:Limit:-5:1:${nonce}:${expiry}`;
    const negSig = Buffer.from(KeyPair.sign(kpA, Buffer.from(negMsg, 'utf8'), DerivationScheme.Ed25519Sha3)).toString('hex');
    const resNegPrice = await cosignRpc({
      action: 'SETTLEMENT',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({
        allowedSigners: [tradeA.publicKey],
        orderContext: {
          pair: 'XPX/USDT',
          side: 'sell',
          type: 'Limit',
          price: '-5',
          amount: '1',
          nonce,
          expiry,
          signature: negSig,
          publicKey: userA.publicKey,
          domainSeparator: domainSep,
        },
      }),
    });
    assert.strictEqual(resNegPrice.approved, false, 'Negative decimal price must be rejected');
    assert(resNegPrice.error.includes('invalid decimal value'), `Unexpected error: ${resNegPrice.error}`);

    // 4e. WITHDRAWAL without recipientAddress must be rejected
    const resNoRecip = await cosignRpc({
      action: 'WITHDRAWAL',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({ mosaics: [{ mosaicHex: config.XPX_MOSAIC_ID, maxWithdrawableAtomic: '1000000' }] }),
    });
    assert.strictEqual(resNoRecip.approved, false, 'WITHDRAWAL without recipientAddress must be rejected');
    assert(resNoRecip.error.includes('missing recipientAddress'), `Unexpected error: ${resNoRecip.error}`);

    // 4f. WITHDRAWAL without approved mosaics must be rejected
    const resNoMosaics = await cosignRpc({
      action: 'WITHDRAWAL',
      aggregate_payload_hex: sellBadSigned.payload,
      generation_hash: config.GENERATION_HASH,
      caller_public_key: userA.publicKey,
      trading_account_public_key: tradeA.publicKey,
      metadata_json: JSON.stringify({ recipientAddress: tradeB.address.plain() }),
    });
    assert.strictEqual(resNoMosaics.approved, false, 'WITHDRAWAL without mosaics must be rejected');
    assert(resNoMosaics.error.includes('missing approved mosaic list'), `Unexpected error: ${resNoMosaics.error}`);

    console.log('✅ Test 4 passed: All 6 validator fail-closed gRPC security checks enforced');
  } finally {
    client.close();
    await new Promise((r) => vmServer.tryShutdown(r));
  }

  // ============================================================================
  // TEST 5: assert3NodeConsensus null guard & BFT quorum checks
  // ============================================================================
  assert.throws(
    () => assert3NodeConsensus('EmptyResults', [], { expectSuccess: true, allowQuorum: false }),
    /Consensus divergence|missing primary node/
  );
  console.log('✅ Test 5 passed: assert3NodeConsensus null guard & quorum enforcement');

  clobDb.closeDb();
  try { fs.unlinkSync(tempDbPath); } catch (_) {}
  console.log('\n🎉 ALL COMPREHENSIVE EDGE-CASE & SECURITY REGRESSION TESTS PASSED!');
}

runTests().catch((err) => {
  console.error('❌ Test failed:', err);
  try { clobDb.closeDb(); } catch (_) {}
  try { fs.unlinkSync(tempDbPath); } catch (_) {}
  process.exit(1);
});
