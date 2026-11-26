/**
 * Red-Team 5-Pass Defensive Hardening Regression Suite
 * Verifies every critical & high vulnerability fixed across:
 *   Pass 1: PancakeSwap Oracle & Market Maker Vault
 *   Pass 2: Legacy Vault Deposit, Withdrawal Recovery & Emergency Unlock
 *   Pass 3: 2-of-2 Smart Trading Account Multisig & Level-2 Cluster Validation
 *   Pass 4: Cross-Chain Bridge Treasury (Finality, BigInt Math, Memo Validation, No Double-Credit)
 *   Pass 5: Deployment, Key Permissions & Container Hardening
 */

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

// Isolate SQLite DB so production cluster/clob_ledger.db is never touched
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sirius-redteam-5pass-'));
process.env.CLOB_DB_FILE = path.join(tmpDir, 'test_clob.db');
process.env.SIRIUS_NETWORK = 'testnet2';

const clobApi = require('../dashboard/clob_api');
const swapApi = require('../dashboard/swap_api');
const cosigner = require('../dashboard/trading_account_cosigner');
const { ClobMarketMaker } = require('../scripts/clob_market_maker');

let passed = 0;
async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log(`  ✅ ${name}`);
  } catch (err) {
    console.error(`  ❌ ${name}:`, err.message);
    process.exitCode = 1;
  }
}

async function main() {
  console.log('\n🛡️  Running Red-Team 5-Pass Defensive Hardening Regression Suite...\n');

  // --- PASS 2: Legacy Vault ---
  await test('Pass 2: creditVerifiedDeposit is strictly idempotent on duplicate txHash', () => {
    const crypto = require('crypto');
    const pub = crypto.randomBytes(32).toString('hex');
    const txHash = crypto.randomBytes(32).toString('hex');
    const depFile = path.join(__dirname, '../cluster/processed_deposits.json');
    const before = clobApi.getAccountBalance(pub);
    try {
      clobApi.creditVerifiedDeposit({
        txHash,
        senderPublicKey: pub,
        mosaicIdHex: '13BFC518E40549D7',
        amountAtomic: '5000000',
        blockHeight: 100,
      });
      const afterFirst = clobApi.getAccountBalance(pub);
      // Replay exact same deposit hash — must NOT double-credit!
      clobApi.creditVerifiedDeposit({
        txHash,
        senderPublicKey: pub,
        mosaicIdHex: '13BFC518E40549D7',
        amountAtomic: '5000000',
        blockHeight: 100,
      });
      const afterReplay = clobApi.getAccountBalance(pub);
      assert.strictEqual(BigInt(afterFirst.xpx) - BigInt(before.xpx), 5000000n);
      assert.strictEqual(BigInt(afterReplay.xpx), BigInt(afterFirst.xpx));
    } finally {
      // Remove synthetic entry from processed_deposits.json so cluster state stays pristine
      try {
        if (fs.existsSync(depFile)) {
          const d = JSON.parse(fs.readFileSync(depFile, 'utf8'));
          delete d[txHash.toLowerCase()];
          fs.writeFileSync(depFile, JSON.stringify(d, null, 2));
        }
      } catch (_) {}
    }
  });

  await test('Pass 2: creditVerifiedDeposit ignores zero or negative amounts', () => {
    const pub = 'b'.repeat(64);
    const before = clobApi.getAccountBalance(pub);
    clobApi.creditVerifiedDeposit({
      txHash: 'cafebabe'.repeat(8),
      senderPublicKey: pub,
      mosaicIdHex: '13BFC518E40549D7',
      amountAtomic: '0',
      blockHeight: 101,
    });
    const after = clobApi.getAccountBalance(pub);
    assert.strictEqual(BigInt(after.xpx), BigInt(before.xpx));
  });

  // --- PASS 3: Multisig Validation ---
  await test('Pass 3: isValidTwoOfTwoTradingMultisig rejects 2-of-3 attacker bypass', () => {
    const clusterPk = '1'.repeat(64).toUpperCase();
    const attacker1 = '2'.repeat(64).toUpperCase();
    const attacker2 = '3'.repeat(64).toUpperCase();

    // Valid exact 2-of-2
    assert.strictEqual(
      cosigner.isValidTwoOfTwoTradingMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [attacker1, clusterPk] },
        clusterPk,
        attacker1
      ),
      true
    );

    // CRITICAL ATTACK: 2-of-3 [attacker1, attacker2, clusterPk] -> MUST BE REJECTED!
    assert.strictEqual(
      cosigner.isValidTwoOfTwoTradingMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [attacker1, attacker2, clusterPk] },
        clusterPk
      ),
      false
    );

    // minRemoval < 2 -> MUST BE REJECTED!
    assert.strictEqual(
      cosigner.isValidTwoOfTwoTradingMultisig(
        { minApproval: 2, minRemoval: 1, cosignatories: [attacker1, clusterPk] },
        clusterPk,
        attacker1
      ),
      false
    );

    // Wrong owner -> MUST BE REJECTED!
    assert.strictEqual(
      cosigner.isValidTwoOfTwoTradingMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [attacker2, clusterPk] },
        clusterPk,
        attacker1
      ),
      false
    );
  });

  await test('Pass 3: isValidValidatorClusterMultisig enforces exact expected validator node set', () => {
    const n1 = 'a1'.repeat(32).toUpperCase();
    const n2 = 'b2'.repeat(32).toUpperCase();
    const n3 = 'c3'.repeat(32).toUpperCase();
    const rogue = 'ff'.repeat(32).toUpperCase();
    const cfg = { nodes: [{ publicKey: n1 }, { publicKey: n2 }, { publicKey: n3 }] };

    assert.strictEqual(
      cosigner.isValidValidatorClusterMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [n1, n2, n3] },
        cfg
      ),
      true
    );
    // Rogue cosignatory injected -> REJECT
    assert.strictEqual(
      cosigner.isValidValidatorClusterMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [n1, n2, rogue] },
        cfg
      ),
      false
    );
    // Extra 4th cosignatory -> REJECT
    assert.strictEqual(
      cosigner.isValidValidatorClusterMultisig(
        { minApproval: 2, minRemoval: 2, cosignatories: [n1, n2, n3, rogue] },
        cfg
      ),
      false
    );
  });

  // --- PASS 4: Bridge Treasury ---
  await test('Pass 4: bscWeiToSiriusAtomic performs exact 18->6 BigInt integer division', () => {
    // 123.456789999999999999 USDT (18 decimals) -> 123456789n micro-USDT (truncates dust, no float drift)
    assert.strictEqual(
      swapApi.bscWeiToSiriusAtomic(123456789999999999999n),
      123456789n
    );
    // Huge value > Number.MAX_SAFE_INTEGER (2^53) stays exact in BigInt
    assert.strictEqual(
      swapApi.bscWeiToSiriusAtomic(100000000000000000000000000n), // 100M tokens
      100000000000000n
    );
    assert.strictEqual(swapApi.bscWeiToSiriusAtomic(999999999999n), 0n);
    assert.strictEqual(swapApi.MIN_BRIDGE_CONFIRMATIONS, 15);
  });

  await test('Pass 4: extractSiriusAddress rejects arbitrary 64-hex ABI words and invalid checksums', () => {
    // Random 32-byte ABI word must NOT be treated as a Sirius public key
    const fakeAbiWord = '0x' + '00'.repeat(32);
    assert.strictEqual(swapApi.extractSiriusAddress(fakeAbiWord), null);

    // Invalid checksum V-address must be rejected
    const badAddrHex = '0x' + Buffer.from('VAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'utf8').toString('hex');
    assert.strictEqual(swapApi.extractSiriusAddress(badAddrHex), null);

    // Valid Testnet2 address (VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV) must be accepted
    const goodAddrHex = '0x' + Buffer.from('VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV', 'utf8').toString('hex');
    assert.strictEqual(swapApi.extractSiriusAddress(goodAddrHex), 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV');
  });

  // --- PASS 1: Oracle & Market Maker ---
  await test('Pass 1: clob_market_maker.js never deletes market_state.bin in seedMarketLadder', () => {
    const mmSrc = fs.readFileSync(path.join(__dirname, 'clob_market_maker.js'), 'utf8');
    assert.strictEqual(
      mmSrc.includes('unlinkSync(stateBin)'),
      false,
      'seedMarketLadder must never delete live market_state.bin!'
    );
    assert.ok(typeof ClobMarketMaker === 'function');
  });

  // --- PASS 5: Infra Hardening ---
  await test('Pass 5: start.sh enables umask 077 and creates cluster/ before PID writes', () => {
    const startSh = fs.readFileSync(path.join(__dirname, '../start.sh'), 'utf8');
    assert.ok(startSh.includes('umask 077'), 'start.sh must set umask 077');
    assert.ok(
      startSh.indexOf('mkdir -p cluster') < startSh.indexOf('cluster/node1_vm.pid'),
      'mkdir -p cluster must precede PID file creation'
    );
  });

  await test('Pass 5: sirius-validator-node Dockerfile & docker-compose.yml drop root and preserve node.env secrets', () => {
    const valDir = path.resolve(__dirname, '../../sirius-validator-node');
    const dockerfile = fs.readFileSync(path.join(valDir, 'Dockerfile'), 'utf8');
    const compose = fs.readFileSync(path.join(valDir, 'docker-compose.yml'), 'utf8');
    assert.ok(dockerfile.includes('USER node'), 'Dockerfile must run as non-root USER node');
    assert.strictEqual(
      compose.includes('SIRIUS_VM_AUTH_TOKEN=${SIRIUS_VM_AUTH_TOKEN:-}'),
      false,
      'docker-compose.yml must not clobber SIRIUS_VM_AUTH_TOKEN with empty default'
    );
    assert.ok(compose.includes('/app/contracts:ro'), 'contracts volume must be mounted read-only');
    assert.ok(compose.includes('no-new-privileges:true'), 'compose must enforce no-new-privileges');
  });

  await test('Pass 5: GET /api/clob/solvency and /api/clob/solvency/audit are strictly read-only', () => {
    const clobApiSrc = fs.readFileSync(path.join(__dirname, '../dashboard/clob_api.js'), 'utf8');
    const serverSrc = fs.readFileSync(path.join(__dirname, '../dashboard/server.js'), 'utf8');
    const fnStart = clobApiSrc.indexOf('function assertSolvencyReconciliation(');
    const fnEnd = clobApiSrc.indexOf('function encodePlaceOrder(', fnStart);
    assert.ok(fnStart > 0 && fnEnd > fnStart, 'Could not locate assertSolvencyReconciliation');
    const fnBody = clobApiSrc.slice(fnStart, fnEnd);
    assert.strictEqual(
      fnBody.includes('saveBalances('),
      false,
      'assertSolvencyReconciliation must never mutate balances via saveBalances()'
    );
    assert.strictEqual(
      serverSrc.includes("parsedUrl.searchParams.get('trip') === 'true'"),
      false,
      'GET /api/clob/solvency/audit must never allow public ?trip=true mutation'
    );
  });

  await test('Pass 5: Mainnet hard guard blocks legacy custodial deposits, orders, withdrawals & emergencyUnlock', async () => {
    const prevNet = process.env.SIRIUS_NETWORK;
    process.env.SIRIUS_NETWORK = 'mainnet';
    const dummyPub = 'e'.repeat(64);
    try {
      await assert.rejects(
        () => clobApi.depositTokens({ publicKey: dummyPub, amount: '10', mosaicId: 'XPX' }),
        /Legacy custodial vault deposits are permanently disabled on Mainnet/
      );
      await assert.rejects(
        () => clobApi.placeOrder({ publicKey: dummyPub, side: 'BUY', price: '0.001', amount: '10', pair: 'XPX/USDT' }),
        /Mainnet requires an active non-custodial 2-of-2 Smart Trading Account/
      );
      await assert.rejects(
        () => clobApi.withdrawTokens({ publicKey: dummyPub, amount: '10', mosaic: 'XPX', nonce: '1', expiry: Date.now() + 60000 }),
        /Legacy custodial vault withdrawals are disabled on Mainnet/
      );
      await assert.rejects(
        () => clobApi.emergencyUnlock({ publicKey: dummyPub, nonce: '1', expiry: Date.now() + 60000, signature: '0'.repeat(128) }),
        /Legacy custodial vault emergency unlock is disabled on Mainnet/
      );
    } finally {
      if (prevNet === undefined) delete process.env.SIRIUS_NETWORK;
      else process.env.SIRIUS_NETWORK = prevNet;
    }
  });

  // --- ROUND 2 HARDENING CHECKS ---
  await test('Round 2 Pass 1: registerHeartbeat & deregisterHeartbeat reject unauthenticated calls', () => {
    const victimPub = 'f'.repeat(64);
    assert.throws(
      () => clobApi.registerHeartbeat({ publicKey: victimPub, timeoutMs: 1000, markets: ['XPX/USDT'] }),
      /Missing ed25519 signature/
    );
    assert.throws(
      () => clobApi.deregisterHeartbeat({ publicKey: victimPub }),
      /Missing ed25519 signature/
    );
  });

  await test('Round 2 Pass 2: clob_db enforces forward-only state machine on bridge_swaps & settlements', () => {
    const clobDb = require('../dashboard/clob_db');
    const txHash = '0x' + 'ab'.repeat(32);
    clobDb.saveBridgeSwapRecord({
      txnHash: txHash,
      swapId: 'SWAP-TEST-1',
      network: 'BSC',
      token: 'USDT',
      amount: 10,
      recipient: 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV',
      status: 'CONFIRMED',
      confirmations: 15,
    });
    // Backward transition CONFIRMED -> PENDING must be rejected
    assert.throws(
      () => clobDb.saveBridgeSwapRecord({
        txnHash: txHash,
        swapId: 'SWAP-TEST-1',
        network: 'BSC',
        token: 'USDT',
        amount: 10,
        recipient: 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV',
        status: 'PENDING',
        confirmations: 0,
      }),
      /Invalid backward bridge swap status transition/
    );
    // Negative amount must be rejected
    assert.throws(
      () => clobDb.saveBridgeSwapRecord({
        txnHash: '0x' + 'cd'.repeat(32),
        swapId: 'SWAP-TEST-NEG',
        network: 'BSC',
        token: 'USDT',
        amount: -5,
        recipient: 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV',
        status: 'CONFIRMED',
      }),
      /Invalid bridge swap amount/
    );
    // LIKE wildcard injection in getPendingSettlementsForCosigner returns []
    assert.deepStrictEqual(clobDb.getPendingSettlementsForCosigner('%'), []);
  });

  await test('Round 2 Pass 4: grpc_tls fails closed on non-loopback Mainnet bind without TLS', () => {
    const grpcTls = require('../executor/grpc_tls');
    const prevNet = process.env.SIRIUS_NETWORK;
    const prevAllow = process.env.SIRIUS_VM_ALLOW_INSECURE_REMOTE;
    process.env.SIRIUS_NETWORK = 'mainnet';
    delete process.env.SIRIUS_VM_ALLOW_INSECURE_REMOTE;
    try {
      assert.throws(
        () => grpcTls.createGrpcServerCredentials('0.0.0.0:5051'),
        /Refusing to bind non-loopback validator.*without TLS on Mainnet/
      );
      // Loopback bind remains allowed for local development/tests
      const loop = grpcTls.createGrpcServerCredentials('127.0.0.1:5051');
      assert.strictEqual(loop.mode, 'insecure');
    } finally {
      if (prevNet === undefined) delete process.env.SIRIUS_NETWORK;
      else process.env.SIRIUS_NETWORK = prevNet;
      if (prevAllow !== undefined) process.env.SIRIUS_VM_ALLOW_INSECURE_REMOTE = prevAllow;
    }
  });

  await test('Round 3: grpc_vm_server verifyVmAuthMetadata fails closed on Mainnet when token unset', () => {
    const vmServer = require('../executor/grpc_vm_server');
    const prevNet = process.env.SIRIUS_NETWORK;
    const prevTok = process.env.SIRIUS_VM_AUTH_TOKEN;
    process.env.SIRIUS_NETWORK = 'mainnet';
    delete process.env.SIRIUS_VM_AUTH_TOKEN;
    try {
      assert.strictEqual(vmServer.verifyVmAuthMetadata({ metadata: { get: () => [] } }), false);
    } finally {
      if (prevNet === undefined) delete process.env.SIRIUS_NETWORK;
      else process.env.SIRIUS_NETWORK = prevNet;
      if (prevTok !== undefined) process.env.SIRIUS_VM_AUTH_TOKEN = prevTok;
    }
  });

  await test('Round 3: wasm_worker write_file_stream enforces assertWasmBounds', () => {
    const workerSrc = fs.readFileSync(path.join(__dirname, '../executor/wasm_worker.js'), 'utf8');
    const idx = workerSrc.indexOf('write_file_stream:');
    assert.ok(idx > 0);
    const snippet = workerSrc.slice(idx, idx + 200);
    assert.ok(snippet.includes('assertWasmBounds(bufPtr, bufLen)'), 'write_file_stream must call assertWasmBounds');
  });

  await test('Live10Acc P0-1: buildSettlementAggregateTx attaches 1-micro-XPX Vault anchor on pure P2P swaps', () => {
    const { Account, TransactionMapping } = require('tsjs-xpx-chain-sdk');
    const testnetClient = require('../dashboard/testnet2_client');
    const vaultPub = testnetClient.getOrInitVaultAccount().publicKey.toUpperCase();
    const ta1 = Account.generateNewAccount(168);
    const ta2 = Account.generateNewAccount(168);
    const o1 = Account.generateNewAccount(168);
    const o2 = Account.generateNewAccount(168);

    const pureP2p = cosigner.buildSettlementAggregateTx({
      orderId: 999,
      transfers: [
        { fromTradingPub: ta1.publicKey, fromType: 'TRADING', ownerPublicKey: o1.publicKey, toAddress: ta2.address.plain(), mosaicHex: '13BFC518E40549D7', amountAtomic: '1000000', symbol: 'XPX' },
        { fromTradingPub: ta2.publicKey, fromType: 'TRADING', ownerPublicKey: o2.publicKey, toAddress: ta1.address.plain(), mosaicHex: '4430AEFEF7E15D0C', amountAtomic: '85', symbol: 'USDT' },
      ],
    });
    assert.ok(pureP2p && pureP2p.signedByVault && pureP2p.allowedInnerSigners.has(vaultPub), 'Vault must be an eligible inner signer for pure P2P aggregate');
    const decodedP2p = TransactionMapping.createFromPayload(pureP2p.signedByVault.payload);
    assert.strictEqual(decodedP2p.innerTransactions.length, 3, 'Pure P2P aggregate must include 1-micro-XPX Vault anchor inner tx');
    assert.ok(decodedP2p.innerTransactions.some((t) => t.signer.publicKey.toUpperCase() === vaultPub), 'At least one inner tx must be signed by Vault initiator');

    // When a VAULT trade leg already exists, no extra anchor tx should be appended
    const hybrid = cosigner.buildSettlementAggregateTx({
      orderId: 1000,
      transfers: [
        { fromTradingPub: vaultPub, fromType: 'VAULT', ownerPublicKey: null, toAddress: ta2.address.plain(), mosaicHex: '13BFC518E40549D7', amountAtomic: '1000000', symbol: 'XPX' },
        { fromTradingPub: ta2.publicKey, fromType: 'TRADING', ownerPublicKey: o2.publicKey, toAddress: ta1.address.plain(), mosaicHex: '4430AEFEF7E15D0C', amountAtomic: '85', symbol: 'USDT' },
      ],
    });
    const decodedHybrid = TransactionMapping.createFromPayload(hybrid.signedByVault.payload);
    assert.strictEqual(decodedHybrid.innerTransactions.length, 2, 'Hybrid Vault+P2P aggregate should not duplicate anchor leg');
  });

  await test('Live10Acc P0-2: Smart Trading settlements emit taker fee transfers to FEE_COLLECTOR_ADDRESS on BUY & SELL', () => {
    const apiSrc = fs.readFileSync(path.join(__dirname, '../dashboard/clob_api.js'), 'utf8');
    assert.ok(apiSrc.includes('if (baseLeg.legFee > 0n && testnet2Config.FEE_COLLECTOR_ADDRESS)'), 'BUY taker fee transfer missing');
    assert.ok(apiSrc.includes('if (quoteLeg.legFee > 0n && testnet2Config.FEE_COLLECTOR_ADDRESS)'), 'SELL taker fee transfer missing');
    assert.ok(apiSrc.includes('Leg A2 (Taker Fee on BUY)') && apiSrc.includes('Leg B2 (Taker Fee on SELL)'));
  });

  await test('Live10Acc P0-3: GET /validator-info auto-refreshes L1 multisig status & modal skips admin bootstrap', () => {
    const srvSrc = fs.readFileSync(path.join(__dirname, '../dashboard/server.js'), 'utf8');
    assert.ok(srvSrc.includes('refreshValidatorClusterOnChainStatus'), 'server.js must refresh validator cluster L1 status');
    const modalPath = path.resolve(__dirname, '../../web-wallet-exchange/src/modules/services/submodule/clob/components/TradingAccountSetupModal.vue');
    if (fs.existsSync(modalPath)) {
      const modalSrc = fs.readFileSync(modalPath, 'utf8');
      assert.ok(!modalSrc.includes('/api/clob/validator-cluster/bootstrap'), 'User modal must not call admin-gated /validator-cluster/bootstrap');
    }
  });

  await test('Live10Acc P1-1/P1-2/P1-3/P2-1: Slippage floor, PartiallyFilled status, STP market_state sync & PostOnly support', () => {
    const clobApi = require('../dashboard/clob_api');
    const msg = clobApi.constructOrderMessage('XPX/USDT', 'sell', 'PostOnly', '0.000090', '10', 'n1', 9999999999999);
    assert.ok(msg.includes(':ORDER:XPX/USDT:sell:PostOnly:'), `Expected PostOnly in canonical msg, got: ${msg}`);
    const apiSrc = fs.readFileSync(path.join(__dirname, '../dashboard/clob_api.js'), 'utf8');
    assert.ok(apiSrc.includes('priceTicks = (slippageCeilingTicks / tickSize) * tickSize;'), 'Market Buy slippage ceiling must floor to tick grid');
    assert.ok(apiSrc.includes("totalFilled > 0n ? 'PartiallyFilled' : 'Open'"), 'reconcileActiveOrdersWithTrades must preserve PartiallyFilled');
    assert.ok(apiSrc.includes('function readLiveOrdersFromMarketState('), 'STP sync must reconcile against market_state.bin');
  });

  await test('Live10Acc P1-4: grpc_vm_server uses WarmWasmWorkerPool and wasm_worker supports poolMode', () => {
    const vmSrc = fs.readFileSync(path.join(__dirname, '../executor/grpc_vm_server.js'), 'utf8');
    const workerSrc = fs.readFileSync(path.join(__dirname, '../executor/wasm_worker.js'), 'utf8');
    assert.ok(vmSrc.includes('class WarmWasmWorkerPool'), 'WarmWasmWorkerPool missing from grpc_vm_server.js');
    assert.ok(workerSrc.includes('poolMode = false') && workerSrc.includes("msg.type === 'execute'"), 'poolMode missing from wasm_worker.js');
  });

  await test('Track 1: Maker/Taker participantContexts attached & ViewClobExchange enforces strict publicKey + cumulative cap', () => {
    const apiSrc = fs.readFileSync(path.join(__dirname, '../dashboard/clob_api.js'), 'utf8');
    assert.ok(apiSrc.includes('participantContexts[mkPub]') && apiSrc.includes('signedOrderContext:'), 'clob_api.js must persist signedOrderContext and attach participantContexts for Makers');
    const vuePath = path.resolve(__dirname, '../../web-wallet-exchange/src/modules/services/submodule/clob/views/ViewClobExchange.vue');
    if (fs.existsSync(vuePath)) {
      const vueSrc = fs.readFileSync(vuePath, 'utf8');
      assert.ok(vueSrc.includes('cumulativeOrderOutflows'), 'ViewClobExchange.vue must track cumulative partial-fill outflows');
      assert.ok(vueSrc.includes('candidateCtx.publicKey &&'), 'ViewClobExchange.vue must strictly require publicKey on orderContext');
      assert.ok(!vueSrc.includes('(!ctx.publicKey ||'), 'Weak !ctx.publicKey fallback must be removed');
    }
  });

  await test('Track 2: Stuck pending_cosign settlement expires safely, releases deltas, rejects stale cosig & penalizes ONLY defaulter', async () => {
    const clobDb = require('../dashboard/clob_db');
    const { Account } = require('tsjs-xpx-chain-sdk');
    const maker = Account.generateNewAccount(168);
    const taker = Account.generateNewAccount(168);
    const makerTa = Account.generateNewAccount(168);
    const takerTa = Account.generateNewAccount(168);
    const sid = `SET_TEST_EXPIRE_${Date.now()}`;
    const oldCreatedAt = Date.now() - 48 * 60 * 60 * 1000; // 48h old (> 24h TTL)

    // Taker signed, Maker defaulted!
    clobDb.saveTradingSettlement({
      settlementId: sid,
      orderId: '777',
      ownerPublicKey: taker.publicKey,
      tradingPublicKey: takerTa.publicKey,
      counterpartyKey: maker.publicKey,
      txHash: 'AA'.repeat(32),
      aggregatePayload: '00'.repeat(120),
      requiredCosigners: [taker.publicKey.toUpperCase(), maker.publicKey.toUpperCase()],
      collectedCosigs: [{ signer: taker.publicKey.toUpperCase(), signature: 'BB'.repeat(64) }],
      transfers: [{ fromTradingPub: takerTa.publicKey, fromType: 'TRADING', ownerPublicKey: taker.publicKey, toAddress: makerTa.address.plain(), mosaicHex: '4430AEFEF7E15D0C', amountAtomic: '1000', symbol: 'USDT' }],
      delta: { xpx: '1000000', usdt: '-1000', bnb: '0' },
      status: 'pending_cosign',
      createdAt: oldCreatedAt,
    });

    const expiredList = await cosigner.expireStalePendingSettlements({ ttlMs: 60_000, now: Date.now() });
    const hit = expiredList.find((e) => e.settlementId === sid);
    assert.ok(hit, 'Stale pending_cosign settlement must be expired');
    assert.deepStrictEqual(hit.defaulters, [maker.publicKey.toUpperCase()], 'Only non-cosigning Maker should be flagged as defaulter');

    const after = clobDb.getTradingSettlement(sid);
    assert.strictEqual(after.status, 'expired');
    assert.strictEqual(cosigner.checkCosignDefaulterCooldown(taker.publicKey).restricted, false, 'Honest cosigning Taker must NOT be restricted');
    assert.strictEqual(cosigner.checkCosignDefaulterCooldown(maker.publicKey).restricted, true, 'Defaulting Maker MUST receive order-placement cooldown');

    // Stale late cosignature on expired settlement must be rejected
    await assert.rejects(
      () => cosigner.submitSettlementCosignature({ settlementId: sid, cosignature: { signer: maker.publicKey, signature: 'CC'.repeat(64) } }),
      /terminal \(expired\)/
    );
  });

  await test('Track 3: swap_api Mainnet fail-closed guard, durable PENDING reservation, oracle timeout & ws_hub right-most IP', () => {
    const swapSrc = fs.readFileSync(path.join(__dirname, '../dashboard/swap_api.js'), 'utf8');
    assert.ok(swapSrc.includes('assertBridgeNetworkConfigured'), 'swap_api.js must fail closed on Mainnet without BSC Mainnet env');
    assert.ok(swapSrc.includes("status: 'PENDING'") && swapSrc.includes('isValidSiriusAddressForActiveNetwork(normClaimRecipient)'), 'swap_api.js must reserve PENDING before L1 delivery and validate claimRecipient');
    assert.ok(swapSrc.includes('(SiriusNetwork:${config.NETWORK_TYPE}, BscChain:${ACTIVE_BSC_CHAIN_ID})'), 'swap_api.js must domain-separate EVM claim signatures');

    const oracleSrc = fs.readFileSync(path.join(__dirname, '../dashboard/pancakeswap_oracle.js'), 'utf8');
    assert.ok(oracleSrc.includes('getSharedBscProvider') && oracleSrc.includes('withTimeout('), 'pancakeswap_oracle.js must reuse provider and enforce RPC timeout');

    const wsSrc = fs.readFileSync(path.join(__dirname, '../dashboard/ws_hub.js'), 'utf8');
    assert.ok(wsSrc.includes('parts[parts.length - 1]') && wsSrc.includes('MAX_WS_BUFFERED_BYTES') && wsSrc.includes("ws.on('error'"), 'ws_hub.js must use right-most XFF IP, backpressure cap, and error handler');
  });

  await test('Track 4: Fair per-pair round-robin consensus lanes prevent starvation across XPX/USDT & XPX/BNB', async () => {
    const clobApi = require('../dashboard/clob_api');
    const order = [];
    // Enqueue 3 xpx_usdt tasks + 2 xpx_bnb tasks concurrently while lane drains; xpx_bnb must interleave!
    const p1 = clobApi.withClusterConsensusLock(async () => { await new Promise((r) => setTimeout(r, 15)); order.push('usdt-1'); }, 'xpx_usdt');
    const p2 = clobApi.withClusterConsensusLock(async () => { order.push('usdt-2'); }, 'xpx_usdt');
    const p3 = clobApi.withClusterConsensusLock(async () => { order.push('usdt-3'); }, 'xpx_usdt');
    const p4 = clobApi.withClusterConsensusLock(async () => { order.push('bnb-1'); }, 'xpx_bnb');
    const p5 = clobApi.withClusterConsensusLock(async () => { order.push('bnb-2'); }, 'xpx_bnb');
    await Promise.all([p1, p2, p3, p4, p5]);
    assert.strictEqual(order.length, 5);
    assert.ok(order.indexOf('bnb-1') < order.indexOf('usdt-3'), `Expected round-robin interleaving so bnb-1 runs before usdt-3, got: ${JSON.stringify(order)}`);
  });

  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}

  console.log(`\n🎉 Red-Team 5-Pass Hardening Suite: ${passed} tests passed.\n`);
  if (process.exitCode) process.exit(process.exitCode);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
