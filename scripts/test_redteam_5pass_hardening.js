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

  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}

  console.log(`\n🎉 Red-Team 5-Pass Hardening Suite: ${passed} tests passed.\n`);
  if (process.exitCode) process.exit(process.exitCode);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
