/**
 * SiriusCLOB Native Co-Signed Smart Trading Account Coordinator (Solution 2 — Model A)
 *
 * Implements Hierarchical Native Sirius Multi-Level Multisig:
 *   - Level 1: User Trading Account (2-of-2: UserMainAccount + ValidatorClusterMultisig)
 *   - Level 2: ValidatorClusterMultisig (2-of-3 BFT: Node1, Node2, Node3 on separate servers)
 *
 * Neither the Exchange nor any compromised server can spend user funds without the user's signature,
 * and the user cannot double-spend funds on L1 without 2-of-3 Validator Server cosignatures.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const grpc = require('@grpc/grpc-js');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const VALIDATOR_CLUSTER_FILE = path.join(CLUSTER_DIR, 'validator_cluster_multisig.json');

const sdk = require('tsjs-xpx-chain-sdk');
const {
  Account,
  Address,
  AggregateTransaction,
  TransferTransaction,
  ModifyMultisigAccountTransaction,
  MultisigCosignatoryModification,
  MultisigCosignatoryModificationType,
  CosignatureTransaction,
  TransactionMapping,
  TransactionType,
  Deadline,
  Mosaic,
  MosaicId,
  UInt64,
  PlainMessage,
  KeyPair,
  DerivationScheme,
  PublicAccount,
} = sdk;

const {
  AggregateTransaction: BuilderAgg,
} = require('tsjs-xpx-chain-sdk/dist/src/infrastructure/builders/AggregateTransaction');
const {
  VerifiableTransaction,
} = require('tsjs-xpx-chain-sdk/dist/src/infrastructure/builders/VerifiableTransaction');

const config = require('./testnet2_config');
const testnetClient = require('./testnet2_client');
const clobDb = require('./clob_db');
const { NODES } = require('../executor/milestone2_verifier');
const { supercontractserver, getOrInitNodeValidatorAccount } = require('../executor/grpc_vm_server');

const VALIDATOR_QUORUM = 2; // 2-of-3 BFT Threshold
const GRPC_COSIGN_TIMEOUT_MS = 6000;
const DEFAULT_CLUSTER_SEED = process.env.DRIVE_KEY_HEX || 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
let cachedClusterCfg = null;
let lastClusterProbeAt = 0;

const vmClientCache = new Map();
function getVmClient(node) {
  const addr = `${node.vmHost || '127.0.0.1'}:${node.vmPort || 5051}`;
  if (!vmClientCache.has(addr)) {
    const client = new supercontractserver.SupercontractServer(
      addr,
      grpc.credentials.createInsecure()
    );
    vmClientCache.set(addr, client);
  }
  return vmClientCache.get(addr);
}

function buildVmMetadata() {
  const md = new grpc.Metadata();
  if (process.env.SIRIUS_VM_AUTH_TOKEN) {
    md.add('x-sirius-vm-token', process.env.SIRIUS_VM_AUTH_TOKEN);
  }
  return md;
}

function pingNodeValidator(node) {
  return new Promise((resolve) => {
    const client = getVmClient(node);
    const deadline = new Date(Date.now() + 3000);
    client.Ping({ name: 'clob-gateway' }, buildVmMetadata(), { deadline }, (err, reply) => {
      if (err || !reply || !reply.validator_public_key) {
        return resolve(null);
      }
      resolve({
        name: reply.node_name || node.name,
        host: node.vmHost,
        port: node.vmPort,
        publicKey: reply.validator_public_key.toUpperCase(),
        address: Address.createFromPublicKey(reply.validator_public_key, config.NETWORK_TYPE).plain(),
      });
    });
  });
}

function cosignOnSingleNode(node, cosignReq) {
  return new Promise((resolve) => {
    const client = getVmClient(node);
    const deadline = new Date(Date.now() + GRPC_COSIGN_TIMEOUT_MS);
    client.CosignTransaction(cosignReq, buildVmMetadata(), { deadline }, (err, reply) => {
      if (err) {
        return resolve({
          approved: false,
          node_name: node.name,
          error: err.message || 'gRPC error',
        });
      }
      resolve(reply || { approved: false, node_name: node.name, error: 'Empty gRPC reply' });
    });
  });
}

/**
 * Load or initialize Level 2 Validator Cluster Multisig account & 3 Validator Node identities.
 * Note: Gateway stores ONLY the Cluster account info and Validator Node PUBLIC KEYS.
 * Each physical validator server holds only its own private key.
 */
function getOrInitValidatorClusterConfig() {
  if (
    process.env.SIRIUS_VALIDATOR_CLUSTER_PUBKEY &&
    /^[0-9a-fA-F]{64}$/.test(process.env.SIRIUS_VALIDATOR_CLUSTER_PUBKEY.trim())
  ) {
    const clusterPub = process.env.SIRIUS_VALIDATOR_CLUSTER_PUBKEY.trim().toUpperCase();
    const clusterAddr = Address.createFromPublicKey(clusterPub, config.NETWORK_TYPE).plain();
    const nodePubs = (process.env.SIRIUS_VALIDATOR_NODE_PUBKEYS || '')
      .split(',')
      .map((s) => s.trim().toUpperCase())
      .filter((s) => /^[0-9A-F]{64}$/.test(s));
    return {
      network: config.NETWORK_NAME,
      networkType: config.NETWORK_TYPE,
      clusterPublicKey: clusterPub,
      clusterAddress: clusterAddr,
      minApproval: VALIDATOR_QUORUM,
      minRemoval: VALIDATOR_QUORUM,
      isMultisigConfirmedOnChain: Boolean(cachedClusterCfg && cachedClusterCfg.isMultisigConfirmedOnChain),
      nodes: NODES.map((n, idx) => ({
        name: n.name,
        host: n.vmHost,
        port: n.vmPort,
        publicKey: nodePubs[idx] || getOrInitNodeValidatorAccount(n.name).publicKey.toUpperCase(),
      })),
    };
  }

  if (cachedClusterCfg && cachedClusterCfg.clusterPublicKey) {
    return cachedClusterCfg;
  }

  if (fs.existsSync(VALIDATOR_CLUSTER_FILE)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(VALIDATOR_CLUSTER_FILE, 'utf8'));
      if (parsed && parsed.clusterPublicKey && Array.isArray(parsed.nodes) && parsed.nodes.length >= 3) {
        cachedClusterCfg = parsed;
        return cachedClusterCfg;
      }
    } catch (_) {}
  }

  const isMainnet = config.NETWORK_TYPE === 184 || String(config.NETWORK_NAME || '').toLowerCase() === 'mainnet';
  const clusterSeed = (process.env.SIRIUS_CLUSTER_SEED || '').trim() || (!isMainnet ? DEFAULT_CLUSTER_SEED : '');
  let clusterAccount;
  if (clusterSeed && !process.env.SIRIUS_DISABLE_DETERMINISTIC_KEYS) {
    const clusterPriv = crypto
      .createHash('sha256')
      .update(`${clusterSeed}:sirius-clob:${config.NETWORK_TYPE}:validator-cluster`)
      .digest('hex')
      .toUpperCase();
    clusterAccount = Account.createFromPrivateKey(clusterPriv, config.NETWORK_TYPE, 1);
  } else {
    clusterAccount = Account.generateNewAccount(config.NETWORK_TYPE);
  }
  const nodeEntries = NODES.map((n) => {
    const acc = getOrInitNodeValidatorAccount(n.name);
    return {
      name: n.name,
      host: n.vmHost,
      port: n.vmPort,
      publicKey: acc.publicKey.toUpperCase(),
      address: acc.address.plain(),
    };
  });

  const cfg = {
    description: 'SiriusCLOB Hierarchical Level-2 BFT Validator Cluster (2-of-3 Multisig)',
    network: config.NETWORK_NAME,
    networkType: config.NETWORK_TYPE,
    clusterAddress: clusterAccount.address.plain(),
    clusterPublicKey: clusterAccount.publicKey.toUpperCase(),
    // Bootstrap private key used ONLY once if needed to convert Level-2 cluster account to 2-of-3 multisig on L1
    bootstrapPrivateKey: clusterAccount.privateKey,
    minApproval: VALIDATOR_QUORUM,
    minRemoval: VALIDATOR_QUORUM,
    nodes: nodeEntries,
    isMultisigConfirmedOnChain: false,
    createdAt: new Date().toISOString(),
  };

  try {
    fs.mkdirSync(path.dirname(VALIDATOR_CLUSTER_FILE), { recursive: true, mode: 0o700 });
    fs.writeFileSync(VALIDATOR_CLUSTER_FILE, JSON.stringify(cfg, null, 2), { mode: 0o600 });
  } catch (_) {}
  cachedClusterCfg = cfg;
  return cachedClusterCfg;
}

async function findFundedBackendSponsor(xpxHex, minBalanceAtomic = 700_000n) {
  const candidates = [];
  try { candidates.push(testnetClient.getOrInitVaultAccount()); } catch (_) {}
  try {
    const t = testnetClient.getOrInitTreasuryAccount();
    if (t) candidates.push(t);
  } catch (_) {}
  try {
    const f = testnetClient.getOrInitFeeCollectorAccount();
    if (f) candidates.push(f);
  } catch (_) {}

  for (const acc of candidates) {
    if (!acc) continue;
    try {
      const bal = await testnetClient.getAccountOnChainBalances(acc.address.plain(), true);
      if ((bal.get(xpxHex) || 0n) >= minBalanceAtomic) {
        return acc;
      }
    } catch (_) {}
  }
  return null;
}

/**
 * Verify that the Level 2 Validator Cluster Account is converted to 2-of-3 multisig on Sirius L1.
 * Never broadcasts an unfunded transaction: returns { ok: false, needsFunding: true } if no sponsor has >= 0.7 XPX.
 */
async function ensureValidatorClusterOnChain(waitForConfirmation = true) {
  const cfg = getOrInitValidatorClusterConfig();
  const clusterAddr = cfg.clusterAddress;

  // 1. Check live L1 multisig status
  try {
    const nodeUrl = testnetClient.getActiveNodeUrl();
    const info = await testnetClient.fetchJson(`${nodeUrl}/account/${clusterAddr}/multisig`, 4000);
    if (info && info.multisig && info.multisig.minApproval >= 2 && Array.isArray(info.multisig.cosignatories)) {
      if (!cfg.isMultisigConfirmedOnChain) {
        cfg.isMultisigConfirmedOnChain = true;
        delete cfg.bootstrapPrivateKey;
        if (cachedClusterCfg) cachedClusterCfg.isMultisigConfirmedOnChain = true;
        try {
          fs.writeFileSync(VALIDATOR_CLUSTER_FILE, JSON.stringify(cfg, null, 2), { mode: 0o600 });
        } catch (_) {}
      }
      return { ok: true, alreadyMultisig: true, isMultisigConfirmedOnChain: true, clusterAddress: clusterAddr, clusterPublicKey: cfg.clusterPublicKey, multisig: info.multisig };
    }
  } catch (_) {
    // 404 means not yet converted to multisig on L1
  }

  // 2. Refresh node public keys via gRPC Ping if possible
  const pingResults = await Promise.all(NODES.map((n) => pingNodeValidator(n)));
  for (let i = 0; i < NODES.length; i++) {
    if (pingResults[i] && pingResults[i].publicKey) {
      cfg.nodes[i].publicKey = pingResults[i].publicKey;
      cfg.nodes[i].address = pingResults[i].address;
    }
  }

  if (!cfg.bootstrapPrivateKey) {
    return {
      ok: false,
      alreadyMultisig: false,
      isMultisigConfirmedOnChain: false,
      clusterAddress: clusterAddr,
      clusterPublicKey: cfg.clusterPublicKey,
      message: 'Validator cluster not yet converted on-chain and bootstrapPrivateKey is external.',
    };
  }

  const clusterAcc = Account.createFromPrivateKey(cfg.bootstrapPrivateKey, config.NETWORK_TYPE, 1);

  // Check if a previous Level-2 activation tx is already unconfirmed/pending in mempool
  try {
    const unconf = await testnetClient.fetchJson(`${testnetClient.getActiveNodeUrl()}/account/${clusterAddr}/transactions/unconfirmed`, 3000);
    if (Array.isArray(unconf) && unconf.length > 0) {
      if (waitForConfirmation) {
        for (let i = 0; i < 25; i++) {
          await new Promise((r) => setTimeout(r, 2500));
          try {
            const info = await testnetClient.fetchJson(`${testnetClient.getActiveNodeUrl()}/account/${clusterAddr}/multisig`, 3000);
            if (info && info.multisig && info.multisig.minApproval >= 2) {
              cfg.isMultisigConfirmedOnChain = true;
              delete cfg.bootstrapPrivateKey;
              try {
                fs.writeFileSync(VALIDATOR_CLUSTER_FILE, JSON.stringify(cfg, null, 2), { mode: 0o600 });
              } catch (_) {}
              return {
                ok: true,
                alreadyMultisig: false,
                isMultisigConfirmedOnChain: true,
                clusterAddress: clusterAddr,
                clusterPublicKey: cfg.clusterPublicKey,
              };
            }
          } catch (_) {}
        }
      }
      return {
        ok: true,
        alreadyMultisig: false,
        pendingOnChain: true,
        isMultisigConfirmedOnChain: false,
        clusterAddress: clusterAddr,
        clusterPublicKey: cfg.clusterPublicKey,
      };
    }
  } catch (_) {}

  // Ensure clusterAcc has >= 0.35 XPX to pay its own L1 AggregateComplete conversion fee
  const xpxHex = (config.XPX_MOSAIC_ID || '13BFC518E40549D7').toLowerCase();
  let clusterXpx = 0n;
  try {
    const cBal = await testnetClient.getAccountOnChainBalances(clusterAddr, true);
    clusterXpx = cBal.get(xpxHex) || 0n;
  } catch (_) {}

  if (clusterXpx < 350_000n) {
    const sponsor = await findFundedBackendSponsor(xpxHex, 700_000n);
    if (!sponsor) {
      return {
        ok: false,
        alreadyMultisig: false,
        needsFunding: true,
        isMultisigConfirmedOnChain: false,
        clusterAddress: clusterAddr,
        clusterPublicKey: cfg.clusterPublicKey,
        requiredAtomic: '500000',
        message: `Level-2 Validator Cluster (${clusterAddr}) needs 0.5 XPX on-chain to activate 2-of-3 multisig.`,
      };
    }

    const fundTx = TransferTransaction.create(
      Deadline.create(),
      clusterAcc.address,
      [new Mosaic(new MosaicId(xpxHex.toUpperCase()), UInt64.fromUint(500_000))],
      PlainMessage.create('CLOB Level-2 Validator Cluster Bootstrap'),
      config.NETWORK_TYPE,
      UInt64.fromUint(100000)
    );
    const signedFund = sponsor.sign(fundTx, config.GENERATION_HASH);
    await testnetClient.announceSignedTransaction(signedFund);
    await testnetClient.verifyTransactionAccepted(signedFund.hash, 8000);
    for (let i = 0; i < 30; i++) {
      await new Promise((r) => setTimeout(r, 2500));
      try {
        const st = await testnetClient.fetchJson(`${testnetClient.getActiveNodeUrl()}/transactionStatus/${signedFund.hash}`, 3000);
        if (st && st.group === 'confirmed') break;
      } catch (_) {}
    }
  }

  const cosignerModifications = cfg.nodes.map(
    (n) =>
      new MultisigCosignatoryModification(
        MultisigCosignatoryModificationType.Add,
        PublicAccount.createFromPublicKey(n.publicKey, config.NETWORK_TYPE)
      )
  );

  const modTx = ModifyMultisigAccountTransaction.create(
    Deadline.create(),
    cfg.minApproval,
    cfg.minRemoval,
    cosignerModifications,
    config.NETWORK_TYPE
  ).toAggregate(clusterAcc.publicAccount);
  modTx.version.signatureDScheme = 0;

  const aggTx = AggregateTransaction.createCompleteV1(
    Deadline.create(),
    [modTx],
    config.NETWORK_TYPE,
    [],
    UInt64.fromUint(250000)
  );

  // clusterAcc itself is inner signer & outer initiator (100% eligible cosigner!)
  const signedByCluster = clusterAcc.sign(aggTx, config.GENERATION_HASH);

  // Gather cosignatures from all 3 validator nodes
  const cosigs = [];
  for (const n of cfg.nodes) {
    const nodeAcc = getOrInitNodeValidatorAccount(n.name);
    cosigs.push(
      CosignatureTransaction.signTransactionPayload(nodeAcc, signedByCluster.payload, config.GENERATION_HASH)
    );
  }

  const combined = BuilderAgg.appendSignaturesV1(signedByCluster, cosigs);
  await testnetClient.announceSignedTransaction(combined);
  await testnetClient.verifyTransactionAccepted(combined.hash, 8000);

  cfg.activationTxHash = combined.hash;
  try {
    fs.writeFileSync(VALIDATOR_CLUSTER_FILE, JSON.stringify(cfg, null, 2), { mode: 0o600 });
  } catch (_) {}

  console.log(`[TradingCosigner] 🚀 Level-2 Validator Cluster (${clusterAddr}) 2-of-3 Multisig broadcast: ${combined.hash}`);

  if (waitForConfirmation) {
    for (let i = 0; i < 30; i++) {
      await new Promise((r) => setTimeout(r, 2500));
      try {
        const info = await testnetClient.fetchJson(`${testnetClient.getActiveNodeUrl()}/account/${clusterAddr}/multisig`, 3000);
        if (info && info.multisig && info.multisig.minApproval >= 2) {
          cfg.isMultisigConfirmedOnChain = true;
          delete cfg.bootstrapPrivateKey;
          try {
            fs.writeFileSync(VALIDATOR_CLUSTER_FILE, JSON.stringify(cfg, null, 2), { mode: 0o600 });
          } catch (_) {}
          return {
            ok: true,
            alreadyMultisig: false,
            isMultisigConfirmedOnChain: true,
            txHash: combined.hash,
            clusterAddress: clusterAddr,
            clusterPublicKey: cfg.clusterPublicKey,
          };
        }
      } catch (_) {}
    }
  }

  return {
    ok: true,
    alreadyMultisig: false,
    isMultisigConfirmedOnChain: false,
    txHash: combined.hash,
    clusterAddress: clusterAddr,
    clusterPublicKey: cfg.clusterPublicKey,
  };
}

function getValidatorClusterInfo() {
  const cfg = getOrInitValidatorClusterConfig();
  return {
    ok: true,
    model: 'HIERARCHICAL_MULTISIG_MODEL_A',
    description: 'Level-1 2-of-2 (User + ValidatorCluster) -> Level-2 2-of-3 BFT (Distributed Validator Servers)',
    network: config.NETWORK_NAME,
    networkType: config.NETWORK_TYPE,
    generationHash: config.GENERATION_HASH,
    clusterPublicKey: cfg.clusterPublicKey,
    clusterAddress: cfg.clusterAddress,
    level1MinApproval: 2,
    level2MinApproval: cfg.minApproval || VALIDATOR_QUORUM,
    level2TotalNodes: cfg.nodes.length,
    isMultisigConfirmedOnChain: Boolean(cfg.isMultisigConfirmedOnChain),
    nodes: cfg.nodes.map((n) => ({
      name: n.name,
      publicKey: n.publicKey,
      address: n.address || Address.createFromPublicKey(n.publicKey, config.NETWORK_TYPE).plain(),
    })),
  };
}

function verifyCosignatureObject(cosig, payloadHex, expectedPubKeyHex = null) {
  if (!cosig || !cosig.signer || !cosig.signature) {
    throw new Error('Malformed cosignature object: missing signer or signature');
  }
  const signerHex = String(cosig.signer).toUpperCase();
  if (expectedPubKeyHex && signerHex !== expectedPubKeyHex.toUpperCase()) {
    throw new Error(`Cosignature signer mismatch: expected ${expectedPubKeyHex}, got ${signerHex}`);
  }
  const expectedHashHex = VerifiableTransaction.createTransactionHash(
    payloadHex,
    Array.from(Buffer.from(config.GENERATION_HASH, 'hex'))
  ).toUpperCase();

  if (cosig.parentHash && cosig.parentHash.toUpperCase() !== expectedHashHex) {
    throw new Error(`Cosignature parentHash mismatch: expected ${expectedHashHex}, got ${cosig.parentHash}`);
  }

  const valid = KeyPair.verify(
    Buffer.from(signerHex, 'hex'),
    Buffer.from(expectedHashHex, 'hex'),
    Buffer.from(cosig.signature, 'hex'),
    DerivationScheme.Ed25519Sha3
  );
  if (!valid) {
    throw new Error(`Invalid Ed25519 cosignature from ${signerHex}`);
  }
  return {
    parentHash: expectedHashHex,
    signer: signerHex,
    signature: String(cosig.signature).toUpperCase(),
  };
}

/**
 * Verify the outer signature of an AggregateCompleteV1 payload signed by initiator.
 */
function verifyOuterInitiatorSignature(payloadHex) {
  if (!payloadHex || typeof payloadHex !== 'string' || payloadHex.length < 240) {
    throw new Error('Malformed aggregate payload hex');
  }
  const txHashHex = VerifiableTransaction.createTransactionHash(
    payloadHex,
    Array.from(Buffer.from(config.GENERATION_HASH, 'hex'))
  ).toUpperCase();

  const signatureHex = payloadHex.slice(8, 136);
  const initiatorPubHex = payloadHex.slice(136, 200).toUpperCase();
  const bodyHex = payloadHex.slice(200);
  const signingBytes = Buffer.from(config.GENERATION_HASH + bodyHex, 'hex');

  const valid = KeyPair.verify(
    Buffer.from(initiatorPubHex, 'hex'),
    signingBytes,
    Buffer.from(signatureHex, 'hex'),
    DerivationScheme.Ed25519Sha3
  );
  if (!valid) {
    throw new Error(`Invalid outer initiator Ed25519 signature for ${initiatorPubHex}`);
  }
  return { txHashHex, initiatorPubHex };
}

/**
 * Fan out CosignTransaction RPC across all validator servers in parallel and require >= 2-of-3 quorum.
 */
async function requestBftNodeCosignatures({
  action,
  aggregatePayloadHex,
  callerPublicKey = '',
  tradingAccountPublicKey = '',
  metadata = {},
}) {
  const cfg = getOrInitValidatorClusterConfig();
  const trustedNodePks = new Set(cfg.nodes.map((n) => n.publicKey.toUpperCase()));
  const initiatorPubHex = aggregatePayloadHex.slice(136, 200).toUpperCase();

  const req = {
    action,
    aggregate_payload_hex: aggregatePayloadHex,
    generation_hash: config.GENERATION_HASH,
    caller_public_key: String(callerPublicKey || '').toUpperCase(),
    trading_account_public_key: String(tradingAccountPublicKey || '').toUpperCase(),
    validator_cluster_public_key: cfg.clusterPublicKey.toUpperCase(),
    metadata_json: JSON.stringify(metadata || {}),
  };

  const replies = await Promise.all(NODES.map((n) => cosignOnSingleNode(n, req)));
  const approvedCosigs = [];
  const seenSigners = new Set([initiatorPubHex]);
  const errors = [];

  for (const r of replies) {
    if (!r || !r.approved) {
      errors.push(`${r ? r.node_name : 'node'}: ${r ? r.error : 'no response'}`);
      continue;
    }
    const pk = String(r.signer_public_key || '').toUpperCase();
    if (!trustedNodePks.has(pk)) {
      errors.push(`${r.node_name}: untrusted validator signer ${pk}`);
      continue;
    }
    if (seenSigners.has(pk)) {
      continue;
    }
    try {
      const verified = verifyCosignatureObject(
        { parentHash: r.parent_hash, signer: pk, signature: r.signature },
        aggregatePayloadHex,
        pk
      );
      seenSigners.add(pk);
      approvedCosigs.push({
        nodeName: r.node_name,
        ...verified,
      });
    } catch (vErr) {
      errors.push(`${r.node_name}: ${vErr.message}`);
    }
  }

  if (approvedCosigs.length < VALIDATOR_QUORUM) {
    throw new Error(
      `BFT Validator Cosign quorum not met (${approvedCosigs.length}/${VALIDATOR_QUORUM} required). Reasons: ${errors.join('; ')}`
    );
  }

  // Return strictly VALIDATOR_QUORUM signatures (or all unique valid signatures)
  return approvedCosigs.slice(0, VALIDATOR_QUORUM);
}

/**
 * Deduplicate cosignatures while excluding the outer initiator signer (Catapult rejects redundant cosigs).
 */
function combineAndDedupeCosignatures(payloadHex, initiatorPubHex, cosigsList) {
  const seen = new Set([String(initiatorPubHex).toUpperCase()]);
  const out = [];
  for (const c of cosigsList) {
    if (!c || !c.signer) continue;
    const pk = String(c.signer).toUpperCase();
    if (seen.has(pk)) continue;
    seen.add(pk);
    out.push({
      parentHash: c.parentHash,
      signer: pk,
      signature: String(c.signature).toUpperCase(),
    });
  }
  return out;
}

/**
 * Discover active/pending Trading Account for an owner, including direct on-chain L1 multisig graph discovery!
 */
async function discoverTradingAccountStatus(ownerPublicKeyOrAddress, existingLocks = null) {
  let cfg = getOrInitValidatorClusterConfig();
  if (!cfg.isMultisigConfirmedOnChain && Date.now() - lastClusterProbeAt > 15000) {
    lastClusterProbeAt = Date.now();
    try {
      await ensureValidatorClusterOnChain(false);
      cfg = getOrInitValidatorClusterConfig();
    } catch (_) {}
  }
  let ownerPublicKey = null;
  let ownerAddress = null;

  const rawInput = String(ownerPublicKeyOrAddress || '').replace(/[- ]/g, '').trim();
  if (/^[0-9a-fA-F]{64}$/.test(rawInput)) {
    ownerPublicKey = rawInput.toLowerCase();
    ownerAddress = Address.createFromPublicKey(ownerPublicKey, config.NETWORK_TYPE).plain();
  } else if (rawInput) {
    ownerAddress = rawInput.toUpperCase();
  }

  let record = clobDb.getTradingAccountByOwner(ownerPublicKey || ownerAddress);
  if (!record) {
    record = clobDb.getTradingAccountByTradingKey(ownerPublicKey || ownerAddress);
  }

  // If record is 'pending', check if confirmed on-chain
  if (record && record.status === 'pending') {
    try {
      const msInfo = await testnetClient.fetchJson(
        `${testnetClient.getActiveNodeUrl()}/account/${record.trading_address}/multisig`,
        3000
      );
      if (msInfo && msInfo.multisig && msInfo.multisig.minApproval >= 2) {
        clobDb.updateTradingAccountStatus(record.trading_public_key, 'active', record.activation_tx_hash);
        record = clobDb.getTradingAccountByTradingKey(record.trading_public_key);
      }
    } catch (_) {}
  }

  // On-Chain L1 Self-Discovery (works both when queried by Main Owner OR directly by Trading Account!)
  if (!record && ownerAddress) {
    try {
      const nodeUrl = testnetClient.getActiveNodeUrl();
      const inputMs = await testnetClient.fetchJson(`${nodeUrl}/account/${ownerAddress}/multisig`, 3500);
      const msObj = inputMs && inputMs.multisig;
      const inputCosigs = ((msObj && msObj.cosignatories) || []).map((c) => String(c).toUpperCase());

      // Case 1: Queried directly with the 2-of-2 Trading Account address/publicKey
      if (msObj && msObj.minApproval === 2 && inputCosigs.includes(cfg.clusterPublicKey.toUpperCase())) {
        const realOwnerPk = inputCosigs.find((pk) => pk !== cfg.clusterPublicKey.toUpperCase());
        const tradingPk = (msObj.account && String(msObj.account).length === 64 ? String(msObj.account) : ownerPublicKey) || '';
        if (realOwnerPk && tradingPk) {
          const realOwnerAddr = Address.createFromPublicKey(realOwnerPk, config.NETWORK_TYPE).plain();
          record = clobDb.saveTradingAccount({
            tradingPublicKey: tradingPk.toLowerCase(),
            tradingAddress: ownerAddress,
            ownerPublicKey: realOwnerPk.toLowerCase(),
            ownerAddress: realOwnerAddr,
            validatorClusterPublicKey: cfg.clusterPublicKey,
            validatorClusterAddress: cfg.clusterAddress,
            minApproval: 2,
            status: 'active',
          });
        }
      }

      // Case 2: Queried with Main Owner address/publicKey -> inspect its multisigAccounts
      if (!record) {
        const multisigAccounts = (msObj && msObj.multisigAccounts) || [];
        for (const candPub of multisigAccounts) {
          const candAddr = Address.createFromPublicKey(candPub, config.NETWORK_TYPE).plain();
          try {
            const candMs = await testnetClient.fetchJson(`${nodeUrl}/account/${candAddr}/multisig`, 3500);
            const cosigs = ((candMs && candMs.multisig && candMs.multisig.cosignatories) || []).map((c) =>
              String(c).toUpperCase()
            );
            if (
              candMs.multisig.minApproval === 2 &&
              cosigs.includes(cfg.clusterPublicKey.toUpperCase())
            ) {
              const otherPk = cosigs.find((pk) => pk !== cfg.clusterPublicKey.toUpperCase()) || ownerPublicKey;
              record = clobDb.saveTradingAccount({
                tradingPublicKey: candPub.toLowerCase(),
                tradingAddress: candAddr,
                ownerPublicKey: otherPk ? otherPk.toLowerCase() : ownerPublicKey,
                ownerAddress,
                validatorClusterPublicKey: cfg.clusterPublicKey,
                validatorClusterAddress: cfg.clusterAddress,
                minApproval: 2,
                status: 'active',
              });
              break;
            }
          } catch (_) {}
        }
      }
    } catch (_) {}
  }

  if (!record) {
    return {
      ok: true,
      hasTradingAccount: false,
      status: 'none',
      validatorCluster: getValidatorClusterInfo(),
    };
  }

  // Fetch live on-chain balance of the Trading Account
  const effectiveOwnerPk = record.owner_public_key;
  const balances = await computeTradingAccountBalances(record, existingLocks);
  const pendingSettlements = clobDb.getPendingSettlementsForOwner(effectiveOwnerPk);
  const pendingWithdrawals = clobDb.getPendingTradingWithdrawals(effectiveOwnerPk);

  return {
    ok: true,
    hasTradingAccount: true,
    status: record.status,
    tradingAccount: {
      tradingPublicKey: record.trading_public_key,
      tradingAddress: record.trading_address,
      ownerPublicKey: record.owner_public_key,
      ownerAddress: record.owner_address,
      validatorClusterPublicKey: record.validator_cluster_public_key,
      validatorClusterAddress: record.validator_cluster_address,
      minApproval: record.min_approval,
      activationTxHash: record.activation_tx_hash,
    },
    balances,
    pendingSettlementsCount: pendingSettlements.filter((s) => s.status === 'pending_cosign').length,
    pendingWithdrawalsCount: pendingWithdrawals.length,
    validatorCluster: getValidatorClusterInfo(),
  };
}

/**
 * Compute authoritative atomic balances for a Smart Trading Account:
 *   - onChain = Live Sirius L1 mosaics inside T_user
 *   - pendingSettlementDelta = Sum of unsettled trade deltas (status in pending_cosign/ready/broadcast)
 *   - pendingWithdrawalDelta = Sum of in-flight withdrawal debits (status in pending/broadcast)
 *   - effective = onChain + pendingSettlementDelta - pendingWithdrawalDelta
 *   - tradeable = effective - lockedInOrders
 *   - withdrawable = onChain + min(0, pendingSettlementDelta) - pendingWithdrawalDelta - lockedInOrders
 */
async function computeTradingAccountBalances(record, lockedOverrides = null, forceOnChainRefresh = false) {
  const mosaics = await testnetClient.getAccountOnChainBalances(record.trading_address, forceOnChainRefresh);
  const xpxHex = (config.XPX_MOSAIC_ID || '13BFC518E40549D7').toLowerCase();
  const usdtHex = (config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toLowerCase();
  const bnbHex = (config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toLowerCase();

  const onChainXpx = mosaics.get(xpxHex) || 0n;
  const onChainUsdt = mosaics.get(usdtHex) || 0n;
  const onChainBnb = mosaics.get(bnbHex) || 0n;

  const ownerPk = record.owner_public_key.toLowerCase();
  const settlements = clobDb.getPendingSettlementsForOwner(ownerPk);
  let deltaXpx = 0n;
  let deltaUsdt = 0n;
  let deltaBnb = 0n;

  for (const s of settlements) {
    if (s.status === 'confirmed' || s.status === 'failed') continue;
    const d = s.delta || {};
    deltaXpx += BigInt(d.xpx || '0');
    deltaUsdt += BigInt(d.usdt || '0');
    deltaBnb += BigInt(d.bnb || '0');
  }

  const withdrawals = clobDb.getPendingTradingWithdrawals(ownerPk);
  let inFlightWithdrawXpx = 0n;
  let inFlightWithdrawUsdt = 0n;
  let inFlightWithdrawBnb = 0n;

  for (const w of withdrawals) {
    const amt = BigInt(w.amount_atomic || '0');
    const mh = String(w.mosaic_hex || '').toLowerCase();
    if (mh === xpxHex || w.mosaic === 'XPX') inFlightWithdrawXpx += amt;
    else if (mh === usdtHex || w.mosaic === 'USDT') inFlightWithdrawUsdt += amt;
    else if (mh === bnbHex || w.mosaic === 'BNB') inFlightWithdrawBnb += amt;
  }

  const dbBal = clobDb.getDbAccountBalance(ownerPk);
  const lockedXpx = BigInt(lockedOverrides ? (lockedOverrides.lockedXpx || 0n) : (dbBal ? dbBal.lockedXpx : 0n));
  const lockedUsdt = BigInt(lockedOverrides ? (lockedOverrides.lockedUsdt || 0n) : (dbBal ? dbBal.lockedUsdt : 0n));
  const lockedBnb = BigInt(lockedOverrides ? (lockedOverrides.lockedBnb || 0n) : (dbBal ? dbBal.lockedBnb : 0n));

  const negDeltaXpx = deltaXpx < 0n ? deltaXpx : 0n;
  const negDeltaUsdt = deltaUsdt < 0n ? deltaUsdt : 0n;
  const negDeltaBnb = deltaBnb < 0n ? deltaBnb : 0n;

  // Never allow unconfirmed positive settlement credits (pending_cosign/ready/broadcast) to back
  // new trades or withdrawals before Sirius L1 confirms; negative liabilities ALWAYS deduct immediately.
  const effectiveTotalXpx = onChainXpx + negDeltaXpx - inFlightWithdrawXpx;
  const effectiveTotalUsdt = onChainUsdt + negDeltaUsdt - inFlightWithdrawUsdt;
  const effectiveTotalBnb = onChainBnb + negDeltaBnb - inFlightWithdrawBnb;

  const tradeableXpx = effectiveTotalXpx > lockedXpx ? effectiveTotalXpx - lockedXpx : 0n;
  const tradeableUsdt = effectiveTotalUsdt > lockedUsdt ? effectiveTotalUsdt - lockedUsdt : 0n;
  const tradeableBnb = effectiveTotalBnb > lockedBnb ? effectiveTotalBnb - lockedBnb : 0n;

  const withdrawableRawXpx = effectiveTotalXpx - lockedXpx;
  const withdrawableRawUsdt = effectiveTotalUsdt - lockedUsdt;
  const withdrawableRawBnb = effectiveTotalBnb - lockedBnb;

  return {
    onChain: {
      xpx: onChainXpx.toString(),
      usdt: onChainUsdt.toString(),
      bnb: onChainBnb.toString(),
    },
    pendingDelta: {
      xpx: deltaXpx.toString(),
      usdt: deltaUsdt.toString(),
      bnb: deltaBnb.toString(),
    },
    inFlightWithdraw: {
      xpx: inFlightWithdrawXpx.toString(),
      usdt: inFlightWithdrawUsdt.toString(),
      bnb: inFlightWithdrawBnb.toString(),
    },
    locked: {
      xpx: lockedXpx.toString(),
      usdt: lockedUsdt.toString(),
      bnb: lockedBnb.toString(),
    },
    tradeable: {
      xpx: tradeableXpx.toString(),
      usdt: tradeableUsdt.toString(),
      bnb: tradeableBnb.toString(),
    },
    withdrawable: {
      xpx: (withdrawableRawXpx > 0n ? withdrawableRawXpx : 0n).toString(),
      usdt: (withdrawableRawUsdt > 0n ? withdrawableRawUsdt : 0n).toString(),
      bnb: (withdrawableRawBnb > 0n ? withdrawableRawBnb : 0n).toString(),
    },
  };
}

/**
 * Activate a User's Native 2-of-2 Co-Signed Smart Trading Account on Sirius L1.
 */
async function activateTradingAccount(reqBody) {
  const {
    signedByInitiatorPayload,
    tradingAccountCosig,
    ownerCosig = null,
    ownerPublicKey,
    tradingAccountPublicKey,
  } = reqBody || {};

  if (!signedByInitiatorPayload || !tradingAccountCosig || !ownerPublicKey || !tradingAccountPublicKey) {
    throw new Error('Missing required fields: signedByInitiatorPayload, tradingAccountCosig, ownerPublicKey, tradingAccountPublicKey');
  }

  const existingStatus = await discoverTradingAccountStatus(ownerPublicKey);
  if (
    existingStatus &&
    existingStatus.hasTradingAccount &&
    existingStatus.tradingAccount &&
    (existingStatus.status === 'active' || existingStatus.status === 'pending')
  ) {
    throw new Error(
      `Account already has an ${existingStatus.status} Smart Trading Account (${existingStatus.tradingAccount.tradingAddress}).`
    );
  }

  // Ensure Level 2 Validator Cluster is ready on-chain first
  const clusterBoot = await ensureValidatorClusterOnChain(true);
  if (!clusterBoot.ok) {
    return {
      ok: false,
      needsFunding: Boolean(clusterBoot.needsFunding),
      clusterAddress: clusterBoot.clusterAddress,
      requiredAtomic: clusterBoot.requiredAtomic || '500000',
      error: clusterBoot.message || 'Level-2 Validator Cluster is not yet ready on-chain.',
    };
  }
  const cfg = getOrInitValidatorClusterConfig();

  const { txHashHex, initiatorPubHex } = verifyOuterInitiatorSignature(signedByInitiatorPayload);

  // Decode & inspect inner ModifyMultisigAccountTransaction
  const decoded = TransactionMapping.createFromPayload(signedByInitiatorPayload);
  if (decoded.type !== TransactionType.AGGREGATE_COMPLETE_V1) {
    throw new Error(`Expected AGGREGATE_COMPLETE_V1, got type ${decoded.type}`);
  }
  if (!Array.isArray(decoded.innerTransactions) || decoded.innerTransactions.length !== 1) {
    throw new Error('Activation aggregate must contain strictly 1 ModifyMultisigAccountTransaction');
  }
  const inner = decoded.innerTransactions[0];
  if (inner.type !== TransactionType.MODIFY_MULTISIG_ACCOUNT) {
    throw new Error('Inner transaction must be MODIFY_MULTISIG_ACCOUNT');
  }
  if (!inner.signer || inner.signer.publicKey.toUpperCase() !== tradingAccountPublicKey.toUpperCase()) {
    throw new Error('ModifyMultisigAccountTransaction signer must be tradingAccountPublicKey');
  }
  if (inner.minApprovalDelta !== 2 || inner.minRemovalDelta !== 2) {
    throw new Error(`Invalid thresholds: expected 2-of-2 (got ${inner.minApprovalDelta}/${inner.minRemovalDelta})`);
  }

  const modPks = (inner.modifications || []).map((m) => {
    if (m.type !== 0) throw new Error('Only Add modifications allowed during Trading Account activation');
    return m.cosignatoryPublicAccount.publicKey.toUpperCase();
  });
  if (
    modPks.length !== 2 ||
    !modPks.includes(ownerPublicKey.toUpperCase()) ||
    !modPks.includes(cfg.clusterPublicKey.toUpperCase())
  ) {
    throw new Error('Trading Account cosignatories must strictly match [ownerPublicKey, validatorClusterPublicKey]');
  }

  // Verify tradingAccountCosig and optional ownerCosig
  const verifiedTradingCosig = verifyCosignatureObject(
    tradingAccountCosig,
    signedByInitiatorPayload,
    tradingAccountPublicKey
  );

  const clientCosigs = [verifiedTradingCosig];
  if (initiatorPubHex !== ownerPublicKey.toUpperCase()) {
    if (!ownerCosig) {
      throw new Error('ownerCosig required when initiator is not ownerPublicKey');
    }
    clientCosigs.push(verifyCosignatureObject(ownerCosig, signedByInitiatorPayload, ownerPublicKey));
  }

  // Collect 2-of-3 BFT cosignatures across independent Validator Servers over gRPC
  const validatorCosigs = await requestBftNodeCosignatures({
    action: 'ACTIVATE_TRADING_ACCOUNT',
    aggregatePayloadHex: signedByInitiatorPayload,
    callerPublicKey: ownerPublicKey,
    tradingAccountPublicKey,
    metadata: {
      ownerPublicKey,
      validatorClusterPublicKey: cfg.clusterPublicKey,
    },
  });

  const finalCosigs = combineAndDedupeCosignatures(signedByInitiatorPayload, initiatorPubHex, [
    ...clientCosigs,
    ...validatorCosigs,
  ]);

  const completedSigned = BuilderAgg.appendSignaturesV1(
    { payload: signedByInitiatorPayload, hash: txHashHex },
    finalCosigs
  );

  await testnetClient.announceSignedTransaction(completedSigned);
  await testnetClient.verifyTransactionAccepted(completedSigned.hash, 8000);

  const tradingAddress = Address.createFromPublicKey(tradingAccountPublicKey, config.NETWORK_TYPE).plain();
  const ownerAddress = Address.createFromPublicKey(ownerPublicKey, config.NETWORK_TYPE).plain();

  const saved = clobDb.saveTradingAccount({
    tradingPublicKey: tradingAccountPublicKey.toLowerCase(),
    tradingAddress,
    ownerPublicKey: ownerPublicKey.toLowerCase(),
    ownerAddress,
    validatorClusterPublicKey: cfg.clusterPublicKey.toLowerCase(),
    validatorClusterAddress: cfg.clusterAddress,
    minApproval: 2,
    activationTxHash: completedSigned.hash,
    status: 'pending',
  });

  return {
    ok: true,
    txHash: completedSigned.hash,
    status: 'pending',
    tradingAccount: saved,
    approvedByNodes: validatorCosigs.map((v) => v.nodeName),
  };
}

/**
 * Co-sign a user-initiated non-custodial withdrawal from their 2-of-2 Trading Account on Sirius L1.
 */
async function cosignWithdrawal(reqBody, getLocksForOwnerFn = null) {
  const {
    signedByInitiatorPayload,
    callerPublicKey,
    tradingAccountPublicKey,
    recipientAddress,
    ownerCosig = null,
  } = reqBody || {};

  if (!signedByInitiatorPayload || !callerPublicKey) {
    throw new Error('Missing signedByInitiatorPayload or callerPublicKey');
  }

  const record = clobDb.getTradingAccountByOwner(callerPublicKey);
  if (!record || record.status !== 'active') {
    throw new Error('No active Smart Trading Account found for callerPublicKey');
  }
  if (
    tradingAccountPublicKey &&
    tradingAccountPublicKey.toLowerCase() !== record.trading_public_key.toLowerCase()
  ) {
    throw new Error('tradingAccountPublicKey mismatch with registered Trading Account');
  }

  const { txHashHex, initiatorPubHex } = verifyOuterInitiatorSignature(signedByInitiatorPayload);

  // Verify Cosigner 1 (Owner) authorization: either initiator OR explicit valid cosignature
  const extraCosigs = [];
  if (initiatorPubHex !== record.owner_public_key.toUpperCase()) {
    if (!ownerCosig) {
      throw new Error('Withdrawal must be initiated or cosigned by Trading Account ownerPublicKey');
    }
    extraCosigs.push(verifyCosignatureObject(ownerCosig, signedByInitiatorPayload, record.owner_public_key));
  }

  // Inspect all inner transfers
  const decoded = TransactionMapping.createFromPayload(signedByInitiatorPayload);
  if (decoded.type !== TransactionType.AGGREGATE_COMPLETE_V1) {
    throw new Error(`Expected AGGREGATE_COMPLETE_V1, got ${decoded.type}`);
  }
  if (!Array.isArray(decoded.innerTransactions) || decoded.innerTransactions.length === 0) {
    throw new Error('Withdrawal aggregate has no inner transactions');
  }

  const xpxHex = (config.XPX_MOSAIC_ID || '13BFC518E40549D7').toUpperCase();
  const usdtHex = (config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toUpperCase();
  const bnbHex = (config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toUpperCase();

  let outXpx = 0n;
  let outUsdt = 0n;
  let outBnb = 0n;
  let primaryRecipient = recipientAddress ? String(recipientAddress).replace(/[- ]/g, '').toUpperCase() : '';
  let primaryMosaicHex = '';

  for (const inner of decoded.innerTransactions) {
    if (inner.type !== TransactionType.TRANSFER) {
      throw new Error(`Security rejection: only TRANSFER allowed in withdrawal aggregate (got ${inner.type})`);
    }
    if (!inner.signer || inner.signer.publicKey.toUpperCase() !== record.trading_public_key.toUpperCase()) {
      throw new Error('Security rejection: transfer signer must be the user Trading Account');
    }
    const recipObj = inner.recipient || inner.recipientAddress;
    const recipPlain = recipObj && typeof recipObj.plain === 'function' ? recipObj.plain().replace(/[- ]/g, '').toUpperCase() : '';
    if (!primaryRecipient) primaryRecipient = recipPlain;
    if (primaryRecipient && recipPlain !== primaryRecipient) {
      throw new Error(`Security rejection: transfer recipient ${recipPlain} != ${primaryRecipient}`);
    }
    for (const m of (inner.mosaics || [])) {
      const mHex = m.id.toHex().toUpperCase();
      const amt = BigInt(m.amount.compact ? m.amount.compact() : m.amount);
      if (amt <= 0n) throw new Error('Transfer mosaic amount must be > 0');
      if (mHex === xpxHex) { outXpx += amt; primaryMosaicHex = primaryMosaicHex || xpxHex; }
      else if (mHex === usdtHex) { outUsdt += amt; primaryMosaicHex = primaryMosaicHex || usdtHex; }
      else if (mHex === bnbHex) { outBnb += amt; primaryMosaicHex = primaryMosaicHex || bnbHex; }
      else throw new Error(`Unsupported mosaic ${mHex} in Trading Account withdrawal`);
    }
  }

  const lockedOverrides = typeof getLocksForOwnerFn === 'function' ? getLocksForOwnerFn(record.owner_public_key) : null;
  const balState = await computeTradingAccountBalances(record, lockedOverrides, true);

  const availXpx = BigInt(balState.withdrawable.xpx);
  const availUsdt = BigInt(balState.withdrawable.usdt);
  const availBnb = BigInt(balState.withdrawable.bnb);

  if (outXpx > availXpx) {
    throw new Error(`Insufficient withdrawable XPX: requested ${outXpx}, free available ${availXpx}`);
  }
  if (outUsdt > availUsdt) {
    throw new Error(`Insufficient withdrawable USDT: requested ${outUsdt}, free available ${availUsdt}`);
  }
  if (outBnb > availBnb) {
    throw new Error(`Insufficient withdrawable BNB: requested ${outBnb}, free available ${availBnb}`);
  }

  // Build full per-mosaic metadata for independent BFT server checks (supports single & multi-mosaic withdrawals)
  const mosaicChecks = [];
  if (outXpx > 0n) {
    mosaicChecks.push({
      symbol: 'XPX',
      mosaicHex: xpxHex,
      mosaicIdDecimal: BigInt('0x0DC67FBE1CAD29E3').toString(10),
      requestedAmountAtomic: outXpx.toString(),
      maxWithdrawableAtomic: availXpx.toString(),
      onChainBalanceAtomic: balState.onChain.xpx,
    });
  }
  if (outUsdt > 0n) {
    mosaicChecks.push({
      symbol: 'USDT',
      mosaicHex: usdtHex,
      mosaicIdDecimal: BigInt('0x10112CBA1E7B9037').toString(10),
      requestedAmountAtomic: outUsdt.toString(),
      maxWithdrawableAtomic: availUsdt.toString(),
      onChainBalanceAtomic: balState.onChain.usdt,
    });
  }
  if (outBnb > 0n) {
    mosaicChecks.push({
      symbol: 'BNB',
      mosaicHex: bnbHex,
      mosaicIdDecimal: BigInt('0x1E5C5FADEC7B34DD').toString(10),
      requestedAmountAtomic: outBnb.toString(),
      maxWithdrawableAtomic: availBnb.toString(),
      onChainBalanceAtomic: balState.onChain.bnb,
    });
  }

  const primaryCheck = mosaicChecks[0];
  if (!primaryCheck) {
    throw new Error('Withdrawal must include at least one supported mosaic (XPX, USDT, or BNB)');
  }

  // Fan out to 3 Distributed Validator Servers (require 2-of-3 quorum)
  const validatorCosigs = await requestBftNodeCosignatures({
    action: 'WITHDRAWAL',
    aggregatePayloadHex: signedByInitiatorPayload,
    callerPublicKey: record.owner_public_key,
    tradingAccountPublicKey: record.trading_public_key,
    metadata: {
      recipientAddress: primaryRecipient,
      mosaicHex: primaryCheck.mosaicHex,
      requestedAmountAtomic: primaryCheck.requestedAmountAtomic,
      maxWithdrawableAtomic: primaryCheck.maxWithdrawableAtomic,
      onChainBalanceAtomic: primaryCheck.onChainBalanceAtomic,
      mosaicIdDecimal: primaryCheck.mosaicIdDecimal,
      mosaics: mosaicChecks,
    },
  });

  const finalCosigs = combineAndDedupeCosignatures(signedByInitiatorPayload, initiatorPubHex, [
    ...extraCosigs,
    ...validatorCosigs,
  ]);
  const completedSigned = BuilderAgg.appendSignaturesV1(
    { payload: signedByInitiatorPayload, hash: txHashHex },
    finalCosigs
  );

  // Record BEFORE L1 broadcast so concurrent mempool double-withdrawals are blocked immediately!
  const withdrawalId = `TW_${txHashHex.slice(0, 16)}_${Date.now()}`;
  const assetsToRecord = [];
  if (outXpx > 0n) assetsToRecord.push({ sym: 'XPX', hex: xpxHex, amt: outXpx });
  if (outUsdt > 0n) assetsToRecord.push({ sym: 'USDT', hex: usdtHex, amt: outUsdt });
  if (outBnb > 0n) assetsToRecord.push({ sym: 'BNB', hex: bnbHex, amt: outBnb });

  for (const a of assetsToRecord) {
    clobDb.saveTradingWithdrawal({
      withdrawalId: assetsToRecord.length === 1 ? withdrawalId : `${withdrawalId}_${a.sym}`,
      ownerPublicKey: record.owner_public_key,
      tradingPublicKey: record.trading_public_key,
      mosaic: a.sym,
      mosaicHex: a.hex,
      amountAtomic: a.amt.toString(),
      recipientAddress: primaryRecipient,
      txHash: completedSigned.hash,
      status: 'broadcast',
    });
  }

  try {
    await testnetClient.announceSignedTransaction(completedSigned);
    await testnetClient.verifyTransactionAccepted(completedSigned.hash, 8000);
  } catch (err) {
    for (const a of assetsToRecord) {
      const id = assetsToRecord.length === 1 ? withdrawalId : `${withdrawalId}_${a.sym}`;
      clobDb.updateTradingWithdrawalStatus(id, 'failed', completedSigned.hash);
    }
    throw err;
  }

  const updatedBalances = await computeTradingAccountBalances(record, lockedOverrides, false);

  return {
    ok: true,
    txHash: completedSigned.hash,
    withdrawalId,
    approvedByNodes: validatorCosigs.map((v) => v.nodeName),
    balances: updatedBalances,
  };
}

/**
 * Create an Atomic Trade Settlement AggregateCompleteV1 between a User's Trading Account
 * and the Counterparty (another Trading Account or Clearing Pool/Vault).
 */
async function createTradeSettlement({
  orderId,
  ownerPublicKey,
  counterpartyKey,
  transfers, // [{ fromTradingPub, fromType: 'TRADING'|'VAULT', toAddress, mosaicHex, amountAtomic, symbol }]
  delta,     // { xpx: '...', usdt: '...', bnb: '...' } net change for owner
  orderContext = null,
}) {
  const record = clobDb.getTradingAccountByOwner(ownerPublicKey);
  if (!record || record.status !== 'active') return null;

  const vault = testnetClient.getOrInitVaultAccount();
  const embeddedTxs = [];
  const requiredUserSigners = new Set();
  const allowedInnerSigners = new Set();

  for (const tr of transfers) {
    const amt = BigInt(tr.amountAtomic || '0');
    if (amt <= 0n) continue;
    const fromPub = tr.fromType === 'VAULT' ? vault.publicKey : tr.fromTradingPub;
    allowedInnerSigners.add(fromPub.toUpperCase());
    if (tr.fromType !== 'VAULT' && tr.ownerPublicKey) {
      requiredUserSigners.add(tr.ownerPublicKey.toUpperCase());
    }
    const mosaic = new Mosaic(
      new MosaicId(tr.mosaicHex.toUpperCase()),
      UInt64.fromHex(amt.toString(16).padStart(16, '0'))
    );
    const innerTx = TransferTransaction.create(
      Deadline.create(),
      Address.createFromRawAddress(tr.toAddress),
      [mosaic],
      PlainMessage.create(`CLOB_P2P_SETTLE:${orderId || 'MATCH'}`.slice(0, 120)),
      config.NETWORK_TYPE
    ).toAggregate(PublicAccount.createFromPublicKey(fromPub, config.NETWORK_TYPE));
    innerTx.version.signatureDScheme = 0;
    embeddedTxs.push(innerTx);
  }

  if (embeddedTxs.length === 0) return null;

  const feeUnits = 120000 + embeddedTxs.length * 40000;
  const aggTx = AggregateTransaction.createCompleteV1(
    Deadline.create(),
    embeddedTxs,
    config.NETWORK_TYPE,
    [],
    UInt64.fromUint(feeUnits)
  );

  // Vault initiates and pays the tiny L1 settlement fee (satisfies Vault leg if Vault is sending!)
  const signedByVault = vault.sign(aggTx, config.GENERATION_HASH);
  const settlementId = `SET_${signedByVault.hash.slice(0, 16)}_${crypto.randomBytes(3).toString('hex')}`;

  const saved = clobDb.saveTradingSettlement({
    settlementId,
    orderId: orderId ? String(orderId) : null,
    ownerPublicKey: record.owner_public_key,
    tradingPublicKey: record.trading_public_key,
    counterpartyKey: counterpartyKey || null,
    txHash: signedByVault.hash,
    aggregatePayload: signedByVault.payload,
    requiredCosigners: Array.from(requiredUserSigners),
    collectedCosigs: [],
    transfers,
    delta,
    orderContext,
    status: requiredUserSigners.size === 0 ? 'ready' : 'pending_cosign',
  });

  return saved;
}

/**
 * Submit User Cosigner-1 signature for a pending atomic trade settlement;
 * once all required user cosignatures are collected, requests 2-of-3 Validator Server cosigs and broadcasts!
 */
async function submitSettlementCosignature({ settlementId, cosignature }) {
  const s = clobDb.getTradingSettlement(settlementId);
  if (!s) throw new Error(`Settlement ${settlementId} not found`);
  if (s.status === 'broadcast' || s.status === 'confirmed') {
    return { ok: true, alreadySubmitted: true, txHash: s.txHash, status: s.status };
  }

  const verified = verifyCosignatureObject(cosignature, s.aggregatePayload);
  const reqSet = new Set(s.requiredCosigners.map((pk) => pk.toUpperCase()));
  if (!reqSet.has(verified.signer.toUpperCase())) {
    throw new Error(`Signer ${verified.signer} is not a required cosigner for settlement ${settlementId}`);
  }

  const existing = Array.isArray(s.collectedCosigs) ? [...s.collectedCosigs] : [];
  if (!existing.some((c) => c.signer.toUpperCase() === verified.signer.toUpperCase())) {
    existing.push(verified);
  }

  const collectedSet = new Set(existing.map((c) => c.signer.toUpperCase()));
  const allUsersSigned = Array.from(reqSet).every((pk) => collectedSet.has(pk));

  if (!allUsersSigned) {
    clobDb.updateTradingSettlementStatus(settlementId, 'pending_cosign', s.txHash, existing);
    return { ok: true, status: 'pending_cosign', txHash: s.txHash, collected: existing.length, required: reqSet.size };
  }

  // All users signed -> gather 2-of-3 BFT Validator Server cosignatures over gRPC!
  const allowedSigners = Array.from(
    new Set(
      (s.transfers || [])
        .map((t) => (t.fromType === 'VAULT' ? testnetClient.getOrInitVaultAccount().publicKey : t.fromTradingPub))
        .filter(Boolean)
    )
  );

  const validatorCosigs = await requestBftNodeCosignatures({
    action: 'SETTLEMENT',
    aggregatePayloadHex: s.aggregatePayload,
    callerPublicKey: s.ownerPublicKey,
    tradingAccountPublicKey: s.tradingPublicKey,
    metadata: {
      settlementId,
      allowedSigners,
      orderContext: s.orderContext || null,
      feeCollectorAddress: config.FEE_COLLECTOR_ADDRESS || null,
    },
  });

  const initiatorPubHex = s.aggregatePayload.slice(136, 200).toUpperCase();
  const finalCosigs = combineAndDedupeCosignatures(s.aggregatePayload, initiatorPubHex, [
    ...existing,
    ...validatorCosigs,
  ]);

  const completedSigned = BuilderAgg.appendSignaturesV1(
    { payload: s.aggregatePayload, hash: s.txHash },
    finalCosigs
  );

  try {
    await testnetClient.announceSignedTransaction(completedSigned);
    await testnetClient.verifyTransactionAccepted(completedSigned.hash, 2500, 400);
  } catch (l1Err) {
    const errMsg = l1Err && l1Err.message ? l1Err.message : String(l1Err || '');
    if (errMsg.includes('Sirius node rejected transaction')) {
      clobDb.updateTradingSettlementStatus(settlementId, 'failed', completedSigned.hash, existing);
      const hasVaultLeg = Array.isArray(s.transfers) && s.transfers.some((t) => t.fromType === 'VAULT');
      if (hasVaultLeg && errMsg.includes('Insufficient_Balance')) {
        let vaultAddr = '';
        try { vaultAddr = testnetClient.getOrInitVaultAccount().address.plain(); } catch (_) {}
        throw new Error(
          `Market Maker Vault (${vaultAddr || 'clearing vault'}) lacks on-chain XPX/USDT liquidity on Sirius L1 to settle this MM fill (${errMsg}). Set SIRIUS_VAULT_PRIVATE_KEY on the backend or fund the Vault.`
        );
      }
    }
    throw l1Err;
  }

  clobDb.updateTradingSettlementStatus(settlementId, 'broadcast', completedSigned.hash, existing);

  return {
    ok: true,
    status: 'broadcast',
    txHash: completedSigned.hash,
    approvedByNodes: validatorCosigs.map((v) => v.nodeName),
  };
}

/**
 * Background Reconciler: promotes pending Trading Accounts, Withdrawals, and Settlements on L1 confirmation.
 */
async function reconcileInFlightL1Txs() {
  // 1. Pending Trading Account Activations
  try {
    const pendingAccs = clobDb.getTradingAccountsByStatus('pending');
    for (const acc of pendingAccs) {
      try {
        const ms = await testnetClient.fetchJson(
          `${testnetClient.getActiveNodeUrl()}/account/${acc.trading_address}/multisig`,
          3000
        );
        if (ms && ms.multisig && ms.multisig.minApproval >= 2) {
          clobDb.updateTradingAccountStatus(acc.trading_public_key, 'active', acc.activation_tx_hash);
          console.log(`[TradingCosigner] ✅ Trading Account confirmed active on L1: ${acc.trading_address}`);
        }
      } catch (_) {}
    }
  } catch (_) {}

  // 2. Broadcast Withdrawals
  try {
    const inFlightW = clobDb.getAllInFlightTradingWithdrawals();
    for (const w of inFlightW) {
      if (!w.tx_hash) continue;
      try {
        const st = await testnetClient.fetchJson(
          `${testnetClient.getActiveNodeUrl()}/transactionStatus/${w.tx_hash}`,
          3000
        );
        if (st && st.group === 'confirmed') {
          const h = Array.isArray(st.height) ? Number(st.height[0] || 0) : Number(st.height || 1);
          const rec = clobDb.getTradingAccountByTradingKey(w.trading_public_key);
          if (rec) testnetClient.invalidateAccountBalanceCache(rec.trading_address);
          clobDb.updateTradingWithdrawalStatus(w.withdrawal_id, 'confirmed', w.tx_hash, h || 1);
        } else if (st && st.group === 'failed') {
          clobDb.updateTradingWithdrawalStatus(w.withdrawal_id, 'failed', w.tx_hash);
        }
      } catch (_) {}
    }
  } catch (_) {}

  // 3. Broadcast Settlements
  try {
    const inFlightS = clobDb.getAllInFlightTradingSettlements();
    for (const s of inFlightS) {
      if (s.status === 'broadcast' && s.txHash) {
        try {
          const st = await testnetClient.fetchJson(
            `${testnetClient.getActiveNodeUrl()}/transactionStatus/${s.txHash}`,
            3000
          );
          if (st && st.group === 'confirmed') {
            const rec = clobDb.getTradingAccountByTradingKey(s.tradingPublicKey);
            if (rec) testnetClient.invalidateAccountBalanceCache(rec.trading_address);
            clobDb.updateTradingSettlementStatus(s.settlementId, 'confirmed', s.txHash, null);
          } else if (st && st.group === 'failed') {
            clobDb.updateTradingSettlementStatus(s.settlementId, 'failed', s.txHash, null);
          }
        } catch (_) {}
      }
    }
  } catch (_) {}
}

module.exports = {
  VALIDATOR_QUORUM,
  getOrInitValidatorClusterConfig,
  ensureValidatorClusterOnChain,
  getValidatorClusterInfo,
  discoverTradingAccountStatus,
  computeTradingAccountBalances,
  activateTradingAccount,
  cosignWithdrawal,
  createTradeSettlement,
  submitSettlementCosignature,
  requestBftNodeCosignatures,
  reconcileInFlightL1Txs,
  verifyOuterInitiatorSignature,
  verifyCosignatureObject,
};
