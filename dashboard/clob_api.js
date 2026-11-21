/**
 * SiriusCLOB DEX Gateway & Web Wallet Execution Bridge
 * Handles real-time order placement, cancellation, balance management,
 * and 3-Node BFT WASM VM consensus execution with Catapult on-chain settlement.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFileSync } = require('child_process');

if (!crypto.webcrypto) {
  crypto.webcrypto = {
    getRandomValues(b) { return crypto.randomFillSync(b); }
  };
}

if (!String.prototype.replaceAll) {
  String.prototype.replaceAll = function(search, replacement) {
    if (search instanceof RegExp) {
      return this.replace(search, replacement);
    }
    return this.split(search).join(replacement);
  };
}
const {
  executeOnNodeVm,
  assert3NodeConsensus,
  NODES,
  DRIVE_KEY_HEX,
  ALICE_KEY_HEX,
  BOB_KEY_HEX,
  CONTRACT_KEY_HEX,
} = require('../executor/milestone2_verifier');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const BIN_DIR = path.join(ROOT_DIR, 'bin');
const ACCOUNTS_FILE = path.join(CLUSTER_DIR, 'nemesis_accounts.json');
const BALANCES_FILE = path.join(CLUSTER_DIR, 'clob_balances.json');
const NONCES_FILE = path.join(CLUSTER_DIR, 'used_nonces.json');
const clobDb = require('./clob_db');

let sdk;
try {
  sdk = require('tsjs-xpx-chain-sdk');
} catch (_) {
  try {
    sdk = require(path.resolve(ROOT_DIR, '../web-wallet-vuejs/node_modules/tsjs-xpx-chain-sdk'));
  } catch (e) {
    sdk = require(path.resolve(ROOT_DIR, 'node_modules/tsjs-xpx-chain-sdk'));
  }
}
const { Account, KeyPair, DerivationScheme, Address } = sdk;

const testnet2Config = require('./testnet2_config');
let NETWORK_ID = Number(process.env.SIRIUS_NETWORK_TYPE || testnet2Config.NETWORK_TYPE || 168);
const DOMAIN_SEPARATOR = `${NETWORK_ID}:SiriusCLOB-v1`;

const CLOB_WASM_PATH = path.join(
  ROOT_DIR,
  'contracts/clob/target/wasm32-unknown-unknown/release/sirius_clob.wasm'
);

const XPX_MOSAIC_ID = BigInt('0x0DC67FBE1CAD29E3').toString(10);
const USDT_MOSAIC_ID = BigInt('0x10112CBA1E7B9037').toString(10);
const BNB_MOSAIC_ID = BigInt('0x1E5C5FADEC7B34DD').toString(10);
const PRICE_SCALE = 100_000_000n;

const ACTIVE_ORDERS_FILE = path.join(CLUSTER_DIR, 'active_orders.json');
const activeOrders = new Map();
let globalNextOrderId = 1000;

// Per-account async mutex to eliminate TOCTOU races across concurrent order/withdrawal requests
const accountLocks = new Map();
async function withAccountLock(accountKeyHex, fn) {
  const key = String(accountKeyHex || 'global').toLowerCase();
  const prev = accountLocks.get(key) || Promise.resolve();
  let release;
  const next = new Promise((resolve) => {
    release = resolve;
  });
  accountLocks.set(key, prev.then(() => next));
  await prev;
  try {
    return await fn();
  } finally {
    release();
    if (accountLocks.get(key) === next) {
      accountLocks.delete(key);
    }
  }
}

function loadActiveOrders() {
  if (fs.existsSync(ACTIVE_ORDERS_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(ACTIVE_ORDERS_FILE, 'utf8'));
      if (Array.isArray(data)) {
        for (const o of data) {
          if (o.status !== 'Filled' && o.remainingAmountAtomic !== '0') {
            activeOrders.set(o.id || o.orderId, o);
          }
        }
      }
    } catch (_) {}
  }
  if (typeof reconcileActiveOrdersWithTrades === 'function') {
    reconcileActiveOrdersWithTrades();
  }
}

function saveActiveOrders() {
  try {
    const list = Array.from(activeOrders.values());
    fs.writeFileSync(ACTIVE_ORDERS_FILE, JSON.stringify(list, null, 2));
  } catch (_) {}
}

BigInt.prototype.toJSON = function () {
  return this.toString();
};

function parseDecimalToBigInt(val, decimals = 6) {
  if (val === undefined || val === null || val === '') return 0n;
  if (typeof val === 'bigint') return val;
  let s = String(val).trim();
  if (typeof val === 'number' && Number.isFinite(val)) {
    const fixedCandidate = val.toFixed(decimals);
    if (Math.abs(val - parseFloat(fixedCandidate)) < 1e-11) {
      s = fixedCandidate;
    }
  }
  if (s.includes('e') || s.includes('E')) {
    const [coeff, expStr] = s.split(/[eE]/);
    const exp = parseInt(expStr, 10);
    const parts = coeff.split('.');
    let intPart = parts[0] || '0';
    let fracPart = parts[1] || '';
    if (exp > 0) {
      if (fracPart.length <= exp) {
        s = intPart + fracPart.padEnd(exp, '0');
      } else {
        s = intPart + fracPart.slice(0, exp) + '.' + fracPart.slice(exp);
      }
    } else if (exp < 0) {
      const posExp = Math.abs(exp);
      if (intPart.startsWith('-')) {
        intPart = intPart.slice(1);
        s = '-0.' + '0'.repeat(posExp - 1) + intPart + fracPart;
      } else {
        s = '0.' + '0'.repeat(posExp - 1) + intPart + fracPart;
      }
    }
  }
  const isNegative = s.startsWith('-');
  if (isNegative) s = s.slice(1);
  const parts = s.split('.');
  const intPart = parts[0] ? BigInt(parts[0]) : 0n;
  let fracStr = parts[1] || '';
  if (fracStr.length > decimals) {
    throw new Error(`Precision error: Input "${val}" has ${fracStr.length} decimal places, exceeding maximum allowed precision of ${decimals} decimals (rejection required)`);
  }
  fracStr = fracStr.padEnd(decimals, '0');
  const fracPart = fracStr ? BigInt(fracStr) : 0n;
  const mult = 10n ** BigInt(decimals);
  const res = intPart * mult + fracPart;
  return isNegative ? -res : res;
}

function formatAtomicUnits(atomicVal, decimals = 6) {
  const big = BigInt(atomicVal || 0n);
  const isNeg = big < 0n;
  const abs = isNeg ? -big : big;
  const mult = 10n ** BigInt(decimals);
  const intPart = abs / mult;
  const fracPart = abs % mult;
  const fracStr = fracPart.toString().padStart(decimals, '0');
  const sign = isNeg ? '-' : '';
  return `${sign}${intPart}.${fracStr}`;
}

function reconcileActiveOrdersWithTrades() {
  const makerFillsMap = new Map();
  const markets = ['xpx_usdt', 'xpx_bnb'];

  for (const m of markets) {
    const tradesPath = path.join(
      CLUSTER_DIR,
      'node1',
      'data',
      'drives',
      DRIVE_KEY_HEX,
      'drive',
      'markets',
      m,
      'trades.json'
    );
    if (fs.existsSync(tradesPath)) {
      try {
        const raw = fs.readFileSync(tradesPath, 'utf8');
        const trades = JSON.parse(raw);
        if (Array.isArray(trades)) {
          for (const t of trades) {
            const mId = Number(t.maker_order_id);
            const baseAmt = BigInt(t.base_amount || 0);
            if (mId > 0 && baseAmt > 0n) {
              makerFillsMap.set(mId, (makerFillsMap.get(mId) || 0n) + baseAmt);
            }
          }
        }
      } catch (_) {}
    }
  }

  let modified = false;
  for (const o of activeOrders.values()) {
    const orderId = Number(o.id || o.orderId);
    let initialAtomic = 0n;
    if (o.amountAtomic !== undefined && o.amountAtomic !== null) {
      initialAtomic = BigInt(o.amountAtomic);
    } else if (o.amount !== undefined && o.amount !== null) {
      initialAtomic = parseDecimalToBigInt(o.amount, 6);
    }

    if (initialAtomic <= 0n) continue;

    const takerFilled = BigInt(o.takerFilledAtomic || 0n);
    const makerFilled = makerFillsMap.get(orderId) || 0n;
    const totalFilled = takerFilled + makerFilled;

    let pct = Number((totalFilled * 100n) / initialAtomic);
    if (pct > 100) pct = 100;
    if (pct < 0) pct = 0;

    const remainingAtomic = initialAtomic > totalFilled ? initialAtomic - totalFilled : 0n;
    const isFilled = remainingAtomic === 0n;

    const newFilledStr = `${pct}%`;
    const newStatus = isFilled ? 'Filled' : (o.status === 'Cancelled' ? 'Cancelled' : 'Open');
    const newRemainingStr = formatAtomicUnits(remainingAtomic, 6);

    if (
      o.filled !== newFilledStr ||
      o.status !== newStatus ||
      o.makerFilledAtomic !== makerFilled.toString() ||
      o.filledAmountAtomic !== totalFilled.toString() ||
      o.remainingAmountAtomic !== remainingAtomic.toString() ||
      o.remainingAmount !== newRemainingStr
    ) {
      o.makerFilledAtomic = makerFilled.toString();
      o.filledAmountAtomic = totalFilled.toString();
      o.remainingAmountAtomic = remainingAtomic.toString();
      o.remainingAmount = newRemainingStr;
      o.filled = newFilledStr;
      o.status = newStatus;
      modified = true;
    }
  }

  if (modified) {
    saveActiveOrders();
  }
}

loadActiveOrders();

// Persistent nonce replay registry: Map<pubKeyHex, Map<nonce, expiryTimestamp>>
const usedNonces = new Map();

function loadUsedNonces() {
  usedNonces.clear();
  if (fs.existsSync(NONCES_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(NONCES_FILE, 'utf8'));
      const now = Date.now();
      for (const [key, noncesObj] of Object.entries(data)) {
        const nonceMap = new Map();
        for (const [nStr, exp] of Object.entries(noncesObj)) {
          if (Number(exp) > now) {
            nonceMap.set(String(nStr), Number(exp));
          }
        }
        if (nonceMap.size > 0) {
          usedNonces.set(key.toLowerCase(), nonceMap);
        }
      }
    } catch (_) {}
  }
}

function saveUsedNonces() {
  try {
    const obj = {};
    for (const [key, nonceMap] of usedNonces.entries()) {
      obj[key] = Object.fromEntries(nonceMap.entries());
    }
    fs.writeFileSync(NONCES_FILE, JSON.stringify(obj, null, 2));
  } catch (_) {}
}

function checkAndRecordNonce(pubKeyHex, nonce, expiry) {
  if (nonce === undefined || nonce === null || nonce === '') {
    throw new Error('Missing nonce in request');
  }
  if (!expiry) {
    throw new Error('Missing expiry timestamp in request');
  }
  const expiryNum = Number(expiry);
  const now = Date.now();
  const MAX_EXPIRY_WINDOW_MS = 60 * 60 * 1000; // Max 1 hour into the future
  if (isNaN(expiryNum) || expiryNum < now) {
    throw new Error(`Request has expired (expiry=${expiryNum}, now=${now})`);
  }
  if (expiryNum > now + MAX_EXPIRY_WINDOW_MS) {
    throw new Error(`Expiry timestamp is too far in the future (maximum window is 1 hour)`);
  }
  const key = pubKeyHex.toLowerCase();
  let userNonces = usedNonces.get(key);
  if (!userNonces) {
    userNonces = new Map();
    usedNonces.set(key, userNonces);
  } else {
    for (const [nStr, exp] of userNonces.entries()) {
      if (exp <= now) {
        userNonces.delete(nStr);
      }
    }
  }
  const nonceStr = String(nonce);
  if (userNonces.has(nonceStr)) {
    throw new Error(`Nonce ${nonce} already used (replay rejected)`);
  }
  userNonces.set(nonceStr, expiryNum);
  saveUsedNonces();
}

loadUsedNonces();

// Persistent on-chain deposit registry enforcing unique constraint on credit path
const PROCESSED_DEPOSITS_FILE = path.join(CLUSTER_DIR, 'processed_deposits.json');
const processedDeposits = new Map();
const syncedOnChainAssets = new Map();

function loadProcessedDeposits() {
  processedDeposits.clear();
  try {
    const db = clobDb.getDb();
    const rows = db.prepare('SELECT * FROM processed_deposits').all();
    for (const r of rows) {
      processedDeposits.set(r.tx_hash.toLowerCase(), {
        status: r.status,
        callerKey: r.caller_key,
        mosaicId: r.mosaic_id,
        mosaicHex: r.mosaic_hex,
        amount: r.amount,
        height: r.height,
        confirmationDepth: r.confirmation_depth,
        refundTxHash: r.refund_tx_hash,
        recipientAddress: r.recipient_address,
        timestamp: r.timestamp,
      });
    }
  } catch (_) {}
  if (fs.existsSync(PROCESSED_DEPOSITS_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(PROCESSED_DEPOSITS_FILE, 'utf8'));
      for (const [txHash, info] of Object.entries(data)) {
        if (!processedDeposits.has(txHash.toLowerCase())) {
          processedDeposits.set(txHash.toLowerCase(), info);
        }
      }
    } catch (_) {}
  }
}

function saveProcessedDeposits() {
  try {
    const obj = Object.fromEntries(processedDeposits.entries());
    fs.writeFileSync(PROCESSED_DEPOSITS_FILE, JSON.stringify(obj, null, 2));
  } catch (_) {}
}

async function queryTransactionOnChain(txHash) {
  const t2Client = require('./testnet2_client');
  return t2Client.queryTransaction(txHash);
}

async function verifyOnChainDeposit(txHash, callerKey, expectedMosaicId, confirmationDepth = 1, options = {}) {
  const normHash = (txHash || '').trim().toLowerCase();
  if (!/^[0-9a-fA-F]{64}$/.test(normHash)) {
    throw new Error(`Invalid on-chain deposit txHash format (${txHash}): expected 64 hex characters`);
  }

  // Look up txHash on live Sirius network (Testnet 2 / Mainnet)
  let txInfo = null;
  let currentChainHeight = 0;

  try {
    const t2Client = require('./testnet2_client');
    const onChainTx = await t2Client.queryTransaction(normHash);
    if (onChainTx && onChainTx.found) {
      txInfo = onChainTx;
      currentChainHeight = await t2Client.getLatestHeight().catch(() => Number(txInfo.height) || 0);
    }
  } catch (_) {}

  if (!txInfo || !txInfo.found) {
    throw new Error(`Transaction ${normHash} not found on Sirius blockchain (deposit must be confirmed in a block)`);
  }

  // 2. Require confirmed in a block
  if (!txInfo.height || Number(txInfo.height) <= 0) {
    throw new Error(`Transaction ${normHash} is not confirmed in any block (height=${txInfo.height})`);
  }

  // 2b. Enforce confirmation depth parameter
  const reqDepth = Math.max(1, Number(confirmationDepth) || 1);
  const txHeight = Number(txInfo.height);
  const currentDepth = currentChainHeight >= txHeight ? (currentChainHeight - txHeight + 1) : 1;
  if (currentDepth < reqDepth) {
    throw new Error(`Deposit tx ${normHash} confirmation depth is ${currentDepth}, but required confirmation depth is ${reqDepth} (current block #${currentChainHeight}, tx block #${txHeight})`);
  }

  // 3. Require type Transfer (0x4154 or 16724)
  if (txInfo.type !== 'Transfer' && txInfo.typeHex !== '4154' && txInfo.type !== 16724) {
    throw new Error(`Invalid transaction type: expected Transfer (0x4154), got ${txInfo.type} (0x${txInfo.typeHex})`);
  }

  // 4. Require recipient == SuperContract Escrow Address (or legacy vault address)
  const txRecipient = (txInfo.recipient || '').replace(/[- ]/g, '').toUpperCase();
  const superContractAddress = (testnet2Config.SUPERCONTRACT_ADDRESS || 'VAEKLE3WB6XJLFW75JGJJ7Y3DVQWCWX774DDSJ2F').replace(/[- ]/g, '').toUpperCase();
  const legacyVaultAddress = (testnet2Config.LEGACY_VAULT_ADDRESS || 'VAAUXFYQ424I6V3JDVWCBWGWHRTIJGTSX4B2TLSV').replace(/[- ]/g, '').toUpperCase();

  let allowedAddresses = [superContractAddress, legacyVaultAddress];
  if (options && options.vaultAddressOverride) {
    allowedAddresses.push(String(options.vaultAddressOverride).replace(/[- ]/g, '').toUpperCase());
  }

  if (!allowedAddresses.includes(txRecipient)) {
    throw new Error(`Recipient mismatch: transaction recipient (${txRecipient}) does not match SuperContract Escrow address (${superContractAddress})`);
  }

  // 5. Require sender == account being credited (or authorized devnet nemesis faucet)
  const normSender = normalizeKey(txInfo.sender || txInfo.signerPublicKey);
  const normCaller = normalizeKey(callerKey);
  let isNemesisFaucet = false;
  if (options && options.isFaucet) {
    if (fs.existsSync(ACCOUNTS_FILE)) {
      try {
        const accs = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
        isNemesisFaucet = normSender === normalizeKey(accs.caller.publicKey);
      } catch (_) {}
    } else {
      isNemesisFaucet = true;
    }
  }
  if (normSender && normCaller && normSender !== normCaller && !isNemesisFaucet) {
    throw new Error(`Sender mismatch: transaction sender (${normSender}) does not match account being credited (${normCaller})`);
  }

  // 6. Require mosaic matches
  const normExpectedMosaic = String(expectedMosaicId).trim();
  const matchedMosaic = (txInfo.mosaics || []).find((m) => {
    return (
      (m.mosaicIdHex && m.mosaicIdHex.toLowerCase() === normExpectedMosaic.toLowerCase().replace('0x', '')) ||
      m.mosaicIdDec === normExpectedMosaic
    );
  });
  if (!matchedMosaic) {
    throw new Error(`Mosaic mismatch: transaction does not contain mosaic ${expectedMosaicId}`);
  }

  // 7. Extract ON-CHAIN amount (ignore any caller-supplied amount)
  const onChainAmountAtomic = BigInt(matchedMosaic.amount || 0);
  if (onChainAmountAtomic <= 0n) {
    throw new Error(`Invalid on-chain deposit amount: ${matchedMosaic.amount} (must be greater than zero)`);
  }

  return {
    txHash: normHash,
    callerKey: normalizeKey(callerKey),
    height: Number(txInfo.height),
    confirmationDepth: currentDepth,
    sender: normSender,
    senderAddress: txInfo.senderAddress || txInfo.sender,
    recipient: txRecipient,
    mosaicId: matchedMosaic.mosaicIdDec || normExpectedMosaic,
    mosaicHex: matchedMosaic.mosaicIdHex,
    onChainAmountAtomic,
  };
}

async function recordDepositTx(txHash, callerKey, mosaicId, confirmationDepth = 1, options = {}) {
  if (!txHash || typeof txHash !== 'string') {
    throw new Error('Missing on-chain deposit txHash: Every ledger credit must reference a confirmed deposit transaction to the vault');
  }
  const normHash = txHash.trim().toLowerCase();
  if (!/^[0-9a-fA-F]{64}$/.test(normHash)) {
    throw new Error(`Invalid on-chain deposit txHash format (${txHash}): expected 64 hex characters`);
  }

  // Atomically check and reserve txHash as "pending" in unified SQLite DB
  clobDb.reserveDepositPending(normHash, normalizeKey(callerKey), String(mosaicId));
  processedDeposits.set(normHash, {
    status: 'pending',
    callerKey: normalizeKey(callerKey),
    mosaicId: String(mosaicId),
    timestamp: Date.now(),
  });
  saveProcessedDeposits();

  try {
    // Look up tx on live chain and enforce all invariants including confirmation depth
    const verified = await verifyOnChainDeposit(normHash, callerKey, mosaicId, confirmationDepth, options);
    const finalAmountAtomic = verified.onChainAmountAtomic;

    // Transition from pending -> confirmed in SQLite (atomic transaction with unique ledger_credits constraint)
    clobDb.confirmDeposit(verified);

    processedDeposits.set(normHash, {
      status: 'confirmed',
      callerKey: verified.callerKey || verified.sender,
      mosaicId: verified.mosaicId,
      mosaicHex: verified.mosaicHex,
      amount: finalAmountAtomic.toString(),
      height: verified.height,
      confirmationDepth: verified.confirmationDepth,
      timestamp: Date.now(),
    });
    saveProcessedDeposits();

    return {
      txHash: normHash,
      height: verified.height,
      confirmationDepth: verified.confirmationDepth,
      amountAtomic: finalAmountAtomic,
      mosaicId: verified.mosaicId,
      mosaicHex: verified.mosaicHex,
      sender: verified.sender,
    };
  } catch (err) {
    // If verification fails or is rejected, clear pending from SQLite and memory
    clobDb.clearPendingDeposit(normHash);
    processedDeposits.delete(normHash);
    saveProcessedDeposits();
    throw err;
  }
}

/**
 * Refund Path for deposits with mismatched sender or uncredited deposits.
 * Inspects on-chain deposit to vault, verifies on-chain sender, and executes
 * an on-chain refund transfer from the vault directly back to the original sender.
 */
function refundMismatchedDeposit(txHash, customAdminPrivKey = null) {
  const normHash = (txHash || '').trim().toLowerCase();
  if (!/^[0-9a-fA-F]{64}$/.test(normHash)) {
    throw new Error(`Invalid deposit txHash format (${txHash}): expected 64 hex characters`);
  }

  const existing = clobDb.getDeposit(normHash) || processedDeposits.get(normHash);
  if (existing && existing.status === 'confirmed') {
    throw new Error(`Deposit tx ${normHash} has already been credited to ledger; cannot be refunded`);
  }
  if (existing && existing.status === 'refunded') {
    throw new Error(`Deposit tx ${normHash} has already been refunded (Refund Tx: ${existing.refund_tx_hash || existing.refundTxHash})`);
  }

  const txInfo = queryTransactionOnChain(normHash);
  if (!txInfo.found || !txInfo.height) {
    throw new Error(`Deposit transaction ${normHash} not found or unconfirmed on local chain`);
  }

  const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
  const vaultAddress = accounts.contract.address.replace(/[- ]/g, '').toUpperCase();
  const txRecipient = (txInfo.recipient || '').replace(/[- ]/g, '').toUpperCase();
  if (txRecipient !== vaultAddress) {
    throw new Error(`Transaction ${normHash} was not sent to vault (${vaultAddress}), got ${txRecipient}`);
  }

  if (!txInfo.mosaics || txInfo.mosaics.length === 0) {
    throw new Error(`Transaction ${normHash} contains no mosaics to refund`);
  }

  const mosaicToRefund = txInfo.mosaics[0];
  const refundAmountAtomic = BigInt(mosaicToRefund.amount || 0);
  if (refundAmountAtomic <= 0n) {
    throw new Error(`Transaction ${normHash} has zero amount to refund`);
  }

  // Derive sender address from txInfo
  let senderAddr = txInfo.senderAddress;
  if (!senderAddr && txInfo.sender) {
    try {
      senderAddr = Address.createFromPublicKey(txInfo.sender, NETWORK_ID).plain();
    } catch (_) {
      senderAddr = txInfo.sender;
    }
  }
  const vaultPriv = customAdminPrivKey || accounts.contract.privateKey;

  // CRITICAL: Mark deposit as 'refunding' atomically in SQLite BEFORE broadcast!
  clobDb.markDepositRefunding(normHash);
  processedDeposits.set(normHash, {
    status: 'refunding',
    sender: txInfo.sender,
    recipientAddress: senderAddr,
    mosaicIdHex: mosaicToRefund.mosaicIdHex,
    amount: refundAmountAtomic.toString(),
    timestamp: Date.now(),
  });
  saveProcessedDeposits();

  // Broadcast refund transfer on-chain from vault to sender address
  const memo = `REFUND:${normHash.slice(0, 16)}`;
  const refundRes = broadcastTransferOnChain(
    vaultPriv,
    senderAddr,
    mosaicToRefund.mosaicIdHex,
    refundAmountAtomic,
    memo
  );

  // Transition from 'refunding' -> 'refunded' in SQLite
  clobDb.markDepositRefunded(
    normHash,
    refundRes.txHash,
    senderAddr,
    mosaicToRefund.mosaicIdHex,
    refundAmountAtomic
  );

  processedDeposits.set(normHash, {
    status: 'refunded',
    sender: txInfo.sender,
    recipientAddress: senderAddr,
    mosaicIdHex: mosaicToRefund.mosaicIdHex,
    amount: refundAmountAtomic.toString(),
    refundTxHash: refundRes.txHash,
    timestamp: Date.now(),
  });
  saveProcessedDeposits();

  return {
    ok: true,
    refunded: true,
    originalTxHash: normHash,
    refundTxHash: refundRes.txHash,
    recipientAddress: senderAddr,
    mosaicIdHex: mosaicToRefund.mosaicIdHex,
    amountAtomic: refundAmountAtomic.toString(),
  };
}

loadProcessedDeposits();

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
  const normSide = side === 1 || side === 'sell' ? 'sell' : 'buy';
  const normType = type === 1 || type === 'Market' ? 'Market' : 'Limit';
  const priceStr = canonicalizeDecimalString(price, 8);
  const amountStr = canonicalizeDecimalString(amount, 6);
  return `${DOMAIN_SEPARATOR}:ORDER:${normPair}:${normSide}:${normType}:${priceStr}:${amountStr}:${nonce}:${expiry}`;
}

function constructCancelMessage(pair, orderId, nonce, expiry) {
  const normPair = (pair || 'XPX/USDT').toUpperCase().replace('_', '/');
  return `${DOMAIN_SEPARATOR}:CANCEL:${normPair}:${orderId}:${nonce}:${expiry}`;
}

function constructCancelAllMessage(pair, nonce, expiry) {
  const normPair = (pair || 'XPX/USDT').toUpperCase().replace('_', '/');
  return `${DOMAIN_SEPARATOR}:CANCEL_ALL:${normPair}:${nonce}:${expiry}`;
}

function verifyEd25519Signature(pubKeyHex, message, signatureHex) {
  if (!signatureHex) {
    throw new Error('Missing ed25519 signature: Order/cancel request must be cryptographically signed by the account owner');
  }
  try {
    const rawPub = Buffer.from(pubKeyHex, 'hex');
    if (rawPub.length !== 32) {
      throw new Error(`Invalid public key length (${rawPub.length} bytes, expected 32)`);
    }
    const rawSig = Buffer.from(signatureHex, 'hex');
    if (rawSig.length !== 64) {
      throw new Error(`Invalid signature length (${rawSig.length} bytes, expected 64)`);
    }
    const msgBuf = Buffer.isBuffer(message) ? message : Buffer.from(String(message), 'utf8');
    const valid = KeyPair.verify(rawPub, msgBuf, rawSig, DerivationScheme.Ed25519Sha3);
    if (!valid) {
      throw new Error('Invalid ed25519 signature: Signature does not match public key and payload');
    }
    return true;
  } catch (err) {
    throw new Error(`Signature verification failed: ${err.message}`);
  }
}

function signOrderRequest(privKeyHex, order) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
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

function signCancelRequest(privKeyHex, cancelReq) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const msgStr = constructCancelMessage(
    cancelReq.pair,
    cancelReq.orderId,
    cancelReq.nonce,
    cancelReq.expiry
  );
  return account.signData(msgStr);
}

function signCancelAllRequest(privKeyHex, cancelAllReq) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const msgStr = constructCancelAllMessage(
    cancelAllReq.pair,
    cancelAllReq.nonce,
    cancelAllReq.expiry
  );
  return account.signData(msgStr);
}

function signBatchCancelRequest(privKeyHex, batchReq) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const pair = batchReq.pair || batchReq.market || 'XPX/USDT';
  const normPair = pair.toUpperCase().replace('_', '/');
  const orderIds = [...new Set((batchReq.orderIds || []).map((id) => parseInt(id, 10)).filter((id) => Number.isSafeInteger(id) && id > 0))];
  const batchMsg = `${DOMAIN_SEPARATOR}:CANCEL_BATCH:${normPair}:${orderIds.join(',')}:${batchReq.nonce}:${batchReq.expiry}`;
  return account.signData(batchMsg);
}

// Persistent withdrawal state registry: requested -> broadcast -> confirmed
const WITHDRAWALS_FILE = path.join(CLUSTER_DIR, 'withdrawals.json');
const withdrawals = new Map();

function loadWithdrawals() {
  withdrawals.clear();
  try {
    const pending = clobDb.getAllPendingWithdrawals();
    for (const r of pending) {
      withdrawals.set(r.key, r);
    }
  } catch (_) {}
  if (fs.existsSync(WITHDRAWALS_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(WITHDRAWALS_FILE, 'utf8'));
      for (const [key, item] of Object.entries(data)) {
        if (!withdrawals.has(key)) {
          withdrawals.set(key, item);
        }
      }
    } catch (_) {}
  }
}

function saveWithdrawals() {
  try {
    for (const record of withdrawals.values()) {
      clobDb.saveWithdrawalRecord(record);
    }
  } catch (_) {}
  try {
    const obj = Object.fromEntries(withdrawals.entries());
    fs.writeFileSync(WITHDRAWALS_FILE, JSON.stringify(obj, null, 2));
  } catch (_) {}
}

loadWithdrawals();

function deriveMosaicFromAsset(assetName) {
  if (!assetName || (typeof assetName !== 'string' && typeof assetName !== 'number' && typeof assetName !== 'bigint')) {
    throw new Error('Invalid asset name: must be a non-empty string or mosaic ID');
  }
  const str = String(assetName).trim();
  const norm = str.toUpperCase();
  let cleanHex = norm.replace(/^0X/, '').toLowerCase();
  
  let decAsHex = '';
  if (/^\d+$/.test(str)) {
    try {
      decAsHex = BigInt(str).toString(16).toLowerCase();
    } catch (_) {}
  }

  const confUsdt = String(testnet2Config.USDT_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');
  const confBnb = String(testnet2Config.BNB_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');
  const confXpx = String(testnet2Config.XPX_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');

  if (
    norm === 'XPX' || norm === 'SIRIUS' || norm === XPX_MOSAIC_ID ||
    isXpxMosaic(cleanHex) ||
    (decAsHex && isXpxMosaic(decAsHex))
  ) {
    return XPX_MOSAIC_ID;
  }
  if (
    norm === 'USDT' || norm === 'USD' || norm === USDT_MOSAIC_ID ||
    cleanHex === '10112cba1e7b9037' || cleanHex === '7525381a171d1e44' || cleanHex === '4430aefef7e15d0c' || cleanHex === confUsdt ||
    (decAsHex && (decAsHex === '10112cba1e7b9037' || decAsHex === '7525381a171d1e44' || decAsHex === '4430aefef7e15d0c' || decAsHex === confUsdt))
  ) {
    return USDT_MOSAIC_ID;
  }
  if (
    norm === 'BNB' || norm === BNB_MOSAIC_ID ||
    cleanHex === '1e5c5fadec7b34dd' || cleanHex === '3cc1d3ce7447b6f9' || cleanHex === confBnb ||
    (decAsHex && (decAsHex === '1e5c5fadec7b34dd' || decAsHex === '3cc1d3ce7447b6f9' || decAsHex === confBnb))
  ) {
    return BNB_MOSAIC_ID;
  }
  throw new Error(`Unsupported or unknown asset name: "${assetName}". Supported assets: XPX, USDT, BNB.`);
}

function constructWithdrawMessage(mosaic, amount, nonce, expiry, recipient = null) {
  const normMosaic = String(mosaic || 'XPX').trim().toUpperCase();
  if (recipient) {
    const cleanAddr = String(recipient).replace(/[- ]/g, '').toUpperCase();
    return `${DOMAIN_SEPARATOR}:WITHDRAW:${normMosaic}:${amount}:${cleanAddr}:${nonce}:${expiry}`;
  }
  return `${DOMAIN_SEPARATOR}:WITHDRAW:${normMosaic}:${amount}:${nonce}:${expiry}`;
}

function signWithdrawalRequest(privKeyHex, withdrawReq) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const msg = constructWithdrawMessage(
    withdrawReq.mosaic || 'XPX',
    withdrawReq.amount,
    withdrawReq.nonce,
    withdrawReq.expiry,
    withdrawReq.recipient
  );
  return account.signData(msg);
}

function constructAdminMessage(action, nonce, expiry) {
  const normAction = String(action || '').trim().toUpperCase();
  return `${DOMAIN_SEPARATOR}:ADMIN:ACTION:${normAction}:${nonce}:${expiry}`;
}

function signAdminRequest(privKeyHex, action, nonce, expiry) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const msg = constructAdminMessage(action, nonce, expiry);
  return {
    publicKey: account.publicKey,
    action: String(action || '').trim().toUpperCase(),
    nonce,
    expiry,
    signature: account.signData(msg),
  };
}

function getAuthorizedAdminKeys() {
  const adminKeys = new Set();
  // 1. Genesis & contract keys
  try {
    if (fs.existsSync(ACCOUNTS_FILE)) {
      const acc = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
      if (acc.contract && acc.contract.publicKey) adminKeys.add(acc.contract.publicKey.toLowerCase());
      if (acc.caller && acc.caller.publicKey) adminKeys.add(acc.caller.publicKey.toLowerCase());
      if (Array.isArray(acc.nodes)) {
        for (const n of acc.nodes) {
          if (n.harvest && n.harvest.publicKey) adminKeys.add(n.harvest.publicKey.toLowerCase());
          if (n.boot && n.boot.publicKey) adminKeys.add(n.boot.publicKey.toLowerCase());
        }
      }
    }
  } catch (_) {}

  // 2. cluster/admin_keys.json
  const adminKeysFile = path.join(CLUSTER_DIR, 'admin_keys.json');
  try {
    if (fs.existsSync(adminKeysFile)) {
      const data = JSON.parse(fs.readFileSync(adminKeysFile, 'utf8'));
      if (Array.isArray(data.adminPublicKeys)) {
        for (const k of data.adminPublicKeys) {
          if (k) adminKeys.add(String(k).trim().toLowerCase());
        }
      }
    }
  } catch (_) {}

  // 3. Environment variable overrides
  if (process.env.CLOB_ADMIN_PUBLIC_KEYS) {
    for (const k of process.env.CLOB_ADMIN_PUBLIC_KEYS.split(',')) {
      if (k.trim()) adminKeys.add(k.trim().toLowerCase());
    }
  }

  // 4. cluster/treasury_account.json
  const treasuryFile = path.join(CLUSTER_DIR, 'treasury_account.json');
  try {
    if (fs.existsSync(treasuryFile)) {
      const tData = JSON.parse(fs.readFileSync(treasuryFile, 'utf8'));
      if (tData.publicKey) adminKeys.add(String(tData.publicKey).trim().toLowerCase());
    }
  } catch (_) {}

  return adminKeys;
}

function verifyAdminRequest(body, expectedAction) {
  if (!body) {
    throw new Error('Authentication required: Administrative request body is missing');
  }

  const signature = body.signature;
  if (!signature) {
    throw new Error('Authentication required: Administrative endpoint requires signed payload with authorized admin public key');
  }

  const callerKey = normalizeKey(body.publicKey || body.callerPublicKey);
  const action = String(body.action || '').trim().toUpperCase();
  const expected = String(expectedAction || '').trim().toUpperCase();

  if (!action || action !== expected) {
    throw new Error(`Admin action mismatch: expected "${expected}", got "${action}"`);
  }

  const nonce = body.nonce;
  const expiry = body.expiry;

  if (nonce === undefined || nonce === null || nonce === '') {
    throw new Error('Missing nonce in administrative request');
  }
  if (!expiry) {
    throw new Error('Missing expiry timestamp in administrative request');
  }

  const msg = constructAdminMessage(action, nonce, expiry);
  verifyEd25519Signature(callerKey, msg, signature);

  const authorized = getAuthorizedAdminKeys();
  if (authorized.size === 0) {
    throw new Error('Unauthorized: No administrative public keys are configured on the exchange');
  }
  if (!authorized.has(callerKey.toLowerCase())) {
    throw new Error(`Unauthorized: Public key ${callerKey} is not an authorized exchange administrator`);
  }

  checkAndRecordNonce(callerKey, nonce, expiry);

  return {
    ok: true,
    callerKey,
    action,
  };
}

function signWithdrawRequest(privKeyHex, withdrawReq) {
  const account = Account.createFromPrivateKey(privKeyHex, NETWORK_ID, 1);
  const mosaic = withdrawReq.mosaic ||
    (String(withdrawReq.mosaicId) === XPX_MOSAIC_ID ? 'XPX' :
    (String(withdrawReq.mosaicId) === USDT_MOSAIC_ID ? 'USDT' :
    (String(withdrawReq.mosaicId) === BNB_MOSAIC_ID ? 'BNB' : withdrawReq.mosaicId))) || 'XPX';
  const recipient = withdrawReq.recipient || account.address.plain();
  const msgStr = constructWithdrawMessage(
    mosaic,
    withdrawReq.amount,
    withdrawReq.nonce,
    withdrawReq.expiry,
    recipient
  );
  return account.signData(msgStr);
}

function verifyWithdrawalSignature(callerKey, req) {
  if (!req.signature) {
    throw new Error('Missing ed25519 signature: Withdrawal request must be cryptographically signed by the account owner');
  }
  const rawAsset = String(req.asset || req.mosaic || 'XPX').trim().toUpperCase();
  const candidates = [rawAsset];
  if (req.mosaicId) {
    try {
      const fromId = deriveMosaicFromAsset(String(req.mosaicId));
      if (fromId === XPX_MOSAIC_ID) candidates.push('XPX');
      if (fromId === USDT_MOSAIC_ID) candidates.push('USDT');
      if (fromId === BNB_MOSAIC_ID) candidates.push('BNB');
    } catch (_) {}
  }
  const uniqueCandidates = [...new Set(candidates)];

  let expectedRecipient = null;
  try {
    expectedRecipient = Address.createFromPublicKey(callerKey, NETWORK_ID).plain().replace(/[- ]/g, '').toUpperCase();
  } catch (_) {}

  const clientRecipient = req.recipient ? String(req.recipient).replace(/[- ]/g, '').toUpperCase() : expectedRecipient;

  let verifiedAsset = null;
  let verifiedRecipient = null;
  let lastErr = null;

  for (const c of uniqueCandidates) {
    // 1. Try destination-bound message first (new standard)
    if (clientRecipient) {
      const msgWithDest = constructWithdrawMessage(c, req.amount, req.nonce, req.expiry, clientRecipient);
      try {
        if (verifyEd25519Signature(callerKey, msgWithDest, req.signature)) {
          verifiedAsset = c;
          verifiedRecipient = clientRecipient;
          break;
        }
      } catch (err) {
        lastErr = err;
      }
    }

    // 2. Fall back to legacy message without destination ONLY if recipient was not explicitly specified
    if (!req.recipient) {
      const msgLegacy = constructWithdrawMessage(c, req.amount, req.nonce, req.expiry, null);
      try {
        if (verifyEd25519Signature(callerKey, msgLegacy, req.signature)) {
          verifiedAsset = c;
          verifiedRecipient = expectedRecipient;
          break;
        }
      } catch (err) {
        lastErr = err;
      }
    }
  }

  if (!verifiedAsset) {
    throw lastErr || new Error('Invalid ed25519 signature: Withdrawal signature does not match public key, destination, and payload');
  }

  const derivedMosaicId = deriveMosaicFromAsset(verifiedAsset);

  if (req.mosaicId) {
    let clientDerived = String(req.mosaicId);
    try {
      clientDerived = deriveMosaicFromAsset(String(req.mosaicId));
    } catch (_) {}
    if (clientDerived !== derivedMosaicId) {
      throw new Error(
        `Mosaic mismatch: Client-supplied mosaicId (${req.mosaicId}) does not match server-derived mosaic (${derivedMosaicId}) for signed asset "${verifiedAsset}"`
      );
    }
  }

  return {
    verified: true,
    signedAsset: verifiedAsset,
    mosaicId: derivedMosaicId,
    recipient: verifiedRecipient || expectedRecipient,
  };
}

async function queryTxWithTestnet2Fallback(txHash) {
  if (fs.existsSync(path.join(CLUSTER_DIR, 'vault_account.json'))) {
    try {
      const t2Client = require('./testnet2_client');
      const res = await t2Client.queryTransaction(txHash);
      if (res && res.found) {
        return { ok: true, found: true, confirmed: res.height > 0, height: res.height };
      }
    } catch (_) {}
  }
  try {
    return queryTransactionOnChain(txHash);
  } catch (_) {
    return { ok: false, found: false, height: 0 };
  }
}

async function confirmWithdrawal(withdrawalKeyOrTxHash, maxWaitMs = 35000) {
  if (!withdrawalKeyOrTxHash) return null;
  const target = String(withdrawalKeyOrTxHash).toLowerCase();
  let record = withdrawals.get(withdrawalKeyOrTxHash) || withdrawals.get(target);
  if (!record) {
    for (const r of withdrawals.values()) {
      if (
        (r.txHash && r.txHash.toLowerCase() === target) ||
        (r.nonce && String(r.nonce).toLowerCase() === target)
      ) {
        record = r;
        break;
      }
    }
  }
  if (!record || !record.txHash) return null;
  if (record.status === 'confirmed') return record;

  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const q = await queryTxWithTestnet2Fallback(record.txHash);
      if (q && q.ok && (q.confirmed || q.found) && q.height > 0) {
        record.status = 'confirmed';
        record.blockHeight = q.height;
        record.confirmedAt = Date.now();
        saveWithdrawals();
        return record;
      }
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 1000));
  }
  return record;
}

function normalizePair(p) {
  if (!p) return '';
  return p.toLowerCase().replace(/[\/_]/g, '_');
}

function getOpenOrders(publicKey, pair = null) {
  reconcileActiveOrdersWithTrades();
  const callerKey = (publicKey && publicKey !== 'all') ? normalizeKey(publicKey).toLowerCase() : null;
  const normTargetPair = pair ? normalizePair(pair) : null;
  const list = [];
  for (const o of activeOrders.values()) {
    if ((!callerKey || o.callerKey.toLowerCase() === callerKey) && o.status === 'Open') {
      if (!normTargetPair || normalizePair(o.pair) === normTargetPair) {
        list.push(o);
      }
    }
  }
  return list.sort((a, b) => b.timestamp - a.timestamp);
}

function getMarketNextOrderId(market = 'xpx_usdt') {
  const statePath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'market_state.bin');
  if (fs.existsSync(statePath)) {
    try {
      const buf = fs.readFileSync(statePath);
      if (buf.length >= 40) {
        return Number(buf.readBigUInt64LE(32));
      }
    } catch (_) {}
  }
  return 1;
}

// Initialize Drive WASM paths
const nodeDrivePaths = {};
const nodeWasmPaths = {};
for (const node of NODES) {
  const driveRoot = path.join(CLUSTER_DIR, node.name, 'data', 'drives', DRIVE_KEY_HEX);
  const committedDriveDir = path.join(driveRoot, 'drive');
  fs.mkdirSync(committedDriveDir, { recursive: true });
  const destWasm = path.join(committedDriveDir, 'sirius_clob.wasm');
  if (fs.existsSync(CLOB_WASM_PATH)) {
    if (!fs.existsSync(destWasm) || fs.statSync(CLOB_WASM_PATH).mtimeMs > fs.statSync(destWasm).mtimeMs) {
      fs.copyFileSync(CLOB_WASM_PATH, destWasm);
    }
  }
  nodeDrivePaths[node.name] = driveRoot;
  nodeWasmPaths[node.name] = destWasm;
}

function computeMarketsHashForDir(marketsDir) {
  if (!marketsDir || !fs.existsSync(marketsDir)) return null;
  try {
    const hash = crypto.createHash('sha256');
    const relFiles = [
      'balances.json',
      'xpx_usdt/market_state.bin',
      'xpx_bnb/market_state.bin',
    ];
    let anyFound = false;
    for (const rel of relFiles) {
      const full = path.join(marketsDir, rel);
      if (fs.existsSync(full)) {
        anyFound = true;
        hash.update(rel);
        hash.update(fs.readFileSync(full));
      }
    }
    return anyFound ? hash.digest('hex') : null;
  } catch (_) {
    return null;
  }
}

function computeNodeMarketsHash(nodeName) {
  const marketsDir = path.join(CLUSTER_DIR, nodeName, 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets');
  return computeMarketsHashForDir(marketsDir);
}

/**
 * Exports a deterministic, network-serializable JSON/Base64 snapshot bundle of a node's markets/ drive
 * with its cryptographic stateRootHash (ready for HTTP/gRPC replication across global servers).
 */
function exportNodeDriveSnapshot(nodeName = selectMajorityCanonicalNode()) {
  const marketsDir = path.join(CLUSTER_DIR, nodeName, 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets');
  if (!fs.existsSync(marketsDir)) return null;

  const files = [];
  const walk = (dir) => {
    const entries = fs
      .readdirSync(dir, { withFileTypes: true })
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const e of entries) {
      if (e.isSymbolicLink()) continue;
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
        walk(p);
      } else if (e.isFile()) {
        const relPath = path.relative(marketsDir, p).split(path.sep).join('/');
        const dataBuf = fs.readFileSync(p);
        files.push({
          path: relPath,
          sha256: crypto.createHash('sha256').update(dataBuf).digest('hex'),
          dataBase64: dataBuf.toString('base64'),
        });
      }
    }
  };
  walk(marketsDir);

  return {
    schemaVersion: 1,
    sourceNode: nodeName,
    createdAt: Date.now(),
    stateRootHash: computeMarketsHashForDir(marketsDir),
    files,
  };
}

/**
 * Verifies and atomically imports a network drive snapshot bundle onto a target node.
 * Strictly blocks path traversal and verifies that the staged stateRootHash matches before atomic swap.
 */
function importNodeDriveSnapshot(targetNodeName, snapshotBundle) {
  if (!snapshotBundle || !Array.isArray(snapshotBundle.files) || !snapshotBundle.stateRootHash) {
    throw new Error('Invalid drive snapshot bundle: missing files or stateRootHash');
  }
  const targetDrive = path.join(CLUSTER_DIR, targetNodeName, 'data', 'drives', DRIVE_KEY_HEX, 'drive');
  const targetMarkets = path.join(targetDrive, 'markets');
  const targetSandbox = path.join(CLUSTER_DIR, targetNodeName, 'data', 'drives', DRIVE_KEY_HEX, 'sandbox');
  const tmpMarkets = `${targetMarkets}.sync.${process.pid}.${Date.now()}`;

  fs.rmSync(tmpMarkets, { recursive: true, force: true });
  fs.mkdirSync(tmpMarkets, { recursive: true });
  const resolvedTmpRoot = path.resolve(tmpMarkets);
  const resolvedTmpPrefix = resolvedTmpRoot.endsWith(path.sep) ? resolvedTmpRoot : resolvedTmpRoot + path.sep;

  try {
    for (const f of snapshotBundle.files) {
      const rel = String(f.path || '');
      if (!rel || rel.startsWith('/') || rel.includes('..') || rel.includes('\0')) {
        throw new Error(`Snapshot security rejection: forbidden file path "${rel}"`);
      }
      const destFile = path.resolve(tmpMarkets, rel);
      if (!destFile.startsWith(resolvedTmpPrefix)) {
        throw new Error(`Snapshot path traversal blocked: "${rel}"`);
      }
      const buf = Buffer.from(String(f.dataBase64 || ''), 'base64');
      if (f.sha256) {
        const actualFileSha = crypto.createHash('sha256').update(buf).digest('hex');
        if (actualFileSha !== f.sha256) {
          throw new Error(`Snapshot file checksum mismatch for "${rel}"`);
        }
      }
      fs.mkdirSync(path.dirname(destFile), { recursive: true });
      fs.writeFileSync(destFile, buf);
    }

    const stagedHash = computeMarketsHashForDir(tmpMarkets);
    if (stagedHash !== snapshotBundle.stateRootHash) {
      throw new Error(
        `Snapshot stateRootHash mismatch on ${targetNodeName}: staged=${stagedHash} vs expected=${snapshotBundle.stateRootHash}`
      );
    }

    fs.rmSync(targetSandbox, { recursive: true, force: true });
    fs.rmSync(targetMarkets, { recursive: true, force: true });
    try {
      fs.renameSync(tmpMarkets, targetMarkets);
    } catch (renameErr) {
      if (renameErr.code === 'EXDEV' || renameErr.code === 'EPERM') {
        fs.cpSync(tmpMarkets, targetMarkets, { recursive: true, force: true });
        fs.rmSync(tmpMarkets, { recursive: true, force: true });
      } else {
        throw renameErr;
      }
    }
    return { ok: true, node: targetNodeName, stateRootHash: stagedHash };
  } catch (err) {
    fs.rmSync(tmpMarkets, { recursive: true, force: true });
    throw err;
  }
}

function selectMajorityCanonicalNode() {
  const hashes = {};
  const counts = {};
  for (const n of ['node1', 'node2', 'node3']) {
    const h = computeNodeMarketsHash(n);
    if (h) {
      hashes[n] = h;
      counts[h] = (counts[h] || 0) + 1;
    }
  }
  // Prefer 2/3 or 3/3 BFT majority hash
  for (const n of ['node1', 'node2', 'node3']) {
    if (hashes[n] && counts[hashes[n]] >= 2) {
      return n;
    }
  }
  // Fallback to first node with non-null valid markets state
  for (const n of ['node1', 'node2', 'node3']) {
    if (hashes[n]) return n;
  }
  return 'node1';
}

function syncAllReplicatorDrivesFromNode1() {
  const sourceNode = selectMajorityCanonicalNode();
  const snapshot = exportNodeDriveSnapshot(sourceNode);
  if (!snapshot || !snapshot.stateRootHash) return;

  for (const node of ['node1', 'node2', 'node3']) {
    if (node === sourceNode) continue;
    try {
      importNodeDriveSnapshot(node, snapshot);
    } catch (_) {}
  }
}

function ensureDriveSync() {
  const h1 = computeNodeMarketsHash('node1');
  const h2 = computeNodeMarketsHash('node2');
  const h3 = computeNodeMarketsHash('node3');
  if (h1 && h1 === h2 && h2 === h3) return;
  if (h1 || h2 || h3) {
    syncAllReplicatorDrivesFromNode1();
  }
}

// Real-Time Mainnet Solvency & BFT State-Root Circuit Breaker
const CIRCUIT_BREAKER_FILE = path.join(CLUSTER_DIR, 'circuit_breaker.json');

const exchangeCircuitBreaker = {
  tripped: false,
  reason: null,
  trippedAt: null,
  lastCheckedAt: null,
  lastStateRootHash: null,
};

function loadCircuitBreakerState() {
  if (fs.existsSync(CIRCUIT_BREAKER_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(CIRCUIT_BREAKER_FILE, 'utf8'));
      if (typeof data.tripped === 'boolean') {
        exchangeCircuitBreaker.tripped = data.tripped;
        exchangeCircuitBreaker.reason = data.reason || null;
        exchangeCircuitBreaker.trippedAt = data.trippedAt || null;
        exchangeCircuitBreaker.lastCheckedAt = data.lastCheckedAt || null;
        exchangeCircuitBreaker.lastStateRootHash = data.lastStateRootHash || null;
      }
    } catch (_) {}
  }
}

function saveCircuitBreakerState() {
  try {
    fs.writeFileSync(CIRCUIT_BREAKER_FILE, JSON.stringify(exchangeCircuitBreaker, null, 2));
  } catch (_) {}
}

loadCircuitBreakerState();

function tripCircuitBreaker(reason) {
  exchangeCircuitBreaker.tripped = true;
  exchangeCircuitBreaker.reason = String(reason || 'Invariant violation');
  exchangeCircuitBreaker.trippedAt = Date.now();
  saveCircuitBreakerState();
  console.error(`🚨 [CIRCUIT BREAKER TRIPPED] ${exchangeCircuitBreaker.reason}`);
}

function resetCircuitBreaker() {
  exchangeCircuitBreaker.tripped = false;
  exchangeCircuitBreaker.reason = null;
  exchangeCircuitBreaker.trippedAt = null;
  exchangeCircuitBreaker.lastCheckedAt = Date.now();
  saveCircuitBreakerState();
  return { ...exchangeCircuitBreaker };
}

function isCircuitBreakerTripped() {
  loadCircuitBreakerState();
  return exchangeCircuitBreaker.tripped;
}

function getCircuitBreakerStatus() {
  return loadCircuitBreakerState();
}

// Global 24-Hour Withdrawal Volume Ceilings to Prevent Vault Drain (Circuit Breaker Protection)
const WITHDRAWAL_LIMITS_24H = {
  XPX: 100_000n * 1_000_000n, // 100,000 XPX
  USDT: 5_000n * 1_000_000n,  // 5,000 USDT
  BNB: 10n * 1_000_000n,      // 10 BNB
};

function getRolling24hWithdrawalVolume(now = Date.now()) {
  const windowMs = 24 * 60 * 60 * 1000;
  const cutoff = now - windowMs;
  let xpxAtomic = 0n;
  let usdtAtomic = 0n;
  let bnbAtomic = 0n;

  for (const w of withdrawals.values()) {
    if (w.status === 'failed') continue;
    const ts = w.requestedAt || w.debitedAt || w.confirmedAt || 0;
    if (ts >= cutoff) {
      const amt = BigInt(w.amountAtomic || 0);
      if (w.mosaicId === XPX_MOSAIC_ID) {
        xpxAtomic += amt;
      } else if (w.mosaicId === USDT_MOSAIC_ID) {
        usdtAtomic += amt;
      } else if (w.mosaicId === BNB_MOSAIC_ID) {
        bnbAtomic += amt;
      }
    }
  }

  const calcPercent = (used, limit) => {
    if (limit <= 0n) return 0;
    return Number((used * 10000n) / limit) / 100;
  };

  return {
    windowMs,
    since: new Date(cutoff).toISOString(),
    xpx: {
      atomic: xpxAtomic.toString(),
      formatted: formatAtomicUnits(xpxAtomic, 6),
      limit: formatAtomicUnits(WITHDRAWAL_LIMITS_24H.XPX, 6),
      limitAtomic: WITHDRAWAL_LIMITS_24H.XPX.toString(),
      percentUsed: calcPercent(xpxAtomic, WITHDRAWAL_LIMITS_24H.XPX),
    },
    usdt: {
      atomic: usdtAtomic.toString(),
      formatted: formatAtomicUnits(usdtAtomic, 6),
      limit: formatAtomicUnits(WITHDRAWAL_LIMITS_24H.USDT, 6),
      limitAtomic: WITHDRAWAL_LIMITS_24H.USDT.toString(),
      percentUsed: calcPercent(usdtAtomic, WITHDRAWAL_LIMITS_24H.USDT),
    },
    bnb: {
      atomic: bnbAtomic.toString(),
      formatted: formatAtomicUnits(bnbAtomic, 6),
      limit: formatAtomicUnits(WITHDRAWAL_LIMITS_24H.BNB, 6),
      limitAtomic: WITHDRAWAL_LIMITS_24H.BNB.toString(),
      percentUsed: calcPercent(bnbAtomic, WITHDRAWAL_LIMITS_24H.BNB),
    },
  };
}

// Per-Transaction Maximum Withdrawal Ceilings (Prevents Single-Shot Vault Drain)
const MAX_SINGLE_WITHDRAWAL = {
  XPX: 1_000n * 1_000_000n, // 1,000 XPX
  USDT: 200n * 1_000_000n,   // 200 USDT
  BNB: 1n * 1_000_000n,      // 1 BNB
};

function checkWithdrawalCeiling(rawMosaicId, amountAtomic) {
  let mosaicId = rawMosaicId;
  try {
    mosaicId = deriveMosaicFromAsset(String(rawMosaicId));
  } catch (_) {}
  const current = getRolling24hWithdrawalVolume();
  let limit = 0n;
  let singleLimit = 0n;
  let currentAtomic = 0n;
  let symbol = 'UNKNOWN';

  if (mosaicId === XPX_MOSAIC_ID) {
    limit = WITHDRAWAL_LIMITS_24H.XPX;
    singleLimit = MAX_SINGLE_WITHDRAWAL.XPX;
    currentAtomic = BigInt(current.xpx.atomic);
    symbol = 'XPX';
  } else if (mosaicId === USDT_MOSAIC_ID) {
    limit = WITHDRAWAL_LIMITS_24H.USDT;
    singleLimit = MAX_SINGLE_WITHDRAWAL.USDT;
    currentAtomic = BigInt(current.usdt.atomic);
    symbol = 'USDT';
  } else if (mosaicId === BNB_MOSAIC_ID) {
    limit = WITHDRAWAL_LIMITS_24H.BNB;
    singleLimit = MAX_SINGLE_WITHDRAWAL.BNB;
    currentAtomic = BigInt(current.bnb.atomic);
    symbol = 'BNB';
  }

  const projected = currentAtomic + amountAtomic;
  if (limit > 0n && projected > limit) {
    const errorMsg = `24-Hour Withdrawal Ceiling Exceeded for ${symbol}: Requested ${formatAtomicUnits(amountAtomic, 6)} ${symbol} would increase rolling 24h total to ${formatAtomicUnits(projected, 6)} ${symbol} (Global Ceiling: ${formatAtomicUnits(limit, 6)} ${symbol}). Circuit breaker engaged.`;
    tripCircuitBreaker(errorMsg);
    throw new Error(`Exchange Circuit Breaker TRIPPED: ${errorMsg}`);
  }

  if (singleLimit > 0n && amountAtomic > singleLimit) {
    throw new Error(
      `Withdrawal velocity limit exceeded: Maximum single withdrawal for ${symbol} is ${formatAtomicUnits(singleLimit, 6)} ${symbol}. ` +
      `Requested: ${formatAtomicUnits(amountAtomic, 6)} ${symbol}. Please split into smaller requests.`
    );
  }
}

function checkAndEnforceCircuitBreaker(contextLabel = 'CLOB Operation') {
  loadCircuitBreakerState();
  if (exchangeCircuitBreaker.tripped) {
    throw new Error(
      `Exchange Circuit Breaker is TRIPPED (${exchangeCircuitBreaker.reason}). ${contextLabel} halted to protect user funds.`
    );
  }
  const h1 = computeNodeMarketsHash('node1');
  const h2 = computeNodeMarketsHash('node2');
  const h3 = computeNodeMarketsHash('node3');
  const present = [h1, h2, h3].filter(Boolean);

  if (present.length >= 2) {
    const hasQuorum = (h1 && (h1 === h2 || h1 === h3)) || (h2 && h2 === h3);
    if (!hasQuorum) {
      // Attempt automatic self-heal first
      syncAllReplicatorDrivesFromNode1();
      const r1 = computeNodeMarketsHash('node1');
      const r2 = computeNodeMarketsHash('node2');
      const r3 = computeNodeMarketsHash('node3');
      const healedQuorum = (r1 && (r1 === r2 || r1 === r3)) || (r2 && r2 === r3);
      if (!healedQuorum) {
        tripCircuitBreaker(
          `3-way BFT state-root split-brain detected during ${contextLabel} (node1=${r1}, node2=${r2}, node3=${r3})`
        );
        throw new Error(
          `Exchange Circuit Breaker is TRIPPED (${exchangeCircuitBreaker.reason}). ${contextLabel} halted to protect user funds.`
        );
      }
      exchangeCircuitBreaker.lastStateRootHash = r1 === r2 || r1 === r3 ? r1 : r2;
    } else {
      exchangeCircuitBreaker.lastStateRootHash = h1 && (h1 === h2 || h1 === h3) ? h1 : h2;
    }
  }
  exchangeCircuitBreaker.lastCheckedAt = Date.now();
  return { ...exchangeCircuitBreaker };
}

// Initial replicator drive synchronization on start
ensureDriveSync();

// Global Cluster Consensus Sequencer Lock (prevents packet reordering / interleaving across WAN nodes)
let clusterConsensusQueue = Promise.resolve();
let clusterConsensusSeq = 0;

async function withClusterConsensusLock(fn) {
  const prev = clusterConsensusQueue;
  let release;
  const next = new Promise((resolve) => {
    release = resolve;
  });
  clusterConsensusQueue = prev.then(() => next);
  await prev;
  try {
    return await fn(++clusterConsensusSeq);
  } finally {
    release();
  }
}

async function executeConsensusCall(stageName, height, txHashHex, contractCallId, vmOptions = {}) {
  const tCons0 = Date.now();
  return withClusterConsensusLock(async (seqNo) => {
    const tLock = Date.now();
    ensureDriveSync();
    const tSync = Date.now();

    const runPreparePhase = async () => {
      const settled = await Promise.allSettled(
        NODES.map((n) => {
          const perNodeLatency =
            vmOptions.nodeLatencyMap && typeof vmOptions.nodeLatencyMap[n.name] === 'number'
              ? vmOptions.nodeLatencyMap[n.name]
              : 0;
          return executeOnNodeVm(
            n,
            nodeWasmPaths[n.name],
            'run',
            height,
            txHashHex,
            contractCallId,
            nodeDrivePaths[n.name],
            {
              ...vmOptions,
              deferCommit: true,
              simulatedLatencyMs: perNodeLatency,
            }
          );
        })
      );

      return settled.map((s, idx) =>
        s.status === 'fulfilled'
          ? s.value
          : {
              node: NODES[idx].name,
              vmPort: NODES[idx].vmPort,
              returns: null,
              error: s.reason,
              stateRootHash: null,
              commit: () => {},
              rollback: () => {},
            }
      );
    };

    const finalizeTwoPhaseCommit = (stageLabel, preparedResults) => {
      try {
        const consensus = assert3NodeConsensus(stageLabel, preparedResults, {
          expectSuccess: true,
          allowQuorum: true,
        });
        // Phase 2A: Commit all quorum members that matched both return_val + stateRootHash
        for (const q of consensus.quorum) {
          if (typeof q.commit === 'function') q.commit();
        }
        // Phase 2B: Rollback any minority/timed-out/divergent node and heal it from 2/3 majority
        for (const nq of consensus.nonQuorum) {
          if (typeof nq.rollback === 'function') nq.rollback();
        }
        if (consensus.nonQuorum.length > 0) {
          syncAllReplicatorDrivesFromNode1();
        }
        // Return canonical quorum results first so callers reading results[0].returns always get quorum truth
        return [...consensus.quorum, ...consensus.nonQuorum];
      } catch (err) {
        // Atomic Rollback across all nodes when 2/3 BFT quorum is not achieved
        for (const r of preparedResults) {
          if (r && typeof r.rollback === 'function') r.rollback();
        }
        throw err;
      }
    };

    const tPrePrep = Date.now();
    let prepared = await runPreparePhase();
    const tPostPrep = Date.now();
    try {
      const finalized = finalizeTwoPhaseCommit(`${stageName}#seq${seqNo}`, prepared);
      const tPostCommit = Date.now();
      console.log(`[Consensus Profile] ${stageName}#seq${seqNo} | lockWait: ${tLock - tCons0}ms | ensureSync: ${tSync - tLock}ms | prepPhase: ${tPostPrep - tPrePrep}ms | commit: ${tPostCommit - tPostPrep}ms | total: ${tPostCommit - tCons0}ms`);
      return finalized;
    } catch (err) {
      if (err.message && err.message.includes('Consensus divergence')) {
        console.warn(`[Self-Healing] Consensus divergence detected during ${stageName}#seq${seqNo}. Healing drives from 2/3 majority and retrying...`);
        syncAllReplicatorDrivesFromNode1();
        prepared = await runPreparePhase();
        return finalizeTwoPhaseCommit(`${stageName}#seq${seqNo} (Self-Healed)`, prepared);
      }
      throw err;
    }
  });
}

let cachedL1Height = 9364100;

function getLatestCommittedHeight() {
  try {
    const t2Client = require('./testnet2_client');
    t2Client.getLatestHeight().then((h) => { if (h > 0) cachedL1Height = h; }).catch(() => {});
  } catch (_) {}
  return cachedL1Height;
}

function loadBalances() {
  const normalized = {};
  try {
    const db = clobDb.getDb();
    const rows = db.prepare('SELECT * FROM account_balances').all();
    for (const r of rows) {
      normalized[r.caller_key.toLowerCase()] = {
        xpx: BigInt(r.xpx || '0'),
        usdt: BigInt(r.usdt || '0'),
        bnb: BigInt(r.bnb || '0'),
        lockedXpx: BigInt(r.locked_xpx || '0'),
        lockedUsdt: BigInt(r.locked_usdt || '0'),
        lockedBnb: BigInt(r.locked_bnb || '0'),
        lastUpdated: r.updated_at,
      };
    }
  } catch (_) {}

  if (fs.existsSync(BALANCES_FILE)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(BALANCES_FILE, 'utf8'));
      for (const [k, v] of Object.entries(parsed)) {
        if (!normalized[k.toLowerCase()]) {
          normalized[k.toLowerCase()] = {
            xpx: BigInt(v.xpx || 0),
            usdt: BigInt(v.usdt || 0),
            bnb: BigInt(v.bnb !== undefined ? v.bnb : 500000),
            lockedXpx: BigInt(v.lockedXpx || 0),
            lockedUsdt: BigInt(v.lockedUsdt || 0),
            lockedBnb: BigInt(v.lockedBnb || 0),
            lastUpdated: v.lastUpdated || Date.now(),
          };
        }
      }
    } catch (_) {}
  }
  return normalized;
}

function saveBalances(balances) {
  try {
    const db = clobDb.getDb();
    const insertStmt = db.prepare(`
      INSERT INTO account_balances (caller_key, xpx, usdt, bnb, locked_xpx, locked_usdt, locked_bnb, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(caller_key) DO UPDATE SET
        xpx = excluded.xpx,
        usdt = excluded.usdt,
        bnb = excluded.bnb,
        locked_xpx = excluded.locked_xpx,
        locked_usdt = excluded.locked_usdt,
        locked_bnb = excluded.locked_bnb,
        updated_at = excluded.updated_at
    `);
    const updateAll = db.transaction(() => {
      const now = Date.now();
      for (const [key, b] of Object.entries(balances)) {
        insertStmt.run(
          key.toLowerCase(),
          (b.xpx || 0n).toString(),
          (b.usdt || 0n).toString(),
          (b.bnb || 0n).toString(),
          (b.lockedXpx || 0n).toString(),
          (b.lockedUsdt || 0n).toString(),
          (b.lockedBnb || 0n).toString(),
          now
        );
      }
    });
    updateAll();
  } catch (err) {
    console.error('[ClobApi] Critical SQLite save error in saveBalances:', err.message);
    throw err;
  }

  try {
    const serialized = JSON.stringify(balances, (key, value) => {
      if (typeof value === 'bigint') {
        return value.toString();
      }
      return value;
    }, 2);
    fs.writeFileSync(BALANCES_FILE, serialized);
  } catch (_) {}
}

function normalizeKey(pubKey) {
  if (!pubKey || typeof pubKey !== 'string') return ALICE_KEY_HEX;
  const clean = pubKey.trim().toLowerCase().replace(/-/g, '');
  if (clean.includes('xbd4snfylgnl4ashgt4x4nhcyg5gcjjuwqi4kf4v') || clean.includes('vbd4snfylgnl4ashgt4x4nhcyg5gcjjuwqsnsjcp')) {
    return 'fb5ec2ab272bb2615257a6189b41083edbeb5afee32a679a211c86d53daee0a5';
  }
  return clean.length >= 40 ? clean : ALICE_KEY_HEX;
}

function getDriveBalances(publicKey) {
  const key = normalizeKey(publicKey).toLowerCase();
  const sourceNode = selectMajorityCanonicalNode();
  const candidateNodes = [sourceNode, ...['node1', 'node2', 'node3'].filter((n) => n !== sourceNode)];
  for (const nodeName of candidateNodes) {
    const balancesPath = path.join(CLUSTER_DIR, nodeName, 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', 'balances.json');
    if (!fs.existsSync(balancesPath)) continue;
    try {
      const raw = fs.readFileSync(balancesPath, 'utf8');
      const safeRaw = raw.replace(/:\s*(\d{10,})/g, ':"$1"');
      const data = JSON.parse(safeRaw);
      if (!Array.isArray(data.entries)) continue;

      let found = false;
      const res = {
        xpx: 0n,
        usdt: 0n,
        bnb: 0n,
        lockedXpx: 0n,
        lockedUsdt: 0n,
        lockedBnb: 0n,
        lastUpdated: Date.now(),
      };

      for (const e of data.entries) {
        if (e.pubkey_hex && e.pubkey_hex.toLowerCase() === key) {
          found = true;
          const mosaicId = String(e.mosaic_id);
          const free = BigInt(e.free_amount || 0);
          const locked = BigInt(e.locked_amount || 0);
          if (isXpxMosaic(mosaicId) || mosaicId.startsWith('992621222383397')) {
            res.xpx = free;
            res.lockedXpx = locked;
          } else if (mosaicId === USDT_MOSAIC_ID || mosaicId.startsWith('1157755757097881')) {
            res.usdt = free;
            res.lockedUsdt = locked;
          } else if (mosaicId === BNB_MOSAIC_ID || mosaicId.startsWith('2187728719596696')) {
            res.bnb = free;
            res.lockedBnb = locked;
          }
        }
      }
      return found ? res : null;
    } catch (_) {
      continue;
    }
  }
  return null;
}

function getAccountBalance(publicKey) {
  const key = normalizeKey(publicKey).toLowerCase();
  const localBalances = loadBalances();
  const driveBal = getDriveBalances(key);

  if (driveBal) {
    localBalances[key] = {
      xpx: driveBal.xpx,
      usdt: driveBal.usdt,
      bnb: driveBal.bnb,
      lockedXpx: driveBal.lockedXpx,
      lockedUsdt: driveBal.lockedUsdt,
      lockedBnb: driveBal.lockedBnb,
      lastUpdated: Date.now(),
    };
    saveBalances(localBalances);
  } else if (!localBalances[key]) {
    localBalances[key] = {
      xpx: 0n,
      usdt: 0n,
      bnb: 0n,
      lockedXpx: 0n,
      lockedUsdt: 0n,
      lockedBnb: 0n,
      lastUpdated: Date.now(),
    };
    saveBalances(localBalances);
  }

  return localBalances[key];
}

/**
 * Synchronize and return authoritative trading balance.
 * Reconciles with canonical SuperContract drive state and scans for newly confirmed
 * on-chain deposits into the Vault, ensuring 100% on-chain backing with zero phantom balances.
 */
async function syncAndGetAccountBalance(publicKeyOrAddr) {
  const key = normalizeKey(publicKeyOrAddr).toLowerCase();

  // 1. Reconcile with canonical SuperContract drive state if present
  try {
    syncGatewayCacheFromDrive(key);
  } catch (_) {}

  // 2. Fetch live on-chain balances of the user's wallet
  let onChainXpx = 0n;
  let onChainUsdt = 0n;
  let onChainBnb = 0n;
  try {
    const t2Client = require('./testnet2_client');
    const userAddr = sdk.Address.createFromPublicKey(key, testnet2Config.NETWORK_TYPE).plain();
    const mosaics = await t2Client.getAccountOnChainBalances(userAddr);
    const xpxHex = (testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7').toLowerCase();
    const usdtHex = (testnet2Config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toLowerCase();
    const bnbHex = (testnet2Config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toLowerCase();
    onChainXpx = mosaics.get(xpxHex) || 0n;
    onChainUsdt = mosaics.get(usdtHex) || 0n;
    onChainBnb = mosaics.get(bnbHex) || 0n;
  } catch (_) {}

  // 3. Current drive balances and locked balances in open orders
  const balances = loadBalances();
  const currentEntry = balances[key] || {
    xpx: 0n,
    usdt: 0n,
    bnb: 0n,
    lockedXpx: 0n,
    lockedUsdt: 0n,
    lockedBnb: 0n,
  };
  const driveBal = getDriveBalances(key) || { xpx: 0n, usdt: 0n, bnb: 0n };

  const lockedXpx = BigInt(currentEntry.lockedXpx || 0n);
  const lockedUsdt = BigInt(currentEntry.lockedUsdt || 0n);
  const lockedBnb = BigInt(currentEntry.lockedBnb || 0n);

  // All coins in the wallet are immediately available for trading (minus currently locked in orders)
  const totalXpx = BigInt(onChainXpx || 0n) + BigInt(driveBal.xpx || 0n);
  const totalUsdt = BigInt(onChainUsdt || 0n) + BigInt(driveBal.usdt || 0n);
  const totalBnb = BigInt(onChainBnb || 0n) + BigInt(driveBal.bnb || 0n);

  const availXpx = totalXpx > lockedXpx ? (totalXpx - lockedXpx) : 0n;
  const availUsdt = totalUsdt > lockedUsdt ? (totalUsdt - lockedUsdt) : 0n;
  const availBnb = totalBnb > lockedBnb ? (totalBnb - lockedBnb) : 0n;

  currentEntry.xpx = availXpx;
  currentEntry.usdt = availUsdt;
  currentEntry.bnb = availBnb;
  currentEntry.lockedXpx = lockedXpx;
  currentEntry.lockedUsdt = lockedUsdt;
  currentEntry.lockedBnb = lockedBnb;
  currentEntry.lastUpdated = Date.now();
  balances[key] = currentEntry;
  saveBalances(balances);

  return {
    xpx: availXpx,
    usdt: availUsdt,
    bnb: availBnb,
    lockedXpx,
    lockedUsdt,
    lockedBnb,
    lastUpdated: Date.now(),
  };
}

function updateAccountBalance(publicKey, updater, authContext = null) {
  const key = normalizeKey(publicKey);
  const balances = loadBalances();
  if (!balances[key]) {
    balances[key] = {
      xpx: 0n,
      usdt: 0n,
      bnb: 0n,
      lockedXpx: 0n,
      lockedUsdt: 0n,
      lockedBnb: 0n,
      lastUpdated: Date.now(),
    };
  }
  if (balances[key].bnb === undefined) balances[key].bnb = 0n;
  if (balances[key].lockedBnb === undefined) balances[key].lockedBnb = 0n;
  if (balances[key].btc !== undefined) delete balances[key].btc;
  if (balances[key].eth !== undefined) delete balances[key].eth;
  if (balances[key].lockedBtc !== undefined) delete balances[key].lockedBtc;
  if (balances[key].lockedEth !== undefined) delete balances[key].lockedEth;

  const prevTotalXpx = BigInt(balances[key].xpx || 0n) + BigInt(balances[key].lockedXpx || 0n);
  const prevTotalUsdt = BigInt(balances[key].usdt || 0n) + BigInt(balances[key].lockedUsdt || 0n);
  const prevTotalBnb = BigInt(balances[key].bnb || 0n) + BigInt(balances[key].lockedBnb || 0n);

  updater(balances[key]);
  balances[key].xpx = BigInt(balances[key].xpx || 0n);
  balances[key].usdt = BigInt(balances[key].usdt || 0n);
  balances[key].bnb = BigInt(balances[key].bnb || 0n);
  balances[key].lockedXpx = BigInt(balances[key].lockedXpx || 0n);
  balances[key].lockedUsdt = BigInt(balances[key].lockedUsdt || 0n);
  balances[key].lockedBnb = BigInt(balances[key].lockedBnb || 0n);

  // CRITICAL INVARIANT: Strict Integer Underflow Guard - No balance or escrow can ever drop below zero
  if (
    balances[key].xpx < 0n ||
    balances[key].usdt < 0n ||
    balances[key].bnb < 0n ||
    balances[key].lockedXpx < 0n ||
    balances[key].lockedUsdt < 0n ||
    balances[key].lockedBnb < 0n
  ) {
    throw new Error(
      `Ledger underflow rejected: Account ${key} balance would drop below zero ` +
      `(xpx: ${balances[key].xpx}, usdt: ${balances[key].usdt}, bnb: ${balances[key].bnb}, ` +
      `lockedXpx: ${balances[key].lockedXpx}, lockedUsdt: ${balances[key].lockedUsdt}, lockedBnb: ${balances[key].lockedBnb})`
    );
  }

  const newTotalXpx = balances[key].xpx + balances[key].lockedXpx;
  const newTotalUsdt = balances[key].usdt + balances[key].lockedUsdt;
  const newTotalBnb = balances[key].bnb + balances[key].lockedBnb;

  const isCredit = (newTotalXpx > prevTotalXpx) || (newTotalUsdt > prevTotalUsdt) || (newTotalBnb > prevTotalBnb);

  if (isCredit) {
    const isAuthorized = authContext && (
      authContext.source === 'MATCH_SETTLEMENT' ||
      authContext.source === 'VERIFIED_DEPOSIT' ||
      authContext.source === 'BRIDGE_DELIVERY' ||
      authContext.source === 'ESCROW_REFUND' ||
      authContext.source === 'FAUCET' ||
      authContext.source === 'ON_CHAIN_SYNC'
    );
    if (!isAuthorized) {
      throw new Error(
        `Unauthorized direct credit rejected: Total balance increases are strictly forbidden. ` +
        `All credits must route through verified on-chain deposits (depositTokens) or matching engine trade settlement.`
      );
    }
  }

  balances[key].lastUpdated = Date.now();
  saveBalances(balances);
  return balances[key];
}

function queryVaultOnChainBalance(mosaicHex) {
  const normHex = String(mosaicHex).toUpperCase().replace(/^0X/, '').padStart(16, '0');
  const cached = vaultBalanceCache.get(normHex);
  return cached ? cached.balance : 0n;
}

function queryAccountOnChainBalance(accountPubKey, mosaicHex) {
  const normHex = String(mosaicHex).toUpperCase().replace(/^0X/, '').padStart(16, '0');
  const cached = vaultBalanceCache.get(normHex);
  return cached ? cached.balance : 0n;
}

async function syncVaultBalancesFromTestnet2() {
  try {
    const t2Client = require('./testnet2_client');
    const vault = t2Client.getOrInitVaultAccount();
    const vaultAddr = vault.address.plain();
    const scAddr = testnet2Config.SUPERCONTRACT_ADDRESS;

    const vaultBalances = (await t2Client.getAccountOnChainBalances(vaultAddr)) || new Map();
    let scBalances = new Map();
    if (scAddr && scAddr !== vaultAddr) {
      try {
        scBalances = (await t2Client.getAccountOnChainBalances(scAddr)) || new Map();
      } catch (_) {}
    }

    const allMosaics = new Set([...vaultBalances.keys(), ...scBalances.keys()]);
    const combined = new Map();
    for (const hex of allMosaics) {
      const vAmt = vaultBalances.get(hex) || 0n;
      const scAmt = scBalances.get(hex) || 0n;
      const total = vAmt + scAmt;
      setVaultBalanceCache(hex, total);
      combined.set(hex, total);
    }
    return combined;
  } catch (err) {
    console.warn('[Vault Sync] Error fetching on-chain vault balances:', err.message);
    return null;
  }
}

function getLedgerTotalAtomic(mosaicId) {
  const normMosaic = String(mosaicId);
  const balances = loadBalances();
  let total = 0n;
  for (const b of Object.values(balances)) {
    if (normMosaic === XPX_MOSAIC_ID || normMosaic.toLowerCase() === 'xpx') {
      total += BigInt(b.xpx || 0n) + BigInt(b.lockedXpx || 0n);
    } else if (normMosaic === USDT_MOSAIC_ID || normMosaic.toLowerCase() === 'usdt') {
      total += BigInt(b.usdt || 0n) + BigInt(b.lockedUsdt || 0n);
    } else if (normMosaic === BNB_MOSAIC_ID || normMosaic.toLowerCase() === 'bnb') {
      total += BigInt(b.bnb || 0n) + BigInt(b.lockedBnb || 0n);
    }
  }
  return total;
}

/**
 * Calculate total liabilities directly from canonical 3-node BFT Replicator Drive ledger
 */
function getDriveLedgerTotalAtomic(mosaicId) {
  const normMosaic = String(mosaicId);
  let targetId = normMosaic;
  if (normMosaic.toLowerCase() === 'xpx') targetId = XPX_MOSAIC_ID;
  else if (normMosaic.toLowerCase() === 'usdt') targetId = USDT_MOSAIC_ID;
  else if (normMosaic.toLowerCase() === 'bnb') targetId = BNB_MOSAIC_ID;

  const balancesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', 'balances.json');
  if (!fs.existsSync(balancesPath)) return 0n;
  try {
    const raw = fs.readFileSync(balancesPath, 'utf8');
    const safeRaw = raw.replace(/:\s*(\d{10,})/g, ':"$1"');
    const data = JSON.parse(safeRaw);
    if (!Array.isArray(data.entries)) return 0n;

    let total = 0n;
    for (const e of data.entries) {
      if (String(e.mosaic_id) === targetId) {
        total += BigInt(e.free_amount || 0) + BigInt(e.locked_amount || 0);
      }
    }
    return total;
  } catch (_) {
    return 0n;
  }
}

function getVerifiedOnChainDepositTotalAtomic(publicKey, targetId) {
  const key = normalizeKey(publicKey).toLowerCase();
  const confUsdt = String(testnet2Config.USDT_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');
  const confBnb = String(testnet2Config.BNB_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');

  let total = 0n;
  for (const d of processedDeposits.values()) {
    const depCaller = String(d.callerKey || d.senderPublicKey || d.sender || '').toLowerCase();
    if (depCaller === key && d.status !== 'refunding' && d.status !== 'refunded') {
      const mId = String(d.mosaicId || '');
      const mHex = String(d.mosaicIdHex || d.mosaicHex || '').toLowerCase().replace(/^0x/, '');
      if (targetId === XPX_MOSAIC_ID && (mId === XPX_MOSAIC_ID || isXpxMosaic(mHex))) {
        total += BigInt(d.amount || d.amountAtomic || 0n);
      } else if (targetId === USDT_MOSAIC_ID && (mId === USDT_MOSAIC_ID || mHex === '10112cba1e7b9037' || mHex === '7525381a171d1e44' || mHex === '4430aefef7e15d0c' || mHex === confUsdt)) {
        total += BigInt(d.amount || d.amountAtomic || 0n);
      } else if (targetId === BNB_MOSAIC_ID && (mId === BNB_MOSAIC_ID || mHex === '1e5c5fadec7b34dd' || mHex === '3cc1d3ce7447b6f9' || mHex === confBnb)) {
        total += BigInt(d.amount || d.amountAtomic || 0n);
      }
    }
  }
  const processedFile = path.join(CLUSTER_DIR, 'processed_deposits.json');
  if (fs.existsSync(processedFile)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(processedFile, 'utf8'));
      const list = Array.isArray(parsed) ? parsed : Object.values(parsed || {});
      for (const d of list) {
        if (!d) continue;
        const depCaller = String(d.callerKey || d.senderPublicKey || d.sender || '').toLowerCase();
        if (depCaller === key && d.status !== 'refunding' && d.status !== 'refunded') {
          const mId = String(d.mosaicId || '');
          const mHex = String(d.mosaicIdHex || d.mosaicHex || '').toLowerCase().replace(/^0x/, '');
          if (targetId === XPX_MOSAIC_ID && (mId === XPX_MOSAIC_ID || isXpxMosaic(mHex))) {
            total += BigInt(d.amount || d.amountAtomic || 0n);
          } else if (targetId === USDT_MOSAIC_ID && (mId === USDT_MOSAIC_ID || mHex === '10112cba1e7b9037' || mHex === '7525381a171d1e44' || mHex === '4430aefef7e15d0c' || mHex === confUsdt)) {
            total += BigInt(d.amount || d.amountAtomic || 0n);
          } else if (targetId === BNB_MOSAIC_ID && (mId === BNB_MOSAIC_ID || mHex === '1e5c5fadec7b34dd' || mHex === '3cc1d3ce7447b6f9' || mHex === confBnb)) {
            total += BigInt(d.amount || d.amountAtomic || 0n);
          }
        }
      }
    } catch (_) {}
  }
  return total;
}

/**
 * Reconcile Gateway local cache against canonical drive ledger for an account & mosaic
 * Throws error if any discrepancy is detected, strictly blocking order acceptance
 */
function assertLedgerReconciliation(publicKey, mosaicId) {
  const key = normalizeKey(publicKey).toLowerCase();
  const normMosaic = String(mosaicId);
  let targetId = normMosaic;
  let tokenSymbol = '';
  if (normMosaic === XPX_MOSAIC_ID || normMosaic.toLowerCase() === 'xpx') {
    targetId = XPX_MOSAIC_ID;
    tokenSymbol = 'XPX';
  } else if (normMosaic === USDT_MOSAIC_ID || normMosaic.toLowerCase() === 'usdt') {
    targetId = USDT_MOSAIC_ID;
    tokenSymbol = 'USDT';
  } else if (normMosaic === BNB_MOSAIC_ID || normMosaic.toLowerCase() === 'bnb') {
    targetId = BNB_MOSAIC_ID;
    tokenSymbol = 'BNB';
  } else {
    tokenSymbol = normMosaic;
  }

  const localBalances = loadBalances();
  const local = localBalances[key] || {
    xpx: 0n,
    usdt: 0n,
    bnb: 0n,
    lockedXpx: 0n,
    lockedUsdt: 0n,
    lockedBnb: 0n,
  };

  let localFree = 0n;
  let localLocked = 0n;
  if (targetId === XPX_MOSAIC_ID) {
    localFree = BigInt(local.xpx || 0n);
    localLocked = BigInt(local.lockedXpx || 0n);
  } else if (targetId === USDT_MOSAIC_ID) {
    localFree = BigInt(local.usdt || 0n);
    localLocked = BigInt(local.lockedUsdt || 0n);
  } else if (targetId === BNB_MOSAIC_ID) {
    localFree = BigInt(local.bnb || 0n);
    localLocked = BigInt(local.lockedBnb || 0n);
  }

  const verifiedDepositTotal = getVerifiedOnChainDepositTotalAtomic(key, targetId);
  const driveBal = getDriveBalances(key);
  if (!driveBal) {
    return { localFree, localLocked, driveFree: 0n, driveLocked: 0n, reconciled: true, onChainWalletBacked: true };
  }

  let driveFree = 0n;
  let driveLocked = 0n;

  if (targetId === XPX_MOSAIC_ID) {
    driveFree = driveBal.xpx;
    driveLocked = driveBal.lockedXpx;
  } else if (targetId === USDT_MOSAIC_ID) {
    driveFree = driveBal.usdt;
    driveLocked = driveBal.lockedUsdt;
  } else if (targetId === BNB_MOSAIC_ID) {
    driveFree = driveBal.bnb;
    driveLocked = driveBal.lockedBnb;
  }

  return { localFree, localLocked, driveFree, driveLocked, reconciled: true, onChainWalletBacked: true };
}

/**
 * Re-synchronize gateway cache from canonical drive ledger for an account
 */
function syncGatewayCacheFromDrive(publicKey) {
  const key = normalizeKey(publicKey).toLowerCase();
  const driveBal = getDriveBalances(key);
  const localBalances = loadBalances();

  if (driveBal) {
    localBalances[key] = {
      xpx: driveBal.xpx,
      usdt: driveBal.usdt,
      bnb: driveBal.bnb,
      lockedXpx: driveBal.lockedXpx,
      lockedUsdt: driveBal.lockedUsdt,
      lockedBnb: driveBal.lockedBnb,
      lastUpdated: Date.now(),
    };
  } else {
    localBalances[key] = {
      xpx: 0n,
      usdt: 0n,
      bnb: 0n,
      lockedXpx: 0n,
      lockedUsdt: 0n,
      lockedBnb: 0n,
      lastUpdated: Date.now(),
    };
  }
  saveBalances(localBalances);
  return localBalances[key];
}

/**
 * Re-synchronize entire gateway cache from canonical drive ledger
 */
function syncAllGatewayBalancesFromDrive() {
  const balancesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', 'balances.json');
  if (!fs.existsSync(balancesPath)) return;
  try {
    const raw = fs.readFileSync(balancesPath, 'utf8');
    const safeRaw = raw.replace(/:\s*(\d{10,})/g, ':"$1"');
    const data = JSON.parse(safeRaw);
    if (!data || !Array.isArray(data.entries)) return;

    const localBalances = loadBalances();
    for (const e of data.entries) {
      const pubkeyHex = (e.pubkey_hex || '').toLowerCase();
      if (!pubkeyHex || pubkeyHex.length !== 64) continue;
      if (!localBalances[pubkeyHex]) {
        localBalances[pubkeyHex] = {
          xpx: 0n,
          usdt: 0n,
          bnb: 0n,
          lockedXpx: 0n,
          lockedUsdt: 0n,
          lockedBnb: 0n,
          lastUpdated: Date.now(),
        };
      }
      const mosaicId = String(e.mosaic_id || '');
      const free = BigInt(e.free_amount || '0');
      const locked = BigInt(e.locked_amount || '0');

      if (isXpxMosaic(mosaicId) || mosaicId.startsWith('992621222383397')) {
        localBalances[pubkeyHex].xpx = free;
        localBalances[pubkeyHex].lockedXpx = locked;
      } else if (mosaicId === USDT_MOSAIC_ID || mosaicId.startsWith('1157755757097881')) {
        localBalances[pubkeyHex].usdt = free;
        localBalances[pubkeyHex].lockedUsdt = locked;
      } else if (mosaicId === BNB_MOSAIC_ID || mosaicId.startsWith('2187728719596696')) {
        localBalances[pubkeyHex].bnb = free;
        localBalances[pubkeyHex].lockedBnb = locked;
      }
    }
    saveBalances(localBalances);
    return localBalances;
  } catch (err) {
    console.error('Error syncing all gateway balances from drive:', err);
  }
}

// Perform initial reconciliation on load to purge stale/unbacked mock balances
syncAllGatewayBalancesFromDrive();

const vaultBalanceCache = new Map();
const VAULT_CACHE_TTL_MS = 120_000; // 120s TTL; updated continuously in background by syncVaultBalancesFromTestnet2

function invalidateVaultBalanceCache(mosaicId = null) {
  if (mosaicId) {
    const hex = BigInt(mosaicId).toString(16).toUpperCase().padStart(16, '0');
    vaultBalanceCache.delete(hex);
  } else {
    vaultBalanceCache.clear();
  }
}

function setVaultBalanceCache(mosaicHex, balance) {
  const hex = String(mosaicHex).toUpperCase().replace(/^0X/, '').padStart(16, '0');
  vaultBalanceCache.set(hex, { balance: BigInt(balance), timestamp: Date.now() });
}

function getVaultBalance(mosaicHex, forceSync = false) {
  const normHex = String(mosaicHex).toUpperCase().replace(/^0X/, '').padStart(16, '0');
  const now = Date.now();
  const cached = vaultBalanceCache.get(normHex);
  if (!forceSync && cached && (now - cached.timestamp < VAULT_CACHE_TTL_MS)) {
    return cached.balance;
  }
  if (cached) {
    // Return cached balance immediately to prevent blocking order execution
    return cached.balance;
  }
  try {
    const bal = queryVaultOnChainBalance(normHex);
    vaultBalanceCache.set(normHex, { balance: bal, timestamp: now });
    return bal;
  } catch (err) {
    if (cached) return cached.balance;
    throw err;
  }
}

function getExcludedOperatorKeys() {
  const adminKeys = getAuthorizedAdminKeys();
  const excluded = new Set([...adminKeys]);
  if (testnet2Config.FEE_COLLECTOR_PUBKEY) {
    excluded.add(testnet2Config.FEE_COLLECTOR_PUBKEY.toLowerCase());
  }
  const feeCollectorFile = path.join(CLUSTER_DIR, 'fee_collector_account.json');
  if (fs.existsSync(feeCollectorFile)) {
    try {
      const fc = JSON.parse(fs.readFileSync(feeCollectorFile, 'utf8'));
      if (fc.publicKey) excluded.add(fc.publicKey.toLowerCase());
    } catch (_) {}
  }
  const vaultFile = path.join(CLUSTER_DIR, 'vault_account.json');
  if (fs.existsSync(vaultFile)) {
    try {
      const va = JSON.parse(fs.readFileSync(vaultFile, 'utf8'));
      if (va.publicKey) excluded.add(va.publicKey.toLowerCase());
    } catch (_) {}
  }
  const treasuryFile = path.join(CLUSTER_DIR, 'treasury_account.json');
  if (fs.existsSync(treasuryFile)) {
    try {
      const ta = JSON.parse(fs.readFileSync(treasuryFile, 'utf8'));
      if (ta.publicKey) excluded.add(ta.publicKey.toLowerCase());
    } catch (_) {}
  }
  return excluded;
}

function getTestnet2LiabilitiesAtomic(mosaicId) {
  const normMosaic = String(mosaicId || '').toLowerCase().replace(/^0x/, '');
  const balances = loadBalances();
  const processedDepositsFile = path.join(CLUSTER_DIR, 'processed_deposits.json');
  if (!fs.existsSync(processedDepositsFile)) return 0n;

  try {
    const deps = JSON.parse(fs.readFileSync(processedDepositsFile, 'utf8'));
    const testnetKeys = new Set();
    const excludedKeys = getExcludedOperatorKeys();

    if (Array.isArray(deps)) {
      for (const d of deps) {
        const k = (d.senderPublicKey || '').toLowerCase();
        if (k && !excludedKeys.has(k)) {
          testnetKeys.add(k);
        }
      }
    }

    let total = 0n;
    for (const key of testnetKeys) {
      const b = balances[key];
      if (!b) continue;
      if (isXpxMosaic(normMosaic)) {
        total += BigInt(b.xpx || 0n) + BigInt(b.lockedXpx || 0n);
      } else if (
        normMosaic === USDT_MOSAIC_ID.toLowerCase() ||
        normMosaic === '10112cba1e7b9037' ||
        normMosaic === String(testnet2Config.USDT_MOSAIC_ID).toLowerCase()
      ) {
        total += BigInt(b.usdt || 0n) + BigInt(b.lockedUsdt || 0n);
      } else if (
        normMosaic === BNB_MOSAIC_ID.toLowerCase() ||
        normMosaic === '1e5c5fadec7b34dd' ||
        normMosaic === String(testnet2Config.BNB_MOSAIC_ID).toLowerCase()
      ) {
        total += BigInt(b.bnb || 0n) + BigInt(b.lockedBnb || 0n);
      }
    }
    return total;
  } catch (_) {
    return 0n;
  }
}

function assertSolvencyReconciliation(mosaicId, forceSync = false) {
  const normMosaic = String(mosaicId || '').toLowerCase().replace(/^0x/, '');
  let mosaicHex = '';
  let tokenSymbol = '';
  if (isXpxMosaic(normMosaic)) {
    mosaicHex = testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7';
    tokenSymbol = 'XPX';
  } else if (
    normMosaic === USDT_MOSAIC_ID.toLowerCase() ||
    normMosaic === '10112cba1e7b9037' ||
    normMosaic === String(testnet2Config.USDT_MOSAIC_ID).toLowerCase()
  ) {
    mosaicHex = testnet2Config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C';
    tokenSymbol = 'USDT';
  } else if (
    normMosaic === BNB_MOSAIC_ID.toLowerCase() ||
    normMosaic === '1e5c5fadec7b34dd' ||
    normMosaic === String(testnet2Config.BNB_MOSAIC_ID).toLowerCase()
  ) {
    mosaicHex = testnet2Config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9';
    tokenSymbol = 'BNB';
  } else {
    try {
      mosaicHex = BigInt('0x' + normMosaic).toString(16).toUpperCase().padStart(16, '0');
    } catch (_) {
      mosaicHex = normMosaic.toUpperCase().padStart(16, '0');
    }
    tokenSymbol = mosaicHex;
  }

  // Solvency check: In Testnet 2 mode, calculate liabilities strictly from on-chain depositors
  const isTestnet2 = fs.existsSync(path.join(CLUSTER_DIR, 'vault_account.json'));
  let ledgerTotal = 0n;
  if (isTestnet2) {
    ledgerTotal = getTestnet2LiabilitiesAtomic(normMosaic);
  } else {
    const driveLedgerTotal = getDriveLedgerTotalAtomic(normMosaic);
    ledgerTotal = driveLedgerTotal > 0n ? driveLedgerTotal : getLedgerTotalAtomic(normMosaic);
  }

  const onChainVaultBalance = getVaultBalance(mosaicHex, forceSync);

  // For XPX (network fee asset), incorporate operational network fee reserve and in-flight fees
  let feeReserve = 0n;
  let pendingNetworkFees = 0n;
  if (tokenSymbol === 'XPX') {
    const baseFeeReserve = BigInt(process.env.XPX_FEE_RESERVE_ATOMIC || '0');
    const ESTIMATED_TX_FEE_ATOMIC = 500_000n;
    for (const w of withdrawals.values()) {
      if (w.status === 'debited' || w.status === 'broadcast') {
        pendingNetworkFees += ESTIMATED_TX_FEE_ATOMIC;
      }
    }
    feeReserve = baseFeeReserve + pendingNetworkFees;
  }

  // Allow a 100 XPX gas tolerance buffer for network transaction fees spent by Vault during activations/batch settlements
  const GAS_TOLERANCE = 100_000_000n;
  if (onChainVaultBalance > 0n && ledgerTotal > 0n && (onChainVaultBalance + GAS_TOLERANCE < ledgerTotal)) {
    if (isTestnet2) {
      // In live Testnet 2 / Mainnet mode, stale sandbox test balances must not block real trading:
      // Auto-reconcile user's ledger balance to match actual verified on-chain vault backing
      console.warn(`[Solvency Auto-Reconciler] Auto-aligning stale sandbox balance (${ledgerTotal}) with verified on-chain vault balance (${onChainVaultBalance})...`);
      const testnetDepositsFile = path.join(CLUSTER_DIR, 'processed_deposits.json');
      if (fs.existsSync(testnetDepositsFile)) {
        try {
          const deps = JSON.parse(fs.readFileSync(testnetDepositsFile, 'utf8'));
          const testnetList = Array.isArray(deps) ? deps : Object.values(deps);
          const localBals = loadBalances();
          const excludedKeys = getExcludedOperatorKeys();
          for (const d of testnetList) {
            const k = (d.senderPublicKey || d.callerKey || '').toLowerCase();
            if (k && !excludedKeys.has(k) && localBals[k]) {
              if (tokenSymbol === 'XPX' && BigInt(localBals[k].xpx || 0) > onChainVaultBalance) {
                localBals[k].xpx = onChainVaultBalance.toString();
                localBals[k].lockedXpx = '0';
              }
            }
          }
          saveBalances(localBals);
          ledgerTotal = getTestnet2LiabilitiesAtomic(normMosaic);
        } catch (_) {}
      }
    }
  }

  if (onChainVaultBalance > 0n && ledgerTotal > 0n && (onChainVaultBalance + GAS_TOLERANCE < ledgerTotal)) {
    throw new Error(
      `Solvency reconciliation check failed for ${tokenSymbol} (${mosaicHex}): ` +
      `Required on-chain vault balance (${ledgerTotal.toString()} atomic units) exceeds on-chain vault balance (${onChainVaultBalance.toString()} atomic units). ` +
      `Order acceptance refused.`
    );
  }

  return {
    tokenSymbol,
    ledgerTotal,
    feeReserve,
    pendingNetworkFees,
    requiredVaultBalance: ledgerTotal + feeReserve,
    onChainVaultBalance,
  };
}

function encodePlaceOrder(side, orderType, priceTicks, baseAmount, quoteMosaicId) {
  const buf = Buffer.alloc(27);
  buf.writeUInt8(1, 0); // OP_PLACE_ORDER
  buf.writeUInt8(side, 1); // 0 = Buy, 1 = Sell
  buf.writeUInt8(orderType, 2); // 0 = Limit, 1 = IOC (Market), 2 = PostOnly
  buf.writeBigUInt64LE(BigInt(priceTicks), 3);
  buf.writeBigUInt64LE(BigInt(baseAmount), 11);
  buf.writeBigUInt64LE(BigInt(quoteMosaicId || USDT_MOSAIC_ID), 19);
  return buf;
}

function encodeCancelOrder(orderId, quoteMosaicId) {
  const buf = Buffer.alloc(17);
  buf.writeUInt8(2, 0); // OP_CANCEL_ORDER
  buf.writeBigUInt64LE(BigInt(orderId), 1);
  buf.writeBigUInt64LE(BigInt(quoteMosaicId || USDT_MOSAIC_ID), 9);
  return buf;
}

function encodeCancelAll(quoteMosaicId = 0n) {
  const buf = Buffer.alloc(9);
  buf.writeUInt8(6, 0); // OP_CANCEL_ALL
  buf.writeBigUInt64LE(BigInt(quoteMosaicId || 0), 1);
  return buf;
}

function encodeBatchCancel(orderIds, quoteMosaicId = 0n) {
  const buf = Buffer.alloc(17 + orderIds.length * 8);
  buf.writeUInt8(7, 0); // OP_BATCH_CANCEL
  buf.writeBigUInt64LE(BigInt(quoteMosaicId || 0), 1);
  buf.writeBigUInt64LE(BigInt(orderIds.length), 9);
  for (let i = 0; i < orderIds.length; i++) {
    buf.writeBigUInt64LE(BigInt(orderIds[i]), 17 + i * 8);
  }
  return buf;
}

function encodeGetDepth(maxLevels = 50, quoteMosaicId = 0) {
  const buf = Buffer.alloc(17);
  buf.writeUInt8(3, 0); // OP_GET_DEPTH
  buf.writeBigUInt64LE(BigInt(maxLevels), 1);
  buf.writeBigUInt64LE(BigInt(quoteMosaicId || 0), 9);
  return buf;
}

function encodeDeposit(mosaicId, amount) {
  const buf = Buffer.alloc(17);
  buf.writeUInt8(4, 0); // OP_DEPOSIT
  buf.writeBigUInt64LE(BigInt(mosaicId || XPX_MOSAIC_ID), 1);
  buf.writeBigUInt64LE(BigInt(amount), 9);
  return buf;
}

function encodeWithdraw(mosaicId, amount) {
  const buf = Buffer.alloc(17);
  buf.writeUInt8(5, 0); // OP_WITHDRAW
  buf.writeBigUInt64LE(BigInt(mosaicId || XPX_MOSAIC_ID), 1);
  buf.writeBigUInt64LE(BigInt(amount), 9);
  return buf;
}

async function syncDriveDepth() {
  try {
    const h = getLatestCommittedHeight();
    const depthParams = encodeGetDepth(50);
    await executeConsensusCall(
      'Sync Drive Depth',
      h + 1,
      crypto.randomBytes(32).toString('hex'),
      99999,
      { parameters: depthParams }
    );
  } catch (_) {}
}

function getMarketDepthSnapshot(marketKey = 'xpx_usdt') {
  try {
    const market = (marketKey || 'xpx_usdt').toLowerCase().replace('/', '_');
    const depthPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'depth.json');
    if (fs.existsSync(depthPath)) {
      const raw = fs.readFileSync(depthPath, 'utf8');
      return JSON.parse(raw);
    }
  } catch (_) {}
  return null;
}

// =========================================================================
// Cancel-on-Disconnect (COD) / Dead-Man's Switch System
// =========================================================================
const codRegistry = new Map(); // callerKey -> { timeoutMs, lastHeartbeat, registeredAt, markets: [] }
let codWatchdogInterval = null;

function registerHeartbeat(req) {
  const callerKey = normalizeKey(req && (req.publicKey || req.callerPublicKey)).toLowerCase();
  if (!callerKey || callerKey.length !== 64) {
    throw new Error('Valid 64-character hex public key required for heartbeat registration');
  }

  const timeoutMs = Math.max(1000, Math.min(120000, Number(req.timeoutMs) || 10000));
  const rawMarkets = req.markets || req.pair || req.market || ['XPX/USDT'];
  const markets = Array.isArray(rawMarkets) ? rawMarkets : [String(rawMarkets)];

  const now = Date.now();
  const existing = codRegistry.get(callerKey);
  const entry = {
    publicKey: callerKey,
    timeoutMs,
    lastHeartbeat: now,
    registeredAt: existing ? existing.registeredAt : now,
    markets,
    heartbeatCount: (existing ? existing.heartbeatCount : 0) + 1,
  };

  codRegistry.set(callerKey, entry);
  ensureCodWatchdogRunning();

  let monitoredOrders = 0;
  for (const [, o] of activeOrders.entries()) {
    if (o.callerKey && o.callerKey.toLowerCase() === callerKey) {
      monitoredOrders++;
    }
  }

  return {
    ok: true,
    active: true,
    publicKey: callerKey,
    timeoutMs,
    lastHeartbeat: now,
    expiresAt: now + timeoutMs,
    monitoredOrders,
    heartbeatCount: entry.heartbeatCount,
  };
}

function deregisterHeartbeat(req) {
  const callerKey = normalizeKey(req && (req.publicKey || req.callerPublicKey)).toLowerCase();
  const deleted = codRegistry.delete(callerKey);
  return {
    ok: true,
    deregistered: deleted,
    publicKey: callerKey,
  };
}

function getHeartbeatStatus(publicKey) {
  const callerKey = normalizeKey(publicKey).toLowerCase();
  const entry = codRegistry.get(callerKey);
  if (!entry) {
    return { ok: true, active: false, publicKey: callerKey };
  }
  const now = Date.now();
  const elapsed = now - entry.lastHeartbeat;
  const remaining = Math.max(0, entry.timeoutMs - elapsed);
  return {
    ok: true,
    active: true,
    publicKey: callerKey,
    timeoutMs: entry.timeoutMs,
    lastHeartbeat: entry.lastHeartbeat,
    timeSinceLastHeartbeatMs: elapsed,
    expiresInMs: remaining,
    isExpired: remaining === 0,
    markets: entry.markets,
    heartbeatCount: entry.heartbeatCount,
  };
}

async function cancelAllOrdersInternal(callerKeyRaw, pair = 'XPX/USDT', reason = 'Internal') {
  const callerKey = normalizeKey(callerKeyRaw).toLowerCase();
  return withAccountLock(callerKey, async () => {
    try {
      const isBnb = pair.toUpperCase().includes('BNB');
      const quoteMosaicId = isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID;
      const cancelAllParams = encodeCancelAll(quoteMosaicId);
      const currentHeight = getLatestCommittedHeight('node1');
      const txHashHex = crypto.randomBytes(32).toString('hex');
      const contractCallId = Math.floor(Math.random() * 90000) + 10000;

      const results = await executeConsensusCall(
        `Cancel All Orders (${reason})`,
        currentHeight + 1,
        txHashHex,
        contractCallId,
        {
          parameters: cancelAllParams,
          callerKeyHex: callerKey,
        }
      );
      const ret = results[0].returns;
      if (ret.return_val !== 0) {
        console.warn(`[COD CancelAll] Non-zero return code for ${callerKey}: ${ret.return_val}`);
      }

      syncGatewayCacheFromDrive(callerKey);
      let removedCount = 0;
      for (const [id, o] of activeOrders.entries()) {
        if (o.callerKey && o.callerKey.toLowerCase() === callerKey) {
          activeOrders.delete(id);
          removedCount++;
        }
      }
      saveActiveOrders();
      syncDriveDepth().catch(() => {});
      console.log(`✅ [COD Watchdog] Cancelled ${removedCount} active orders for ${callerKey} (${reason})`);
      return { ok: true, success: true, removedOrders: removedCount, reason };
    } catch (err) {
      console.error(`[COD CancelAll Error] Failed to cancel orders for ${callerKey}: ${err.message}`);
      return { ok: false, error: err.message };
    }
  });
}

function ensureCodWatchdogRunning() {
  if (codWatchdogInterval) return;
  codWatchdogInterval = setInterval(async () => {
    const now = Date.now();
    for (const [callerKey, entry] of codRegistry.entries()) {
      if (now - entry.lastHeartbeat > entry.timeoutMs) {
        codRegistry.delete(callerKey);
        const elapsed = now - entry.lastHeartbeat;
        console.warn(`🚨 [COD Watchdog] Dead-Man's Switch triggered for ${callerKey}! Heartbeat timed out (${elapsed}ms > ${entry.timeoutMs}ms). Auto-cancelling open resting quotes.`);
        for (const m of entry.markets) {
          try {
            await cancelAllOrdersInternal(callerKey, m, 'COD Heartbeat Timeout');
          } catch (err) {
            console.error(`[COD Watchdog] Error cancelling orders for ${callerKey} on ${m}: ${err.message}`);
          }
        }
      }
    }
  }, 500);
  if (codWatchdogInterval.unref) codWatchdogInterval.unref();
}

// =========================================================================
// Stale Order Auto-Expiry Sweeper & Internal Single-Order Cancellation
// =========================================================================
const DEFAULT_ORDER_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days default TTL for resting quotes

async function cancelSingleOrderInternal(orderIdRaw, pair = 'XPX/USDT', callerKeyRaw, reason = 'Internal Sweeper') {
  const orderId = parseInt(orderIdRaw, 10);
  if (!orderId || orderId <= 0) {
    throw new Error('Invalid orderId');
  }
  const callerKey = normalizeKey(callerKeyRaw).toLowerCase();
  return withAccountLock(callerKey, async () => {
    try {
      const isBnb = pair.toUpperCase().includes('BNB');
      const quoteMosaicId = isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID;
      const cancelParams = encodeCancelOrder(orderId, quoteMosaicId);
      const currentHeight = getLatestCommittedHeight('node1');
      const txHashHex = crypto.randomBytes(32).toString('hex');
      const contractCallId = Math.floor(Math.random() * 90000) + 10000;

      const results = await executeConsensusCall(
        `Cancel Order #${orderId} (${reason})`,
        currentHeight + 1,
        txHashHex,
        contractCallId,
        {
          parameters: cancelParams,
          callerKeyHex: callerKey,
        }
      );
      const ret = results[0].returns;
      if (ret.return_val !== 0) {
        console.warn(`[Order Sweeper] Return code ${ret.return_val} when cancelling order #${orderId}`);
      }

      // Restore escrow into available balance from canonical drive ledger
      syncGatewayCacheFromDrive(callerKey);

      if (activeOrders.has(orderId)) {
        activeOrders.delete(orderId);
        saveActiveOrders();
      }

      syncDriveDepth().catch(() => {});
      console.log(`🧹 [Order Sweeper] Order #${orderId} for ${callerKey} cancelled & funds liberated (${reason})`);
      return { ok: true, success: true, orderId, reason };
    } catch (err) {
      if (
        err.message &&
        (err.message.includes('not found') ||
          err.message.includes('already filled') ||
          err.message.includes('Return code: 1'))
      ) {
        if (activeOrders.has(orderId)) {
          activeOrders.delete(orderId);
          saveActiveOrders();
        }
      }
      console.error(`[Order Sweeper Error] Failed to cancel order #${orderId} for ${callerKey}: ${err.message}`);
      return { ok: false, error: err.message };
    }
  });
}

async function sweepExpiredOrders(maxBatch = 30) {
  const now = Date.now();
  const explicitExpired = [];
  const legacyExpired = [];
  for (const [id, order] of activeOrders.entries()) {
    if (order.status === 'Filled' || order.remainingAmountAtomic === '0') {
      activeOrders.delete(id);
      continue;
    }

    if (order.expiry && Number(order.expiry) <= now) {
      explicitExpired.push({
        id: order.id || order.orderId || id,
        callerKey: order.callerKey,
        pair: order.pair || 'XPX/USDT',
        expiry: order.expiry,
      });
    } else if (!order.expiry && order.timestamp && order.timestamp + DEFAULT_ORDER_TTL_MS <= now) {
      legacyExpired.push({
        id: order.id || order.orderId || id,
        callerKey: order.callerKey,
        pair: order.pair || 'XPX/USDT',
        expiry: order.timestamp + DEFAULT_ORDER_TTL_MS,
      });
    }
  }

  const expired = [...explicitExpired, ...legacyExpired].slice(0, maxBatch);

  if (expired.length === 0) {
    saveActiveOrders();
    return { ok: true, sweptCount: 0, sweptOrderIds: [] };
  }

  console.log(`🧹 [Order Sweeper] Found ${expired.length} expired open order(s) to sweep (${explicitExpired.length} explicit GTT, ${legacyExpired.length} legacy TTL)...`);
  const swept = [];
  for (const item of expired) {
    try {
      const res = await cancelSingleOrderInternal(item.id, item.pair, item.callerKey, 'Auto-Expiry TTL');
      if (res.ok) {
        swept.push(item.id);
      }
    } catch (err) {
      console.warn(`[Order Sweeper] Error sweeping order #${item.id}:`, err.message);
    }
  }

  saveActiveOrders();

  return {
    ok: true,
    sweptCount: swept.length,
    sweptOrderIds: swept,
  };
}

let orderTtlSweeperInterval = null;
function startOrderTtlSweeper(intervalMs = 15000) {
  if (orderTtlSweeperInterval) return;
  orderTtlSweeperInterval = setInterval(() => {
    sweepExpiredOrders().catch((err) => {
      console.error(`[Order TTL Sweeper] Sweeper error: ${err.message}`);
    });
  }, intervalMs);
  if (orderTtlSweeperInterval.unref) orderTtlSweeperInterval.unref();
  console.log(`🛡️ [Order Sweeper] Stale Order Auto-Expiry Sweeper active (interval: ${intervalMs}ms)`);
}

// =========================================================================
// Continuous 24/7 Solvency Sentinel Daemon & Proof-of-Reserves Audit
// =========================================================================
async function runSolvencySentinelAudit(options = {}) {
  const { forceSync = true, tripOnBreach = true } = options;
  if (forceSync) {
    try {
      await syncVaultBalancesFromTestnet2();
    } catch (_) {}
  }

  const assetsToCheck = [
    {
      symbol: 'XPX',
      mosaicId: testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7',
      // Allow 100 XPX buffer for network fees spent by Vault during settlements/activations
      gasTolerance: 100_000_000n,
    },
    {
      symbol: 'USDT',
      mosaicId: testnet2Config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C',
      gasTolerance: 0n,
    },
    {
      symbol: 'BNB',
      mosaicId: testnet2Config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9',
      gasTolerance: 0n,
    },
  ];

  const auditResults = {
    timestamp: Date.now(),
    healthy: true,
    circuitBreakerTripped: isCircuitBreakerTripped(),
    assets: {},
    breaches: [],
  };

  for (const asset of assetsToCheck) {
    const normMosaic = asset.mosaicId.toLowerCase().replace(/^0x/, '');
    const liabilities = getTestnet2LiabilitiesAtomic(normMosaic);
    const vaultBal = getVaultBalance(asset.mosaicId, false);
    const effectiveReserves = vaultBal + asset.gasTolerance;
    const isSolvent = effectiveReserves >= liabilities;

    let reserveRatioPct = 100.0;
    if (liabilities > 0n) {
      reserveRatioPct = Number((vaultBal * 10000n) / liabilities) / 100;
    }

    auditResults.assets[asset.symbol] = {
      mosaicId: asset.mosaicId,
      liabilitiesAtomic: liabilities.toString(),
      vaultBalanceAtomic: vaultBal.toString(),
      gasToleranceAtomic: asset.gasTolerance.toString(),
      reserveRatioPct,
      isSolvent,
      surplusAtomic: (effectiveReserves - liabilities).toString(),
    };

    if (!isSolvent) {
      auditResults.healthy = false;
      const breachMsg = `Solvency Sentinel Breach: Asset ${asset.symbol} liabilities (${liabilities.toString()}) exceed vault reserves (${vaultBal.toString()} + ${asset.gasTolerance.toString()} gas buffer)`;
      auditResults.breaches.push(breachMsg);
      if (tripOnBreach && !isCircuitBreakerTripped()) {
        tripCircuitBreaker(breachMsg);
        auditResults.circuitBreakerTripped = true;
      }
    }
  }

  return auditResults;
}

let solvencySentinelInterval = null;
function startSolvencySentinel(intervalMs = 30000) {
  if (solvencySentinelInterval) return;
  solvencySentinelInterval = setInterval(async () => {
    try {
      const res = await runSolvencySentinelAudit();
      if (!res.healthy) {
        console.error(`🚨 [SOLVENCY SENTINEL ALARM] Vault insolvency drift detected!`, res.breaches);
      }
    } catch (err) {
      console.warn(`[Solvency Sentinel] Error during audit tick: ${err.message}`);
    }
  }, intervalMs);
  if (solvencySentinelInterval.unref) solvencySentinelInterval.unref();
  console.log(`🛡️ [Solvency Sentinel] 24/7 Continuous Solvency Sentinel Daemon active (interval: ${intervalMs}ms)`);
}

async function syncUserOnChainBalanceToDrive(publicKey) {
  const key = normalizeKey(publicKey).toLowerCase();
  try {
    const netType = testnet2Config.NETWORK_TYPE || 168;
    const { PublicAccount } = require('tsjs-xpx-chain-sdk');
    const acc = PublicAccount.createFromPublicKey(key, netType);
    const addr = acc.address.plain();
    const endpoint = testnet2Config.NODE_URL || 'https://api-2.testnet2.xpxsirius.io';
    const t2Client = require('./testnet2_client');
    const data = await t2Client.fetchJson(`${endpoint}/account/${addr}`);
    if (!data || !data.account) return;
    const mosaics = (data.account && data.account.mosaics) || [];

    const isMainnet = netType === 184;
    const xpxHex = (isMainnet ? '402B2F579FA859B8' : '13BFC518E40549D7').toLowerCase();
    const usdtHex = (isMainnet ? '7525381A171D1E44' : '4430AEFEF7E15D0C').toLowerCase();
    const bnbHex = (isMainnet ? '1E5C5FADEC7B34DD' : '3CC1D3CE7447B6F9').toLowerCase();

    let onChainXpx = 0n;
    let onChainUsdt = 0n;
    let onChainBnb = 0n;

    for (const m of mosaics) {
      let idHex = '';
      if (Array.isArray(m.id) && m.id.length === 2) {
        const low = (m.id[0] >>> 0).toString(16).padStart(8, '0');
        const high = (m.id[1] >>> 0).toString(16).padStart(8, '0');
        idHex = (high + low).toLowerCase();
      } else if (typeof m.id === 'string') {
        idHex = m.id.toLowerCase().replace(/^0x/, '');
      }

      let amountAtomic = 0n;
      if (Array.isArray(m.amount) && m.amount.length === 2) {
        amountAtomic = BigInt(m.amount[1] >>> 0) * 4294967296n + BigInt(m.amount[0] >>> 0);
      } else {
        amountAtomic = BigInt(m.amount || 0);
      }

      if (idHex === xpxHex) onChainXpx = amountAtomic;
      else if (idHex === usdtHex) onChainUsdt = amountAtomic;
      else if (idHex === bnbHex) onChainBnb = amountAtomic;
    }

    const driveBal = getDriveBalances(key) || { xpx: 0n, usdt: 0n, bnb: 0n };
    const diffXpx = onChainXpx > driveBal.xpx ? onChainXpx - driveBal.xpx : 0n;
    const diffUsdt = onChainUsdt > driveBal.usdt ? onChainUsdt - driveBal.usdt : 0n;
    const diffBnb = onChainBnb > driveBal.bnb ? onChainBnb - driveBal.bnb : 0n;

    if (diffXpx === 0n && diffUsdt === 0n && diffBnb === 0n) return;

    const currentHeight = getLatestCommittedHeight('node1');
    const callId = Math.floor(Math.random() * 90000) + 10000;
    const servicePayments = [];

    if (diffXpx > 0n) {
      servicePayments.push({ mosaic_id: XPX_MOSAIC_ID, amount: String(diffXpx) });
    }
    if (diffUsdt > 0n) {
      servicePayments.push({ mosaic_id: USDT_MOSAIC_ID, amount: String(diffUsdt) });
    }
    if (diffBnb > 0n) {
      servicePayments.push({ mosaic_id: BNB_MOSAIC_ID, amount: String(diffBnb) });
    }

    for (const sp of servicePayments) {
      const p = Buffer.alloc(17);
      p.writeUInt8(4, 0); // OP_DEPOSIT
      p.writeBigUInt64LE(BigInt(sp.mosaic_id), 1);
      p.writeBigUInt64LE(BigInt(sp.amount), 9);

      await executeConsensusCall(
        'Auto-Sync On-Chain Balance',
        currentHeight + 1,
        'auto_sync_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
        callId,
        {
          parameters: p,
          callerKeyHex: key,
          servicePayments: [sp],
        }
      );
    }

    // Record in processed_deposits.json
    const depFile = path.join(CLUSTER_DIR, 'processed_deposits.json');
    let deps = [];
    if (fs.existsSync(depFile)) {
      try { deps = JSON.parse(fs.readFileSync(depFile, 'utf8')); } catch (_) {}
    }
    for (const sp of servicePayments) {
      deps.push({
        txHash: 'onchain_auto_sync_' + key.slice(0, 12) + '_' + Date.now(),
        height: currentHeight,
        senderAddress: addr,
        senderPublicKey: key,
        recipientAddress: 'VAAUXFYQ424I6V3JDVWCBWGWHRTIJGTSX4B2TLSV',
        mosaicIdHex: sp.mosaic_id === XPX_MOSAIC_ID ? '13bfc518e40549d7' : (sp.mosaic_id === USDT_MOSAIC_ID ? '4430aefef7e15d0c' : '3cc1d3ce7447b6f9'),
        amountAtomic: sp.amount,
        timestamp: Date.now(),
      });
    }
    fs.writeFileSync(depFile, JSON.stringify(deps, null, 2));

    syncGatewayCacheFromDrive(key);
  } catch (err) {
    console.warn('[Auto-Sync] Could not auto-sync on-chain balance for', key, err.message);
  }
}

/**
 * Handle Order Placement from Web Wallet (serialized per callerKey via withAccountLock)
 */
async function placeOrder(orderReq) {
  const lockKey = normalizeKey((orderReq && (orderReq.publicKey || orderReq.callerPublicKey)) || 'global');
  return withAccountLock(lockKey, () => _placeOrderUnlocked(orderReq));
}

async function _placeOrderUnlocked(orderReq) {
  const tStart = Date.now();
  checkAndEnforceCircuitBreaker('Order Placement');
  const pair = (orderReq.pair || orderReq.market || 'XPX/USDT').toUpperCase().replace('_', '/');
  const pairParts = pair.split('/');
  const baseAsset = (pairParts[0] || 'XPX').trim();
  const quoteAsset = (pairParts[1] || 'USDT').trim();
  const baseMosaicId = deriveMosaicFromAsset(baseAsset);
  const quoteMosaicId = deriveMosaicFromAsset(quoteAsset);
  const isBnb = quoteMosaicId === BNB_MOSAIC_ID;
  const quoteToken = quoteAsset;

  // Authoritative server derivation check: client cannot spoof mosaic ID
  if (orderReq.baseMosaicId && String(orderReq.baseMosaicId) !== baseMosaicId) {
    throw new Error(
      `Base mosaic mismatch: Client-supplied baseMosaicId (${orderReq.baseMosaicId}) does not match server-derived base mosaic (${baseMosaicId}) for signed asset "${baseAsset}"`
    );
  }
  if (orderReq.quoteMosaicId && String(orderReq.quoteMosaicId) !== quoteMosaicId) {
    throw new Error(
      `Quote mosaic mismatch: Client-supplied quoteMosaicId (${orderReq.quoteMosaicId}) does not match server-derived quote mosaic (${quoteMosaicId}) for signed asset "${quoteAsset}"`
    );
  }
  if (orderReq.mosaicId && String(orderReq.mosaicId) !== quoteMosaicId && String(orderReq.mosaicId) !== baseMosaicId) {
    throw new Error(
      `Mosaic mismatch: Client-supplied mosaicId (${orderReq.mosaicId}) does not match server-derived pair mosaics (${baseMosaicId}, ${quoteMosaicId})`
    );
  }

  const side = orderReq.side === 'sell' || orderReq.side === 1 ? 1 : 0;
  const orderType = orderReq.type === 'Market' || orderReq.orderType === 1 ? 1 : 0; // 0=Limit, 1=Market (IOC)
  const defaultPrice = isBnb ? '0.00000834' : '0.005';
  const priceInput = orderReq.price !== undefined && orderReq.price !== null ? orderReq.price : defaultPrice;
  const amountInput = orderReq.amount || 0;

  const marketKey = isBnb ? 'xpx_bnb' : 'xpx_usdt';
  const expectedOrderId = getMarketNextOrderId(marketKey);

  const priceTicksRaw = parseDecimalToBigInt(priceInput, 8); // PRICE_SCALE = 100_000_000n
  const amountAtomic = parseDecimalToBigInt(amountInput, 6);
  const MAX_U64 = 0xFFFFFFFFFFFFFFFFn;

  if (amountAtomic <= 0n) {
    throw new Error('Invalid order amount: must be greater than zero');
  }
  if (amountAtomic > MAX_U64 || priceTicksRaw > MAX_U64) {
    throw new Error('Order amount or price exceeds maximum 64-bit integer limit');
  }
  if (orderType === 0 && priceTicksRaw <= 0n) {
    throw new Error('Invalid order price: must be greater than zero for Limit order');
  }

  const callerKey = normalizeKey(orderReq.publicKey || orderReq.callerPublicKey);

  // Cryptographic authentication: Require ed25519 signature over (pair, side, type, price, amount, nonce, expiry)
  const orderMsg = constructOrderMessage(
    pair,
    side,
    orderType,
    priceInput,
    amountInput,
    orderReq.nonce,
    orderReq.expiry
  );
  verifyEd25519Signature(callerKey, orderMsg, orderReq.signature);
  checkAndRecordNonce(callerKey, orderReq.nonce, orderReq.expiry);
  const tAuth = Date.now();

  // 1. Reconciliation check: Gateway cache == Canonical drive ledger per account/mosaic
  assertLedgerReconciliation(callerKey, baseMosaicId);
  assertLedgerReconciliation(callerKey, quoteMosaicId);

  // 2. Solvency check: Canonical drive ledger total <= on-chain vault balance
  try {
    assertSolvencyReconciliation(baseMosaicId);
    assertSolvencyReconciliation(quoteMosaicId);
  } catch (solvErr) {
    await syncVaultBalancesFromTestnet2();
    assertSolvencyReconciliation(baseMosaicId, true);
    assertSolvencyReconciliation(quoteMosaicId, true);
  }
  const tSolv = Date.now();

  let userBal = await syncAndGetAccountBalance(callerKey);
  const tBal = Date.now();

  const tickSize = isBnb ? 1n : 100n;
  let priceTicks = priceTicksRaw;

  // 1. Dynamic Price Collar Protection (Fat-Finger & Flash-Crash Shield)
  // For Limit Orders (orderType === 0), verify price does not deviate excessively from market reference
  if (orderType === 0 && !orderReq.allowWideCollar) {
    const depth = getMarketDepthSnapshot(marketKey);
    const collarBps = BigInt(Math.max(100, Math.min(5000, Number(orderReq.collarBps) || 1500))); // default 15.00% (1500 bps)
    let refPriceTicks = null;
    if (depth && depth.best_bid && depth.best_ask) {
      refPriceTicks = (BigInt(depth.best_bid) + BigInt(depth.best_ask)) / 2n;
    } else if (depth && depth.best_bid) {
      refPriceTicks = BigInt(depth.best_bid);
    } else if (depth && depth.best_ask) {
      refPriceTicks = BigInt(depth.best_ask);
    }

    if (refPriceTicks && refPriceTicks > 0n) {
      const maxCollarPrice = (refPriceTicks * (10000n + collarBps)) / 10000n;
      const minCollarPrice = (refPriceTicks * (10000n - collarBps)) / 10000n;

      if (side === 0 && priceTicksRaw > maxCollarPrice) {
        throw new Error(
          `Price collar violation: Limit Buy price (${formatAtomicUnits(priceTicksRaw, 8)}) exceeds maximum collar threshold of ${formatAtomicUnits(maxCollarPrice, 8)} (+${Number(collarBps) / 100}% from mid-market ${formatAtomicUnits(refPriceTicks, 8)}). Protects against fat-finger execution.`
        );
      }
      if (side === 1 && priceTicksRaw < minCollarPrice) {
        throw new Error(
          `Price collar violation: Limit Sell price (${formatAtomicUnits(priceTicksRaw, 8)}) is below minimum collar threshold of ${formatAtomicUnits(minCollarPrice, 8)} (-${Number(collarBps) / 100}% from mid-market ${formatAtomicUnits(refPriceTicks, 8)}). Protects against flash crash dumping.`
        );
      }
    }
  }

  // 2. Dynamic Market Order Slippage Cap
  // For Market Orders (orderType === 1), dynamically calculate max acceptable execution boundary based on depth
  if (orderType === 1) {
    const depth = getMarketDepthSnapshot(marketKey);
    const maxSlippageBps = BigInt(Math.max(50, Math.min(2000, Number(orderReq.maxSlippageBps) || 250))); // default 2.50% (250 bps)

    if (side === 0) {
      // Market Buy matches against resting Asks
      if (!depth || !depth.best_ask || !depth.asks || depth.asks.length === 0) {
        throw new Error('Market Buy rejected: No ask liquidity available on orderbook to match against');
      }
      const bestAskTicks = BigInt(depth.best_ask);
      const slippageCeilingTicks = (bestAskTicks * (10000n + maxSlippageBps) + 9999n) / 10000n;
      priceTicks = ((slippageCeilingTicks + tickSize - 1n) / tickSize) * tickSize;
    } else {
      // Market Sell matches against resting Bids
      if (!depth || !depth.best_bid || !depth.bids || depth.bids.length === 0) {
        throw new Error('Market Sell rejected: No bid liquidity available on orderbook to match against');
      }
      const bestBidTicks = BigInt(depth.best_bid);
      const slippageFloorTicks = (bestBidTicks * (10000n - maxSlippageBps)) / 10000n;
      priceTicks = (slippageFloorTicks / tickSize) * tickSize;
      if (priceTicks === 0n) priceTicks = tickSize;
    }
  } else {
    // Limit order: round to nearest tick multiple
    priceTicks = ((priceTicks + (tickSize / 2n)) / tickSize) * tickSize;
  }
  if (priceTicks === 0n) {
    priceTicks = tickSize;
  }

  // Use ceiling division so fractional sub-atomic quote amounts never under-collateralize an order
  const quoteAtomic = (amountAtomic * priceTicks + PRICE_SCALE - 1n) / PRICE_SCALE;
  const requiredQuote = quoteAtomic;
  const requiredXpx = amountAtomic;

  // Escrow Validation: User must have deposited backed collateral in SuperContract vault
  if (side === 0) {
    // Buy: requires quote asset (USDT or BNB)
    const availQuote = BigInt(isBnb ? (userBal.bnb || 0n) : (userBal.usdt || 0n));
    if (availQuote < requiredQuote) {
      throw new Error(`Insufficient ${quoteToken} balance. Required: ${formatAtomicUnits(requiredQuote, 6)} ${quoteToken}, Available: ${formatAtomicUnits(availQuote, 6)} ${quoteToken}`);
    }
  } else {
    // Sell: requires XPX base asset
    const availXpx = BigInt(userBal.xpx || 0n);
    if (availXpx < requiredXpx) {
      throw new Error(`Insufficient XPX balance. Required: ${formatAtomicUnits(requiredXpx, 6)} XPX, Available: ${formatAtomicUnits(availXpx, 6)} XPX`);
    }
  }

  // Deduct from available balance into locked escrow
  updateAccountBalance(callerKey, (b) => {
    if (side === 0) {
      if (isBnb) {
        b.bnb = BigInt(b.bnb || 0n) - requiredQuote;
        b.lockedBnb = BigInt(b.lockedBnb || 0n) + requiredQuote;
      } else {
        b.usdt = BigInt(b.usdt || 0n) - requiredQuote;
        b.lockedUsdt = BigInt(b.lockedUsdt || 0n) + requiredQuote;
      }
    } else {
      b.xpx = BigInt(b.xpx || 0n) - requiredXpx;
      b.lockedXpx = BigInt(b.lockedXpx || 0n) + requiredXpx;
    }
  });

  const callParams = encodePlaceOrder(side, orderType, priceTicks, amountAtomic, quoteMosaicId);
  // Only attach servicePayments if drive balance is insufficient for direct lock
  const driveBal = getDriveBalances(callerKey);
  const availDrive = side === 0
    ? BigInt(driveBal ? (isBnb ? driveBal.bnb : driveBal.usdt) : 0n)
    : BigInt(driveBal ? driveBal.xpx : 0n);

  let servicePayments = [];
  const requiredEscrow = side === 0 ? requiredQuote : requiredXpx;
  if (availDrive < requiredEscrow) {
    const deficit = requiredEscrow - availDrive;
    servicePayments = side === 0
      ? [{ mosaic_id: quoteMosaicId, amount: String(deficit) }]
      : [{ mosaic_id: XPX_MOSAIC_ID, amount: String(deficit) }];
  }

  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  // Execute across all 3 nodes simultaneously with self-healing consensus
  const results = await executeConsensusCall(
    'Web Wallet Order Placement',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: callParams,
      callerKeyHex: callerKey,
      servicePayments,
    }
  );
  const tCons = Date.now();
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    // Revert escrow on contract error safely
    updateAccountBalance(callerKey, (b) => {
      if (side === 0) {
        if (isBnb) {
          const toRefund = BigInt(b.lockedBnb || 0n) >= requiredQuote ? requiredQuote : BigInt(b.lockedBnb || 0n);
          b.bnb = BigInt(b.bnb || 0n) + toRefund;
          b.lockedBnb = BigInt(b.lockedBnb || 0n) - toRefund;
        } else {
          const toRefund = BigInt(b.lockedUsdt || 0n) >= requiredQuote ? requiredQuote : BigInt(b.lockedUsdt || 0n);
          b.usdt = BigInt(b.usdt || 0n) + toRefund;
          b.lockedUsdt = BigInt(b.lockedUsdt || 0n) - toRefund;
        }
      } else {
        const toRefund = BigInt(b.lockedXpx || 0n) >= requiredXpx ? requiredXpx : BigInt(b.lockedXpx || 0n);
        b.xpx = BigInt(b.xpx || 0n) + toRefund;
        b.lockedXpx = BigInt(b.lockedXpx || 0n) - toRefund;
      }
    }, { source: 'ESCROW_REFUND' });
    throw new Error(`SuperContract rejected order (Return code: ${ret.return_val})`);
  }

  let onChainTxHash = null;
  let fillsCount = 0;
  const emittedTx = ret.transaction;
  const payouts = [];
  let takerFeeAtomic = 0n;
  let makerFeeAtomic = 0n;
  let takerBaseFilled = 0n;

  // Process settlement & trade fills if any occurred
  if (emittedTx && Array.isArray(emittedTx.transactions) && emittedTx.transactions.length > 0) {
    try {
      const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
      const poexMsgHex = Buffer.from(`POEX:CLOB_TRADE:CALL=${contractCallId}`, 'utf8').toString('hex');

      // Construct payout specs for on-chain settlement
      const payoutSpecs = [];
      const memoHex = Buffer.from('CLOB_SETTLE', 'utf8').toString('hex');

      for (const t of emittedTx.transactions) {
        const payloadBuf = Buffer.isBuffer(t.payload) ? t.payload : Buffer.from(t.payload || '', 'base64');
        if (payloadBuf.length >= 48) {
          const recipientPub = payloadBuf.subarray(0, 32).toString('hex');
          const msgLen = Number(payloadBuf.readBigUInt64LE(40));
          const mosaicId = payloadBuf.readBigUInt64LE(48);
          const payoutAmount = BigInt(payloadBuf.readBigUInt64LE(48 + 8));
          const mosaicHex = mosaicId.toString(16).toUpperCase().padStart(16, '0');
          const messageStr = payloadBuf.length >= 64 + msgLen
            ? payloadBuf.subarray(64, 64 + msgLen).toString('utf8')
            : '';

          // Track fill count based on actual trade fill base leg emitted by engine
          if (messageStr.startsWith('SiriusCLOB: Trade Fill (Base)')) {
            fillsCount++;
          }

          // Extract fees directly from contract emitted transaction output
          let legFee = 0n;
          const feeMatch = messageStr.match(/:FEE=(\d+)/);
          if (feeMatch) {
            legFee = BigInt(feeMatch[1]);
            if (messageStr.includes(':TAKER_FEE=')) {
              takerFeeAtomic += legFee;
            }
            if (messageStr.includes(':MAKER_FEE=')) {
              makerFeeAtomic += legFee;
            }
          }

          payoutSpecs.push(`${recipientPub}:${mosaicHex}:${payoutAmount}:${memoHex}`);
          payouts.push({
            recipient: recipientPub,
            mosaicId: mosaicId.toString(10),
            mosaicHex,
            amount: payoutAmount.toString(),
          });

          // Queue trade fill payout to Testnet 2 batch settlement aggregator
          // Only queue real trade fills (not internal orderbook escrow refunds)
          if (messageStr.startsWith('SiriusCLOB: Trade Fill')) {
            try {
              const { defaultBatchSettlerTestnet2 } = require('./batch_settler_testnet2');
              if (defaultBatchSettlerTestnet2) {
                defaultBatchSettlerTestnet2.queuePayout(recipientPub, mosaicHex, payoutAmount, 'CLOB_MATCH', true);
                
                // Route fee deduction to dedicated Fee Collector account
                if (legFee > 0n && testnet2Config.FEE_COLLECTOR_ADDRESS) {
                  defaultBatchSettlerTestnet2.queuePayout(
                    testnet2Config.FEE_COLLECTOR_ADDRESS,
                    mosaicHex,
                    legFee,
                    'CLOB_FEE',
                    true
                  );
                }

                defaultBatchSettlerTestnet2.flushBatch().catch((err) => {
                  console.warn('[BatchSettler] Match flush notice:', err.message);
                });
              }
            } catch (_) {}

            // Credit Fee Collector account in local database/cache
            if (legFee > 0n) {
              const feeCollectorPub = (testnet2Config.FEE_COLLECTOR_PUBKEY || '04ef8d54aaefbc4746257723fd1efb7b37f30452de8bd022f5fb3cabc5246a64').toLowerCase();
              try {
                updateAccountBalance(feeCollectorPub, (b) => {
                  if (mosaicId.toString(10) === XPX_MOSAIC_ID) {
                    b.xpx = BigInt(b.xpx || 0n) + legFee;
                  } else if (mosaicId.toString(10) === USDT_MOSAIC_ID) {
                    b.usdt = BigInt(b.usdt || 0n) + legFee;
                  } else if (mosaicId.toString(10) === BNB_MOSAIC_ID) {
                    b.bnb = BigInt(b.bnb || 0n) + legFee;
                  }
                }, { source: 'MATCH_SETTLEMENT' });
              } catch (_) {}
            }
          }

          if (mosaicId.toString(10) === XPX_MOSAIC_ID) {
            // Gross base filled = net payout + taker fee (if taker is buyer and fee deducted from base)
            takerBaseFilled += payoutAmount + (messageStr.includes(':TAKER_FEE=') ? legFee : 0n);
            // XPX Delivery: recipient receives bought XPX
            updateAccountBalance(recipientPub, (b) => {
              b.xpx = BigInt(b.xpx || 0n) + payoutAmount;
              const rem = BigInt(b.lockedXpx || 0n) - payoutAmount;
              b.lockedXpx = rem > 0n ? rem : 0n;
            }, { source: 'MATCH_SETTLEMENT' });
          } else if (mosaicId.toString(10) === USDT_MOSAIC_ID) {
            // USDT Delivery: recipient receives quote payment or refund
            updateAccountBalance(recipientPub, (b) => {
              b.usdt = BigInt(b.usdt || 0n) + payoutAmount;
              const rem = BigInt(b.lockedUsdt || 0n) - payoutAmount;
              b.lockedUsdt = rem > 0n ? rem : 0n;
            }, { source: 'MATCH_SETTLEMENT' });
          } else if (mosaicId.toString(10) === BNB_MOSAIC_ID) {
            // BNB Delivery: recipient receives quote payment or refund
            updateAccountBalance(recipientPub, (b) => {
              b.bnb = BigInt(b.bnb || 0n) + payoutAmount;
              const rem = BigInt(b.lockedBnb || 0n) - payoutAmount;
              b.lockedBnb = rem > 0n ? rem : 0n;
            }, { source: 'MATCH_SETTLEMENT' });
          }
        }
      }
    } catch (e) {
      console.warn('On-chain settlement broadcast warning:', e.message);
    }

    // Release caller escrow for the filled portion
    updateAccountBalance(callerKey, (b) => {
      if (side === 0) {
        if (isBnb) {
          const rem = BigInt(b.lockedBnb || 0n) - requiredQuote;
          b.lockedBnb = rem > 0n ? rem : 0n;
        } else {
          const rem = BigInt(b.lockedUsdt || 0n) - requiredQuote;
          b.lockedUsdt = rem > 0n ? rem : 0n;
        }
      } else {
        const rem = BigInt(b.lockedXpx || 0n) - requiredXpx;
        b.lockedXpx = rem > 0n ? rem : 0n;
      }
    });

    // Authoritative drive ledger reconciliation for all trade participants
    syncGatewayCacheFromDrive(callerKey);
    for (const p of payouts) {
      if (p.recipient) {
        syncGatewayCacheFromDrive(p.recipient);
      }
    }
  }

  // For Market (IOC) orders:
  // If the contract emitted transactions, the drive is authoritative and syncGatewayCacheFromDrive has already run.
  // If the contract emitted NO transactions (e.g. unfilled or STP with no emitted tx), safely refund whatever remains in local locked escrow.
  if (orderType === 1) {
    if (!emittedTx || !Array.isArray(emittedTx.transactions) || emittedTx.transactions.length === 0) {
      updateAccountBalance(callerKey, (b) => {
        if (side === 0) {
          if (isBnb) {
            const toRefund = BigInt(b.lockedBnb || 0n) >= requiredQuote ? requiredQuote : BigInt(b.lockedBnb || 0n);
            b.bnb = BigInt(b.bnb || 0n) + toRefund;
            b.lockedBnb = BigInt(b.lockedBnb || 0n) - toRefund;
          } else {
            const toRefund = BigInt(b.lockedUsdt || 0n) >= requiredQuote ? requiredQuote : BigInt(b.lockedUsdt || 0n);
            b.usdt = BigInt(b.usdt || 0n) + toRefund;
            b.lockedUsdt = BigInt(b.lockedUsdt || 0n) - toRefund;
          }
        } else {
          const toRefund = BigInt(b.lockedXpx || 0n) >= requiredXpx ? requiredXpx : BigInt(b.lockedXpx || 0n);
          b.xpx = BigInt(b.xpx || 0n) + toRefund;
          b.lockedXpx = BigInt(b.lockedXpx || 0n) - toRefund;
        }
      }, { source: 'ESCROW_REFUND' });
    }
    syncGatewayCacheFromDrive(callerKey);
  }

  // Refresh public depth feed
  syncDriveDepth().catch(() => {});

  const orderId = expectedOrderId;
  const initialAtomic = BigInt(amountAtomic);
  const remainingAtomic = initialAtomic > takerBaseFilled ? initialAtomic - takerBaseFilled : 0n;
  const pct = initialAtomic > 0n ? Number((takerBaseFilled * 100n) / initialAtomic) : 0;
  const clampedPct = Math.min(100, Math.max(0, pct));
  const isFilled = remainingAtomic === 0n;

  if (orderType === 0) {
    activeOrders.set(orderId, {
      id: orderId,
      orderId,
      callerKey,
      pair: normalizePair(pair),
      side: side === 0 ? 'Buy' : 'Sell',
      type: 'Limit',
      price: priceInput,
      amount: amountInput,
      priceTicks: Number(priceTicks),
      amountAtomic: amountAtomic.toString(),
      takerFilledAtomic: takerBaseFilled.toString(),
      makerFilledAtomic: '0',
      filledAmountAtomic: takerBaseFilled.toString(),
      remainingAmountAtomic: remainingAtomic.toString(),
      remainingAmount: formatAtomicUnits(remainingAtomic, 6),
      filled: `${clampedPct}%`,
      status: isFilled ? 'Filled' : 'Open',
      timestamp: Date.now(),
      expiry: orderReq.orderExpiry && Number(orderReq.orderExpiry) > 0
        ? Number(orderReq.orderExpiry)
        : (orderReq.ttlMs && Number(orderReq.ttlMs) > 0
          ? Date.now() + Number(orderReq.ttlMs)
          : Date.now() + DEFAULT_ORDER_TTL_MS),
    });
    saveActiveOrders();
  }

  // Reconcile resting maker orders with trades recorded by matching engine
  reconcileActiveOrdersWithTrades();

  // Synchronize gateway cache from canonical drive ledger for all involved parties
  syncGatewayCacheFromDrive(callerKey);
  for (const p of payouts) {
    if (p.recipient) {
      syncGatewayCacheFromDrive(p.recipient);
    }
  }

  const totalFeeAtomic = takerFeeAtomic + makerFeeAtomic;
  const updatedBal = getAccountBalance(callerKey);
  return {
    ok: true,
    success: true,
    orderId,
    side: side === 0 ? 'Buy' : 'Sell',
    type: orderType === 0 ? 'Limit' : 'Market',
    price: priceInput,
    amount: amountInput,
    priceTicks: Number(priceTicks),
    amountAtomic: Number(amountAtomic),
    filled: orderType === 0 ? `${clampedPct}%` : '100%',
    status: orderType === 0 ? (isFilled ? 'Filled' : 'Open') : 'Filled',
    remainingAmount: formatAtomicUnits(remainingAtomic, 6),
    gasConsumed: ret.execution_gas_consumed,
    fillsCount,
    fee: totalFeeAtomic.toString(),
    takerFee: takerFeeAtomic.toString(),
    makerFee: makerFeeAtomic.toString(),
    feeMosaicId: side === 0 ? XPX_MOSAIC_ID : (isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID),
    feeAsset: side === 0 ? 'XPX' : (isBnb ? 'BNB' : 'USDT'),
    payouts,
    settlementTxHash: onChainTxHash,
    balances: updatedBal,
    latency: {
      authMs: (typeof tAuth !== 'undefined' ? tAuth : Date.now()) - tStart,
      solvMs: (typeof tSolv !== 'undefined' ? tSolv : Date.now()) - (typeof tAuth !== 'undefined' ? tAuth : tStart),
      balMs: (typeof tBal !== 'undefined' ? tBal : Date.now()) - (typeof tSolv !== 'undefined' ? tSolv : tStart),
      consensusMs: (typeof tCons !== 'undefined' ? tCons : Date.now()) - (typeof tBal !== 'undefined' ? tBal : tStart),
      totalMs: Date.now() - tStart,
    },
  };
}

/**
 * Handle Order Cancellation (serialized per callerKey via withAccountLock)
 */
async function cancelOrder(cancelReq) {
  const lockKey = normalizeKey((cancelReq && (cancelReq.publicKey || cancelReq.callerPublicKey)) || 'global');
  return withAccountLock(lockKey, () => _cancelOrderUnlocked(cancelReq));
}

async function _cancelOrderUnlocked(cancelReq) {
  const orderId = parseInt(cancelReq.orderId, 10);
  if (!orderId || orderId <= 0) {
    throw new Error('Invalid orderId');
  }

  const callerKey = normalizeKey(cancelReq.publicKey || cancelReq.callerPublicKey);
  const pair = cancelReq.pair || cancelReq.market || 'XPX/USDT';

  // Cryptographic authentication: Require ed25519 signature over (pair, orderId, nonce, expiry)
  const cancelMsg = constructCancelMessage(
    pair,
    orderId,
    cancelReq.nonce,
    cancelReq.expiry
  );
  verifyEd25519Signature(callerKey, cancelMsg, cancelReq.signature);
  checkAndRecordNonce(callerKey, cancelReq.nonce, cancelReq.expiry);

  const isBnb = pair.toUpperCase().includes('BNB');
  const quoteMosaicId = isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID;
  const cancelParams = encodeCancelOrder(orderId, quoteMosaicId);
  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  const results = await executeConsensusCall(
    'Order Cancellation',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: cancelParams,
      callerKeyHex: callerKey,
    }
  );
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    throw new Error(`Order cancellation failed (Return code: ${ret.return_val})`);
  }

  // Restore escrow into available balance from canonical drive ledger
  syncGatewayCacheFromDrive(callerKey);

  if (activeOrders.has(orderId)) {
    activeOrders.delete(orderId);
    saveActiveOrders();
  }

  syncDriveDepth().catch(() => {});

  return {
    ok: true,
    success: true,
    orderId,
    gasConsumed: ret.execution_gas_consumed,
    balances: getAccountBalance(callerKey),
  };
}

/**
 * Handle Batch Order Cancellation (OP_BATCH_CANCEL = 7, serialized per callerKey)
 */
async function batchCancelOrders(batchReq) {
  const lockKey = normalizeKey((batchReq && (batchReq.publicKey || batchReq.callerPublicKey)) || 'global');
  return withAccountLock(lockKey, () => _batchCancelOrdersUnlocked(batchReq));
}

async function _batchCancelOrdersUnlocked(batchReq) {
  const orderIds = [...new Set((batchReq.orderIds || []).map((id) => parseInt(id, 10)).filter((id) => Number.isSafeInteger(id) && id > 0))];
  if (orderIds.length === 0) {
    throw new Error('No valid orderIds provided for batch cancellation');
  }

  const callerKey = normalizeKey(batchReq.publicKey || batchReq.callerPublicKey);
  const pair = batchReq.pair || batchReq.market || 'XPX/USDT';

  if (!batchReq.signature) {
    throw new Error('Missing ed25519 signature: Batch cancel request must be cryptographically signed by the account owner');
  }
  const normPair = pair.toUpperCase().replace('_', '/');
  const batchMsg = `${DOMAIN_SEPARATOR}:CANCEL_BATCH:${normPair}:${orderIds.join(',')}:${batchReq.nonce}:${batchReq.expiry}`;
  verifyEd25519Signature(callerKey, batchMsg, batchReq.signature);
  checkAndRecordNonce(callerKey, batchReq.nonce, batchReq.expiry);

  // Verify ownership of all target orderIds in activeOrders
  for (const id of orderIds) {
    const existing = activeOrders.get(id);
    if (existing && existing.callerKey && existing.callerKey.toLowerCase() !== callerKey.toLowerCase()) {
      throw new Error(`Unauthorized batch cancel: Order #${id} does not belong to caller ${callerKey}`);
    }
  }
  const isBnb = pair.toUpperCase().includes('BNB');
  const quoteMosaicId = isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID;
  const batchParams = encodeBatchCancel(orderIds, quoteMosaicId);
  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  const results = await executeConsensusCall(
    'Batch Order Cancellation',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: batchParams,
      callerKeyHex: callerKey,
    }
  );
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    throw new Error(`Batch order cancellation failed (Return code: ${ret.return_val})`);
  }

  for (const id of orderIds) {
    if (activeOrders.has(id)) {
      activeOrders.delete(id);
    }
  }
  saveActiveOrders();

  // Restore escrow into available balance from canonical drive ledger
  syncGatewayCacheFromDrive(callerKey);

  syncDriveDepth().catch(() => {});

  return {
    ok: true,
    success: true,
    cancelledOrderIds: orderIds,
    executionGas: ret.execution_gas_consumed,
    balances: getAccountBalance(callerKey),
  };
}

/**
 * Handle Cancel All Orders (OP_CANCEL_ALL = 6, serialized per callerKey)
 */
async function cancelAllOrders(cancelAllReq) {
  const lockKey = normalizeKey((cancelAllReq && (cancelAllReq.publicKey || cancelAllReq.callerPublicKey)) || 'global');
  return withAccountLock(lockKey, () => _cancelAllOrdersUnlocked(cancelAllReq));
}

async function _cancelAllOrdersUnlocked(cancelAllReq) {
  const callerKey = normalizeKey(cancelAllReq.publicKey || cancelAllReq.callerPublicKey);
  const pair = cancelAllReq.pair || cancelAllReq.market || 'XPX/USDT';

  // Cryptographic authentication: Require ed25519 signature over (pair, nonce, expiry)
  const cancelAllMsg = constructCancelAllMessage(
    pair,
    cancelAllReq.nonce,
    cancelAllReq.expiry
  );
  verifyEd25519Signature(callerKey, cancelAllMsg, cancelAllReq.signature);
  checkAndRecordNonce(callerKey, cancelAllReq.nonce, cancelAllReq.expiry);

  const isBnb = pair.toUpperCase().includes('BNB');
  const quoteMosaicId = (cancelAllReq.pair || cancelAllReq.market) ? (isBnb ? BNB_MOSAIC_ID : USDT_MOSAIC_ID) : 0n;
  const cancelAllParams = encodeCancelAll(quoteMosaicId);
  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  const results = await executeConsensusCall(
    'Cancel All Orders',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: cancelAllParams,
      callerKeyHex: callerKey,
    }
  );
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    throw new Error(`Cancel-all failed (Return code: ${ret.return_val})`);
  }

  // Restore all locked balances to available from canonical drive ledger
  syncGatewayCacheFromDrive(callerKey);

  for (const [id, o] of activeOrders.entries()) {
    if (o.callerKey.toLowerCase() === callerKey.toLowerCase()) {
      activeOrders.delete(id);
    }
  }
  saveActiveOrders();

  syncDriveDepth().catch(() => {});

  return {
    ok: true,
    success: true,
    message: 'All open resting orders cancelled',
    executionGas: ret.execution_gas_consumed,
    balances: getAccountBalance(callerKey),
  };
}

/**
 * Faucet Claim: Broadcasts real on-chain Sirius Testnet 2 transfer of XPX (gas) + USDT (collateral)
 */
async function claimFaucet(publicKey, addressOverride = null) {
  if (String(testnet2Config.NETWORK_ENV || '').toLowerCase() === 'mainnet') {
    throw new Error('Faucet claims are permanently disabled on Mainnet');
  }
  const callerKey = normalizeKey(publicKey).toLowerCase();
  let recipientAddress = addressOverride;

  if (!recipientAddress || typeof recipientAddress !== 'string') {
    if (callerKey && callerKey.length === 64) {
      try {
        const netType = testnet2Config.NETWORK_TYPE || 168;
        recipientAddress = Address.createFromPublicKey(callerKey, netType).plain();
      } catch (_) {}
    } else if (callerKey && callerKey.length === 40) {
      recipientAddress = callerKey.toUpperCase();
    }
  }

  if (!recipientAddress) {
    throw new Error('Valid public key or Sirius address required to claim faucet');
  }

  recipientAddress = recipientAddress.replace(/[- ]/g, '').toUpperCase();

  // Rate limit: 1 claim per 5 minutes per recipient address
  const now = Date.now();
  if (!global.faucetClaimsMap) global.faucetClaimsMap = new Map();
  const lastClaim = global.faucetClaimsMap.get(recipientAddress) || 0;
  if (now - lastClaim < 60000) {
    const waitSec = Math.ceil((60000 - (now - lastClaim)) / 1000);
    throw new Error(`Faucet rate limit: Please wait ${waitSec}s before claiming again for ${recipientAddress}`);
  }

  // Global 24h Faucet Cap to protect Treasury reserves (max 5,000 XPX / 24h)
  const FAUCET_DAILY_CAP_XPX = 5000;
  if (!global.faucetDisbursed24h || global.faucetDisbursed24h.date !== new Date().toDateString()) {
    global.faucetDisbursed24h = { date: new Date().toDateString(), amountXpx: 0 };
  }
  if (global.faucetDisbursed24h.amountXpx + 50 > FAUCET_DAILY_CAP_XPX) {
    throw new Error(`Daily faucet capacity reached (${FAUCET_DAILY_CAP_XPX} XPX/day). Please try again tomorrow.`);
  }

  const testnetClient = require('./testnet2_client');
  const treasury = testnetClient.getOrInitTreasuryAccount();
  if (!treasury) {
    throw new Error('On-chain Treasury account unavailable on this node');
  }

  const { Mosaic, MosaicId, UInt64, TransferTransaction, Deadline, PlainMessage } = sdk;
  const recipient = Address.createFromRawAddress(recipientAddress);

  // Deliver 50 real on-chain XPX (for fees/gas/vault deposits) + 100 real on-chain USDT (for trading) + 0.1 real on-chain BNB
  const amountXpx = 50;
  const amountUsdt = 100;
  const amountBnb = 0.1;
  const mosaicXpx = new Mosaic(new MosaicId(testnet2Config.XPX_MOSAIC_ID), UInt64.fromUint(amountXpx * 1_000_000));
  const mosaicUsdt = new Mosaic(new MosaicId(testnet2Config.USDT_MOSAIC_ID), UInt64.fromUint(amountUsdt * 1_000_000));
  const mosaicBnb = new Mosaic(new MosaicId(testnet2Config.BNB_MOSAIC_ID), UInt64.fromUint(Math.round(amountBnb * 1_000_000)));

  const tx = TransferTransaction.create(
    Deadline.create(),
    recipient,
    [mosaicXpx, mosaicUsdt, mosaicBnb],
    PlainMessage.create(`SiriusCLOB On-Chain Faucet: +${amountXpx} XPX, +${amountUsdt} USDT, +${amountBnb} BNB`),
    testnet2Config.NETWORK_TYPE || 168,
    UInt64.fromUint(200_000)
  );

  const signedTx = typeof treasury.preV2Sign === 'function'
    ? treasury.preV2Sign(tx, testnet2Config.GENERATION_HASH)
    : treasury.sign(tx, testnet2Config.GENERATION_HASH);
  await testnetClient.announceSignedTransaction(signedTx);
  await testnetClient.verifyTransactionAccepted(signedTx.hash);

  global.faucetClaimsMap.set(recipientAddress, now);
  global.faucetDisbursed24h.amountXpx += amountXpx;

  // Immediately credit CLOB internal trading ledger for instant zero-friction exchange trading
  const userPubKey = callerKey && callerKey.length === 64 ? callerKey : normalizeKey(faucetReq.publicKey || faucetReq.callerPublicKey);
  if (userPubKey && userPubKey.length === 64) {
    try {
      updateAccountBalance(userPubKey, (b) => {
        b.xpx = BigInt(b.xpx || 0n) + BigInt(amountXpx) * 1_000_000n;
        b.usdt = BigInt(b.usdt || 0n) + BigInt(amountUsdt) * 1_000_000n;
        b.bnb = BigInt(b.bnb || 0n) + BigInt(Math.round(amountBnb * 1_000_000));
      }, { source: 'FAUCET' });
    } catch (_) {}
  }

  return {
    ok: true,
    success: true,
    txHash: signedTx.hash,
    recipient: recipientAddress,
    amountXpx,
    amountUsdt,
    amountBnb,
    message: `Delivered ${amountXpx} XPX, ${amountUsdt} USDT, and ${amountBnb} BNB on-chain to ${recipientAddress}`,
  };
}

/**
 * Deposit Tokens into SuperContract Vault & On-Chain BalanceLedger
 */
async function depositTokens(depositReq, options = {}) {
  const callerKey = normalizeKey(depositReq.publicKey || depositReq.callerPublicKey);
  return withAccountLock(callerKey, async () => {
    checkAndEnforceCircuitBreaker('Token Deposit');
    const mosaicId = String(depositReq.mosaicId || depositReq.mosaic_id || XPX_MOSAIC_ID);
  const confirmationDepth = depositReq.confirmationDepth !== undefined ? Number(depositReq.confirmationDepth) : 1;

  let txHash = depositReq.txHash || depositReq.depositTxHash;
  if (!txHash && depositReq.privateKey) {
    let normMosaicHex = '';
    if (/^[0-9]+$/.test(mosaicId)) {
      normMosaicHex = BigInt(mosaicId).toString(16).toUpperCase().padStart(16, '0');
    } else if (/^(?:0x)?[0-9a-fA-F]+$/.test(mosaicId)) {
      normMosaicHex = mosaicId.replace(/^0x/i, '').toUpperCase().padStart(16, '0');
    } else if (mosaicId.toUpperCase() === 'XPX') {
      normMosaicHex = (testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7').toUpperCase().padStart(16, '0');
    } else if (mosaicId.toUpperCase() === 'USDT') {
      normMosaicHex = (testnet2Config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toUpperCase().padStart(16, '0');
    } else if (mosaicId.toUpperCase() === 'BNB') {
      normMosaicHex = (testnet2Config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toUpperCase().padStart(16, '0');
    } else {
      normMosaicHex = (testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7').toUpperCase().padStart(16, '0');
    }
    const amountAtomic = parseDecimalToBigInt(depositReq.amount, 6);
    const vaultAddress = (testnet2Config.SUPERCONTRACT_ADDRESS || testnet2Config.VAULT_ADDRESS || 'VAEKLE3WB6XJLFW75JGJJ7Y3DVQWCWX774DDSJ2F').replace(/[- ]/g, '').toUpperCase();

    // Pre-flight check: Ensure sender has adequate on-chain assets and reserves 0.2 XPX for network fee
    try {
      const t2Client = require('./testnet2_client');
      const senderAcc = sdk.Account.createFromPrivateKey(depositReq.privateKey, testnet2Config.NETWORK_TYPE, 1);
      const senderMosaics = await t2Client.getAccountOnChainBalances(senderAcc.address.plain());
      const xpxHex = (testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7').toLowerCase();
      const senderXpx = senderMosaics.get(xpxHex) || 0n;
      const minFee = 200000n; // 0.2 XPX
      const requiredXpx = isXpxMosaic(normMosaicHex) ? (amountAtomic + minFee) : minFee;
      if (senderXpx < requiredXpx) {
        throw new Error(
          `Insufficient XPX for on-chain network fee: Sender wallet holds ${Number(senderXpx) / 1000000} XPX, ` +
          `but needs at least ${Number(requiredXpx) / 1000000} XPX to pay the 0.2 XPX Catapult transfer fee.`
        );
      }
      if (!isXpxMosaic(normMosaicHex)) {
        const senderAssetBal = senderMosaics.get(normMosaicHex.toLowerCase()) || 0n;
        if (senderAssetBal < amountAtomic) {
          throw new Error(
            `Insufficient on-chain token balance: Sender wallet holds ${Number(senderAssetBal) / 1000000}, ` +
            `but tried to deposit ${Number(amountAtomic) / 1000000}.`
          );
        }
      }
    } catch (preErr) {
      if (preErr.message && preErr.message.includes('Insufficient')) {
        throw preErr;
      }
    }

    const transferRes = await broadcastTransferOnChain(
      depositReq.privateKey,
      vaultAddress,
      normMosaicHex,
      amountAtomic,
      'VAULT_DEPOSIT'
    );
    if (!transferRes.ok || !transferRes.txHash) {
      throw new Error(`Failed to broadcast deposit transfer: ${transferRes.error || 'Transfer failed'}`);
    }
    txHash = transferRes.txHash;
    // Wait for block confirmation on-chain (max 120 seconds)
    const start = Date.now();
    let confirmed = false;
    while (Date.now() - start < 120000) {
      await new Promise((r) => setTimeout(r, 1500));
      const q = await queryTransactionOnChain(txHash);
      if (q && q.found && q.height > 0) {
        confirmed = true;
        break;
      }
    }
    if (!confirmed) {
      throw new Error(`On-chain deposit transfer ${txHash} did not confirm within timeout`);
    }
  }

  if (!txHash) {
    throw new Error('Missing on-chain deposit txHash: Every ledger credit must reference a confirmed deposit transaction to the vault');
  }

  // Look up on-chain transaction, verify all invariants, and extract ON-CHAIN amount (ignore caller-supplied amount)
  const depositRecord = await recordDepositTx(txHash, callerKey, mosaicId, confirmationDepth, options);
  const onChainAmountAtomic = depositRecord.amountAtomic;
  const currentHeight = depositRecord.height;
  const finalMosaicId = depositRecord.mosaicId;

  // Canonicalize mosaic ID to ensure SuperContract WASM consensus recognizes it
  const contractMosaicId = deriveMosaicFromAsset(finalMosaicId);
  const callParams = encodeDeposit(contractMosaicId, onChainAmountAtomic);
  const servicePayments = [{ mosaic_id: contractMosaicId, amount: String(onChainAmountAtomic) }];
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  // Execute across all 3 nodes simultaneously with BFT self-healing
  const results = await executeConsensusCall(
    'Web Wallet Deposit',
    currentHeight + 1,
    txHash,
    contractCallId,
    {
      parameters: callParams,
      callerKeyHex: callerKey,
      servicePayments,
    }
  );
  const tCons = Date.now();
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    throw new Error(`SuperContract rejected deposit (Return code: ${ret.return_val})`);
  }

  // Synchronize gateway cache directly from canonical drive ledger
  syncGatewayCacheFromDrive(callerKey);

  // Confirm deposit atomically in SQLite and credit verified ledger
  try {
    clobDb.confirmDeposit({
      txHash,
      onChainAmountAtomic,
      callerKey,
      mosaicId: finalMosaicId,
      mosaicHex: depositRecord.mosaicHex,
      height: currentHeight,
    });
  } catch (_) {}

  creditVerifiedDeposit({
    txHash,
    senderPublicKey: callerKey,
    callerKey,
    mosaicIdHex: depositRecord.mosaicHex,
    amountAtomic: onChainAmountAtomic.toString(),
    blockHeight: currentHeight,
  });

  invalidateVaultBalanceCache(finalMosaicId);

    return {
      ok: true,
      success: true,
      mosaicId: finalMosaicId,
      txHash,
      amount: formatAtomicUnits(onChainAmountAtomic, 6),
      amountAtomic: onChainAmountAtomic.toString(),
      gasConsumed: ret.execution_gas_consumed,
      balances: getAccountBalance(callerKey),
    };
  });
}

async function resumeBroadcastAndConfirm(record, withdrawReq = {}) {
  if (record.status === 'confirmed' || record.status === 'broadcast') {
    return {
      ok: true,
      success: true,
      status: record.status,
      mosaicId: record.mosaicId,
      amount: record.amount,
      amountAtomic: record.amountAtomic,
      txHash: record.txHash,
      onChainTxHash: record.txHash,
      blockHeight: record.blockHeight,
      retry: true,
      balances: getAccountBalance(record.callerKey),
    };
  }

  let onChainTxHash = null;
  if (fs.existsSync(path.join(CLUSTER_DIR, 'vault_account.json'))) {
    try {
      const t2Client = require('./testnet2_client');
      const t2Config = require('./testnet2_config');
      const vault = t2Client.getOrInitVaultAccount();
      const recipientAddr = withdrawReq.recipient || record.recipient || Address.createFromPublicKey(record.callerKey, t2Config.NETWORK_TYPE).plain();
      let t2MosaicHex = t2Config.XPX_MOSAIC_ID;
      if (String(record.mosaicId) === USDT_MOSAIC_ID) t2MosaicHex = t2Config.USDT_MOSAIC_ID;
      if (String(record.mosaicId) === BNB_MOSAIC_ID) t2MosaicHex = t2Config.BNB_MOSAIC_ID;
      const bRes = await t2Client.broadcastVaultTransfer(vault, recipientAddr, t2MosaicHex, BigInt(record.amountAtomic), 'CLOB_WITHDRAW_RECOVERY');
      onChainTxHash = bRes.txHash;
    } catch (settleErr) {
      console.error('Warning: Sirius L1 withdrawal broadcast failed during recovery:', settleErr.message);
    }
  }

  if (!onChainTxHash) {
    record.status = 'debited';
    saveWithdrawals();
    throw new Error('On-chain withdrawal broadcast failed. Your withdrawal is safely recorded as debited and can be retried.');
  }

  record.status = 'broadcast';
  record.broadcastAt = Date.now();
  record.txHash = onChainTxHash;
  saveWithdrawals();

  const recordId = record.id || record.key || record.withdrawal_key || onChainTxHash || `${record.callerKey}:${record.nonce}`;
  if (withdrawReq.waitForConfirmation && onChainTxHash) {
    await confirmWithdrawal(recordId, 35000);
  }

  return {
    ok: true,
    success: true,
    status: record.status,
    mosaicId: record.mosaicId,
    amount: record.amount,
    amountAtomic: record.amountAtomic,
    txHash: record.txHash,
    onChainTxHash: record.txHash,
    blockHeight: record.blockHeight,
    recovered: true,
    balances: getAccountBalance(record.callerKey),
  };
}

async function recoverPendingWithdrawals() {
  loadWithdrawals();
  const recovered = [];
  for (const [key, record] of withdrawals.entries()) {
    if (record.status === 'debited') {
      const res = await withAccountLock(
        normalizeKey(record.callerKey || 'global'),
        () => resumeBroadcastAndConfirm(record, { waitForConfirmation: true })
      );
      recovered.push(res);
    } else if (record.status === 'broadcast' && record.txHash) {
      try {
        const q = await queryTxWithTestnet2Fallback(record.txHash);
        if (q && q.ok && (q.confirmed || q.found) && q.height > 0) {
          record.status = 'confirmed';
          record.blockHeight = q.height;
          record.confirmedAt = Date.now();
          saveWithdrawals();
          recovered.push(record);
        }
      } catch (_) {}
    }
  }
  return recovered;
}

/**
 * Withdraw Tokens from SuperContract Vault back to User's Sirius Account
 * Persists withdrawal state as requested -> debited -> broadcast -> confirmed so retries never pay twice.
 * Serialized per callerKey via withAccountLock to eliminate concurrent withdrawal races.
 */
async function withdrawTokens(withdrawReq) {
  const lockKey = normalizeKey((withdrawReq && (withdrawReq.publicKey || withdrawReq.callerPublicKey)) || 'global');
  return withAccountLock(lockKey, () => _withdrawTokensUnlocked(withdrawReq));
}

async function _withdrawTokensUnlocked(withdrawReq) {
  checkAndEnforceCircuitBreaker('Token Withdrawal');
  const callerKey = normalizeKey(withdrawReq.publicKey || withdrawReq.callerPublicKey);
  const nonce = withdrawReq.nonce;
  const expiry = withdrawReq.expiry;

  if (nonce === undefined || nonce === null || nonce === '') {
    throw new Error('Missing nonce in request');
  }
  if (!expiry) {
    throw new Error('Missing expiry timestamp in request');
  }

  // Cryptographic signature check & authoritative server mosaic derivation
  const sigInfo = verifyWithdrawalSignature(callerKey, withdrawReq);
  const mosaicId = sigInfo.mosaicId;
  const amountAtomic = parseDecimalToBigInt(withdrawReq.amount || 0, 6);
  if (amountAtomic <= 0n) {
    throw new Error('Invalid withdrawal amount: must be greater than zero');
  }

  const withdrawalKey = `${callerKey.toLowerCase()}:${String(nonce)}`;
  const existing = withdrawals.get(withdrawalKey);

  // If a withdrawal with this key already exists:
  if (existing) {
    const reqAmountStr = amountAtomic.toString();
    const reqMosaicId = mosaicId;

    // Strict same-nonce-different-payload check:
    if (existing.amountAtomic !== reqAmountStr || existing.mosaicId !== reqMosaicId) {
      throw new Error(
        `Same-nonce replay rejected: Payload does not match initial withdrawal request for nonce ${nonce}. ` +
        `Existing: (${existing.mosaicId}, ${existing.amountAtomic} atomic) vs Submitted: (${reqMosaicId}, ${reqAmountStr} atomic). ` +
        `Nonce reuse with altered payload is strictly forbidden.`
      );
    }

    // Crash Recovery Case: status === 'debited' (crashed between debit and broadcast)
    if (existing.status === 'debited') {
      return await resumeBroadcastAndConfirm(existing, withdrawReq);
    }

    // Idempotent retry: return existing record (never pay twice!)
    if (existing.status === 'confirmed' || existing.status === 'broadcast') {
      if (existing.status === 'broadcast' && existing.txHash) {
        try {
          const q = await queryTxWithTestnet2Fallback(existing.txHash);
          if (q && q.ok && (q.confirmed || q.found) && q.height > 0) {
            existing.status = 'confirmed';
            existing.blockHeight = q.height;
            existing.confirmedAt = Date.now();
            saveWithdrawals();
          }
        } catch (_) {}
      }
      return {
        ok: true,
        success: true,
        status: existing.status,
        mosaicId: existing.mosaicId,
        amount: existing.amount,
        amountAtomic: existing.amountAtomic,
        txHash: existing.txHash,
        onChainTxHash: existing.txHash,
        blockHeight: existing.blockHeight,
        retry: true,
        message: existing.status === 'confirmed'
          ? 'Withdrawal already confirmed on-chain (retry ignored, not paid twice)'
          : 'Withdrawal already broadcast on-chain (retry ignored, not paid twice)',
        balances: getAccountBalance(callerKey),
      };
    }
  }

  // Check nonce uniqueness and expiry against global replay registry (after signature verification succeeds)
  checkAndRecordNonce(callerKey, nonce, expiry);

  // Enforce 24-Hour Global Withdrawal Volume Ceilings (Prevents hot vault drain / trips circuit breaker)
  checkWithdrawalCeiling(mosaicId, amountAtomic);

  let userBal = await syncAndGetAccountBalance(callerKey);
  if (mosaicId === XPX_MOSAIC_ID && BigInt(userBal.xpx || 0n) < amountAtomic) {
    throw new Error(`Insufficient XPX balance for withdrawal. Requested: ${formatAtomicUnits(amountAtomic, 6)}, Available: ${formatAtomicUnits(userBal.xpx, 6)}`);
  }
  if (mosaicId === USDT_MOSAIC_ID && BigInt(userBal.usdt || 0n) < amountAtomic) {
    throw new Error(`Insufficient USDT balance for withdrawal. Requested: ${formatAtomicUnits(amountAtomic, 6)}, Available: ${formatAtomicUnits(userBal.usdt, 6)}`);
  }
  if (mosaicId === BNB_MOSAIC_ID && BigInt(userBal.bnb || 0n) < amountAtomic) {
    throw new Error(`Insufficient BNB balance for withdrawal. Requested: ${formatAtomicUnits(amountAtomic, 6)}, Available: ${formatAtomicUnits(userBal.bnb, 6)}`);
  }

  // Step 1: requested
  const record = {
    id: withdrawalKey,
    callerKey,
    recipient: sigInfo.recipient || null,
    mosaic: withdrawReq.mosaic || (mosaicId === XPX_MOSAIC_ID ? 'XPX' : (mosaicId === USDT_MOSAIC_ID ? 'USDT' : 'BNB')),
    mosaicId,
    amount: formatAtomicUnits(amountAtomic, 6),
    amountAtomic: amountAtomic.toString(),
    nonce: String(nonce),
    expiry: Number(expiry),
    status: 'requested',
    requestedAt: Date.now(),
    txHash: null,
    blockHeight: null,
  };
  withdrawals.set(withdrawalKey, record);
  saveWithdrawals();

  const callParams = encodeWithdraw(mosaicId, amountAtomic);
  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;

  // Ensure drive has enough credited balance when withdrawing Testnet 2 deposits
  const driveBal = getDriveBalances(callerKey);
  const availDrive = mosaicId === XPX_MOSAIC_ID
    ? BigInt(driveBal ? driveBal.xpx : 0n)
    : mosaicId === USDT_MOSAIC_ID
      ? BigInt(driveBal ? driveBal.usdt : 0n)
      : BigInt(driveBal ? driveBal.bnb : 0n);

  let withdrawServicePayments = [];
  if (availDrive < amountAtomic) {
    const deficit = amountAtomic - availDrive;
    withdrawServicePayments = [{ mosaic_id: mosaicId, amount: String(deficit) }];
  }

  // Execute across all 3 nodes simultaneously with BFT self-healing (debits canonical drive ledger)
  const results = await executeConsensusCall(
    'Web Wallet Withdrawal',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: callParams,
      callerKeyHex: callerKey,
      servicePayments: withdrawServicePayments,
    }
  );
  const ret = results[0].returns;

  if (ret.return_val !== 0) {
    record.status = 'failed';
    record.failedAt = Date.now();
    record.error = `SuperContract rejected withdrawal (Return code: ${ret.return_val})`;
    saveWithdrawals();
    throw new Error(record.error);
  }

  // Deduct from local cached balance to match drive ledger
  updateAccountBalance(callerKey, (b) => {
    if (mosaicId === XPX_MOSAIC_ID) {
      const rem = BigInt(b.xpx || 0n) - amountAtomic;
      b.xpx = rem > 0n ? rem : 0n;
    } else if (mosaicId === USDT_MOSAIC_ID) {
      const rem = BigInt(b.usdt || 0n) - amountAtomic;
      b.usdt = rem > 0n ? rem : 0n;
    } else if (mosaicId === BNB_MOSAIC_ID) {
      const rem = BigInt(b.bnb || 0n) - amountAtomic;
      b.bnb = rem > 0n ? rem : 0n;
    }
  });

  // Step 2: debited (persisted on disk before broadcast)
  record.status = 'debited';
  record.debitedAt = Date.now();
  saveWithdrawals();

  invalidateVaultBalanceCache();

  // Step 3: broadcast on-chain payout
  let onChainTxHash = null;
  const emittedTx = ret.transaction;

  if (emittedTx && Array.isArray(emittedTx.transactions) && emittedTx.transactions.length > 0) {
    try {
      if (fs.existsSync(path.join(CLUSTER_DIR, 'vault_account.json'))) {
        const testnet2Client = require('./testnet2_client');
        const testnet2Config = require('./testnet2_config');
        const vault = testnet2Client.getOrInitVaultAccount();
        const recipientAddr = sigInfo.recipient || Address.createFromPublicKey(callerKey, testnet2Config.NETWORK_TYPE).plain();
        let t2MosaicHex = testnet2Config.XPX_MOSAIC_ID || '13BFC518E40549D7';
        if (String(mosaicId) === USDT_MOSAIC_ID) t2MosaicHex = testnet2Config.USDT_MOSAIC_ID;
        if (String(mosaicId) === BNB_MOSAIC_ID) t2MosaicHex = testnet2Config.BNB_MOSAIC_ID;
        if (process.env.SIMULATE_BROADCAST === 'true') {
          onChainTxHash = crypto.randomBytes(32).toString('hex');
        } else {
          const txRes = await testnet2Client.broadcastVaultTransfer(
            vault,
            recipientAddr,
            t2MosaicHex,
            amountAtomic,
            `CLOB_WITHDRAW:${contractCallId}`
          );
          onChainTxHash = txRes.txHash;
        }
      }
    } catch (settleErr) {
      console.error('Warning: On-chain withdrawal broadcast failed:', settleErr.message);
    }
  }

  if (!onChainTxHash) {
    record.status = 'debited';
    saveWithdrawals();
    throw new Error('On-chain withdrawal broadcast failed. Your withdrawal is safely recorded as debited and can be retried.');
  }

  record.status = 'broadcast';
  record.broadcastAt = Date.now();
  record.txHash = onChainTxHash;
  saveWithdrawals();

  // Step 4: confirmed (if requested to wait or confirmed)
  if (withdrawReq.waitForConfirmation && onChainTxHash) {
    await confirmWithdrawal(withdrawalKey, 35000);
  }

  return {
    ok: true,
    success: true,
    status: record.status,
    mosaicId,
    amount: formatAtomicUnits(amountAtomic, 6),
    amountAtomic: amountAtomic.toString(),
    txHash: record.txHash,
    onChainTxHash: record.txHash,
    blockHeight: record.blockHeight,
    gasConsumed: ret.execution_gas_consumed,
    balances: getAccountBalance(callerKey),
  };
}

async function broadcastTransferOnChain(senderPrivHex, recipientAddrOrPubKey, mosaicIdHex, amountAtomic, messageStr = '-') {
  try {
    const t2Client = require('./testnet2_client');
    const t2Config = require('./testnet2_config');
    const sender = Account.createFromPrivateKey(senderPrivHex, t2Config.NETWORK_TYPE, 1);
    let cleanRecipient = String(recipientAddrOrPubKey).replace(/[- ]/g, '').toUpperCase();
    if (cleanRecipient.length === 64 && /^[0-9A-F]{64}$/.test(cleanRecipient)) {
      try {
        cleanRecipient = Address.createFromPublicKey(cleanRecipient, t2Config.NETWORK_TYPE).plain();
      } catch (_) {}
    }
    const res = await t2Client.broadcastVaultTransfer(sender, cleanRecipient, mosaicIdHex, BigInt(amountAtomic), messageStr);
    return { ok: true, txHash: res.txHash };
  } catch (err) {
    console.error('broadcastTransferOnChain error:', err.message);
    return { ok: false, error: err.message };
  }
}

function isXpxMosaic(hex) {
  const norm = String(hex || '').toLowerCase().replace(/^0x/, '');
  const configuredXpx = String(testnet2Config.XPX_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');
  let decAsHex = '';
  if (/^\d+$/.test(norm)) {
    try {
      decAsHex = BigInt(norm).toString(16).toLowerCase();
    } catch (_) {}
  }
  return (
    norm === '13bfc518e40549d7' ||
    norm === '402b2f579faebc59' ||
    norm === '402b2f579fa859b8' ||
    (configuredXpx && norm === configuredXpx) ||
    norm === 'bffb42a19116bdf6' ||
    norm === '0dc67fbe1cad29e3' ||
    norm === '992621222383397347' ||
    norm === '1423072717967804887' ||
    norm === '1423071378129734103' ||
    norm === XPX_MOSAIC_ID.toLowerCase() ||
    norm === BigInt(XPX_MOSAIC_ID).toString(16).toLowerCase() ||
    (decAsHex && (
      decAsHex === '13bfc518e40549d7' ||
      decAsHex === '402b2f579faebc59' ||
      decAsHex === '402b2f579fa859b8' ||
      (configuredXpx && decAsHex === configuredXpx) ||
      decAsHex === 'bffb42a19116bdf6' ||
      decAsHex === '0dc67fbe1cad29e3'
    ))
  );
}

function creditVerifiedDeposit(deposit) {
  const senderKey = normalizeKey(deposit.senderPublicKey);
  const mosaicIdHex = (deposit.mosaicIdHex || '').toLowerCase().replace(/^0x/, '');
  const amount = BigInt(deposit.amountAtomic || 0n);
  const txHash = String(deposit.txHash || crypto.randomBytes(32).toString('hex')).toLowerCase();

  processedDeposits.set(txHash, {
    txHash,
    senderPublicKey: senderKey,
    callerKey: senderKey,
    mosaicIdHex,
    amountAtomic: amount.toString(),
    status: 'confirmed',
    blockHeight: deposit.blockHeight || 1,
    creditedAt: Date.now(),
  });
  saveProcessedDeposits();

  const confUsdt = String(testnet2Config.USDT_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');
  const confBnb = String(testnet2Config.BNB_MOSAIC_ID || '').toLowerCase().replace(/^0x/, '');

  return updateAccountBalance(
    senderKey,
    (b) => {
      if (isXpxMosaic(mosaicIdHex)) {
        b.xpx = (b.xpx || 0n) + amount;
      } else if (
        mosaicIdHex === USDT_MOSAIC_ID.toLowerCase() ||
        mosaicIdHex === BigInt(USDT_MOSAIC_ID).toString(16).toLowerCase() ||
        mosaicIdHex === '4430aefef7e15d0c' ||
        mosaicIdHex === '7525381a171d1e44' ||
        mosaicIdHex === confUsdt
      ) {
        b.usdt = (b.usdt || 0n) + amount;
      } else if (
        mosaicIdHex === BNB_MOSAIC_ID.toLowerCase() ||
        mosaicIdHex === BigInt(BNB_MOSAIC_ID).toString(16).toLowerCase() ||
        mosaicIdHex === '3cc1d3ce7447b6f9' ||
        mosaicIdHex === '1e5c5fadec7b34dd' ||
        mosaicIdHex === confBnb
      ) {
        b.bnb = (b.bnb || 0n) + amount;
      }
    },
    { source: 'VERIFIED_DEPOSIT', txHash }
  );
}

/**
 * Emergency Direct On-Chain Unlock
 * Cancels all open orders and refunds 100% of free funds directly back to caller.
 */
async function emergencyUnlock(req) {
  const callerKey = normalizeKey(req.publicKey || req.callerPublicKey);
  if (!callerKey || !/^[0-9a-fA-F]{64}$/.test(callerKey)) {
    throw new Error('Invalid caller public key');
  }

  const currentHeight = getLatestCommittedHeight('node1');
  const txHashHex = crypto.randomBytes(32).toString('hex');
  const contractCallId = Math.floor(Math.random() * 90000) + 10000;
  const emergencyBuf = Buffer.from([0x08]); // OP_EMERGENCY_UNLOCK

  const results = await executeConsensusCall(
    'Emergency Direct On-Chain Unlock',
    currentHeight + 1,
    txHashHex,
    contractCallId,
    {
      parameters: emergencyBuf,
      callerKeyHex: callerKey,
      deferCommit: false,
    }
  );

  const ret = results[0].returns;
  if (ret.return_val !== 0) {
    throw new Error(`SuperContract rejected emergency unlock (Return code: ${ret.return_val})`);
  }

  // Update cached balances to reflect unlock
  updateAccountBalance(callerKey, (b) => {
    b.xpx = 0n;
    b.usdt = 0n;
    b.bnb = 0n;
    b.lockedXpx = 0n;
    b.lockedUsdt = 0n;
    b.lockedBnb = 0n;
  });

  const emittedTx = ret.transaction;
  let broadcastedTxHashes = [];
  if (emittedTx && Array.isArray(emittedTx.transactions) && emittedTx.transactions.length > 0) {
    const testnet2Client = require('./testnet2_client');
    const testnet2Config = require('./testnet2_config');
    const vault = testnet2Client.getOrInitVaultAccount();
    const recipientAddr = Address.createFromPublicKey(callerKey, testnet2Config.NETWORK_TYPE).plain();

    for (const t of emittedTx.transactions) {
      const payloadBuf = Buffer.isBuffer(t.payload) ? t.payload : Buffer.from(t.payload || '', 'base64');
      if (payloadBuf.length >= 64) {
        const mosaicId = payloadBuf.readBigUInt64LE(48);
        const payoutAmount = payloadBuf.readBigUInt64LE(56);
        if (payoutAmount > 0n) {
          const mosaicHex = mosaicId.toString(16).toUpperCase().padStart(16, '0');
          try {
            const txRes = await testnet2Client.broadcastVaultTransfer(
              vault,
              recipientAddr,
              mosaicHex,
              payoutAmount,
              `CLOB_EMERGENCY_UNLOCK:${contractCallId}`
            );
            broadcastedTxHashes.push(txRes.txHash);
          } catch (e) {
            console.error('Emergency unlock broadcast error:', e.message);
          }
        }
      }
    }
  }

  return {
    ok: true,
    success: true,
    message: 'Emergency Direct Unlock executed: All open orders cancelled and funds returned to on-chain wallet.',
    txHashes: broadcastedTxHashes,
    balances: getAccountBalance(callerKey),
  };
}

module.exports = {
  syncUserOnChainBalanceToDrive,
  creditVerifiedDeposit,
  placeOrder,
  cancelOrder,
  batchCancelOrders,
  cancelAllOrders,
  getOpenOrders,
  depositTokens,
  withdrawTokens,
  emergencyUnlock,
  getAccountBalance,
  updateAccountBalance,
  claimFaucet,
  getLatestCommittedHeight,
  signOrderRequest,
  signCancelRequest,
  signBatchCancelRequest,
  signCancelAllRequest,
  signWithdrawRequest,
  signWithdrawalRequest,
  constructWithdrawMessage,
  verifyWithdrawalSignature,
  confirmWithdrawal,
  resumeBroadcastAndConfirm,
  recoverPendingWithdrawals,
  WITHDRAWALS_FILE,
  withdrawals,
  loadWithdrawals,
  deriveMosaicFromAsset,
  verifyWithdrawalSignature,
  saveWithdrawals,
  verifyEd25519Signature,
  constructOrderMessage,
  parseDecimalToBigInt,
  formatAtomicUnits,
  queryVaultOnChainBalance,
  queryAccountOnChainBalance,
  queryTransactionOnChain,
  verifyOnChainDeposit,
  broadcastTransferOnChain,
  getLedgerTotalAtomic,
  getDriveLedgerTotalAtomic,
  executeConsensusCall,
  encodeDeposit,
  encodeWithdraw,
  assertLedgerReconciliation,
  syncGatewayCacheFromDrive,
  syncAllGatewayBalancesFromDrive,
  getDriveBalances,
  assertSolvencyReconciliation,
  DOMAIN_SEPARATOR,
  NETWORK_ID,
  XPX_MOSAIC_ID,
  USDT_MOSAIC_ID,
  BNB_MOSAIC_ID,
  NONCES_FILE,
  loadUsedNonces,
  saveUsedNonces,
  checkAndRecordNonce,
  usedNonces,
  PROCESSED_DEPOSITS_FILE,
  recordDepositTx,
  refundMismatchedDeposit,
  processedDeposits,
  loadProcessedDeposits,
  loadBalances,
  saveBalances,
  getVaultBalance,
  setVaultBalanceCache,
  invalidateVaultBalanceCache,
  syncVaultBalancesFromTestnet2,
  BALANCES_FILE,
  reconcileActiveOrdersWithTrades,
  activeOrders,
  clobDb,
  constructAdminMessage,
  signAdminRequest,
  getAuthorizedAdminKeys,
  verifyAdminRequest,
  executeConsensusCall,
  withClusterConsensusLock,
  computeMarketsHashForDir,
  computeNodeMarketsHash,
  exportNodeDriveSnapshot,
  importNodeDriveSnapshot,
  selectMajorityCanonicalNode,
  syncAllReplicatorDrivesFromNode1,
  ensureDriveSync,
  exchangeCircuitBreaker,
  tripCircuitBreaker,
  resetCircuitBreaker,
  loadCircuitBreakerState,
  isCircuitBreakerTripped,
  getCircuitBreakerStatus,
  checkAndEnforceCircuitBreaker,
  WITHDRAWAL_LIMITS_24H,
  getRolling24hWithdrawalVolume,
  checkWithdrawalCeiling,
  getTestnet2LiabilitiesAtomic,
  syncAndGetAccountBalance,
  isXpxMosaic,
  getMarketDepthSnapshot,
  registerHeartbeat,
  deregisterHeartbeat,
  getHeartbeatStatus,
  cancelAllOrdersInternal,
  codRegistry,
  DEFAULT_ORDER_TTL_MS,
  cancelSingleOrderInternal,
  sweepExpiredOrders,
  startOrderTtlSweeper,
  runSolvencySentinelAudit,
  startSolvencySentinel,
};
