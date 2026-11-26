/**
 * Sirius Testnet 2 Native REST Client
 * Replaces local filesystem RocksDB index.dat checks and TXBRIDGE_BIN calls with live Catapult REST API.
 */
const nodeCrypto = require('crypto');
if (!nodeCrypto.webcrypto) {
  nodeCrypto.webcrypto = {
    getRandomValues(b) { return nodeCrypto.randomFillSync(b); }
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

const fs = require('fs');
const path = require('path');
module.paths.push(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules'));
const {
  Account,
  Address,
  RawAddress,
  ChainHttp,
  AccountHttp,
  TransactionHttp,
  TransferTransaction,
  Deadline,
  PlainMessage,
  Mosaic,
  MosaicId,
  UInt64,
} = require('tsjs-xpx-chain-sdk');

function uint64ToBigIntExact(u64) {
  if (!u64) return 0n;
  if (typeof u64.toHex === 'function') {
    return BigInt('0x' + u64.toHex());
  }
  return BigInt(u64.compact());
}
const config = require('./testnet2_config');

const CANDIDATE_NODE_URLS = Array.from(
  new Set([config.NODE_URL, ...(config.FALLBACK_NODE_URLS || [])].filter(Boolean))
);
let activeNodeUrl = CANDIDATE_NODE_URLS[0] || config.NODE_URL;

const httpClientsByUrl = new Map();
function getHttpClients(nodeUrl = activeNodeUrl) {
  if (!httpClientsByUrl.has(nodeUrl)) {
    httpClientsByUrl.set(nodeUrl, {
      nodeUrl,
      chainHttp: new ChainHttp(nodeUrl),
      accountHttp: new AccountHttp(nodeUrl),
      transactionHttp: new TransactionHttp(nodeUrl),
    });
  }
  return httpClientsByUrl.get(nodeUrl);
}

function getActiveNodeUrl() {
  return activeNodeUrl;
}

/**
 * Perform a JSON GET request with timeout against a specific URL
 */
async function fetchJson(url, timeoutMs = 5000) {
  let controller = null;
  let timer = null;
  const options = {};
  if (typeof AbortController !== 'undefined') {
    controller = new AbortController();
    timer = setTimeout(() => controller.abort(), timeoutMs);
    options.signal = controller.signal;
  }
  try {
    const res = await fetch(url, options);
    if (!res.ok) {
      const err = new Error(`HTTP ${res.status} for ${url}`);
      err.status = res.status;
      throw err;
    }
    return await res.json();
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Execute an async operation across candidate Sirius REST nodes with automatic failover.
 * Also checks `/node/health` if the primary node fails so frozen nodes (`ServiceUnavailable` / 503) are skipped.
 */
async function withNodeFailover(fn) {
  const orderedUrls = [
    activeNodeUrl,
    ...CANDIDATE_NODE_URLS.filter((u) => u !== activeNodeUrl),
  ];
  let lastError = null;

  for (let i = 0; i < orderedUrls.length; i++) {
    const candidateUrl = orderedUrls[i];
    try {
      if (i > 0) {
        // Pre-verify candidate node health before failing over to it
        try {
          const health = await fetchJson(`${candidateUrl}/node/health`, 3000);
          if (health && health.status && (health.status.apiNode === 'down' || health.status.db === 'down')) {
            throw new Error(`Node ${candidateUrl} reported unhealthy status: ${JSON.stringify(health.status)}`);
          }
        } catch (healthErr) {
          // Only skip if explicitly 503 / ServiceUnavailable or network error
          if (healthErr.status === 503 || (healthErr.message && healthErr.message.includes('unhealthy'))) {
            lastError = healthErr;
            continue;
          }
        }
      }
      const clients = getHttpClients(candidateUrl);
      const result = await fn(clients, candidateUrl);
      if (candidateUrl !== activeNodeUrl) {
        console.log(`[Testnet2Client] Automatic RPC failover: switched active node ${activeNodeUrl} -> ${candidateUrl}`);
        activeNodeUrl = candidateUrl;
      }
      return result;
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error('All configured Sirius L1 RPC endpoints failed');
}

/**
 * Get current blockchain height from Sirius L1 with automatic multi-node failover
 */
async function getLatestHeight() {
  return withNodeFailover(
    ({ chainHttp }) =>
      new Promise((resolve, reject) => {
        chainHttp.getBlockchainHeight().subscribe(
          (h) => resolve(h.compact()),
          (err) => reject(err)
        );
      })
  );
}

const onChainBalancesCache = new Map();
const inFlightBalanceFetches = new Map();
const ONCHAIN_BALANCE_CACHE_TTL_MS = 15000; // 15s TTL to avoid flooding Sirius L1 RPC
const MAX_ONCHAIN_BALANCE_CACHE_ENTRIES = 2000; // Bound cache size to prevent memory DoS

function normalizeCacheAddress(rawAddress) {
  return String(rawAddress || '').replace(/[- ]/g, '').trim().toUpperCase();
}

function invalidateAccountBalanceCache(rawAddress) {
  if (rawAddress) {
    onChainBalancesCache.delete(normalizeCacheAddress(rawAddress));
  } else {
    onChainBalancesCache.clear();
  }
}

/**
 * Get on-chain mosaic balances for an account address with automatic failover, memory caching,
 * and in-flight promise deduplication (prevents concurrent RPC storms for the same address).
 */
async function getAccountOnChainBalances(rawAddress, forceRefresh = false) {
  const normAddr = normalizeCacheAddress(rawAddress);
  const now = Date.now();
  const cached = onChainBalancesCache.get(normAddr);
  if (!forceRefresh && cached && (now - cached.timestamp < ONCHAIN_BALANCE_CACHE_TTL_MS)) {
    return new Map(cached.balances);
  }

  if (!forceRefresh && inFlightBalanceFetches.has(normAddr)) {
    const shared = await inFlightBalanceFetches.get(normAddr);
    return new Map(shared);
  }

  const fetchPromise = withNodeFailover(
    ({ accountHttp }) =>
      new Promise((resolve, reject) => {
        try {
          const addr = Address.createFromRawAddress(normAddr);
          accountHttp.getAccountInfo(addr).subscribe(
            (info) => {
              const res = new Map();
              for (const m of info.mosaics) {
                res.set(m.id.toHex().toLowerCase(), uint64ToBigIntExact(m.amount));
              }
              resolve(res);
            },
            (err) => {
              if (
                (err.response && err.response.status === 404) ||
                (err.message && (err.message.includes('ResourceNotFound') || err.message.includes('404')))
              ) {
                resolve(new Map());
              } else {
                reject(err);
              }
            }
          );
        } catch (e) {
          reject(e);
        }
      })
  ).finally(() => {
    inFlightBalanceFetches.delete(normAddr);
  });

  inFlightBalanceFetches.set(normAddr, fetchPromise);
  const balances = await fetchPromise;
  if (onChainBalancesCache.size >= MAX_ONCHAIN_BALANCE_CACHE_ENTRIES && !onChainBalancesCache.has(normAddr)) {
    const oldestKey = onChainBalancesCache.keys().next().value;
    if (oldestKey !== undefined) onChainBalancesCache.delete(oldestKey);
  }
  onChainBalancesCache.set(normAddr, { balances, timestamp: Date.now() });
  return new Map(balances);
}

/**
 * Query transaction status and details from Sirius L1 with automatic failover
 */
async function queryTransaction(txHash) {
  try {
    return await withNodeFailover(
      ({ transactionHttp }) =>
        new Promise((resolve, reject) => {
          transactionHttp.getTransaction(txHash).subscribe(
            (tx) => {
              resolve({
                found: true,
                height: tx.transactionInfo ? tx.transactionInfo.height.compact() : 0,
                type: tx.type === 16724 ? 'Transfer' : tx.type,
                typeHex: tx.type === 16724 ? '4154' : (tx.type ? tx.type.toString(16) : ''),
                recipient: tx.recipient ? tx.recipient.plain() : '',
                signer: tx.signer ? tx.signer.address.plain() : '',
                sender: tx.signer ? tx.signer.publicKey : '',
                senderAddress: tx.signer ? tx.signer.address.plain() : '',
                signerPublicKey: tx.signer ? tx.signer.publicKey : '',
                mosaics: (tx.mosaics || []).map((m) => {
                  const hex = m.id.toHex().toUpperCase();
                  return {
                    mosaicIdHex: hex,
                    mosaicIdDec: BigInt('0x' + hex).toString(10),
                    amount: uint64ToBigIntExact(m.amount),
                  };
                }),
              });
            },
            (err) => {
              if (
                (err.response && err.response.status === 404) ||
                (err.message && (err.message.includes('ResourceNotFound') || err.message.includes('404')))
              ) {
                resolve({ found: false });
              } else {
                reject(err);
              }
            }
          );
        })
    );
  } catch (_) {
    return { found: false };
  }
}

/**
 * Announce a signed transaction with automatic multi-node failover
 */
async function announceSignedTransaction(signedTx) {
  return withNodeFailover(
    ({ transactionHttp }, nodeUrl) =>
      new Promise((resolve, reject) => {
        transactionHttp.announce(signedTx).subscribe(
          (r) => resolve({ response: r, nodeUrl }),
          (err) => reject(err)
        );
      })
  );
}

/**
 * Verify a transaction was accepted on-chain (not rejected in group === 'failed')
 */
async function verifyTransactionAccepted(txHash, maxWaitMs = 6000, pollIntervalMs = 600) {
  const start = Date.now();
  const sleepMs = Math.max(200, Number(pollIntervalMs) || 600);
  while (Date.now() - start < maxWaitMs) {
    try {
      const status = await withNodeFailover(async (_, nodeUrl) => {
        try {
          return await fetchJson(`${nodeUrl}/transactionStatus/${txHash}`, 3000);
        } catch (e) {
          if (e.status === 404 || (e.message && e.message.includes('404'))) {
            // 404 on /transactionStatus simply means not indexed yet; do not treat as node down
            return { group: 'not_found' };
          }
          throw e;
        }
      });
      if (status && status.group) {
        if (status.group === 'failed') {
          throw new Error(`Sirius node rejected transaction ${txHash}: ${status.status || 'Validation failed'}`);
        }
        if (status.group === 'unconfirmed' || status.group === 'confirmed') {
          return status;
        }
      }
    } catch (err) {
      if (err.message && err.message.startsWith('Sirius node rejected transaction')) {
        throw err;
      }
    }
    await new Promise((r) => setTimeout(r, sleepMs));
  }
  return { hash: txHash, group: 'announced' };
}

/**
 * Broadcast an on-chain transfer from the Exchange Vault to a user
 */
async function broadcastVaultTransfer(vaultAccount, recipientAddressStr, mosaicIdHex, amountAtomic, messageStr = 'CLOB Withdrawal') {
  const amt = BigInt(amountAtomic);
  if (amt <= 0n || amt > 0xFFFFFFFFFFFFFFFFn) {
    throw new Error(`Invalid transfer amount: ${amt.toString()}`);
  }
  const cleanMosaicHex = (mosaicIdHex || '').replace(/^0x/i, '').toUpperCase().padStart(16, '0');
  const onChainMosaicHex = (
    cleanMosaicHex === config.WASM_XPX_HEX ||
    cleanMosaicHex === config.XPX_NAMESPACE_ID.toUpperCase() ||
    cleanMosaicHex === config.XPX_MOSAIC_ID
  ) ? config.XPX_MOSAIC_ID : cleanMosaicHex;

  const cleanRecipient = String(recipientAddressStr || '').replace(/[- ]/g, '').trim().toUpperCase();
  if (RawAddress && typeof RawAddress.isValidAddress === 'function') {
    if (!RawAddress.isValidAddress(RawAddress.stringToAddress(cleanRecipient))) {
      throw new Error(`Invalid Sirius recipient address checksum: ${recipientAddressStr}`);
    }
  }
  const recipient = Address.createFromRawAddress(cleanRecipient);
  if (recipient.networkType !== config.NETWORK_TYPE) {
    throw new Error(
      `Recipient address network mismatch: address ${cleanRecipient} has networkType ${recipient.networkType}, expected ${config.NETWORK_TYPE}`
    );
  }
  const mosaic = new Mosaic(
    new MosaicId(onChainMosaicHex),
    UInt64.fromHex(amt.toString(16).padStart(16, '0'))
  );

  const safeMsg = String(messageStr || 'CLOB Withdrawal').slice(0, 250);
  const transferTx = TransferTransaction.create(
    Deadline.create(),
    recipient,
    [mosaic],
    PlainMessage.create(safeMsg),
    config.NETWORK_TYPE,
    UInt64.fromUint(200000) // Default fee units
  );

  const signedTx = typeof vaultAccount.preV2Sign === 'function'
    ? vaultAccount.preV2Sign(transferTx, config.GENERATION_HASH)
    : vaultAccount.sign(transferTx, config.GENERATION_HASH);

  const { response: res, nodeUrl } = await announceSignedTransaction(signedTx);

  await verifyTransactionAccepted(signedTx.hash);

  return {
    txHash: signedTx.hash,
    payload: signedTx.payload,
    status: res.message,
    endpoint: nodeUrl,
  };
}

/**
 * Load or initialize Exchange Vault account (supports env var, file, or safe initialization)
 */
function getOrInitVaultAccount() {
  if (process.env.SIRIUS_VAULT_PRIVATE_KEY && /^[0-9a-fA-F]{64}$/.test(process.env.SIRIUS_VAULT_PRIVATE_KEY.trim())) {
    return Account.createFromPrivateKey(process.env.SIRIUS_VAULT_PRIVATE_KEY.trim(), config.NETWORK_TYPE, 1);
  }

  if (fs.existsSync(config.VAULT_KEY_FILE)) {
    const raw = fs.readFileSync(config.VAULT_KEY_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (!data || !data.privateKey || !/^[0-9a-fA-F]{64}$/.test(data.privateKey)) {
      throw new Error(`CRITICAL: Vault key file ${config.VAULT_KEY_FILE} exists but is malformed. Refusing to overwrite.`);
    }
    return Account.createFromPrivateKey(data.privateKey, config.NETWORK_TYPE, 1);
  }

  // Generate fresh vault account ONLY if no vault_account.json exists and not configured via env
  const newVault = Account.generateNewAccount(config.NETWORK_TYPE);
  const payload = {
    address: newVault.address.plain(),
    publicKey: newVault.publicKey,
    privateKey: newVault.privateKey,
    createdAt: new Date().toISOString(),
    network: config.NETWORK_NAME,
  };

  fs.mkdirSync(path.dirname(config.VAULT_KEY_FILE), { recursive: true, mode: 0o700 });
  fs.writeFileSync(config.VAULT_KEY_FILE, JSON.stringify(payload, null, 2), { mode: 0o600 });
  return newVault;
}

/**
 * Load Treasury account from env var or cluster/treasury_account.json
 */
function getOrInitTreasuryAccount() {
  if (process.env.SIRIUS_TREASURY_PRIVATE_KEY && /^[0-9a-fA-F]{64}$/.test(process.env.SIRIUS_TREASURY_PRIVATE_KEY.trim())) {
    return Account.createFromPrivateKey(process.env.SIRIUS_TREASURY_PRIVATE_KEY.trim(), config.NETWORK_TYPE, 1);
  }

  const treasuryFile = config.TREASURY_KEY_FILE || path.join(config.STATE_DIR, 'treasury_account.json');
  if (fs.existsSync(treasuryFile)) {
    const raw = fs.readFileSync(treasuryFile, 'utf8');
    const data = JSON.parse(raw);
    if (data && data.privateKey && /^[0-9a-fA-F]{64}$/.test(data.privateKey)) {
      return Account.createFromPrivateKey(data.privateKey, config.NETWORK_TYPE, 1);
    }
  }
  return null;
}

/**
 * Load Fee Collector account from env var or cluster/fee_collector_account.json
 */
function getOrInitFeeCollectorAccount() {
  if (process.env.SIRIUS_FEE_COLLECTOR_PRIVATE_KEY && /^[0-9a-fA-F]{64}$/.test(process.env.SIRIUS_FEE_COLLECTOR_PRIVATE_KEY.trim())) {
    return Account.createFromPrivateKey(process.env.SIRIUS_FEE_COLLECTOR_PRIVATE_KEY.trim(), config.NETWORK_TYPE, 1);
  }

  const feeFile = config.FEE_COLLECTOR_FILE || path.join(config.STATE_DIR, 'fee_collector_account.json');
  if (fs.existsSync(feeFile)) {
    const raw = fs.readFileSync(feeFile, 'utf8');
    const data = JSON.parse(raw);
    if (data && data.privateKey && /^[0-9a-fA-F]{64}$/.test(data.privateKey)) {
      return Account.createFromPrivateKey(data.privateKey, config.NETWORK_TYPE, 1);
    }
  }
  return null;
}

module.exports = {
  fetchJson,
  withNodeFailover,
  getActiveNodeUrl,
  announceSignedTransaction,
  verifyTransactionAccepted,
  getLatestHeight,
  getAccountOnChainBalances,
  invalidateAccountBalanceCache,
  queryTransaction,
  broadcastVaultTransfer,
  getOrInitVaultAccount,
  getOrInitTreasuryAccount,
  getOrInitFeeCollectorAccount,
};
