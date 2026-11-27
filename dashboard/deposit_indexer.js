/**
 * Sirius Testnet 2 Automated On-Chain Deposit Indexer
 * 
 * Periodically polls incoming TransferTransactions to the exchange Vault account on Testnet 2,
 * validates received mosaics (XPX, USDT, BNB), checks block confirmation depth,
 * ensures idempotency, and credits user balances.
 */
const fs = require('fs');
const path = require('path');
module.paths.push(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules'));
const {
  Address,
  TransactionQueryParams,
} = require('tsjs-xpx-chain-sdk');
const config = require('./testnet2_config');
const t2Client = require('./testnet2_client');
const { atomicWriteJsonSync } = require('./atomic_fs');
let clobApi = null;
try {
  clobApi = require('./clob_api');
} catch (_) {}

function getRequiredConfirmationDepth() {
  const isMainnet =
    config.NETWORK_TYPE === 184 ||
    String(config.NETWORK_ENV || '').toLowerCase() === 'mainnet';
  const configured = Number(config.CONFIRMATION_DEPTH || 1);
  return isMainnet ? Math.max(3, configured) : Math.max(1, configured);
}
const MAX_SCAN_PAGES = 20;
const DEFAULT_POLL_INTERVAL_MS = 10000; // 10s poll cycle (block time ~15s)

let isRunning = false;
let isScanning = false;
let pollTimer = null;
let processedHashes = new Set();
let vaultAddress = null;

/**
 * Load processed transaction hashes for idempotency
 */
function loadProcessedDeposits() {
  processedHashes.clear();
  if (fs.existsSync(config.PROCESSED_DEPOSITS_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(config.PROCESSED_DEPOSITS_FILE, 'utf8'));
      const entries = Array.isArray(data)
        ? data
        : (data && typeof data === 'object' ? Object.entries(data).map(([k, v]) => (typeof v === 'object' && v ? { txHash: k, ...v } : k)) : []);
      for (const item of entries) {
        const hash = typeof item === 'string' ? item : item.txHash;
        if (hash) {
          if (typeof item === 'object' && item.mosaicIdHex) {
            processedHashes.add(`${hash.toLowerCase()}:${item.mosaicIdHex.toLowerCase()}`);
          } else {
            processedHashes.add(hash.toLowerCase());
          }
        }
      }
    } catch (e) {
      console.warn('[Deposit Indexer] Could not parse processed_deposits.json:', e.message);
    }
  }
}

/**
 * Persist a newly processed deposit atomically (write temp file + rename)
 */
function recordProcessedDeposit(record) {
  const txKey = record.txHash.toLowerCase();
  const mosaicKey = record.mosaicIdHex ? `${txKey}:${record.mosaicIdHex.toLowerCase()}` : txKey;
  processedHashes.add(mosaicKey);
  if (!record.mosaicIdHex) {
    processedHashes.add(txKey);
  }
  try {
    let list = null;
    const filePath = config.PROCESSED_DEPOSITS_FILE;
    const bakPath = `${filePath}.bak`;

    if (fs.existsSync(filePath)) {
      try {
        const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        if (Array.isArray(parsed)) {
          list = parsed;
        }
      } catch (parseErr) {
        const corruptPath = `${filePath}.corrupt.${Date.now()}`;
        try {
          fs.copyFileSync(filePath, corruptPath);
          console.error(`[Deposit Indexer] ⚠️ Corrupt processed_deposits.json backed up to ${corruptPath}: ${parseErr.message}`);
        } catch (_) {
          console.error(`[Deposit Indexer] ⚠️ Corrupt processed_deposits.json (backup failed): ${parseErr.message}`);
        }
      }
    }

    // If primary file was missing or corrupt, try last-known-good backup before falling back to in-memory hashes
    if (list === null && fs.existsSync(bakPath)) {
      try {
        const bakParsed = JSON.parse(fs.readFileSync(bakPath, 'utf8'));
        if (Array.isArray(bakParsed)) {
          list = bakParsed;
          console.warn(`[Deposit Indexer] Restored ${list.length} prior records from ${bakPath}`);
        }
      } catch (_) {}
    }

    if (list === null) {
      // Never wipe in-memory idempotency state if disk files were unreadable
      list = Array.from(processedHashes)
        .filter((k) => !k.includes(':') && k !== txKey)
        .map((h) => ({ txHash: h, recoveredFromMemory: true }));
    }

    list.push(record);
    atomicWriteJsonSync(filePath, list, { backup: true });
    if (!fs.existsSync(bakPath)) {
      atomicWriteJsonSync(bakPath, list, { backup: false });
    }
  } catch (err) {
    console.error('[Deposit Indexer] Error saving processed deposit:', err.message);
  }
}

/**
 * Get current height from Testnet 2 with multi-node failover
 */
async function fetchCurrentHeight() {
  try {
    return await t2Client.getLatestHeight();
  } catch (err) {
    return t2Client.withNodeFailover(async (clients) => {
      return new Promise((resolve, reject) => {
        clients.chainHttp.getBlockchainHeight().subscribe(
          (h) => resolve(h.compact()),
          (e) => reject(e)
        );
      });
    });
  }
}

/**
 * Fetch incoming transactions for vault account with automatic node failover and pagination
 */
async function fetchVaultIncomingTransactions(addr, limit = 50, pageNumber = 1) {
  return t2Client.withNodeFailover(async (clients) => {
    return new Promise((resolve, reject) => {
      let qp;
      if (typeof TransactionQueryParams === 'function') {
        qp = new TransactionQueryParams();
        qp.pageSize = limit;
        if (pageNumber > 1) {
          qp.pageNumber = pageNumber;
        }
      }
      clients.accountHttp.incomingTransactions(addr, qp).subscribe(
        (txs) => resolve(txs || []),
        (err) => {
          // Account has no transactions on chain yet
          if (
            (err.response && err.response.status === 404) ||
            (err.message && (err.message.includes('ResourceNotFound') || err.message.includes('404')))
          ) {
            resolve([]);
          } else {
            reject(err);
          }
        }
      );
    });
  });
}

async function syncVaultBalances() {
  if (!vaultAddress || !clobApi) return;
  try {
    const balances = await t2Client.getAccountOnChainBalances(vaultAddress.plain());
    for (const [mHex, amount] of balances.entries()) {
      if (typeof clobApi.setVaultBalanceCache === 'function') {
        clobApi.setVaultBalanceCache(mHex, amount);
      }
    }
  } catch (_) {}
}

/**
 * Execute a single scan for new confirmed deposits
 */
async function scanDeposits(onDepositCallback) {
  if (!vaultAddress || isScanning) return;
  const isMainnet =
    config.NETWORK_TYPE === 184 ||
    String(config.NETWORK_ENV || '').toLowerCase() === 'mainnet' ||
    String(process.env.SIRIUS_NETWORK || '').toLowerCase() === 'mainnet';
  if (isMainnet && process.env.ALLOW_MAINNET_LEGACY_VAULT !== 'true') {
    return;
  }
  isScanning = true;

  try {
    await syncVaultBalances();
    const currentHeight = await fetchCurrentHeight();

    const targets = [vaultAddress];
    if (config.SUPERCONTRACT_ADDRESS) {
      try {
        const scAddr = Address.createFromRawAddress(config.SUPERCONTRACT_ADDRESS);
        if (scAddr.plain() !== vaultAddress.plain()) {
          targets.push(scAddr);
        }
      } catch (_) {}
    }

    const requiredDepth = getRequiredConfirmationDepth();
    for (const targetAddr of targets) {
      let page = 1;
      while (page <= MAX_SCAN_PAGES) {
        const incomingTxs = await fetchVaultIncomingTransactions(targetAddr, 50, page);
        if (!incomingTxs || incomingTxs.length === 0) break;

        let hasNewOnPage = false;
        let hasUnconfirmedOrFailedOnPage = false;
        for (const tx of incomingTxs) {
          if (!tx.transactionInfo || !tx.transactionInfo.hash) continue;
          const txHash = tx.transactionInfo.hash.toLowerCase();

          // Legacy tx-level idempotency check (for string-only legacy records without mosaicIdHex)
          if (processedHashes.has(txHash)) continue;

          // Type 16724 = TransferTransaction
          if (tx.type !== 16724) continue;

          // Confirmation depth check (must be confirmed on-chain with height > 0)
          const txHeight = tx.transactionInfo.height ? tx.transactionInfo.height.compact() : 0;
          if (txHeight <= 0 || currentHeight - txHeight < requiredDepth) {
            hasUnconfirmedOrFailedOnPage = true;
            continue; // Wait for confirmation depth
          }

          // Verify recipient matches target and sender is not the target itself
          if (!tx.recipient || tx.recipient.plain() !== targetAddr.plain()) {
            continue;
          }
          if (!tx.signer || !tx.signer.address || tx.signer.address.plain() === targetAddr.plain()) {
            continue;
          }

          const senderAddress = tx.signer.address.plain();
          const senderPublicKey = tx.signer.publicKey;

          // Process each deposited mosaic
          for (const m of tx.mosaics || []) {
            const mosaicIdHex = m.id.toHex().toLowerCase();
            const mosaicKey = `${txHash}:${mosaicIdHex}`;
            if (processedHashes.has(mosaicKey)) continue;

            const amountAtomic = m.amount && typeof m.amount.toHex === 'function'
              ? BigInt('0x' + m.amount.toHex())
              : BigInt(m.amount.compact());

            if (amountAtomic <= 0n) continue;

            const depositRecord = {
              txHash,
              height: txHeight,
              senderAddress,
              senderPublicKey,
              recipientAddress: targetAddr.plain(),
              mosaicIdHex,
              amountAtomic: amountAtomic.toString(),
              timestamp: Date.now(),
            };

            try {
              if (typeof onDepositCallback === 'function') {
                await onDepositCallback(depositRecord);
              }
              recordProcessedDeposit(depositRecord);
              hasNewOnPage = true;
              console.log(`[Deposit Indexer] Credited deposit ${mosaicIdHex} (${amountAtomic}) from ${senderAddress} to ${targetAddr.plain()} (Tx: ${txHash.slice(0, 16)}...)`);
            } catch (cbErr) {
              hasUnconfirmedOrFailedOnPage = true;
              console.error(`[Deposit Indexer] Callback error for deposit tx ${txHash}:`, cbErr.message);
            }
          }
        }

        // Fast exit ONLY when all confirmed txs on this page were already processed and nothing is pending/failed
        if (!hasNewOnPage && !hasUnconfirmedOrFailedOnPage && processedHashes.size > 0) {
          break;
        }
        if (incomingTxs.length < 50) break;
        page++;
      }
    }
  } catch (err) {
    console.warn('[Deposit Indexer] Poll cycle encountered temporary error:', err.message);
  } finally {
    isScanning = false;
  }
}

/**
 * Start the deposit indexer polling loop
 */
function startDepositIndexer(vaultRawAddress, onDepositCallback, intervalMs = DEFAULT_POLL_INTERVAL_MS) {
  if (isRunning) return;
  isRunning = true;
  vaultAddress = Address.createFromRawAddress(vaultRawAddress);
  loadProcessedDeposits();

  console.log(`[Deposit Indexer] Started monitoring deposits for Vault ${vaultAddress.plain()} (Interval: ${intervalMs}ms)`);

  const pollLoop = async () => {
    if (!isRunning) return;
    await scanDeposits(onDepositCallback);
    if (isRunning) {
      pollTimer = setTimeout(pollLoop, intervalMs);
    }
  };

  pollLoop();
}

/**
 * Stop the deposit indexer polling loop
 */
function stopDepositIndexer() {
  isRunning = false;
  if (pollTimer) {
    clearTimeout(pollTimer);
    pollTimer = null;
  }
  console.log('[Deposit Indexer] Stopped.');
}

module.exports = {
  startDepositIndexer,
  stopDepositIndexer,
  scanDeposits,
  loadProcessedDeposits,
  recordProcessedDeposit,
};
