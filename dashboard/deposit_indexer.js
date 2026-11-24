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
let clobApi = null;
try {
  clobApi = require('./clob_api');
} catch (_) {}

const CONFIRMATION_DEPTH = 1; // 1 block confirmation required
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
      if (Array.isArray(data)) {
        for (const item of data) {
          const hash = typeof item === 'string' ? item : item.txHash;
          if (hash) {
            processedHashes.add(hash.toLowerCase());
            if (typeof item === 'object' && item.mosaicIdHex) {
              processedHashes.add(`${hash.toLowerCase()}:${item.mosaicIdHex.toLowerCase()}`);
            }
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
  processedHashes.add(txKey);
  processedHashes.add(mosaicKey);
  try {
    let list = [];
    if (fs.existsSync(config.PROCESSED_DEPOSITS_FILE)) {
      try {
        list = JSON.parse(fs.readFileSync(config.PROCESSED_DEPOSITS_FILE, 'utf8'));
        if (!Array.isArray(list)) list = [];
      } catch (_) {
        list = [];
      }
    }
    list.push(record);
    fs.mkdirSync(path.dirname(config.PROCESSED_DEPOSITS_FILE), { recursive: true });
    const tmpFile = `${config.PROCESSED_DEPOSITS_FILE}.tmp.${process.pid}`;
    fs.writeFileSync(tmpFile, JSON.stringify(list, null, 2));
    fs.renameSync(tmpFile, config.PROCESSED_DEPOSITS_FILE);
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

    for (const targetAddr of targets) {
      let page = 1;
      while (true) {
        const incomingTxs = await fetchVaultIncomingTransactions(targetAddr, 50, page);
        if (!incomingTxs || incomingTxs.length === 0) break;

        let hasNewOnPage = false;
        for (const tx of incomingTxs) {
          if (!tx.transactionInfo || !tx.transactionInfo.hash) continue;
          const txHash = tx.transactionInfo.hash.toLowerCase();

          // Idempotency check
          if (processedHashes.has(txHash)) continue;
          hasNewOnPage = true;

          // Type 16724 = TransferTransaction
          if (tx.type !== 16724) continue;

          // Confirmation depth check (must be confirmed on-chain with height > 0)
          const txHeight = tx.transactionInfo.height ? tx.transactionInfo.height.compact() : 0;
          if (txHeight <= 0 || currentHeight - txHeight < CONFIRMATION_DEPTH) {
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

            const amountAtomic = BigInt(m.amount.compact());

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
              console.log(`[Deposit Indexer] Credited deposit ${mosaicIdHex} (${amountAtomic}) from ${senderAddress} to ${targetAddr.plain()} (Tx: ${txHash.slice(0, 16)}...)`);
            } catch (cbErr) {
              console.error(`[Deposit Indexer] Callback error for deposit tx ${txHash}:`, cbErr.message);
            }
          }
        }

        // Fast exit: if all transactions on page are already indexed and we're not cold-starting, break
        if (!hasNewOnPage && processedHashes.size > 0) {
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
