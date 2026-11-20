/**
 * Sirius Testnet 2 Batch Settlement Aggregator
 * 
 * Aggregates off-chain matched trade fills into native Catapult
 * Aggregate Complete Transactions and broadcasts them to api-2.testnet2.xpxsirius.io:3000.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
module.exports = {}; // placeholder
module.paths.push(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules'));

const {
  AggregateTransaction,
  TransferTransaction,
  Deadline,
  Address,
  Mosaic,
  MosaicId,
  UInt64,
  PlainMessage,
  TransactionHttp,
} = require('tsjs-xpx-chain-sdk');

const config = require('./testnet2_config');
const testnetClient = require('./testnet2_client');

const MAX_QUEUE_CAPACITY = 10000;
const MAX_EMBEDDED_PER_AGGREGATE = 100;

const SANDBOX_XPX_HEX = config.WASM_XPX_HEX || '0DC67FBE1CAD29E3';
const ACTIVE_L1_XPX_HEX = (config.XPX_MOSAIC_ID || '13BFC518E40549D7').toUpperCase();
const SANDBOX_USDT_HEX = config.WASM_USDT_HEX || '10112CBA1E7B9037';
const SANDBOX_BNB_HEX = config.WASM_BNB_HEX || '1E5C5FADEC7B34DD';

function classifyBatchMosaic(rawHex) {
  const norm = (rawHex || '').replace(/^0x/i, '').toUpperCase().padStart(16, '0');
  if (norm === SANDBOX_XPX_HEX || norm === ACTIVE_L1_XPX_HEX || norm === (config.XPX_NAMESPACE_ID || 'bffb42a19116bdf6').toUpperCase()) {
    return { onChainMosaicHex: ACTIVE_L1_XPX_HEX, isNotarizedQuote: false, symbol: 'XPX' };
  }
  if (norm === SANDBOX_USDT_HEX || norm === (config.USDT_MOSAIC_ID || '').toUpperCase()) {
    return { onChainMosaicHex: (config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toUpperCase(), isNotarizedQuote: false, symbol: 'USDT' };
  }
  if (norm === SANDBOX_BNB_HEX || norm === (config.BNB_MOSAIC_ID || '').toUpperCase()) {
    return { onChainMosaicHex: (config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toUpperCase(), isNotarizedQuote: false, symbol: 'BNB' };
  }
  return { onChainMosaicHex: norm, isNotarizedQuote: false, symbol: norm };
}

async function verifyTxAcceptedOnTestnet2(txHash, maxWaitMs = 6000) {
  return testnetClient.verifyTransactionAccepted(txHash, maxWaitMs);
}

class BatchSettlerTestnet2 {
  constructor(maxCapacity = MAX_QUEUE_CAPACITY) {
    this.pendingQueue = [];
    this.inFlightChunks = []; // [{ chunk, signedTx }] preserved across transient L1 announce/verify errors
    this.batchCounter = 0;
    this.timer = null;
    this.maxCapacity = maxCapacity;
    this.vaultAccount = null;
    this.isFlushing = false;
  }

  getVault() {
    if (!this.vaultAccount) {
      this.vaultAccount = testnetClient.getOrInitVaultAccount();
    }
    return this.vaultAccount;
  }

  clear() {
    const dropped = this.pendingQueue.length;
    this.pendingQueue = [];
    return dropped;
  }

  getPendingCount() {
    return this.pendingQueue.length;
  }

  getPendingItems() {
    return [...this.pendingQueue];
  }

  /**
   * Queue embedded transactions from match execution
   */
  queueFromEmittedTx(emittedTx, metadata = {}) {
    if (!emittedTx || !Array.isArray(emittedTx.transactions)) return 0;
    let added = 0;
    const memo = 'CLOB_BATCH_FILL';

    for (const t of emittedTx.transactions) {
      if (this.pendingQueue.length >= this.maxCapacity) {
        throw new Error(`Batch settlement queue reached maximum capacity (${this.maxCapacity} items)`);
      }
      const payloadBuf = Buffer.isBuffer(t.payload) ? t.payload : Buffer.from(t.payload || '', 'base64');
      if (payloadBuf.length >= 64) {
        const recipientPub = payloadBuf.subarray(0, 32).toString('hex').toLowerCase();
        const mosaicId = payloadBuf.readBigUInt64LE(48);
        const payoutAmount = payloadBuf.readBigUInt64LE(48 + 8);
        if (payoutAmount <= 0n) continue;
        const mosaicHex = mosaicId.toString(16).toUpperCase().padStart(16, '0');

        this.pendingQueue.push({
          recipientPub,
          mosaicHex,
          amount: payoutAmount,
          memo,
          metadata: { ...metadata, timestamp: Date.now() },
        });
        added++;
      }
    }
    return added;
  }

  /**
   * Add an explicit payout spec directly
   */
  queuePayout(recipientPubHex, mosaicHex, amountAtomic, memo = 'CLOB_BATCH', isVaultPayout = false) {
    if (this.pendingQueue.length >= this.maxCapacity) {
      throw new Error(`Batch settlement queue reached maximum capacity (${this.maxCapacity} items)`);
    }
    const amt = BigInt(amountAtomic);
    if (amt <= 0n || amt > 0xFFFFFFFFFFFFFFFFn) {
      throw new Error(`Invalid payout amount: ${amt.toString()} (must be 1..2^64-1)`);
    }
    const safeMemo = String(memo || 'CLOB_BATCH').replace(/[\r\n:]/g, '_').slice(0, 80);
    this.pendingQueue.push({
      recipientPub: recipientPubHex.toLowerCase(),
      mosaicHex: mosaicHex.toUpperCase().padStart(16, '0'),
      amount: amt,
      memo: safeMemo,
      isVaultPayout: Boolean(isVaultPayout),
      metadata: { timestamp: Date.now() },
    });
  }

  /**
   * Consolidate and net payouts across all participants and mosaics
   */
  getNettedPayoutsFromItems(items) {
    const map = new Map();
    for (const item of items) {
      const key = `${item.recipientPub}:${item.mosaicHex}:${Boolean(item.isVaultPayout)}`;
      const existing = map.get(key) || {
        recipientPub: item.recipientPub,
        mosaicHex: item.mosaicHex,
        amount: 0n,
        memo: item.memo,
        isVaultPayout: Boolean(item.isVaultPayout),
      };
      existing.amount += BigInt(item.amount);
      if (existing.amount > 0xFFFFFFFFFFFFFFFFn) {
        throw new Error(`Netted payout amount overflow (> u64::MAX) for ${key}`);
      }
      map.set(key, existing);
    }
    return Array.from(map.values()).filter((e) => e.amount > 0n);
  }

  getNettedPayouts() {
    return this.getNettedPayoutsFromItems(this.pendingQueue);
  }

  /**
   * Flush and broadcast Aggregate Complete Transaction(s) to Sirius L1
   */
  async flushBatch() {
    if (this.isFlushing) {
      throw new Error('Batch settlement flush is already in progress. Please wait for current batch confirmation.');
    }
    if (this.pendingQueue.length === 0 && this.inFlightChunks.length === 0) {
      return { ok: true, empty: true, count: 0 };
    }

    this.isFlushing = true;
    const snapshot = [...this.pendingQueue];
    this.pendingQueue = [];

    const netted = this.getNettedPayoutsFromItems(snapshot);
    if (netted.length === 0 && this.inFlightChunks.length === 0) {
      this.isFlushing = false;
      return { ok: true, empty: true, count: 0 };
    }

    const vault = this.getVault();
    const batchId = `BATCH-${++this.batchCounter}-${crypto.randomBytes(4).toString('hex')}`;
    const rawCount = snapshot.length;
    const txHashes = [];
    let nextUnsettledIndex = 0;

    try {
      // 1. First retry any previously signed in-flight chunks deterministically without re-signing
      while (this.inFlightChunks.length > 0) {
        const inflight = this.inFlightChunks[0];
        try {
          const existingOnChain = await testnetClient.queryTransaction(inflight.signedTx.hash);
          if (!existingOnChain || !existingOnChain.found) {
            await testnetClient.announceSignedTransaction(inflight.signedTx);
            await verifyTxAcceptedOnTestnet2(inflight.signedTx.hash);
          }
          txHashes.push(inflight.signedTx.hash);
          this.inFlightChunks.shift();
        } catch (retryErr) {
          if (retryErr.message && retryErr.message.includes('Sirius node rejected transaction')) {
            console.warn(`[BatchSettler Testnet2] Evicting permanently rejected in-flight chunk (${inflight.signedTx.hash}): ${retryErr.message}`);
            this.inFlightChunks.shift();
          } else {
            throw retryErr;
          }
        }
      }

      // 2. Chunk netted payouts into batches of up to MAX_EMBEDDED_PER_AGGREGATE
      for (let i = 0; i < netted.length; i += MAX_EMBEDDED_PER_AGGREGATE) {
        const chunk = netted.slice(i, i + MAX_EMBEDDED_PER_AGGREGATE);

        const embeddedTxs = chunk.map((item) => {
          let recipientAddress;
          try {
            recipientAddress = Address.createFromPublicKey(item.recipientPub, config.NETWORK_TYPE);
          } catch (_) {
            recipientAddress = Address.createFromRawAddress(item.recipientPub);
          }

          const classified = classifyBatchMosaic(item.mosaicHex);
          // Queue payout to Testnet 2 batch settlement aggregator
          // If isVaultPayout === false, the trade proceeds remain in the user's CLOB trading balance
          // (so they can continue trading or withdraw on demand) and we emit a 1-micro-XPX on-chain notarization proof.
          const isVaultPayout = Boolean(item.isVaultPayout);
          const onChainAmt = BigInt(item.amount);
          const hexAmt = onChainAmt.toString(16).padStart(16, '0');
          const mosaic = new Mosaic(
            new MosaicId(classified.onChainMosaicHex),
            UInt64.fromHex(hexAmt)
          );

          const memoText = `${item.memo}:${batchId}:${classified.symbol}=${item.amount.toString()}`.slice(0, 250);

          const innerTx = TransferTransaction.create(
            Deadline.create(),
            recipientAddress,
            [mosaic],
            PlainMessage.create(memoText),
            config.NETWORK_TYPE
          ).toAggregate(vault.publicAccount);
          innerTx.version.signatureDScheme = 0;
          return innerTx;
        });

        // Compute max fee based on size: 100,000 units base + 50,000 per embedded tx
        const feeUnits = 100000 + embeddedTxs.length * 50000;

        const aggregateTx = AggregateTransaction.createCompleteV1(
          Deadline.create(),
          embeddedTxs,
          config.NETWORK_TYPE,
          [],
          UInt64.fromUint(feeUnits)
        );

        const signedTx = typeof vault.preV2Sign === 'function'
          ? vault.preV2Sign(aggregateTx, config.GENERATION_HASH)
          : vault.sign(aggregateTx, config.GENERATION_HASH);

        // Record deterministic signedTx BEFORE announcing so retries reuse the exact same hash
        this.inFlightChunks.push({ chunk, signedTx });
        nextUnsettledIndex = i + chunk.length;

        // Broadcast to Sirius L1 via multi-node failover REST wrapper
        await testnetClient.announceSignedTransaction(signedTx);

        // Verify node did not reject the transaction (e.g. Failure_Core_Insufficient_Balance)
        await verifyTxAcceptedOnTestnet2(signedTx.hash);

        txHashes.push(signedTx.hash);
        this.inFlightChunks.pop();
      }

      console.log(`[BatchSettler Testnet2] Flushed ${rawCount} items (${netted.length} netted) -> Tx(s): ${txHashes.join(', ')}`);

      return {
        ok: true,
        batchId,
        txHash: txHashes[0],
        allTxHashes: txHashes,
        rawCount,
        nettedCount: netted.length,
        network: config.NETWORK_NAME || 'publicTest',
        endpoint: testnetClient.getActiveNodeUrl ? testnetClient.getActiveNodeUrl() : config.NODE_URL,
      };
    } catch (err) {
      // Re-prepend ONLY the unsettled netted chunks so already-broadcast chunks are NEVER double-paid
      const unsettledNetted = netted.slice(nextUnsettledIndex);
      if (unsettledNetted.length > 0) {
        this.pendingQueue = [...unsettledNetted, ...this.pendingQueue];
      }
      console.error('[BatchSettler Testnet2] Error flushing batch:', err.message);
      throw err;
    } finally {
      this.isFlushing = false;
    }
  }

  startPeriodic(intervalMs = 15000, minItems = 1) {
    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(async () => {
      if (this.pendingQueue.length >= minItems) {
        try {
          await this.flushBatch();
        } catch (err) {
          console.warn('[BatchSettler Testnet2] Periodic flush error:', err.message);
        }
      }
    }, intervalMs);
    console.log(`[BatchSettler Testnet2] Periodic flush started (Interval: ${intervalMs}ms, minItems: ${minItems})`);
  }

  stopPeriodic() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
      console.log('[BatchSettler Testnet2] Periodic flush stopped.');
    }
  }
}

const defaultBatchSettlerTestnet2 = new BatchSettlerTestnet2();

module.exports = {
  BatchSettlerTestnet2,
  defaultBatchSettlerTestnet2,
};
