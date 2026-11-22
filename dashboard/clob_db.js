/**
 * SiriusCLOB SQLite Unified Database Manager
 *
 * Consolidates processedDeposits, ledger credits, withdrawals, and account balances
 * into a single ACID SQLite database with unique constraints, foreign constraints,
 * and atomic transactions.
 */

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const DB_FILE = path.join(CLUSTER_DIR, 'clob_state.db');

let dbInstance = null;

function getDb(customPath = null) {
  if (dbInstance && !customPath) return dbInstance;
  const targetPath = customPath || DB_FILE;
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });

  const db = new Database(targetPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('foreign_keys = ON');

  // Initialize schema with strict unique and integrity check constraints
  db.exec(`
    CREATE TABLE IF NOT EXISTS processed_deposits (
      tx_hash TEXT PRIMARY KEY,
      caller_key TEXT NOT NULL,
      mosaic_id TEXT NOT NULL,
      mosaic_hex TEXT,
      amount TEXT,
      height INTEGER,
      confirmation_depth INTEGER,
      status TEXT NOT NULL CHECK(status IN ('pending', 'confirmed', 'refunding', 'refunded')),
      refund_tx_hash TEXT,
      recipient_address TEXT,
      timestamp INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ledger_credits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tx_hash TEXT UNIQUE NOT NULL,
      caller_key TEXT NOT NULL,
      mosaic_id TEXT NOT NULL,
      amount TEXT NOT NULL CHECK(CAST(amount AS INTEGER) > 0 AND amount NOT LIKE '-%' AND amount != '0'),
      source TEXT NOT NULL CHECK(source IN ('VERIFIED_DEPOSIT', 'MATCH_SETTLEMENT')),
      timestamp INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS withdrawals (
      withdrawal_key TEXT PRIMARY KEY,
      caller_key TEXT NOT NULL,
      mosaic TEXT NOT NULL,
      mosaic_id TEXT NOT NULL,
      amount TEXT NOT NULL,
      amount_atomic TEXT NOT NULL CHECK(CAST(amount_atomic AS INTEGER) > 0 AND amount_atomic NOT LIKE '-%' AND amount_atomic != '0'),
      nonce TEXT NOT NULL,
      expiry INTEGER NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('requested', 'debited', 'broadcast', 'confirmed')),
      tx_hash TEXT,
      block_height INTEGER,
      requested_at INTEGER NOT NULL,
      debited_at INTEGER,
      broadcast_at INTEGER,
      confirmed_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS account_balances (
      caller_key TEXT PRIMARY KEY,
      xpx TEXT NOT NULL DEFAULT '0' CHECK(CAST(xpx AS INTEGER) >= 0 AND xpx NOT LIKE '-%'),
      usdt TEXT NOT NULL DEFAULT '0' CHECK(CAST(usdt AS INTEGER) >= 0 AND usdt NOT LIKE '-%'),
      bnb TEXT NOT NULL DEFAULT '0' CHECK(CAST(bnb AS INTEGER) >= 0 AND bnb NOT LIKE '-%'),
      locked_xpx TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_xpx AS INTEGER) >= 0 AND locked_xpx NOT LIKE '-%'),
      locked_usdt TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_usdt AS INTEGER) >= 0 AND locked_usdt NOT LIKE '-%'),
      locked_bnb TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_bnb AS INTEGER) >= 0 AND locked_bnb NOT LIKE '-%'),
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS bridge_swaps (
      txn_hash TEXT PRIMARY KEY,
      swap_id TEXT UNIQUE NOT NULL,
      network TEXT NOT NULL,
      token TEXT NOT NULL,
      amount TEXT NOT NULL,
      sender_address TEXT,
      recipient_address TEXT NOT NULL,
      sirius_tx_hash TEXT,
      block_number INTEGER,
      confirmations INTEGER,
      status TEXT NOT NULL,
      auto_detected INTEGER NOT NULL DEFAULT 0,
      timestamp TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS collected_fees (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fee_type TEXT NOT NULL,
      mosaic TEXT NOT NULL,
      mosaic_id TEXT NOT NULL,
      amount TEXT NOT NULL,
      amount_atomic TEXT NOT NULL CHECK(CAST(amount_atomic AS INTEGER) > 0 AND amount_atomic NOT LIKE '-%' AND amount_atomic != '0'),
      reference_id TEXT,
      caller_key TEXT,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_collected_fees_mosaic ON collected_fees(mosaic);
    CREATE INDEX IF NOT EXISTS idx_collected_fees_created ON collected_fees(created_at);

    CREATE TABLE IF NOT EXISTS faucet_claims (
      recipient TEXT PRIMARY KEY,
      last_claim_time INTEGER NOT NULL,
      amount_xpx INTEGER NOT NULL,
      claim_date TEXT NOT NULL
    );
  `);

  // Automated schema migration to ensure CHECK constraints on pre-existing databases
  const balancesTable = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='account_balances'").get();
  if (balancesTable && !balancesTable.sql.includes('CAST(xpx AS INTEGER) >= 0')) {
    db.transaction(() => {
      db.exec(`
        CREATE TABLE account_balances_migrated (
          caller_key TEXT PRIMARY KEY,
          xpx TEXT NOT NULL DEFAULT '0' CHECK(CAST(xpx AS INTEGER) >= 0 AND xpx NOT LIKE '-%'),
          usdt TEXT NOT NULL DEFAULT '0' CHECK(CAST(usdt AS INTEGER) >= 0 AND usdt NOT LIKE '-%'),
          bnb TEXT NOT NULL DEFAULT '0' CHECK(CAST(bnb AS INTEGER) >= 0 AND bnb NOT LIKE '-%'),
          locked_xpx TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_xpx AS INTEGER) >= 0 AND locked_xpx NOT LIKE '-%'),
          locked_usdt TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_usdt AS INTEGER) >= 0 AND locked_usdt NOT LIKE '-%'),
          locked_bnb TEXT NOT NULL DEFAULT '0' CHECK(CAST(locked_bnb AS INTEGER) >= 0 AND locked_bnb NOT LIKE '-%'),
          updated_at INTEGER NOT NULL
        );
        INSERT INTO account_balances_migrated SELECT * FROM account_balances;
        DROP TABLE account_balances;
        ALTER TABLE account_balances_migrated RENAME TO account_balances;
      `);
    })();
  }

  const creditsTable = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='ledger_credits'").get();
  if (creditsTable && !creditsTable.sql.includes('CAST(amount AS INTEGER) > 0')) {
    db.transaction(() => {
      db.exec(`
        CREATE TABLE ledger_credits_migrated (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tx_hash TEXT UNIQUE NOT NULL,
          caller_key TEXT NOT NULL,
          mosaic_id TEXT NOT NULL,
          amount TEXT NOT NULL CHECK(CAST(amount AS INTEGER) > 0 AND amount NOT LIKE '-%' AND amount != '0'),
          source TEXT NOT NULL CHECK(source IN ('VERIFIED_DEPOSIT', 'MATCH_SETTLEMENT')),
          timestamp INTEGER NOT NULL
        );
        INSERT INTO ledger_credits_migrated SELECT * FROM ledger_credits;
        DROP TABLE ledger_credits;
        ALTER TABLE ledger_credits_migrated RENAME TO ledger_credits;
      `);
    })();
  }

  const withdrawalsTable = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='withdrawals'").get();
  if (withdrawalsTable && !withdrawalsTable.sql.includes('CAST(amount_atomic AS INTEGER) > 0')) {
    db.transaction(() => {
      db.exec(`
        CREATE TABLE withdrawals_migrated (
          withdrawal_key TEXT PRIMARY KEY,
          caller_key TEXT NOT NULL,
          mosaic TEXT NOT NULL,
          mosaic_id TEXT NOT NULL,
          amount TEXT NOT NULL,
          amount_atomic TEXT NOT NULL CHECK(CAST(amount_atomic AS INTEGER) > 0 AND amount_atomic NOT LIKE '-%' AND amount_atomic != '0'),
          nonce TEXT NOT NULL,
          expiry INTEGER NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('requested', 'debited', 'broadcast', 'confirmed')),
          tx_hash TEXT,
          block_height INTEGER,
          requested_at INTEGER NOT NULL,
          debited_at INTEGER,
          broadcast_at INTEGER,
          confirmed_at INTEGER
        );
        INSERT INTO withdrawals_migrated SELECT * FROM withdrawals;
        DROP TABLE withdrawals;
        ALTER TABLE withdrawals_migrated RENAME TO withdrawals;
      `);
    })();
  }

  if (!customPath) {
    dbInstance = db;
  }
  return db;
}

// -----------------------------------------------------------------------------
// Deposit Operations
// -----------------------------------------------------------------------------

function getDeposit(txHash, customDb = null) {
  const db = customDb || getDb();
  const row = db.prepare('SELECT * FROM processed_deposits WHERE tx_hash = ?').get(txHash.toLowerCase());
  return row || null;
}

function getLedgerCredit(txHash, customDb = null) {
  const db = customDb || getDb();
  const row = db.prepare('SELECT * FROM ledger_credits WHERE tx_hash = ?').get(txHash.toLowerCase());
  return row || null;
}

function reserveDepositPending(txHash, callerKey, mosaicId, customDb = null) {
  const db = customDb || getDb();
  const normHash = txHash.toLowerCase();

  const reserveTx = db.transaction(() => {
    const existing = db.prepare('SELECT * FROM processed_deposits WHERE tx_hash = ?').get(normHash);
    if (existing) {
      if (existing.status === 'confirmed') {
        throw new Error(`Deposit tx ${normHash} has already been credited (duplicate deposit rejected)`);
      }
      if (existing.status === 'refunding') {
        throw new Error(`Deposit tx ${normHash} is currently being refunded (cannot be credited)`);
      }
      if (existing.status === 'refunded') {
        throw new Error(`Deposit tx ${normHash} has already been refunded to original sender (cannot be credited)`);
      }
      if (existing.status === 'pending') {
        // Different caller or altered mosaic trying to claim a pending deposit is strictly rejected
        if (existing.caller_key !== callerKey) {
          throw new Error(`Deposit tx ${normHash} is currently being verified (pending) and already reserved by caller ${existing.caller_key}`);
        }
        if (existing.mosaic_id && existing.mosaic_id !== mosaicId) {
          throw new Error(`Deposit tx ${normHash} is already reserved for mosaic ${existing.mosaic_id} (cannot change to ${mosaicId})`);
        }
      }
    }

    const now = Date.now();
    db.prepare(`
      INSERT INTO processed_deposits (tx_hash, caller_key, mosaic_id, status, timestamp, updated_at)
      VALUES (?, ?, ?, 'pending', ?, ?)
      ON CONFLICT(tx_hash) DO UPDATE SET
        status = 'pending',
        caller_key = excluded.caller_key,
        mosaic_id = excluded.mosaic_id,
        updated_at = excluded.updated_at
    `).run(normHash, callerKey, mosaicId, now, now);
  });

  reserveTx();
}

function confirmDeposit(depositData, customDb = null) {
  const db = customDb || getDb();
  const normHash = depositData.txHash.toLowerCase();
  const now = Date.now();

  const confirmTx = db.transaction(() => {
    const existing = db.prepare('SELECT * FROM processed_deposits WHERE tx_hash = ?').get(normHash);
    if (!existing) {
      throw new Error(`Cannot confirm unreserved deposit ${normHash}`);
    }
    if (existing.status !== 'pending') {
      throw new Error(`Deposit ${normHash} is in status '${existing.status}', expected 'pending'`);
    }

    const amtStr = (depositData.onChainAmountAtomic || depositData.amountAtomic || 0n).toString();
    const caller = depositData.callerKey || depositData.sender;

    if (existing.caller_key && existing.caller_key !== caller) {
      throw new Error(`Caller mismatch on deposit confirmation: reserved for ${existing.caller_key}, attempting to confirm for ${caller}`);
    }

    // 1. Update processed_deposits to confirmed checking status='pending' atomically
    const updateRes = db.prepare(`
      UPDATE processed_deposits SET
        status = 'confirmed',
        caller_key = ?,
        mosaic_id = ?,
        mosaic_hex = ?,
        amount = ?,
        height = ?,
        confirmation_depth = ?,
        updated_at = ?
      WHERE tx_hash = ? AND status = 'pending'
    `).run(
      caller,
      depositData.mosaicId,
      depositData.mosaicHex,
      amtStr,
      depositData.height,
      depositData.confirmationDepth,
      now,
      normHash
    );

    if (updateRes.changes !== 1) {
      throw new Error(`Failed to transition deposit ${normHash} to confirmed (conflict detected)`);
    }

    // 2. Insert into ledger_credits with unique constraint
    db.prepare(`
      INSERT INTO ledger_credits (tx_hash, caller_key, mosaic_id, amount, source, timestamp)
      VALUES (?, ?, ?, ?, 'VERIFIED_DEPOSIT', ?)
    `).run(
      normHash,
      caller,
      depositData.mosaicId,
      amtStr,
      now
    );
  });

  confirmTx();
}

function clearPendingDeposit(txHash, customDb = null) {
  const db = customDb || getDb();
  const normHash = txHash.toLowerCase();
  db.prepare("DELETE FROM processed_deposits WHERE tx_hash = ? AND status = 'pending'").run(normHash);
}

function markDepositRefunding(txHash, customDb = null) {
  const db = customDb || getDb();
  const normHash = txHash.toLowerCase();
  const now = Date.now();

  const refundingTx = db.transaction(() => {
    const existing = db.prepare('SELECT * FROM processed_deposits WHERE tx_hash = ?').get(normHash);
    if (existing && existing.status === 'confirmed') {
      throw new Error(`Deposit tx ${normHash} has already been credited to ledger; cannot be refunded`);
    }
    if (existing && existing.status === 'refunded') {
      throw new Error(`Deposit tx ${normHash} has already been refunded (Refund Tx: ${existing.refund_tx_hash})`);
    }
    if (existing && existing.status === 'refunding') {
      // Resuming refund after crash: update timestamp and allow broadcast to proceed
      db.prepare(`UPDATE processed_deposits SET updated_at = ? WHERE tx_hash = ?`).run(now, normHash);
      return;
    }

    if (existing && existing.status === 'pending') {
      const res = db.prepare(`
        UPDATE processed_deposits SET
          status = 'refunding',
          updated_at = ?
        WHERE tx_hash = ? AND status = 'pending'
      `).run(now, normHash);
      if (res.changes !== 1) {
        throw new Error(`Failed to transition deposit ${normHash} to refunding`);
      }
      return;
    }

    db.prepare(`
      INSERT INTO processed_deposits (tx_hash, caller_key, mosaic_id, status, timestamp, updated_at)
      VALUES (?, 'refund_vault', 'unknown', 'refunding', ?, ?)
    `).run(normHash, now, now);
  });

  refundingTx();
}

function markDepositRefunded(txHash, refundTxHash, recipientAddr, mosaicIdHex, amountAtomic, customDb = null) {
  const db = customDb || getDb();
  const normHash = txHash.toLowerCase();
  const now = Date.now();

  const refundedTx = db.transaction(() => {
    const existing = db.prepare('SELECT * FROM processed_deposits WHERE tx_hash = ?').get(normHash);
    if (!existing || existing.status !== 'refunding') {
      throw new Error(`Deposit ${normHash} must be in status 'refunding' prior to being marked refunded`);
    }

    const res = db.prepare(`
      UPDATE processed_deposits SET
        status = 'refunded',
        refund_tx_hash = ?,
        recipient_address = ?,
        mosaic_hex = ?,
        amount = ?,
        updated_at = ?
      WHERE tx_hash = ? AND status = 'refunding'
    `).run(
      refundTxHash,
      recipientAddr,
      mosaicIdHex,
      amountAtomic ? amountAtomic.toString() : '0',
      now,
      normHash
    );

    if (res.changes !== 1) {
      throw new Error(`Failed to transition deposit ${normHash} to refunded`);
    }
  });

  refundedTx();
}

// -----------------------------------------------------------------------------
// Withdrawal Operations
// -----------------------------------------------------------------------------

function getWithdrawal(key, customDb = null) {
  const db = customDb || getDb();
  const row = db.prepare('SELECT * FROM withdrawals WHERE withdrawal_key = ?').get(key);
  if (!row) return null;
  return {
    key: row.withdrawal_key,
    callerKey: row.caller_key,
    mosaic: row.mosaic,
    mosaicId: row.mosaic_id,
    amount: row.amount,
    amountAtomic: row.amount_atomic,
    nonce: row.nonce,
    expiry: row.expiry,
    status: row.status,
    txHash: row.tx_hash,
    blockHeight: row.block_height,
    requestedAt: row.requested_at,
    debitedAt: row.debited_at,
    broadcastAt: row.broadcast_at,
    confirmedAt: row.confirmed_at,
  };
}

function saveWithdrawalRecord(record, customDb = null) {
  const db = customDb || getDb();
  const key = record.key || `${record.callerKey}:${record.nonce}`;
  const statusRank = { requested: 1, debited: 2, broadcast: 3, confirmed: 4, failed: 0 };
  const saveTx = db.transaction(() => {
    const existing = db.prepare('SELECT status FROM withdrawals WHERE withdrawal_key = ?').get(key);
    if (existing && (statusRank[existing.status] || 0) > (statusRank[record.status] || 0) && record.status !== 'failed') {
      throw new Error(`Invalid backward withdrawal state transition for ${key}: ${existing.status} -> ${record.status}`);
    }
    db.prepare(`
      INSERT INTO withdrawals (
        withdrawal_key, caller_key, mosaic, mosaic_id, amount, amount_atomic,
        nonce, expiry, status, tx_hash, block_height,
        requested_at, debited_at, broadcast_at, confirmed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(withdrawal_key) DO UPDATE SET
        status = excluded.status,
        tx_hash = COALESCE(excluded.tx_hash, withdrawals.tx_hash),
        block_height = COALESCE(excluded.block_height, withdrawals.block_height),
        debited_at = COALESCE(excluded.debited_at, withdrawals.debited_at),
        broadcast_at = COALESCE(excluded.broadcast_at, withdrawals.broadcast_at),
        confirmed_at = COALESCE(excluded.confirmed_at, withdrawals.confirmed_at)
    `).run(
      key,
      record.callerKey,
      record.mosaic || 'XPX',
      record.mosaicId,
      record.amount,
      record.amountAtomic,
      record.nonce,
      record.expiry,
      record.status,
      record.txHash || null,
      record.blockHeight || null,
      record.requestedAt || Date.now(),
      record.debitedAt || null,
      record.broadcastAt || null,
      record.confirmedAt || null
    );
  });
  saveTx();
}

function getAllPendingWithdrawals(customDb = null) {
  const db = customDb || getDb();
  const rows = db.prepare("SELECT * FROM withdrawals WHERE status IN ('requested', 'debited', 'broadcast')").all();
  return rows.map((r) => ({
    key: r.withdrawal_key,
    callerKey: r.caller_key,
    mosaic: r.mosaic,
    mosaicId: r.mosaic_id,
    amount: r.amount,
    amountAtomic: r.amount_atomic,
    nonce: r.nonce,
    expiry: r.expiry,
    status: r.status,
    txHash: r.tx_hash,
    blockHeight: r.block_height,
    requestedAt: r.requested_at,
    debitedAt: r.debited_at,
    broadcastAt: r.broadcast_at,
    confirmedAt: r.confirmed_at,
  }));
}

// -----------------------------------------------------------------------------
// Account Balance Operations
// -----------------------------------------------------------------------------

function getDbAccountBalance(callerKey, customDb = null) {
  const db = customDb || getDb();
  const row = db.prepare('SELECT * FROM account_balances WHERE caller_key = ?').get(callerKey.toLowerCase());
  if (!row) {
    return {
      xpx: 0n,
      usdt: 0n,
      bnb: 0n,
      lockedXpx: 0n,
      lockedUsdt: 0n,
      lockedBnb: 0n,
    };
  }
  return {
    xpx: BigInt(row.xpx || '0'),
    usdt: BigInt(row.usdt || '0'),
    bnb: BigInt(row.bnb || '0'),
    lockedXpx: BigInt(row.locked_xpx || '0'),
    lockedUsdt: BigInt(row.locked_usdt || '0'),
    lockedBnb: BigInt(row.locked_bnb || '0'),
  };
}

function updateDbAccountBalance(callerKey, updaterFn, options = {}, customDb = null) {
  const db = customDb || getDb();
  const normKey = callerKey.toLowerCase();
  const now = Date.now();

  const updateTx = db.transaction(() => {
    const current = getDbAccountBalance(normKey, db);
    const prevTotal = current.xpx + current.usdt + current.bnb + current.lockedXpx + current.lockedUsdt + current.lockedBnb;

    const modified = { ...current };
    updaterFn(modified);

    const newTotal = BigInt(modified.xpx || 0n) + BigInt(modified.usdt || 0n) + BigInt(modified.bnb || 0n) +
                     BigInt(modified.lockedXpx || 0n) + BigInt(modified.lockedUsdt || 0n) + BigInt(modified.lockedBnb || 0n);

    // Provenance invariant: balance increases must have explicit authorized source
    if (newTotal > prevTotal) {
      const source = options.source;
      if (
        source !== 'VERIFIED_DEPOSIT' &&
        source !== 'MATCH_SETTLEMENT' &&
        source !== 'ESCROW_REFUND' &&
        source !== 'BRIDGE_DELIVERY' &&
        source !== 'FAUCET' &&
        source !== 'ON_CHAIN_SYNC'
      ) {
        throw new Error(
          'Unauthorized direct credit rejected: Total balance increases are strictly forbidden. All credits must route through verified on-chain deposits (depositTokens) or matching engine trade settlement.'
        );
      }
    }

    db.prepare(`
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
    `).run(
      normKey,
      (modified.xpx || 0n).toString(),
      (modified.usdt || 0n).toString(),
      (modified.bnb || 0n).toString(),
      (modified.lockedXpx || 0n).toString(),
      (modified.lockedUsdt || 0n).toString(),
      (modified.lockedBnb || 0n).toString(),
      now
    );
  });

  updateTx();
}

function closeDb() {
  if (dbInstance) {
    try {
      dbInstance.close();
    } catch (_) {}
    dbInstance = null;
  }
}

function saveBridgeSwapRecord(record, customDb = null) {
  const db = customDb || getDb();
  const stmt = db.prepare(`
    INSERT INTO bridge_swaps (
      txn_hash, swap_id, network, token, amount, sender_address,
      recipient_address, sirius_tx_hash, block_number, confirmations,
      status, auto_detected, timestamp, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(txn_hash) DO UPDATE SET
      status = excluded.status,
      confirmations = excluded.confirmations,
      sirius_tx_hash = COALESCE(excluded.sirius_tx_hash, bridge_swaps.sirius_tx_hash)
  `);
  stmt.run(
    (record.txnHash || '').toLowerCase(),
    record.swapId,
    record.network || 'BSC',
    record.token,
    String(record.amount),
    record.from || record.sender_address || null,
    record.recipient || record.recipient_address,
    record.siriusTxHash || null,
    record.blockNumber || null,
    record.confirmations || 0,
    record.status || 'CONFIRMED',
    record.autoDetected ? 1 : 0,
    record.timestamp || new Date().toISOString(),
    Date.now()
  );
}

function getBridgeSwapRecord(txnHash, customDb = null) {
  const db = customDb || getDb();
  return db.prepare('SELECT * FROM bridge_swaps WHERE txn_hash = ?').get((txnHash || '').toLowerCase());
}

function getBridgeRolling24hVolume(tokenSymbol, customDb = null) {
  const db = customDb || getDb();
  const since = Date.now() - 24 * 60 * 60 * 1000;
  const rows = db.prepare(`
    SELECT amount FROM bridge_swaps
    WHERE token = ? AND status IN ('CONFIRMED', 'DELIVERED') AND created_at >= ?
  `).all(tokenSymbol.toUpperCase(), since);
  let total = 0;
  for (const r of rows) {
    total += parseFloat(r.amount) || 0;
  }
  return total;
}

function recordCollectedFee(feeRecord, customDb = null) {
  const db = customDb || getDb();
  const amtAtomic = BigInt(feeRecord.amountAtomic || 0);
  if (amtAtomic <= 0n) return null;
  const stmt = db.prepare(`
    INSERT INTO collected_fees (
      fee_type, mosaic, mosaic_id, amount, amount_atomic, reference_id, caller_key, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const res = stmt.run(
    feeRecord.feeType || 'TRADE',
    (feeRecord.mosaic || 'XPX').toUpperCase(),
    String(feeRecord.mosaicId || ''),
    String(feeRecord.amount || '0'),
    amtAtomic.toString(),
    feeRecord.referenceId || null,
    feeRecord.callerKey || null,
    feeRecord.createdAt || Date.now()
  );
  return res.lastInsertRowid;
}

function getCollectedFeesSummary(customDb = null) {
  const db = customDb || getDb();
  let allRows = [];
  try {
    allRows = db.prepare('SELECT * FROM collected_fees ORDER BY id DESC').all();
  } catch (_) {
    return {
      total: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', xpxAtomic: '0', usdtAtomic: '0', bnbAtomic: '0', count: 0 },
      byType: {
        trade: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
        withdrawal: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
        bridge: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
      },
      recent: [],
    };
  }

  let totalXpxAtomic = 0n;
  let totalUsdtAtomic = 0n;
  let totalBnbAtomic = 0n;

  const byType = {
    TRADE: { xpx: 0n, usdt: 0n, bnb: 0n, count: 0 },
    WITHDRAWAL: { xpx: 0n, usdt: 0n, bnb: 0n, count: 0 },
    BRIDGE: { xpx: 0n, usdt: 0n, bnb: 0n, count: 0 },
  };

  for (const r of allRows) {
    const amt = BigInt(r.amount_atomic || '0');
    const mos = (r.mosaic || 'XPX').toUpperCase();
    const type = r.fee_type.startsWith('TRADE') ? 'TRADE' : (r.fee_type === 'WITHDRAWAL' ? 'WITHDRAWAL' : 'BRIDGE');

    if (byType[type]) {
      byType[type].count++;
      if (mos === 'XPX') byType[type].xpx += amt;
      else if (mos === 'USDT') byType[type].usdt += amt;
      else if (mos === 'BNB') byType[type].bnb += amt;
    }

    if (mos === 'XPX') totalXpxAtomic += amt;
    else if (mos === 'USDT') totalUsdtAtomic += amt;
    else if (mos === 'BNB') totalBnbAtomic += amt;
  }

  const recent = allRows.slice(0, 30).map((r) => ({
    id: r.id,
    type: r.fee_type,
    mosaic: r.mosaic,
    amount: r.amount,
    amountAtomic: r.amount_atomic,
    referenceId: r.reference_id,
    callerKey: r.caller_key ? (r.caller_key.slice(0, 8) + '...' + r.caller_key.slice(-6)) : null,
    timestamp: r.created_at,
    time: new Date(r.created_at).toLocaleTimeString(),
  }));

  return {
    total: {
      xpx: (Number(totalXpxAtomic) / 1e6).toFixed(4),
      usdt: (Number(totalUsdtAtomic) / 1e6).toFixed(4),
      bnb: (Number(totalBnbAtomic) / 1e6).toFixed(6),
      xpxAtomic: totalXpxAtomic.toString(),
      usdtAtomic: totalUsdtAtomic.toString(),
      bnbAtomic: totalBnbAtomic.toString(),
      count: allRows.length,
    },
    byType: {
      trade: {
        xpx: (Number(byType.TRADE.xpx) / 1e6).toFixed(4),
        usdt: (Number(byType.TRADE.usdt) / 1e6).toFixed(4),
        bnb: (Number(byType.TRADE.bnb) / 1e6).toFixed(6),
        count: byType.TRADE.count,
      },
      withdrawal: {
        xpx: (Number(byType.WITHDRAWAL.xpx) / 1e6).toFixed(4),
        usdt: (Number(byType.WITHDRAWAL.usdt) / 1e6).toFixed(4),
        bnb: (Number(byType.WITHDRAWAL.bnb) / 1e6).toFixed(6),
        count: byType.WITHDRAWAL.count,
      },
      bridge: {
        xpx: (Number(byType.BRIDGE.xpx) / 1e6).toFixed(4),
        usdt: (Number(byType.BRIDGE.usdt) / 1e6).toFixed(4),
        bnb: (Number(byType.BRIDGE.bnb) / 1e6).toFixed(6),
        count: byType.BRIDGE.count,
      },
    },
    recent,
  };
}

function recordFaucetClaim(recipient, amountXpx, timestamp) {
  const db = getDb();
  const dateStr = new Date(timestamp).toDateString();
  const stmt = db.prepare(`
    INSERT INTO faucet_claims (recipient, last_claim_time, amount_xpx, claim_date)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(recipient) DO UPDATE SET
      last_claim_time = excluded.last_claim_time,
      amount_xpx = excluded.amount_xpx,
      claim_date = excluded.claim_date
  `);
  stmt.run(recipient, timestamp, amountXpx, dateStr);
}

function getLastFaucetClaim(recipient) {
  const db = getDb();
  const row = db.prepare('SELECT last_claim_time FROM faucet_claims WHERE recipient = ?').get(recipient);
  return row ? row.last_claim_time : 0;
}

function getDailyFaucetDisbursed(dateStr) {
  const db = getDb();
  const row = db.prepare('SELECT SUM(amount_xpx) as total FROM faucet_claims WHERE claim_date = ?').get(dateStr);
  return (row && row.total) ? Number(row.total) : 0;
}

module.exports = {
  DB_FILE,
  getDb,
  closeDb,
  getDeposit,
  getLedgerCredit,
  reserveDepositPending,
  confirmDeposit,
  clearPendingDeposit,
  markDepositRefunding,
  markDepositRefunded,
  getWithdrawal,
  saveWithdrawalRecord,
  getAllPendingWithdrawals,
  getDbAccountBalance,
  updateDbAccountBalance,
  saveBridgeSwapRecord,
  getBridgeSwapRecord,
  getBridgeRolling24hVolume,
  recordCollectedFee,
  getCollectedFeesSummary,
  recordFaucetClaim,
  getLastFaucetClaim,
  getDailyFaucetDisbursed,
};
