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
const DB_FILE = process.env.CLOB_DB_FILE || path.join(CLUSTER_DIR, 'clob_state.db');

let dbInstance = null;

function getDb(customPath = null) {
  if (dbInstance && !customPath) return dbInstance;
  const targetPath = customPath || process.env.CLOB_DB_FILE || DB_FILE;
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });

  const db = new Database(targetPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('foreign_keys = ON');
  db.pragma('busy_timeout = 5000');

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
      has_traded INTEGER NOT NULL DEFAULT 0,
      last_on_chain_xpx TEXT NOT NULL DEFAULT '0',
      last_on_chain_usdt TEXT NOT NULL DEFAULT '0',
      last_on_chain_bnb TEXT NOT NULL DEFAULT '0',
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

    CREATE TABLE IF NOT EXISTS trading_accounts (
      trading_public_key TEXT PRIMARY KEY,
      trading_address TEXT UNIQUE NOT NULL,
      owner_public_key TEXT NOT NULL,
      owner_address TEXT NOT NULL,
      validator_cluster_public_key TEXT NOT NULL,
      validator_cluster_address TEXT NOT NULL,
      min_approval INTEGER NOT NULL DEFAULT 2,
      activation_tx_hash TEXT,
      status TEXT NOT NULL CHECK(status IN ('pending', 'active', 'revoked')),
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trading_accounts_owner_pk ON trading_accounts(owner_public_key);
    CREATE INDEX IF NOT EXISTS idx_trading_accounts_owner_addr ON trading_accounts(owner_address);

    CREATE TABLE IF NOT EXISTS trading_withdrawals (
      withdrawal_id TEXT PRIMARY KEY,
      owner_public_key TEXT NOT NULL,
      trading_public_key TEXT NOT NULL,
      mosaic TEXT NOT NULL,
      mosaic_hex TEXT NOT NULL,
      amount_atomic TEXT NOT NULL,
      recipient_address TEXT NOT NULL,
      tx_hash TEXT,
      status TEXT NOT NULL CHECK(status IN ('pending', 'broadcast', 'confirmed', 'failed')),
      block_height INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trading_withdrawals_owner ON trading_withdrawals(owner_public_key, status);

    CREATE TABLE IF NOT EXISTS trading_settlements (
      settlement_id TEXT PRIMARY KEY,
      order_id TEXT,
      owner_public_key TEXT NOT NULL,
      trading_public_key TEXT NOT NULL,
      counterparty_key TEXT,
      tx_hash TEXT,
      aggregate_payload TEXT NOT NULL,
      required_cosigners_json TEXT NOT NULL,
      collected_cosigs_json TEXT NOT NULL,
      transfers_json TEXT NOT NULL,
      delta_json TEXT NOT NULL,
      order_context_json TEXT,
      status TEXT NOT NULL CHECK(status IN ('pending_cosign', 'ready', 'broadcast', 'confirmed', 'failed', 'expired')),
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trading_settlements_owner ON trading_settlements(owner_public_key, status);
  `);

  try {
    const settlementCols = db.prepare("PRAGMA table_info(trading_settlements)").all().map((c) => c.name);
    if (!settlementCols.includes('order_context_json')) {
      db.exec('ALTER TABLE trading_settlements ADD COLUMN order_context_json TEXT;');
    }
  } catch (_) {}

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

  // Automated schema migration for trade persistence & on-chain delta tracking
  try {
    const balanceCols = db.prepare("PRAGMA table_info(account_balances)").all().map(c => c.name);
    if (!balanceCols.includes('has_traded')) {
      db.exec(`
        ALTER TABLE account_balances ADD COLUMN has_traded INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE account_balances ADD COLUMN last_on_chain_xpx TEXT NOT NULL DEFAULT '0';
        ALTER TABLE account_balances ADD COLUMN last_on_chain_usdt TEXT NOT NULL DEFAULT '0';
        ALTER TABLE account_balances ADD COLUMN last_on_chain_bnb TEXT NOT NULL DEFAULT '0';
      `);
    }
  } catch (_) {}

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
      hasTraded: false,
      lastOnChainXpx: 0n,
      lastOnChainUsdt: 0n,
      lastOnChainBnb: 0n,
    };
  }
  return {
    xpx: BigInt(row.xpx || '0'),
    usdt: BigInt(row.usdt || '0'),
    bnb: BigInt(row.bnb || '0'),
    lockedXpx: BigInt(row.locked_xpx || '0'),
    lockedUsdt: BigInt(row.locked_usdt || '0'),
    lockedBnb: BigInt(row.locked_bnb || '0'),
    hasTraded: Boolean(row.has_traded),
    lastOnChainXpx: BigInt(row.last_on_chain_xpx || '0'),
    lastOnChainUsdt: BigInt(row.last_on_chain_usdt || '0'),
    lastOnChainBnb: BigInt(row.last_on_chain_bnb || '0'),
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
      INSERT INTO account_balances (
        caller_key, xpx, usdt, bnb, locked_xpx, locked_usdt, locked_bnb,
        has_traded, last_on_chain_xpx, last_on_chain_usdt, last_on_chain_bnb, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(caller_key) DO UPDATE SET
        xpx = excluded.xpx,
        usdt = excluded.usdt,
        bnb = excluded.bnb,
        locked_xpx = excluded.locked_xpx,
        locked_usdt = excluded.locked_usdt,
        locked_bnb = excluded.locked_bnb,
        has_traded = excluded.has_traded,
        last_on_chain_xpx = excluded.last_on_chain_xpx,
        last_on_chain_usdt = excluded.last_on_chain_usdt,
        last_on_chain_bnb = excluded.last_on_chain_bnb,
        updated_at = excluded.updated_at
    `).run(
      normKey,
      (modified.xpx || 0n).toString(),
      (modified.usdt || 0n).toString(),
      (modified.bnb || 0n).toString(),
      (modified.lockedXpx || 0n).toString(),
      (modified.lockedUsdt || 0n).toString(),
      (modified.lockedBnb || 0n).toString(),
      modified.hasTraded ? 1 : 0,
      (modified.lastOnChainXpx || 0n).toString(),
      (modified.lastOnChainUsdt || 0n).toString(),
      (modified.lastOnChainBnb || 0n).toString(),
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
  const numAmt = Number(record.amount);
  if (!Number.isFinite(numAmt) || numAmt <= 0) {
    throw new Error(`Invalid bridge swap amount: ${record.amount}`);
  }
  const normHash = (record.txnHash || '').toLowerCase();
  const statusRank = { PENDING: 1, CONFIRMED: 2, DELIVERED: 3, FAUCET_DELIVERED: 3 };
  const nextStatus = String(record.status || 'CONFIRMED').toUpperCase();

  const saveTx = db.transaction(() => {
    const existing = db.prepare('SELECT status FROM bridge_swaps WHERE txn_hash = ?').get(normHash);
    if (existing) {
      const prevRank = statusRank[String(existing.status || '').toUpperCase()] || 0;
      const nextRank = statusRank[nextStatus] || 0;
      if (prevRank > nextRank) {
        throw new Error(`Invalid backward bridge swap status transition for ${normHash}: ${existing.status} -> ${nextStatus}`);
      }
    }
    const stmt = db.prepare(`
      INSERT INTO bridge_swaps (
        txn_hash, swap_id, network, token, amount, sender_address,
        recipient_address, sirius_tx_hash, block_number, confirmations,
        status, auto_detected, timestamp, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(txn_hash) DO UPDATE SET
        status = excluded.status,
        confirmations = MAX(bridge_swaps.confirmations, excluded.confirmations),
        sirius_tx_hash = COALESCE(excluded.sirius_tx_hash, bridge_swaps.sirius_tx_hash)
    `);
    stmt.run(
      normHash,
      record.swapId,
      record.network || 'BSC',
      record.token,
      String(record.amount),
      record.from || record.sender_address || null,
      record.recipient || record.recipient_address,
      record.siriusTxHash || null,
      record.blockNumber || null,
      record.confirmations || 0,
      nextStatus,
      record.autoDetected ? 1 : 0,
      record.timestamp || new Date().toISOString(),
      Date.now()
    );
  });
  saveTx();
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

// === Smart Trading Account (Solution 2 Model A) Persistence Helpers ===

function saveTradingAccount(rec) {
  const db = getDb();
  const now = Date.now();
  db.prepare(`
    INSERT INTO trading_accounts (
      trading_public_key, trading_address, owner_public_key, owner_address,
      validator_cluster_public_key, validator_cluster_address, min_approval,
      activation_tx_hash, status, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(trading_public_key) DO UPDATE SET
      validator_cluster_public_key = excluded.validator_cluster_public_key,
      validator_cluster_address = excluded.validator_cluster_address,
      min_approval = excluded.min_approval,
      activation_tx_hash = COALESCE(excluded.activation_tx_hash, trading_accounts.activation_tx_hash),
      status = excluded.status,
      updated_at = excluded.updated_at
  `).run(
    String(rec.tradingPublicKey).toLowerCase(),
    String(rec.tradingAddress).replace(/[- ]/g, '').toUpperCase(),
    String(rec.ownerPublicKey).toLowerCase(),
    String(rec.ownerAddress).replace(/[- ]/g, '').toUpperCase(),
    String(rec.validatorClusterPublicKey).toLowerCase(),
    String(rec.validatorClusterAddress).replace(/[- ]/g, '').toUpperCase(),
    Number(rec.minApproval || 2),
    rec.activationTxHash || null,
    rec.status || 'pending',
    rec.createdAt || now,
    now
  );
  return getTradingAccountByTradingKey(rec.tradingPublicKey);
}

function getTradingAccountByTradingKey(tradingKeyOrAddr) {
  if (!tradingKeyOrAddr) return null;
  const db = getDb();
  const norm = String(tradingKeyOrAddr).replace(/[- ]/g, '').trim();
  if (norm.length === 64) {
    return db.prepare('SELECT * FROM trading_accounts WHERE trading_public_key = ?').get(norm.toLowerCase()) || null;
  }
  return db.prepare('SELECT * FROM trading_accounts WHERE trading_address = ?').get(norm.toUpperCase()) || null;
}

function getTradingAccountByOwner(ownerKeyOrAddr) {
  if (!ownerKeyOrAddr) return null;
  const db = getDb();
  const norm = String(ownerKeyOrAddr).replace(/[- ]/g, '').trim();
  if (norm.length === 64) {
    return db.prepare(
      "SELECT * FROM trading_accounts WHERE owner_public_key = ? AND status IN ('active', 'pending') ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, updated_at DESC LIMIT 1"
    ).get(norm.toLowerCase()) || null;
  }
  return db.prepare(
    "SELECT * FROM trading_accounts WHERE owner_address = ? AND status IN ('active', 'pending') ORDER BY CASE WHEN status='active' THEN 0 ELSE 1 END, updated_at DESC LIMIT 1"
  ).get(norm.toUpperCase()) || null;
}

function getTradingAccountsByStatus(status) {
  const db = getDb();
  return db.prepare('SELECT * FROM trading_accounts WHERE status = ?').all(status);
}

function updateTradingAccountStatus(tradingPublicKey, status, activationTxHash = null) {
  const db = getDb();
  const normPk = String(tradingPublicKey).toLowerCase();
  if (status === 'pending') {
    // Never downgrade an already active trading account back to pending
    db.prepare(`
      UPDATE trading_accounts
      SET status = ?, activation_tx_hash = COALESCE(?, activation_tx_hash), updated_at = ?
      WHERE trading_public_key = ? AND status != 'active'
    `).run(status, activationTxHash, Date.now(), normPk);
    return;
  }
  db.prepare(`
    UPDATE trading_accounts
    SET status = ?, activation_tx_hash = COALESCE(?, activation_tx_hash), updated_at = ?
    WHERE trading_public_key = ?
  `).run(status, activationTxHash, Date.now(), normPk);
}

function saveTradingWithdrawal(rec) {
  const db = getDb();
  const now = Date.now();
  db.prepare(`
    INSERT INTO trading_withdrawals (
      withdrawal_id, owner_public_key, trading_public_key, mosaic, mosaic_hex,
      amount_atomic, recipient_address, tx_hash, status, block_height, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(withdrawal_id) DO UPDATE SET
      tx_hash = COALESCE(excluded.tx_hash, trading_withdrawals.tx_hash),
      status = excluded.status,
      block_height = COALESCE(excluded.block_height, trading_withdrawals.block_height),
      updated_at = excluded.updated_at
  `).run(
    rec.withdrawalId,
    String(rec.ownerPublicKey).toLowerCase(),
    String(rec.tradingPublicKey).toLowerCase(),
    rec.mosaic,
    String(rec.mosaicHex).toUpperCase(),
    String(rec.amountAtomic),
    String(rec.recipientAddress).replace(/[- ]/g, '').toUpperCase(),
    rec.txHash || null,
    rec.status || 'pending',
    rec.blockHeight || null,
    rec.createdAt || now,
    now
  );
}

function getPendingTradingWithdrawals(ownerPublicKey) {
  const db = getDb();
  return db.prepare(
    "SELECT * FROM trading_withdrawals WHERE owner_public_key = ? AND status IN ('pending', 'broadcast')"
  ).all(String(ownerPublicKey).toLowerCase());
}

function getAllInFlightTradingWithdrawals() {
  const db = getDb();
  return db.prepare("SELECT * FROM trading_withdrawals WHERE status IN ('pending', 'broadcast')").all();
}

function updateTradingWithdrawalStatus(withdrawalId, status, txHash = null, blockHeight = null) {
  const db = getDb();
  // Forward-only finality guard: never revert a confirmed withdrawal
  db.prepare(`
    UPDATE trading_withdrawals
    SET status = ?, tx_hash = COALESCE(?, tx_hash), block_height = COALESCE(?, block_height), updated_at = ?
    WHERE withdrawal_id = ? AND status != 'confirmed'
  `).run(status, txHash, blockHeight, Date.now(), withdrawalId);
}

function saveTradingSettlement(rec) {
  const db = getDb();
  const now = Date.now();
  db.prepare(`
    INSERT INTO trading_settlements (
      settlement_id, order_id, owner_public_key, trading_public_key, counterparty_key,
      tx_hash, aggregate_payload, required_cosigners_json, collected_cosigs_json,
      transfers_json, delta_json, order_context_json, status, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(settlement_id) DO UPDATE SET
      tx_hash = COALESCE(excluded.tx_hash, trading_settlements.tx_hash),
      aggregate_payload = excluded.aggregate_payload,
      collected_cosigs_json = excluded.collected_cosigs_json,
      order_context_json = COALESCE(excluded.order_context_json, trading_settlements.order_context_json),
      status = excluded.status,
      updated_at = excluded.updated_at
  `).run(
    rec.settlementId,
    rec.orderId ? String(rec.orderId) : null,
    String(rec.ownerPublicKey).toLowerCase(),
    String(rec.tradingPublicKey).toLowerCase(),
    rec.counterpartyKey ? String(rec.counterpartyKey).toLowerCase() : null,
    rec.txHash || null,
    rec.aggregatePayload,
    JSON.stringify(rec.requiredCosigners || []),
    JSON.stringify(rec.collectedCosigs || []),
    JSON.stringify(rec.transfers || []),
    JSON.stringify(rec.delta || {}),
    rec.orderContext ? JSON.stringify(rec.orderContext) : null,
    rec.status || 'pending_cosign',
    rec.createdAt || now,
    now
  );
  return getTradingSettlement(rec.settlementId);
}

function getTradingSettlement(settlementId) {
  const db = getDb();
  const row = db.prepare('SELECT * FROM trading_settlements WHERE settlement_id = ?').get(settlementId);
  return row ? hydrateSettlementRow(row) : null;
}

function hydrateSettlementRow(row) {
  let orderContext = null;
  try {
    orderContext = row.order_context_json ? JSON.parse(row.order_context_json) : null;
  } catch (_) {}
  return {
    settlementId: row.settlement_id,
    orderId: row.order_id,
    ownerPublicKey: row.owner_public_key,
    tradingPublicKey: row.trading_public_key,
    counterpartyKey: row.counterparty_key,
    txHash: row.tx_hash,
    aggregatePayload: row.aggregate_payload,
    requiredCosigners: JSON.parse(row.required_cosigners_json || '[]'),
    collectedCosigs: JSON.parse(row.collected_cosigs_json || '[]'),
    transfers: JSON.parse(row.transfers_json || '[]'),
    delta: JSON.parse(row.delta_json || '{}'),
    orderContext,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function getPendingSettlementsForOwner(ownerPublicKey) {
  const db = getDb();
  const rows = db.prepare(
    "SELECT * FROM trading_settlements WHERE owner_public_key = ? AND status IN ('pending_cosign', 'ready', 'broadcast') ORDER BY created_at ASC"
  ).all(String(ownerPublicKey).toLowerCase());
  return rows.map(hydrateSettlementRow);
}

function getPendingSettlementsForCosigner(publicKey) {
  const db = getDb();
  const pkLower = String(publicKey || '').trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(pkLower)) return [];
  const rows = db.prepare(
    `SELECT * FROM trading_settlements
     WHERE status IN ('pending_cosign', 'ready', 'broadcast')
       AND (owner_public_key = ? OR counterparty_key = ? OR LOWER(required_cosigners_json) LIKE ?)
     ORDER BY created_at ASC`
  ).all(pkLower, pkLower, `%${pkLower}%`);
  return rows
    .map(hydrateSettlementRow)
    .filter((s) => (
      s.ownerPublicKey === pkLower ||
      s.counterpartyKey === pkLower ||
      (Array.isArray(s.requiredCosigners) && s.requiredCosigners.some((r) => String(r || '').toLowerCase() === pkLower))
    ));
}

function getAllInFlightTradingSettlements() {
  const db = getDb();
  const rows = db.prepare("SELECT * FROM trading_settlements WHERE status IN ('ready', 'broadcast')").all();
  return rows.map(hydrateSettlementRow);
}

function getAllPendingCosignSettlements() {
  const db = getDb();
  const rows = db.prepare("SELECT * FROM trading_settlements WHERE status = 'pending_cosign' ORDER BY created_at ASC").all();
  return rows.map(hydrateSettlementRow);
}

/**
 * Atomically expire a stuck pending_cosign settlement.
 * Guarded by `WHERE status = 'pending_cosign'` so broadcast/confirmed settlements can NEVER be expired.
 */
function expirePendingCosignSettlement(settlementId) {
  const db = getDb();
  const now = Date.now();
  try {
    const res = db.prepare(`
      UPDATE trading_settlements
      SET status = 'expired', collected_cosigs_json = '[]', updated_at = ?
      WHERE settlement_id = ? AND status = 'pending_cosign'
    `).run(now, settlementId);
    return res.changes === 1;
  } catch (err) {
    if (String(err && err.message || '').includes('CHECK constraint failed')) {
      // Legacy pre-existing DB whose CHECK constraint predates 'expired': fall back safely to terminal 'failed'
      const res = db.prepare(`
        UPDATE trading_settlements
        SET status = 'failed', collected_cosigs_json = '[]', updated_at = ?
        WHERE settlement_id = ? AND status = 'pending_cosign'
      `).run(now, settlementId);
      return res.changes === 1;
    }
    throw err;
  }
}

function updateTradingSettlementStatus(settlementId, status, txHash = null, collectedCosigs = null) {
  const db = getDb();
  // Forward-only finality guard: never overwrite an already confirmed settlement
  if (collectedCosigs !== null) {
    db.prepare(`
      UPDATE trading_settlements
      SET status = ?, tx_hash = COALESCE(?, tx_hash), collected_cosigs_json = ?, updated_at = ?
      WHERE settlement_id = ? AND status != 'confirmed'
    `).run(status, txHash, JSON.stringify(collectedCosigs), Date.now(), settlementId);
  } else {
    db.prepare(`
      UPDATE trading_settlements
      SET status = ?, tx_hash = COALESCE(?, tx_hash), updated_at = ?
      WHERE settlement_id = ? AND status != 'confirmed'
    `).run(status, txHash, Date.now(), settlementId);
  }
}

function updateTradingSettlementPayload(settlementId, { txHash, aggregatePayload, collectedCosigs = [], status = 'pending_cosign' }) {
  const db = getDb();
  // Forward-only finality guard: never rebuild/overwrite a confirmed settlement
  db.prepare(`
    UPDATE trading_settlements
    SET tx_hash = ?, aggregate_payload = ?, collected_cosigs_json = ?, status = ?, updated_at = ?
    WHERE settlement_id = ? AND status != 'confirmed'
  `).run(txHash, aggregatePayload, JSON.stringify(collectedCosigs), status, Date.now(), settlementId);
  return getTradingSettlement(settlementId);
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
  saveTradingAccount,
  getTradingAccountByTradingKey,
  getTradingAccountByOwner,
  getTradingAccountsByStatus,
  updateTradingAccountStatus,
  saveTradingWithdrawal,
  getPendingTradingWithdrawals,
  getAllInFlightTradingWithdrawals,
  updateTradingWithdrawalStatus,
  saveTradingSettlement,
  getTradingSettlement,
  getPendingSettlementsForOwner,
  getPendingSettlementsForCosigner,
  getAllInFlightTradingSettlements,
  getAllPendingCosignSettlements,
  expirePendingCosignSettlement,
  updateTradingSettlementStatus,
  updateTradingSettlementPayload,
};
