/**
 * Sirius Supercontract & DBRB Sandbox Cockpit Dashboard
 * Zero-dependency Node.js HTTP server with real-time log streaming & interactive cluster control.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const net = require('net');
module.paths.push(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules'));
const { exec, execFile, spawn } = require('child_process');
const clobApi = require('./clob_api');
const clobDb = require('./clob_db');
const swapApi = require('./swap_api');
const tradingCosigner = require('./trading_account_cosigner');
const { defaultBatchSettlerTestnet2: defaultBatchSettler } = require('./batch_settler_testnet2');
const l1Config = require('./testnet2_config');
const testnet2Config = l1Config;
const l1Client = require('./testnet2_client');

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 8088;
const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const ADMIN_SECRET_FILE = path.join(CLUSTER_DIR, 'admin_secret.key');
let clusterAdminKey = process.env.CLOB_ADMIN_KEY || '';

function getOrInitAdminKey() {
  if (clusterAdminKey && clusterAdminKey.trim()) {
    return clusterAdminKey.trim();
  }
  try {
    if (fs.existsSync(ADMIN_SECRET_FILE)) {
      const existing = fs.readFileSync(ADMIN_SECRET_FILE, 'utf8').trim();
      if (existing.length >= 32) {
        clusterAdminKey = existing;
        return clusterAdminKey;
      }
    }
    const generated = crypto.randomBytes(32).toString('hex');
    fs.mkdirSync(path.dirname(ADMIN_SECRET_FILE), { recursive: true });
    fs.writeFileSync(ADMIN_SECRET_FILE, generated, { mode: 0o600 });
    clusterAdminKey = generated;
    console.log('[SECURITY] Generated new persistent cluster admin key in cluster/admin_secret.key');
    return clusterAdminKey;
  } catch (err) {
    console.error('[SECURITY] Error initializing admin key:', err.message);
    return clusterAdminKey;
  }
}

// Log admin key status at startup (without leaking full key if configured via env)
const initialAdminKey = getOrInitAdminKey();
if (process.env.CLOB_ADMIN_KEY) {
  console.log('[SECURITY] CLOB Admin Key initialized from environment variable CLOB_ADMIN_KEY');
} else {
  console.log(`[SECURITY] CLOB Admin Key initialized: ${initialAdminKey.slice(0, 8)}...${initialAdminKey.slice(-6)} (Saved to cluster/admin_secret.key)`);
}

function verifyAdminAuth(req, body = null, expectedAction = null) {
  const secretKey = getOrInitAdminKey();

  // 1. Check Bearer Token or X-Admin-Key header or body.adminKey
  const authHeader = req.headers['authorization'] || '';
  const bearerToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : '';
  const xAdminKey = (req.headers['x-admin-key'] || '').trim();
  const candidateSecret = bearerToken || xAdminKey || (body && body.adminKey ? String(body.adminKey).trim() : '');

  if (secretKey && candidateSecret) {
    try {
      const bufExpected = Buffer.from(secretKey);
      const bufCandidate = Buffer.from(candidateSecret);
      if (bufExpected.length === bufCandidate.length && crypto.timingSafeEqual(bufExpected, bufCandidate)) {
        return { authorized: true, method: 'ADMIN_SECRET' };
      }
    } catch (_) {}
  }

  // 2. Check Catapult Ed25519 Cryptographic Signature
  if (body && (body.signature || body.callerPublicKey || body.publicKey)) {
    try {
      const action = expectedAction || body.action;
      const verifyRes = clobApi.verifyAdminRequest(body, action);
      return { authorized: true, method: 'ED25519_SIGNATURE', callerKey: verifyRes.callerKey };
    } catch (sigErr) {
      return { authorized: false, error: sigErr.message };
    }
  }

  return {
    authorized: false,
    error: 'Authentication required: Valid X-Admin-Key header or signed Ed25519 administrative payload required'
  };
}
const TRADE_TIMESTAMPS_FILE = path.join(CLUSTER_DIR, 'trade_timestamps.json');
const tradeTimestamps = new Map();

try {
  if (fs.existsSync(TRADE_TIMESTAMPS_FILE)) {
    const raw = JSON.parse(fs.readFileSync(TRADE_TIMESTAMPS_FILE, 'utf8'));
    for (const [k, v] of Object.entries(raw)) {
      tradeTimestamps.set(k, v);
    }
  }
} catch (_) {}

let timestampsDirty = false;
function saveTradeTimestampsDebounced() {
  if (timestampsDirty) return;
  timestampsDirty = true;
  setTimeout(() => {
    try {
      const obj = {};
      for (const [k, v] of tradeTimestamps.entries()) {
        obj[k] = v;
      }
      fs.writeFileSync(TRADE_TIMESTAMPS_FILE, JSON.stringify(obj, null, 2));
    } catch (_) {}
    timestampsDirty = false;
  }, 2000);
}

function enrichTradeWithTimestamp(t, idx, totalLen, market = 'xpx_usdt') {
  const fingerprint = `${market}_${t.maker_order_id}_${t.price}_${t.base_amount}_${t.quote_amount}_${idx}`;
  if (!tradeTimestamps.has(fingerprint)) {
    if (tradeTimestamps.size > 10000) {
      const oldestKey = tradeTimestamps.keys().next().value;
      if (oldestKey) tradeTimestamps.delete(oldestKey);
    }
    const now = Date.now();
    // Stagger historical trades by 30 seconds so they show distinct, realistic execution times
    const historicalOffset = Math.max(0, (totalLen - 1 - idx)) * 30000;
    tradeTimestamps.set(fingerprint, now - historicalOffset);
    saveTradeTimestampsDebounced();
  }
  const ts = tradeTimestamps.get(fingerprint);
  const date = new Date(ts);
  return {
    timestamp: ts,
    time: date.toTimeString().split(' ')[0]
  };
}

// Resilient in-memory caches to guarantee order books & market trades are NEVER empty
const lastValidDepth = {};
const lastValidTrades = {};

const { getPancakeSwapPriceCached, getCachedPancakeSwapPriceSync } = require('./pancakeswap_oracle');

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 2 * 1024 * 1024) {
        req.destroy();
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(new Error('Malformed JSON body: ' + err.message));
      }
    });
    req.on('error', reject);
  });
}

function getClusterStatus() {
  const { execSync } = require('child_process');
  const nodes = ['node1', 'node2', 'node3'];
  const vmPorts = [5051, 5052, 5053];

  return nodes.map((node, i) => {
    const vmPidFile = path.join(CLUSTER_DIR, `${node}_vm.pid`);
    let vmRunning = false;
    let vmPid = null;

    if (fs.existsSync(vmPidFile)) {
      try {
        vmPid = parseInt(fs.readFileSync(vmPidFile, 'utf8').trim(), 10);
        process.kill(vmPid, 0);
        vmRunning = true;
      } catch (e) {
        vmRunning = false;
        vmPid = null;
      }
    }

    if (!vmRunning) {
      try {
        const out = execSync(`lsof -ti :${vmPorts[i]}`, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
        if (out) {
          vmPid = parseInt(out.split('\n')[0], 10);
          vmRunning = true;
        }
      } catch (_) {}
    }

    let vaultAddr = 'VAAUXFYQ424I6V3JDVWCBWGWHRTIJGTSX4B2TLSV';
    try {
      vaultAddr = l1Client.getOrInitVaultAccount().address.plain();
    } catch (_) {}

    return {
      name: node,
      running: vmRunning,
      pid: vmRunning ? vmPid : null,
      vmRunning,
      vmPid: vmRunning ? vmPid : null,
      vmPort: vmPorts[i],
      l1Network: l1Config.NETWORK_ENV === 'mainnet' ? 'Sirius Mainnet' : 'Sirius Testnet 2',
      l1Endpoint: l1Config.NODE_URL,
      vaultAddress: vaultAddr
    };
  });
}

let cachedOverviewData = null;
let lastOverviewFetchTime = 0;
const OVERVIEW_CACHE_TTL_MS = 2500;

async function getAdminOverviewData() {
  const now = Date.now();
  if (cachedOverviewData && (now - lastOverviewFetchTime < OVERVIEW_CACHE_TTL_MS)) {
    return cachedOverviewData;
  }

  const vaultAcc = l1Client.getOrInitVaultAccount();
  const feeAcc = l1Client.getOrInitFeeCollectorAccount();
  const treasuryAcc = l1Client.getOrInitTreasuryAccount();
  const scAddress = l1Config.SUPERCONTRACT_ADDRESS || 'VAEKLE3WB6XJLFW75JGJJ7Y3DVQWCWX774DDSJ2F';

  let scBal = new Map(), vBal = new Map(), fBal = new Map(), tBal = new Map(), height = 0;
  try {
    [scBal, vBal, fBal, tBal, height] = await Promise.all([
      l1Client.getAccountOnChainBalances(scAddress),
      l1Client.getAccountOnChainBalances(vaultAcc.address.plain()),
      l1Client.getAccountOnChainBalances(feeAcc.address.plain()),
      l1Client.getAccountOnChainBalances(treasuryAcc.address.plain()),
      l1Client.getLatestHeight()
    ]);
  } catch (err) {
    console.warn('[Admin Overview] Balance fetch warning:', err.message);
  }

  clobApi.loadCircuitBreakerState();
  const rolling = clobApi.getRolling24hWithdrawalVolume();

  let feesSummary = {
    total: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', xpxAtomic: '0', usdtAtomic: '0', bnbAtomic: '0', count: 0 },
    byType: {
      trade: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
      withdrawal: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
      bridge: { xpx: '0.0000', usdt: '0.0000', bnb: '0.0000', count: 0 },
    },
    recent: [],
  };
  try {
    feesSummary = clobDb.getCollectedFeesSummary();
  } catch (err) {
    console.warn('[Admin Overview] Failed to fetch fees summary:', err.message);
  }

  let xpxLiabilities = 0n, usdtLiabilities = 0n, bnbLiabilities = 0n;
  try {
    xpxLiabilities = clobApi.getTestnet2LiabilitiesAtomic ? clobApi.getTestnet2LiabilitiesAtomic('13bfc518e40549d7') : 0n;
    usdtLiabilities = clobApi.getTestnet2LiabilitiesAtomic ? clobApi.getTestnet2LiabilitiesAtomic('4430aefef7e15d0c') : 0n;
    bnbLiabilities = clobApi.getTestnet2LiabilitiesAtomic ? clobApi.getTestnet2LiabilitiesAtomic('3cc1d3ce7447b6f9') : 0n;
  } catch (_) {}

  const vXpxAtomic = (scBal.get('13bfc518e40549d7') || 0n) + (vBal.get('13bfc518e40549d7') || 0n);
  const vUsdtAtomic = (scBal.get('4430aefef7e15d0c') || 0n) + (vBal.get('4430aefef7e15d0c') || 0n);
  const vBnbAtomic = (scBal.get('3cc1d3ce7447b6f9') || 0n) + (vBal.get('3cc1d3ce7447b6f9') || 0n);

  const xpxRatio = xpxLiabilities > 0n ? Math.round(Number(vXpxAtomic * 10000n / xpxLiabilities) / 100) : 100;
  const usdtRatio = usdtLiabilities > 0n ? Math.round(Number(vUsdtAtomic * 10000n / usdtLiabilities) / 100) : 100;
  const bnbRatio = bnbLiabilities > 0n ? Math.round(Number(vBnbAtomic * 10000n / bnbLiabilities) / 100) : 100;

  let activeCount = 0;
  const activeOrdersFile = path.join(CLUSTER_DIR, 'active_orders.json');
  if (fs.existsSync(activeOrdersFile)) {
    try {
      const ao = JSON.parse(fs.readFileSync(activeOrdersFile, 'utf8'));
      activeCount = Array.isArray(ao) ? ao.length : Object.keys(ao).length;
    } catch (_) {}
  }

  let pendingSettlementCount = 0;
  try {
    if (typeof defaultBatchSettler.getPendingSettlementQueue === 'function') {
      pendingSettlementCount = defaultBatchSettler.getPendingSettlementQueue().length;
    }
  } catch (_) {}

  let recentTrades = [];
  const tradesFile = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23', 'drive', 'markets', 'xpx_usdt', 'trades.json');
  if (fs.existsSync(tradesFile)) {
    try {
      const td = JSON.parse(fs.readFileSync(tradesFile, 'utf8'));
      if (Array.isArray(td)) {
        const totalLen = td.length;
        recentTrades = td.slice(-10).reverse().map((t, revIdx) => {
          const originalIdx = totalLen - 1 - revIdx;
          const { timestamp, time } = enrichTradeWithTimestamp(t, originalIdx, totalLen, 'xpx_usdt');
          return {
            time,
            timestamp,
            pair: 'XPX/USDT',
            side: t.taker_side || 'Buy',
            price: (Number(t.price || 0) / 1e8).toFixed(6),
            amount: (Number(t.base_amount || 0) / 1e6).toFixed(2),
            fee: (Number(t.taker_fee || 0) / 1e6).toFixed(4),
          };
        });
      }
    } catch (_) {}
  }

  let mmRunning = false;
  const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
  if (fs.existsSync(mmPidFile)) {
    try {
      const pid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
      process.kill(pid, 0);
      mmRunning = true;
    } catch (_) {
      mmRunning = false;
    }
  }

  const h1 = clobApi.computeNodeMarketsHash('node1');
  const h2 = clobApi.computeNodeMarketsHash('node2');
  const h3 = clobApi.computeNodeMarketsHash('node3');
  const bftSynced = (h1 === h2 && h2 === h3 && !!h1);

  const overview = {
    timestamp: now,
    l1Height: height,
    l1Network: l1Config.NETWORK_ENV === 'mainnet' ? 'Sirius Mainnet (184)' : 'Sirius Testnet 2 (168)',
    l1Endpoint: l1Config.NODE_URL,
    circuitBreaker: {
      tripped: !!clobApi.exchangeCircuitBreaker.tripped,
      reason: clobApi.exchangeCircuitBreaker.reason || null,
      trippedAt: clobApi.exchangeCircuitBreaker.trippedAt || null,
    },
    accounts: {
      vault: {
        address: scAddress,
        xpx: (Number(vXpxAtomic) / 1e6).toFixed(2),
        usdt: (Number(vUsdtAtomic) / 1e6).toFixed(2),
        bnb: (Number(vBnbAtomic) / 1e8).toFixed(4),
        xpxLiabilities: (Number(xpxLiabilities) / 1e6).toFixed(2),
        usdtLiabilities: (Number(usdtLiabilities) / 1e6).toFixed(2),
        bnbLiabilities: (Number(bnbLiabilities) / 1e8).toFixed(4),
        xpxCoverage: xpxRatio,
        usdtCoverage: usdtRatio,
        bnbCoverage: bnbRatio,
        solvent: vXpxAtomic >= xpxLiabilities && vUsdtAtomic >= usdtLiabilities,
      },
      feeCollector: {
        address: feeAcc.address.plain(),
        xpx: feesSummary.total.xpx,
        usdt: feesSummary.total.usdt,
        bnb: feesSummary.total.bnb,
        onChainXpx: (Number(fBal.get('13bfc518e40549d7') || 0n) / 1e6).toFixed(4),
        onChainUsdt: (Number(fBal.get('4430aefef7e15d0c') || 0n) / 1e6).toFixed(4),
        totalEvents: feesSummary.total.count,
      },
      treasury: {
        address: treasuryAcc.address.plain(),
        xpx: (Number(tBal.get('13bfc518e40549d7') || 0n) / 1e6).toFixed(2),
        usdt: (Number(tBal.get('4430aefef7e15d0c') || 0n) / 1e6).toFixed(2),
        bnb: (Number(tBal.get('3cc1d3ce7447b6f9') || 0n) / 1e8).toFixed(4),
      }
    },
    stats: {
      activeOrders: activeCount,
      pendingSettlements: pendingSettlementCount,
      marketMakerRunning: mmRunning,
      bftSynced,
      bftStateRoot: h1 ? h1.slice(0, 16) + '...' : 'syncing',
    },
    velocity24h: {
      xpx: {
        used: (Number(rolling.xpx.atomic) / 1e6).toFixed(2),
        limit: (Number(rolling.xpx.limitAtomic) / 1e6).toFixed(2),
        percent: Math.min(100, rolling.xpx.percentUsed || 0),
      },
      usdt: {
        used: (Number(rolling.usdt.atomic) / 1e6).toFixed(2),
        limit: (Number(rolling.usdt.limitAtomic) / 1e6).toFixed(2),
        percent: Math.min(100, rolling.usdt.percentUsed || 0),
      },
      bnb: {
        used: (Number(rolling.bnb.atomic) / 1e8).toFixed(4),
        limit: (Number(rolling.bnb.limitAtomic) / 1e8).toFixed(4),
        percent: Math.min(100, rolling.bnb.percentUsed || 0),
      },
    },
    recentTrades,
    fees: feesSummary,
    feeSchedule: {
      trading: {
        makerBps: 0,
        takerBps: 10,
        makerPercent: '0.00%',
        takerPercent: '0.10%',
      },
      withdrawal: {
        xpx: '1.0 XPX',
        usdt: '1.0 USDT',
        bnb: '0.001 BNB',
        xpxAtomic: '1000000',
        usdtAtomic: '1000000',
        bnbAtomic: '1000',
      },
    },
  };

  cachedOverviewData = overview;
  lastOverviewFetchTime = now;
  return overview;
}

const ALLOWED_LOG_NODES = new Set(['node1', 'node2', 'node3', 'mm', 'exchange', 'server']);
const ALLOWED_MARKETS = new Set(['xpx_usdt', 'xpx_bnb']);

function sanitizeLogNode(rawNode) {
  const n = String(rawNode || 'node1').trim().toLowerCase();
  return ALLOWED_LOG_NODES.has(n) ? n : 'node1';
}

function sanitizeMarket(rawPair) {
  const m = String(rawPair || 'xpx_usdt').trim().toLowerCase().replace('/', '_');
  return ALLOWED_MARKETS.has(m) ? m : 'xpx_usdt';
}

function getLogTail(rawNode, lines = 100) {
  const node = sanitizeLogNode(rawNode);
  let logFile;
  if (node === 'mm') {
    logFile = path.join(CLUSTER_DIR, 'mm.log');
  } else if (node === 'exchange' || node === 'server') {
    const exLog = path.join(ROOT_DIR, '.logs/exchange.log');
    const dashLog = path.join(CLUSTER_DIR, 'dashboard.log');
    logFile = fs.existsSync(exLog) ? exLog : dashLog;
  } else {
    const vmLog = path.join(CLUSTER_DIR, node, 'logs/vm.log');
    const consoleLog = path.join(CLUSTER_DIR, node, 'logs/console.log');
    logFile = fs.existsSync(vmLog) ? vmLog : consoleLog;
  }
  if (!fs.existsSync(logFile)) return 'No logs yet.\n';
  try {
    const content = fs.readFileSync(logFile, 'utf8');
    const allLines = content.split('\n');
    return allLines.slice(-lines).join('\n');
  } catch (e) {
    return `Error reading log: ${e.message}\n`;
  }
}

function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function ansiToHtml(str) {
  // Convert basic ANSI colors to HTML spans
  const colors = {
    '31': '#ef4444', // red
    '32': '#22c55e', // green
    '33': '#eab308', // yellow
    '34': '#3b82f6', // blue
    '35': '#a855f7', // purple
    '36': '#06b6d4', // cyan
    '37': '#f3f4f6', // white
    '90': '#9ca3af', // gray
  };

  let html = escapeHtml(str);
  html = html.replace(/\x1b\[0?m/g, '</span>');
  for (const [code, hex] of Object.entries(colors)) {
    const regex = new RegExp(`\\x1b\\[(?:0;)?${code}m`, 'g');
    html = html.replace(regex, `<span style="color: ${hex}">`);
  }
  // Strip remaining escape codes
  html = html.replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '');
  return html;
}

function getClientIp(req) {
  // If cf-connecting-ip is provided, validate it is a legitimate IP address
  const cfIp = req.headers['cf-connecting-ip'];
  if (cfIp && typeof cfIp === 'string') {
    const trimmed = cfIp.trim();
    if (net.isIP(trimmed)) {
      return trimmed;
    }
  }
  const xForwardedFor = req.headers['x-forwarded-for'];
  if (xForwardedFor && typeof xForwardedFor === 'string') {
    const parts = xForwardedFor.split(',');
    // First IP in X-Forwarded-For is the originating client IP
    for (const part of parts) {
      const candidate = part.trim();
      if (net.isIP(candidate)) {
        return candidate;
      }
    }
  }
  const remote = (req.socket && req.socket.remoteAddress) || (req.connection && req.connection.remoteAddress) || '127.0.0.1';
  return remote.replace(/^::ffff:/, '');
}

function isDirectLocalhost(req) {
  // If request contains Cloudflare or external proxy headers, it is NOT direct localhost
  if (req.headers['cf-connecting-ip'] || req.headers['x-forwarded-for'] || req.headers['forwarded']) {
    return false;
  }
  const remote = (req.socket && req.socket.remoteAddress) || (req.connection && req.connection.remoteAddress) || '';
  const clean = remote.replace(/^::ffff:/, '');
  return clean === '127.0.0.1' || clean === '::1' || clean === 'localhost';
}

function isTelemetryAllowed(req) {
  const adminHeader = req.headers['x-admin-key'] || '';
  if (adminHeader && adminHeader.trim() === getOrInitAdminKey()) {
    return true;
  }
  // Direct localhost connection allowed for local operator cockpit debugging
  if (isDirectLocalhost(req)) {
    return true;
  }
  // Fail-closed by default: strictly disallow public access unless explicitly enabled
  if (process.env.ALLOW_PUBLIC_COCKPIT === 'true') {
    return true;
  }
  return false;
}

// In-Memory Sliding Window Rate Limiter
class SlidingWindowRateLimiter {
  constructor() {
    this.buckets = new Map();
    const interval = setInterval(() => this.cleanup(), 120000);
    if (interval.unref) interval.unref();
  }

  check(key, maxRequests, windowMs) {
    const now = Date.now();
    let timestamps = this.buckets.get(key);
    if (!timestamps) {
      timestamps = [];
      this.buckets.set(key, timestamps);
    }
    const cutoff = now - windowMs;
    while (timestamps.length > 0 && timestamps[0] <= cutoff) {
      timestamps.shift();
    }
    if (timestamps.length >= maxRequests) {
      const retryAfter = Math.max(1, Math.ceil((timestamps[0] + windowMs - now) / 1000));
      return { allowed: false, retryAfter, count: timestamps.length, limit: maxRequests };
    }
    timestamps.push(now);
    return { allowed: true, retryAfter: 0, count: timestamps.length, limit: maxRequests };
  }

  cleanup() {
    const now = Date.now();
    for (const [key, timestamps] of this.buckets.entries()) {
      if (timestamps.length === 0 || timestamps[timestamps.length - 1] < now - 600000) {
        this.buckets.delete(key);
      }
    }
  }

  reset() {
    this.buckets.clear();
  }
}

const apiRateLimiter = new SlidingWindowRateLimiter();

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;
  const clientIp = getClientIp(req);
  const isDirectLocal = isDirectLocalhost(req);

  // Hardened Security & CORS headers
  const isAdminRoute = pathname.startsWith('/api/admin') || pathname.startsWith('/api/logs');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader(
    'Access-Control-Allow-Headers',
    isAdminRoute
      ? 'Content-Type, Authorization, X-Requested-With, X-Admin-Key'
      : 'Content-Type, Authorization, X-Requested-With'
  );
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  res.setHeader(
    'Content-Security-Policy',
    "default-src 'self' 'unsafe-inline' https:; font-src 'self' https: data:; img-src 'self' data: https:; connect-src 'self' https: wss:;"
  );

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // ===========================================================================
  // Per-IP Sliding Window Rate Limiting Enforcement
  // ===========================================================================
  // 1. Faucets: 3 requests per 5 minutes (300,000 ms)
  if (pathname === '/api/clob/faucet' || pathname === '/api/swap/faucet') {
    const rate = apiRateLimiter.check(`faucet:${clientIp}`, 3, 300000);
    if (!rate.allowed) {
      res.writeHead(429, {
        'Content-Type': 'application/json',
        'Retry-After': String(rate.retryAfter),
      });
      res.end(JSON.stringify({
        ok: false,
        error: `Rate limit exceeded: Faucets are limited to 3 claims per 5 minutes per IP. Please wait ${rate.retryAfter}s before retrying.`,
        retryAfter: rate.retryAfter,
      }));
      return;
    }
  }

  // 2. CLOB Trading Orders, Withdrawals & Cancellations: High-throughput capacity for active trading & load testing
  if (
    pathname === '/api/clob/order' ||
    pathname === '/api/clob/cancel' ||
    pathname === '/api/clob/cancel/batch' ||
    pathname === '/api/clob/cancel/all' ||
    pathname === '/api/clob/withdraw' ||
    pathname === '/api/clob/emergency-unlock' ||
    pathname === '/api/clob/deposit' ||
    pathname === '/api/clob/account/activate-trading-account' ||
    pathname === '/api/clob/trading-account/cosign-withdrawal' ||
    pathname === '/api/clob/trading-account/cosign-settlement'
  ) {
    const adminHeader = req.headers['x-admin-key'] || req.headers['x-load-test-key'];
    const isLoadTestAuth = Boolean(
      adminHeader &&
      (adminHeader === clusterAdminKey || (process.env.NODE_ENV !== 'production' && adminHeader === 'sirius-load-test-suite'))
    );
    const isPrivileged = isDirectLocal || isLoadTestAuth;
    const ipLimit = isPrivileged ? 2400 : (process.env.CLOB_IP_RATE_LIMIT ? parseInt(process.env.CLOB_IP_RATE_LIMIT, 10) : 600);
    const rate = apiRateLimiter.check(`clob:${clientIp}`, ipLimit, 60000);
    if (!rate.allowed) {
      res.writeHead(429, {
        'Content-Type': 'application/json',
        'Retry-After': String(rate.retryAfter),
      });
      res.end(JSON.stringify({
        ok: false,
        error: `Rate limit exceeded: CLOB transactions are limited to ${ipLimit} requests per minute per IP. Please wait ${rate.retryAfter}s before retrying.`,
        retryAfter: rate.retryAfter,
      }));
      return;
    }
  }

  // 3. Cross-Chain Swap Gateway Operations: 60 requests per minute (60,000 ms)
  if (pathname.startsWith('/api/swap/') && pathname !== '/api/swap/faucet') {
    const rate = apiRateLimiter.check(`swap:${clientIp}`, 60, 60000);
    if (!rate.allowed) {
      res.writeHead(429, {
        'Content-Type': 'application/json',
        'Retry-After': String(rate.retryAfter),
      });
      res.end(JSON.stringify({
        ok: false,
        error: `Rate limit exceeded: Cross-chain swap operations are limited to 60 requests per minute per IP. Please wait ${rate.retryAfter}s before retrying.`,
        retryAfter: rate.retryAfter,
      }));
      return;
    }
  }

  // 4. CLOB General Market & Account Read Queries: High-throughput capacity for streaming & load testing
  if (pathname.startsWith('/api/clob/') && req.method === 'GET') {
    const adminHeader = req.headers['x-admin-key'] || req.headers['x-load-test-key'];
    const isLoadTestAuth = Boolean(
      adminHeader &&
      (adminHeader === clusterAdminKey || (process.env.NODE_ENV !== 'production' && adminHeader === 'sirius-load-test-suite'))
    );
    const isPrivileged = isDirectLocal || isLoadTestAuth;
    const readLimit = isPrivileged ? 3600 : (process.env.CLOB_READ_RATE_LIMIT ? parseInt(process.env.CLOB_READ_RATE_LIMIT, 10) : 1200);
    const rate = apiRateLimiter.check(`clob_read:${clientIp}`, readLimit, 60000);
    if (!rate.allowed) {
      res.writeHead(429, {
        'Content-Type': 'application/json',
        'Retry-After': String(rate.retryAfter),
      });
      res.end(JSON.stringify({
        ok: false,
        error: `Rate limit exceeded: Market read queries are limited to ${readLimit} requests per minute per IP. Please wait ${rate.retryAfter}s before retrying.`,
        retryAfter: rate.retryAfter,
      }));
      return;
    }
  }

  // API Endpoints
  if (pathname === '/api/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ nodes: getClusterStatus() }));
    return;
  }

  if (pathname === '/api/logs') {
    if (!isTelemetryAllowed(req)) {
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Forbidden: Log inspection is restricted to authorized operators' }));
      return;
    }
    const node = sanitizeLogNode(parsedUrl.searchParams.get('node'));
    const lines = Math.min(2000, Math.max(1, parseInt(parsedUrl.searchParams.get('lines') || '150', 10) || 150));
    const rawLogs = getLogTail(node, lines);
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(rawLogs);
    return;
  }

  // SSE Stream for Real-Time Logs
  if (pathname === '/api/logs/stream') {
    if (!isTelemetryAllowed(req)) {
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Forbidden: Log streaming is restricted to authorized operators' }));
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });

    const node = sanitizeLogNode(parsedUrl.searchParams.get('node'));
    let lastLength = 0;

    const interval = setInterval(() => {
      const logFile = node === 'mm'
        ? path.join(CLUSTER_DIR, 'mm.log')
        : path.join(CLUSTER_DIR, node, 'logs/vm.log');
      if (fs.existsSync(logFile)) {
        try {
          const stats = fs.statSync(logFile);
          if (stats.size > lastLength) {
            const stream = fs.createReadStream(logFile, {
              start: Math.max(0, lastLength),
              end: stats.size
            });
            let chunk = '';
            stream.on('data', d => chunk += d);
            stream.on('end', () => {
              if (chunk.length > 0) {
                res.write(`data: ${JSON.stringify({ log: chunk })}\n\n`);
              }
              lastLength = stats.size;
            });
          }
        } catch (e) {}
      }
    }, 1000);

    req.on('close', () => clearInterval(interval));
    return;
  }

  // Admin Action: Emergency Halt Exchange (Trip Circuit Breaker, Admin Protected)
  if (pathname === '/api/action/emergency-halt' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'EMERGENCY_HALT');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const reason = (body && body.reason) || 'Manual emergency halt triggered by administrator from Cockpit Dashboard';
      const breaker = clobApi.tripCircuitBreaker(reason);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        ok: true,
        message: '🚨 EMERGENCY HALT ENGAGED: Exchange order intake, matching, and withdrawals frozen immediately.',
        circuitBreaker: breaker
      }));
    });
    return;
  }

  // Admin Action: Resume Trading (Reset Circuit Breaker, Admin Protected)
  if (pathname === '/api/action/resume-trading' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'RESUME_TRADING');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const breaker = clobApi.resetCircuitBreaker();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        ok: true,
        message: '🟢 TRADING RESUMED: Circuit breaker reset to active state.',
        circuitBreaker: breaker
      }));
    });
    return;
  }

  // Admin Action: Restart All Services Cleanly (Admin Protected)
  if (pathname === '/api/action/restart' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'RESTART');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const servicesSh = path.join(ROOT_DIR, 'services.sh');
      if (fs.existsSync(servicesSh)) {
        exec(`${servicesSh} restart`, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: !err, output: stdout + (stderr || '') }));
        });
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          success: true,
          ok: true,
          message: 'Cloud container restart initiated: Container will reboot cleanly in 1 second.'
        }));
        setTimeout(() => {
          console.log('[ADMIN] Graceful container restart initiated by administrator');
          process.exit(0);
        }, 1000);
      }
    });
    return;
  }

  // Admin Action: Start Services (Admin Protected)
  if (pathname === '/api/action/start' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'START');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const servicesSh = path.join(ROOT_DIR, 'services.sh');
      if (fs.existsSync(servicesSh)) {
        exec(`${servicesSh} start`, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: !err, output: stdout + (stderr || '') }));
        });
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: 'All cloud services are running and managed by container orchestrator.' }));
      }
    });
    return;
  }

  // Admin Action: Stop Services (Admin Protected)
  if (pathname === '/api/action/stop' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'STOP');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const servicesSh = path.join(ROOT_DIR, 'services.sh');
      if (fs.existsSync(servicesSh)) {
        exec(`${servicesSh} stop`, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: !err, output: stdout + (stderr || '') }));
        });
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Direct service stoppage is disabled in cloud container environments. Use restart or platform console.' }));
      }
    });
    return;
  }

  // Admin Overview Data Endpoint (Real-time live telemetry for Cockpit)
  if (pathname === '/api/admin/overview' && req.method === 'GET') {
    if (!isTelemetryAllowed(req)) {
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Forbidden: Admin telemetry is restricted to authorized operators' }));
      return;
    }
    (async () => {
      try {
        const overview = await getAdminOverviewData();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, data: overview }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      }
    })();
    return;
  }

  // SiriusCLOB DEX Endpoint: Circuit Breaker Status & 24h Global Withdrawal Volume Ceilings
  if ((pathname === '/api/clob/circuit-breaker' || pathname === '/api/clob/health') && req.method === 'GET') {
    const clobApi = require('./clob_api');
    if (typeof clobApi.loadCircuitBreakerState === 'function') clobApi.loadCircuitBreakerState();
    const h1 = clobApi.computeNodeMarketsHash('node1');
    const h2 = clobApi.computeNodeMarketsHash('node2');
    const h3 = clobApi.computeNodeMarketsHash('node3');
    const majorityNode = clobApi.selectMajorityCanonicalNode();
    const rolling24h = clobApi.getRolling24hWithdrawalVolume();

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        ok: !clobApi.exchangeCircuitBreaker.tripped,
        circuitBreaker: clobApi.exchangeCircuitBreaker,
        majorityCanonicalNode: majorityNode,
        nodeStateRoots: { node1: h1, node2: h2, node3: h3 },
        rolling24hWithdrawalVolume: rolling24h,
        withdrawalLimits: {
          xpx: clobApi.WITHDRAWAL_LIMITS_24H.XPX.toString(),
          usdt: clobApi.WITHDRAWAL_LIMITS_24H.USDT.toString(),
          bnb: clobApi.WITHDRAWAL_LIMITS_24H.BNB.toString(),
        },
      })
    );
    return;
  }

  // SiriusCLOB DEX Endpoint: Reset Circuit Breaker (Admin Protected)
  if (pathname === '/api/clob/circuit-breaker/reset' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const action = String((body && body.action) || 'RESET_CIRCUIT_BREAKER').toUpperCase();
          const expected = (action === 'CIRCUIT_BREAKER_RESET' || action === 'RESET_CIRCUIT_BREAKER') ? action : 'RESET_CIRCUIT_BREAKER';
          const auth = verifyAdminAuth(req, body, expected);
          if (!auth.authorized) {
            res.writeHead(401, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
            return;
          }
          const resetBreaker = clobApi.resetCircuitBreaker();
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(
            JSON.stringify({
              ok: true,
              success: true,
              message: 'Circuit breaker reset successfully by administrator',
              circuitBreaker: resetBreaker,
              rolling24hWithdrawalVolume: clobApi.getRolling24hWithdrawalVolume(),
            })
          );
        } catch (err) {
          const status = err.message.includes('Unauthorized') ? 403 : (err.message.includes('Authentication required') ? 401 : 400);
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Export Network Drive Snapshot Bundle (for cross-machine WAN state sync)
  if (pathname === '/api/clob/snapshot' && req.method === 'GET') {
    const clobApi = require('./clob_api');
    const snapshot = clobApi.exportNodeDriveSnapshot();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: Boolean(snapshot), snapshot }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Run 3-Node E2E Verification (Admin Protected)
  if (pathname === '/api/clob/test-e2e' && req.method === 'POST') {
    parseJsonBody(req).catch(() => ({})).then((body) => {
      const auth = verifyAdminAuth(req, body, 'TEST_E2E');
      if (!auth.authorized) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
        return;
      }
      const testScript = path.join(ROOT_DIR, 'scripts/test_clob_e2e.js');
      if (!fs.existsSync(testScript)) {
        res.writeHead(501, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: 'E2E test suite script is not installed in production cloud container.' }));
        return;
      }
      execFile('node', [testScript], { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ok: !err,
          success: !err,
          summary: stdout.split('\n').filter(l => l.includes('Criterion') || l.includes('|') || l.includes('✅')).join('\n'),
          output: stdout,
          stdout,
          stderr: err ? err.message : null
        }));
      });
    });
    return;
  }

  // SiriusCLOB DEX Endpoint: Get Market Maker Bot Status
  if (pathname === '/api/clob/mm/status' && req.method === 'GET') {
    const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
    const mmStatusFile = path.join(CLUSTER_DIR, 'mm_status.json');
    let running = false;
    let pid = null;

    if (fs.existsSync(mmPidFile)) {
      try {
        pid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
        process.kill(pid, 0);
        running = true;
      } catch (_) {
        running = false;
      }
    }

    let statusData = {
      running,
      pid: running ? pid : null,
      mode: running ? 'daemon' : 'stopped',
      round: 0,
      totalTrades: 0,
      volumeXPX: 0,
      volumeUSDT: 0,
      midPrice: 500000,
      spreadTicks: 200,
      lastAction: running ? 'Active' : 'Idle',
      lastUpdate: Date.now()
    };

    if (fs.existsSync(mmStatusFile)) {
      try {
        const fileContent = JSON.parse(fs.readFileSync(mmStatusFile, 'utf8'));
        statusData = { ...statusData, ...fileContent, running, pid: running ? pid : null };
      } catch (_) {}
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(statusData));
    return;
  }

  // SiriusCLOB DEX Endpoint: Start Market Maker Bot (Admin Protected)
  if (pathname === '/api/clob/mm/start' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const auth = verifyAdminAuth(req, body, 'MM_START');
          if (!auth.authorized) {
            res.writeHead(401, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
            return;
          }
          const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
          if (fs.existsSync(mmPidFile)) {
            try {
              const existingPid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
              process.kill(existingPid, 0);
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, success: true, message: 'Already running', pid: existingPid, output: `Market Maker is already running (PID ${existingPid})` }));
              return;
            } catch (_) {}
          }

          const logFile = fs.openSync(path.join(CLUSTER_DIR, 'mm.log'), 'a');
          const child = spawn('node', [path.join(ROOT_DIR, 'scripts/clob_market_maker.js'), '--daemon'], {
            detached: true,
            stdio: ['ignore', logFile, logFile],
            cwd: ROOT_DIR
          });
          child.unref();

          fs.writeFileSync(mmPidFile, String(child.pid));
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, success: true, message: 'Market Maker bot launched', pid: child.pid, output: `Market Maker launched in daemon mode (PID ${child.pid})` }));
        } catch (err) {
          const status = err.message.includes('Unauthorized') ? 403 : (err.message.includes('Authentication required') ? 401 : 400);
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Stop Market Maker Bot (Admin Protected)
  if (pathname === '/api/clob/mm/stop' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const auth = verifyAdminAuth(req, body, 'MM_STOP');
          if (!auth.authorized) {
            res.writeHead(401, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
            return;
          }
          const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
          let stoppedPid = null;
          if (fs.existsSync(mmPidFile)) {
            try {
              stoppedPid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
              process.kill(stoppedPid, 'SIGTERM');
            } catch (_) {}
            try { fs.unlinkSync(mmPidFile); } catch (_) {}
          }
          const mmStatusFile = path.join(CLUSTER_DIR, 'mm_status.json');
          if (fs.existsSync(mmStatusFile)) {
            try {
              const cur = JSON.parse(fs.readFileSync(mmStatusFile, 'utf8'));
              cur.running = false;
              cur.pid = null;
              cur.lastAction = 'Market Maker stopped by user';
              fs.writeFileSync(mmStatusFile, JSON.stringify(cur, null, 2));
            } catch (_) {}
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, success: true, message: 'Market Maker bot stopped', output: stoppedPid ? `Market Maker (PID ${stoppedPid}) stopped.` : 'Market Maker was not running.' }));
        } catch (err) {
          const status = err.message.includes('Unauthorized') ? 403 : (err.message.includes('Authentication required') ? 401 : 400);
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Execute 1 Step (Single Trading Round, Admin Protected)
  if (pathname === '/api/clob/mm/step' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const auth = verifyAdminAuth(req, body, 'MM_STEP');
          if (!auth.authorized) {
            res.writeHead(401, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
            return;
          }
          execFile('node', [path.join(ROOT_DIR, 'scripts/clob_market_maker.js'), '--step'], { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              ok: !err,
              success: !err,
              output: stdout,
              stdout,
              stderr: err ? err.message : null
            }));
          });
        } catch (err) {
          const status = err.message.includes('Unauthorized') ? 403 : (err.message.includes('Authentication required') ? 401 : 400);
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Market Maker Logs
  if (pathname === '/api/clob/mm/logs' && req.method === 'GET') {
    const mmLogFile = path.join(CLUSTER_DIR, 'mm.log');
    if (fs.existsSync(mmLogFile)) {
      try {
        const content = fs.readFileSync(mmLogFile, 'utf8');
        const lines = content.split('\n').slice(-100).join('\n');
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end(lines);
        return;
      } catch (e) {
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Error reading mm.log: ' + e.message);
        return;
      }
    }
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('No Market Maker logs yet.\n');
    return;
  }

  // SiriusCLOB DEX Endpoint: Live Trade History (from Replicator Drive)
  if (pathname === '/api/clob/trades' && req.method === 'GET') {
    const market = sanitizeMarket(parsedUrl.searchParams.get('pair') || parsedUrl.searchParams.get('market'));
    const limitParam = parseInt(parsedUrl.searchParams.get('limit') || '100', 10);
    const limit = Math.min(500, Math.max(1, isNaN(limitParam) ? 100 : limitParam));
    const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
    const tradesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'trades.json');
    if (fs.existsSync(tradesPath)) {
      try {
        const raw = fs.readFileSync(tradesPath, 'utf8');
        const rawTrades = JSON.parse(raw);
        if (Array.isArray(rawTrades) && rawTrades.length > 0) {
          const totalLen = rawTrades.length;
          const enriched = rawTrades.map((t, idx) => {
            const { timestamp, time } = enrichTradeWithTimestamp(t, idx, totalLen, market);
            return {
              ...t,
              timestamp,
              time
            };
          });
          const recentTrades = enriched.slice(-limit).reverse();
          lastValidTrades[market] = recentTrades;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(recentTrades));
          return;
        }
      } catch (_) {}
    }
    // Fallback to in-memory cached trades so trades list is NEVER empty
    if (lastValidTrades[market] && lastValidTrades[market].length > 0) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(lastValidTrades[market]));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('[]');
    return;
  }

  // SiriusCLOB DEX Endpoint: Historical OHLCV Candles (from Replicator Drive Trades)
  if (pathname === '/api/clob/candles' && req.method === 'GET') {
    const market = sanitizeMarket(parsedUrl.searchParams.get('pair') || parsedUrl.searchParams.get('market'));
    const intervalStr = (parsedUrl.searchParams.get('interval') || '15m').toLowerCase();
    const intervalMinutes = intervalStr === '1m' ? 1 : (intervalStr === '5m' ? 5 : (intervalStr === '1h' ? 60 : (intervalStr === '1d' ? 1440 : 15)));
    const intervalSec = intervalMinutes * 60;
    const limit = Math.min(500, Math.max(10, parseInt(parsedUrl.searchParams.get('limit') || '200', 10)));

    const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
    const tradesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'trades.json');

    const cachedOracle = getCachedPancakeSwapPriceSync();
    let defaultPrice = market === 'xpx_bnb' ? 0.00000014 : 0.000076;
    if (cachedOracle) {
      if (market === 'xpx_bnb' && cachedOracle.priceBnb) defaultPrice = cachedOracle.priceBnb;
      if (market === 'xpx_usdt' && cachedOracle.priceUsdt) defaultPrice = cachedOracle.priceUsdt;
    }
    const decimals = market === 'xpx_bnb' ? 8 : 6;

    if (fs.existsSync(tradesPath)) {
      try {
        const raw = fs.readFileSync(tradesPath, 'utf8');
        const rawTrades = JSON.parse(raw);
        if (Array.isArray(rawTrades) && rawTrades.length > 0) {
          const totalLen = rawTrades.length;
          const enriched = rawTrades.map((t, idx) => {
            const { timestamp } = enrichTradeWithTimestamp(t, idx, totalLen, market);
            const price = Number(t.price || 0) / 1e8;
            const amount = Number(t.base_amount || 0) / 1e6;
            return { timestamp, price, amount };
          }).sort((a, b) => a.timestamp - b.timestamp);

          const buckets = new Map();
          for (const t of enriched) {
            const sec = Math.floor(t.timestamp / 1000);
            const bucketKey = Math.floor(sec / intervalSec) * intervalSec;
            if (!buckets.has(bucketKey)) {
              buckets.set(bucketKey, {
                time: bucketKey,
                open: parseFloat(t.price.toFixed(decimals)),
                high: parseFloat(t.price.toFixed(decimals)),
                low: parseFloat(t.price.toFixed(decimals)),
                close: parseFloat(t.price.toFixed(decimals)),
                volume: parseFloat(t.amount.toFixed(2))
              });
            } else {
              const b = buckets.get(bucketKey);
              if (t.price > b.high) b.high = parseFloat(t.price.toFixed(decimals));
              if (t.price < b.low) b.low = parseFloat(t.price.toFixed(decimals));
              b.close = parseFloat(t.price.toFixed(decimals));
              b.volume = parseFloat((b.volume + t.amount).toFixed(2));
            }
          }

          let candles = Array.from(buckets.values()).sort((a, b) => a.time - b.time);
          if (candles.length > limit) {
            candles = candles.slice(-limit);
          }
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true, pair: market, interval: intervalStr, candles }));
          return;
        }
      } catch (_) {}
    }

    const nowSec = Math.floor(Date.now() / 1000);
    const bucketKey = Math.floor(nowSec / intervalSec) * intervalSec;
    const fallbackCandle = {
      time: bucketKey,
      open: defaultPrice,
      high: defaultPrice,
      low: defaultPrice,
      close: defaultPrice,
      volume: 0
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, pair: market, interval: intervalStr, candles: [fallbackCandle] }));
    return;
  }

  // SiriusCLOB DEX Endpoint: PancakeSwap Real Spot Price Oracle (BSC On-Chain + DexScreener Fallback)
  if (pathname === '/api/clob/oracle/pancakeswap' && req.method === 'GET') {
    getPancakeSwapPriceCached()
      .then((oracleData) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(oracleData));
      })
      .catch((e) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Public Network & Exchange Configuration
  if ((pathname === '/api/clob/config' || pathname === '/api/clob/info') && req.method === 'GET') {
    const configData = {
      ok: true,
      network: l1Config.NETWORK_ENV,
      networkName: l1Config.NETWORK_NAME,
      networkType: l1Config.NETWORK_TYPE,
      generationHash: l1Config.GENERATION_HASH,
      nodeUrl: l1Config.NODE_URL,
      vaultAddress: l1Config.VAULT_ADDRESS,
      supercontractAddress: l1Config.SUPERCONTRACT_ADDRESS,
      feeCollectorAddress: l1Config.FEE_COLLECTOR_ADDRESS,
      explorerBaseUrl: l1Config.EXPLORER_BASE_URL,
      faucetEnabled: Boolean(l1Config.FAUCET_ENABLED),
      confirmationDepth: l1Config.CONFIRMATION_DEPTH,
      pairs: [
        {
          symbol: 'XPX/USDT',
          baseSymbol: 'XPX',
          quoteSymbol: 'USDT',
          baseMosaicId: l1Config.XPX_MOSAIC_ID,
          quoteMosaicId: l1Config.USDT_MOSAIC_ID,
          priceDecimals: 6,
          amountDecimals: 6,
          tickSize: 0.000001,
          minOrderAmount: 1,
        },
        {
          symbol: 'XPX/BNB',
          baseSymbol: 'XPX',
          quoteSymbol: 'BNB',
          baseMosaicId: l1Config.XPX_MOSAIC_ID,
          quoteMosaicId: l1Config.BNB_MOSAIC_ID,
          priceDecimals: 8,
          amountDecimals: 6,
          tickSize: 0.00000001,
          minOrderAmount: 1,
        },
      ],
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(configData));
    return;
  }

  // SiriusCLOB DEX Endpoint: Live Depth Snapshot
  if (pathname === '/api/clob/depth' && req.method === 'GET') {
    const market = sanitizeMarket(parsedUrl.searchParams.get('pair') || parsedUrl.searchParams.get('market') || parsedUrl.searchParams.get('symbol'));
    const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
    const depthPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'depth.json');
    if (fs.existsSync(depthPath)) {
      try {
        const data = fs.readFileSync(depthPath, 'utf8');
        const parsed = JSON.parse(data);
        if (parsed && Array.isArray(parsed.bids) && Array.isArray(parsed.asks) && (parsed.bids.length > 0 || parsed.asks.length > 0)) {
          lastValidDepth[market] = data;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(data);
          return;
        }
      } catch (_) {}
    }
    // Fallback to in-memory cached depth so order book is NEVER empty
    if (lastValidDepth[market]) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(lastValidDepth[market]);
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      base_mosaic_hex: "0x0DC67FBE1CAD29E3",
      quote_mosaic_hex: market === 'xpx_bnb' ? "0x1E5C5FADEC7B34DD" : "0x10112CBA1E7B9037",
      best_bid: null,
      best_ask: null,
      bids: [],
      asks: []
    }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Place Order from Web Wallet
  if (pathname === '/api/clob/order' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.placeOrder(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Cancel Order from Web Wallet
  if (pathname === '/api/clob/cancel' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.cancelOrder(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          const status = err.statusCode || 400;
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
        }
      })
      .catch((err) => {
        const status = err.statusCode || 400;
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Batch Cancel Orders from Web Wallet or Bot
  if (pathname === '/api/clob/cancel/batch' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.batchCancelOrders(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          const status = err.statusCode || 400;
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
        }
      })
      .catch((err) => {
        const status = err.statusCode || 400;
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Cancel All Orders from Web Wallet or Bot
  if (pathname === '/api/clob/cancel/all' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.cancelAllOrders(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          const status = err.statusCode || 400;
          res.writeHead(status, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
        }
      })
      .catch((err) => {
        const status = err.statusCode || 400;
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message, statusCode: status }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Register or Refresh Cancel-on-Disconnect (COD) Heartbeat
  if (pathname === '/api/clob/heartbeat' && req.method === 'POST') {
    parseJsonBody(req)
      .then((body) => {
        try {
          const result = clobApi.registerHeartbeat(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Deregister Cancel-on-Disconnect (COD) Heartbeat
  if (pathname === '/api/clob/heartbeat/deregister' && req.method === 'POST') {
    parseJsonBody(req)
      .then((body) => {
        try {
          const result = clobApi.deregisterHeartbeat(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Query Heartbeat Status
  if (pathname === '/api/clob/heartbeat/status' && req.method === 'GET') {
    const publicKey = parsedUrl.searchParams.get('publicKey') || parsedUrl.searchParams.get('key');
    if (!publicKey) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'publicKey query parameter required' }));
      return;
    }
    const status = clobApi.getHeartbeatStatus(publicKey);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(status));
    return;
  }

  // SiriusCLOB DEX Endpoint: Solvency Sentinel Proof-of-Reserves Audit
  if (pathname === '/api/clob/solvency/audit' && req.method === 'GET') {
    const tripOnBreach = parsedUrl.searchParams.get('trip') === 'true';
    clobApi
      .runSolvencySentinelAudit({ forceSync: true, tripOnBreach })
      .then((audit) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, audit }));
      })
      .catch((err) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Manual Trigger for Sweeping Expired Stale Orders
  if (pathname === '/api/clob/orders/sweep-expired' && req.method === 'POST') {
    clobApi
      .sweepExpiredOrders()
      .then((result) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      })
      .catch((err) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Get Open Orders for User
  if (pathname === '/api/clob/orders/open' && req.method === 'GET') {
    const publicKey = parsedUrl.searchParams.get('publicKey') || parsedUrl.searchParams.get('key');
    const pair = parsedUrl.searchParams.get('pair') || parsedUrl.searchParams.get('market');
    const orders = clobApi.getOpenOrders(publicKey, pair);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, orders }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Deposit Tokens to Vault & Ledger
  if (pathname === '/api/clob/deposit' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.depositTokens(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Withdraw Tokens from Vault to User Account
  if (pathname === '/api/clob/withdraw' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.withdrawTokens(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Emergency Direct On-Chain Unlock
  if (pathname === '/api/clob/emergency-unlock' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.emergencyUnlock(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // === Solution 2 (Model A): Native Co-Signed Smart Trading Account Endpoints ===

  // 1. Get Level-2 BFT Validator Cluster info & public keys
  if (pathname === '/api/clob/validator-info' && req.method === 'GET') {
    try {
      const info = tradingCosigner.getValidatorClusterInfo();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(info));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    }
    return;
  }

  // 2. Discover & query user Smart Trading Account status (SQLite + direct Sirius L1 multisig graph discovery)
  if (pathname === '/api/clob/trading-account/status' && req.method === 'GET') {
    const owner = parsedUrl.searchParams.get('ownerPublicKey') ||
      parsedUrl.searchParams.get('publicKey') ||
      parsedUrl.searchParams.get('ownerAddress') ||
      parsedUrl.searchParams.get('address');
    if (!owner) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'ownerPublicKey or ownerAddress query parameter required' }));
      return;
    }
    tradingCosigner
      .discoverTradingAccountStatus(owner)
      .then((status) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(status));
      })
      .catch((err) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // 3. Activate Native 2-of-2 Co-Signed Smart Trading Account (ModifyMultisig + 2-of-3 BFT Validator Cosign)
  if (pathname === '/api/clob/account/activate-trading-account' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await tradingCosigner.activateTradingAccount(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // 4. Co-sign non-custodial withdrawal from User's 2-of-2 Smart Trading Account
  if (pathname === '/api/clob/trading-account/cosign-withdrawal' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await clobApi.cosignTradingAccountWithdrawal(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // 5. Query pending atomic settlements requiring User Cosigner-1 signature
  if (pathname === '/api/clob/trading-account/pending-settlements' && req.method === 'GET') {
    const ownerPub = parsedUrl.searchParams.get('ownerPublicKey') || parsedUrl.searchParams.get('publicKey');
    if (!ownerPub) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'ownerPublicKey query parameter required' }));
      return;
    }
    try {
      const settlements = clobDb.getPendingSettlementsForOwner(ownerPub);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, settlements }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    }
    return;
  }

  // 6. Submit User Cosigner-1 signature for atomic trade settlement & trigger 2-of-3 BFT Validator broadcast
  if (pathname === '/api/clob/trading-account/cosign-settlement' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const result = await tradingCosigner.submitSettlementCosignature(body);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Inspect On-Chain Balance Ledger
  if (pathname === '/api/clob/ledger' && req.method === 'GET') {
    const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
    const balancesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', 'balances.json');
    if (fs.existsSync(balancesPath)) {
      try {
        const data = fs.readFileSync(balancesPath, 'utf8');
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(data);
        return;
      } catch (_) {}
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ entries: [] }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Inspect Pending Batch Settlements
  if ((pathname === '/api/clob/batch/pending' || pathname === '/api/clob/batch/status') && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      pendingCount: defaultBatchSettler.getPendingCount(),
      items: defaultBatchSettler.getPendingItems(),
      netted: defaultBatchSettler.getNettedPayouts(),
    }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Fee Collector Status & Balances
  if (pathname === '/api/clob/fee-collector' && req.method === 'GET') {
    const feeAddr = l1Config.FEE_COLLECTOR_ADDRESS || 'VB3QOFNGGL7DENKPBW2RO3OVIXLOVLQ5F5WLG4XE';
    const feePub = (l1Config.FEE_COLLECTOR_PUBKEY || '04EF8D54AAEFBC4746257723FD1EFB7B37F30452DE8BD022F5FB3CABC5246A64').toLowerCase();
    
    (async () => {
      let onChain = { xpx: '0', usdt: '0', bnb: '0' };
      try {
        const onChainMap = await l1Client.getAccountOnChainBalances(feeAddr);
        const XPX_HEX = (l1Config.XPX_MOSAIC_ID || '13BFC518E40549D7').toLowerCase();
        const USDT_HEX = (l1Config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C').toLowerCase();
        const BNB_HEX = (l1Config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9').toLowerCase();
        onChain = {
          xpx: (Number(onChainMap.get(XPX_HEX) || 0n) / 1e6).toFixed(6),
          usdt: (Number(onChainMap.get(USDT_HEX) || 0n) / 1e6).toFixed(6),
          bnb: (Number(onChainMap.get(BNB_HEX) || 0n) / 1e6).toFixed(6),
          raw: Object.fromEntries(Array.from(onChainMap.entries()).map(([k, v]) => [k, v.toString()])),
        };
      } catch (_) {}

      const ledger = clobApi.getAccountBalance(feePub);
      const feesSummary = clobDb.getCollectedFeesSummary();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        role: 'Exchange Fee Collector',
        address: feeAddr,
        publicKey: feePub,
        onChain,
        ledger,
        feesSummary,
      }));
    })().catch((err) => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    });
    return;
  }

  // SiriusCLOB DEX Endpoint: Protocol Revenue & Collected Fees API
  if (pathname === '/api/clob/fees' && req.method === 'GET') {
    const feeAddr = l1Config.FEE_COLLECTOR_ADDRESS || 'VB3QOFNGGL7DENKPBW2RO3OVIXLOVLQ5F5WLG4XE';
    const feePub = (l1Config.FEE_COLLECTOR_PUBKEY || '04EF8D54AAEFBC4746257723FD1EFB7B37F30452DE8BD022F5FB3CABC5246A64').toLowerCase();
    const summary = clobDb.getCollectedFeesSummary();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      role: 'Exchange Protocol Fee Collector',
      address: feeAddr,
      publicKey: feePub,
      feeSchedule: {
        trading: {
          makerBps: 0,
          takerBps: 10,
          makerPercent: '0.00%',
          takerPercent: '0.10%',
        },
        withdrawal: {
          XPX: '1.0 XPX',
          USDT: '1.0 USDT',
          BNB: '0.001 BNB',
          XPX_atomic: '1000000',
          USDT_atomic: '1000000',
          BNB_atomic: '1000',
        },
      },
      summary,
    }));
    return;
  }

  // SiriusCLOB DEX Endpoint: Flush Pending Batch Settlements (Admin Protected)
  if (pathname === '/api/clob/batch/flush' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        const auth = verifyAdminAuth(req, body, 'BATCH_FLUSH');
        if (!auth.authorized) {
          res.writeHead(401, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, success: false, error: auth.error }));
          return;
        }

        try {
          const result = await defaultBatchSettler.flushBatch();
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, success: false, error: err.message }));
      });
    return;
  }

  // SiriusCLOB DEX Endpoint: Get Local Sandbox Trading Balance
  if (pathname === '/api/clob/balance' && req.method === 'GET') {
    const publicKey = parsedUrl.searchParams.get('publicKey') || parsedUrl.searchParams.get('key');
    (async () => {
      try {
        const balances = await clobApi.syncAndGetAccountBalance(publicKey);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, balances }));
      } catch (_) {
        const balances = clobApi.getAccountBalance(publicKey);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, balances }));
      }
    })();
    return;
  }

  // SiriusCLOB DEX Endpoint: Live On-Chain Mosaics for any Account (Supports Testnet 2 & Mainnet)
  if (pathname === '/api/clob/account/onchain-balance' && req.method === 'GET') {
    const address = (parsedUrl.searchParams.get('address') || '').replace(/[- ]/g, '').toUpperCase();
    if (!address || address.length < 40) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'Valid 40-character Sirius address required' }));
      return;
    }
    (async () => {
      try {
        const isMainnet = address.startsWith('X') || String(testnet2Config.NETWORK_ENV || '').toLowerCase() === 'mainnet';
        let balancesMap = new Map();

        if (address.startsWith('X')) {
          // Direct Mainnet Catapult query
          const mainnetUrls = [
            'https://betelgeuse.xpxsirius.io',
            'https://arcturus.xpxsirius.io',
            'https://aldebaran.xpxsirius.io',
          ];
          for (const mUrl of mainnetUrls) {
            let controller = null;
            let timer = null;
            if (typeof AbortController !== 'undefined') {
              controller = new AbortController();
              timer = setTimeout(() => controller.abort(), 4000);
            }
            try {
              const res = await fetch(`${mUrl}/account/${address}`, {
                headers: { 'Content-Type': 'application/json' },
                signal: controller ? controller.signal : undefined,
              });
              if (res.ok) {
                const aData = await res.json();
                for (const m of (aData.account?.mosaics || [])) {
                  let mHex = '';
                  if (Array.isArray(m.id)) {
                    const low = (m.id[0] >>> 0).toString(16).padStart(8, '0');
                    const high = (m.id[1] >>> 0).toString(16).padStart(8, '0');
                    mHex = (high + low).toLowerCase();
                  } else if (typeof m.id === 'string') {
                    mHex = m.id.toLowerCase().replace(/^0x/, '');
                  }
                  let amt = 0n;
                  if (Array.isArray(m.amount)) {
                    amt = BigInt(m.amount[1] >>> 0) * 4294967296n + BigInt(m.amount[0] >>> 0);
                  } else if (m.amount) {
                    amt = BigInt(m.amount);
                  }
                  if (mHex) balancesMap.set(mHex, amt);
                }
                break;
              }
            } catch (_) {
            } finally {
              if (timer) clearTimeout(timer);
            }
          }
        } else {
          const t2Client = require('./testnet2_client');
          balancesMap = await t2Client.getAccountOnChainBalances(address);
        }

        const xpxHex = (isMainnet ? '402b2f579faebc59' : (testnet2Config.XPX_MOSAIC_ID || '13bfc518e40549d7')).toLowerCase();
        const usdtHex = (isMainnet ? '7525381a171d1e44' : (testnet2Config.USDT_MOSAIC_ID || '4430aefef7e15d0c')).toLowerCase();
        const bnbHex = (isMainnet ? '1e5c5fadec7b34dd' : (testnet2Config.BNB_MOSAIC_ID || '3cc1d3ce7447b6f9')).toLowerCase();

        const xpxAtomic = balancesMap.get(xpxHex) || 
                          (isMainnet ? (balancesMap.get('bffb42a19116bdf6') || 0n) : 0n);
        const usdtAtomic = balancesMap.get(usdtHex) || 0n;
        const bnbAtomic = balancesMap.get(bnbHex) || 0n;

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ok: true,
          address,
          network: isMainnet ? 'mainnet' : 'testnet2',
          balances: {
            xpx: Number(xpxAtomic) / 1e6,
            usdt: Number(usdtAtomic) / 1e6,
            bnb: Number(bnbAtomic) / 1e6,
            xpxAtomic: xpxAtomic.toString(),
            usdtAtomic: usdtAtomic.toString(),
            bnbAtomic: bnbAtomic.toString()
          }
        }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      }
    })();
    return;
  }

  // SiriusCLOB DEX Endpoint: Solvency Reconciliation & Gas Reserve Audit
  if (pathname === '/api/clob/solvency' && req.method === 'GET') {
    (async () => {
      try {
        const forceSync = parsedUrl.searchParams.get('force') === 'true';
        if (forceSync) {
          await clobApi.syncVaultBalancesFromTestnet2();
        }
        const xpx = clobApi.assertSolvencyReconciliation(clobApi.XPX_MOSAIC_ID, forceSync);
        const usdt = clobApi.assertSolvencyReconciliation(clobApi.USDT_MOSAIC_ID, forceSync);
        let bnb = null;
        try {
          bnb = clobApi.assertSolvencyReconciliation(clobApi.BNB_MOSAIC_ID, forceSync);
        } catch (_) {}

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          ok: true,
          solvency: {
            xpx: {
              solvent: true,
              ledgerTotal: xpx.ledgerTotal.toString(),
              feeReserve: xpx.feeReserve.toString(),
              pendingNetworkFees: xpx.pendingNetworkFees.toString(),
              requiredVaultBalance: xpx.requiredVaultBalance.toString(),
              onChainVaultBalance: xpx.onChainVaultBalance.toString(),
            },
            usdt: {
              solvent: true,
              ledgerTotal: usdt.ledgerTotal.toString(),
              feeReserve: usdt.feeReserve.toString(),
              requiredVaultBalance: usdt.requiredVaultBalance.toString(),
              onChainVaultBalance: usdt.onChainVaultBalance.toString(),
            },
            bnb: bnb ? {
              solvent: true,
              ledgerTotal: bnb.ledgerTotal.toString(),
              feeReserve: bnb.feeReserve.toString(),
              requiredVaultBalance: bnb.requiredVaultBalance.toString(),
              onChainVaultBalance: bnb.onChainVaultBalance.toString(),
            } : null,
          },
          timestamp: Date.now(),
        }));
      } catch (err) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, solvent: false, error: err.message }));
      }
    })().catch((err) => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    });
    return;
  }

  // SiriusCLOB DEX Endpoint: Claim Faucet Tokens
  if (pathname === '/api/clob/faucet' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        try {
          const publicKey = body.publicKey || parsedUrl.searchParams.get('publicKey');
          const address = body.address || parsedUrl.searchParams.get('address');
          const result = await clobApi.claimFaucet(publicKey, address);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
        } catch (err) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  // ===========================================================================
  // Cross-Chain Swap Gateway Endpoints (BSC Testnet <-> Sirius Chain)
  // ===========================================================================
  if (pathname === '/api/swap/bsc/checkBalanceOk' || pathname === '/api/swap/checkBalanceOk') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, status: 'ok' }));
    return;
  }

  if (pathname === '/api/swap/relayer/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getRelayerStatus()));
    return;
  }

  // SiriusCLOB DEX Endpoint: Set or Get Active Wallet Account for Auto-Bridge Routing
  if (pathname === '/api/swap/active-account' && req.method === 'POST') {
    parseJsonBody(req)
      .then((body) => {
        if (body && body.address) {
          swapApi.setActiveUserAccount(body.address);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, activeAccount: swapApi.getActiveUserAccount() }));
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  if (pathname === '/api/swap/active-account' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, activeAccount: swapApi.getActiveUserAccount() }));
    return;
  }

  if (pathname === '/api/swap/price/latest' || pathname === '/api/swap/price') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getPrices()));
    return;
  }

  if (pathname === '/api/swap/bsc/swapTokenList' || pathname === '/api/swap/swapTokenList') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getSwapTokenList()));
    return;
  }

  if (pathname === '/api/swap/gas/gaslimit/bsc' || pathname === '/api/swap/gaslimit/bsc') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getGasLimits()));
    return;
  }

  if (pathname === '/api/swap/gas/gasprice/bsc' || pathname === '/api/swap/gasprice/bsc') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getGasPrices()));
    return;
  }

  if (pathname.startsWith('/api/swap/sirius/') && pathname.endsWith('/service-info')) {
    const parts = pathname.split('/');
    const tokenName = parts[parts.length - 2] || 'usdt';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getServiceInfo(tokenName)));
    return;
  }

  if (pathname.startsWith('/api/swap/sirius/') && pathname.endsWith('/transfer') && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        const result = await swapApi.processTransfer(body);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  if (pathname.startsWith('/api/swap/verify-tx/')) {
    const parts = pathname.split('/');
    const txHash = parts[parts.length - 1];
    swapApi.verifyBscTx(txHash).then((result) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    }).catch((err) => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    });
    return;
  }

  if (pathname.startsWith('/api/swap/swap/BSC/') || pathname.startsWith('/api/swap/bsc/swapByTx/')) {
    const parts = pathname.split('/');
    const txHash = parts[parts.length - 1];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getSwapStatus(txHash)));
    return;
  }

  if (pathname === '/api/swap/history' || pathname === '/api/swap/bsc/history') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getSwapHistory()));
    return;
  }

  if (pathname.startsWith('/api/swap/deposit-info') || pathname.startsWith('/api/swap/bsc/deposit-info')) {
    const token = parsedUrl.searchParams.get('token') || 'usdt';
    const address = parsedUrl.searchParams.get('address') || '';
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(swapApi.getDepositInfo(token, address)));
    return;
  }

  if (pathname === '/api/swap/faucet' && req.method === 'POST') {
    parseJsonBody(req)
      .then(async (body) => {
        const target = body.address || body.publicKey;
        const result = await swapApi.claimTestTokens(target, body.amount, body.token);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      })
      .catch((err) => {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    return;
  }

  if (pathname === '/clob' || pathname === '/clob.html' || pathname === '/dex') {
    const redirectHost = req.headers.host && !req.headers.host.includes('8088') ? `//${req.headers.host}` : 'http://127.0.0.1:3030';
    res.writeHead(302, { Location: `${redirectHost}/#/clob-exchange` });
    res.end();
    return;
  }

  // HTML Dashboard
  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(getDashboardHtml());
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

function getDashboardHtml() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SiriusCLOB Executive Exchange Cockpit</title>
  <style>
    :root {
      --bg: #070b14;
      --card-bg: #0f172a;
      --card-border: #1e293b;
      --card-border-glow: rgba(56, 189, 248, 0.2);
      --accent: #38bdf8;
      --accent-hover: #0ea5e9;
      --emerald: #10b981;
      --rose: #f43f5e;
      --amber: #f59e0b;
      --purple: #a855f7;
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      line-height: 1.5;
      padding: 24px;
    }
    .container { max-width: 1380px; margin: 0 auto; }
    
    /* Top Header */
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--card-border);
      flex-wrap: wrap;
      gap: 16px;
    }
    .title-group h1 {
      font-size: 22px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 12px;
      letter-spacing: -0.02em;
    }
    .pills { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .pill {
      font-size: 11px;
      text-transform: uppercase;
      font-weight: 700;
      padding: 4px 10px;
      border-radius: 9999px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      letter-spacing: 0.05em;
    }
    .pill-green {
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .pill-red {
      background: rgba(244, 63, 94, 0.2);
      color: #fb7185;
      border: 1px solid rgba(244, 63, 94, 0.4);
      animation: pulse 1.5s infinite;
    }
    .pill-blue {
      background: rgba(56, 189, 248, 0.15);
      color: var(--accent);
      border: 1px solid rgba(56, 189, 248, 0.3);
    }
    .pill-purple {
      background: rgba(168, 85, 247, 0.15);
      color: #c084fc;
      border: 1px solid rgba(168, 85, 247, 0.3);
    }
    .pill-amber {
      background: rgba(245, 158, 11, 0.15);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.3);
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.6; }
    }

    /* Grids */
    .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }
    .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 20px; }
    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }
    @media (max-width: 1100px) {
      .grid-4 { grid-template-columns: 1fr 1fr; }
      .grid-3 { grid-template-columns: 1fr; }
      .grid-2 { grid-template-columns: 1fr; }
    }
    @media (max-width: 650px) {
      .grid-4 { grid-template-columns: 1fr; }
    }

    /* Cards */
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 18px;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
      position: relative;
    }
    .card-header-line {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    .card-title {
      font-size: 12px;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .hero-number {
      font-size: 24px;
      font-weight: 800;
      letter-spacing: -0.02em;
      margin-bottom: 4px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    }
    .hero-subtitle {
      font-size: 12px;
      color: var(--text-muted);
      margin-bottom: 12px;
    }
    .meta-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 6px 0;
      font-size: 12px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .meta-row:last-child { border: none; }
    .meta-val {
      font-family: "SF Mono", Monaco, Menlo, Consolas, monospace;
      color: var(--text);
      font-weight: 600;
    }

    /* Emergency Control Banner */
    .emergency-banner {
      background: #0f172a;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }
    .emergency-banner.tripped {
      border-color: #ef4444;
      background: rgba(239, 68, 68, 0.08);
    }
    .action-buttons {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    button {
      background: var(--accent);
      color: #070b14;
      border: none;
      padding: 9px 16px;
      border-radius: 8px;
      font-weight: 700;
      font-size: 13px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      transition: all 0.15s ease;
    }
    button:hover { filter: brightness(1.15); transform: translateY(-1px); }
    button:active { transform: translateY(0); }
    .btn-danger {
      background: #dc2626 !important;
      color: white !important;
    }
    .btn-success {
      background: #10b981 !important;
      color: white !important;
    }
    .btn-purple {
      background: #8b5cf6 !important;
      color: white !important;
    }
    .btn-secondary {
      background: #1e293b !important;
      color: #e2e8f0 !important;
      border: 1px solid #334155 !important;
    }
    .btn-secondary:hover { background: #334155 !important; }

    /* Progress bar */
    .progress-track {
      width: 100%;
      height: 6px;
      background: rgba(255, 255, 255, 0.1);
      border-radius: 9999px;
      overflow: hidden;
      margin: 8px 0;
    }
    .progress-fill {
      height: 100%;
      border-radius: 9999px;
      transition: width 0.3s ease;
    }

    /* Trades table */
    .table-container {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow: hidden;
      margin-bottom: 20px;
    }
    .table-header {
      background: #131d31;
      padding: 12px 18px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--card-border);
    }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th {
      text-align: left;
      padding: 10px 18px;
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 11px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    td {
      padding: 10px 18px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.03);
      font-family: "SF Mono", Monaco, Menlo, Consolas, monospace;
    }
    tr:last-child td { border: none; }
    .badge-buy { color: #34d399; font-weight: 700; }
    .badge-sell { color: #f87171; font-weight: 700; }

    /* Log section */
    .log-container {
      background: #050811;
      border: 1px solid var(--card-border);
      border-radius: 12px;
      overflow: hidden;
    }
    .log-header {
      background: #0f172a;
      padding: 10px 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--card-border);
      flex-wrap: wrap;
      gap: 10px;
    }
    .log-tabs { display: flex; gap: 6px; flex-wrap: wrap; }
    .log-tab {
      padding: 6px 12px;
      font-size: 12px;
      border-radius: 6px;
      background: transparent;
      color: var(--text-muted);
      cursor: pointer;
      border: 1px solid transparent;
      font-weight: 600;
    }
    .log-tab.active {
      background: #1e293b;
      color: var(--text);
      border-color: #334155;
    }
    .terminal-body {
      font-family: "SF Mono", Monaco, Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 1.6;
      padding: 16px;
      height: 420px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
      color: #e2e8f0;
    }
    .status-toast {
      padding: 8px 16px;
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 6px;
      font-size: 13px;
      display: none;
      margin-left: auto;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="title-group">
        <h1>
          <span>🌌 SiriusCLOB Executive Exchange Cockpit</span>
        </h1>
        <div class="pills" style="margin-top: 6px;">
          <span class="pill pill-blue" id="netPill">Sirius Testnet 2 (168)</span>
          <span class="pill pill-blue" id="blockPill">Block #---</span>
          <span class="pill pill-green" id="healthPill">🟢 OPERATIONAL & SOLVENT</span>
        </div>
      </div>
      <div style="display: flex; align-items: center; gap: 14px;">
        <button id="btnAdminAuth" class="btn-secondary" style="font-size: 12px;" onclick="promptAdminKey()">
          🔑 Admin Auth
        </button>
        <button class="btn-secondary" style="font-size: 12px;" onclick="openWebWallet()">
          🚀 Open Web Wallet Exchange UI ↗
        </button>
        <div id="liveClock" style="font-family: monospace; font-size: 13px; color: var(--text-muted);"></div>
      </div>
    </header>

    <!-- Emergency Controls & Circuit Breaker Center -->
    <div class="emergency-banner" id="emergencyBanner">
      <div>
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 4px;">
          <span style="font-size: 16px;" id="cbIcon">🛡️</span>
          <strong style="font-size: 15px;" id="cbTitle">Circuit Breaker: INACTIVE (Normal Trading Active)</strong>
        </div>
        <div style="font-size: 12px; color: var(--text-muted);" id="cbSubtitle">
          All customer order acceptance, crossing settlements, and vault custody operations are online and protected.
        </div>
      </div>
      <div class="action-buttons">
        <button class="btn-danger" id="btnHalt" onclick="confirmEmergencyHalt()">
          🚨 EMERGENCY HALT EXCHANGE
        </button>
        <button class="btn-success" id="btnResume" onclick="confirmResumeTrading()" style="display: none;">
          🟢 RESUME TRADING
        </button>
        <button class="btn-secondary" onclick="triggerAction('/api/clob/batch/flush')">
          ⚡ Flush On-Chain Batch
        </button>
        <button class="btn-purple" id="btnMmToggle" onclick="toggleMarketMaker()">
          🤖 Market Maker Bot
        </button>
        <button class="btn-secondary" onclick="confirmRestartStack()">
          🔄 Restart Stack
        </button>
        <button class="btn-secondary" onclick="confirmStopStack()">
          ⏹️ Stop Stack
        </button>
        <div id="actionToast" class="status-toast"></div>
      </div>
    </div>

    <!-- Critical Exchange Accounts & Solvency Reserves (Unified Table) -->
    <div class="table-container" style="margin-bottom: 24px;">
      <div class="table-header">
        <div style="display: flex; align-items: center; gap: 10px;">
          <span style="font-weight: 700; font-size: 14px; color: #f8fafc;">🏛️ CRITICAL EXCHANGE ACCOUNTS &amp; SOLVENCY RESERVES</span>
          <span class="pill pill-green" id="vaultSolventPill">100% SOLVENT</span>
        </div>
        <span style="font-size: 12px; color: var(--text-muted);">Real-time on-chain verifiable ledger state</span>
      </div>
      <div style="overflow-x: auto;">
        <table>
          <thead>
            <tr>
              <th style="width: 22%;">Account / Role</th>
              <th style="width: 12%;">Network</th>
              <th style="width: 20%;">Account Address</th>
              <th style="width: 12%; text-align: right;">XPX Balance</th>
              <th style="width: 12%; text-align: right;">USDT Balance</th>
              <th style="width: 10%; text-align: right;">BNB Balance</th>
              <th style="width: 14%; text-align: center;">Status / Solvency</th>
              <th style="width: 8%; text-align: center;">Action</th>
            </tr>
          </thead>
          <tbody id="accountsTableBody">
            <!-- Row 1: Autonomous SuperContract Escrow -->
            <tr>
              <td>
                <div style="font-weight: 700; color: #f8fafc; display: flex; align-items: center; gap: 6px;">
                  <span>🛡️ Autonomous SuperContract Escrow</span>
                </div>
                <div style="font-size: 11px; color: #34d399;">Zero Custodial Risk (3/3 BFT WASM Consensus)</div>
              </td>
              <td><span class="pill pill-green" style="font-size: 10px;">Sirius L1 (168)</span></td>
              <td>
                <span id="vaultAddr" style="font-size: 12px; color: #38bdf8; font-family: monospace; letter-spacing: 0.02em;">Loading...</span>
              </td>
              <td style="text-align: right;">
                <span id="vaultXpx" style="color: #38bdf8; font-weight: 700; font-size: 13px;">--- XPX</span>
              </td>
              <td style="text-align: right;">
                <span id="vaultUsdt" style="color: #34d399; font-weight: 700; font-size: 13px;">--- USDT</span>
              </td>
              <td style="text-align: right;">
                <span id="vaultBnb" style="color: #f59e0b; font-weight: 700; font-size: 13px;">--- BNB</span>
              </td>
              <td style="text-align: center;">
                <span class="pill pill-green" id="vaultXpxCoverage" style="font-size: 10px;">100% SOLVENT</span>
              </td>
              <td style="text-align: center;">
                <a id="vaultExplorerLink" href="#" target="_blank" style="color: var(--accent); text-decoration: none; font-size: 11px; font-weight: 600;">Explorer ↗</a>
              </td>
            </tr>

            <!-- Row 2: Protocol Fee Collector -->
            <tr>
              <td>
                <div style="font-weight: 700; color: #f8fafc; display: flex; align-items: center; gap: 6px;">
                  <span>💰 Protocol Fee Collector</span>
                </div>
                <div style="font-size: 11px; color: var(--text-muted);">10 bps Maker/Taker Fee Accumulator</div>
              </td>
              <td><span class="pill pill-blue" style="font-size: 10px;">Sirius L1 (168)</span></td>
              <td>
                <span id="feeCollectorAddr" style="font-size: 12px; color: #34d399; font-family: monospace; letter-spacing: 0.02em;">Loading...</span>
              </td>
              <td style="text-align: right;">
                <span id="feeCollectorXpx" style="color: #38bdf8; font-weight: 700; font-size: 13px;">--- XPX</span>
              </td>
              <td style="text-align: right;">
                <span id="feeCollectorUsdt" style="color: #34d399; font-weight: 700; font-size: 13px;">--- USDT</span>
              </td>
              <td style="text-align: right;">
                <span id="feeCollectorBnb" style="color: #f59e0b; font-weight: 700; font-size: 13px;">--- BNB</span>
              </td>
              <td style="text-align: center;">
                <span class="pill pill-blue" style="font-size: 10px;">Revenue Sink</span>
              </td>
              <td style="text-align: center;">
                <a id="feeExplorerLink" href="#" target="_blank" style="color: var(--accent); text-decoration: none; font-size: 11px; font-weight: 600;">Explorer ↗</a>
              </td>
            </tr>

            <!-- Row 3: Treasury Reserve -->
            <tr>
              <td>
                <div style="font-weight: 700; color: #f8fafc; display: flex; align-items: center; gap: 6px;">
                  <span>🏛️ Treasury Reserve</span>
                </div>
                <div style="font-size: 11px; color: var(--text-muted);">House Gas Buffer &amp; Cold Liquidity Backing</div>
              </td>
              <td><span class="pill pill-blue" style="font-size: 10px;">Sirius L1 (168)</span></td>
              <td>
                <span id="treasuryAddr" style="font-size: 12px; color: #a855f7; font-family: monospace; letter-spacing: 0.02em;">Loading...</span>
              </td>
              <td style="text-align: right;">
                <span id="treasuryXpx" style="color: #38bdf8; font-weight: 700; font-size: 13px;">--- XPX</span>
              </td>
              <td style="text-align: right;">
                <span id="treasuryUsdt" style="color: #34d399; font-weight: 700; font-size: 13px;">--- USDT</span>
              </td>
              <td style="text-align: right;">
                <span id="treasuryBnb" style="color: #f59e0b; font-weight: 700; font-size: 13px;">--- BNB</span>
              </td>
              <td style="text-align: center;">
                <span class="pill pill-purple" style="font-size: 10px;">House Gas Buffer</span>
              </td>
              <td style="text-align: center;">
                <a id="treasuryExplorerLink" href="#" target="_blank" style="color: var(--accent); text-decoration: none; font-size: 11px; font-weight: 600;">Explorer ↗</a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Protocol Revenue & Fee Economics Grid (4 Cards) -->
    <div class="grid-4" style="margin-bottom: 24px;">
      <div class="card" style="border-left: 3px solid #38bdf8;">
        <div class="card-title" style="display: flex; justify-content: space-between; align-items: center;">
          <span>XPX Protocol Revenue</span>
          <span class="pill pill-blue" style="font-size: 9px;">10 bps Taker</span>
        </div>
        <div class="hero-number" style="color: #38bdf8;" id="feeTotalXpx">0.0000 XPX</div>
        <div class="hero-subtitle" id="feeSubXpx">Trade: 0.0000 | Withdraw: 0.0000</div>
      </div>
      <div class="card" style="border-left: 3px solid #34d399;">
        <div class="card-title" style="display: flex; justify-content: space-between; align-items: center;">
          <span>USDT Protocol Revenue</span>
          <span class="pill pill-green" style="font-size: 9px;">1.0 USDT Fixed</span>
        </div>
        <div class="hero-number" style="color: #34d399;" id="feeTotalUsdt">0.0000 USDT</div>
        <div class="hero-subtitle" id="feeSubUsdt">Trade: 0.0000 | Withdraw: 0.0000</div>
      </div>
      <div class="card" style="border-left: 3px solid #f59e0b;">
        <div class="card-title" style="display: flex; justify-content: space-between; align-items: center;">
          <span>BNB Protocol Revenue</span>
          <span class="pill pill-amber" style="font-size: 9px;">0.001 BNB Fixed</span>
        </div>
        <div class="hero-number" style="color: #f59e0b;" id="feeTotalBnb">0.000000 BNB</div>
        <div class="hero-subtitle" id="feeSubBnb">Network Gas Surcharge Buffer</div>
      </div>
      <div class="card" style="border-left: 3px solid #a855f7;">
        <div class="card-title" style="display: flex; justify-content: space-between; align-items: center;">
          <span>Total Fee Events</span>
          <span class="pill pill-purple" style="font-size: 9px;">ACID SQLite Ledger</span>
        </div>
        <div class="hero-number" style="color: #a855f7;" id="feeTotalEvents">0 Events</div>
        <div class="hero-subtitle">Maker: 0 bps (0%) | Taker: 10 bps (0.1%)</div>
      </div>
    </div>

    <!-- Operations Counters & Consensus Health Grid (4 Metrics) -->
    <div class="grid-4">
      <div class="card">
        <div class="card-title">Active Resting Orders</div>
        <div class="hero-number" style="color: #38bdf8;" id="statActiveOrders">---</div>
        <div class="hero-subtitle">Open Bids & Asks in Orderbook</div>
      </div>
      <div class="card">
        <div class="card-title">Pending L1 Batch Settlements</div>
        <div class="hero-number" style="color: #f59e0b;" id="statPendingSettlements">0</div>
        <div class="hero-subtitle">Waiting for next on-chain commit</div>
      </div>
      <div class="card">
        <div class="card-title">3-Node BFT VM Consensus</div>
        <div class="hero-number" style="color: #34d399;" id="statBftConsensus">3/3 SYNCED</div>
        <div class="hero-subtitle" id="statStateRoot">State-Root: Loading...</div>
      </div>
      <div class="card">
        <div class="card-title">Market Maker Bot</div>
        <div class="hero-number" id="statMmStatus">IDLE</div>
        <div class="hero-subtitle" id="statMmSubtitle">Autonomous liquidity provider</div>
      </div>
    </div>

    <!-- Live Recent Trades Tape -->
    <div class="table-container">
      <div class="table-header">
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-size: 14px;">⚡</span>
          <strong style="font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em;">Live Recent Trade Tape (XPX/USDT)</strong>
        </div>
        <span style="font-size: 11px; color: var(--text-muted);">Real-Time Replicator Feed</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Market</th>
            <th>Side</th>
            <th>Execution Price</th>
            <th>Trade Amount</th>
            <th>Taker Fee</th>
          </tr>
        </thead>
        <tbody id="tradesBody">
          <tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 20px;">Loading recent trades...</td></tr>
        </tbody>
      </table>
    </div>

    <!-- Real-Time Protocol Fee Ledger Table -->
    <div class="table-container" style="margin-bottom: 24px;">
      <div class="table-header">
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-size: 14px;">💎</span>
          <strong style="font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em;">Real-Time Protocol Fee Ledger (ACID SQLite)</strong>
        </div>
        <div style="display: flex; align-items: center; gap: 8px;">
          <span class="pill pill-green" style="font-size: 10px;">Self-Sustaining Protocol</span>
          <span style="font-size: 11px; color: var(--text-muted);">Auto-Settled to Fee Collector</span>
        </div>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width: 14%;">Time</th>
            <th style="width: 16%;">Event Type</th>
            <th style="width: 10%;">Asset</th>
            <th style="width: 18%; text-align: right;">Fee Amount</th>
            <th style="width: 24%;">Reference / Order ID</th>
            <th style="width: 18%;">Caller / Account</th>
          </tr>
        </thead>
        <tbody id="feesBody">
          <tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 20px;">Loading fee ledger...</td></tr>
        </tbody>
      </table>
    </div>

    <!-- Real-Time Console Terminal -->
    <div class="log-container">
      <div class="log-header">
        <div class="log-tabs">
          <button class="log-tab active" data-tab="exchange" onclick="switchLogTab('exchange')">⚡ Exchange Relayer (:8088)</button>
          <button class="log-tab" data-tab="mm" onclick="switchLogTab('mm')">🤖 Market Maker Bot</button>
          <button class="log-tab" data-tab="node1" onclick="switchLogTab('node1')">Node 1 VM (:5051)</button>
          <button class="log-tab" data-tab="node2" onclick="switchLogTab('node2')">Node 2 VM (:5052)</button>
          <button class="log-tab" data-tab="node3" onclick="switchLogTab('node3')">Node 3 VM (:5053)</button>
          <button class="log-tab" data-tab="actions" onclick="switchLogTab('actions')">🏆 Action Output</button>
        </div>
        <div style="display: flex; gap: 8px; align-items: center;">
          <span id="scrollIndicator" style="font-size: 11px; color: #10b981;">🟢 Auto-Scroll ON</span>
          <button class="btn-secondary" style="padding: 4px 8px; font-size: 11px;" onclick="scrollToBottom()">⬇ Bottom</button>
          <button class="btn-secondary" style="padding: 4px 8px; font-size: 11px;" onclick="clearLogs()">Clear</button>
        </div>
      </div>
      <div class="terminal-body" id="terminal" style="scroll-behavior: auto;">Loading exchange logs...</div>
    </div>
  </div>

  <script>
    function esc(s) {
      if (s === null || s === undefined) return '';
      return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }
    let activeNode = 'exchange';
    let autoScroll = true;
    let lastLogByNode = {};
    let actionHistoryLog = 'Operator action log initialized.\\n';
    let isMmActive = false;
    const terminal = document.getElementById('terminal');
    const toast = document.getElementById('actionToast');
    const scrollIndicator = document.getElementById('scrollIndicator');

    terminal.addEventListener('scroll', () => {
      const distanceFromBottom = terminal.scrollHeight - terminal.scrollTop - terminal.clientHeight;
      autoScroll = distanceFromBottom < 35;
      updateScrollIndicator();
    });

    function updateScrollIndicator() {
      if (autoScroll) {
        scrollIndicator.textContent = '🟢 Auto-Scroll ON';
        scrollIndicator.style.color = '#10b981';
      } else {
        scrollIndicator.textContent = '⏸️ Paused (Reading History)';
        scrollIndicator.style.color = '#f59e0b';
      }
    }

    function scrollToBottom() {
      autoScroll = true;
      terminal.scrollTop = terminal.scrollHeight;
      updateScrollIndicator();
    }

    function updateClock() {
      document.getElementById('liveClock').textContent = new Date().toLocaleTimeString();
    }
    setInterval(updateClock, 1000);
    updateClock();

    async function fetchOverview() {
      try {
        const res = await fetch('/api/admin/overview');
        const json = await res.json();
        if (json.ok && json.data) {
          renderOverview(json.data);
        }
      } catch (e) {
        console.error('Overview fetch error:', e);
      }
    }

    function renderOverview(d) {
      // Header pills
      document.getElementById('blockPill').textContent = 'Block #' + (d.l1Height || '---');
      
      // Circuit Breaker state
      const banner = document.getElementById('emergencyBanner');
      const cbIcon = document.getElementById('cbIcon');
      const cbTitle = document.getElementById('cbTitle');
      const cbSub = document.getElementById('cbSubtitle');
      const btnHalt = document.getElementById('btnHalt');
      const btnResume = document.getElementById('btnResume');
      const healthPill = document.getElementById('healthPill');

      if (d.circuitBreaker.tripped) {
        banner.classList.add('tripped');
        cbIcon.textContent = '🚨';
        cbTitle.textContent = 'EXCHANGE CIRCUIT BREAKER TRIPPED (TRADING HALTED)';
        cbTitle.style.color = '#ef4444';
        cbSub.textContent = 'Reason: ' + (d.circuitBreaker.reason || 'Manual emergency stop triggered by operator');
        btnHalt.style.display = 'none';
        btnResume.style.display = 'inline-flex';
        healthPill.className = 'pill pill-red';
        healthPill.textContent = '🚨 EMERGENCY HALTED';
      } else {
        banner.classList.remove('tripped');
        cbIcon.textContent = '🛡️';
        cbTitle.textContent = 'Circuit Breaker: INACTIVE (Trading Active)';
        cbTitle.style.color = '#f8fafc';
        cbSub.textContent = 'All customer order acceptance, crossing settlements, and vault custody operations are online and protected.';
        btnHalt.style.display = 'inline-flex';
        btnResume.style.display = 'none';
        healthPill.className = 'pill pill-green';
        healthPill.textContent = '🟢 100% SOLVENT & OPERATIONAL';
      }

      // Vault Row
      document.getElementById('vaultXpx').textContent = d.accounts.vault.xpx + ' XPX';
      document.getElementById('vaultAddr').textContent = d.accounts.vault.address;
      document.getElementById('vaultAddr').title = d.accounts.vault.address;
      document.getElementById('vaultUsdt').textContent = d.accounts.vault.usdt + ' USDT';
      document.getElementById('vaultBnb').textContent = d.accounts.vault.bnb + ' BNB';
      const covEl = document.getElementById('vaultXpxCoverage');
      if (covEl) {
        covEl.textContent = d.accounts.vault.xpxCoverage + '% SOLVENT (' + d.accounts.vault.xpxLiabilities + ' Liab)';
        covEl.className = d.accounts.vault.solvent ? 'pill pill-green' : 'pill pill-red';
      }
      document.getElementById('vaultExplorerLink').href = 'https://proximax-foundry.github.io/web-explorer-vuejs/#/account/' + d.accounts.vault.address;

      // Fee Collector Row
      document.getElementById('feeCollectorXpx').textContent = d.accounts.feeCollector.xpx + ' XPX';
      document.getElementById('feeCollectorAddr').textContent = d.accounts.feeCollector.address;
      document.getElementById('feeCollectorAddr').title = d.accounts.feeCollector.address;
      document.getElementById('feeCollectorUsdt').textContent = d.accounts.feeCollector.usdt + ' USDT';
      const fcb = document.getElementById('feeCollectorBnb');
      if (fcb) {
        fcb.textContent = (d.accounts.feeCollector.bnb || '0.000000') + ' BNB';
      }
      document.getElementById('feeExplorerLink').href = 'https://proximax-foundry.github.io/web-explorer-vuejs/#/account/' + d.accounts.feeCollector.address;

      // Treasury Row
      document.getElementById('treasuryXpx').textContent = d.accounts.treasury.xpx + ' XPX';
      document.getElementById('treasuryAddr').textContent = d.accounts.treasury.address;
      document.getElementById('treasuryAddr').title = d.accounts.treasury.address;
      document.getElementById('treasuryUsdt').textContent = d.accounts.treasury.usdt + ' USDT';
      document.getElementById('treasuryBnb').textContent = d.accounts.treasury.bnb + ' BNB';
      document.getElementById('treasuryExplorerLink').href = 'https://proximax-foundry.github.io/web-explorer-vuejs/#/account/' + d.accounts.treasury.address;

      // Operations stats
      document.getElementById('statActiveOrders').textContent = d.stats.activeOrders;
      document.getElementById('statPendingSettlements').textContent = d.stats.pendingSettlements;
      document.getElementById('statBftConsensus').textContent = d.stats.bftSynced ? '3/3 SYNCED' : 'CONSENSUS DRIFT';
      document.getElementById('statBftConsensus').style.color = d.stats.bftSynced ? '#34d399' : '#ef4444';
      document.getElementById('statStateRoot').textContent = 'Root: ' + d.stats.bftStateRoot;

      isMmActive = d.stats.marketMakerRunning;
      const mmStat = document.getElementById('statMmStatus');
      const mmBtn = document.getElementById('btnMmToggle');
      if (isMmActive) {
        mmStat.textContent = 'RUNNING';
        mmStat.style.color = '#34d399';
        mmBtn.textContent = '⏹️ Stop Market Maker Bot';
        mmBtn.className = 'btn-secondary';
      } else {
        mmStat.textContent = 'IDLE';
        mmStat.style.color = '#94a3b8';
        mmBtn.textContent = '🤖 Start Market Maker Bot';
        mmBtn.className = 'btn-purple';
      }

      // Recent Trades Table
      const tb = document.getElementById('tradesBody');
      if (d.recentTrades && d.recentTrades.length > 0) {
        tb.innerHTML = d.recentTrades.map(t => \`
          <tr>
            <td style="color: var(--text-muted); font-variant-numeric: tabular-nums;" title="\${esc(t.timestamp ? new Date(t.timestamp).toLocaleString() : '')}">\${esc(t.time)}</td>
            <td style="font-weight: 700;">\${esc(t.pair)}</td>
            <td class="\${t.side === 'Buy' ? 'badge-buy' : 'badge-sell'}">\${esc((t.side || '').toUpperCase())}</td>
            <td>$\${esc(t.price)}</td>
            <td>\${esc(t.amount)} XPX</td>
            <td style="color: var(--text-muted);">\${esc(t.fee)} XPX</td>
          </tr>
        \`).join('');
      } else {
        tb.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 20px;">No trades recorded yet.</td></tr>';
      }

      // Protocol Fees
      if (d.fees && d.fees.total) {
        const elTotalXpx = document.getElementById('feeTotalXpx');
        if (elTotalXpx) elTotalXpx.textContent = d.fees.total.xpx + ' XPX';
        const elTotalUsdt = document.getElementById('feeTotalUsdt');
        if (elTotalUsdt) elTotalUsdt.textContent = d.fees.total.usdt + ' USDT';
        const elTotalBnb = document.getElementById('feeTotalBnb');
        if (elTotalBnb) elTotalBnb.textContent = d.fees.total.bnb + ' BNB';
        const elTotalEvents = document.getElementById('feeTotalEvents');
        if (elTotalEvents) elTotalEvents.textContent = d.fees.total.count + ' Events';

        if (d.fees.byType) {
          const tXpx = d.fees.byType.trade ? d.fees.byType.trade.xpx : '0.0000';
          const wXpx = d.fees.byType.withdrawal ? d.fees.byType.withdrawal.xpx : '0.0000';
          const elSubXpx = document.getElementById('feeSubXpx');
          if (elSubXpx) elSubXpx.textContent = 'Trade: ' + tXpx + ' | Withdraw: ' + wXpx;

          const tUsdt = d.fees.byType.trade ? d.fees.byType.trade.usdt : '0.0000';
          const wUsdt = d.fees.byType.withdrawal ? d.fees.byType.withdrawal.usdt : '0.0000';
          const elSubUsdt = document.getElementById('feeSubUsdt');
          if (elSubUsdt) elSubUsdt.textContent = 'Trade: ' + tUsdt + ' | Withdraw: ' + wUsdt;
        }

        // Fee Ledger Table Rows
        const fb = document.getElementById('feesBody');
        if (fb) {
          if (d.fees.recent && d.fees.recent.length > 0) {
            fb.innerHTML = d.fees.recent.map(f => \`
              <tr>
                <td style="color: var(--text-muted); font-variant-numeric: tabular-nums;" title="\${esc(f.timestamp ? new Date(f.timestamp).toLocaleString() : '')}">\${esc(f.time)}</td>
                <td>
                  <span class="pill \${f.type === 'WITHDRAWAL' ? 'pill-purple' : (f.type === 'TRADE_TAKER' ? 'pill-blue' : 'pill-green')}" style="font-size: 10px;">
                    \${esc(f.type)}
                  </span>
                </td>
                <td style="font-weight: 700;">\${esc(f.mosaic)}</td>
                <td style="text-align: right; color: #38bdf8; font-weight: 700;">+\${esc(f.amount)} \${esc(f.mosaic)}</td>
                <td style="color: var(--text-muted); font-family: monospace; font-size: 11px;">\${esc(f.referenceId || '—')}</td>
                <td style="color: var(--text-muted); font-family: monospace; font-size: 11px;">\${esc(f.callerKey || 'Exchange')}</td>
              </tr>
            \`).join('');
          } else {
            fb.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-muted); padding: 20px;">No fees collected yet.</td></tr>';
          }
        }
      }
    }

    async function fetchLogs(forceScroll = false) {
      if (activeNode === 'actions') {
        if (terminal.textContent !== actionHistoryLog) {
          const prevTop = terminal.scrollTop;
          terminal.textContent = actionHistoryLog;
          terminal.scrollTop = (autoScroll || forceScroll) ? terminal.scrollHeight : prevTop;
        }
        return;
      }
      try {
        const res = await fetch(\`/api/logs?node=\${activeNode}&lines=500\`);
        const text = await res.text();
        if (lastLogByNode[activeNode] === text && !forceScroll) {
          return;
        }
        lastLogByNode[activeNode] = text;
        const prevScrollTop = terminal.scrollTop;
        terminal.textContent = text;
        if (autoScroll || forceScroll) {
          terminal.scrollTop = terminal.scrollHeight;
        } else {
          terminal.scrollTop = prevScrollTop;
        }
      } catch (e) {}
    }

    function switchLogTab(node) {
      activeNode = node;
      document.querySelectorAll('.log-tab').forEach(t => {
        t.classList.toggle('active', t.getAttribute('data-tab') === node);
      });
      lastLogByNode[activeNode] = null;
      fetchLogs(true);
    }

    function clearLogs() {
      if (activeNode === 'actions') {
        actionHistoryLog = '';
      }
      terminal.textContent = '';
    }

    function openWebWallet() {
      const isLocal = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
      const target = isLocal
        ? 'http://' + window.location.hostname + ':3030/#/clob-exchange'
        : 'https://web-wallet-exchange.igorgoc.workers.dev/#/clob-exchange';
      window.open(target, '_blank');
    }

    function updateAdminAuthBadge() {
      const btn = document.getElementById('btnAdminAuth');
      if (!btn) return;
      const key = localStorage.getItem('clob_admin_key');
      if (key && key.trim()) {
        btn.textContent = '🛡️ Admin: Authenticated';
        btn.style.color = '#34d399';
        btn.style.borderColor = 'rgba(16, 185, 129, 0.4)';
        btn.title = 'Click to change or clear Admin Key';
      } else {
        btn.textContent = '🔑 Admin Auth';
        btn.style.color = '';
        btn.style.borderColor = '';
        btn.title = 'Click to set Admin Key';
      }
    }

    function promptAdminKey() {
      const current = localStorage.getItem('clob_admin_key') || '';
      const entered = prompt(
        '🔑 SiriusCLOB Admin Key Authentication\\n\\nEnter your cluster admin key (CLOB_ADMIN_KEY) to authorize operator controls:\\n(Leave blank to clear stored key)',
        current ? '********' : ''
      );
      if (entered === null) return;
      if (entered.trim() === '' || entered === '********') {
        if (entered.trim() === '') {
          localStorage.removeItem('clob_admin_key');
          updateAdminAuthBadge();
          alert('Admin key cleared.');
        }
        return;
      }
      localStorage.setItem('clob_admin_key', entered.trim());
      updateAdminAuthBadge();
      alert('Admin key saved.');
    }

    async function triggerAction(endpoint, payload = null) {
      toast.style.display = 'block';
      toast.textContent = '⏳ Executing action...';
      try {
        let adminKey = localStorage.getItem('clob_admin_key');
        if (!adminKey) {
          adminKey = prompt('🔑 Admin Authentication Required\\n\\nEnter the CLOB Admin Key to authorize this administrative action:');
          if (adminKey && adminKey.trim()) {
            adminKey = adminKey.trim();
            localStorage.setItem('clob_admin_key', adminKey);
            updateAdminAuthBadge();
          } else {
            toast.textContent = '❌ Action cancelled: Admin key required';
            setTimeout(() => toast.style.display = 'none', 3000);
            return;
          }
        }

        const opts = {
          method: 'POST',
          headers: {
            'X-Admin-Key': adminKey,
          }
        };
        if (payload) {
          opts.headers['Content-Type'] = 'application/json';
          opts.body = JSON.stringify(payload);
        }
        const res = await fetch(endpoint, opts);
        const data = await res.json();
        if (res.status === 401 || res.status === 403) {
          localStorage.removeItem('clob_admin_key');
          updateAdminAuthBadge();
          toast.textContent = '🔒 ' + (data.error || 'Admin authentication failed: Invalid admin key');
          setTimeout(() => toast.style.display = 'none', 4000);
          return;
        }
        toast.textContent = data.success ? (data.message || '✅ Action completed successfully') : ('⚠️ ' + (data.error || 'Action failed'));
        if (data.output || data.message) {
          const ts = new Date().toLocaleTimeString();
          actionHistoryLog += \`\\n=== [\${ts}] \${endpoint} ===\\n\` + (data.message || '') + (data.output || '');
          switchLogTab('actions');
          scrollToBottom();
        }
        fetchOverview();
        setTimeout(() => toast.style.display = 'none', 4000);
      } catch (e) {
        toast.textContent = '❌ Action failed: ' + e.message;
      }
    }

    function confirmEmergencyHalt() {
      if (confirm('🚨 ARE YOU SURE YOU WANT TO EMERGENCY HALT THE EXCHANGE?\\n\\nThis will immediately trip the circuit breaker and freeze all order matching, batch settlements, and hot vault withdrawals.')) {
        triggerAction('/api/action/emergency-halt');
      }
    }

    function confirmResumeTrading() {
      if (confirm('🟢 Resume normal exchange operations and reset the circuit breaker?')) {
        triggerAction('/api/action/resume-trading');
      }
    }

    function confirmRestartStack() {
      if (confirm('Restart all services (Web Wallet, Relayer, and SuperContract VMs)?')) {
        triggerAction('/api/action/restart');
      }
    }

    function confirmStopStack() {
      if (confirm('Stop all exchange services?')) {
        triggerAction('/api/action/stop');
      }
    }

    function toggleMarketMaker() {
      if (isMmActive) {
        triggerAction('/api/clob/mm/stop');
      } else {
        triggerAction('/api/clob/mm/start');
      }
    }

    // Polling intervals
    setInterval(fetchOverview, 2500);
    setInterval(() => fetchLogs(false), 2500);
    updateAdminAuthBadge();
    fetchOverview();
    fetchLogs(true);
  </script>
</body>
</html>`;
}

function reconcileHistoricalFees() {
  try {
    const summary = clobDb.getCollectedFeesSummary();
    if (summary && summary.total && summary.total.count > 0) {
      console.log(`[FEE_ENGINE] Fee ledger populated with ${summary.total.count} events (${summary.total.xpx} XPX, ${summary.total.usdt} USDT).`);
      return;
    }
    const tradesPath = path.join(CLUSTER_DIR, 'node1', 'data', 'drives', 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23', 'drive', 'markets', 'xpx_usdt', 'trades.json');
    if (!fs.existsSync(tradesPath)) return;
    const trades = JSON.parse(fs.readFileSync(tradesPath, 'utf8'));
    let count = 0;
    for (let i = 0; i < trades.length; i++) {
      const t = trades[i];
      const feeAtomic = BigInt(t.taker_fee || '0');
      if (feeAtomic > 0n) {
        clobDb.recordCollectedFee({
          feeType: 'TRADE_TAKER',
          mosaic: 'XPX',
          mosaicId: '13bfc518e40549d7',
          amount: (Number(feeAtomic) / 1e6).toFixed(6),
          amountAtomic: feeAtomic.toString(),
          referenceId: `trade_hist_${i}_${t.taker_order_id || 'fill'}`,
          callerKey: t.taker_account || null,
        });
        count++;
      }
    }
    console.log(`[FEE_ENGINE] Auto-reconciled and backfilled ${count} historical trade fee events into collected_fees table.`);
  } catch (err) {
    console.warn('[FEE_ENGINE] Warning reconciling historical fees:', err.message);
  }
}

const HOST = process.env.HOST || '0.0.0.0';
server.listen(PORT, HOST, () => {
  console.log(`==================================================================`);
  console.log(`🚀 Sirius Devnet Cockpit Dashboard running at:`);
  console.log(`👉 http://${HOST}:${PORT}`);
  console.log(`==================================================================`);

  // Ensure historical fees are reconciled in SQLite collected_fees table
  reconcileHistoricalFees();

  // Auto-start embedded 3-Node BFT WASM VM Executors if not already running on ports 5051..5053
  try {
    const { startVmServer } = require('../executor/grpc_vm_server');
    const vmNodes = [
      { name: 'node1', port: 5051 },
      { name: 'node2', port: 5052 },
      { name: 'node3', port: 5053 },
    ];
    for (const n of vmNodes) {
      startVmServer(n.port, n.name)
        .then(() => console.log(`[BFT Engine] Embedded ${n.name} listening on :${n.port}`))
        .catch((err) => {
          if (err.message && (err.message.includes('EADDRINUSE') || err.message.includes('address already in use'))) {
            console.log(`[BFT Engine] ${n.name} already active on :${n.port}`);
          } else {
            console.warn(`[BFT Engine] Could not start embedded ${n.name}:`, err.message);
          }
        });
    }
  } catch (vmInitErr) {
    console.warn('[BFT Engine] Embedded VM startup skipped:', vmInitErr.message);
  }

  // Start autonomous BSC Testnet block watcher & relayer
  swapApi.startAutoRelayer();

  // Sync and periodically cache real on-chain vault balances from Sirius Testnet 2
  clobApi.syncVaultBalancesFromTestnet2();
  setInterval(() => clobApi.syncVaultBalancesFromTestnet2(), 15000);

  // Start Continuous 24/7 Solvency Sentinel & Stale Order TTL Sweeper
  clobApi.startSolvencySentinel(30000);
  clobApi.startOrderTtlSweeper(15000);

  // Start autonomous Sirius Testnet 2 batch settlement processor
  defaultBatchSettler.startPeriodic(15000, 1);

  // Bootstrap Level-2 BFT Validator Cluster (2-of-3 Multisig) & start Smart Trading Account L1 Reconciler
  setTimeout(() => {
    tradingCosigner.ensureValidatorClusterOnChain().catch((err) => {
      console.warn('[TradingCosigner] Level-2 cluster bootstrap notice:', err.message);
    });
  }, 3000);
  setInterval(() => {
    tradingCosigner.reconcileInFlightL1Txs().catch(() => {});
  }, 8000);

  // Start autonomous Sirius Testnet 2 on-chain deposit indexer
  try {
    const testnetClient = require('./testnet2_client');
    const depositIndexer = require('./deposit_indexer');
    const vault = testnetClient.getOrInitVaultAccount();
    depositIndexer.startDepositIndexer(vault.address.plain(), async (dep) => {
      try {
        clobApi.creditVerifiedDeposit(dep);
      } catch (err) {
        console.error('[Deposit Indexer] Failed to credit deposit:', err.message);
      }
    });
  } catch (err) {
    console.warn('[Deposit Indexer] Could not start auto indexer:', err.message);
  }

  // Start autonomous PancakeSwap AMM Liquidity Mirror Market Maker
  if (process.env.ENABLE_AUTONOMOUS_MM !== 'false') {
    try {
      const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
      let shouldStart = true;
      if (fs.existsSync(mmPidFile)) {
        try {
          const oldPid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
          process.kill(oldPid, 0);
          shouldStart = false; // already running
        } catch (_) {}
      }
      if (shouldStart) {
        const logFile = fs.openSync(path.join(CLUSTER_DIR, 'mm.log'), 'a');
        const child = spawn('node', [path.join(ROOT_DIR, 'scripts/clob_market_maker.js'), '--daemon'], {
          detached: true,
          stdio: ['ignore', logFile, logFile],
          cwd: ROOT_DIR
        });
        child.unref();
        fs.writeFileSync(mmPidFile, String(child.pid));
        console.log(`[Market Maker] Autonomous PancakeSwap Liquidity Mirror started (PID ${child.pid})`);
      }
    } catch (err) {
      console.warn('[Market Maker] Could not auto-start MM bot:', err.message);
    }
  }

  // Autonomous 24/7 Liquidity Watchdog: Guarantees order books are ALWAYS filled on both pairs
  let isWatchdogReseeding = false;
  setInterval(() => {
    try {
      if (isWatchdogReseeding) return;
      const mmPidFile = path.join(CLUSTER_DIR, 'mm.pid');
      let isDaemonAlive = false;
      if (fs.existsSync(mmPidFile)) {
        try {
          const pid = parseInt(fs.readFileSync(mmPidFile, 'utf8').trim(), 10);
          process.kill(pid, 0);
          isDaemonAlive = true;
        } catch (_) {}
      }
      // If the autonomous MM bot is already running, it manages its own continuous depth!
      if (isDaemonAlive) {
        return;
      }

      const usdtDepth = clobApi.getMarketDepthSnapshot('xpx_usdt');
      const bnbDepth = clobApi.getMarketDepthSnapshot('xpx_bnb');
      const usdtDepleted = !usdtDepth || !Array.isArray(usdtDepth.bids) || usdtDepth.bids.length < 2 || !Array.isArray(usdtDepth.asks) || usdtDepth.asks.length < 2;
      const bnbDepleted = !bnbDepth || !Array.isArray(bnbDepth.bids) || bnbDepth.bids.length < 2 || !Array.isArray(bnbDepth.asks) || bnbDepth.asks.length < 2;
      if (usdtDepleted || bnbDepleted) {
        console.log('[LiquidityWatchdog] Depleted depth detected and no MM active. Triggering autonomous reseed step...');
        isWatchdogReseeding = true;
        execFile('node', [path.join(ROOT_DIR, 'scripts/clob_market_maker.js'), '--step'], { maxBuffer: 10 * 1024 * 1024 }, (err) => {
          isWatchdogReseeding = false;
          if (err) console.warn('[LiquidityWatchdog] Reseed step error:', err.message);
          else console.log('[LiquidityWatchdog] Autonomous reseed step successfully completed.');
        });
      }
    } catch (err) {
      console.warn('[LiquidityWatchdog] Check error:', err.message);
    }
  }, 12000);
});
