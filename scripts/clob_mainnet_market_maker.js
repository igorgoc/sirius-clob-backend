#!/usr/bin/env node
'use strict';

/**
 * SiriusCLOB — Production Non-Custodial Mainnet Smart-Trading Market Maker
 *
 * Security & Architecture Guarantees:
 *   - 100% Non-Custodial: Operates strictly through a real funded 2-of-2 Smart Trading Account
 *     via public Ed25519-authenticated HTTP endpoints. ZERO direct VM/gRPC access, ZERO vault
 *     access, ZERO synthetic order-book injection or self-crossing.
 *   - Fail-Closed Transport & Network Verification: Enforces HTTPS on Mainnet (networkType 184)
 *     and validates /api/clob/config networkType & generationHash before signing anything.
 *   - Dead-Man's Switch (Cancel-on-Disconnect): Registers signed COD heartbeat BEFORE quoting,
 *     refreshes periodically, and on clean shutdown cancels all quotes before deregistering COD.
 *   - Fixed-Point BigInt Quoting Math: Anchored to fresh, non-fallback PancakeSwap oracle mid
 *     with spike/jump rejection, strict tick rounding (bids down, asks up), inventory caps,
 *     reserve protection, and PostOnly execution.
 *   - Zero-Trust Auto-Cosigner: Independently parses AggregateComplete inner transfers and
 *     cryptographically verifies the bot's own Ed25519 signed order + cumulative partial-fill
 *     bounds before cosigning any matched L1 settlement.
 *
 * Usage:
 *   MM_OWNER_PRIVATE_KEY=<64-hex> CLOB_API_URL=https://... node scripts/clob_mainnet_market_maker.js --once
 *   MM_KEY_FILE=/path/to/mm.key   CLOB_API_URL=https://... node scripts/clob_mainnet_market_maker.js --daemon
 *   ... --dry-run (computes & logs quotes without submitting orders or cosigning)
 */

const fs = require('fs');
const crypto = require('crypto');
const sdk = require('tsjs-xpx-chain-sdk');

const {
  Account,
  Address,
  CosignatureTransaction,
  TransactionMapping,
  TransactionType,
  KeyPair,
  DerivationScheme,
} = sdk;

const PRICE_SCALE = 100_000_000n;
const AMOUNT_SCALE = 1_000_000n;

// =========================================================================
// Pure Fixed-Point & Cryptographic Helper Utilities (Exported for Testing)
// =========================================================================

function hexToUint8(hex) {
  const clean = String(hex || '').replace(/^0x/i, '').trim();
  if (!/^[0-9a-fA-F]*$/.test(clean) || clean.length % 2 !== 0) {
    throw new Error('Invalid hex string');
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function normalizePair(pair) {
  const clean = String(pair || 'XPX/USDT').trim().toUpperCase().replace('_', '/');
  if (clean === 'XPX/BNB' || clean === 'XPXBNB') return 'XPX/BNB';
  return 'XPX/USDT';
}

function normalizeAddr(addr) {
  return String(addr || '').replace(/[- ]/g, '').trim().toUpperCase();
}

function parseDecBigInt(val, decimals) {
  const s = String(val ?? '0').trim();
  if (!/^\d+(\.\d+)?$/.test(s)) return 0n;
  const [intPart, fracPart = ''] = s.split('.');
  const fracPadded = (fracPart + '0'.repeat(decimals)).slice(0, decimals);
  return BigInt(intPart || '0') * (10n ** BigInt(decimals)) + BigInt(fracPadded || '0');
}

function formatFixedBigInt(valBigInt, scaleDecimals, outDecimals = scaleDecimals, trimTrailingZeros = false) {
  const v = BigInt(valBigInt);
  if (v < 0n) throw new Error('Cannot format negative fixed-point value');
  const factor = 10n ** BigInt(scaleDecimals);
  const intPart = v / factor;
  const fracRaw = (v % factor).toString().padStart(scaleDecimals, '0');
  let frac = fracRaw.slice(0, outDecimals).padEnd(outDecimals, '0');
  if (trimTrailingZeros) {
    frac = frac.replace(/0+$/, '');
  }
  return frac.length > 0 ? `${intPart.toString()}.${frac}` : intPart.toString();
}

function formatTicks(ticksBigInt, priceDecimals) {
  return formatFixedBigInt(ticksBigInt, 8, priceDecimals, false);
}

function formatAtomic(atomicBigInt, decimals = 6) {
  return formatFixedBigInt(atomicBigInt, decimals, decimals, true);
}

function numberToPriceTicks(num) {
  if (typeof num !== 'number' || !Number.isFinite(num) || num <= 0) {
    return 0n;
  }
  return parseDecBigInt(num.toFixed(8), 8);
}

/**
 * Validate PancakeSwap oracle payload against freshness, fallback, positivity, and jump guards.
 */
function validateOracleData(oracle, {
  now = Date.now(),
  maxAgeMs = 60_000,
  prevMidByPair = null,
  maxJumpRatio = 0.20,
} = {}) {
  if (!oracle || !oracle.ok) {
    return { ok: false, error: 'Oracle response missing or not ok' };
  }
  if (oracle.isFallback) {
    return { ok: false, error: 'Oracle returned static fallback price (refusing to quote)' };
  }
  if (oracle.stale) {
    return { ok: false, error: 'Oracle marked stale (refusing to quote)' };
  }
  const ts = Number(oracle.timestamp || 0);
  if (!Number.isFinite(ts) || ts <= 0) {
    return { ok: false, error: 'Oracle missing valid timestamp' };
  }
  const ageMs = now - ts;
  if (ageMs > maxAgeMs) {
    return { ok: false, error: `Oracle price too old (${Math.round(ageMs / 1000)}s > ${Math.round(maxAgeMs / 1000)}s)` };
  }
  if (ageMs < -60_000) {
    return { ok: false, error: `Oracle timestamp is suspiciously far in the future (${ageMs}ms)` };
  }

  const priceUsdt = Number(oracle.priceUsdt);
  const priceBnb = Number(oracle.priceBnb);
  if (!Number.isFinite(priceUsdt) || priceUsdt <= 0 || !Number.isFinite(priceBnb) || priceBnb <= 0) {
    return { ok: false, error: `Oracle returned invalid prices (usdt=${oracle.priceUsdt}, bnb=${oracle.priceBnb})` };
  }

  if (prevMidByPair && typeof prevMidByPair === 'object') {
    const prevUsdt = Number(prevMidByPair['XPX/USDT'] || 0);
    if (prevUsdt > 0) {
      const diff = Math.abs(priceUsdt - prevUsdt) / prevUsdt;
      if (diff > maxJumpRatio) {
        return {
          ok: false,
          spikeDetected: true,
          error: `Oracle XPX/USDT jump ${(diff * 100).toFixed(2)}% exceeds max ${(maxJumpRatio * 100).toFixed(1)}%`,
        };
      }
    }
    const prevBnb = Number(prevMidByPair['XPX/BNB'] || 0);
    if (prevBnb > 0) {
      const diff = Math.abs(priceBnb - prevBnb) / prevBnb;
      if (diff > maxJumpRatio) {
        return {
          ok: false,
          spikeDetected: true,
          error: `Oracle XPX/BNB jump ${(diff * 100).toFixed(2)}% exceeds max ${(maxJumpRatio * 100).toFixed(1)}%`,
        };
      }
    }
  }

  return {
    ok: true,
    ageMs: Math.max(0, ageMs),
    mids: {
      'XPX/USDT': priceUsdt,
      'XPX/BNB': priceBnb,
    },
  };
}

/**
 * Compute deterministic, fixed-point PostOnly bid/ask ladder quotes bounded by real tradeable balances.
 */
function computeLadderQuotes({
  pair,
  midPrice,
  spreadBps = 60,
  levelStepBps = 35,
  levels = 3,
  orderSizeXpx = 5,
  availableBaseAtomic = 0n,
  availableQuoteAtomic = 0n,
  reserveBaseAtomic = 1_000_000n,
  maxInventoryShare = 0.60,
  priceDecimals = null,
  minOrderAmountXpx = 1,
  maxCollarBps = 1500,
}) {
  const normPair = normalizePair(pair);
  const pDec = priceDecimals !== null && priceDecimals !== undefined
    ? Number(priceDecimals)
    : (normPair === 'XPX/BNB' ? 8 : 6);
  const tickUnit = 10n ** BigInt(Math.max(0, 8 - pDec));

  const midTicks = numberToPriceTicks(Number(midPrice));
  if (midTicks <= 0n) {
    return { ok: false, error: `Invalid midPrice for ${normPair}`, quotes: [] };
  }

  const safeLevels = Math.max(1, Math.min(10, Math.floor(Number(levels) || 1)));
  const safeSpreadBps = BigInt(Math.max(10, Math.min(2000, Math.floor(Number(spreadBps) || 60))));
  const safeStepBps = BigInt(Math.max(5, Math.min(1000, Math.floor(Number(levelStepBps) || 35))));
  const safeCollarBps = BigInt(Math.max(100, Math.min(5000, Math.floor(Number(maxCollarBps) || 1500))));

  const shareBps = BigInt(Math.max(100, Math.min(9500, Math.floor((Number(maxInventoryShare) || 0.6) * 10000))));
  const availBase = BigInt(availableBaseAtomic || 0n);
  const availQuote = BigInt(availableQuoteAtomic || 0n);
  const reserveBase = BigInt(reserveBaseAtomic || 0n);

  const usableBase = availBase > reserveBase ? ((availBase - reserveBase) * shareBps) / 10000n : 0n;
  const usableQuote = availQuote > 0n ? (availQuote * shareBps) / 10000n : 0n;

  const minAmtAtomic = parseDecBigInt(String(minOrderAmountXpx), 6);
  const targetAmtAtomic = parseDecBigInt(String(orderSizeXpx), 6);
  if (targetAmtAtomic < minAmtAtomic || targetAmtAtomic <= 0n) {
    return { ok: false, error: `Order size ${orderSizeXpx} XPX below minimum ${minOrderAmountXpx} XPX`, quotes: [] };
  }

  const minAllowedTick = (midTicks * (10000n - safeCollarBps)) / 10000n;
  const maxAllowedTick = (midTicks * (10000n + safeCollarBps) + 9999n) / 10000n;

  const bids = [];
  const asks = [];
  let remainingQuoteBudget = usableQuote;
  let remainingBaseBudget = usableBase;

  for (let i = 0; i < safeLevels; i++) {
    const offsetBps = safeSpreadBps + BigInt(i) * safeStepBps;
    if (offsetBps >= 9000n) break;

    // Bid: round DOWN to tickUnit
    let rawBid = (midTicks * (10000n - offsetBps)) / 10000n;
    let bidTick = (rawBid / tickUnit) * tickUnit;
    if (bidTick < tickUnit) bidTick = tickUnit;

    // Ask: round UP to tickUnit
    let rawAsk = (midTicks * (10000n + offsetBps) + 9999n) / 10000n;
    let askTick = ((rawAsk + tickUnit - 1n) / tickUnit) * tickUnit;

    // Ensure strict non-crossing even on ultra-low-tick markets
    if (askTick <= bidTick) {
      askTick = bidTick + tickUnit;
    }

    // Bid side allocation
    if (bidTick > 0n && bidTick >= minAllowedTick && bidTick < midTicks) {
      const quoteCost = (targetAmtAtomic * bidTick + PRICE_SCALE - 1n) / PRICE_SCALE;
      if (quoteCost > 0n && remainingQuoteBudget >= quoteCost) {
        remainingQuoteBudget -= quoteCost;
        bids.push({
          pair: normPair,
          side: 'buy',
          type: 'PostOnly',
          level: i + 1,
          priceTicks: bidTick,
          price: formatTicks(bidTick, pDec),
          amountAtomic: targetAmtAtomic,
          amount: formatAtomic(targetAmtAtomic, 6),
          quoteCostAtomic: quoteCost,
        });
      }
    }

    // Ask side allocation
    if (askTick > midTicks && askTick <= maxAllowedTick) {
      if (remainingBaseBudget >= targetAmtAtomic) {
        remainingBaseBudget -= targetAmtAtomic;
        asks.push({
          pair: normPair,
          side: 'sell',
          type: 'PostOnly',
          level: i + 1,
          priceTicks: askTick,
          price: formatTicks(askTick, pDec),
          amountAtomic: targetAmtAtomic,
          amount: formatAtomic(targetAmtAtomic, 6),
        });
      }
    }
  }

  // Final strict anti-cross check across entire ladder
  const bestBid = bids.length > 0 ? bids[0].priceTicks : 0n;
  const bestAsk = asks.length > 0 ? asks[0].priceTicks : 0n;
  if (bestBid > 0n && bestAsk > 0n && bestBid >= bestAsk) {
    return { ok: false, error: `Invariant breach: bestBid (${bestBid}) >= bestAsk (${bestAsk})`, quotes: [] };
  }

  return {
    ok: true,
    pair: normPair,
    midTicks,
    priceDecimals: pDec,
    bids,
    asks,
    quotes: [...bids, ...asks],
  };
}

/**
 * Zero-Trust Settlement Verification before Auto-Cosigning.
 * Independently verifies:
 *   1. AggregateComplete structure and strictly TRANSFER-only inner txs
 *   2. Zero outflow from owner personal wallet; zero non-positive amounts
 *   3. No unexpected mosaics leaving the 2-of-2 Trading Account
 *   4. Bot's own Ed25519 signature over the exact local-network domain-separated ORDER string
 *   5. Cumulative partial-fill outflow bounds + execution price bounds (anti-over-cosigning)
 */
function verifySettlementForBot(settlement, {
  networkType,
  ownerPublicKey,
  tradingPublicKey,
  tradingAddress,
  mosaicIds,
  cumulativeOrderOutflows = new Map(),
}) {
  if (!settlement || !settlement.aggregatePayload || !settlement.settlementId) {
    return { ok: false, error: 'Missing settlement payload or settlementId' };
  }
  const ownerPubUpper = String(ownerPublicKey || '').trim().toUpperCase();
  const ownerPubLower = ownerPubUpper.toLowerCase();
  const tradingPubUpper = String(tradingPublicKey || '').trim().toUpperCase();
  const tradingAddrUpper = normalizeAddr(tradingAddress);

  const netTypeNum = Number(networkType);
  if (!Number.isInteger(netTypeNum) || (netTypeNum !== 184 && netTypeNum !== 168)) {
    return { ok: false, error: `Invalid or unsupported networkType (${networkType})` };
  }
  if (!/^[0-9A-F]{64}$/.test(ownerPubUpper) || !/^[0-9A-F]{64}$/.test(tradingPubUpper)) {
    return { ok: false, error: 'Invalid bot ownerPublicKey or tradingPublicKey' };
  }
  if (ownerPubUpper === tradingPubUpper) {
    return { ok: false, error: 'Security block: ownerPublicKey and tradingPublicKey must be distinct in 2-of-2 Smart Trading mode' };
  }

  const reqCosigners = Array.isArray(settlement.requiredCosigners)
    ? settlement.requiredCosigners.map((k) => String(k || '').toUpperCase())
    : [];
  if (!reqCosigners.includes(ownerPubUpper)) {
    return { ok: false, error: 'Bot is not listed in requiredCosigners for this settlement' };
  }

  let aggTx;
  try {
    aggTx = TransactionMapping.createFromPayload(settlement.aggregatePayload);
  } catch (err) {
    return { ok: false, error: `Failed to parse aggregatePayload: ${err.message}` };
  }

  const aggCompleteTypes = new Set(
    [TransactionType.AGGREGATE_COMPLETE_V1, TransactionType.AGGREGATE_COMPLETE, 16705].filter((v) => v !== undefined)
  );
  if (!aggTx || !aggCompleteTypes.has(aggTx.type)) {
    return { ok: false, error: `Refused non-AGGREGATE_COMPLETE settlement transaction (type=${aggTx?.type})` };
  }
  const inners = Array.isArray(aggTx.innerTransactions) ? aggTx.innerTransactions : [];
  if (inners.length === 0) {
    return { ok: false, error: 'Refused empty settlement aggregate' };
  }

  const outByHex = new Map();
  const inByHex = new Map();

  for (const inner of inners) {
    if (!inner || inner.type !== TransactionType.TRANSFER) {
      return { ok: false, error: 'Security block: Refused non-TRANSFER inner transaction in settlement' };
    }
    const sPub = String(inner.signer?.publicKey || '').toUpperCase();
    if (sPub === ownerPubUpper) {
      return { ok: false, error: 'Security block: Settlement attempted to spend directly from owner Main Wallet' };
    }
    const recipObj = inner.recipient || inner.recipientAddress;
    const rAddr = recipObj && typeof recipObj.plain === 'function'
      ? normalizeAddr(recipObj.plain())
      : (typeof recipObj === 'string' ? normalizeAddr(recipObj) : '');
    if (sPub === tradingPubUpper && tradingAddrUpper && rAddr === tradingAddrUpper) {
      return { ok: false, error: 'Security block: Refused self-transfer loop on Trading Account' };
    }
    const mosaics = Array.isArray(inner.mosaics) ? inner.mosaics : [];
    if (mosaics.length === 0) {
      return { ok: false, error: 'Security block: Settlement contains empty-mosaic transfer' };
    }
    for (const m of mosaics) {
      let mHex;
      let amt;
      try {
        if (!m || !m.id || typeof m.id.toHex !== 'function' || m.amount === undefined || m.amount === null) {
          return { ok: false, error: 'Security block: Malformed mosaic entry in settlement transfer' };
        }
        mHex = m.id.toHex().toUpperCase();
        amt = typeof m.amount.toHex === 'function'
          ? BigInt('0x' + m.amount.toHex())
          : BigInt(typeof m.amount.compact === 'function' ? m.amount.compact() : m.amount);
      } catch (parseErr) {
        return { ok: false, error: `Security block: Failed to parse mosaic amount (${parseErr.message})` };
      }
      if (amt <= 0n) {
        return { ok: false, error: 'Security block: Settlement contains zero or negative transfer amount' };
      }
      if (sPub === tradingPubUpper) {
        outByHex.set(mHex, (outByHex.get(mHex) || 0n) + amt);
      }
      if (tradingAddrUpper && rAddr === tradingAddrUpper) {
        inByHex.set(mHex, (inByHex.get(mHex) || 0n) + amt);
      }
    }
  }

  let verifiedOrderFingerprint = null;
  let verifiedOrderSide = null;
  let outXpx = 0n;
  let outQuote = 0n;

  if (outByHex.size > 0) {
    const rootCtx = settlement.orderContext;
    const pMap = rootCtx && typeof rootCtx.participantContexts === 'object' ? rootCtx.participantContexts : null;
    const participantCtx = pMap ? (pMap[ownerPubLower] || pMap[ownerPubUpper] || null) : null;
    const candidateCtx = participantCtx || rootCtx;

    const ctxIsMine = Boolean(
      candidateCtx &&
      candidateCtx.publicKey &&
      String(candidateCtx.publicKey).toUpperCase() === ownerPubUpper
    );
    if (!ctxIsMine || !candidateCtx.signature || !candidateCtx.nonce || !candidateCtx.expiry) {
      return { ok: false, error: 'Security block: Missing bot-owned signed orderContext for outflow settlement' };
    }

    // ALWAYS derive domain separator locally from verified networkType (blocks cross-network replay!)
    const domainSep = `${netTypeNum}:SiriusCLOB-v1`;
    const rawPair = String(candidateCtx.pair || '').trim().toUpperCase().replace('_', '/');
    if (rawPair !== 'XPX/USDT' && rawPair !== 'XPX/BNB') {
      return { ok: false, error: `Security block: Unrecognized or missing pair "${candidateCtx.pair}" in settlement context` };
    }
    const normPair = rawPair;
    const rawSide = String(candidateCtx.side || '').trim().toLowerCase();
    if (rawSide !== 'buy' && rawSide !== 'sell') {
      return { ok: false, error: `Security block: Invalid or missing side "${candidateCtx.side}" in settlement context` };
    }
    const normSide = rawSide;
    const rawType = String(candidateCtx.type || '');
    const normType = rawType === 'PostOnly' ? 'PostOnly' : (rawType === 'Market' ? 'Market' : 'Limit');
    const msg = `${domainSep}:ORDER:${normPair}:${normSide}:${normType}:${String(candidateCtx.price)}:${String(candidateCtx.amount)}:${candidateCtx.nonce}:${candidateCtx.expiry}`;

    let sigOk = false;
    try {
      sigOk = KeyPair.verify(
        hexToUint8(ownerPubUpper),
        new TextEncoder().encode(msg),
        hexToUint8(String(candidateCtx.signature)),
        DerivationScheme.Ed25519Sha3
      );
    } catch (_) {
      sigOk = false;
    }
    if (!sigOk) {
      return { ok: false, error: 'Security block: Invalid Ed25519 order signature inside settlement context' };
    }

    const xpxHex = String(mosaicIds?.XPX || '').toUpperCase();
    const quoteSym = normPair.endsWith('/BNB') ? 'BNB' : 'USDT';
    const quoteHex = String(mosaicIds?.[quoteSym] || '').toUpperCase();
    if (!xpxHex || !quoteHex) {
      return { ok: false, error: `Missing mosaic ID configuration for ${normPair}` };
    }

    for (const [hex] of outByHex.entries()) {
      if (hex !== xpxHex && hex !== quoteHex) {
        return { ok: false, error: `Security block: Unexpected mosaic ${hex} leaving Trading Account` };
      }
    }

    outXpx = outByHex.get(xpxHex) || 0n;
    outQuote = outByHex.get(quoteHex) || 0n;
    const inXpx = inByHex.get(xpxHex) || 0n;
    const inQuote = inByHex.get(quoteHex) || 0n;

    verifiedOrderSide = normSide;
    verifiedOrderFingerprint = `${ownerPubUpper}:${String(candidateCtx.nonce)}:${String(candidateCtx.signature).slice(0, 16)}`;

    const ordAmt = parseDecBigInt(candidateCtx.amount, 6);
    const ordPriceTicks = parseDecBigInt(candidateCtx.price, 8);
    if (ordAmt <= 0n || ordPriceTicks <= 0n) {
      return { ok: false, error: 'Security block: Non-positive order amount or price in settlement context' };
    }

    const prevAcc = cumulativeOrderOutflows.get(verifiedOrderFingerprint) || null;
    const alreadyCounted = Boolean(prevAcc && prevAcc.settlementIds.has(String(settlement.settlementId)));
    const cumOutXpx = (prevAcc && !alreadyCounted ? prevAcc.outXpx : 0n) + outXpx;
    const cumOutQuote = (prevAcc && !alreadyCounted ? prevAcc.outQuote : 0n) + outQuote;

    if (normSide === 'buy') {
      if (outXpx > 0n) {
        return { ok: false, error: 'Security block: BUY settlement attempted to send XPX out of Trading Account' };
      }
      const maxQuote = ((ordAmt * ordPriceTicks + PRICE_SCALE - 1n) / PRICE_SCALE) * 102n / 100n + 1000n;
      if (outQuote > maxQuote || cumOutQuote > maxQuote) {
        return { ok: false, error: 'Security block: BUY settlement quote outflow exceeds signed order limit' };
      }
      if (outQuote > 0n && inXpx === 0n) {
        return { ok: false, error: 'Security block: BUY settlement sends quote without delivering XPX' };
      }
      if (inXpx > 0n && outQuote > 0n) {
        const maxCostForFill = ((inXpx * ordPriceTicks + PRICE_SCALE - 1n) / PRICE_SCALE) * 102n / 100n + 1000n;
        if (outQuote > maxCostForFill) {
          return { ok: false, error: 'Security block: BUY execution cost exceeds signed limit price' };
        }
      }
    } else {
      if (outQuote > 0n) {
        return { ok: false, error: 'Security block: SELL settlement attempted to send quote out of Trading Account' };
      }
      const maxXpx = (ordAmt * 102n) / 100n + 1000n;
      if (outXpx > maxXpx || cumOutXpx > maxXpx) {
        return { ok: false, error: 'Security block: SELL settlement XPX outflow exceeds signed order amount' };
      }
      if (outXpx > 0n && inQuote === 0n) {
        return { ok: false, error: `Security block: SELL settlement sends XPX without delivering ${quoteSym}` };
      }
      if (outXpx > 0n) {
        const minQuote = ((outXpx * ordPriceTicks) / PRICE_SCALE) * 97n / 100n;
        if (minQuote >= 100n && inQuote < minQuote) {
          return { ok: false, error: 'Security block: SELL settlement payout below signed limit price' };
        }
      }
    }
  }

  return {
    ok: true,
    fingerprint: verifiedOrderFingerprint,
    side: verifiedOrderSide,
    outXpx,
    outQuote,
  };
}

// =========================================================================
// Mainnet Smart-Trading Market Maker Client Runtime
// =========================================================================

class MainnetMarketMaker {
  constructor(opts = {}) {
    this.apiBase = String(opts.apiBase || process.env.CLOB_API_URL || 'http://127.0.0.1:8088').replace(/\/+$/, '');
    this.privateKey = String(opts.privateKey || '').trim();
    this.markets = (opts.markets || (process.env.MM_MARKETS || 'XPX/USDT,XPX/BNB').split(','))
      .map((m) => normalizePair(m))
      .filter((v, i, arr) => arr.indexOf(v) === i);
    this.levels = Math.max(1, Math.min(10, Number(opts.levels || process.env.MM_LEVELS || 3)));
    this.spreadBps = Math.max(10, Math.min(2000, Number(opts.spreadBps || process.env.MM_SPREAD_BPS || 60)));
    this.levelStepBps = Math.max(5, Math.min(1000, Number(opts.levelStepBps || process.env.MM_LEVEL_STEP_BPS || 35)));
    this.orderSizeXpx = Math.max(1, Number(opts.orderSizeXpx || process.env.MM_ORDER_SIZE_XPX || 5));
    this.reserveXpx = Math.max(0, Number(opts.reserveXpx ?? process.env.MM_RESERVE_XPX ?? 1));
    this.maxInventoryShare = Math.max(0.05, Math.min(0.95, Number(opts.maxInventoryShare || process.env.MM_MAX_INVENTORY_SHARE || 0.60)));
    this.maxOracleAgeMs = Math.max(5000, Number(opts.maxOracleAgeMs || process.env.MM_MAX_ORACLE_AGE_MS || 60_000));
    this.maxOracleJumpRatio = Math.max(0.02, Math.min(0.80, Number(opts.maxOracleJumpRatio || process.env.MM_MAX_ORACLE_JUMP_RATIO || 0.20)));
    this.codTimeoutMs = Math.max(5000, Math.min(120_000, Number(opts.codTimeoutMs || process.env.MM_COD_TIMEOUT_MS || 30_000)));
    this.heartbeatIntervalMs = Math.max(2000, Math.min(Math.floor(this.codTimeoutMs / 3), Number(opts.heartbeatIntervalMs || process.env.MM_HEARTBEAT_INTERVAL_MS || 8000)));
    this.loopIntervalMs = Math.max(3000, Number(opts.loopIntervalMs || process.env.MM_LOOP_INTERVAL_MS || 15_000));
    this.settlementPollMs = Math.max(1500, Number(opts.settlementPollMs || process.env.MM_SETTLEMENT_POLL_MS || 3000));
    this.httpTimeoutMs = Math.max(3000, Number(opts.httpTimeoutMs || process.env.MM_HTTP_TIMEOUT_MS || 15_000));
    this.expectedNetworkType = opts.expectedNetworkType !== undefined
      ? Number(opts.expectedNetworkType)
      : (process.env.MM_EXPECTED_NETWORK_TYPE ? Number(process.env.MM_EXPECTED_NETWORK_TYPE) : null);
    this.allowInsecureHttp = Boolean(opts.allowInsecureHttp || process.env.MM_ALLOW_INSECURE_HTTP === '1');
    this.dryRun = Boolean(opts.dryRun);

    this.lastNonce = 0;
    this.config = null;
    this.account = null;
    this.tradingAccount = null;
    this.mosaicIds = { XPX: '', USDT: '', BNB: '' };
    this.pairMeta = {};
    this.prevMidByPair = {};
    this.consecutiveSpikes = 0;
    this.codRegistered = false;
    this.cosignedSettlementIds = new Set();
    this.inflightSettlements = new Set();
    this.settlementPollInFlight = false;
    this.heartbeatInFlight = false;
    this.cumulativeOrderOutflows = new Map();
    this.stopped = false;
    this.hbTimer = null;
    this.settleTimer = null;
  }

  nextNonce() {
    const candidate = Date.now() * 1000 + crypto.randomInt(0, 1000);
    this.lastNonce = candidate > this.lastNonce ? candidate : this.lastNonce + 1;
    return this.lastNonce;
  }

  domainSeparator() {
    return `${Number(this.config.networkType)}:SiriusCLOB-v1`;
  }

  async httpJson(method, endpointPath, body = null) {
    const url = `${this.apiBase}${endpointPath}`;
    const init = {
      method,
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(this.httpTimeoutMs),
    };
    if (body !== null) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body);
    }
    const res = await fetch(url, init);
    const text = await res.text();
    let data;
    try {
      data = text ? JSON.parse(text) : {};
    } catch (_) {
      throw new Error(`HTTP ${res.status} non-JSON response from ${endpointPath}: ${text.slice(0, 160)}`);
    }
    return { status: res.status, ok: res.ok, data };
  }

  async initialize() {
    if (!/^[0-9a-fA-F]{64}$/.test(this.privateKey)) {
      throw new Error('Missing or invalid 64-hex MM_OWNER_PRIVATE_KEY (or MM_KEY_FILE)');
    }

    const { ok, data: cfg, status } = await this.httpJson('GET', '/api/clob/config');
    if (!ok || !cfg || !cfg.ok) {
      throw new Error(`Failed to fetch /api/clob/config (HTTP ${status}): ${cfg?.error || 'unknown error'}`);
    }

    const netType = Number(cfg.networkType);
    if (!Number.isFinite(netType) || (netType !== 184 && netType !== 168)) {
      throw new Error(`Unsupported or unrecognized CLOB networkType: ${cfg.networkType}`);
    }
    if (this.expectedNetworkType !== null && netType !== this.expectedNetworkType) {
      throw new Error(`Network mismatch: expected networkType=${this.expectedNetworkType}, gateway reported ${netType}`);
    }
    if (netType === 184 && !this.apiBase.startsWith('https://') && !this.allowInsecureHttp) {
      throw new Error(`Mainnet security guard: CLOB_API_URL must use https:// on Mainnet (got ${this.apiBase})`);
    }
    if (!cfg.generationHash || !/^[0-9a-fA-F]{64}$/.test(String(cfg.generationHash))) {
      throw new Error('Gateway /api/clob/config missing valid 64-hex generationHash');
    }

    this.config = cfg;
    this.account = Account.createFromPrivateKey(this.privateKey, netType, 1);

    for (const p of (Array.isArray(cfg.pairs) ? cfg.pairs : [])) {
      const sym = normalizePair(p.symbol);
      this.pairMeta[sym] = p;
      if (p.baseMosaicId) this.mosaicIds.XPX = String(p.baseMosaicId).toUpperCase();
      if (sym === 'XPX/USDT' && p.quoteMosaicId) this.mosaicIds.USDT = String(p.quoteMosaicId).toUpperCase();
      if (sym === 'XPX/BNB' && p.quoteMosaicId) this.mosaicIds.BNB = String(p.quoteMosaicId).toUpperCase();
    }

    const statusRes = await this.fetchTradingAccountStatus();
    if (!statusRes.hasTradingAccount || !statusRes.tradingAccount || statusRes.tradingAccount.status !== 'active') {
      throw new Error(
        `Smart Trading Account is not active for owner ${this.account.publicKey.slice(0, 12)}... ` +
        `(hasTradingAccount=${Boolean(statusRes?.hasTradingAccount)}, status=${statusRes?.tradingAccount?.status || 'none'}). ` +
        `Activate & fund a 2-of-2 Smart Trading Account before running the Mainnet Market Maker.`
      );
    }
    this.tradingAccount = statusRes.tradingAccount;

    console.log(
      `[MainnetMM] Initialized on ${cfg.networkName || cfg.network} (networkType=${netType}) | ` +
      `Owner=${this.account.address.plain()} | TradingAccount=${this.tradingAccount.tradingAddress} | ` +
      `Markets=${this.markets.join(',')} | DryRun=${this.dryRun}`
    );
    return statusRes;
  }

  async checkHealth() {
    const { ok, data } = await this.httpJson('GET', '/api/clob/health');
    if (!ok || !data || !data.ok || (data.circuitBreaker && data.circuitBreaker.tripped)) {
      const reason = data?.circuitBreaker?.reason || data?.error || 'Unhealthy gateway';
      return { ok: false, error: reason };
    }
    return { ok: true, data };
  }

  async fetchTradingAccountStatus() {
    const pub = this.account.publicKey.toLowerCase();
    const { ok, data, status } = await this.httpJson('GET', `/api/clob/trading-account/status?ownerPublicKey=${pub}`);
    if (!ok || !data || data.ok === false) {
      throw new Error(`Failed to fetch trading-account status (HTTP ${status}): ${data?.error || 'unknown'}`);
    }
    return data;
  }

  async registerHeartbeat() {
    if (this.dryRun) {
      console.log(`[MainnetMM] [DRY-RUN] Would register COD heartbeat (${this.codTimeoutMs}ms)`);
      return { ok: true, dryRun: true };
    }
    const pubLower = this.account.publicKey.toLowerCase();
    const nonce = this.nextNonce();
    const expiry = Date.now() + 60_000;
    const msg = `${this.domainSeparator()}:HEARTBEAT_REGISTER:${pubLower}:${this.codTimeoutMs}:${nonce}:${expiry}`;
    const signature = this.account.signData(msg);
    const { ok, data, status } = await this.httpJson('POST', '/api/clob/heartbeat', {
      publicKey: pubLower,
      timeoutMs: this.codTimeoutMs,
      markets: this.markets,
      nonce,
      expiry,
      signature,
    });
    if (!ok || !data || !data.ok) {
      throw new Error(`COD heartbeat register failed (HTTP ${status}): ${data?.error || 'unknown'}`);
    }
    this.codRegistered = true;
    return data;
  }

  async deregisterHeartbeat() {
    if (this.dryRun || !this.codRegistered) return { ok: true, skipped: true };
    const pubLower = this.account.publicKey.toLowerCase();
    const nonce = this.nextNonce();
    const expiry = Date.now() + 60_000;
    const msg = `${this.domainSeparator()}:HEARTBEAT_DEREGISTER:${pubLower}:${nonce}:${expiry}`;
    const signature = this.account.signData(msg);
    const { ok, data, status } = await this.httpJson('POST', '/api/clob/heartbeat/deregister', {
      publicKey: pubLower,
      nonce,
      expiry,
      signature,
    });
    if (ok && data && data.ok) {
      this.codRegistered = false;
    } else {
      console.warn(`[MainnetMM] ⚠️ COD heartbeat deregister failed (HTTP ${status}): ${data?.error || 'unknown'}`);
    }
    return data;
  }

  async cancelAllForPair(pair) {
    const normPair = normalizePair(pair);
    if (this.dryRun) {
      console.log(`[MainnetMM] [DRY-RUN] Would cancel all orders on ${normPair}`);
      return { ok: true, dryRun: true };
    }
    const pubLower = this.account.publicKey.toLowerCase();
    const nonce = this.nextNonce();
    const expiry = Date.now() + 60_000;
    const msg = `${this.domainSeparator()}:CANCEL_ALL:${normPair}:${nonce}:${expiry}`;
    const signature = this.account.signData(msg);
    const { ok, data, status } = await this.httpJson('POST', '/api/clob/cancel/all', {
      pair: normPair,
      publicKey: pubLower,
      nonce,
      expiry,
      signature,
    });
    if (!ok || !data || !data.ok) {
      throw new Error(`Cancel-All on ${normPair} failed (HTTP ${status}): ${data?.error || 'unknown'}`);
    }
    return data;
  }

  async cancelAllMarkets() {
    const results = {};
    for (const pair of this.markets) {
      try {
        results[pair] = await this.cancelAllForPair(pair);
      } catch (err) {
        console.error(`[MainnetMM] Cancel-All error on ${pair}: ${err.message}`);
        results[pair] = { ok: false, error: err.message };
      }
    }
    return results;
  }

  async placePostOnlyQuote(quote) {
    if (this.dryRun) {
      console.log(`[MainnetMM] [DRY-RUN] ${quote.pair} ${quote.side.toUpperCase()} ${quote.amount} XPX @ ${quote.price} (PostOnly)`);
      return { ok: true, dryRun: true };
    }
    const pubLower = this.account.publicKey.toLowerCase();
    const nonce = this.nextNonce();
    const expiry = Date.now() + 60_000;
    const msg = `${this.domainSeparator()}:ORDER:${quote.pair}:${quote.side}:PostOnly:${quote.price}:${quote.amount}:${nonce}:${expiry}`;
    const signature = this.account.signData(msg);
    const { ok, data, status } = await this.httpJson('POST', '/api/clob/order', {
      pair: quote.pair,
      side: quote.side,
      type: 'PostOnly',
      price: quote.price,
      amount: quote.amount,
      publicKey: pubLower,
      nonce,
      expiry,
      signature,
    });
    if (!ok || !data || !data.ok) {
      return { ok: false, status, error: data?.error || `HTTP ${status}` };
    }
    return { ok: true, orderId: data.orderId, data };
  }

  async pollAndCosignSettlements() {
    if (this.settlementPollInFlight) {
      return { checked: 0, cosigned: 0, rejected: 0, skippedOverlap: true };
    }
    this.settlementPollInFlight = true;
    try {
      const pubLower = this.account.publicKey.toLowerCase();
      const pubUpper = this.account.publicKey.toUpperCase();
      const { ok, data } = await this.httpJson('GET', `/api/clob/trading-account/pending-settlements?ownerPublicKey=${pubLower}`);
      if (!ok || !data || !data.ok || !Array.isArray(data.settlements)) {
        return { checked: 0, cosigned: 0, rejected: 0 };
      }

      let cosigned = 0;
      let rejected = 0;
      for (const s of data.settlements) {
        if (!s || s.status !== 'pending_cosign') continue;
        const alreadySigned = Array.isArray(s.collectedCosigs) &&
          s.collectedCosigs.some((c) => String(c.signer || '').toUpperCase() === pubUpper);
        const signKey = `${s.settlementId}:${s.txHash || ''}`;
        if (alreadySigned || this.cosignedSettlementIds.has(signKey) || this.inflightSettlements.has(signKey)) continue;

        this.inflightSettlements.add(signKey);
        try {
          const res = await this.verifyAndCosignSingle(s, false);
          if (res.ok) cosigned++;
          else rejected++;
        } finally {
          this.inflightSettlements.delete(signKey);
        }
      }
      return { checked: data.settlements.length, cosigned, rejected };
    } finally {
      this.settlementPollInFlight = false;
    }
  }

  async verifyAndCosignSingle(settlement, retriedRebuild = false) {
    const check = verifySettlementForBot(settlement, {
      networkType: Number(this.config.networkType),
      ownerPublicKey: this.account.publicKey,
      tradingPublicKey: this.tradingAccount.tradingPublicKey,
      tradingAddress: this.tradingAccount.tradingAddress,
      mosaicIds: this.mosaicIds,
      cumulativeOrderOutflows: this.cumulativeOrderOutflows,
    });

    if (!check.ok) {
      console.error(`[MainnetMM] ⛔ REFUSED to cosign settlement ${settlement?.settlementId}: ${check.error}`);
      return { ok: false, error: check.error };
    }

    if (this.dryRun) {
      console.log(`[MainnetMM] [DRY-RUN] Verified settlement ${settlement.settlementId} — skipping cosign`);
      return { ok: true, dryRun: true };
    }

    const cosig = CosignatureTransaction.signTransactionPayload(
      this.account,
      settlement.aggregatePayload,
      this.config.generationHash
    );
    const { ok, data, status } = await this.httpJson('POST', '/api/clob/trading-account/cosign-settlement', {
      settlementId: settlement.settlementId,
      cosignature: {
        parentHash: cosig.parentHash,
        signer: cosig.signer,
        signature: cosig.signature,
      },
    });

    if (ok && data && data.ok && data.rebuilt && data.settlement && !retriedRebuild) {
      // Mark expired txHash as processed so it is never re-signed, then verify & cosign rebuilt tx
      this.cosignedSettlementIds.add(`${settlement.settlementId}:${settlement.txHash || ''}`);
      return this.verifyAndCosignSingle(data.settlement, true);
    }
    if (!ok || !data || !data.ok) {
      console.error(`[MainnetMM] Cosign submit failed for ${settlement.settlementId} (HTTP ${status}): ${data?.error || 'unknown'}`);
      return { ok: false, error: data?.error || `HTTP ${status}` };
    }

    this.cosignedSettlementIds.add(`${settlement.settlementId}:${settlement.txHash || ''}`);
    if (check.fingerprint) {
      const acc = this.cumulativeOrderOutflows.get(check.fingerprint) || {
        outXpx: 0n,
        outQuote: 0n,
        settlementIds: new Set(),
      };
      if (!acc.settlementIds.has(String(settlement.settlementId))) {
        acc.outXpx += check.outXpx;
        acc.outQuote += check.outQuote;
        acc.settlementIds.add(String(settlement.settlementId));
        this.cumulativeOrderOutflows.set(check.fingerprint, acc);
      }
    }
    console.log(`[MainnetMM] ✅ Cosigned verified settlement ${settlement.settlementId} (status=${data.status}, tx=${data.txHash || 'pending'})`);
    return { ok: true, status: data.status, txHash: data.txHash };
  }

  async runSingleCycle() {
    // 1. Health & Circuit Breaker Check
    const health = await this.checkHealth();
    if (!health.ok) {
      console.warn(`[MainnetMM] ⚠️ Gateway unhealthy or circuit breaker tripped (${health.error}). Pulling quotes.`);
      await this.cancelAllMarkets();
      return { ok: false, halted: true, reason: health.error };
    }

    // 2. Refresh COD Dead-Man's Switch BEFORE quoting
    await this.registerHeartbeat();

    // 3. Oracle Freshness & Jump Guard
    const { ok: oracleOk, data: oracle } = await this.httpJson('GET', '/api/clob/oracle/pancakeswap');
    const oracleCheck = oracleOk
      ? validateOracleData(oracle, {
          now: Date.now(),
          maxAgeMs: this.maxOracleAgeMs,
          prevMidByPair: this.prevMidByPair,
          maxJumpRatio: this.maxOracleJumpRatio,
        })
      : { ok: false, error: 'Failed to reach /api/clob/oracle/pancakeswap' };

    if (!oracleCheck.ok) {
      if (oracleCheck.spikeDetected) {
        this.consecutiveSpikes += 1;
        console.warn(`[MainnetMM] ⚠️ Oracle spike detected (${this.consecutiveSpikes}/3): ${oracleCheck.error}. Cancelling resting quotes.`);
        await this.cancelAllMarkets();
        if (this.consecutiveSpikes >= 3 && oracle && Number(oracle.priceUsdt) > 0 && Number(oracle.priceBnb) > 0) {
          this.prevMidByPair = { 'XPX/USDT': Number(oracle.priceUsdt), 'XPX/BNB': Number(oracle.priceBnb) };
          this.consecutiveSpikes = 0;
        }
      } else {
        console.warn(`[MainnetMM] ⚠️ Oracle check failed (${oracleCheck.error}). Pulling quotes.`);
        await this.cancelAllMarkets();
      }
      return { ok: false, halted: true, reason: oracleCheck.error };
    }
    this.consecutiveSpikes = 0;
    this.prevMidByPair = { ...oracleCheck.mids };

    // 4. Cancel stale resting quotes cleanly, then fetch authoritative Smart Trading balances
    await this.cancelAllMarkets();
    const taStatus = await this.fetchTradingAccountStatus();
    if (!taStatus.hasTradingAccount || taStatus.tradingAccount?.status !== 'active') {
      console.warn('[MainnetMM] ⚠️ Smart Trading Account no longer active. Halting quotes.');
      return { ok: false, halted: true, reason: 'Trading account inactive' };
    }

    const tradeable = taStatus.balances?.tradeable || {};
    const availXpx = BigInt(tradeable.xpx || '0');
    const availUsdt = BigInt(tradeable.usdt || '0');
    const availBnb = BigInt(tradeable.bnb || '0');
    const reserveBaseAtomic = parseDecBigInt(String(this.reserveXpx), 6);

    const summary = {
      ok: true,
      mids: oracleCheck.mids,
      placed: 0,
      failed: 0,
      markets: {},
    };

    // Split XPX base allocation proportionally across active markets so ask ladders don't collide
    const perMarketBaseAtomic = this.markets.length > 0 ? availXpx / BigInt(this.markets.length) : availXpx;

    for (const pair of this.markets) {
      const meta = this.pairMeta[pair] || {};
      const quoteAvail = pair === 'XPX/BNB' ? availBnb : availUsdt;
      const plan = computeLadderQuotes({
        pair,
        midPrice: oracleCheck.mids[pair],
        spreadBps: this.spreadBps,
        levelStepBps: this.levelStepBps,
        levels: this.levels,
        orderSizeXpx: this.orderSizeXpx,
        availableBaseAtomic: perMarketBaseAtomic,
        availableQuoteAtomic: quoteAvail,
        reserveBaseAtomic,
        maxInventoryShare: this.maxInventoryShare,
        priceDecimals: meta.priceDecimals,
        minOrderAmountXpx: meta.minOrderAmount || 1,
      });

      if (!plan.ok) {
        console.warn(`[MainnetMM] Skipping ${pair}: ${plan.error}`);
        summary.markets[pair] = { placed: 0, failed: 0, skippedReason: plan.error };
        continue;
      }

      let mPlaced = 0;
      let mFailed = 0;
      const orderIds = [];
      for (const q of plan.quotes) {
        const res = await this.placePostOnlyQuote(q);
        if (res.ok) {
          mPlaced++;
          summary.placed++;
          if (res.orderId !== undefined) orderIds.push(res.orderId);
        } else {
          mFailed++;
          summary.failed++;
          console.warn(`[MainnetMM] Quote rejected (${q.pair} ${q.side} ${q.amount} @ ${q.price}): ${res.error}`);
        }
      }
      summary.markets[pair] = {
        mid: oracleCheck.mids[pair],
        bidsPlanned: plan.bids.length,
        asksPlanned: plan.asks.length,
        placed: mPlaced,
        failed: mFailed,
        orderIds,
      };
      console.log(
        `[MainnetMM] ${pair} mid=${oracleCheck.mids[pair]} | placed=${mPlaced}/${plan.quotes.length} ` +
        `(bids=${plan.bids.length}, asks=${plan.asks.length})`
      );
    }

    // 5. Poll & auto-cosign any matched pending settlements
    summary.settlements = await this.pollAndCosignSettlements();
    return summary;
  }

  async shutdown(reason = 'shutdown') {
    if (this.stopped) return;
    this.stopped = true;
    if (this.hbTimer) clearInterval(this.hbTimer);
    if (this.settleTimer) clearInterval(this.settleTimer);
    console.log(`[MainnetMM] Graceful shutdown (${reason}): cancelling all quotes before COD deregister...`);
    try {
      await this.cancelAllMarkets();
    } catch (err) {
      console.error(`[MainnetMM] Error during cancelAllMarkets on shutdown: ${err.message}`);
    }
    try {
      await this.deregisterHeartbeat();
    } catch (err) {
      console.error(`[MainnetMM] Error during COD deregister on shutdown: ${err.message}`);
    }
    console.log('[MainnetMM] Shutdown complete.');
  }

  async runDaemon() {
    await this.initialize();
    await this.registerHeartbeat();

    const onSignal = (sig) => {
      this.shutdown(sig).finally(() => process.exit(0));
    };
    process.on('SIGINT', () => onSignal('SIGINT'));
    process.on('SIGTERM', () => onSignal('SIGTERM'));

    this.hbTimer = setInterval(() => {
      if (this.stopped || this.heartbeatInFlight) return;
      this.heartbeatInFlight = true;
      this.registerHeartbeat()
        .catch((err) => {
          console.warn(`[MainnetMM] Heartbeat refresh warning: ${err.message}`);
        })
        .finally(() => {
          this.heartbeatInFlight = false;
        });
    }, this.heartbeatIntervalMs);
    if (this.hbTimer.unref) this.hbTimer.unref();

    this.settleTimer = setInterval(() => {
      if (this.stopped) return;
      this.pollAndCosignSettlements().catch((err) => {
        console.warn(`[MainnetMM] Settlement poll warning: ${err.message}`);
      });
    }, this.settlementPollMs);
    if (this.settleTimer.unref) this.settleTimer.unref();

    while (!this.stopped) {
      try {
        await this.runSingleCycle();
      } catch (err) {
        console.error(`[MainnetMM] Cycle error: ${err.message}`);
      }
      if (!this.stopped) {
        await new Promise((r) => setTimeout(r, this.loopIntervalMs));
      }
    }
  }
}

function loadPrivateKeyFromEnv() {
  if (process.env.MM_OWNER_PRIVATE_KEY && process.env.MM_OWNER_PRIVATE_KEY.trim()) {
    return process.env.MM_OWNER_PRIVATE_KEY.trim();
  }
  const keyFile = (process.env.MM_KEY_FILE || '').trim();
  if (keyFile && fs.existsSync(keyFile)) {
    try {
      const st = fs.statSync(keyFile);
      if ((st.mode & 0o077) !== 0) {
        console.warn(`[MainnetMM] ⚠️ Security warning: ${keyFile} has overly permissive file mode. Recommend chmod 600.`);
      }
    } catch (_) {}
    const raw = fs.readFileSync(keyFile, 'utf8').trim();
    if (/^[0-9a-fA-F]{64}$/.test(raw)) return raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed.privateKey === 'string') return parsed.privateKey.trim();
    } catch (_) {}
  }
  return '';
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const isOnce = args.includes('--once') || args.includes('--step');
  const isDaemon = args.includes('--daemon') || !isOnce;
  const dryRun = args.includes('--dry-run');
  const keepOrders = args.includes('--keep-orders');

  const privKey = loadPrivateKeyFromEnv();
  const bot = new MainnetMarketMaker({ privateKey: privKey, dryRun });

  (async () => {
    if (isOnce) {
      await bot.initialize();
      const result = await bot.runSingleCycle();
      if (!keepOrders) {
        await bot.shutdown('--once complete');
      }
      console.log('[MainnetMM] --once result:', JSON.stringify(result, null, 2));
      process.exit(result.ok ? 0 : 1);
    } else if (isDaemon) {
      await bot.runDaemon();
    }
  })().catch(async (err) => {
    console.error(`[MainnetMM] Fatal error: ${err.message}`);
    try { await bot.shutdown('fatal-error'); } catch (_) {}
    process.exit(1);
  });
}

module.exports = {
  MainnetMarketMaker,
  parseDecBigInt,
  formatTicks,
  formatAtomic,
  numberToPriceTicks,
  validateOracleData,
  computeLadderQuotes,
  verifySettlementForBot,
};
