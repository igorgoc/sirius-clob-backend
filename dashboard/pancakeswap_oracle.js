/**
 * SiriusCLOB — PancakeSwap V2 BSC On-Chain Oracle Client
 * Queries real-time XPX/WBNB reserves from PancakeSwap V2 Pair:
 *   Pair Address: 0x439B8E49F2b0d9C9824d5AB641D677511f362dA5
 *   Base: XPX (0x6f3aaf802f57d045efdd2ac9c06d8879305542af)
 *   Quote: WBNB (0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c)
 */

let cachedPancakeOracle = null;
let lastPancakeOracleFetch = 0;
let consecutiveSpikeCount = 0;
let sharedBscProvider = null;
let inFlightOracleFetch = null;

const PAIR_ADDRESS = '0x439B8E49F2b0d9C9824d5AB641D677511f362dA5';
const XPX_BSC_CONTRACT = '0x6f3aaf802f57d045efdd2ac9c06d8879305542af';
const BSC_ORACLE_RPC_URL = process.env.BSC_ORACLE_RPC_URL || 'https://bsc-dataseed.binance.org/';
const BSC_RPC_TIMEOUT_MS = 5000;
const MIN_POOL_BNB_RESERVE = 0.05;
const MIN_POOL_XPX_RESERVE = 1000;
const MAX_SINGLE_JUMP_RATIO = 0.50; // Reject >50% single-sample spot manipulation spike unless sustained

function getSharedBscProvider() {
  if (!sharedBscProvider) {
    const { ethers } = require('ethers');
    sharedBscProvider = new ethers.JsonRpcProvider(BSC_ORACLE_RPC_URL, undefined, { staticNetwork: true });
  }
  return sharedBscProvider;
}

function withTimeout(promise, ms, label) {
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
}

function isPositiveFinite(v) {
  return typeof v === 'number' && Number.isFinite(v) && v > 0;
}

function applyOracleJumpGuard(candidate) {
  if (!isPositiveFinite(candidate.priceBnb) || !isPositiveFinite(candidate.priceUsdt)) {
    throw new Error('Oracle returned non-finite or non-positive price');
  }
  if (
    cachedPancakeOracle &&
    !cachedPancakeOracle.isFallback &&
    isPositiveFinite(cachedPancakeOracle.priceBnb) &&
    isPositiveFinite(cachedPancakeOracle.priceUsdt)
  ) {
    const bnbDiff = Math.abs(candidate.priceBnb - cachedPancakeOracle.priceBnb) / cachedPancakeOracle.priceBnb;
    const usdDiff = Math.abs(candidate.priceUsdt - cachedPancakeOracle.priceUsdt) / cachedPancakeOracle.priceUsdt;
    if (bnbDiff > MAX_SINGLE_JUMP_RATIO || usdDiff > MAX_SINGLE_JUMP_RATIO) {
      consecutiveSpikeCount += 1;
      if (consecutiveSpikeCount < 3) {
        console.warn(
          `⚠️ [Oracle Guard] Rejected single-sample spot spike (bnbDiff=${(bnbDiff * 100).toFixed(1)}%, usdDiff=${(usdDiff * 100).toFixed(1)}%, count=${consecutiveSpikeCount}/3). Keeping last verified price.`
        );
        return { ...cachedPancakeOracle, spikeRejected: true };
      }
      consecutiveSpikeCount = 0;
    } else {
      consecutiveSpikeCount = 0;
    }
  }
  return candidate;
}

async function getPancakeSwapPriceCached(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedPancakeOracle && (now - lastPancakeOracleFetch < 15000)) {
    return cachedPancakeOracle;
  }
  if (!forceRefresh && inFlightOracleFetch) {
    return inFlightOracleFetch;
  }
  const promise = fetchPancakeSwapPriceInternal(now).finally(() => {
    if (inFlightOracleFetch === promise) {
      inFlightOracleFetch = null;
    }
  });
  inFlightOracleFetch = promise;
  return promise;
}

async function fetchPancakeSwapPriceInternal(now) {
  // 1. Direct BSC RPC On-Chain Reserves (reusing shared provider with strict RPC timeout)
  try {
    const { ethers } = require('ethers');
    const provider = getSharedBscProvider();
    const pairAbi = [
      'function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
      'function token0() external view returns (address)'
    ];
    const pair = new ethers.Contract(PAIR_ADDRESS, pairAbi, provider);

    const [token0, reserves] = await withTimeout(
      Promise.all([pair.token0(), pair.getReserves()]),
      BSC_RPC_TIMEOUT_MS,
      'BSC PancakeSwap getReserves'
    );

    const isXpx0 = token0.toLowerCase() === XPX_BSC_CONTRACT.toLowerCase();
    const xpxReserve = isXpx0 ? Number(reserves[0]) / 1e6 : Number(reserves[1]) / 1e6;
    const bnbReserve = isXpx0 ? Number(reserves[1]) / 1e18 : Number(reserves[0]) / 1e18;
    if (
      !isPositiveFinite(xpxReserve) ||
      !isPositiveFinite(bnbReserve) ||
      xpxReserve < MIN_POOL_XPX_RESERVE ||
      bnbReserve < MIN_POOL_BNB_RESERVE
    ) {
      throw new Error(`PancakeSwap pool liquidity below safety threshold (xpx=${xpxReserve}, bnb=${bnbReserve})`);
    }
    const priceBnbPerXpx = bnbReserve / xpxReserve;
    if (!isPositiveFinite(priceBnbPerXpx)) {
      throw new Error(`Invalid computed priceBnbPerXpx: ${priceBnbPerXpx}`);
    }

    try {
      const resp = await fetch('https://api.dexscreener.com/latest/dex/pairs/bsc/' + PAIR_ADDRESS, {
        signal: AbortSignal.timeout(3500)
      });
      const resDex = resp.ok ? await resp.json() : null;

      const parsedUsd = resDex && resDex.pair ? parseFloat(resDex.pair.priceUsd) : NaN;
      if (resDex && resDex.pair && isPositiveFinite(parsedUsd)) {
        const liqUsd = resDex.pair.liquidity ? Number(resDex.pair.liquidity.usd) : NaN;
        const vol24h = resDex.pair.volume ? Number(resDex.pair.volume.h24) : 0;
        const chg24h = resDex.pair.priceChange ? Number(resDex.pair.priceChange.h24) : 0;
        const impliedBnbUsd = parsedUsd / priceBnbPerXpx;
        const guarded = applyOracleJumpGuard({
          ok: true,
          source: 'PancakeSwap V2 (BSC On-Chain + DexScreener)',
          pairAddress: PAIR_ADDRESS,
          baseToken: 'XPX',
          quoteToken: 'WBNB',
          xpxReserve,
          bnbReserve,
          priceBnb: priceBnbPerXpx,
          priceUsdt: parsedUsd,
          liquidityUsd: isPositiveFinite(liqUsd) ? liqUsd : (bnbReserve * impliedBnbUsd * 2),
          volume24hUsd: Number.isFinite(vol24h) ? vol24h : 0,
          priceChange24h: Number.isFinite(chg24h) ? chg24h : 0,
          isFallback: false,
          stale: false,
          timestamp: now
        });
        cachedPancakeOracle = guarded;
        if (!guarded.spikeRejected) lastPancakeOracleFetch = now;
        return cachedPancakeOracle;
      }
    } catch (_) {}

    // If DexScreener USD feed is temporarily unavailable, derive BNB/USD from recent verified cache (<10m);
    // NEVER silently serve a hardcoded $650 BNB rate with stale=false!
    const hasRecentBnbUsd = Boolean(
      cachedPancakeOracle &&
      !cachedPancakeOracle.isFallback &&
      isPositiveFinite(cachedPancakeOracle.priceUsdt) &&
      isPositiveFinite(cachedPancakeOracle.priceBnb) &&
      (now - Number(cachedPancakeOracle.timestamp || 0)) < 10 * 60 * 1000
    );
    const derivedBnbUsd = hasRecentBnbUsd
      ? (cachedPancakeOracle.priceUsdt / cachedPancakeOracle.priceBnb)
      : 650;
    const guardedOnChain = applyOracleJumpGuard({
      ok: true,
      source: hasRecentBnbUsd ? 'PancakeSwap V2 (BSC On-Chain + Cached BNB/USD)' : 'PancakeSwap V2 (BSC On-Chain, Estimated USD)',
      pairAddress: PAIR_ADDRESS,
      baseToken: 'XPX',
      quoteToken: 'WBNB',
      xpxReserve,
      bnbReserve,
      priceBnb: priceBnbPerXpx,
      priceUsdt: priceBnbPerXpx * derivedBnbUsd,
      isFallback: !hasRecentBnbUsd,
      stale: !hasRecentBnbUsd,
      timestamp: now
    });
    cachedPancakeOracle = guardedOnChain;
    if (!guardedOnChain.spikeRejected) lastPancakeOracleFetch = now;
    return cachedPancakeOracle;
  } catch (err) {
    // 2. Fallback to DexScreener API
    try {
      const resp = await fetch('https://api.dexscreener.com/latest/dex/pairs/bsc/' + PAIR_ADDRESS, {
        signal: AbortSignal.timeout(4000)
      });
      const resDex = resp.ok ? await resp.json() : null;

      if (resDex && resDex.pair) {
        const p = resDex.pair;
        const parsedBnb = parseFloat(p.priceNative);
        const parsedUsd = parseFloat(p.priceUsd);
        const liqUsd = p.liquidity ? Number(p.liquidity.usd) : 0;
        const vol24h = p.volume ? Number(p.volume.h24) : 0;
        const chg24h = p.priceChange ? Number(p.priceChange.h24) : 0;
        if (isPositiveFinite(parsedBnb) && isPositiveFinite(parsedUsd) && (!Number.isFinite(liqUsd) || liqUsd >= 100)) {
          const guardedDex = applyOracleJumpGuard({
            ok: true,
            source: 'DexScreener API (PancakeSwap V2 Failover)',
            pairAddress: PAIR_ADDRESS,
            baseToken: 'XPX',
            quoteToken: 'WBNB',
            priceBnb: parsedBnb,
            priceUsdt: parsedUsd,
            liquidityUsd: isPositiveFinite(liqUsd) ? liqUsd : 8500,
            volume24hUsd: Number.isFinite(vol24h) ? vol24h : 0,
            priceChange24h: Number.isFinite(chg24h) ? chg24h : 0,
            isFallback: false,
            stale: false,
            timestamp: now
          });
          cachedPancakeOracle = guardedDex;
          if (!guardedDex.spikeRejected) lastPancakeOracleFetch = now;
          return cachedPancakeOracle;
        }
      }
    } catch (_) {}

    // 3. Fallback to last known cache or safe baseline (marked stale/isFallback)
    if (cachedPancakeOracle) {
      return { ...cachedPancakeOracle, stale: true };
    }

    cachedPancakeOracle = {
      ok: true,
      source: 'Static Baseline (Fallback)',
      pairAddress: PAIR_ADDRESS,
      baseToken: 'XPX',
      quoteToken: 'WBNB',
      priceBnb: 0.0000001389,
      priceUsdt: 0.00007597,
      liquidityUsd: 8500,
      volume24hUsd: 500,
      priceChange24h: 0,
      isFallback: true,
      stale: true,
      timestamp: now
    };
    lastPancakeOracleFetch = now;
    return cachedPancakeOracle;
  }
}

function getCachedPancakeSwapPriceSync() {
  return cachedPancakeOracle;
}

module.exports = {
  getPancakeSwapPriceCached,
  getCachedPancakeSwapPriceSync,
  PAIR_ADDRESS,
  XPX_BSC_CONTRACT
};
