/**
 * SiriusCLOB — PancakeSwap V2 BSC On-Chain Oracle Client
 * Queries real-time XPX/WBNB reserves from PancakeSwap V2 Pair:
 *   Pair Address: 0x439B8E49F2b0d9C9824d5AB641D677511f362dA5
 *   Base: XPX (0x6f3aaf802f57d045efdd2ac9c06d8879305542af)
 *   Quote: WBNB (0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c)
 */

let cachedPancakeOracle = null;
let lastPancakeOracleFetch = 0;

const PAIR_ADDRESS = '0x439B8E49F2b0d9C9824d5AB641D677511f362dA5';
const XPX_BSC_CONTRACT = '0x6f3aaf802f57d045efdd2ac9c06d8879305542af';

async function getPancakeSwapPriceCached(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && cachedPancakeOracle && (now - lastPancakeOracleFetch < 15000)) {
    return cachedPancakeOracle;
  }

  // 1. Direct BSC RPC On-Chain Reserves
  try {
    const { ethers } = require('ethers');
    const provider = new ethers.JsonRpcProvider('https://bsc-dataseed.binance.org/', undefined, { staticNetwork: true });
    const pairAbi = [
      'function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)',
      'function token0() external view returns (address)'
    ];
    const pair = new ethers.Contract(PAIR_ADDRESS, pairAbi, provider);

    const [token0, reserves] = await Promise.all([
      pair.token0(),
      pair.getReserves()
    ]);

    const isXpx0 = token0.toLowerCase() === XPX_BSC_CONTRACT.toLowerCase();
    const xpxReserve = isXpx0 ? Number(reserves[0]) / 1e6 : Number(reserves[1]) / 1e6;
    const bnbReserve = isXpx0 ? Number(reserves[1]) / 1e18 : Number(reserves[0]) / 1e18;
    const priceBnbPerXpx = bnbReserve / xpxReserve;

    let bnbUsd = 650;
    try {
      const resDex = await fetch('https://api.dexscreener.com/latest/dex/pairs/bsc/' + PAIR_ADDRESS, {
        signal: AbortSignal.timeout(3500)
      }).then(r => r.json());

      if (resDex && resDex.pair && resDex.pair.priceUsd) {
        cachedPancakeOracle = {
          ok: true,
          source: 'PancakeSwap V2 (BSC On-Chain + DexScreener)',
          pairAddress: PAIR_ADDRESS,
          baseToken: 'XPX',
          quoteToken: 'WBNB',
          xpxReserve,
          bnbReserve,
          priceBnb: priceBnbPerXpx,
          priceUsdt: parseFloat(resDex.pair.priceUsd),
          liquidityUsd: resDex.pair.liquidity ? resDex.pair.liquidity.usd : (bnbReserve * bnbUsd * 2),
          volume24hUsd: resDex.pair.volume ? resDex.pair.volume.h24 : 0,
          priceChange24h: resDex.pair.priceChange ? resDex.pair.priceChange.h24 : 0,
          timestamp: now
        };
        lastPancakeOracleFetch = now;
        return cachedPancakeOracle;
      }
    } catch (_) {}

    cachedPancakeOracle = {
      ok: true,
      source: 'PancakeSwap V2 (BSC On-Chain)',
      pairAddress: PAIR_ADDRESS,
      baseToken: 'XPX',
      quoteToken: 'WBNB',
      xpxReserve,
      bnbReserve,
      priceBnb: priceBnbPerXpx,
      priceUsdt: priceBnbPerXpx * bnbUsd,
      timestamp: now
    };
    lastPancakeOracleFetch = now;
    return cachedPancakeOracle;
  } catch (err) {
    // 2. Fallback to DexScreener API
    try {
      const resDex = await fetch('https://api.dexscreener.com/latest/dex/pairs/bsc/' + PAIR_ADDRESS, {
        signal: AbortSignal.timeout(4000)
      }).then(r => r.json());

      if (resDex && resDex.pair) {
        const p = resDex.pair;
        cachedPancakeOracle = {
          ok: true,
          source: 'DexScreener API (PancakeSwap V2 Failover)',
          pairAddress: p.pairAddress,
          baseToken: 'XPX',
          quoteToken: 'WBNB',
          priceBnb: parseFloat(p.priceNative || '0.0000001389'),
          priceUsdt: parseFloat(p.priceUsd || '0.00007597'),
          liquidityUsd: p.liquidity ? p.liquidity.usd : 8500,
          volume24hUsd: p.volume ? p.volume.h24 : 500,
          priceChange24h: p.priceChange ? p.priceChange.h24 : 0,
          timestamp: now
        };
        lastPancakeOracleFetch = now;
        return cachedPancakeOracle;
      }
    } catch (_) {}

    // 3. Fallback to last known cache or safe baseline
    if (cachedPancakeOracle) {
      return cachedPancakeOracle;
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
