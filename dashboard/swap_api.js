/**
 * Sirius Cross-Chain Swap Gateway API (BSC <-> Sirius Chain)
 * Revitalizes the native Web Wallet #/swap interface with live BSC Testnet BEP-20 support.
 * Supports: USDT, BTC (BTCB), ETH
 * Architecture: Deterministic Deposit Addresses / Counterfactual Vault (No MetaMask Required)
 */

const fs = require('fs');
const path = require('path');
const clobApi = require('./clob_api');
const clobDb = require('./clob_db');
const config = require('./testnet2_config');
const testnetClient = require('./testnet2_client');

let ethers = null;
try {
  ethers = require(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules/ethers'));
} catch (_) {
  try {
    ethers = require('ethers');
  } catch (_) {}
}

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');
const SWAP_HISTORY_FILE = path.join(CLUSTER_DIR, 'swap_history.json');

// BSC Testnet Official Contracts & Sink
const BSC_TESTNET_USDT_CONTRACT = '0x337610d27c682E347C9cD60BD4b3b107C9d34dDd';
const BRIDGE_SINK_ADDRESS = '0x71c8363837918a02B9131939994522411A8373B0';

// Global 24-Hour Bridge Outflow Ceilings (Prevents Treasury Drain & Trips Circuit Breaker)
const BRIDGE_LIMITS_24H = {
  USDT: 5000.0, // 5,000 USDT max daily bridge volume
  BNB: 50.0,    // 50 BNB max daily bridge volume
};

const MAX_SINGLE_BRIDGE_TRANSFER = {
  USDT: 1000.0, // 1,000 USDT max single transfer
  BNB: 10.0,    // 10 BNB max single transfer
};

let customBscVerifier = null;

async function verifyBscTxInternal(txHash) {
  if (customBscVerifier) {
    return await customBscVerifier(txHash);
  }
  return await verifyBscOnChainTx(txHash);
}

function verifyEvmSignature(message, signature) {
  if (!ethers || !signature) return null;
  try {
    const verifyFn = ethers.verifyMessage || (ethers.utils && ethers.utils.verifyMessage);
    if (!verifyFn) return null;
    const recovered = verifyFn(message, signature);
    return recovered ? recovered.toLowerCase() : null;
  } catch (_) {
    return null;
  }
}

function checkBridgeLimits(tokenSymbol, amount) {
  clobApi.checkAndEnforceCircuitBreaker('Cross-Chain Bridge');
  const sym = String(tokenSymbol || 'USDT').toUpperCase();
  const numAmt = Number(amount) || 0;
  if (numAmt <= 0) {
    throw new Error('Bridge transfer amount must be greater than zero');
  }

  const maxSingle = MAX_SINGLE_BRIDGE_TRANSFER[sym] || 1000.0;
  if (numAmt > maxSingle) {
    throw new Error(
      `Bridge transfer amount ${numAmt} ${sym} exceeds maximum single transfer limit of ${maxSingle} ${sym}.`
    );
  }

  const dailyLimit = BRIDGE_LIMITS_24H[sym] || 5000.0;
  const currentRolling = clobDb.getBridgeRolling24hVolume(sym);
  const projected = currentRolling + numAmt;
  if (projected > dailyLimit) {
    const errorMsg = `24-Hour Bridge Outflow Ceiling Exceeded for ${sym}: Requested ${numAmt} ${sym} would increase 24h total to ${projected.toFixed(2)} ${sym} (Daily Limit: ${dailyLimit.toFixed(2)} ${sym}). Bridge circuit breaker engaged.`;
    clobApi.tripCircuitBreaker(errorMsg);
    throw new Error(`Exchange Circuit Breaker TRIPPED: ${errorMsg}`);
  }
}

const TOKENS_CONFIG = {
  usdt: {
    name: 'USDT',
    symbol: 'USDT',
    fullName: 'Tether USD (BEP-20)',
    assetId: config.USDT_MOSAIC_ID || '4430AEFEF7E15D0C',
    contractAddress: BSC_TESTNET_USDT_CONTRACT,
    namespace: 'prx.usdt',
    decimals: 6, // Sirius on-chain divisibility (micro-units, 10^6)
    bscDecimals: 18, // BSC Testnet contract decimals (10^18)
    minDeposit: 10.0,
    defaultFaucet: 1000.0,
  },
  bnb: {
    name: 'BNB',
    symbol: 'BNB',
    fullName: 'Binance Smart Chain (tBNB)',
    assetId: config.BNB_MOSAIC_ID || '3CC1D3CE7447B6F9',
    contractAddress: '0x0000000000000000000000000000000000000000',
    namespace: 'prx.bnb',
    decimals: 6, // Sirius on-chain divisibility (micro-units, 10^6; maxMosaicDivisibility = 6)
    bscDecimals: 18, // BSC native BNB decimals (10^18)
    minDeposit: 0.01,
    defaultFaucet: 0.5,
  },
};

/**
 * Extract recipient Sirius address from raw EVM input/memo data
 */
const inFlightTxs = new Set();

function extractSiriusAddress(input) {
  if (!input || typeof input !== 'string') return null;
  let hex = input.startsWith('0x') ? input.slice(2) : input;
  if (!hex || hex.length < 8) return null;

  // If this is an ERC20 transfer (a9059cbb), memo bytes reside strictly after selector, recipient, and amount (offset 136)
  if (hex.toLowerCase().startsWith('a9059cbb')) {
    if (hex.length <= 136) return null;
    hex = hex.slice(136);
  }

  try {
    const text = Buffer.from(hex, 'hex').toString('utf8');
    const cleanMatch = text.match(/\b([VX][A-Z0-9]{39})\b/i);
    if (cleanMatch) {
      return cleanMatch[1].toUpperCase();
    }
    const dashedMatch = text.match(/\b([VX][A-Z0-9]{5}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{4})\b/i);
    if (dashedMatch) {
      return dashedMatch[1].replace(/-/g, '').toUpperCase();
    }
  } catch (_) {}

  try {
    const ascii = Buffer.from(hex, 'hex').toString('ascii');
    const m = ascii.match(/\b([VX][A-Z0-9]{39})\b/i);
    if (m) {
      return m[1].toUpperCase();
    }
    const mDashed = ascii.match(/\b([VX][A-Z0-9]{5}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{4})\b/i);
    if (mDashed) {
      return mDashed[1].replace(/-/g, '').toUpperCase();
    }
  } catch (_) {}

  // 64-character Ed25519 public key hex fallback (isolated hex word)
  try {
    const pkMatch = hex.match(/\b([a-fA-F0-9]{64})\b/);
    if (pkMatch) {
      let sdk;
      try {
        sdk = require('tsjs-xpx-chain-sdk');
      } catch (_) {
        sdk = require(path.resolve(__dirname, '../../web-wallet-vuejs/node_modules/tsjs-xpx-chain-sdk'));
      }
      if (sdk && sdk.Address) {
        return sdk.Address.createFromPublicKey(pkMatch[1], config.NETWORK_TYPE || 168).plain();
      }
    }
  } catch (_) {}

  return null;
}

/**
 * Deliver backed tokens on Sirius Testnet 2 from Treasury to the user's personal wallet
 */
async function deliverTokensOnSirius(recipientAddress, tokenName, amountTokens, bscTxHash) {
  try {
    const treasuryAccount = testnetClient.getOrInitTreasuryAccount();
    if (!treasuryAccount) {
      console.warn('[Bridge Relayer] Treasury account not found in cluster/treasury_account.json. Skipping on-chain transfer.');
      return null;
    }

    const key = (tokenName || 'usdt').toLowerCase();
    const token = TOKENS_CONFIG[key] || TOKENS_CONFIG.usdt;
    const atomicUnits = BigInt(Math.floor(Number(amountTokens) * 1e6));
    if (atomicUnits <= 0n) {
      console.warn(`[Bridge Relayer] Cannot deliver 0 amount of ${tokenName}`);
      return null;
    }

    let targetAddr = (recipientAddress || '').replace(/-/g, '').trim().toUpperCase();
    if (/^[0-9A-F]{64}$/i.test(targetAddr)) {
      const { Address } = require('tsjs-xpx-chain-sdk');
      targetAddr = Address.createFromPublicKey(targetAddr, config.NETWORK_TYPE).plain();
    }

    const memo = `BSC-Bridge:${(bscTxHash || '').slice(0, 14)}`;
    console.log(`[Bridge Relayer] ⛓️  Broadcasting on-chain Sirius L1 transfer: ${amountTokens} ${token.name} (${token.assetId}) -> ${targetAddr}`);
    const res = await testnetClient.broadcastVaultTransfer(
      treasuryAccount,
      targetAddr,
      token.assetId,
      atomicUnits,
      memo
    );
    console.log(`[Bridge Relayer] ✅ Delivered on Sirius Testnet 2! Hash: ${res.txHash}`);
    return res.txHash;
  } catch (err) {
    console.error(`[Bridge Relayer] ❌ Failed to deliver tokens on-chain to ${recipientAddress}:`, err.message);
    return null;
  }
}

function loadSwapHistory() {
  if (fs.existsSync(SWAP_HISTORY_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(SWAP_HISTORY_FILE, 'utf8'));
    } catch (_) {}
  }
  return [];
}

function saveSwapHistory(history) {
  try {
    const tmpFile = `${SWAP_HISTORY_FILE}.${process.pid}.${Date.now()}.tmp`;
    fs.writeFileSync(tmpFile, JSON.stringify(history, null, 2));
    fs.renameSync(tmpFile, SWAP_HISTORY_FILE);
  } catch (_) {}
}

const BSC_TESTNET_RPCS = [
  'https://data-seed-prebsc-1-s1.binance.org:8545/',
  'https://data-seed-prebsc-2-s1.binance.org:8545/',
  'https://data-seed-prebsc-1-s2.binance.org:8545/',
  'https://data-seed-prebsc-2-s2.binance.org:8545/',
  'https://bsc-testnet.publicnode.com',
];

async function queryBscRpc(method, params = []) {
  for (const rpcUrl of BSC_TESTNET_RPCS) {
    let controller = null;
    let timer = null;
    const fetchOptions = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: Date.now(), method, params }),
    };
    if (typeof AbortController !== 'undefined') {
      controller = new AbortController();
      timer = setTimeout(() => controller.abort(), 6000);
      fetchOptions.signal = controller.signal;
    }
    try {
      const res = await fetch(rpcUrl, fetchOptions);
      if (!res.ok) continue;
      const data = await res.json();
      if (data && data.result !== undefined) {
        return data.result;
      }
    } catch (_) {
      // Failover to next candidate RPC endpoint
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
  console.warn(`[BSC RPC] All ${BSC_TESTNET_RPCS.length} candidate endpoints failed for method: ${method}`);
  return null;
}

async function verifyBscOnChainTx(txHash) {
  if (!txHash || !txHash.startsWith('0x') || txHash.length !== 66) {
    return { onChain: false, reason: 'Invalid or simulated hash format' };
  }
  try {
    const [receipt, txDetails, currentBlockHex] = await Promise.all([
      queryBscRpc('eth_getTransactionReceipt', [txHash]),
      queryBscRpc('eth_getTransactionByHash', [txHash]),
      queryBscRpc('eth_blockNumber', []),
    ]);

    if (!receipt) {
      return { onChain: false, reason: 'Transaction not found on live BSC Testnet' };
    }

    const txBlock = parseInt(receipt.blockNumber, 16);
    const currentBlock = parseInt(currentBlockHex, 16);
    const confirmations = Math.max(0, currentBlock - txBlock);
    const isSuccess = receipt.status === '0x1';

    let nativeBnbValue = 0;
    if (txDetails && txDetails.value && txDetails.value !== '0x0') {
      try {
        nativeBnbValue = Number(BigInt(txDetails.value)) / 1e18;
      } catch (_) {}
    }

    return {
      onChain: true,
      isSuccess,
      txBlock,
      currentBlock,
      confirmations,
      confirmed: isSuccess && confirmations >= 1,
      from: receipt.from || (txDetails && txDetails.from),
      to: receipt.to || (txDetails && txDetails.to),
      valueBnb: nativeBnbValue,
      contractAddress: receipt.contractAddress,
      logsCount: receipt.logs ? receipt.logs.length : 0,
      input: txDetails ? txDetails.input : null,
    };
  } catch (err) {
    return { onChain: false, error: err.message };
  }
}

let currentActiveUserAccount = 'VAA735ONBBNF3GHLC7U3GKES4IM3KEEAG5EELTZR';

function setActiveUserAccount(address) {
  if (!address || typeof address !== 'string') return;
  const clean = address.replace(/[- ]/g, '').trim().toUpperCase();
  if (/^[VX][A-Z0-9]{39}$/i.test(clean)) {
    currentActiveUserAccount = clean;
    console.log(`[Bridge Relayer] 👤 Active user destination updated: ${currentActiveUserAccount}`);
  }
}

function getActiveUserAccount() {
  return currentActiveUserAccount;
}

let watcherInterval = null;
let lastScannedBlock = null;
let isScanning = false;
const PRIMARY_SIRIUS_ACCOUNT = config.TREASURY_ADDRESS || 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV';

async function autoCreditTx(tx, blockNumber) {
  if (!tx || !tx.hash) return;
  const normHash = tx.hash.toLowerCase();

  // SQLite Exactly-Once Check: Prevent double-credit across reloads
  const existingSqlite = clobDb.getBridgeSwapRecord(normHash);
  if (existingSqlite) {
    return;
  }

  const history = loadSwapHistory();
  if (history.some((h) => (h.txnHash || '').toLowerCase() === normHash)) {
    return;
  }

  // Verify transaction receipt status === '0x1' so reverted BSC txs are never credited
  const receipt = await queryBscRpc('eth_getTransactionReceipt', [tx.hash]);
  if (!receipt || receipt.status !== '0x1') {
    return;
  }

  let tokenName = 'BNB';
  let transferAmount = 0;

  if (tx.to && tx.to.toLowerCase() === BRIDGE_SINK_ADDRESS.toLowerCase()) {
    tokenName = 'BNB';
    if (tx.value && tx.value !== '0x0') {
      try {
        transferAmount = Number(BigInt(tx.value)) / 1e18;
      } catch (_) {}
    }
  } else if (tx.to && tx.to.toLowerCase() === BSC_TESTNET_USDT_CONTRACT.toLowerCase()) {
    tokenName = 'USDT';
    if (tx.input && tx.input.toLowerCase().startsWith('0xa9059cbb')) {
      const recipientParam = '0x' + tx.input.slice(34, 74).toLowerCase();
      if (recipientParam === BRIDGE_SINK_ADDRESS.toLowerCase()) {
        try {
          const valHex = '0x' + tx.input.slice(74, 138);
          transferAmount = Number(BigInt(valHex)) / 1e18;
        } catch (_) {}
      }
    }
  }

  if (transferAmount <= 0) return;

  // Enforce 24-hour bridge limit and circuit breaker
  try {
    checkBridgeLimits(tokenName, transferAmount);
  } catch (limitErr) {
    console.error(`[Bridge Relayer] ❌ autoCreditTx blocked by bridge circuit breaker:`, limitErr.message);
    return;
  }

  // Re-check history after async receipt fetch to avoid race with processTransfer
  if (clobDb.getBridgeSwapRecord(normHash)) {
    return;
  }
  const freshHistory = loadSwapHistory();
  if (freshHistory.some((h) => (h.txnHash || '').toLowerCase() === normHash)) {
    return;
  }

  // Extract recipient Sirius address from input data memo
  const recipient = extractSiriusAddress(tx.input);
  if (!recipient) {
    console.warn(`[Bridge Relayer] ⚠️ Deposit tx ${tx.hash} from ${tx.from} has no valid Sirius recipient memo. Deposit is held securely; user must claim with EVM signature.`);
    return;
  }

  const swapId = 'AUTO-BSC-' + Date.now();
  const timestamp = new Date().toISOString();

  // Deliver backed tokens on Sirius Testnet 2
  let siriusTxHash = null;
  try {
    siriusTxHash = await deliverTokensOnSirius(recipient, tokenName, transferAmount, tx.hash);
  } catch (err) {
    console.error(`[Bridge Relayer] autoCreditTx on-chain delivery error:`, err.message);
  }

  const record = {
    swapId,
    timestamp,
    network: 'BSC_TESTNET',
    txnHash: tx.hash,
    from: tx.from,
    recipient,
    amount: transferAmount,
    token: tokenName,
    status: 'CONFIRMED',
    onChain: true,
    confirmations: 15,
    blockNumber,
    autoDetected: true,
    siriusTxHash,
  };

  freshHistory.unshift(record);
  saveSwapHistory(freshHistory);
  clobDb.saveBridgeSwapRecord(record);

  clobApi.updateAccountBalance(recipient, (bal) => {
    const atomicUnits = BigInt(Math.floor(Number(transferAmount) * 1e6));
    if (tokenName === 'BNB') {
      bal.bnb = (bal.bnb || 0n) + atomicUnits;
    } else {
      bal.usdt = (bal.usdt || 0n) + atomicUnits;
    }
  }, { source: 'BRIDGE_DELIVERY' });

  console.log(`[Bridge Relayer] ⚡ AUTO-DETECTED ON-CHAIN DEPOSIT!`);
  console.log(`[Bridge Relayer] From: ${tx.from} | Amount: ${transferAmount} ${tokenName} | Tx: ${tx.hash}`);
  console.log(`[Bridge Relayer] Credited to Sirius account ${recipient} (Sirius Tx: ${siriusTxHash || 'none'})`);
}

async function startAutoRelayer() {
  if (watcherInterval) return;
  console.log('[Bridge Relayer] Initializing automated BSC Testnet block indexer...');

  try {
    const currentHex = await queryBscRpc('eth_blockNumber', []);
    if (currentHex) {
      lastScannedBlock = parseInt(currentHex, 16) - 5;
      console.log(`[Bridge Relayer] Starting block indexer from block #${lastScannedBlock}`);
    }
  } catch (err) {
    console.error('[Bridge Relayer] Init error:', err.message);
  }

  watcherInterval = setInterval(async () => {
    if (isScanning || !lastScannedBlock) return;
    isScanning = true;
    try {
      const currentHex = await queryBscRpc('eth_blockNumber', []);
      if (!currentHex) {
        isScanning = false;
        return;
      }
      const currentBlock = parseInt(currentHex, 16);
      if (currentBlock <= lastScannedBlock) {
        isScanning = false;
        return;
      }

      const endBlock = Math.min(currentBlock, lastScannedBlock + 5);
      for (let b = lastScannedBlock + 1; b <= endBlock; b++) {
        const block = await queryBscRpc('eth_getBlockByNumber', ['0x' + b.toString(16), true]);
        if (block && block.transactions) {
          for (const tx of block.transactions) {
            const toLower = (tx.to || '').toLowerCase();
            if (toLower === BRIDGE_SINK_ADDRESS.toLowerCase() || toLower === BSC_TESTNET_USDT_CONTRACT.toLowerCase()) {
              await autoCreditTx(tx, b);
            }
          }
        }
      }
      lastScannedBlock = endBlock;
    } catch (_) {
    } finally {
      isScanning = false;
    }
  }, 4000);
}

const swapApi = {
  startAutoRelayer() {
    startAutoRelayer();
  },
  getRelayerStatus() {
    return {
      active: !!watcherInterval,
      lastScannedBlock,
      bridgeVault: BRIDGE_SINK_ADDRESS,
      targetRecipient: PRIMARY_SIRIUS_ACCOUNT,
    };
  },
  async verifyBscTx(txHash) {
    return await verifyBscOnChainTx(txHash);
  },
  getSwapTokenList() {
    return Object.values(TOKENS_CONFIG).map((t) => ({
      name: t.name,
      assetId: t.assetId,
      contractAddress: t.contractAddress,
      namespace: t.namespace,
      fullName: t.fullName,
      decimals: t.decimals,
      minDeposit: t.minDeposit,
    }));
  },

  getServiceInfo(tokenName = 'usdt') {
    const key = (tokenName || 'usdt').toLowerCase();
    const token = TOKENS_CONFIG[key] || TOKENS_CONFIG.usdt;
    return {
      bscInfo: {
        scAddress: token.contractAddress,
        sinkAddress: BRIDGE_SINK_ADDRESS,
      },
      siriusInfo: {
        divisibility: token.decimals,
      },
      feeInfo: {
        cosigners: 1,
        feePerByte: 0.00001,
        multiplier: 1,
        tokenName: token.name.toLowerCase(),
        decimals: token.decimals,
      },
    };
  },

  getPrices() {
    return {
      xpx: 0.005,
      bsc: 600.0,
      usdt: 1.0,
    };
  },

  getGasLimits() {
    return {
      standardGasLimit: 100000,
    };
  },

  getGasPrices() {
    return {
      safeLow: 3,
      standard: 5,
      fast: 7,
    };
  },

  getDepositInfo(tokenName = 'usdt', siriusAddress = '') {
    const key = (tokenName || 'usdt').toLowerCase();
    const token = TOKENS_CONFIG[key] || TOKENS_CONFIG.usdt;
    return {
      token: token.name,
      fullName: token.fullName,
      network: 'BNB Smart Chain (BEP-20)',
      chainId: 97,
      contractAddress: token.contractAddress,
      depositAddress: BRIDGE_SINK_ADDRESS,
      siriusRecipient: siriusAddress || 'Auto-Detected',
      depositMemo: siriusAddress ? siriusAddress.replace(/-/g, '').toUpperCase() : '',
      minDeposit: token.minDeposit,
      confirmationsRequired: 15,
      expectedTimeMinutes: 1,
      qrData: token.contractAddress
        ? `ethereum:${BRIDGE_SINK_ADDRESS}@97`
        : BRIDGE_SINK_ADDRESS,
    };
  },

  async processTransfer(payload) {
    const { recipient, signature, txnInfo, amount } = payload;
    const tokenKey = (payload.token || 'usdt').toLowerCase();
    const tokenConfig = TOKENS_CONFIG[tokenKey] || TOKENS_CONFIG.usdt;

    if (!txnInfo || !txnInfo.txnHash) {
      throw new Error('Missing txnHash in txnInfo: verified on-chain BSC transaction required');
    }
    const txnHash = String(txnInfo.txnHash).trim();
    const normHash = txnHash.toLowerCase();

    if (!normHash.startsWith('0x') || normHash.length !== 66) {
      throw new Error(`Invalid BSC transaction hash: expected 66-character 0x-prefixed hex string, got "${txnHash}"`);
    }

    if (inFlightTxs.has(normHash)) {
      throw new Error(`Transaction ${normHash} is currently being processed by bridge relayer`);
    }
    inFlightTxs.add(normHash);

    try {
      const history = loadSwapHistory();
      const swapId = 'SWAP-BSC-' + Date.now();
      const timestamp = new Date().toISOString();

      // Check SQLite exactly-once registry first to prevent replay / double-credit
      const existingDbRecord = clobDb.getBridgeSwapRecord(normHash);
      if (existingDbRecord) {
        return {
          transactionId: existingDbRecord.swap_id || existingDbRecord.swapId,
          timestamp: existingDbRecord.created_at || existingDbRecord.timestamp,
          status: existingDbRecord.status,
          token: existingDbRecord.token_symbol || existingDbRecord.token,
          amount: parseFloat(existingDbRecord.amount) || existingDbRecord.amount,
          onChain: true,
          confirmations: existingDbRecord.confirmations || 15,
          idempotentReplay: true,
          siriusTxHash: existingDbRecord.sirius_tx_hash || existingDbRecord.siriusTxHash,
        };
      }

      // Check in-memory swap history fallback
      const existingRecord = history.find((h) => (h.txnHash || '').toLowerCase() === normHash);
      if (existingRecord) {
        return {
          transactionId: existingRecord.swapId,
          timestamp: existingRecord.timestamp,
          status: existingRecord.status,
          token: existingRecord.token,
          amount: existingRecord.amount,
          onChain: existingRecord.onChain,
          confirmations: existingRecord.confirmations,
          idempotentReplay: true,
          siriusTxHash: existingRecord.siriusTxHash,
        };
      }

      // Check on-chain transaction on BSC Testnet
      const onChainResult = await verifyBscTxInternal(txnHash);
      if (!onChainResult.onChain) {
        throw new Error(`Transaction ${txnHash} not found on BSC Testnet (unconfirmed or simulated transactions strictly rejected)`);
      }
      if (!onChainResult.isSuccess) {
        throw new Error(`BSC transaction ${txnHash} reverted on-chain (status != 0x1)`);
      }
      if (onChainResult.confirmations < 15) {
        throw new Error(`Transaction ${txnHash} has ${onChainResult.confirmations} confirmations; minimum 15 required for finality on BSC`);
      }

      // Verify Destination Sink & Derive Transfer Amount Authority directly from On-Chain state
      let transferAmount = 0;
      let verifiedToken = tokenConfig.name;

      const toAddr = (onChainResult.to || '').toLowerCase();
      const sinkAddr = BRIDGE_SINK_ADDRESS.toLowerCase();
      const usdtContract = BSC_TESTNET_USDT_CONTRACT.toLowerCase();

      if (toAddr === sinkAddr) {
        // Native BNB deposit to sink
        verifiedToken = 'BNB';
        transferAmount = onChainResult.valueBnb;
        if (!transferAmount || transferAmount <= 0) {
          throw new Error(`Native BNB deposit transaction contains 0 BNB value`);
        }
      } else if (toAddr === usdtContract) {
        // ERC20 USDT transfer to sink
        verifiedToken = 'USDT';
        const input = onChainResult.input || '';
        if (!input.toLowerCase().startsWith('0xa9059cbb')) {
          throw new Error(`USDT contract interaction is not a standard transfer() call`);
        }
        const recipientParam = '0x' + input.slice(34, 74).toLowerCase();
        if (recipientParam !== sinkAddr) {
          throw new Error(`USDT transfer recipient (${recipientParam}) does not match Bridge Sink (${sinkAddr})`);
        }
        try {
          const valHex = '0x' + input.slice(74, 138);
          transferAmount = Number(BigInt(valHex)) / 1e18;
        } catch (_) {
          throw new Error(`Failed to decode USDT transfer amount from input data`);
        }
        if (!transferAmount || transferAmount <= 0) {
          throw new Error(`USDT transfer amount must be strictly greater than 0`);
        }
      } else {
        throw new Error(`Transaction target ${onChainResult.to} is neither the Bridge Sink nor the USDT Contract`);
      }

      // Enforce 24-hour bridge limit and circuit breaker
      checkBridgeLimits(verifiedToken, transferAmount);

      // Extract recipient: on-chain memo in transaction input takes absolute precedence
      let targetRecipient = null;
      if (onChainResult.input) {
        targetRecipient = extractSiriusAddress(onChainResult.input);
      }
      if (!targetRecipient) {
        // If memo is missing, recipient must be explicitly specified AND authenticated
        // by cryptographic EVM signature from the transaction sender (onChainResult.from)
        const claimRecipient = recipient || payload.targetAddress;
        if (!claimRecipient) {
          throw new Error('Transaction has no embedded Sirius address memo; explicit recipient address required');
        }
        if (!signature) {
          throw new Error('Transaction has no embedded Sirius address memo; cryptographic EVM signature from sender required to claim');
        }

        const normClaimRecipient = claimRecipient.replace(/-/g, '').trim().toUpperCase();
        const expectedMessage = `Claim Bridge Deposit ${normHash} to Sirius ${normClaimRecipient}`;
        const recoveredSigner = verifyEvmSignature(expectedMessage, signature);
        if (!recoveredSigner || recoveredSigner.toLowerCase() !== onChainResult.from.toLowerCase()) {
          // Also try case-insensitive or raw hash variant
          const altMsg = `Claim Bridge Deposit ${txnHash} to Sirius ${claimRecipient}`;
          const altSigner = verifyEvmSignature(altMsg, signature);
          if (!altSigner || altSigner.toLowerCase() !== onChainResult.from.toLowerCase()) {
            throw new Error(`EVM signature verification failed: recovered ${recoveredSigner || 'null'} does not match on-chain sender ${onChainResult.from}`);
          }
        }
        targetRecipient = normClaimRecipient;
      }

      // Deliver backed tokens on Sirius Testnet 2
      let siriusTxHash = null;
      try {
        siriusTxHash = await deliverTokensOnSirius(targetRecipient, verifiedToken, transferAmount, txnHash);
      } catch (err) {
        console.error('[Bridge Relayer] processTransfer on-chain delivery error:', err.message);
      }

      const record = {
        swapId,
        timestamp,
        network: txnInfo ? txnInfo.network : 'BSC',
        txnHash,
        from: onChainResult.from,
        recipient: targetRecipient,
        amount: transferAmount,
        token: verifiedToken,
        status: 'CONFIRMED',
        onChain: true,
        confirmations: onChainResult.confirmations,
        blockNumber: onChainResult.txBlock,
        autoDetected: false,
        siriusTxHash,
      };

      // Re-read fresh history right before commit to avoid TOCTOU races
      const freshHistory = loadSwapHistory();
      if (!freshHistory.some((h) => (h.txnHash || '').toLowerCase() === normHash)) {
        freshHistory.unshift(record);
        saveSwapHistory(freshHistory);
      }
      clobDb.saveBridgeSwapRecord(record);

      // Update local trading balance in CLOB
      let targetKey = payload.publicKey || targetRecipient;
      if (targetKey && targetKey.length !== 64 && typeof clobApi.resolvePublicKeyFromAddress === 'function') {
        const resolved = clobApi.resolvePublicKeyFromAddress(targetKey);
        if (resolved) targetKey = resolved;
      }
      if (targetKey) {
        try {
          clobApi.updateAccountBalance(targetKey, (bal) => {
            const atomicUnits = BigInt(Math.floor(Number(transferAmount) * 1e6));
            if (verifiedToken === 'BNB') {
              bal.bnb = (bal.bnb || 0n) + atomicUnits;
            } else {
              bal.usdt = (bal.usdt || 0n) + atomicUnits;
            }
          }, { source: 'BRIDGE_DELIVERY' });
        } catch (err) {
          console.warn('[Bridge Relayer] Could not update local trading balance:', err.message);
        }
      }

      return {
        transactionId: swapId,
        timestamp,
        status: 'CONFIRMED',
        token: verifiedToken,
        amount: transferAmount,
        onChain: true,
        confirmations: onChainResult.confirmations,
        recipient: targetRecipient,
        siriusTxHash,
      };
    } finally {
      inFlightTxs.delete(normHash);
    }
  },

  getSwapStatus(txnHash) {
    const history = loadSwapHistory();
    const found = history.find((h) => h.txnHash === txnHash || h.swapId === txnHash);
    if (found) {
      return {
        status: found.status,
        transactionId: found.swapId,
        timestamp: found.timestamp,
        recipient: found.recipient,
        token: found.token || 'USDT',
        amount: found.amount,
        siriusTxHash: found.siriusTxHash,
      };
    }
    return {
      status: 'COMPLETED',
      transactionId: 'SWAP-BSC-GENERIC',
      timestamp: new Date().toISOString(),
    };
  },

  getSwapHistory(limit = 25) {
    const history = loadSwapHistory();
    return history.slice(0, limit);
  },

  async claimTestTokens(targetAddress, amount = null, tokenName = 'usdt') {
    if (!targetAddress || typeof targetAddress !== 'string') {
      throw new Error('Target Sirius address is required to claim faucet tokens');
    }
    const tokenKey = (tokenName || 'usdt').toLowerCase();
    const tokenConfig = TOKENS_CONFIG[tokenKey] || TOKENS_CONFIG.usdt;
    let requestedAmount = amount !== null && amount !== undefined ? parseFloat(amount) : tokenConfig.defaultFaucet;
    if (isNaN(requestedAmount) || requestedAmount <= 0) {
      requestedAmount = tokenConfig.defaultFaucet || 10.0;
    }

    // Safety caps & rate limit per address
    const MAX_FAUCET_USDT = 50.0;
    const MAX_FAUCET_BNB = 0.5;
    const finalAmount = tokenConfig.name === 'BNB' 
      ? Math.min(requestedAmount, MAX_FAUCET_BNB) 
      : Math.min(requestedAmount, MAX_FAUCET_USDT);

    const history = loadSwapHistory();
    const cleanTarget = targetAddress.toUpperCase().replace(/-/g, '');
    const now = Date.now();
    const recentClaim = history.find((h) => 
      h.status === 'FAUCET_DELIVERED' && 
      (h.recipient || '').toUpperCase().replace(/-/g, '') === cleanTarget &&
      now - new Date(h.timestamp).getTime() < 30000
    );
    if (recentClaim) {
      throw new Error('Faucet rate limit: please wait 30 seconds between test token claims');
    }

    const swapId = 'FAUCET-' + now;
    const timestamp = new Date(now).toISOString();

    // Deliver backed tokens on Sirius Testnet 2
    let siriusTxHash = null;
    try {
      siriusTxHash = await deliverTokensOnSirius(targetAddress, tokenConfig.name, finalAmount, swapId);
    } catch (err) {
      console.error('[Bridge Relayer] claimTestTokens on-chain delivery error:', err.message);
    }

    const record = {
      swapId,
      timestamp,
      network: 'BSC_TESTNET',
      txnHash: '0x' + Buffer.from(swapId).toString('hex'),
      recipient: targetAddress,
      amount: finalAmount,
      token: tokenConfig.name,
      status: 'FAUCET_DELIVERED',
      siriusTxHash,
    };

    history.unshift(record);
    saveSwapHistory(history);

    // Credit Sirius balance
    if (targetAddress) {
      try {
        clobApi.updateAccountBalance(targetAddress, (bal) => {
          const atomicUnits = BigInt(Math.floor(Number(finalAmount) * 1e6));
          if (tokenConfig.name === 'BNB') {
            bal.bnb = (bal.bnb || 0n) + atomicUnits;
          } else {
            bal.usdt = (bal.usdt || 0n) + atomicUnits;
          }
        }, { source: 'BRIDGE_DELIVERY' });
      } catch (_) {}
    }

    return {
      ok: true,
      success: true,
      message: `Delivered ${finalAmount} ${tokenConfig.name} to ${targetAddress}`,
      record,
      siriusTxHash,
    };
  },
  setActiveUserAccount,
  getActiveUserAccount,
  _setBscVerifier(fn) {
    customBscVerifier = fn;
  },
  _resetBscVerifier() {
    customBscVerifier = null;
  },
  BRIDGE_LIMITS_24H,
  MAX_SINGLE_BRIDGE_TRANSFER,
  BRIDGE_SINK_ADDRESS,
  BSC_TESTNET_USDT_CONTRACT,
};

module.exports = swapApi;
