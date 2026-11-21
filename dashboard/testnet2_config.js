/**
 * Sirius Network Configuration for Standalone CLOB Relayer
 * Supports seamless switching between Sirius Testnet 2 (168) and Sirius Mainnet (184).
 */
const path = require('path');

let Address = null;
try {
  Address = require('tsjs-xpx-chain-sdk').Address;
} catch (_) {}

function deriveAddress(pubKeyHex, netType, fallback) {
  if (Address && pubKeyHex && pubKeyHex.length === 64) {
    try {
      return Address.createFromPublicKey(pubKeyHex, netType).plain();
    } catch (_) {}
  }
  return fallback;
}

const ROOT_DIR = path.resolve(__dirname, '..');
const STATE_DIR = process.env.CLOB_STATE_DIR || path.join(ROOT_DIR, 'cluster');

const NETWORK_PROFILES = {
  testnet2: {
    NETWORK_NAME: 'publicTest',
    NODE_URL: 'https://api-2.testnet2.xpxsirius.io',
    FALLBACK_NODE_URLS: [
      'https://api-2.testnet2.xpxsirius.io',
      'https://api-1.testnet2.xpxsirius.io',
      'http://api-2.testnet2.xpxsirius.io:3000',
      'http://api-1.testnet2.xpxsirius.io:3000',
    ],
    NETWORK_TYPE: 168, // 0xA8 NetworkType.TEST_NET
    GENERATION_HASH: '56D112C98F7A7E34D1AEDC4BD01BC06CA2276DD546A93E36690B785E82439CA9',
    XPX_MOSAIC_ID: '13BFC518E40549D7', // Canonical Testnet 2 XPX Mosaic ID
    XPX_NAMESPACE_ID: 'bffb42a19116bdf6', // prx.xpx namespace
    USDT_MOSAIC_ID: '4430AEFEF7E15D0C', // Canonical Testnet 2 USDT Mosaic ID (owned by Treasury VC46DF...)
    BNB_MOSAIC_ID: '3CC1D3CE7447B6F9',  // Canonical Testnet 2 BNB Mosaic ID (owned by Treasury VC46DF...)
    EXPLORER_BASE_URL: 'https://proximax-foundry.github.io/web-explorer-vuejs/#',
    CONFIRMATION_DEPTH: 1,
    FAUCET_ENABLED: true,
  },
  mainnet: {
    NETWORK_NAME: 'mainnet',
    NODE_URL: 'https://betelgeuse.xpxsirius.io',
    FALLBACK_NODE_URLS: [
      'https://betelgeuse.xpxsirius.io',
      'https://arcturus.xpxsirius.io',
      'https://aldebaran.xpxsirius.io',
    ],
    NETWORK_TYPE: 184, // 0xB8 NetworkType.MAIN_NET
    GENERATION_HASH: '10540AD3A1BF46B1A05D8B1CF0252BC9FB2E0B53CFD748262B0CE341CEAFEB6B',
    XPX_MOSAIC_ID: '402B2F579FAEBC59', // Official Sirius Mainnet Genesis XPX Mosaic ID (prx.xpx)
    XPX_NAMESPACE_ID: 'bffb42a19116bdf6',
    USDT_MOSAIC_ID: '7525381A171D1E44', // Mainnet Wrapped USDT
    BNB_MOSAIC_ID: '1E5C5FADEC7B34DD',  // Mainnet Wrapped BNB
    EXPLORER_BASE_URL: 'https://explorer.xpxsirius.io/#',
    CONFIRMATION_DEPTH: 2,
    FAUCET_ENABLED: false, // Faucet permanently disabled on Mainnet
  },
};

const activeEnv = (process.env.SIRIUS_NETWORK || 'testnet2').toLowerCase();
const profile = NETWORK_PROFILES[activeEnv] || NETWORK_PROFILES.testnet2;
const networkType = Number(process.env.SIRIUS_NETWORK_TYPE || profile.NETWORK_TYPE);

const SUPERCONTRACT_PUBKEY = process.env.SIRIUS_SUPERCONTRACT_PUBKEY || 'c589bfef210489d3a1bf46b1a05d8b1cf0252bc9fb2e0b53cfd748262b0ce341';
const SUPERCONTRACT_ADDRESS = deriveAddress(
  SUPERCONTRACT_PUBKEY,
  networkType,
  networkType === 184 ? 'XAEKLE3WB6XJLFW75JGJJ7Y3DVQWCWX775NQNWPP' : 'VAEKLE3WB6XJLFW75JGJJ7Y3DVQWCWX774DDSJ2F'
);

const FEE_COLLECTOR_PUBKEY = process.env.SIRIUS_FEE_COLLECTOR_PUBKEY || '04EF8D54AAEFBC4746257723FD1EFB7B37F30452DE8BD022F5FB3CABC5246A64';
const FEE_COLLECTOR_ADDRESS = process.env.SIRIUS_FEE_COLLECTOR_ADDRESS || deriveAddress(
  FEE_COLLECTOR_PUBKEY,
  networkType,
  networkType === 184 ? 'XB3QOFNGGL7DENKPBW2RO3OVIXLOVLQ5F537V4JZ' : 'VB3QOFNGGL7DENKPBW2RO3OVIXLOVLQ5F5WLG4XE'
);

module.exports = {
  NETWORK_ENV: activeEnv,
  NETWORK_NAME: profile.NETWORK_NAME,
  // Network parameters
  NODE_URL: process.env.SIRIUS_NODE_URL || profile.NODE_URL,
  FALLBACK_NODE_URLS: process.env.SIRIUS_FALLBACK_NODE_URLS
    ? process.env.SIRIUS_FALLBACK_NODE_URLS.split(',').map((s) => s.trim()).filter(Boolean)
    : Array.from(new Set([process.env.SIRIUS_NODE_URL || profile.NODE_URL, ...(profile.FALLBACK_NODE_URLS || [])])),
  NETWORK_TYPE: networkType,
  GENERATION_HASH: process.env.SIRIUS_GENERATION_HASH || profile.GENERATION_HASH,
  DOMAIN_SEPARATOR: process.env.SIRIUS_DOMAIN_SEPARATOR || `${networkType}:SiriusCLOB-v1`,
  EXPLORER_BASE_URL: process.env.SIRIUS_EXPLORER_URL || profile.EXPLORER_BASE_URL,
  CONFIRMATION_DEPTH: Number(process.env.SIRIUS_CONFIRMATION_DEPTH || profile.CONFIRMATION_DEPTH || 1),
  FAUCET_ENABLED: process.env.SIRIUS_FAUCET_ENABLED !== undefined ? process.env.SIRIUS_FAUCET_ENABLED === 'true' : profile.FAUCET_ENABLED,

  // Canonical Network Mosaics
  XPX_MOSAIC_ID: (process.env.SIRIUS_XPX_MOSAIC_ID || profile.XPX_MOSAIC_ID).toUpperCase(),
  XPX_NAMESPACE_ID: (process.env.SIRIUS_XPX_NAMESPACE_ID || profile.XPX_NAMESPACE_ID).toLowerCase(),
  USDT_MOSAIC_ID: (process.env.TESTNET_USDT_MOSAIC_ID || process.env.SIRIUS_USDT_MOSAIC_ID || profile.USDT_MOSAIC_ID).toUpperCase(),
  BNB_MOSAIC_ID: (process.env.TESTNET_BNB_MOSAIC_ID || process.env.SIRIUS_BNB_MOSAIC_ID || profile.BNB_MOSAIC_ID).toUpperCase(),

  // Internal WASM SuperContract Mosaic Constants (translated at L1 boundary)
  WASM_XPX_HEX: '0DC67FBE1CAD29E3',
  WASM_USDT_HEX: '10112CBA1E7B9037',
  WASM_BNB_HEX: '1E5C5FADEC7B34DD',

  // State persistence paths
  STATE_DIR,
  DB_PATH: path.join(STATE_DIR, 'clob_state.db'),
  ACTIVE_ORDERS_FILE: path.join(STATE_DIR, 'active_orders.json'),
  BALANCES_FILE: path.join(STATE_DIR, 'clob_balances.json'),
  NONCES_FILE: path.join(STATE_DIR, 'used_nonces.json'),
  PROCESSED_DEPOSITS_FILE: path.join(STATE_DIR, 'processed_deposits.json'),
  WITHDRAWALS_FILE: path.join(STATE_DIR, 'withdrawals.json'),
  ADMIN_KEYS_FILE: path.join(STATE_DIR, 'admin_keys.json'),

  // Autonomous SuperContract L1 Escrow Account
  SUPERCONTRACT_PUBKEY,
  SUPERCONTRACT_ADDRESS,

  // Canonical on-chain deposit & escrow target
  VAULT_ADDRESS: process.env.SIRIUS_VAULT_ADDRESS || 'VAAUXFYQ424I6V3JDVWCBWGWHRTIJGTSX4B2TLSV',
  LEGACY_VAULT_ADDRESS: process.env.SIRIUS_LEGACY_VAULT_ADDRESS || 'VAAUXFYQ424I6V3JDVWCBWGWHRTIJGTSX4B2TLSV',
  VAULT_KEY_FILE: path.join(STATE_DIR, 'vault_account.json'),
  TREASURY_KEY_FILE: path.join(STATE_DIR, 'treasury_account.json'),
  TREASURY_ADDRESS: process.env.SIRIUS_TREASURY_ADDRESS || 'VC46DFR25R5EWJWR32E4TFOYC57VXFNPKAASJTLV',
  FEE_COLLECTOR_FILE: path.join(STATE_DIR, 'fee_collector_account.json'),
  FEE_COLLECTOR_ADDRESS,
  FEE_COLLECTOR_PUBKEY,

  // Standalone matching engine WASM binary
  WASM_PATH: path.join(ROOT_DIR, 'contracts/clob/target/wasm32-unknown-unknown/release/sirius_clob.wasm'),
};
