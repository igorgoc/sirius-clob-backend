/**
 * Verification test for Mainnet Readiness & Multi-Network Abstraction
 */
const assert = require('assert');
const path = require('path');

console.log('--- 1. Testing Default Network Profile (Testnet 2) ---');
const t2Config = require('../dashboard/testnet2_config');
assert.strictEqual(t2Config.NETWORK_TYPE, 168, 'Testnet2 NETWORK_TYPE should be 168');
assert.strictEqual(t2Config.XPX_MOSAIC_ID, '13BFC518E40549D7', 'Testnet2 XPX mosaic should be 13BFC518E40549D7');
assert(t2Config.SUPERCONTRACT_ADDRESS.startsWith('V'), 'Testnet2 address must start with V');
assert(t2Config.FEE_COLLECTOR_ADDRESS.startsWith('V'), 'Testnet2 fee collector address must start with V');
console.log('✓ Default Testnet 2 profile correctly verified');

console.log('--- 2. Testing Mainnet Profile Dynamic Loading ---');
delete require.cache[require.resolve('../dashboard/testnet2_config')];
process.env.SIRIUS_NETWORK = 'mainnet';
delete process.env.SIRIUS_NODE_URL;
delete process.env.SIRIUS_NETWORK_TYPE;

const mainnetConfig = require('../dashboard/testnet2_config');
assert.strictEqual(mainnetConfig.NETWORK_TYPE, 184, 'Mainnet NETWORK_TYPE should be 184');
assert.strictEqual(mainnetConfig.GENERATION_HASH, '10540AD3A1BF46B1A05D8B1CF0252BC9FB2E0B53CFD748262B0CE341CEAFEB6B', 'Mainnet generation hash mismatch');
assert.strictEqual(mainnetConfig.XPX_MOSAIC_ID, '402B2F579FAEBC59', 'Mainnet XPX mosaic must be official 402B2F579FAEBC59');
assert.strictEqual(mainnetConfig.NODE_URL, 'https://betelgeuse.xpxsirius.io', 'Mainnet node URL must not have port :3000 on HTTPS');
assert(mainnetConfig.SUPERCONTRACT_ADDRESS.startsWith('X'), `Mainnet address must start with X, got: ${mainnetConfig.SUPERCONTRACT_ADDRESS}`);
assert(mainnetConfig.FEE_COLLECTOR_ADDRESS.startsWith('X'), `Mainnet fee collector must start with X, got: ${mainnetConfig.FEE_COLLECTOR_ADDRESS}`);
assert.strictEqual(mainnetConfig.FAUCET_ENABLED, false, 'Faucet must be disabled on Mainnet');
assert.strictEqual(mainnetConfig.CONFIRMATION_DEPTH, 2, 'Mainnet confirmation depth should be 2');
console.log('✓ Mainnet profile dynamically loaded and verified with 100% precision');

console.log('--- 3. Testing Environment Variable Vault Key Injection ---');
delete require.cache[require.resolve('../dashboard/testnet2_client')];
process.env.SIRIUS_VAULT_PRIVATE_KEY = '1111111111111111111111111111111111111111111111111111111111111111';
const client = require('../dashboard/testnet2_client');
const vault = client.getOrInitVaultAccount();
assert.strictEqual(vault.privateKey.toLowerCase(), process.env.SIRIUS_VAULT_PRIVATE_KEY.toLowerCase(), 'Vault key from env var was not loaded');
assert(vault.address.plain().startsWith('X'), 'Mainnet vault address must start with X');
console.log('✓ Injected vault private key initialized correctly on Mainnet address:', vault.address.plain());

// Reset env
delete process.env.SIRIUS_NETWORK;
delete process.env.SIRIUS_VAULT_PRIVATE_KEY;
console.log('\n=== ALL MAINNET READINESS UNIT TESTS PASSED ===\n');
