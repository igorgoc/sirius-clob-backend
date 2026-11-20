/**
 * Backward compatibility alias for BatchSettler -> BatchSettlerTestnet2
 * Routes batch settlement to live Sirius Testnet 2 / Mainnet REST API.
 */
const { defaultBatchSettlerTestnet2, BatchSettlerTestnet2 } = require('./batch_settler_testnet2');

module.exports = {
  BatchSettler: BatchSettlerTestnet2,
  BatchSettlerTestnet2,
  defaultBatchSettler: defaultBatchSettlerTestnet2,
  defaultBatchSettlerTestnet2,
};
