# Sirius CLOB Exchange Backend

Production backend service for the Sirius Central Limit Order Book (CLOB) decentralized exchange on Sirius Testnet 2 (Network 184).

## Architecture
- **3-Node BFT WASM Cluster**: ProximaX Sirius SuperContract V2 executors on ports `5051`, `5052`, `5053`.
- **Relayer & Gateway API**: Node.js REST and WebSocket server on port `8088` (or dynamic `PORT`).
- **Solvency Sentinel**: Real-time on-chain reserves auditor and circuit breaker.
