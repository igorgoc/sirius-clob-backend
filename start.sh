#!/usr/bin/env bash
set -e
umask 077

echo "=== Starting Sirius CLOB BFT Consensus Cluster ==="

# Ensure cluster state directory exists with owner-only permissions before writing PIDs/keys
mkdir -p cluster
chmod 700 cluster

# Trap termination signals to kill background processes
cleanup() {
    echo "Shutting down Sirius BFT nodes..."
    kill $(jobs -p) 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# Start 3 BFT WASM VM Executors
echo "Starting BFT Node 1 (:5051)..."
node executor/grpc_vm_server.js 5051 node1 &
echo $! > cluster/node1_vm.pid

echo "Starting BFT Node 2 (:5052)..."
node executor/grpc_vm_server.js 5052 node2 &
echo $! > cluster/node2_vm.pid

echo "Starting BFT Node 3 (:5053)..."
node executor/grpc_vm_server.js 5053 node3 &
echo $! > cluster/node3_vm.pid

sleep 2

# Export default network config
export SIRIUS_NETWORK="${SIRIUS_NETWORK:-testnet2}"
if [ -z "$SIRIUS_NODE_URL" ]; then
    if [ "$SIRIUS_NETWORK" = "mainnet" ]; then
        export SIRIUS_NODE_URL="https://betelgeuse.xpxsirius.io"
    else
        export SIRIUS_NODE_URL="https://api-2.testnet2.xpxsirius.io"
    fi
fi

echo "Starting Market Maker Daemon..."
node scripts/clob_market_maker.js --daemon > cluster/mm.log 2>&1 &
echo $! > cluster/mm.pid

echo "Starting CLOB Relayer API on port ${PORT:-8088}..."
exec node dashboard/server.js
