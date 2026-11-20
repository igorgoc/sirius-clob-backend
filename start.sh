#!/usr/bin/env bash
set -e

echo "=== Starting Sirius CLOB BFT Consensus Cluster ==="

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
export SIRIUS_NODE_URL="${SIRIUS_NODE_URL:-https://api-2.testnet2.xpxsirius.io}"

echo "Starting CLOB Relayer API on port ${PORT:-8088}..."
exec node dashboard/server.js
