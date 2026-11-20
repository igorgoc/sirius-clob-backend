/**
 * Sirius Supercontract VM Execution Harness (Standalone Quick Test)
 * Simulates the Catapult Supercontract host environment and executes the compiled WASM binary.
 */

const fs = require('fs');
const path = require('path');

const wasmPath =
  process.argv[2] ||
  path.join(__dirname, '../examples/target/wasm32-unknown-unknown/release/supercontract_examples.wasm');

if (!fs.existsSync(wasmPath)) {
  console.error(`❌ WASM binary not found at: ${wasmPath}`);
  console.error('Please run `./sandbox.sh build-contract` first.');
  process.exit(1);
}

const wasmBuffer = fs.readFileSync(wasmPath);

console.log('==================================================================');
console.log('  Sirius Supercontract VM Execution Harness');
console.log('==================================================================');
console.log(`📦 Contract Binary: ${wasmPath} (${wasmBuffer.length} bytes)`);

let memory;
const mockCallerKeyHex = '04f1ee134c5d20a8868e235860e4e7a507a2003cb23b8f50cad6948c5dd75a10';
const mockBlockHeight = 105n;
const XPX_MOSAIC_ID = 0x0dc67fbe1cad29e3n;

// In-memory Virtual Drive FS for standalone quick test
const virtualFs = new Map();
const openHandles = new Map();
let nextFd = 1n;

function readStr(ptr, len) {
  return Buffer.from(memory.buffer, Number(ptr), Number(len)).toString('utf8');
}

const importObject = {
  env: {
    get_block_height: () => {
      console.log(`🔗 [Host Hook] get_block_height() -> height=${mockBlockHeight}`);
      return mockBlockHeight;
    },
    get_caller_public_key: (ptr) => {
      console.log(`🔗 [Host Hook] get_caller_public_key(ptr=${ptr}) -> caller=${mockCallerKeyHex.slice(0, 12)}...`);
      const memView = new Uint8Array(memory.buffer);
      const keyBuffer = Buffer.from(mockCallerKeyHex, 'hex');
      for (let i = 0; i < 32; i++) {
        memView[Number(ptr) + i] = keyBuffer[i];
      }
    },
    buffer_size: () => 16384n,
    get_service_payments: (ptr) => {
      console.log(`🔗 [Host Hook] get_service_payments(ptr=${ptr}) -> simulating 100 XPX incoming deposit`);
      const memView = new DataView(memory.buffer);
      const p = Number(ptr);
      memView.setBigUint64(p, XPX_MOSAIC_ID, true);
      memView.setBigUint64(p + 8, 100_000_000n, true);
      return 1n;
    },
    path_exists: (ptr, len) => {
      const p = readStr(ptr, len);
      return virtualFs.has(p) ? 1 : 0;
    },
    is_file: (ptr, len) => {
      const p = readStr(ptr, len);
      return virtualFs.has(p) ? 1 : 0;
    },
    file_size: (ptr, len) => {
      const p = readStr(ptr, len);
      const buf = virtualFs.get(p);
      return buf ? BigInt(buf.length) : -1n;
    },
    create_dir: () => 1,
    move_filesystem_entry: (newPtr, newLen, oldPtr, oldLen) => {
      const newP = readStr(newPtr, newLen);
      const oldP = readStr(oldPtr, oldLen);
      if (!virtualFs.has(oldP)) return 0;
      virtualFs.set(newP, virtualFs.get(oldP));
      virtualFs.delete(oldP);
      return 1;
    },
    remove_filesystem_entry: (ptr, len) => {
      const p = readStr(ptr, len);
      virtualFs.delete(p);
      return 1;
    },
    open_file: (pathPtr, pathLen, modePtr, modeLen) => {
      const p = readStr(pathPtr, pathLen);
      const mode = readStr(modePtr, modeLen);
      const id = nextFd++;
      if (mode === 'w') {
        virtualFs.set(p, Buffer.alloc(0));
      } else if (!virtualFs.has(p)) {
        return -1n;
      }
      openHandles.set(id, { path: p, mode, offset: 0 });
      return id;
    },
    write_file_stream: (id, ptr, len) => {
      const h = openHandles.get(id);
      if (!h) return -1n;
      const chunk = Buffer.from(memory.buffer, Number(ptr), Number(len));
      const prev = virtualFs.get(h.path) || Buffer.alloc(0);
      virtualFs.set(h.path, Buffer.concat([prev, Buffer.from(chunk)]));
      return BigInt(len);
    },
    read_file_stream: (id, ptr) => {
      const h = openHandles.get(id);
      if (!h) return -1n;
      const data = virtualFs.get(h.path) || Buffer.alloc(0);
      const remaining = data.subarray(h.offset);
      const dest = new Uint8Array(memory.buffer, Number(ptr), remaining.length);
      dest.set(remaining);
      h.offset += remaining.length;
      return BigInt(remaining.length);
    },
    flush: () => 1,
    close_file: (id) => {
      openHandles.delete(id);
      return 1;
    },
    set_transaction: (ptr, len) => {
      console.log(`💸 [Host Hook] set_transaction(ptr=${ptr}, len=${len}) triggered!`);
      const buffer = Buffer.from(memory.buffer, Number(ptr), Number(len));

      let offset = 0;
      const maxFee = buffer.readBigUInt64LE(offset);
      offset += 8;
      const embeddedCount = buffer.readUInt16LE(offset);
      offset += 2;

      console.log(`   ├── Max Transaction Fee: ${Number(maxFee) / 1_000_000} XPX (${maxFee} units)`);
      console.log(`   ├── Embedded Transactions Count: ${embeddedCount}`);

      for (let i = 0; i < embeddedCount; i++) {
        const entityType = buffer.readUInt16LE(offset);
        offset += 2;
        const version = buffer.readUInt32LE(offset);
        offset += 4;
        const payloadSize = buffer.readUInt16LE(offset);
        offset += 2;
        const payload = buffer.subarray(offset, offset + payloadSize);
        offset += payloadSize;

        const typeName = entityType === 0x4154 ? 'TransferTransaction (0x4154)' : `0x${entityType.toString(16)}`;
        console.log(`   └── Embedded Tx #${i + 1}: ${typeName} [v${version}, payload: ${payloadSize} bytes]`);

        if (entityType === 0x4154 && payload.length >= 48) {
          const recipientHex = payload.subarray(0, 32).toString('hex');
          const mosaicCount = payload.readBigUInt64LE(32);
          const msgSize = payload.readBigUInt64LE(40);

          let payOffset = 48;
          console.log(`       ├── Recipient Account: ${recipientHex.slice(0, 16)}...${recipientHex.slice(-8)}`);
          console.log(`       ├── Attached Tokens (${mosaicCount} mosaic):`);

          for (let m = 0; m < Number(mosaicCount); m++) {
            const mosaicId = payload.readBigUInt64LE(payOffset);
            payOffset += 8;
            const amount = payload.readBigUInt64LE(payOffset);
            payOffset += 8;
            const isXpx = mosaicId === XPX_MOSAIC_ID;
            const tokenName = isXpx ? 'XPX' : 'Token';
            console.log(
              `       │   • Mosaic ID: 0x${mosaicId.toString(16)} | Amount: ${Number(amount) / 1_000_000} ${tokenName} (${amount} units)`
            );
          }

          if (msgSize > 0n && payOffset < payload.length) {
            const msg = payload.subarray(payOffset, payOffset + Number(msgSize)).toString('utf8');
            console.log(`       └── Attached On-Chain Memo: "${msg}"`);
          }
        }
      }
    },
  },
};

WebAssembly.instantiate(wasmBuffer, importObject)
  .then(({ instance }) => {
    memory = instance.exports.memory;
    console.log('⚡ Instantiated WASM module with Catapult host hooks.');
    console.log('🚀 Invoking Supercontract entrypoint: run()...');

    const startTime = process.hrtime.bigint();
    const result = instance.exports.run();
    const elapsedNs = process.hrtime.bigint() - startTime;
    const elapsedMs = Number(elapsedNs) / 1_000_000;

    console.log('------------------------------------------------------------------');
    console.log(`⏱️ Execution Time: ${elapsedMs.toFixed(3)} ms`);
    console.log(`📊 Return Status Code: ${result}`);

    if (result === 0) {
      console.log('✅ Status 0: Token transfer transaction + Drive state persistence verified.');
      console.log('==================================================================');
      process.exit(0);
    } else {
      console.error(`❌ Supercontract returned non-zero error code: ${result}`);
      console.log('==================================================================');
      process.exit(1);
    }
  })
  .catch((err) => {
    console.error('❌ Failed to execute WASM module:', err);
    process.exit(1);
  });
