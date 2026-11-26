/**
 * Sirius SuperContract V2 — Milestone 2 Full-Cycle 3-Node Executor & Advanced Consensus Verifier
 *
 * Implements the C++ `ExecuteCallRPCHandler` (blockchain_vm.proto + storage_vm.proto)
 * and Drive Sandbox Overlay across all 3 local validator nodes:
 *   - node1 (P2P :7900, DBRB :7903, Replicator :7904, WASM VM gRPC :5051)
 *   - node2 (P2P :7910, DBRB :7913, Replicator :7914, WASM VM gRPC :5052)
 *   - node3 (P2P :7920, DBRB :7923, Replicator :7924, WASM VM gRPC :5053)
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const grpc = require('@grpc/grpc-js');
const { supercontractserver } = require('./grpc_vm_server');
const { createGrpcClientCredentials } = require('./grpc_tls');

const ROOT_DIR = path.resolve(__dirname, '..');
const CLUSTER_DIR = path.join(ROOT_DIR, 'cluster');

const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';
const CONTRACT_KEY_HEX = 'c589bfef210489d3a1bf46b1a05d8b1cf0252bc9fb2e0b53cfd748262b0ce341';
const CALLER_KEY_HEX = '04f1ee134c5d20a8868e235860e4e7a507a2003cb23b8f50cad6948c5dd75a10';
const ALICE_KEY_HEX = '1111111111111111111111111111111111111111111111111111111111111111';
const BOB_KEY_HEX = '2222222222222222222222222222222222222222222222222222222222222222';
const XPX_MOSAIC_ID = BigInt('0x402B2F579FAEBC59').toString(10);

const DEFAULT_NODES = [
  { name: 'node1', vmHost: '127.0.0.1', p2pPort: 7900, dbrbPort: 7903, repPort: 7904, vmPort: 5051 },
  { name: 'node2', vmHost: '127.0.0.1', p2pPort: 7910, dbrbPort: 7913, repPort: 7914, vmPort: 5052 },
  { name: 'node3', vmHost: '127.0.0.1', p2pPort: 7920, dbrbPort: 7923, repPort: 7924, vmPort: 5053 },
];

const NODES = process.env.SIRIUS_VM_NODES
  ? process.env.SIRIUS_VM_NODES.split(',').map((entry, idx) => {
      const clean = entry.trim();
      const [first, second, third] = clean.split(':');
      if (third) {
        return { name: first, vmHost: second, vmPort: Number(third) };
      }
      return {
        ...(DEFAULT_NODES[idx] || { name: `node${idx + 1}` }),
        vmHost: first || '127.0.0.1',
        vmPort: Number(second || (5051 + idx)),
      };
    })
  : DEFAULT_NODES;

function sha256Hex(bufferOrPath) {
  const buf = Buffer.isBuffer(bufferOrPath) ? bufferOrPath : fs.readFileSync(bufferOrPath);
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function copyDirRecursive(src, dest) {
  if (!fs.existsSync(src)) return;
  fs.mkdirSync(dest, { recursive: true });
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    if (entry.isSymbolicLink()) continue; // Never follow symlinks out of sandbox/drive
    if (entry.name.endsWith('.wasm')) continue; // Skip static WASM binaries (immutable code)
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      copyDirRecursive(srcPath, destPath);
    } else if (entry.isFile()) {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}

function safeRmSync(targetPath) {
  if (!targetPath || !fs.existsSync(targetPath)) return;
  try {
    fs.rmSync(targetPath, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 });
  } catch (err) {
    try {
      if (fs.existsSync(targetPath)) {
        const stat = fs.statSync(targetPath);
        if (stat.isDirectory()) {
          for (const file of fs.readdirSync(targetPath)) {
            safeRmSync(path.join(targetPath, file));
          }
          try { fs.rmdirSync(targetPath); } catch (_) {}
        } else {
          try { fs.unlinkSync(targetPath); } catch (_) {}
        }
      }
    } catch (_) {}
  }
}

function safeAtomicRename(src, dest) {
  try {
    fs.renameSync(src, dest);
  } catch (err) {
    if (err.code === 'EXDEV' || err.code === 'EPERM' || err.code === 'EBUSY') {
      if (fs.existsSync(src)) {
        const stat = fs.statSync(src);
        if (stat.isDirectory()) {
          safeRmSync(dest);
          copyDirRecursive(src, dest);
          safeRmSync(src);
        } else {
          fs.copyFileSync(src, dest);
          try { fs.unlinkSync(src); } catch (_) {}
        }
      }
    } else {
      throw err;
    }
  }
}

let overlayCommitSeq = 0;

/**
 * Per-Execution In-Memory Copy-on-Write Drive Sandbox Overlay Manager (`VirtualMachineStorageQueryHandler`)
 * Stages reads lazily from the committed drive (via defensive Buffer copies) and buffers all writes,
 * renames, and deletes purely in RAM during WASM execution, then atomically commits only modified
 * files via temp-file + rename(2) on BFT quorum consensus (or discards in 0ms on rollback).
 */
class DriveSandboxOverlay {
  constructor(nodeName, driveRootDir) {
    this.nodeName = nodeName;
    this.driveRootDir = driveRootDir;
    this.driveDir = path.join(driveRootDir, 'drive');
    this.sandboxDir = path.join(driveRootDir, 'sandbox');
    fs.mkdirSync(this.driveDir, { recursive: true });
    this.nextFileId = 1;
    this.openFiles = new Map(); // id -> { relPath, mode, readBuf, readOffset, chunks, dirty }
    this.nextIterId = 1;
    this.dirIterators = new Map(); // id -> { entries: [{name, depth}], index }
    this.dirtyFiles = new Map(); // relPath -> Buffer
    this.deletedFiles = new Set(); // relPath
    this.deletedDirs = new Set(); // relPath
    this.createdDirs = new Set(); // relPath
  }

  resolveRelPath(relPath) {
    const cleanRel = String(relPath || '').replace(/\\/g, '/').replace(/^\/+/, '');
    if (cleanRel.includes('\0')) {
      throw new Error(`Sandbox null-byte path blocked: ${relPath}`);
    }
    const resolved = path.resolve(this.driveDir, cleanRel);
    const rootWithSep = this.driveDir.endsWith(path.sep) ? this.driveDir : this.driveDir + path.sep;
    if (resolved !== this.driveDir && !resolved.startsWith(rootWithSep)) {
      throw new Error(`Sandbox path traversal blocked: ${relPath}`);
    }
    const rel = path.relative(this.driveDir, resolved).split(path.sep).join('/');
    if (rel === '.' || rel === '') return '';
    if (rel.split('/').some((seg) => seg === '..')) {
      throw new Error(`Sandbox path traversal blocked: ${relPath}`);
    }
    return rel;
  }

  _diskStatSafe(relPath) {
    if (!relPath) {
      try {
        const st = fs.lstatSync(this.driveDir);
        return st.isSymbolicLink() ? null : st;
      } catch (_) {
        return null;
      }
    }
    const abs = path.join(this.driveDir, relPath);
    try {
      const st = fs.lstatSync(abs);
      if (st.isSymbolicLink()) return null;
      return st;
    } catch (_) {
      return null;
    }
  }

  _flushOpenWriterByPath(relPath) {
    for (const entry of this.openFiles.values()) {
      if (entry.mode === 'w' && entry.relPath === relPath && entry.dirty) {
        const materialized = entry.chunks.length === 1 ? entry.chunks[0] : Buffer.concat(entry.chunks);
        entry.chunks = [materialized];
        entry.dirty = false;
        this.dirtyFiles.set(relPath, materialized);
      }
    }
  }

  _flushAllOpenWriters() {
    for (const entry of this.openFiles.values()) {
      if (entry.mode === 'w' && entry.dirty) {
        const materialized = entry.chunks.length === 1 ? entry.chunks[0] : Buffer.concat(entry.chunks);
        entry.chunks = [materialized];
        entry.dirty = false;
        this.dirtyFiles.set(entry.relPath, materialized);
      }
    }
  }

  _ensureDirVirtual(dirRel) {
    if (!dirRel) return;
    const parts = dirRel.split('/').filter(Boolean);
    let cur = '';
    for (const p of parts) {
      cur = cur ? `${cur}/${p}` : p;
      this.deletedDirs.delete(cur);
      this.deletedFiles.delete(cur);
      this.createdDirs.add(cur);
    }
  }

  _isVirtualFile(relPath) {
    if (!relPath) return false;
    this._flushOpenWriterByPath(relPath);
    if (this.dirtyFiles.has(relPath)) return true;
    if (this.deletedFiles.has(relPath) || this.createdDirs.has(relPath)) return false;
    const st = this._diskStatSafe(relPath);
    return Boolean(st && st.isFile());
  }

  _isVirtualDir(relPath) {
    if (!relPath) return true;
    if (this.dirtyFiles.has(relPath)) return false;
    if (this.createdDirs.has(relPath)) return true;
    // Implicitly created by any child dirty file or child created dir
    const prefix = `${relPath}/`;
    for (const k of this.dirtyFiles.keys()) {
      if (k.startsWith(prefix)) return true;
    }
    for (const d of this.createdDirs) {
      if (d.startsWith(prefix)) return true;
    }
    if (this.deletedDirs.has(relPath)) return false;
    const st = this._diskStatSafe(relPath);
    return Boolean(st && st.isDirectory());
  }

  _virtualExists(relPath) {
    if (!relPath) return true;
    return this._isVirtualFile(relPath) || this._isVirtualDir(relPath);
  }

  _readVirtualFileBuffer(relPath) {
    this._flushOpenWriterByPath(relPath);
    if (this.dirtyFiles.has(relPath)) {
      return Buffer.from(this.dirtyFiles.get(relPath));
    }
    if (this.deletedFiles.has(relPath)) {
      throw new Error(`ENOENT: ${relPath}`);
    }
    const st = this._diskStatSafe(relPath);
    if (!st || !st.isFile()) {
      throw new Error(`ENOENT: ${relPath}`);
    }
    return fs.readFileSync(path.join(this.driveDir, relPath));
  }

  _removeVirtualEntry(relPath) {
    if (!relPath) return false;
    this._flushAllOpenWriters();
    const prefix = `${relPath}/`;

    // Close any open handles under this path cleanly
    for (const [id, entry] of this.openFiles.entries()) {
      if (entry.relPath === relPath || entry.relPath.startsWith(prefix)) {
        this.openFiles.delete(id);
      }
    }

    // Collect any existing disk subtree so recreated dirs never resurrect old deleted files
    const st = this._diskStatSafe(relPath);
    if (st) {
      if (st.isFile()) {
        this.deletedFiles.add(relPath);
      } else if (st.isDirectory()) {
        const markDiskTree = (curRel) => {
          this.deletedDirs.add(curRel);
          const abs = path.join(this.driveDir, curRel);
          let entries = [];
          try { entries = fs.readdirSync(abs, { withFileTypes: true }); } catch (_) {}
          for (const e of entries) {
            if (e.isSymbolicLink()) continue;
            const childRel = `${curRel}/${e.name}`;
            if (e.isDirectory()) markDiskTree(childRel);
            else if (e.isFile()) this.deletedFiles.add(childRel);
          }
        };
        markDiskTree(relPath);
      }
    }

    for (const k of Array.from(this.dirtyFiles.keys())) {
      if (k === relPath || k.startsWith(prefix)) {
        this.dirtyFiles.delete(k);
        this.deletedFiles.add(k);
      }
    }
    for (const d of Array.from(this.createdDirs)) {
      if (d === relPath || d.startsWith(prefix)) {
        this.createdDirs.delete(d);
        this.deletedDirs.add(d);
      }
    }
    return true;
  }

  _listDirectChildren(dirRel) {
    this._flushAllOpenWriters();
    const childMap = new Map(); // name -> isDir
    if (!this.deletedDirs.has(dirRel)) {
      const absDir = dirRel ? path.join(this.driveDir, dirRel) : this.driveDir;
      try {
        const st = fs.lstatSync(absDir);
        if (!st.isSymbolicLink() && st.isDirectory()) {
          for (const entry of fs.readdirSync(absDir, { withFileTypes: true })) {
            if (entry.isSymbolicLink()) continue;
            const childRel = dirRel ? `${dirRel}/${entry.name}` : entry.name;
            if (entry.isDirectory()) {
              if (!this.deletedDirs.has(childRel) && !this.dirtyFiles.has(childRel)) {
                childMap.set(entry.name, true);
              }
            } else if (entry.isFile()) {
              if (!this.deletedFiles.has(childRel)) {
                childMap.set(entry.name, false);
              }
            }
          }
        }
      } catch (_) {}
    }

    const prefix = dirRel ? `${dirRel}/` : '';
    for (const d of this.createdDirs) {
      if (prefix ? d.startsWith(prefix) : d.length > 0) {
        const rest = prefix ? d.slice(prefix.length) : d;
        const slashIdx = rest.indexOf('/');
        const name = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
        if (name && !this.dirtyFiles.has(dirRel ? `${dirRel}/${name}` : name)) {
          childMap.set(name, true);
        }
      }
    }
    for (const f of this.dirtyFiles.keys()) {
      if (prefix ? f.startsWith(prefix) : f.length > 0) {
        const rest = prefix ? f.slice(prefix.length) : f;
        const slashIdx = rest.indexOf('/');
        if (slashIdx === -1) {
          childMap.set(rest, false);
        } else {
          const name = rest.slice(0, slashIdx);
          if (name && !childMap.has(name)) childMap.set(name, true);
        }
      }
    }

    return Array.from(childMap.entries())
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([name, isDir]) => ({ name, isDir }));
  }

  collectDirEntries(dirRel, recursive, currentDepth = 0, out = []) {
    const children = this._listDirectChildren(dirRel);
    for (const child of children) {
      out.push({ name: child.name, depth: currentDepth });
      if (recursive && child.isDir) {
        const childRel = dirRel ? `${dirRel}/${child.name}` : child.name;
        this.collectDirEntries(childRel, true, currentDepth + 1, out);
      }
    }
    return out;
  }

  handleMessage(responseMsg) {
    if (responseMsg.open_file) {
      const { path: relPathRaw, mode } = responseMsg.open_file;
      try {
        const relPath = this.resolveRelPath(relPathRaw);
        if (!relPath || relPath.endsWith('.wasm') && mode === 'w') {
          return { open_file_return: { success: false, identifier: '0' } };
        }
        if (mode === 'w') {
          const parentRel = relPath.includes('/') ? relPath.slice(0, relPath.lastIndexOf('/')) : '';
          if (parentRel) this._ensureDirVirtual(parentRel);
          this.deletedFiles.delete(relPath);
          this.dirtyFiles.set(relPath, Buffer.alloc(0));
          const id = String(this.nextFileId++);
          this.openFiles.set(id, {
            relPath,
            mode: 'w',
            readBuf: null,
            readOffset: 0,
            chunks: [],
            dirty: false,
          });
          return { open_file_return: { success: true, identifier: id } };
        } else {
          if (!this._isVirtualFile(relPath)) {
            return { open_file_return: { success: false, identifier: '0' } };
          }
          const readBuf = this._readVirtualFileBuffer(relPath);
          const id = String(this.nextFileId++);
          this.openFiles.set(id, {
            relPath,
            mode: 'r',
            readBuf,
            readOffset: 0,
            chunks: null,
            dirty: false,
          });
          return { open_file_return: { success: true, identifier: id } };
        }
      } catch (e) {
        return { open_file_return: { success: false, identifier: '0' } };
      }
    }

    if (responseMsg.write_file_stream) {
      const { identifier, buffer } = responseMsg.write_file_stream;
      const entry = this.openFiles.get(String(identifier));
      if (!entry || entry.mode !== 'w') {
        return { write_file_stream_return: { success: false, num_bytes_written: '0' } };
      }
      try {
        const buf = Buffer.isBuffer(buffer) ? Buffer.from(buffer) : Buffer.from(buffer || '');
        entry.chunks.push(buf);
        entry.dirty = true;
        return { write_file_stream_return: { success: true, num_bytes_written: String(buf.length) } };
      } catch (e) {
        return { write_file_stream_return: { success: false, num_bytes_written: '0' } };
      }
    }

    if (responseMsg.flush) {
      const { identifier } = responseMsg.flush;
      const entry = this.openFiles.get(String(identifier));
      if (!entry) {
        return { flush_return: { success: false } };
      }
      try {
        if (entry.mode === 'w') {
          this._flushOpenWriterByPath(entry.relPath);
        }
        return { flush_return: { success: true } };
      } catch (e) {
        return { flush_return: { success: false } };
      }
    }

    if (responseMsg.read_file_stream) {
      const { identifier } = responseMsg.read_file_stream;
      const entry = this.openFiles.get(String(identifier));
      if (!entry || entry.mode !== 'r' || !entry.readBuf) {
        return { read_file_stream_return: { success: false, buffer: Buffer.alloc(0) } };
      }
      try {
        const end = Math.min(entry.readBuf.length, entry.readOffset + 16384);
        const slice = entry.readBuf.subarray(entry.readOffset, end);
        entry.readOffset = end;
        return {
          read_file_stream_return: {
            success: true,
            buffer: slice,
          },
        };
      } catch (e) {
        return { read_file_stream_return: { success: false, buffer: Buffer.alloc(0) } };
      }
    }

    if (responseMsg.close_file) {
      const { identifier } = responseMsg.close_file;
      const entry = this.openFiles.get(String(identifier));
      if (!entry) {
        return { close_file_return: { success: false } };
      }
      try {
        if (entry.mode === 'w') {
          this._flushOpenWriterByPath(entry.relPath);
        }
        return { close_file_return: { success: true } };
      } catch (e) {
        return { close_file_return: { success: false } };
      } finally {
        this.openFiles.delete(String(identifier));
      }
    }

    if (responseMsg.path_exist) {
      try {
        const relPath = this.resolveRelPath(responseMsg.path_exist.path);
        return { path_exist_return: { exist: this._virtualExists(relPath) } };
      } catch (e) {
        return { path_exist_return: { exist: false } };
      }
    }

    if (responseMsg.is_file) {
      try {
        const relPath = this.resolveRelPath(responseMsg.is_file.path);
        if (!this._virtualExists(relPath)) {
          return { is_file_return: { success: false, is_file: false } };
        }
        return { is_file_return: { success: true, is_file: this._isVirtualFile(relPath) } };
      } catch (e) {
        return { is_file_return: { success: false, is_file: false } };
      }
    }

    if (responseMsg.file_size) {
      try {
        const relPath = this.resolveRelPath(responseMsg.file_size.path);
        if (!this._virtualExists(relPath)) {
          return { file_size_return: { success: false, file_size: '0' } };
        }
        if (this._isVirtualFile(relPath)) {
          this._flushOpenWriterByPath(relPath);
          if (this.dirtyFiles.has(relPath)) {
            return { file_size_return: { success: true, file_size: String(this.dirtyFiles.get(relPath).length) } };
          }
          const st = this._diskStatSafe(relPath);
          return { file_size_return: { success: Boolean(st), file_size: String(st ? st.size : 0) } };
        }
        return { file_size_return: { success: true, file_size: '0' } };
      } catch (e) {
        return { file_size_return: { success: false, file_size: '0' } };
      }
    }

    if (responseMsg.create_dir) {
      try {
        const relPath = this.resolveRelPath(responseMsg.create_dir.path);
        this._ensureDirVirtual(relPath);
        return { create_dir_return: { success: true } };
      } catch (e) {
        return { create_dir_return: { success: false } };
      }
    }

    if (responseMsg.move_filesystem_entry) {
      const { old_path, new_path } = responseMsg.move_filesystem_entry;
      try {
        const oldRel = this.resolveRelPath(old_path);
        const newRel = this.resolveRelPath(new_path);
        if (!oldRel || !newRel || !this._virtualExists(oldRel) || oldRel === newRel) {
          return { move_filesystem_entry_return: { success: false } };
        }
        this._flushAllOpenWriters();
        if (this._isVirtualFile(oldRel)) {
          const data = this._readVirtualFileBuffer(oldRel);
          const newParent = newRel.includes('/') ? newRel.slice(0, newRel.lastIndexOf('/')) : '';
          if (newParent) this._ensureDirVirtual(newParent);
          this.deletedFiles.delete(newRel);
          this.dirtyFiles.set(newRel, data);
          this._removeVirtualEntry(oldRel);
          return { move_filesystem_entry_return: { success: true } };
        }
        // Directory move: copy virtual tree over recursively, then remove old subtree
        const copyTree = (srcDir, dstDir) => {
          this._ensureDirVirtual(dstDir);
          for (const child of this._listDirectChildren(srcDir)) {
            const sRel = `${srcDir}/${child.name}`;
            const dRel = `${dstDir}/${child.name}`;
            if (child.isDir) {
              copyTree(sRel, dRel);
            } else {
              this.deletedFiles.delete(dRel);
              this.dirtyFiles.set(dRel, this._readVirtualFileBuffer(sRel));
            }
          }
        };
        copyTree(oldRel, newRel);
        this._removeVirtualEntry(oldRel);
        return { move_filesystem_entry_return: { success: true } };
      } catch (e) {
        return { move_filesystem_entry_return: { success: false } };
      }
    }

    if (responseMsg.remove_filesystem_entry) {
      try {
        const relPath = this.resolveRelPath(responseMsg.remove_filesystem_entry.path);
        if (!relPath || !this._virtualExists(relPath)) {
          return { remove_filesystem_entry_return: { success: false } };
        }
        this._removeVirtualEntry(relPath);
        return { remove_filesystem_entry_return: { success: true } };
      } catch (e) {
        return { remove_filesystem_entry_return: { success: false } };
      }
    }

    if (responseMsg.create_dir_iterator) {
      const { path: relPathRaw, recursive } = responseMsg.create_dir_iterator;
      try {
        const relPath = this.resolveRelPath(relPathRaw);
        if (!this._isVirtualDir(relPath)) {
          return { create_dir_iterator_return: { success: false, identifier: '0' } };
        }
        const entries = this.collectDirEntries(relPath, Boolean(recursive));
        const id = String(this.nextIterId++);
        this.dirIterators.set(id, { entries, index: 0 });
        return { create_dir_iterator_return: { success: true, identifier: id } };
      } catch (e) {
        return { create_dir_iterator_return: { success: false, identifier: '0' } };
      }
    }

    if (responseMsg.destroy_dir_iterator) {
      const id = String(responseMsg.destroy_dir_iterator.identifier);
      const existed = this.dirIterators.delete(id);
      return { destroy_dir_iterator_return: { success: existed } };
    }

    if (responseMsg.has_next_iterator) {
      const id = String(responseMsg.has_next_iterator.identifier);
      const it = this.dirIterators.get(id);
      return { has_next_iterator_return: { has_next: Boolean(it && it.index < it.entries.length) } };
    }

    if (responseMsg.next_dir_iterator) {
      const id = String(responseMsg.next_dir_iterator.identifier);
      const it = this.dirIterators.get(id);
      if (!it || it.index >= it.entries.length) {
        return { next_dir_iterator_return: { success: false, name: '', depth: 0 } };
      }
      const item = it.entries[it.index++];
      return { next_dir_iterator_return: { success: true, name: item.name, depth: item.depth } };
    }

    return null;
  }

  closeAllHandles() {
    this._flushAllOpenWriters();
    this.openFiles.clear();
    this.dirIterators.clear();
  }

  commitOverlay() {
    this.closeAllHandles();

    // 1. Apply explicit file deletions (only when not overwritten in the same execution)
    for (const rel of this.deletedFiles) {
      if (this.dirtyFiles.has(rel) || rel.endsWith('.wasm')) continue;
      const abs = path.join(this.driveDir, rel);
      try {
        if (fs.existsSync(abs)) fs.unlinkSync(abs);
      } catch (_) {}
    }

    // 2. Remove deleted directories deepest-first unless recreated
    const sortedDelDirs = Array.from(this.deletedDirs).sort((a, b) => b.length - a.length);
    for (const rel of sortedDelDirs) {
      if (this.createdDirs.has(rel)) continue;
      const abs = path.join(this.driveDir, rel);
      safeRmSync(abs);
    }

    // 3. Ensure created directories exist (unlink colliding file if any)
    for (const rel of this.createdDirs) {
      const absDir = path.join(this.driveDir, rel);
      try {
        const st = fs.lstatSync(absDir);
        if (st.isFile() || st.isSymbolicLink()) fs.unlinkSync(absDir);
      } catch (_) {}
      fs.mkdirSync(absDir, { recursive: true });
    }

    // 4. Atomically write only dirty files via temp-file + same-filesystem rename(2)
    for (const [rel, buf] of this.dirtyFiles.entries()) {
      if (rel.endsWith('.wasm')) continue;
      const abs = path.join(this.driveDir, rel);
      fs.mkdirSync(path.dirname(abs), { recursive: true });
      const tmp = `${abs}.tmp.${process.pid}.${++overlayCommitSeq}`;
      fs.writeFileSync(tmp, buf);
      safeAtomicRename(tmp, abs);
    }

    this.dirtyFiles.clear();
    this.deletedFiles.clear();
    this.deletedDirs.clear();
    this.createdDirs.clear();
  }

  rollbackOverlay() {
    this.openFiles.clear();
    this.dirIterators.clear();
    this.dirtyFiles.clear();
    this.deletedFiles.clear();
    this.deletedDirs.clear();
    this.createdDirs.clear();
  }

  computeStateRootHash() {
    this.closeAllHandles();
    const hasMarkets = this._isVirtualDir('markets');
    const rootRel = hasMarkets ? 'markets' : '';
    const hash = crypto.createHash('sha256');
    let hashedAny = false;

    const walk = (dirRel) => {
      const children = this._listDirectChildren(dirRel);
      for (const child of children) {
        if (child.name.endsWith('.wasm')) continue;
        const childRel = dirRel ? `${dirRel}/${child.name}` : child.name;
        if (child.isDir) {
          walk(childRel);
        } else {
          hashedAny = true;
          const relFromRoot = rootRel ? childRel.slice(rootRel.length + 1) : childRel;
          hash.update(relFromRoot.split('/').join(path.sep));
          hash.update(this._readVirtualFileBuffer(childRel));
        }
      }
    };

    walk(rootRel);
    if (!hashedAny && !hasMarkets && this._listDirectChildren('').length === 0) {
      return null;
    }
    return hash.digest('hex');
  }
}

// Per-node execution mutex to prevent overlapping DriveSandboxOverlay operations on the same Replicator node
const nodeVmLocks = new Map();
async function withNodeVmLock(nodeName, fn) {
  const key = String(nodeName || 'node');
  const prev = nodeVmLocks.get(key) || Promise.resolve();
  let release;
  const next = new Promise((resolve) => {
    release = resolve;
  });
  nodeVmLocks.set(key, prev.then(() => next));
  await prev;
  try {
    return await fn();
  } finally {
    release();
    if (nodeVmLocks.get(key) === next) {
      nodeVmLocks.delete(key);
    }
  }
}

/**
 * Executes a contract function on a single node's gRPC WASM VM over `ExecuteCall` stream
 */
function executeOnNodeVm(
  node,
  wasmFilePath,
  functionToCall,
  blockHeight,
  txHashHex,
  poexPrefix,
  driveRootDir,
  options = {}
) {
  return withNodeVmLock(node.name, () =>
    _executeOnNodeVmUnlocked(
      node,
      wasmFilePath,
      functionToCall,
      blockHeight,
      txHashHex,
      poexPrefix,
      driveRootDir,
      options
    )
  );
}

const vmClientCache = new Map();

function getOrCreateVmClient(targetEndpoint) {
  let client = vmClientCache.get(targetEndpoint);
  if (!client) {
    const { credentials, channelOptions } = createGrpcClientCredentials(targetEndpoint);
    client = new supercontractserver.SupercontractServer(
      targetEndpoint,
      credentials,
      channelOptions
    );
    vmClientCache.set(targetEndpoint, client);
  }
  return client;
}

function resetVmClient(targetEndpoint, client) {
  if (vmClientCache.get(targetEndpoint) === client) {
    vmClientCache.delete(targetEndpoint);
  }
  try {
    client.close();
  } catch (_) {}
}

function _executeOnNodeVmUnlocked(
  node,
  wasmFilePath,
  functionToCall,
  blockHeight,
  txHashHex,
  poexPrefix,
  driveRootDir,
  options = {}
) {
    const {
    parameters = Buffer.alloc(0),
    executionGasLimit = '10000000',
    downloadGasLimit = '10000000',
    callerKeyHex = CALLER_KEY_HEX,
    deferCommit = false,
    simulatedLatencyMs = 0,
    servicePayments = [
      {
        mosaic_id: XPX_MOSAIC_ID,
        amount: '100000000', // 100.000000 XPX incoming service payment
      },
    ],
  } = options;

  return new Promise((resolve, reject) => {
    let isCompleted = false;
    const targetEndpoint = `${node.vmHost || '127.0.0.1'}:${node.vmPort}`;
    const client = getOrCreateVmClient(targetEndpoint);

    const overlay = new DriveSandboxOverlay(node.name, driveRootDir);
    const callMetadata = new grpc.Metadata();
    const vmAuthToken = options.vmAuthToken || process.env.SIRIUS_VM_AUTH_TOKEN;
    if (vmAuthToken) {
      callMetadata.set('x-sirius-vm-token', String(vmAuthToken));
    }
    const stream = client.ExecuteCall(callMetadata);
    let hostCallsHandled = 0;

    const timeoutMs = Number(options.timeoutMs || 45000);
    const timeout = setTimeout(() => {
      if (!isCompleted) {
        isCompleted = true;
        try { stream.cancel(); } catch (_) {}
        overlay.rollbackOverlay();
        resetVmClient(targetEndpoint, client);
        reject(new Error(`[${node.name} VM] ExecuteCall timed out after ${timeoutMs}ms`));
      }
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timeout);
    }

    stream.on('data', (responseMsg) => {
      try {
        // 1. Final execution return from WASM VM
        if (responseMsg.returns) {
          if (isCompleted) return;
          isCompleted = true;
          cleanup();
          const ret = responseMsg.returns;
          const stateRootHash = ret.success ? overlay.computeStateRootHash() : null;
          let finalized = false;
          const commit = () => {
            if (finalized) return;
            finalized = true;
            if (ret.success) overlay.commitOverlay();
            else overlay.rollbackOverlay();
          };
          const rollback = () => {
            if (finalized) return;
            finalized = true;
            overlay.rollbackOverlay();
          };

          if (!deferCommit) {
            commit();
          }
          try { stream.end(); } catch (_) {}

          const finishResolve = () =>
            resolve({
              node: node.name,
              vmPort: node.vmPort,
              hostCallsHandled,
              returns: ret,
              stateRootHash,
              commit,
              rollback,
            });

          if (simulatedLatencyMs > 0) {
            setTimeout(finishResolve, simulatedLatencyMs);
          } else {
            finishResolve();
          }
          return;
        }

        hostCallsHandled++;

        // 2. Blockchain Host Queries (`VirtualMachineBlockchainQueryHandler`)
        if (responseMsg.get_block_height) {
          stream.write({ get_block_height_return: { height: String(blockHeight) } });
          return;
        }
        if (responseMsg.get_block_hash) {
          stream.write({
            get_block_hash_return: {
              hash: crypto.createHash('sha256').update(`block-${blockHeight}`).digest(),
            },
          });
          return;
        }
        if (responseMsg.get_block_time) {
          stream.write({ get_block_time_return: { timestamp: String(1760600000000n + BigInt(blockHeight) * 15000n) } });
          return;
        }
        if (responseMsg.get_block_generation_time) {
          stream.write({ get_block_generation_time_return: { delta: '15000' } });
          return;
        }
        if (responseMsg.get_transaction_hash) {
          stream.write({ get_transaction_hash_return: { hash: Buffer.from(txHashHex, 'hex') } });
          return;
        }
        if (responseMsg.get_caller_public_key) {
          stream.write({ get_caller_public_key_return: { public_key: Buffer.from(callerKeyHex, 'hex') } });
          return;
        }
        if (responseMsg.get_contract_public_key) {
          stream.write({ get_contract_public_key_return: { public_key: Buffer.from(CONTRACT_KEY_HEX, 'hex') } });
          return;
        }
        if (responseMsg.get_service_payments) {
          stream.write({
            get_service_payments_return: {
              payments: servicePayments,
            },
          });
          return;
        }
        if (responseMsg.get_execution_payment) {
          stream.write({ get_execution_payment_return: { amount: '5000000' } });
          return;
        }
        if (responseMsg.get_download_payment) {
          stream.write({ get_download_payment_return: { amount: '1000000' } });
          return;
        }

        // 3. Storage & Filesystem Overlay Queries (`VirtualMachineStorageQueryHandler`)
        const storageReply = overlay.handleMessage(responseMsg);
        if (storageReply) {
          stream.write(storageReply);
          return;
        }
      } catch (handlerErr) {
        if (!isCompleted) {
          isCompleted = true;
          cleanup();
          try { overlay.rollbackOverlay(); } catch (_) {}
          try { stream.cancel(); } catch (_) {}
          resetVmClient(targetEndpoint, client);
          reject(handlerErr);
        }
      }
    });

    stream.on('error', (err) => {
      if (isCompleted) return;
      isCompleted = true;
      cleanup();
      overlay.rollbackOverlay();
      resetVmClient(targetEndpoint, client);
      reject(err);
    });

    stream.on('end', () => {
      if (isCompleted) return;
      isCompleted = true;
      cleanup();
      overlay.rollbackOverlay();
      resetVmClient(targetEndpoint, client);
      reject(new Error(`[${node.name} VM] Stream ended abruptly without execution returns`));
    });

    // Send initial ExecuteRequest pointing to the real Drive path on this Replicator
    stream.write({
      request: {
        file_to_call: wasmFilePath,
        function_to_call: functionToCall,
        parameters,
        poex_secret_data_prefix: String(poexPrefix),
        execution_gas_limit: String(executionGasLimit),
        download_gas_limit: String(downloadGasLimit),
        call_mode: 1,
      },
    });
  });
}

function assert3NodeConsensus(stageName, results, expectSuccessOrOpts = true) {
  const opts =
    typeof expectSuccessOrOpts === 'object' && expectSuccessOrOpts !== null
      ? expectSuccessOrOpts
      : { expectSuccess: Boolean(expectSuccessOrOpts), allowQuorum: false };
  const expectSuccess = opts.expectSuccess !== undefined ? Boolean(opts.expectSuccess) : true;
  const allowQuorum = Boolean(opts.allowQuorum);
  const minQuorum = allowQuorum ? Math.max(2, Math.ceil((NODES.length * 2) / 3)) : NODES.length;

  const validResults = (Array.isArray(results) ? results : []).filter(
    (r) => r && r.returns && Boolean(r.returns.success) === expectSuccess
  );

  if (validResults.length < minQuorum) {
    const details = (Array.isArray(results) ? results : [])
      .map((r) => `${r.node}: err=${r.error ? r.error.message || r.error : 'none'}, ret=${r.returns?.return_val}, success=${r.returns?.success}`)
      .join(', ');
    throw new Error(
      `[${stageName}] Consensus divergence: insufficient BFT quorum (${validResults.length}/${NODES.length} valid, need ${minQuorum}) - [${details}]`
    );
  }

  const serializeEmittedTxs = (ret) =>
    JSON.stringify(
      (ret && ret.transaction && Array.isArray(ret.transaction.transactions) ? ret.transaction.transactions : []).map((tx) => ({
        entity_type: tx.entity_type,
        version: tx.version,
        payloadHex: Buffer.isBuffer(tx.payload) ? tx.payload.toString('hex') : Buffer.from(tx.payload || '', 'base64').toString('hex'),
      }))
    );

  const fingerprint = (r) =>
    JSON.stringify({
      success: Boolean(r.returns.success),
      return_val: r.returns.return_val,
      stateRootHash: r.stateRootHash || null,
      txs: serializeEmittedTxs(r.returns),
    });

  if (!allowQuorum) {
    if (!Array.isArray(results) || results.length === 0 || !results[0] || !results[0].returns) {
      throw new Error(`[${stageName}] Consensus failure: missing primary node execution result`);
    }
    const first = results[0].returns;
    const firstFp = fingerprint(results[0]);
    for (const r of results) {
      if (!r || !r.returns || Boolean(r.returns.success) !== expectSuccess) {
        throw new Error(
          `[${stageName}] Node ${r ? r.node : 'unknown'} success=${r && r.returns ? r.returns.success : false}, expected=${expectSuccess}`
        );
      }
      if (fingerprint(r) !== firstFp) {
        throw new Error(
          `[${stageName}] Consensus divergence on ${r.node}: ${JSON.stringify(r.returns)} (stateRoot=${r.stateRootHash}) vs ${JSON.stringify(first)} (stateRoot=${results[0].stateRootHash})`
        );
      }
    }
    return first;
  }

  // Group by full deterministic fingerprint (including post-execution stateRootHash)
  const groups = new Map();
  for (const r of validResults) {
    const fp = fingerprint(r);
    if (!groups.has(fp)) groups.set(fp, []);
    groups.get(fp).push(r);
  }

  for (const group of groups.values()) {
    if (group.length >= minQuorum) {
      const quorumNodes = new Set(group.map((g) => g.node));
      const nonQuorum = (Array.isArray(results) ? results : []).filter(
        (r) => r && !quorumNodes.has(r.node)
      );
      return {
        returns: group[0].returns,
        stateRootHash: group[0].stateRootHash,
        quorum: group,
        nonQuorum,
      };
    }
  }

  const nodeDetails = validResults
    .map((r) => `${r.node}: ret=${r.returns?.return_val} root=${(r.stateRootHash || 'null').slice(0, 10)}`)
    .join(', ');

  throw new Error(
    `[${stageName}] Consensus divergence across nodes: no ${minQuorum}/${NODES.length} BFT quorum agreed on return_val + stateRootHash (${nodeDetails})`
  );
}

module.exports = {
  DriveSandboxOverlay,
  executeOnNodeVm,
  assert3NodeConsensus,
  safeRmSync,
  NODES,
  DRIVE_KEY_HEX,
  CONTRACT_KEY_HEX,
  CALLER_KEY_HEX,
  ALICE_KEY_HEX,
  BOB_KEY_HEX,
  XPX_MOSAIC_ID,
  sha256Hex,
};
