'use strict';

const fs = require('fs');
const path = require('path');

function fsyncDirBestEffort(dir) {
  let dirFd = null;
  try {
    dirFd = fs.openSync(dir, 'r');
    fs.fsyncSync(dirFd);
  } catch (_) {
    // Directory fsync may be unsupported on some platforms/filesystems
  } finally {
    if (dirFd !== null) {
      try { fs.closeSync(dirFd); } catch (_) {}
    }
  }
}

function writeAndFsyncTemp(tmpPath, content, mode = null) {
  let fd = null;
  try {
    fd = mode !== null && mode !== undefined
      ? fs.openSync(tmpPath, 'w', mode)
      : fs.openSync(tmpPath, 'w');
    fs.writeFileSync(fd, content, 'utf8');
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fd = null;
  } catch (err) {
    if (fd !== null) {
      try { fs.closeSync(fd); } catch (_) {}
    }
    try {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
    } catch (_) {}
    throw err;
  }
}

/**
 * Crash-safe atomic JSON writer:
 * 1. Writes serialized JSON to a unique sibling temp file and calls fsyncSync(fd)
 * 2. If backup=true and existing primary file is valid JSON, atomically rotates it to `<filePath>.bak`
 *    (so a corrupt primary file NEVER overwrites a known-good `.bak` backup)
 * 3. Atomically renames temp file over target path and fsyncs the parent directory
 */
function atomicWriteJsonSync(filePath, data, { backup = true, mode = null, spaces = 2 } = {}) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
    fsyncDirBestEffort(dir);
  }
  const serialized = JSON.stringify(data, null, spaces);
  const uniq = `${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2, 8)}`;
  const tmpFile = `${filePath}.tmp.${uniq}`;
  try {
    writeAndFsyncTemp(tmpFile, serialized, mode);

    if (backup && fs.existsSync(filePath)) {
      try {
        const existingRaw = fs.readFileSync(filePath, 'utf8');
        // Only rotate to .bak if the existing primary file parses as valid JSON
        JSON.parse(existingRaw);
        const bakTmp = `${filePath}.bak.tmp.${uniq}`;
        writeAndFsyncTemp(bakTmp, existingRaw, mode);
        fs.renameSync(bakTmp, `${filePath}.bak`);
      } catch (_) {}
    }

    fs.renameSync(tmpFile, filePath);
    fsyncDirBestEffort(dir);
  } catch (err) {
    try {
      if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
    } catch (_) {}
    throw err;
  }
}

/**
 * Crash-resilient JSON loader:
 * If the primary file is truncated/corrupt, backs it up to `.corrupt.<ts>` and falls back to `.bak`.
 */
function readJsonWithBackupSync(filePath, fallback = null) {
  if (fs.existsSync(filePath)) {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (err) {
      const corruptPath = `${filePath}.corrupt.${Date.now()}`;
      try {
        fs.renameSync(filePath, corruptPath);
        console.warn(`[AtomicFS] ⚠️ Corrupt JSON ${path.basename(filePath)} backed up to ${corruptPath}: ${err.message}`);
      } catch (_) {}
    }
  }
  const bakPath = `${filePath}.bak`;
  if (fs.existsSync(bakPath)) {
    try {
      const restored = JSON.parse(fs.readFileSync(bakPath, 'utf8'));
      console.warn(`[AtomicFS] Restored ${path.basename(filePath)} from .bak backup`);
      try {
        atomicWriteJsonSync(filePath, restored, { backup: false });
      } catch (_) {}
      return restored;
    } catch (_) {}
  }
  return fallback;
}

module.exports = {
  atomicWriteJsonSync,
  readJsonWithBackupSync,
};
