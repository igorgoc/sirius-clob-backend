/**
 * SiriusCLOB Real-Time WebSocket Streaming Hub (/ws/clob)
 *
 * Pushes low-latency (<50ms) orderbook depth, market trade fills, and private user events
 * with monotonic sequence numbers, ping/pong heartbeats, connection caps, and zero-waste diffing.
 */

const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const VALID_MARKETS = new Set(['xpx_usdt', 'xpx_bnb']);
const MAX_WS_CLIENTS = Math.max(20, Number(process.env.CLOB_MAX_WS_CLIENTS || 250));
const HEARTBEAT_INTERVAL_MS = 25000;
const WATCH_INTERVAL_MS = 1000;
const DRIVE_KEY_HEX = 'bd1d100a98c34f62e170a8395b4498dc1e730286f5a12c489b37d01562f0cb23';

function normalizeMarket(raw) {
  const s = String(raw || 'xpx_usdt').trim().toLowerCase().replace('/', '_');
  return VALID_MARKETS.has(s) ? s : 'xpx_usdt';
}

function normalizePubKey(raw) {
  const s = String(raw || '').trim().toLowerCase();
  return /^[0-9a-f]{64}$/.test(s) ? s : null;
}

function attachClobWebSocketHub(httpServer, { clusterDir, clobApi, enrichTradeWithTimestamp }) {
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: 16 * 1024, // 16 KB client message cap
  });

  let seq = 0;
  const lastDepthPayload = { xpx_usdt: '', xpx_bnb: '' };
  const lastTradesMeta = { xpx_usdt: '', xpx_bnb: '' };
  const lastTradesPayload = { xpx_usdt: [], xpx_bnb: [] };

  function nextSeq() {
    seq += 1;
    return seq;
  }

  function safeSend(ws, obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(JSON.stringify(obj));
    } catch (_) {}
  }

  function readDepthForMarket(market) {
    try {
      const parsed = clobApi.getMarketDepthSnapshot(market);
      if (parsed && Array.isArray(parsed.bids) && Array.isArray(parsed.asks)) {
        return parsed;
      }
    } catch (_) {}
    return {
      base_mosaic_hex: '0x0DC67FBE1CAD29E3',
      quote_mosaic_hex: market === 'xpx_bnb' ? '0x1E5C5FADEC7B34DD' : '0x10112CBA1E7B9037',
      best_bid: null,
      best_ask: null,
      bids: [],
      asks: [],
    };
  }

  function readTradesForMarket(market, limit = 100) {
    const tradesPath = path.join(clusterDir, 'node1', 'data', 'drives', DRIVE_KEY_HEX, 'drive', 'markets', market, 'trades.json');
    try {
      const st = fs.statSync(tradesPath);
      if (st.size > 2 * 1024 * 1024) {
        return { changed: false, trades: lastTradesPayload[market] || [] };
      }
      const metaKey = `${st.mtimeMs}:${st.size}`;
      if (lastTradesMeta[market] === metaKey && Array.isArray(lastTradesPayload[market])) {
        return { changed: false, trades: lastTradesPayload[market] };
      }
      const rawTrades = JSON.parse(fs.readFileSync(tradesPath, 'utf8'));
      if (Array.isArray(rawTrades)) {
        const totalLen = rawTrades.length;
        const enriched = rawTrades.map((t, idx) => {
          const { timestamp, time } = typeof enrichTradeWithTimestamp === 'function'
            ? enrichTradeWithTimestamp(t, idx, totalLen, market)
            : { timestamp: Number(t.timestamp) || Date.now(), time: '' };
          return { ...t, timestamp, time };
        });
        const recent = enriched.slice(-limit).reverse();
        lastTradesMeta[market] = metaKey;
        lastTradesPayload[market] = recent;
        return { changed: true, trades: recent };
      }
    } catch (_) {}
    return { changed: false, trades: lastTradesPayload[market] || [] };
  }

  function sendSnapshot(ws, market) {
    const depth = readDepthForMarket(market);
    const { trades } = readTradesForMarket(market, 100);
    safeSend(ws, {
      type: 'snapshot',
      seq: nextSeq(),
      market,
      timestamp: Date.now(),
      depth,
      trades,
    });
  }

  function broadcastMarketUpdate(rawMarket, { force = false } = {}) {
    if (wss.clients.size === 0) return;
    const market = normalizeMarket(rawMarket);

    const depth = readDepthForMarket(market);
    const depthJson = JSON.stringify(depth);
    const depthChanged = force || depthJson !== lastDepthPayload[market];
    if (depthChanged) {
      lastDepthPayload[market] = depthJson;
    }

    const { changed: tradesChanged, trades } = readTradesForMarket(market, 100);
    if (!depthChanged && !tradesChanged) return;

    const now = Date.now();
    const depthFrame = depthChanged ? JSON.stringify({ type: 'depth', seq: nextSeq(), market, timestamp: now, data: depth }) : null;
    const tradesFrame = tradesChanged ? JSON.stringify({ type: 'trades', seq: nextSeq(), market, timestamp: now, data: trades }) : null;

    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      if (client.subscribedMarket !== market) continue;
      try {
        if (depthFrame) client.send(depthFrame);
        if (tradesFrame) client.send(tradesFrame);
      } catch (_) {}
    }
  }

  function notifyUserEvent(publicKey, eventName, payload = {}) {
    const targetPk = normalizePubKey(publicKey);
    if (!targetPk || wss.clients.size === 0) return;
    const safePayload = payload && typeof payload === 'object' ? payload : {};
    const frame = JSON.stringify({
      type: 'user_event',
      seq: nextSeq(),
      event: String(eventName || 'update').slice(0, 48),
      timestamp: Date.now(),
      data: safePayload,
    });
    if (frame.length > 16384) return;
    for (const client of wss.clients) {
      if (client.readyState !== WebSocket.OPEN) continue;
      if (client.subscribedPublicKey === targetPk) {
        try {
          client.send(frame);
        } catch (_) {}
      }
    }
  }

  httpServer.on('upgrade', (req, socket, head) => {
    let pathname = '';
    try {
      const u = new URL(req.url || '/', 'http://localhost');
      pathname = u.pathname;
    } catch (_) {
      socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
      socket.destroy();
      return;
    }

    if (pathname !== '/ws/clob') {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
      return;
    }

    if (wss.clients.size >= MAX_WS_CLIENTS) {
      socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  });

  wss.on('connection', (ws, req) => {
    if (wss.clients.size > MAX_WS_CLIENTS) {
      try { ws.close(1013, 'Server capacity reached'); } catch (_) {}
      return;
    }
    ws.isAlive = true;
    ws.subscribedMarket = 'xpx_usdt';
    ws.subscribedPublicKey = null;
    ws.msgWindowStart = Date.now();
    ws.msgWindowCount = 0;

    try {
      const u = new URL(req.url || '/', 'http://localhost');
      ws.subscribedMarket = normalizeMarket(u.searchParams.get('pair') || u.searchParams.get('market'));
      ws.subscribedPublicKey = normalizePubKey(u.searchParams.get('publicKey'));
    } catch (_) {}

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (raw) => {
      ws.isAlive = true;
      const now = Date.now();
      if (now - ws.msgWindowStart >= 1000) {
        ws.msgWindowStart = now;
        ws.msgWindowCount = 0;
      }
      ws.msgWindowCount += 1;
      if (ws.msgWindowCount > 10) return; // Per-socket DoS rate limit (max 10 msg/s)
      try {
        const msg = JSON.parse(raw.toString('utf8'));
        if (!msg || typeof msg !== 'object') return;
        if (msg.type === 'ping') {
          safeSend(ws, { type: 'pong', timestamp: Date.now() });
          return;
        }
        if (msg.type === 'subscribe') {
          ws.subscribedMarket = normalizeMarket(msg.pair || msg.market || ws.subscribedMarket);
          if (msg.publicKey !== undefined) {
            ws.subscribedPublicKey = normalizePubKey(msg.publicKey);
          }
          sendSnapshot(ws, ws.subscribedMarket);
        }
      } catch (_) {}
    });

    sendSnapshot(ws, ws.subscribedMarket);
  });

  // Heartbeat loop: sever zombie sockets cleanly
  const heartbeatTimer = setInterval(() => {
    for (const ws of wss.clients) {
      if (ws.isAlive === false) {
        try { ws.terminate(); } catch (_) {}
        continue;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch (_) {}
    }
  }, HEARTBEAT_INTERVAL_MS);
  if (heartbeatTimer.unref) heartbeatTimer.unref();

  // Lightweight 1s change-detector for out-of-band updates (e.g. autonomous Market Maker daemon)
  const watchTimer = setInterval(() => {
    if (wss.clients.size === 0) return;
    broadcastMarketUpdate('xpx_usdt');
    broadcastMarketUpdate('xpx_bnb');
  }, WATCH_INTERVAL_MS);
  if (watchTimer.unref) watchTimer.unref();

  wss.on('close', () => {
    clearInterval(heartbeatTimer);
    clearInterval(watchTimer);
  });

  return {
    wss,
    broadcastMarketUpdate,
    notifyUserEvent,
    getClientCount: () => wss.clients.size,
  };
}

module.exports = {
  attachClobWebSocketHub,
};
