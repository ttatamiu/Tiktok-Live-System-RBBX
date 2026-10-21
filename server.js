// Shinbo TikTok Live bridge
// Roblox (HttpService) <-> TikTok LIVE gifts.
//
// Roblox cannot receive webhooks, so the game server POSTs to /poll every
// ~1.5s and this bridge answers with any new gifts for the linked streamers.
//
// ENV:
//   BRIDGE_KEY   (required) secret shared with the Roblox server script
//   PORT         (default 3000)
//   ALLOW_TEST   set to 1 to enable POST /test-gift (fake gifts for testing)
//   MAX_LINKS    max simultaneous linked streamers (default 200)
//   EULER_API_KEY optional Euler Stream key, recommended once many streamers link

const express = require('express');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

const PORT = Number(process.env.PORT) || 3000;
const BRIDGE_KEY = process.env.BRIDGE_KEY;
const ALLOW_TEST = process.env.ALLOW_TEST === '1';
const MAX_LINKS = Number(process.env.MAX_LINKS) || 200;
const EULER_KEY = process.env.EULER_API_KEY || ''; // optional: Euler Stream sign key (higher limits)

const STALE_MS = 90_000;      // no poll from Roblox for this long -> auto unlink
const RETRY_MS = 20_000;      // retry interval while the streamer is offline / errored
const MAX_EVENTS = 200;       // gift events kept per link

if (!BRIDGE_KEY) {
  console.error('Missing BRIDGE_KEY env variable. Example: BRIDGE_KEY=mysecret node server.js');
  process.exit(1);
}

/** @type {Map<number, any>} robloxUserId -> link */
const links = new Map();
/** @type {Map<string, number>} tiktok username -> robloxUserId */
const owners = new Map();

function normalizeUsername(raw) {
  if (typeof raw !== 'string') return null;
  let s = raw.trim();
  const fromUrl = s.match(/tiktok\.com\/@([A-Za-z0-9_.]+)/i);
  if (fromUrl) s = fromUrl[1];
  s = s.replace(/^@/, '').toLowerCase();
  if (!/^[a-z0-9_.]{2,24}$/.test(s)) return null;
  return s;
}

function setStatus(link, status) {
  link.status = status;
}

function onGift(link, d) {
  const det = d.giftDetails || {};
  const streakable = det.giftType === 1;

  // Streakable gifts (combos) fire many times; only count the final one.
  if (streakable && !d.repeatEnd) return;

  const count = Math.max(1, Number(d.repeatCount) || 1);
  const unit = Number(det.diamondCount) || 0;

  link.events.push({
    id: ++link.seq,
    name: String(det.giftName || `Gift ${d.giftId}`),
    giftId: Number(d.giftId) || 0,
    count,
    unit,
    diamonds: unit * count,
    from: String(d.user?.uniqueId || 'viewer'),
    t: Date.now(),
  });

  if (link.events.length > MAX_EVENTS) {
    link.events.splice(0, link.events.length - MAX_EVENTS);
  }
}

function scheduleRetry(link, status) {
  if (link.stopped) return;
  setStatus(link, status);
  clearTimeout(link.retryTimer);
  link.retryTimer = setTimeout(() => connect(link).catch(() => {}), RETRY_MS);
}

async function connect(link) {
  if (link.stopped) return;

  clearTimeout(link.retryTimer);
  setStatus(link, 'connecting');

  const conn = new TikTokLiveConnection(link.username, EULER_KEY ? { signApiKey: EULER_KEY } : {});
  link.conn = conn;

  conn.on(WebcastEvent.GIFT, (d) => {
    if (link.conn === conn) onGift(link, d);
  });

  conn.on(ControlEvent.DISCONNECTED, () => {
    if (link.conn === conn) scheduleRetry(link, 'offline');
  });

  conn.on(WebcastEvent.STREAM_END, () => {
    if (link.conn === conn) scheduleRetry(link, 'offline');
  });

  conn.on(ControlEvent.ERROR, (err) => {
    console.error(`[${link.username}] connection error:`, err?.message || err);
  });

  try {
    await conn.connect();

    if (link.stopped || link.conn !== conn) {
      try { conn.disconnect(); } catch {}
      return;
    }

    setStatus(link, 'connected');
    console.log(`[${link.username}] connected`);
  } catch (err) {
    if (link.conn !== conn) return;

    const text = `${err?.name || ''} ${err?.message || ''}`;
    const offline = /offline|not.?live|UserOffline|room.?id/i.test(text);

    console.log(`[${link.username}] ${offline ? 'not live' : 'connect failed'}: ${err?.message || err}`);
    scheduleRetry(link, offline ? 'offline' : 'error');
  }
}

function destroyLink(link) {
  link.stopped = true;
  clearTimeout(link.retryTimer);

  const conn = link.conn;
  link.conn = null;

  if (conn) {
    try { conn.removeAllListeners(); } catch {}
    try { conn.disconnect(); } catch {}
  }

  links.delete(link.userId);
  if (owners.get(link.username) === link.userId) owners.delete(link.username);
}

const app = express();
app.use(express.json({ limit: '32kb' }));

app.get('/health', (_req, res) => {
  res.json({ ok: true, links: links.size });
});

app.use((req, res, next) => {
  if (req.get('x-bridge-key') !== BRIDGE_KEY) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
});

// Link a Roblox player to a TikTok username
app.post('/link', (req, res) => {
  const userId = Number(req.body?.userId);
  const jobId = String(req.body?.jobId ?? '');
  const username = normalizeUsername(req.body?.username);

  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: 'bad_user' });
  if (!username) return res.status(400).json({ error: 'bad_username' });

  // Someone else already owns this username (and is still active)?
  const ownerId = owners.get(username);
  if (ownerId && ownerId !== userId) {
    const other = links.get(ownerId);
    if (other && Date.now() - other.lastPoll < STALE_MS) {
      return res.status(409).json({ error: 'username_in_use' });
    }
    if (other) destroyLink(other);
  }

  // Same player re-linking
  const existing = links.get(userId);
  if (existing) {
    if (existing.username === username && !existing.stopped) {
      existing.jobId = jobId;
      existing.lastPoll = Date.now();
      return res.json({ ok: true, status: existing.status, seq: existing.seq });
    }
    destroyLink(existing);
  }

  if (links.size >= MAX_LINKS) return res.status(503).json({ error: 'bridge_full' });

  const link = {
    userId,
    username,
    jobId,
    status: 'connecting',
    seq: 0,
    events: [],
    conn: null,
    retryTimer: null,
    lastPoll: Date.now(),
    stopped: false,
  };

  links.set(userId, link);
  owners.set(username, userId);
  connect(link).catch((e) => console.error('connect crashed:', e?.message || e));

  res.json({ ok: true, status: link.status, seq: link.seq });
});

app.post('/unlink', (req, res) => {
  const link = links.get(Number(req.body?.userId));
  if (link) destroyLink(link);
  res.json({ ok: true });
});

// Batch poll: body { jobId, players: { "<userId>": <lastSeenSeq>, ... } }
app.post('/poll', (req, res) => {
  const jobId = String(req.body?.jobId ?? '');
  const players = req.body?.players && typeof req.body.players === 'object' ? req.body.players : {};
  const out = {};

  for (const [key, sinceRaw] of Object.entries(players)) {
    const link = links.get(Number(key));

    if (!link) {
      out[key] = { status: 'none' };
      continue;
    }

    // Player moved to a different Roblox server: only the newest one gets gifts
    if (link.jobId !== jobId) {
      out[key] = { status: 'replaced' };
      continue;
    }

    link.lastPoll = Date.now();
    const since = Number(sinceRaw) || 0;

    out[key] = {
      status: link.status,
      username: link.username,
      seq: link.seq,
      events: link.events.filter((e) => e.id > since),
    };
  }

  res.json({ ok: true, players: out });
});

// Fake gift for testing without being live (needs ALLOW_TEST=1)
app.post('/test-gift', (req, res) => {
  if (!ALLOW_TEST) return res.status(404).json({ error: 'disabled' });

  const link = links.get(Number(req.body?.userId));
  if (!link) return res.status(404).json({ error: 'not_linked' });

  const count = Math.max(1, Number(req.body?.count) || 1);
  const unit = Number(req.body?.diamonds) || 1;

  link.events.push({
    id: ++link.seq,
    name: String(req.body?.giftName || 'Rose'),
    giftId: 0,
    count,
    unit,
    diamonds: unit * count,
    from: 'test_viewer',
    t: Date.now(),
  });

  res.json({ ok: true, seq: link.seq });
});

// Drop links whose Roblox server stopped polling (player left / server closed)
setInterval(() => {
  const now = Date.now();
  for (const link of [...links.values()]) {
    if (now - link.lastPoll > STALE_MS) {
      console.log(`[${link.username}] stale, unlinking`);
      destroyLink(link);
    }
  }
}, 30_000).unref();

process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e?.message || e));
process.on('uncaughtException', (e) => console.error('uncaughtException:', e?.message || e));

app.listen(PORT, () => console.log(`Shinbo TikTok bridge listening on :${PORT}`));
