import express from 'express';
import cors from 'cors';

const app = express();
app.use(express.json());
app.use(cors());

const PORT = Number(process.env.PORT || 4001);
const NODE_ID = String(process.env.NODE_ID || `NODE_${PORT}`).trim();
const PEERS = String(process.env.PEERS || '').split(',').map((u) => u.trim()).filter(Boolean);
const STORE_URL = String(process.env.STORE_URL || '').trim().replace(/\/+$/, '');
const STORE_TOKEN = String(process.env.STORE_TOKEN || '').trim();

const CLUSTER_SIZE = PEERS.length > 0 ? PEERS.length : 5;
const MAJORITY = Math.floor(CLUSTER_SIZE / 2) + 1;
const COMPACTION_THRESHOLD = 20;
const KEY_PREFIX = `raft:v26:node:${NODE_ID}`;
const mem = new Map();

async function kvGet(k) {
  const fk = `${KEY_PREFIX}:${k}`;
  if (STORE_URL && STORE_TOKEN) {
    const r = await fetch(`${STORE_URL}/get/${encodeURIComponent(fk)}`, {
      headers: { Authorization: `Bearer ${STORE_TOKEN}` },
    });
    if (!r.ok) throw new Error(`Redis GET ${r.status}`);
    const j = await r.json();
    return j.result ?? null;
  }
  return mem.get(fk) ?? null;
}

async function kvSet(k, v) {
  const fk = `${KEY_PREFIX}:${k}`;
  const val = typeof v === 'string' ? v : JSON.stringify(v);
  if (STORE_URL && STORE_TOKEN) {
    const r = await fetch(`${STORE_URL}/set/${encodeURIComponent(fk)}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${STORE_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(val),
    });
    if (!r.ok) throw new Error(`Redis SET ${r.status}`);
    return;
  }
  mem.set(fk, val);
}

async function kvClear() {
  for (const s of ['term', 'votedFor', 'log', 'snapshot', 'applied']) {
    const fk = `${KEY_PREFIX}:${s}`;
    if (STORE_URL && STORE_TOKEN) {
      await fetch(`${STORE_URL}/del/${encodeURIComponent(fk)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${STORE_TOKEN}` },
      });
    } else {
      mem.delete(fk);
    }
  }
}

const s = {
  id: NODE_ID,
  role: 'Follower',
  currentTerm: 0,
  votedFor: null,
  leaderId: null,
  commitIndex: 0,
  lastApplied: 0,
  log: [],
  snapshot: { lastIncludedIndex: 0, lastIncludedTerm: 0, kv: {} },
  applied: {},
  alive: true,
  isolated: false,
  nextIndex: {},
  matchIndex: {},
  lastHb: Date.now(),
  electionsStarted: 0,
  latencies: [],
  lastLeaderChangeAt: null,
};

function randTimeout() {
  return 1500 + Math.floor(Math.random() * 1501);
}

function lastMeta() {
  if (s.log.length > 0) {
    const x = s.log[s.log.length - 1];
    return { index: x.index, term: x.term };
  }
  return { index: s.snapshot.lastIncludedIndex, term: s.snapshot.lastIncludedTerm };
}

function upToDate(cTerm, cIdx) {
  const m = lastMeta();
  return cTerm !== m.term ? cTerm > m.term : cIdx >= m.index;
}

async function saveHard() {
  await kvSet('term', String(s.currentTerm));
  await kvSet('votedFor', s.votedFor === null ? '' : String(s.votedFor));
}

async function saveState() {
  await kvSet('log', JSON.stringify(s.log));
  await kvSet('snapshot', JSON.stringify(s.snapshot));
  await kvSet('applied', JSON.stringify(s.applied));
  if (s.role === 'Leader' && STORE_URL && STORE_TOKEN) {
    await fetch(${STORE_URL}/set/, {
      method: 'POST',
      headers: { Authorization: Bearer , 'Content-Type': 'application/json' },
      body: JSON.stringify(JSON.stringify({ updatedBy: s.id, term: s.currentTerm, kv: currentKv(), log: s.log })),
    }).catch(() => {});
  }
}

function currentKv() {
  const out = { ...s.snapshot.kv };
  for (const e of s.log) {
    if (e.index <= s.lastApplied && e.committed) out[e.key] = e.value;
  }
  return out;
}

function compactIfNeeded() {
  const committed = s.log.filter((e) => e.committed);
  if (committed.length < COMPACTION_THRESHOLD) return;
  const cut = committed[committed.length - 1];
  const kv = currentKv();
  for (const e of committed) kv[e.key] = e.value;
  s.snapshot = { lastIncludedIndex: cut.index, lastIncludedTerm: cut.term, kv };
  s.log = s.log.filter((e) => e.index > cut.index);
  s.lastApplied = Math.max(s.lastApplied, cut.index);
  s.commitIndex = Math.max(s.commitIndex, cut.index);
}

function applyCommitted() {
  for (const e of s.log) {
    if (e.index <= s.commitIndex && e.index > s.lastApplied) {
      e.committed = true;
      s.lastApplied = e.index;
    }
  }
  compactIfNeeded();
}

async function loadState() {
  const t = await kvGet('term');
  const v = await kvGet('votedFor');
  const l = await kvGet('log');
  const snap = await kvGet('snapshot');
  const appReq = await kvGet('applied');

  s.currentTerm = t ? Number(t) || 0 : 0;
  s.votedFor = v && v.length > 0 ? v : null;
  s.snapshot = snap ? JSON.parse(snap) : { lastIncludedIndex: 0, lastIncludedTerm: 0, kv: {} };
  s.log = l ? JSON.parse(l) : [];
  s.applied = appReq ? JSON.parse(appReq) : {};
  s.commitIndex = s.snapshot.lastIncludedIndex || 0;
  s.lastApplied = s.snapshot.lastIncludedIndex || 0;
  applyCommitted();
}

async function stepDown(newTerm) {
  if (newTerm > s.currentTerm) {
    s.currentTerm = newTerm;
    s.votedFor = null;
  }
  s.role = 'Follower';
  if (s.leaderId === s.id) s.leaderId = null;
  await saveHard();
}

async function rpc(peer, route, body) {
  if (!s.alive || s.isolated) throw new Error('blocked');
  const ac = new AbortController();
  const id = setTimeout(() => ac.abort(), 350);
  try {
    const r = await fetch(`${peer.replace(/\/+$/, '')}${route}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: ac.signal,
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } finally {
    clearTimeout(id);
  }
}

async function replicateAll() {
  if (!s.alive || s.isolated || s.role !== 'Leader') return;
  const termSnap = s.currentTerm;
  const m = lastMeta();

  await Promise.all(
    PEERS.map(async (p) => {
      try {
        const nextIdx = s.nextIndex[p] ?? m.index + 1;
        const entries = s.log.filter((e) => e.index >= nextIdx);
        const prevIdx = Math.max(s.snapshot.lastIncludedIndex, nextIdx - 1);
        let prevTerm = s.snapshot.lastIncludedTerm;
        if (prevIdx > 0) {
          const found = s.log.find((e) => e.index === prevIdx);
          if (found) prevTerm = found.term;
        }
        const resp = await rpc(p, '/rpc/append-entries', {
          term: s.currentTerm,
          leaderId: s.id,
          prevLogIndex: prevIdx,
          prevLogTerm: prevTerm,
          entries,
          leaderCommit: s.commitIndex,
        });
        if (resp?.term > s.currentTerm) {
          await stepDown(resp.term);
          return;
        }
        if (resp?.success) {
          const maxSent = entries.length > 0 ? entries[entries.length - 1].index : prevIdx;
          s.matchIndex[p] = Math.max(s.matchIndex[p] || 0, maxSent);
          s.nextIndex[p] = s.matchIndex[p] + 1;
        } else if (resp) {
          s.nextIndex[p] = Math.max(s.snapshot.lastIncludedIndex + 1, (s.nextIndex[p] || 2) - 1);
        }
      } catch {}
    })
  );

  if (s.role !== 'Leader' || s.currentTerm !== termSnap) return;
  for (let idx = lastMeta().index; idx > s.commitIndex; idx--) {
    const entry = s.log.find((e) => e.index === idx);
    if (!entry || entry.term !== s.currentTerm) continue;
    let count = 1;
    for (const p of PEERS) {
      if ((s.matchIndex[p] || 0) >= idx) count += 1;
    }
    if (count >= MAJORITY) {
      s.commitIndex = idx;
      applyCommitted();
      await saveState();
      break;
    }
  }
}

async function runElection() {
  if (!s.alive || s.isolated) return;
  s.currentTerm += 1;
  s.role = 'Candidate';
  s.votedFor = s.id;
  s.leaderId = null;
  s.electionsStarted += 1;
  s.lastHb = Date.now();
  await saveHard();

  const t = s.currentTerm;
  const m = lastMeta();
  let votes = 1;

  const resps = await Promise.all(
    PEERS.map(async (p) => {
      try {
        const resp = await rpc(p, '/rpc/request-vote', {
          term: t,
          candidateId: s.id,
          lastLogIndex: m.index,
          lastLogTerm: m.term,
        });
        if (resp?.term > s.currentTerm) await stepDown(resp.term);
        return Boolean(resp?.voteGranted);
      } catch {
        return false;
      }
    })
  );

  if (!s.alive || s.isolated || s.role !== 'Candidate' || s.currentTerm !== t) return;
  for (const g of resps) if (g) votes += 1;

  if (votes >= MAJORITY) {
    s.role = 'Leader';
    s.leaderId = s.id;
    s.lastLeaderChangeAt = new Date().toISOString();
    const curLast = lastMeta().index;
    for (const p of PEERS) {
      s.nextIndex[p] = curLast + 1;
      s.matchIndex[p] = s.snapshot.lastIncludedIndex;
    }
    await replicateAll();
  }
}

app.post('/rpc/request-vote', async (req, res) => { // Raft RequestVote RPC

  if (!s.alive || s.isolated) return res.status(503).json({ term: s.currentTerm, voteGranted: false });
  const { term, candidateId, lastLogIndex = 0, lastLogTerm = 0 } = req.body || {};
  if (typeof term !== 'number') return res.status(400).json({ error: 'bad term' });
  if (term > s.currentTerm) await stepDown(term);
  if (term < s.currentTerm) return res.json({ term: s.currentTerm, voteGranted: false });

  const allow = (s.votedFor === null || s.votedFor === candidateId) && upToDate(Number(lastLogTerm), Number(lastLogIndex));
  if (allow) {
    s.votedFor = candidateId;
    s.lastHb = Date.now();
    await saveHard();
    return res.json({ term: s.currentTerm, voteGranted: true });
  }
  return res.json({ term: s.currentTerm, voteGranted: false });
});

app.post('/rpc/append-entries', async (req, res) => {
  if (!s.alive || s.isolated) return res.status(503).json({ term: s.currentTerm, success: false });
  const { term, leaderId, prevLogIndex = 0, prevLogTerm = 0, entries = [], leaderCommit = 0 } = req.body || {};
  if (typeof term !== 'number') return res.status(400).json({ error: 'bad term' });
  if (term < s.currentTerm) return res.json({ term: s.currentTerm, success: false });
  if (term > s.currentTerm || s.role !== 'Follower') await stepDown(term);

  s.leaderId = leaderId;
  s.lastHb = Date.now();

  if (prevLogIndex > 0) {
    if (prevLogIndex < s.snapshot.lastIncludedIndex) return res.json({ term: s.currentTerm, success: false });
    if (prevLogIndex === s.snapshot.lastIncludedIndex) {
      if (s.snapshot.lastIncludedTerm !== prevLogTerm) return res.json({ term: s.currentTerm, success: false });
    } else {
      const prev = s.log.find((e) => e.index === prevLogIndex);
      if (!prev || prev.term !== prevLogTerm) return res.json({ term: s.currentTerm, success: false });
    }
  }

  for (const inc of entries) {
    if (inc.index <= s.snapshot.lastIncludedIndex) continue;
    const cIdx = s.log.findIndex((e) => e.index === inc.index);
    if (cIdx !== -1 && s.log[cIdx].term !== inc.term) s.log.splice(cIdx);
    if (!s.log.some((e) => e.index === inc.index)) {
      s.log.push({ index: inc.index, term: inc.term, key: inc.key, value: inc.value, requestId: inc.requestId, committed: false });
    }
    if (inc.requestId) {
      s.applied[inc.requestId] = { index: inc.index, term: inc.term, key: inc.key, value: inc.value, committed: false };
    }
  }

  const lastNew = entries.length > 0 ? entries[entries.length - 1].index : lastMeta().index;
  if (typeof leaderCommit === 'number' && leaderCommit > s.commitIndex) {
    s.commitIndex = Math.min(leaderCommit, Math.max(lastNew, s.commitIndex));
    applyCommitted();
  }

  await saveState();
  return res.json({ term: s.currentTerm, success: true });
});

app.post('/client/write', async (req, res) => {
  if (!s.alive) return res.status(503).json({ error: `Node ${s.id} is dead.` });
  if (s.isolated) return res.status(503).json({ error: `Node ${s.id} is isolated.`, leaderId: s.leaderId });
  if (s.role !== 'Leader') return res.status(409).json({ error: 'Not leader.', leaderId: s.leaderId });

  const { key, value, requestId } = req.body || {};
  if (!key || String(key).trim() === '') return res.status(400).json({ error: 'Key required.' });
  if (requestId && s.applied[requestId]) {
    const ex = s.applied[requestId];
    return res.status(200).json({ status: 'deduplicated', index: ex.index, term: ex.term, committed: ex.committed });
  }

  const t0 = Date.now();
  const idx = lastMeta().index + 1;
  const entry = {
    index: idx,
    term: s.currentTerm,
    key: String(key),
    value: String(value ?? ''),
    requestId: requestId || `${s.id}-${Date.now()}-${idx}`,
    committed: false,
  };
  s.log.push(entry);
  s.applied[entry.requestId] = entry;
  await saveState();

  const termSnap = s.currentTerm;
  let acks = 1;
  await Promise.all(
    PEERS.map(async (p) => {
      try {
        const prevIdx = idx - 1;
        let prevTerm = s.snapshot.lastIncludedTerm;
        if (prevIdx > 0) {
          const prev = s.log.find((e) => e.index === prevIdx);
          if (prev) prevTerm = prev.term;
        }
        const resp = await rpc(p, '/rpc/append-entries', {
          term: s.currentTerm,
          leaderId: s.id,
          prevLogIndex: prevIdx,
          prevLogTerm: prevTerm,
          entries: [entry],
          leaderCommit: s.commitIndex,
        });
        if (resp?.term > s.currentTerm) await stepDown(resp.term);
        if (resp?.success) {
          acks += 1;
          s.matchIndex[p] = Math.max(s.matchIndex[p] || 0, idx);
          s.nextIndex[p] = idx + 1;
        }
      } catch {}
    })
  );

  if (s.isolated || s.role !== 'Leader' || s.currentTerm !== termSnap || acks < MAJORITY || entry.term !== s.currentTerm) {
    return res.status(202).json({
      status: 'uncommitted',
      reason: `Stored (${acks}/${CLUSTER_SIZE} acks, majority ${MAJORITY}); not committed.`,
      index: entry.index,
      term: entry.term,
      committed: false,
    });
  }

  s.commitIndex = Math.max(s.commitIndex, idx);
  entry.committed = true;
  s.applied[entry.requestId] = entry;
  applyCommitted();
  await saveState();

  const lat = Date.now() - t0;
  s.latencies.push(lat);
  if (s.latencies.length > 100) s.latencies.shift();

  return res.status(200).json({
    status: 'committed',
    index: entry.index,
    term: entry.term,
    key: entry.key,
    value: entry.value,
    committed: true,
    latencyMs: lat,
    snapshotIndex: s.snapshot.lastIncludedIndex,
  });
});

app.get('/', (_req, res) => {
  res.json({
    service: 'raft-consensus-node',
    nodeId: s.id,
    role: s.alive ? (s.isolated ? 'Isolated' : s.role) : 'Dead',
    term: s.currentTerm,
    commitIndex: s.commitIndex,
    cloudStorageConnected: Boolean(STORE_URL && STORE_TOKEN),
    logEntriesCount: s.log.length,
  });
});

app.get('/status', (_req, res) => {
  if (!s.alive) {
    return res.status(503).json({
      id: s.id, role: 'Dead', term: s.currentTerm, votedFor: s.votedFor, leaderId: null,
      commitIndex: s.commitIndex, lastApplied: s.lastApplied, snapshotIndex: s.snapshot.lastIncludedIndex,
      logLength: s.log.length, log: s.log, kv: currentKv(), alive: false, isolated: s.isolated,
      clusterSize: CLUSTER_SIZE, majority: MAJORITY,
    });
  }
  const avgLat = s.latencies.length ? Math.round(s.latencies.reduce((a, b) => a + b, 0) / s.latencies.length) : 0;
  return res.json({
    id: s.id, role: s.role, term: s.currentTerm, votedFor: s.votedFor, leaderId: s.isolated ? null : s.leaderId,
    commitIndex: s.commitIndex, lastApplied: s.lastApplied, snapshotIndex: s.snapshot.lastIncludedIndex,
    logLength: s.log.length, log: s.log, kv: currentKv(), alive: s.alive, isolated: s.isolated,
    clusterSize: CLUSTER_SIZE, majority: MAJORITY,
    metrics: { electionsStarted: s.electionsStarted, avgCommitLatencyMs: avgLat, lastLeaderChangeAt: s.lastLeaderChangeAt },
  });
});

app.post('/admin/kill', (_req, res) => {
  s.alive = false;
  s.role = 'Follower';
  s.leaderId = null;
  s.commitIndex = s.snapshot.lastIncludedIndex || 0;
  s.lastApplied = s.snapshot.lastIncludedIndex || 0;
  res.json({ ok: true, id: s.id, action: 'kill' });
});

app.post('/admin/revive', async (_req, res) => {
  await loadState();
  s.alive = true;
  s.isolated = false;
  s.role = 'Follower';
  s.lastHb = Date.now();
  res.json({ ok: true, id: s.id, action: 'revive', term: s.currentTerm });
});

app.post('/admin/isolate', (_req, res) => {
  s.isolated = true;
  if (s.role === 'Leader') { s.role = 'Follower'; s.leaderId = null; }
  res.json({ ok: true, id: s.id, action: 'isolate' });
});

app.post('/admin/heal', (_req, res) => {
  s.isolated = false;
  s.lastHb = Date.now();
  res.json({ ok: true, id: s.id, action: 'heal' });
});

app.post('/admin/reset', async (_req, res) => {
  await kvClear();
  s.role = 'Follower'; s.currentTerm = 0; s.votedFor = null; s.leaderId = null;
  s.commitIndex = 0; s.lastApplied = 0; s.log = [];
  s.snapshot = { lastIncludedIndex: 0, lastIncludedTerm: 0, kv: {} };
  s.applied = {}; s.alive = true; s.isolated = false; s.lastHb = Date.now();
  res.json({ ok: true, id: s.id, action: 'reset' });
});

let timeoutMs = randTimeout();
setInterval(async () => {
  if (!s.alive || s.isolated) return;
  if (s.role === 'Leader') { await replicateAll(); return; }
  if (Date.now() - s.lastHb >= timeoutMs) { timeoutMs = randTimeout(); await runElection(); }
}, 150);

loadState().then(() => app.listen(PORT, () => console.log(`[${NODE_ID}] on :${PORT}`)));

