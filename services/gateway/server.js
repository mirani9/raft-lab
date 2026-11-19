import express from 'express';
import cors from 'cors';

const app = express();
app.use(express.json());
app.use(cors());

const PORT = Number(process.env.GATEWAY_PORT || process.env.PORT || 5000);
const NODE_URLS = String(process.env.NODE_URLS || process.env.PEERS || '').split(',').map((u) => u.trim().replace(/\/+$/, '')).filter(Boolean);
const CLUSTER_SIZE = NODE_URLS.length || 5;
const MAJORITY = Math.floor(CLUSTER_SIZE / 2) + 1;

const clients = new Set();
const prevMap = new Map();

function emit(type, term, message, extra = {}) {
  const data = JSON.stringify({ type, term: Number(term) || 0, message, timestamp: new Date().toISOString(), ...extra });
  for (const res of clients) res.write(`data: ${data}\n\n`);
}

async function ping(url) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), 700);
  try {
    const r = await fetch(`${url}/status`, { signal: ac.signal });
    const b = await r.json().catch(() => ({}));
    if (!r.ok) return { url, id: b.id || url, role: 'Dead', term: b.term ?? 0, votedFor: b.votedFor ?? null, leaderId: null, commitIndex: b.commitIndex ?? 0, snapshotIndex: b.snapshotIndex ?? 0, logLength: b.logLength ?? 0, log: b.log || [], alive: false, isolated: Boolean(b.isolated), reachable: false };
    return { ...b, url, reachable: !b.isolated && b.alive !== false };
  } catch {
    return { url, id: url, role: 'Dead', term: 0, votedFor: null, leaderId: null, commitIndex: 0, snapshotIndex: 0, logLength: 0, log: [], alive: false, isolated: false, reachable: false };
  } finally {
    clearTimeout(t);
  }
}

async function snapshot() {
  const nodes = await Promise.all(NODE_URLS.map(ping));
  const reachable = nodes.filter((n) => n.alive && !n.isolated);
  const leaders = reachable.filter((n) => n.role === 'Leader');
  const term = nodes.reduce((m, n) => Math.max(m, Number(n.term || 0)), 0);

  for (const n of nodes) {
    const key = n.id || n.url;
    const prev = prevMap.get(key);
    if (!prev) { prevMap.set(key, { role: n.role, commitIndex: n.commitIndex, alive: n.alive, isolated: n.isolated }); continue; }
    if (prev.alive && !n.alive) emit('node_killed', n.term, `Node ${n.id} killed.`, { nodeId: n.id });
    if (!prev.alive && n.alive) emit('node_revived', n.term, `Node ${n.id} revived as Follower.`, { nodeId: n.id });
    if (!prev.isolated && n.isolated) emit('node_isolated', n.term, `Node ${n.id} isolated.`, { nodeId: n.id });
    if (prev.isolated && !n.isolated) emit('node_healed', n.term, `Node ${n.id} healed.`, { nodeId: n.id });
    if (prev.role !== n.role) {
      if (n.role === 'Candidate') emit('election_started', n.term, `Node ${n.id} started election.`, { nodeId: n.id });
      else if (n.role === 'Leader') emit('leader_elected', n.term, `Node ${n.id} elected Leader.`, { nodeId: n.id });
      else if (prev.role === 'Leader' && n.role === 'Follower') emit('step_down', n.term, `Node ${n.id} stepped down.`, { nodeId: n.id });
    }
    if (n.commitIndex > prev.commitIndex) emit('entry_committed', n.term, `Node ${n.id} committed #${n.commitIndex}.`, { nodeId: n.id });
    prevMap.set(key, { role: n.role, commitIndex: n.commitIndex, alive: n.alive, isolated: n.isolated });
  }

  return { term, leaderId: leaders.length === 1 ? leaders[0].id : null, clusterSize: CLUSTER_SIZE, majority: MAJORITY, reachableCount: reachable.length, nodes };
}

app.get('/cluster', async (_req, res) => res.json(await snapshot()));

app.post('/write', async (req, res) => {
  const { key, value, requestId } = req.body || {};
  if (!key || !String(key).trim()) return res.status(400).json({ error: 'Key required.' });
  const reqId = requestId || `req-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
  const snap = await snapshot();
  const leaders = snap.nodes.filter((n) => n.alive && !n.isolated && n.role === 'Leader');
  if (!leaders.length) {
    emit('write_rejected', snap.term, `Write [${key}=${value}] rejected: 503 No leader.`);
    return res.status(503).json({ error: 'No active leader available.' });
  }

  let url = leaders[0].url;
  for (let i = 0; i < 3; i++) {
    try {
      const r = await fetch(`${url}/client/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, value, requestId: reqId }),
      });
      const d = await r.json().catch(() => ({}));
      if (r.status === 409 && d.leaderId) {
        const next = snap.nodes.find((n) => n.id === d.leaderId && n.alive && !n.isolated);
        if (next) { url = next.url; continue; }
      }
      if (r.status === 200 && d.committed) emit('entry_committed', d.term ?? snap.term, `Committed [${key}=${value}] at #${d.index}.`);
      if (r.status === 202) emit('entry_uncommitted', d.term ?? snap.term, `Uncommitted [${key}=${value}] at #${d.index}.`);
      return res.status(r.status).json({ ...d, requestId: reqId });
    } catch (e) {
      if (i === 2) return res.status(502).json({ error: e.message });
    }
  }
});

app.post('/admin/:nodeId/:action', async (req, res) => {
  const { nodeId, action } = req.params;
  if (!['kill', 'revive', 'isolate', 'heal', 'reset'].includes(action)) return res.status(400).json({ error: 'Invalid action' });
  const snap = await snapshot();
  const target = snap.nodes.find((n) => String(n.id).toLowerCase() === String(nodeId).toLowerCase());
  if (!target) return res.status(404).json({ error: 'Node not found' });
  const r = await fetch(`${target.url}/admin/${action}`, { method: 'POST' });
  const d = await r.json().catch(() => ({}));
  emit(`admin_${action}`, snap.term, `Admin ${action} on ${target.id}.`, { nodeId: target.id });
  return res.status(r.status).json(d);
});

app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();
  res.write(`data: ${JSON.stringify({ type: 'connected', term: 0, message: 'SSE connected.', timestamp: new Date().toISOString() })}\n\n`);
  clients.add(res);
  req.on('close', () => clients.delete(res));
});

setInterval(() => { snapshot().catch(() => {}); }, 1000);
app.listen(PORT, () => console.log(`Gateway on :${PORT}`));
