const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.GATEWAY_PORT || process.env.PORT || 5000);
const RAW_URLS = [
  process.env.NODE_URLS,
  process.env.RAFT_NODE_URL,
  process.env.PEERS,
]
  .filter(Boolean)
  .join(',');

const NODE_URLS = RAW_URLS
  .split(',')
  .map((u) => u.trim().replace(/\/+$/, ''))
  .filter(Boolean);

const CLUSTER_SIZE = NODE_URLS.length || 5;
const sseClients = new Set();

function broadcast(ev) {
  const payload = `data: ${JSON.stringify(ev)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch {}
  }
}

async function ping(url) {
  const idFromUrl = url.split('/').pop() || url;
  try {
    const r = await fetch(`${url}/status`, { signal: AbortSignal.timeout(1500) });
    if (!r.ok) return { id: idFromUrl, url, alive: false, isolated: false, role: 'Dead', term: 0, log: [], kv: {} };
    const d = await r.json();
    return { ...d, url };
  } catch {
    return { id: idFromUrl, url, alive: false, isolated: false, role: 'Dead', term: 0, log: [], kv: {} };
  }
}

async function snapshot() {
  const nodes = await Promise.all(NODE_URLS.map(ping));
  const reachable = nodes.filter((n) => n.alive && !n.isolated);
  const leaders = reachable.filter((n) => n.role === 'Leader');
  const maxTerm = nodes.reduce((m, n) => Math.max(m, Number(n.term || 0)), 0);
  const totalConfigured = Math.max(CLUSTER_SIZE, nodes.length);
  const majority = Math.floor(totalConfigured / 2) + 1;

  return {
    term: maxTerm,
    leaderId: leaders.length === 1 ? leaders[0].id : null,
    clusterSize: totalConfigured,
    majority,
    reachableCount: reachable.length,
    nodes,
  };
}

function mountRoutes(prefix) {
  app.get(`${prefix}/cluster`, async (_req, res) => res.json(await snapshot()));

  app.post(`${prefix}/write`, async (req, res) => {
    const { key, value, requestId } = req.body || {};
    if (!key || !String(key).trim()) return res.status(400).json({ error: 'Key required.' });

    const snap = await snapshot();
    const ordered = [
      ...snap.nodes.filter((n) => n.id === snap.leaderId && n.alive && !n.isolated),
      ...snap.nodes.filter((n) => n.alive && !n.isolated && n.id !== snap.leaderId),
    ];
    if (!ordered.length) return res.status(503).json({ error: 'No active leader available.' });

    for (const node of ordered) {
      try {
        const r = await fetch(`${node.url}/client/write`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key: String(key).trim(), value, requestId }),
          signal: AbortSignal.timeout(4000),
        });
        const data = await r.json().catch(() => ({}));
        if (r.status === 200 || r.status === 202) {
          broadcast({
            type: 'client_write',
            term: data.term ?? snap.term,
            timestamp: new Date().toISOString(),
            message: `${data.committed ? 'Committed' : 'Uncommitted'} ${key}=${value} via ${node.id}`,
          });
          return res.status(r.status).json(data);
        }
      } catch {}
    }
    return res.status(503).json({ error: 'No active leader available.' });
  });

  app.post(`${prefix}/admin/:nodeId/:action`, async (req, res) => {
    const { nodeId, action } = req.params;
    if (!['kill', 'revive', 'isolate', 'heal', 'reset'].includes(action)) {
      return res.status(400).json({ error: 'Invalid action' });
    }
    const snap = await snapshot();
    const target = snap.nodes.find((n) => n.id === nodeId);
    if (!target) return res.status(404).json({ error: `Node ${nodeId} not found` });

    try {
      const r = await fetch(`${target.url}/admin/${action}`, { method: 'POST', signal: AbortSignal.timeout(2000) });
      const data = await r.json().catch(() => ({}));
      broadcast({
        type: `admin_${action}`,
        nodeId,
        term: data.term ?? snap.term,
        timestamp: new Date().toISOString(),
        message: `Admin ${action} on ${nodeId}`,
      });
      return res.status(r.status).json(data);
    } catch (err) {
      return res.status(502).json({ error: err.message || 'Admin call failed' });
    }
  });

  app.get(`${prefix}/events`, (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();
    res.write(`data: ${JSON.stringify({ type: 'connected', term: 0, timestamp: new Date().toISOString(), message: 'SSE connected' })}\n\n`);
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
  });
}

mountRoutes('');
mountRoutes('/api');

app.listen(PORT, () => console.log(`RaftLab Gateway on :${PORT} (${NODE_URLS.length} nodes)`));
