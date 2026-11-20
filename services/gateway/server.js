import express from 'express';
import cors from 'cors';

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.GATEWAY_PORT || process.env.PORT || 5000);
const RAW_URLS = process.env.NODE_URLS || '';
const NODE_URLS = RAW_URLS.split(',')
  .map((u) => u.trim().replace(/\/+$/, ''))
  .filter(Boolean);

async function pingNode(url) {
  const fallbackId = url.split('/').pop() || url;
  try {
    const r = await fetch(`${url}/status`, { signal: AbortSignal.timeout(2000) });
    if (!r.ok) {
      const errData = await r.json().catch(() => ({}));
      return { id: errData.id || fallbackId, url, alive: false, role: 'Dead', term: errData.term || 0 };
    }
    const d = await r.json();
    return { ...d, url };
  } catch {
    return { id: fallbackId, url, alive: false, role: 'Dead', term: 0 };
  }
}

async function clusterSnapshot() {
  const nodes = await Promise.all(NODE_URLS.map(pingNode));
  const reachable = nodes.filter((n) => n.alive && !n.isolated);
  const leaders = reachable.filter((n) => n.role === 'Leader');
  const maxTerm = nodes.reduce((m, n) => Math.max(m, Number(n.term || 0)), 0);
  const total = Math.max(NODE_URLS.length, nodes.length);
  const majority = Math.floor(total / 2) + 1;
  return {
    term: maxTerm,
    leaderId: leaders.length === 1 ? leaders[0].id : null,
    nodes,
    reachableCount: reachable.length,
    totalNodes: total,
    majority,
  };
}

app.get('/', async (_req, res) => {
  const snap = await clusterSnapshot();
  res.json({
    service: 'raft-api-gateway',
    status: 'online',
    cloudProvider: 'Render + Upstash Redis',
    leaderId: snap.leaderId,
    term: snap.term,
    reachableNodes: `${snap.reachableCount}/${snap.totalNodes}`,
    majorityRequired: snap.majority,
    endpoints: ['/cluster', '/write', '/health', '/metrics', '/admin/:nodeId/:action'],
  });
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'raft-gateway', uptime: process.uptime() });
});

app.get('/metrics', async (_req, res) => {
  const snap = await clusterSnapshot();
  res.json({
    term: snap.term,
    leaderId: snap.leaderId,
    reachableNodes: snap.reachableCount,
    totalNodes: snap.totalNodes,
    majority: snap.majority,
  });
});

app.get('/cluster', async (_req, res) => {
  const snap = await clusterSnapshot();
  res.json(snap);
});

app.post('/write', async (req, res) => {
  const { key, value, requestId } = req.body || {};
  if (!key || String(key).trim() === '') {
    return res.status(400).json({ error: 'Key required.' });
  }

  const snap = await clusterSnapshot();
  if (!snap.leaderId) {
    return res.status(503).json({ error: 'No active leader available.' });
  }

  const leaderNode = snap.nodes.find((n) => n.id === snap.leaderId && n.alive && !n.isolated);
  if (!leaderNode) {
    return res.status(503).json({ error: 'Leader not reachable.' });
  }

  try {
    const r = await fetch(`${leaderNode.url}/client/write`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: String(key), value, requestId }),
    });
    const data = await r.json();
    return res.status(r.status).json(data);
  } catch (e) {
    return res.status(502).json({ error: e.message || 'Failed to contact leader.' });
  }
});

app.post('/admin/:nodeId/:action', async (req, res) => {
  const { nodeId, action } = req.params;
  if (!['kill', 'revive', 'isolate', 'heal', 'reset'].includes(action)) {
    return res.status(400).json({ error: 'Invalid admin action' });
  }

  const snap = await clusterSnapshot();
  const matchedNode = snap.nodes.find((n) => String(n.id).toLowerCase() === String(nodeId).toLowerCase());
  const targetUrl =
    matchedNode?.url ||
    NODE_URLS.find((u) => u.toLowerCase().includes(nodeId.toLowerCase().replace('n', 'node-')));

  if (!targetUrl) {
    return res.status(404).json({ error: `Node ${nodeId} not found` });
  }

  try {
    const r = await fetch(`${targetUrl}/admin/${action}`, { method: 'POST' });
    const data = await r.json().catch(() => ({}));
    return res.status(r.status).json(data);
  } catch (e) {
    return res.status(502).json({ error: e.message || 'Admin call failed' });
  }
});

app.listen(PORT, () => console.log(`Raft Gateway listening on :${PORT}`));
