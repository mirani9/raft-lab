const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Configuration
const PORT = Number(process.env.GATEWAY_PORT || process.env.PORT || 5000);
const RAW_URLS = process.env.NODE_URLS || '';
const NODE_URLS = RAW_URLS.split(',').map(u => u.trim().replace(/\\+$/,'' )).filter(Boolean);

// Helper to ping a node for its status
async function pingNode(url) {
  const id = url.split('/').pop() || url;
  try {
    const r = await fetch(`${url}/status`, { signal: AbortSignal.timeout(1500) });
    if (!r.ok) return { id, url, alive: false, role: 'Dead', term: 0 };
    const d = await r.json();
    return { ...d, url };
  } catch {
    return { id, url, alive: false, role: 'Dead', term: 0 };
  }
}

// Collect snapshot of the cluster
async function clusterSnapshot() {
  const nodes = await Promise.all(NODE_URLS.map(pingNode));
  const reachable = nodes.filter(n => n.alive && !n.isolated);
  const leaders = reachable.filter(n => n.role === 'Leader');
  const maxTerm = nodes.reduce((m, n) => Math.max(m, Number(n.term || 0)), 0);
  const total = Math.max(NODE_URLS.length, nodes.length);
  const majority = Math.floor(total / 2) + 1;
  return { term: maxTerm, leaderId: leaders.length===1 ? leaders[0].id : null, nodes, reachableCount: reachable.length, majority };
}

// ==== Public API ==== //

// Health check â€“ always returns 200 if the gateway process is alive
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// Simple metrics endpoint (JSON) â€“ useful for monitoring
app.get('/metrics', async (req, res) => {
  const snap = await clusterSnapshot();
  res.json({
    term: snap.term,
    leaderId: snap.leaderId,
    reachableNodes: snap.reachableCount,
    totalNodes: NODE_URLS.length,
    majority
  });
});

// Write endpoint â€“ forwards the client write to the current leader only
app.post('/write', async (req, res) => {
  const { key, value, requestId } = req.body || {};
  if (!key || String(key).trim() === '') return res.status(400).json({ error: 'Key required.' });

  const snap = await clusterSnapshot();
  if (!snap.leaderId) return res.status(503).json({ error: 'No active leader available.' });

  const leaderNode = snap.nodes.find(n => n.id === snap.leaderId && n.alive && !n.isolated);
  if (!leaderNode) return res.status(503).json({ error: 'Leader not reachable.' });

  try {
    const r = await fetch(`${leaderNode.url}/client/write`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: String(key), value, requestId })
    });
    const data = await r.json();
    return res.status(r.status).json(data);
  } catch (e) {
    return res.status(502).json({ error: e.message || 'Failed to contact leader.' });
  }
});

// Admin proxy â€“ forwards admin actions to the target node
app.post('/admin/:nodeId/:action', async (req, res) => {
  const { nodeId, action } = req.params;
  if (!['kill','revive','isolate','heal'].includes(action)) {
    return res.status(400).json({ error: 'Invalid admin action' });
  }
  const target = NODE_URLS.find(u => u.endsWith(`/${nodeId}`) || u.includes(`/${nodeId}`));
  if (!target) return res.status(404).json({ error: `Node ${nodeId} not found` });
  try {
    const r = await fetch(`${target}/admin/${action}`, { method: 'POST' });
    const data = await r.json().catch(() => ({}));
    return res.status(r.status).json(data);
  } catch (e) {
    return res.status(502).json({ error: e.message || 'Admin call failed' });
  }
});

// Cluster status endpoint â€“ returns same shape as Vercel snapshot for UI consumption
app.get('/cluster', async (req, res) => {
  const snap = await clusterSnapshot();
  res.json(snap);
});

app.listen(PORT, () => console.log(`Raft Gateway listening on :${PORT}`));

