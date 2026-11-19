import express, { Request, Response } from 'express';
import cors from 'cors';
import path from 'path';

const app = express();
app.use(express.json());
app.use(cors());
app.use(express.static(path.join(__dirname, '../public')));

const PORT = Number(process.env.PORT || 3000);
const NODE_ID = (process.env.NODE_ID || 'A').toUpperCase();
const envTotal = process.env.TOTAL_NODES ? parseInt(process.env.TOTAL_NODES, 10) : 5;
const TOTAL_NODES = Math.max(3, Math.min(10, Number.isNaN(envTotal) ? 5 : envTotal));

const envQuorum = process.env.QUORUM_OVERRIDE ? parseInt(process.env.QUORUM_OVERRIDE, 10) : NaN;
const DEFAULT_MAJORITY = Math.floor(TOTAL_NODES / 2) + 1;
const MAJORITY = !Number.isNaN(envQuorum) && envQuorum >= 1 ? envQuorum : DEFAULT_MAJORITY;

type NodeRole = 'Follower' | 'Candidate' | 'Leader' | 'Dead';

interface PeerInfo {
  id: string;
  url: string;
  isAlive: boolean;
  isIsolated: boolean;
}

const ALL_IDS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'];

function buildInitialPeers(size: number): Record<string, PeerInfo> {
  const peers: Record<string, PeerInfo> = {};
  for (let i = 0; i < size; i++) {
    const id = ALL_IDS[i];
    peers[id] = {
      id,
      url: `http://raft-node-${id.toLowerCase()}:3000`,
      isAlive: true,
      isIsolated: false,
    };
  }
  return peers;
}

const state = {
  id: NODE_ID,
  role: 'Follower' as NodeRole,
  term: 1,
  votedFor: null as string | null,
  commitIndex: 0,
  isAlive: true,
  isIsolated: false,
  clusterSize: TOTAL_NODES,
  peers: buildInitialPeers(TOTAL_NODES),
  kv: {} as Record<string, string>,
  logs: [] as Array<{ index: number; term: number; key: string; value: string; committed: boolean }>,
};

function getReachableCount(): number {
  if (!state.isAlive || state.isIsolated) return 0;
  let reachable = state.peers[NODE_ID]?.isAlive && !state.peers[NODE_ID]?.isIsolated ? 1 : 0;
  for (const [id, peer] of Object.entries(state.peers)) {
    if (id !== NODE_ID && peer.isAlive && !peer.isIsolated) {
      reachable += 1;
    }
  }
  return reachable;
}

app.get('/health', (_req: Request, res: Response) => {
  res.json({ ok: true, id: state.id, role: state.role, term: state.term, majority: MAJORITY });
});

app.get('/status', (_req: Request, res: Response) => {
  const activeSize = Object.keys(state.peers).length;
  const majority = !Number.isNaN(envQuorum) && envQuorum >= 1 ? envQuorum : Math.floor(activeSize / 2) + 1;
  res.json({
    id: state.id,
    role: state.role,
    term: state.term,
    votedFor: state.votedFor,
    commitIndex: state.commitIndex,
    isAlive: state.isAlive,
    isIsolated: state.isIsolated,
    reachableSize: getReachableCount(),
    totalSize: activeSize,
    majorityRequired: majority,
    kv: state.kv,
    logs: state.logs,
  });
});

app.post('/write', (req: Request, res: Response) => {
  const { key, value } = req.body || {};
  if (!state.isAlive) {
    return res.status(503).json({ success: false, error: `Node ${state.id} is dead.` });
  }
  if (state.isIsolated) {
    return res.status(503).json({
      success: false,
      error: `Split-Brain Partition: Node ${state.id} is isolated from majority quorum.`,
    });
  }
  if (state.role !== 'Leader') {
    return res.status(409).json({
      success: false,
      error: `Node ${state.id} is not Leader (current role: ${state.role}).`,
    });
  }

  const activeSize = Object.keys(state.peers).length;
  const majority = !Number.isNaN(envQuorum) && envQuorum >= 1 ? envQuorum : Math.floor(activeSize / 2) + 1;
  const reachable = getReachableCount();

  if (reachable < majority) {
    return res.status(503).json({
      success: false,
      error: `Quorum Lost: Reachable nodes (${reachable}/${activeSize}) < Majority required (${majority}). Write rejected.`,
    });
  }

  const nextIndex = state.logs.length + 1;
  const entry = {
    index: nextIndex,
    term: state.term,
    key: String(key ?? ''),
    value: String(value ?? ''),
    committed: true,
  };
  state.logs.push(entry);
  if (entry.key) {
    state.kv[entry.key] = entry.value;
  }
  state.commitIndex = nextIndex;

  return res.json({
    success: true,
    index: nextIndex,
    term: state.term,
    reachable,
    activeSize,
    majority,
    kv: state.kv,
  });
});

app.post('/control/candidate', (_req: Request, res: Response) => {
  if (!state.isAlive || state.isIsolated) {
    return res.status(400).json({ success: false, error: 'Dead or isolated node cannot campaign.' });
  }
  state.term += 1;
  state.role = 'Candidate';
  state.votedFor = state.id;
  res.json({ success: true, id: state.id, role: state.role, term: state.term });
});

app.post('/control/leader', (_req: Request, res: Response) => {
  if (!state.isAlive || state.isIsolated) {
    return res.status(400).json({ success: false, error: 'Dead or isolated node cannot become leader.' });
  }
  state.role = 'Leader';
  state.votedFor = state.id;
  res.json({ success: true, id: state.id, role: state.role, term: state.term });
});

app.post('/control/kill', (_req: Request, res: Response) => {
  state.isAlive = false;
  state.role = 'Dead';
  if (state.peers[state.id]) state.peers[state.id].isAlive = false;
  res.json({ success: true, id: state.id, role: state.role });
});

app.post('/control/revive', (_req: Request, res: Response) => {
  state.isAlive = true;
  state.isIsolated = false;
  state.role = 'Follower';
  if (state.peers[state.id]) {
    state.peers[state.id].isAlive = true;
    state.peers[state.id].isIsolated = false;
  }
  res.json({ success: true, id: state.id, role: state.role });
});

app.post('/control/isolate', (_req: Request, res: Response) => {
  state.isIsolated = true;
  if (state.peers[state.id]) state.peers[state.id].isIsolated = true;
  if (state.role === 'Leader') state.role = 'Follower';
  res.json({ success: true, id: state.id, isIsolated: true });
});

app.post('/control/heal', (_req: Request, res: Response) => {
  state.isIsolated = false;
  if (state.peers[state.id]) state.peers[state.id].isIsolated = false;
  if (state.isAlive && state.role === 'Dead') state.role = 'Follower';
  res.json({ success: true, id: state.id, isIsolated: false });
});

app.listen(PORT, () => {
  console.log(`Raft Node ${NODE_ID} listening on ${PORT} (cluster=${TOTAL_NODES}, majority=${MAJORITY})`);
});
