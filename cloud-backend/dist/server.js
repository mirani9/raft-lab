"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const NODE_IDS = ["A", "B", "C", "D", "E"];
const nodes = {};
const eventStream = [];
function logEvent(msg) {
    const entry = `[${new Date().toLocaleTimeString()}] ${msg}`;
    eventStream.unshift(entry);
    if (eventStream.length > 50)
        eventStream.pop();
}
NODE_IDS.forEach((id) => {
    nodes[id] = {
        id,
        role: "Follower",
        currentTerm: 0,
        votedFor: null,
        log: [],
        commitIndex: 0,
        lastApplied: 0,
        kvStore: {},
        partitionGroup: 0,
    };
});
function canCommunicate(a, b) {
    if (a.role === "Dead" || b.role === "Dead")
        return false;
    return a.partitionGroup === b.partitionGroup;
}
setInterval(() => {
    const activeLeaders = NODE_IDS.map((id) => nodes[id]).filter((n) => n.role === "Leader");
    activeLeaders.forEach((leader) => {
        NODE_IDS.forEach((peerId) => {
            if (peerId === leader.id)
                return;
            const peer = nodes[peerId];
            if (!canCommunicate(leader, peer))
                return;
            if (peer.currentTerm < leader.currentTerm) {
                peer.currentTerm = leader.currentTerm;
                peer.role = "Follower";
                peer.votedFor = null;
            }
            if (peer.log.length < leader.log.length) {
                peer.log = JSON.parse(JSON.stringify(leader.log));
            }
            if (peer.commitIndex < leader.commitIndex) {
                peer.commitIndex = leader.commitIndex;
                peer.kvStore = { ...leader.kvStore };
                peer.lastApplied = leader.lastApplied;
            }
        });
    });
    const candidateId = NODE_IDS[Math.floor(Math.random() * NODE_IDS.length)];
    const candidate = nodes[candidateId];
    if (candidate.role === "Dead" || candidate.role === "Leader")
        return;
    const hasReachableLeader = activeLeaders.some((l) => canCommunicate(candidate, l));
    if (!hasReachableLeader) {
        candidate.role = "Candidate";
        candidate.currentTerm += 1;
        candidate.votedFor = candidate.id;
        let votes = 1;
        NODE_IDS.forEach((voterId) => {
            if (voterId === candidate.id)
                return;
            const voter = nodes[voterId];
            if (!canCommunicate(candidate, voter))
                return;
            const candLastTerm = candidate.log.at(-1)?.term ?? 0;
            const voterLastTerm = voter.log.at(-1)?.term ?? 0;
            const logUpToDate = candLastTerm > voterLastTerm ||
                (candLastTerm === voterLastTerm &&
                    candidate.log.length >= voter.log.length);
            if (voter.currentTerm < candidate.currentTerm &&
                logUpToDate) {
                voter.currentTerm = candidate.currentTerm;
                voter.votedFor = candidate.id;
                voter.role = "Follower";
                votes += 1;
            }
        });
        if (votes >= 3) {
            candidate.role = "Leader";
            logEvent(`Node ${candidate.id} elected LEADER for Term ${candidate.currentTerm} (${votes}/5 votes)`);
        }
        else {
            candidate.role = "Follower";
        }
    }
}, 2500);
const app = (0, express_1.default)();
app.use((0, cors_1.default)());
app.use(express_1.default.json());
app.get("/api/cluster", (_req, res) => {
    res.json({
        nodes: Object.values(nodes),
        events: eventStream,
    });
});
app.post("/api/client/set", (req, res) => {
    const { key, value } = req.body;
    const leader = Object.values(nodes).find((n) => n.role === "Leader" && n.partitionGroup === 0) || Object.values(nodes).find((n) => n.role === "Leader");
    if (!leader) {
        return res.status(503).json({ error: "No active leader available" });
    }
    const reachablePeers = Object.values(nodes).filter((n) => canCommunicate(leader, n));
    if (reachablePeers.length < 3) {
        return res
            .status(503)
            .json({ error: "Majority quorum (3/5) unavailable in partition" });
    }
    const newEntry = {
        index: leader.log.length + 1,
        term: leader.currentTerm,
        command: `SET ${key}=${value}`,
    };
    leader.log.push(newEntry);
    leader.commitIndex = newEntry.index;
    leader.lastApplied = newEntry.index;
    leader.kvStore[key] = String(value);
    reachablePeers.forEach((peer) => {
        if (peer.id !== leader.id) {
            peer.log = JSON.parse(JSON.stringify(leader.log));
            peer.commitIndex = leader.commitIndex;
            peer.lastApplied = leader.lastApplied;
            peer.kvStore = { ...leader.kvStore };
        }
    });
    logEvent(`Committed SET ${key}=${value} via Leader ${leader.id}`);
    res.json({ success: true, leader: leader.id, entry: newEntry });
});
app.get("/api/client/get/:key", (req, res) => {
    const leader = Object.values(nodes).find((n) => n.role === "Leader");
    if (!leader) {
        return res.status(503).json({ error: "No active leader available" });
    }
    const val = leader.kvStore[req.params.key] ?? null;
    res.json({ key: req.params.key, value: val, servedBy: leader.id });
});
app.post("/api/chaos/:action", (req, res) => {
    const { action } = req.params;
    const { nodeId } = req.body;
    if (action === "kill" && nodes[nodeId]) {
        nodes[nodeId].role = "Dead";
        logEvent(`CHAOS: Node ${nodeId} killed`);
    }
    else if (action === "revive" && nodes[nodeId]) {
        nodes[nodeId].role = "Follower";
        logEvent(`CHAOS: Node ${nodeId} revived`);
    }
    else if (action === "split-brain") {
        nodes["A"].partitionGroup = 1;
        nodes["B"].partitionGroup = 1;
        nodes["C"].partitionGroup = 0;
        nodes["D"].partitionGroup = 0;
        nodes["E"].partitionGroup = 0;
        logEvent("CHAOS: Split-Brain activated {A, B} vs {C, D, E}");
    }
    else if (action === "heal") {
        NODE_IDS.forEach((id) => (nodes[id].partitionGroup = 0));
        logEvent("CHAOS: All network partitions healed");
    }
    res.json({ success: true, nodes: Object.values(nodes) });
});
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
    console.log(`VLAB Cloud Raft Engine running on port ${PORT}`);
});
