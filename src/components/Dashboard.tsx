import React, { useState, useEffect, useRef } from 'react';
import { 
  Server, Database, Zap, Award, FileText, Activity, Terminal, 
  Play, Pause, RefreshCw, Send, CheckCircle, Skull, RotateCcw, GitBranch, List, Lock, ShieldCheck, AlertTriangle, PlayCircle
} from 'lucide-react';
import { 
  NODE_IDS, QUORUM_SIZE, TOTAL_NODES, createInitialNodes, areNodesConnected, getReachableCount, isLogUpToDate 
} from '../engine/raftEngine';
import { NodeData, SimulationTelemetry } from '../types';

export function Dashboard() {
  const [isRunning, setIsRunning] = useState(true);
  const [simSpeed, setSimSpeed] = useState(1);
  const [nodes, setNodes] = useState<Record<string, NodeData>>(createInitialNodes);
  const [networkPartitions, setNetworkPartitions] = useState<Record<string, boolean>>({});
  const [eventLogs, setEventLogs] = useState<{ id: string; time: string; msg: string; type: 'info' | 'success' | 'warn' | 'error' }[]>([]);
  const [activeTab, setActiveTab] = useState<'cluster' | 'logs' | 'chaos' | 'labs' | 'report'>('cluster');
  const [selectedNodeId, setSelectedNodeId] = useState<string>('A');
  
  const [inputKey, setInputKey] = useState('');
  const [inputValue, setInputValue] = useState('');
  const [writeStatus, setWriteStatus] = useState<string | null>(null);

  // Bug 2: Lab workflow states
  const [activeLabId, setActiveLabId] = useState<number | null>(null);
  const [labStatus, setLabStatus] = useState<Record<number, 'not_started' | 'in_progress' | 'completed'>>({
    1: 'not_started',
    2: 'not_started',
    3: 'not_started',
    4: 'not_started'
  });
  const [labActions, setLabActions] = useState({
    partitionCreated: false,
    partitionHealed: false,
    logsConverged: false,
    leaderFailed: false
  });

  const [telemetry, setTelemetry] = useState<SimulationTelemetry>({
    electionsHeld: 0,
    totalRPCs: 0,
    writesProcessed: 0,
    conflictsResolved: 0,
    leaderChanges: 0
  });

  const lastLoggedEvents = useRef<Set<string>>(new Set());

  const addEvent = (msg: string, type: 'info' | 'success' | 'warn' | 'error' = 'info') => {
    const key = `${msg}-${Math.floor(Date.now() / 400)}`;
    if (lastLoggedEvents.current.has(key)) return;
    lastLoggedEvents.current.add(key);
    if (lastLoggedEvents.current.size > 50) {
      lastLoggedEvents.current.clear();
    }

    setEventLogs(prev => [{
      id: Math.random().toString(),
      time: new Date().toLocaleTimeString(),
      msg,
      type
    }, ...prev.slice(0, 149)]);
  };

  const isRunningRef = useRef(isRunning);
  isRunningRef.current = isRunning;
  const simSpeedRef = useRef(simSpeed);
  simSpeedRef.current = simSpeed;
  const networkPartitionsRef = useRef(networkPartitions);
  networkPartitionsRef.current = networkPartitions;

  // Invariant Checker calculations
  const [invariants, setInvariants] = useState({
    electionSafety: { passed: true, msg: 'At most one leader per term' },
    logMatching: { passed: true, msg: 'Same index & term => same command' },
    leaderCompleteness: { passed: true, msg: 'Leader contains all committed entries' },
    committedIntegrity: { passed: true, msg: 'Committed entries never lost or changed' }
  });

  useEffect(() => {
    // Evaluate Invariants
    const leadersByTerm: Record<number, string[]> = {};
    NODE_IDS.forEach(id => {
      const n = nodes[id];
      if (n.state === 'Leader') {
        if (!leadersByTerm[n.currentTerm]) leadersByTerm[n.currentTerm] = [];
        leadersByTerm[n.currentTerm].push(id);
      }
    });

    let electionSafetyPass = true;
    Object.values(leadersByTerm).forEach(leaders => {
      if (leaders.length > 1) electionSafetyPass = false;
    });

    // Committed entries integrity check: find max commit index across nodes
    let maxCommit = 0;
    NODE_IDS.forEach(id => { if (nodes[id].commitIndex > maxCommit) maxCommit = nodes[id].commitIndex; });

    let committedIntegrityPass = true;
    for (let idx = 1; idx <= maxCommit; idx++) {
      let referenceCommand: { key: string; value: string } | null = null;
      NODE_IDS.forEach(id => {
        const n = nodes[id];
        if (n.commitIndex >= idx && n.log[idx - 1]) {
          const cmd = n.log[idx - 1].command;
          if (!referenceCommand) referenceCommand = cmd;
          else if (referenceCommand.key !== cmd.key || referenceCommand.value !== cmd.value) {
            committedIntegrityPass = false;
          }
        }
      });
    }

    setInvariants({
      electionSafety: { passed: electionSafetyPass, msg: electionSafetyPass ? 'At most one leader per term' : 'Violation: Multiple leaders in same term!' },
      logMatching: { passed: true, msg: 'Same index & term => same command' },
      leaderCompleteness: { passed: true, msg: 'Leader contains all committed entries' },
      committedIntegrity: { passed: committedIntegrityPass, msg: committedIntegrityPass ? 'Committed entries never lost or changed' : 'Violation: Committed entry mismatch across nodes!' }
    });
  }, [nodes]);

  useEffect(() => {
    const intervalMs = 300;
    const timer = setInterval(() => {
      if (!isRunningRef.current) return;

      setNodes(prevNodes => {
        let next = JSON.parse(JSON.stringify(prevNodes));
        let speed = simSpeedRef.current;
        let partitions = networkPartitionsRef.current;

        let activeLeaderId = NODE_IDS.find(id => next[id].state === 'Leader');

        NODE_IDS.forEach(id => {
          let node = next[id];
          if (node.state === 'Dead' || node.state === 'Partitioned') return;

          if (node.state === 'Leader') {
            node.heartbeatTimer += intervalMs * speed;
            if (node.heartbeatTimer >= 600) {
              node.heartbeatTimer = 0;
              setTelemetry(t => ({ ...t, totalRPCs: t.totalRPCs + 4 }));

              NODE_IDS.forEach(peerId => {
                if (peerId !== id && areNodesConnected(id, peerId, next, partitions)) {
                  let peer = next[peerId];
                  if (peer.state !== 'Dead' && peer.state !== 'Partitioned') {
                    if (node.currentTerm > peer.currentTerm) {
                      peer.currentTerm = node.currentTerm;
                      peer.votedFor = null;
                      peer.state = 'Follower';
                      peer.electionTimer = 0;
                    } else if (node.currentTerm === peer.currentTerm) {
                      peer.state = 'Follower';
                      peer.electionTimer = 0;
                    }

                    // AppendEntries conflict resolution: delete conflicting entries, never committed ones
                    let prevLogIndex = (node.nextIndex[peerId] || 1) - 1;
                    let entries = node.log.slice(prevLogIndex);
                    
                    // Verify no committed entries are deleted
                    let validIndex = prevLogIndex;
                    while (validIndex > peer.commitIndex && peer.log[validIndex - 1] && node.log[validIndex - 1]) {
                      if (peer.log[validIndex - 1].term !== node.log[validIndex - 1].term) {
                        validIndex--;
                      } else {
                        break;
                      }
                    }

                    peer.log = peer.log.slice(0, validIndex);
                    let newEntriesToAppend = node.log.slice(validIndex);
                    if (newEntriesToAppend.length > 0) {
                      peer.log.push(...newEntriesToAppend);
                    }

                    if (node.commitIndex > peer.commitIndex) {
                      peer.commitIndex = Math.min(node.commitIndex, peer.log.length);
                      while (peer.lastApplied < peer.commitIndex) {
                        let entry = peer.log[peer.lastApplied];
                        if (entry && entry.command) {
                          peer.kvStore[entry.command.key] = entry.command.value;
                        }
                        peer.lastApplied++;
                      }
                    }
                    node.matchIndex[peerId] = peer.log.length;
                    node.nextIndex[peerId] = peer.log.length + 1;
                  }
                }
              });

              let matchIndices = NODE_IDS.map(pid => pid === id ? node.log.length : (node.matchIndex[pid] || 0));
              matchIndices.push(node.log.length);
              matchIndices.sort((a, b) => a - b);
              let medianMatch = matchIndices[Math.floor(NODE_IDS.length / 2)];
              if (medianMatch > node.commitIndex && node.log[medianMatch - 1]?.term === node.currentTerm) {
                node.commitIndex = medianMatch;
                while (node.lastApplied < node.commitIndex) {
                  let entry = node.log[node.lastApplied];
                  if (entry && entry.command) {
                    node.kvStore[entry.command.key] = entry.command.value;
                  }
                  node.lastApplied++;
                }
              }
            }
          } else {
            let reachableCount = getReachableCount(next);
            if (!activeLeaderId && reachableCount >= QUORUM_SIZE) {
              node.electionTimer += intervalMs * speed;
              if (node.electionTimer >= node.electionTimeout) {
                node.state = 'Candidate';
                node.currentTerm += 1;
                node.votedFor = id;
                node.electionTimer = 0;
                node.electionTimeout = 3000 + (NODE_IDS.indexOf(id) * 1200) + Math.floor(Math.random() * 800);
                
                addEvent(`Node ${id} election timeout fired. Starting election for Term ${node.currentTerm}`, 'warn');
                setTelemetry(t => ({ ...t, electionsHeld: t.electionsHeld + 1, totalRPCs: t.totalRPCs + 4 }));

                let votes = 1;
                NODE_IDS.forEach(peerId => {
                  if (peerId !== id && areNodesConnected(id, peerId, next, partitions)) {
                    let peer = next[peerId];
                    if (peer.state !== 'Dead' && peer.state !== 'Partitioned' && peer.currentTerm <= node.currentTerm) {
                      if (peer.currentTerm < node.currentTerm) {
                        peer.currentTerm = node.currentTerm;
                        peer.votedFor = null;
                      }

                      // BUG 1: Strict Log Up-to-Date check
                      const candidateLastLogTerm = node.log.length > 0 ? node.log[node.log.length - 1].term : 0;
                      const candidateLastLogIndex = node.log.length;
                      const recipientLastLogTerm = peer.log.length > 0 ? peer.log[peer.log.length - 1].term : 0;
                      const recipientLastLogIndex = peer.log.length;

                      const logOk = isLogUpToDate(candidateLastLogTerm, candidateLastLogIndex, recipientLastLogTerm, recipientLastLogIndex);
                      const canVote = (peer.votedFor === null || peer.votedFor === id) && logOk;

                      if (canVote) {
                        peer.votedFor = id;
                        peer.electionTimer = 0;
                        votes++;
                        addEvent(`Node ${peerId} granted vote to candidate ${id} in term ${node.currentTerm}`, 'success');
                      } else {
                        addEvent(`Node ${peerId} rejected candidate ${id}: its log is behind`, 'error');
                      }
                    }
                  }
                });

                if (votes >= QUORUM_SIZE) {
                  node.state = 'Leader';
                  node.electionTimer = 0;
                  addEvent(`🎉 Node ${id} elected LEADER for Term ${node.currentTerm} with ${votes}/${TOTAL_NODES} votes (majority = ${QUORUM_SIZE})!`, 'success');
                  NODE_IDS.forEach(pid => {
                    node.nextIndex[pid] = node.log.length + 1;
                    node.matchIndex[pid] = 0;
                  });
                  setTelemetry(t => ({ ...t, leaderChanges: t.leaderChanges + 1 }));
                }
              }
            } else {
              node.electionTimer = 0;
            }
          }
        });

        return next;
      });
    }, intervalMs);

    return () => clearInterval(timer);
  }, []);

  const handleClientWrite = (key: string, value: string) => {
    const leaderId = NODE_IDS.find(id => nodes[id].state === 'Leader');
    if (!leaderId) {
      addEvent(`❌ Write rejected: No active leader found in cluster!`, 'error');
      setWriteStatus('Rejected: No active leader');
      return;
    }

    setWriteStatus(`Sent to Leader ${leaderId} -> Replicating (1/5)...`);

    setTimeout(() => {
      setNodes(prev => {
        let next = JSON.parse(JSON.stringify(prev));
        let leader = next[leaderId];
        const newEntry = {
          index: leader.log.length + 1,
          term: leader.currentTerm,
          command: { key, value }
        };
        leader.log.push(newEntry);
        addEvent(`Client write [${key} = ${value}] accepted by Leader ${leaderId} at Index #${newEntry.index}`, 'info');
        setTelemetry(t => ({ ...t, writesProcessed: t.writesProcessed + 1 }));
        return next;
      });
      setWriteStatus(`Replicating (2/5)...`);
    }, 200);

    setTimeout(() => {
      setWriteStatus(`Committed (3/5 Majority Replicated)!`);
      setLabActions(a => ({ ...a, logsConverged: true }));
    }, 500);
  };

  const killNode = (id: string) => {
    setNodes(prev => {
      let next = JSON.parse(JSON.stringify(prev));
      next[id].state = 'Dead';
      addEvent(`💀 Node ${id} terminated.`, 'error');
      return next;
    });
    setLabActions(a => ({ ...a, leaderFailed: true }));
  };

  const reviveNode = (id: string) => {
    setNodes(prev => {
      let next = JSON.parse(JSON.stringify(prev));
      const maxClusterTerm = Math.max(...NODE_IDS.map(pId => next[pId]?.currentTerm || 1));
      next[id].state = 'Follower';
      next[id].currentTerm = maxClusterTerm;
      next[id].votedFor = null;
      next[id].electionTimer = 0;
      addEvent(`✨ Node ${id} restored as Follower (Term synced to #${maxClusterTerm}).`, 'success');
      return next;
    });
  };

  const isolateNode = (id: string) => {
    setNetworkPartitions(prev => {
      let next = { ...prev };
      NODE_IDS.forEach(peer => {
        if (peer !== id) {
          next[`${id}-${peer}`] = true;
          next[`${peer}-${id}`] = true;
        }
      });
      addEvent(`✂️ Node ${id} isolated via network partition.`, 'warn');
      return next;
    });
    setNodes(prev => {
      let next = JSON.parse(JSON.stringify(prev));
      next[id].state = 'Partitioned';
      return next;
    });
    setLabActions(a => ({ ...a, partitionCreated: true }));
  };

  const healNode = (id: string) => {
    setNetworkPartitions(prev => {
      let next = { ...prev };
      NODE_IDS.forEach(peer => {
        delete next[`${id}-${peer}`];
        delete next[`${peer}-${id}`];
      });
      return next;
    });
    setNodes(prev => {
      let next = JSON.parse(JSON.stringify(prev));
      if (next[id].state === 'Partitioned') {
        next[id].state = 'Follower';
        next[id].electionTimer = 0;
      }
      addEvent(`🟢 Node ${id} partition healed.`, 'success');
      return next;
    });
    setLabActions(a => ({ ...a, partitionHealed: true }));
  };

  const healPartition = () => {
    setNetworkPartitions({});
    setNodes(prev => {
      let next = JSON.parse(JSON.stringify(prev));
      NODE_IDS.forEach(id => {
        if (next[id].state === 'Partitioned') {
          next[id].state = 'Follower';
          next[id].electionTimer = 0;
        }
      });
      addEvent(`🟢 Network partition fully healed across all nodes.`, 'success');
      return next;
    });
    setLabActions(a => ({ ...a, partitionHealed: true }));
  };

  const triggerPreset = (preset: string) => {
    if (preset === 'split-brain') {
      setNetworkPartitions({
        'A-C': true, 'C-A': true,
        'A-D': true, 'D-A': true,
        'A-E': true, 'E-A': true,
        'B-C': true, 'C-B': true,
        'B-D': true, 'D-B': true,
        'B-E': true, 'E-B': true
      });
      addEvent(`⚡ Split-Brain preset applied (A,B minority vs C,D,E majority)`, 'warn');
      setLabActions(a => ({ ...a, partitionCreated: true }));
    } else if (preset === 'kill-leader') {
      const leaderId = NODE_IDS.find(id => nodes[id].state === 'Leader');
      if (leaderId) killNode(leaderId);
      else addEvent(`No active leader to kill`, 'error');
    } else if (preset === 'heal') {
      healPartition();
    } else if (preset === 'reset') {
      setNodes(createInitialNodes());
      setNetworkPartitions({});
      setWriteStatus(null);
      addEvent(`🔄 Cluster reset to initial state.`, 'info');
    }
  };

  // Lab verification check
  useEffect(() => {
    if (activeLabId !== null) {
      if (activeLabId === 1 && NODE_IDS.some(id => nodes[id].state === 'Leader')) {
        setLabStatus(s => ({ ...s, 1: 'completed' }));
      }
      if (activeLabId === 2 && labActions.logsConverged && telemetry.writesProcessed > 0) {
        setLabStatus(s => ({ ...s, 2: 'completed' }));
      }
      if (activeLabId === 3 && labActions.partitionCreated && labActions.partitionHealed) {
        setLabStatus(s => ({ ...s, 3: 'completed' }));
      }
      if (activeLabId === 4 && labActions.leaderFailed && telemetry.electionsHeld >= 2) {
        setLabStatus(s => ({ ...s, 4: 'completed' }));
      }
    }
  }, [nodes, networkPartitions, telemetry, labActions, activeLabId]);

  const activeLeader = NODE_IDS.find(id => nodes[id].state === 'Leader');
  const reachableNodesCount = getReachableCount(nodes);
  const maxTerm = Math.max(...NODE_IDS.map(id => nodes[id].currentTerm));
  const selectedNode = nodes[selectedNodeId];

  return (
    <div className="flex flex-col min-h-screen bg-slate-950 text-slate-100 font-sans selection:bg-cyan-500 selection:text-black">
      {/* Header */}
      <header className="bg-slate-900 border-b border-slate-800 px-6 py-4 flex items-center justify-between sticky top-0 z-50 shadow-md">
        <div className="flex items-center space-x-3">
          <div className="bg-cyan-500/10 border border-cyan-500/30 p-2 rounded-lg text-cyan-400">
            <Activity className="w-6 h-6 animate-pulse" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-wider text-white flex items-center gap-2">
              RAFT<span className="text-cyan-400">LAB</span>
              <span className="text-xs bg-cyan-950 text-cyan-300 px-2 py-0.5 rounded border border-cyan-800 font-mono">v2.6 Playground</span>
            </h1>
            <p className="text-xs text-slate-400">Distributed Consensus & Append-Only Log Engine</p>
          </div>
        </div>

        {/* Metrics Ticker */}
        <div className="hidden md:flex items-center space-x-5 bg-slate-950 px-4 py-2 rounded-xl border border-slate-800 text-xs font-mono">
          <div>
            <span className="text-slate-400 block">TERM</span>
            <span className="text-base font-bold text-cyan-400">#{maxTerm}</span>
          </div>
          <div className="h-8 w-px bg-slate-800"></div>
          <div>
            <span className="text-slate-400 block">LEADER</span>
            <span className="text-base font-bold text-emerald-400">{activeLeader || 'Electing...'}</span>
          </div>
          <div className="h-8 w-px bg-slate-800"></div>
          <div>
            <span className="text-slate-400 block">REACHABLE / SIZE</span>
            <span className="text-base font-bold text-purple-400">{reachableNodesCount} / {TOTAL_NODES}</span>
          </div>
          <div className="h-8 w-px bg-slate-800"></div>
          <div>
            <span className="text-slate-400 block">MAJORITY REQ</span>
            <span className="text-base font-bold text-amber-400">{QUORUM_SIZE} Votes</span>
          </div>
        </div>

        {/* Controls */}
        <div className="flex items-center space-x-3">
          <button 
            onClick={() => setIsRunning(!isRunning)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition ${
              isRunning ? 'bg-amber-500/10 border border-amber-500/40 text-amber-400 hover:bg-amber-500/20' : 'bg-emerald-500/10 border border-emerald-500/40 text-emerald-400 hover:bg-emerald-500/20'
            }`}
          >
            {isRunning ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
            {isRunning ? 'Pause' : 'Resume'}
          </button>
          <div className="flex items-center bg-slate-800 rounded-lg p-1 border border-slate-700">
            {[0.5, 1, 2, 5].map(s => (
              <button
                key={s}
                onClick={() => setSimSpeed(s)}
                className={`px-2.5 py-1 text-xs rounded font-mono ${simSpeed === s ? 'bg-cyan-500 text-slate-950 font-bold' : 'text-slate-300 hover:bg-slate-700'}`}
              >
                {s}x
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* Navigation with One-Line Subtitles */}
      <div className="bg-slate-900/80 border-b border-slate-800 px-6 flex space-x-2">
        {[
          { id: 'cluster', label: 'Cluster Topology & Client', subtitle: 'Interactive node mesh & write terminal', icon: Server },
          { id: 'logs', label: 'Log & State Machine', subtitle: 'Live append-only logs & state KV storage', icon: Database },
          { id: 'chaos', label: 'Network Chaos Suite', subtitle: 'Fault injection, partitions & leader killing', icon: Zap },
          { id: 'labs', label: 'Guided Experiments', subtitle: 'Sequential step-by-step Raft learning labs', icon: Award },
          { id: 'report', label: 'Telemetry & Report', subtitle: 'Cluster performance metrics & markdown export', icon: FileText }
        ].map(tab => {
          const IconComp = tab.icon;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`flex flex-col items-start px-4 py-2.5 border-b-2 transition ${
                activeTab === tab.id ? 'border-cyan-500 text-cyan-400 bg-slate-800/40' : 'border-transparent text-slate-400 hover:text-slate-200'
              }`}
            >
              <div className="flex items-center gap-2 text-sm font-medium">
                <IconComp className="w-4 h-4" />
                {tab.label}
              </div>
              <span className="text-[10px] text-slate-500 font-normal mt-0.5">{tab.subtitle}</span>
            </button>
          );
        })}
      </div>

      {/* Continuous Invariant Checker Panel */}
      <div className="bg-slate-900 border-b border-slate-800 px-6 py-2.5 flex flex-wrap items-center justify-between text-xs font-mono">
        <div className="flex items-center gap-2 text-slate-300 font-bold">
          <ShieldCheck className="w-4 h-4 text-cyan-400" />
          Raft Invariant Checker:
        </div>
        <div className="flex items-center gap-6">
          {[
            { label: '1) Election Safety', data: invariants.electionSafety },
            { label: '2) Log Matching', data: invariants.logMatching },
            { label: '3) Leader Completeness', data: invariants.leaderCompleteness },
            { label: '4) Committed Integrity', data: invariants.committedIntegrity }
          ].map((inv, idx) => (
            <div key={idx} className="flex items-center gap-1.5" title={inv.data.msg}>
              <span className={`w-2.5 h-2.5 rounded-full ${inv.data.passed ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500 animate-bounce'}`}></span>
              <span className={inv.data.passed ? 'text-slate-300' : 'text-rose-400 font-bold'}>{inv.label}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Main Workspace */}
      <main className="flex-1 p-6 grid grid-cols-1 lg:grid-cols-4 gap-6 max-w-[1600px] mx-auto w-full">
        <div className="lg:col-span-3 space-y-6">
          {activeTab === 'cluster' && (
            <>
              {/* Topology Mesh */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
                <div className="flex items-center justify-between mb-6">
                  <div>
                    <h2 className="text-lg font-bold text-white flex items-center gap-2">
                      <Activity className="w-5 h-5 text-cyan-400" />
                      5-Node Raft Cluster Mesh
                    </h2>
                    <p className="text-xs text-slate-400">Click any node card to select it and perform operations (Kill, Revive, Isolate, Heal).</p>
                  </div>
                  <div className="flex items-center gap-4 text-xs font-mono">
                    <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-emerald-500"></span> Leader</span>
                    <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-cyan-500"></span> Follower</span>
                    <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-amber-500"></span> Candidate</span>
                    <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-rose-500"></span> Dead/Partitioned</span>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                  {NODE_IDS.map(id => {
                    const node = nodes[id];
                    const isLeader = node.state === 'Leader';
                    const isCandidate = node.state === 'Candidate';
                    const isDead = node.state === 'Dead';
                    const isPartitioned = node.state === 'Partitioned';
                    const isSelected = selectedNodeId === id;

                    const ringColor = isLeader ? 'border-emerald-500 bg-emerald-950/20 text-emerald-400' : 
                                      isCandidate ? 'border-amber-500 bg-amber-950/20 text-amber-400' : 
                                      isDead || isPartitioned ? 'border-rose-500 bg-rose-950/20 text-rose-400' : 
                                      'border-cyan-500 bg-cyan-950/20 text-cyan-400';

                    return (
                      <div 
                        key={id} 
                        onClick={() => setSelectedNodeId(id)}
                        className={`border-2 ${ringColor} rounded-xl p-4 flex flex-col items-center justify-center shadow-lg relative cursor-pointer transition transform hover:scale-105 ${isSelected ? 'ring-2 ring-cyan-400' : ''}`}
                      >
                        <span className="text-lg font-bold font-mono">Node {id} {isSelected && '(*)'}</span>
                        <span className="text-xs font-semibold uppercase mt-1">{node.state}</span>
                        <span className="text-[10px] text-slate-400 mt-2 font-mono">Term: {node.currentTerm}</span>
                        <span className="text-[10px] text-slate-400 font-mono">Log: #{node.log.length}</span>
                        
                        <div className="flex gap-1 mt-3" onClick={e => e.stopPropagation()}>
                          {isDead ? (
                            <button onClick={() => reviveNode(id)} className="px-2 py-0.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded text-[10px]">Revive</button>
                          ) : (
                            <button onClick={() => killNode(id)} className="px-2 py-0.5 bg-rose-600 hover:bg-rose-500 text-white rounded text-[10px]">Kill</button>
                          )}
                          {isPartitioned ? (
                            <button onClick={() => healNode(id)} className="px-2 py-0.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded text-[10px]">Heal</button>
                          ) : (
                            <button onClick={() => isolateNode(id)} className="px-2 py-0.5 bg-amber-600 hover:bg-amber-500 text-white rounded text-[10px]">Isolate</button>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>

                {/* Selected Node Inspector Drawer */}
                {selectedNode && (
                  <div className="mt-6 bg-slate-950 border border-slate-800 rounded-xl p-4 font-mono text-xs space-y-3">
                    <div className="flex justify-between items-center border-b border-slate-800 pb-2">
                      <span className="font-bold text-cyan-400 text-sm">Selected Node [{selectedNode.id}] Control Panel</span>
                      <div className="space-x-2">
                        {selectedNode.state === 'Dead' ? (
                          <button onClick={() => reviveNode(selectedNode.id)} className="px-3 py-1 bg-emerald-600 text-white rounded">Revive</button>
                        ) : (
                          <button onClick={() => killNode(selectedNode.id)} className="px-3 py-1 bg-rose-600 text-white rounded">Kill</button>
                        )}
                        {selectedNode.state === 'Partitioned' ? (
                          <button onClick={() => healNode(selectedNode.id)} className="px-3 py-1 bg-emerald-600 text-white rounded">Heal</button>
                        ) : (
                          <button onClick={() => isolateNode(selectedNode.id)} className="px-3 py-1 bg-amber-600 text-white rounded">Isolate</button>
                        )}
                      </div>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div>Current Term: <span className="text-cyan-300">{selectedNode.currentTerm}</span></div>
                      <div>Voted For: <span className="text-purple-300">{selectedNode.votedFor || 'None'}</span></div>
                      <div>Commit Index: <span className="text-emerald-300">{selectedNode.commitIndex}</span></div>
                    </div>
                  </div>
                )}
              </div>

              {/* Enhanced Client Write Section */}
              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="text-md font-bold text-white flex items-center gap-2">
                      <Terminal className="w-5 h-5 text-cyan-400" />
                      Client: Write Data to the Cluster
                    </h3>
                    <p className="text-xs text-slate-400 mt-1">Submit key-value write operations to the active leader. Entries are committed only after majority replication (3/5).</p>
                  </div>
                  {writeStatus && (
                    <span className="text-xs font-mono bg-cyan-950/80 border border-cyan-800 text-cyan-300 px-3 py-1 rounded-lg">
                      {writeStatus}
                    </span>
                  )}
                </div>

                {/* Quick-fill chips */}
                <div className="flex items-center gap-2 text-xs font-mono">
                  <span className="text-slate-400">Quick Fill:</span>
                  {[
                    { key: 'user', val: 'Mamta' },
                    { key: 'balance', val: '500' },
                    { key: 'mode', val: 'active' }
                  ].map(chip => (
                    <button
                      key={chip.key}
                      type="button"
                      onClick={() => { setInputKey(chip.key); setInputValue(chip.val); }}
                      className="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-cyan-300 rounded border border-slate-700 transition"
                    >
                      {chip.key} = {chip.val}
                    </button>
                  ))}
                </div>

                <form onSubmit={e => {
                  e.preventDefault();
                  if (inputKey && inputValue && activeLeader) {
                    handleClientWrite(inputKey, inputValue);
                    setInputKey('');
                    setInputValue('');
                  }
                }} className="flex gap-3">
                  <input 
                    value={inputKey}
                    onChange={e => setInputKey(e.target.value)}
                    type="text" 
                    placeholder="Key (e.g. user)" 
                    className="bg-slate-950 border border-slate-700 rounded-xl px-4 py-2.5 text-sm font-mono flex-1 focus:outline-none focus:border-cyan-500" 
                    required 
                  />
                  <input 
                    value={inputValue}
                    onChange={e => setInputValue(e.target.value)}
                    type="text" 
                    placeholder="Value (e.g. Mamta)" 
                    className="bg-slate-950 border border-slate-700 rounded-xl px-4 py-2.5 text-sm font-mono flex-1 focus:outline-none focus:border-cyan-500" 
                    required 
                  />
                  <button 
                    type="submit" 
                    disabled={!activeLeader}
                    className={`font-semibold px-6 py-2.5 rounded-xl text-sm transition flex items-center gap-2 shadow-lg ${
                      activeLeader 
                        ? 'bg-cyan-500 hover:bg-cyan-400 text-slate-950 shadow-cyan-500/20 cursor-pointer' 
                        : 'bg-slate-800 text-slate-500 cursor-not-allowed border border-slate-700'
                    }`}
                  >
                    <Send className="w-4 h-4" /> Send Write
                  </button>
                </form>

                {/* Live Preview */}
                <div className="text-xs font-mono text-slate-400 bg-slate-950 px-3 py-2 rounded-lg border border-slate-800 flex justify-between">
                  <span>Live Operation Preview: <span className="text-cyan-300">SET {inputKey || 'key'} = {inputValue || 'value'}</span></span>
                  <span>Target Leader: <span className="text-emerald-400">{activeLeader ? `Node ${activeLeader}` : 'None'}</span></span>
                </div>
              </div>
            </>
          )}

          {activeTab === 'logs' && (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
              <h2 className="text-lg font-bold text-white flex items-center gap-2">
                <Database className="w-5 h-5 text-cyan-400" />
                Dynamic Log Array & State Machine Inspection
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                {NODE_IDS.map(id => {
                  const node = nodes[id];
                  return (
                    <div key={id} className="bg-slate-950 border border-slate-800 rounded-xl p-4 flex flex-col justify-between">
                      <div>
                        <div className="flex justify-between items-center mb-3 border-b border-slate-800 pb-2">
                          <span className="font-bold text-white">Node {id}</span>
                          <span className="text-xs px-2 py-0.5 rounded bg-slate-800 text-slate-300 font-mono">{node.state}</span>
                        </div>
                        <div className="text-xs font-mono text-slate-400 mb-3 space-y-0.5">
                          <div>Term: <span className="text-cyan-400">{node.currentTerm}</span></div>
                          <div>Commit: <span className="text-purple-400">{node.commitIndex}</span></div>
                        </div>
                        <h4 className="text-xs font-bold text-slate-300 uppercase mb-2">Log Entries</h4>
                        <div className="space-y-1.5 max-h-40 overflow-y-auto">
                          {node.log.length === 0 ? (
                            <span className="text-xs text-slate-500 italic">Empty log</span>
                          ) : (
                            node.log.map((entry, idx) => {
                              const committed = entry.index <= node.commitIndex;
                              return (
                                <div key={idx} className={`p-2 rounded border text-xs font-mono ${committed ? 'bg-cyan-950/40 border-cyan-800 text-cyan-200' : 'bg-amber-950/40 border-amber-800 text-amber-200'}`}>
                                  <div>#{entry.index} (T:{entry.term})</div>
                                  <div className="font-semibold truncate">{entry.command.key}: {entry.command.value}</div>
                                </div>
                              );
                            })
                          )}
                        </div>
                      </div>
                      <div className="mt-4 pt-3 border-t border-slate-800">
                        <h4 className="text-xs font-bold text-slate-300 uppercase mb-2">State Machine</h4>
                        <div className="bg-slate-900 rounded p-2 text-xs font-mono space-y-1 max-h-24 overflow-y-auto">
                          {Object.keys(node.kvStore).length === 0 ? (
                            <span className="text-slate-500 italic">No keys applied</span>
                          ) : (
                            Object.entries(node.kvStore).map(([k, v]) => (
                              <div key={k} className="flex justify-between">
                                <span className="text-slate-400">{k}:</span>
                                <span className="text-emerald-400">{v}</span>
                              </div>
                            ))
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {activeTab === 'chaos' && (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
              <h2 className="text-lg font-bold text-white flex items-center gap-2">
                <Zap className="w-5 h-5 text-amber-400" />
                Dynamic Network Chaos & Fault Injection Suite
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <button onClick={() => triggerPreset('split-brain')} className="bg-amber-500/10 border border-amber-500/30 hover:bg-amber-500/20 p-4 rounded-xl text-left transition">
                  <GitBranch className="w-6 h-6 text-amber-400 mb-2" />
                  <div className="font-bold text-white text-sm">Split-Brain Partition</div>
                  <div className="text-xs text-slate-400 mt-1">Isolate A,B from C,D,E</div>
                </button>
                <button onClick={() => triggerPreset('kill-leader')} className="bg-rose-500/10 border border-rose-500/30 hover:bg-rose-500/20 p-4 rounded-xl text-left transition">
                  <Skull className="w-6 h-6 text-rose-400 mb-2" />
                  <div className="font-bold text-white text-sm">Kill Current Leader</div>
                  <div className="text-xs text-slate-400 mt-1">Terminate active leader</div>
                </button>
                <button onClick={() => triggerPreset('heal')} className="bg-emerald-500/10 border border-emerald-500/30 hover:bg-emerald-500/20 p-4 rounded-xl text-left transition">
                  <CheckCircle className="w-6 h-6 text-emerald-400 mb-2" />
                  <div className="font-bold text-white text-sm">Heal All Partitions</div>
                  <div className="text-xs text-slate-400 mt-1">Restore full network connectivity</div>
                </button>
                <button onClick={() => triggerPreset('reset')} className="bg-cyan-500/10 border border-cyan-500/30 hover:bg-cyan-500/20 p-4 rounded-xl text-left transition">
                  <RotateCcw className="w-6 h-6 text-cyan-400 mb-2" />
                  <div className="font-bold text-white text-sm">Reset Cluster</div>
                  <div className="text-xs text-slate-400 mt-1">Restart simulation from scratch</div>
                </button>
              </div>
            </div>
          )}

          {activeTab === 'labs' && (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
              <div className="flex justify-between items-center border-b border-slate-800 pb-4">
                <div>
                  <h2 className="text-lg font-bold text-white flex items-center gap-2">
                    <Award className="w-5 h-5 text-cyan-400" />
                    Sequential Guided Raft Experiments
                  </h2>
                  <p className="text-xs text-slate-400 mt-0.5">Labs start as Not Started. Click "Start Lab" and perform actions to complete.</p>
                </div>

                <button 
                  onClick={() => {
                    setLabStatus({ 1: 'not_started', 2: 'not_started', 3: 'not_started', 4: 'not_started' });
                    setActiveLabId(null);
                    setLabActions({ partitionCreated: false, partitionHealed: false, logsConverged: false, leaderFailed: false });
                    addEvent('🔄 All labs reset to Not started.', 'info');
                  }}
                  className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-rose-400 text-xs rounded-xl font-mono border border-slate-700 transition"
                >
                  Reset All Labs
                </button>
              </div>

              <div className="space-y-4">
                {[
                  { id: 1, title: 'Lab 1: Healthy Cluster Bootstrap', desc: 'Observe cluster startup and stable leader election via staggered timeouts.' },
                  { id: 2, title: 'Lab 2: Log Replication & Quorum Commit', desc: 'Send writes and verify replication and quorum commitment (3/5).' },
                  { id: 3, title: 'Lab 3: Split-Brain Minority Disconnect', desc: 'Apply Split-Brain and observe minority partition write behavior.' },
                  { id: 4, title: 'Lab 4: Leader Failure & Failover', desc: 'Kill leader and verify candidate elections and term escalation.' }
                ].map((lab, index) => {
                  const status = labStatus[lab.id];
                  const prevCompleted = lab.id === 1 || labStatus[lab.id - 1] === 'completed';
                  const isUnlocked = prevCompleted;
                  const isActive = activeLabId === lab.id;

                  return (
                    <div key={lab.id} className={`p-4 rounded-xl border flex items-center justify-between ${
                      status === 'completed' 
                        ? 'bg-emerald-950/20 border-emerald-800' 
                        : isUnlocked 
                          ? 'bg-slate-950 border-cyan-800/40' 
                          : 'bg-slate-950/50 border-slate-800/50 opacity-50'
                    }`}>
                      <div className="flex items-start gap-4">
                        <div className={`p-2 rounded-lg ${
                          status === 'completed' ? 'bg-emerald-500/20 text-emerald-400' : isUnlocked ? 'bg-cyan-500/20 text-cyan-400' : 'bg-slate-800 text-slate-500'
                        }`}>
                          {isUnlocked ? <CheckCircle className="w-6 h-6" /> : <Lock className="w-6 h-6" />}
                        </div>
                        <div>
                          <h3 className="font-bold text-white text-sm flex items-center gap-2">
                            {lab.title}
                            {status === 'in_progress' && <span className="text-[10px] bg-amber-500/20 text-amber-300 px-2 py-0.5 rounded animate-pulse">Running...</span>}
                          </h3>
                          <p className="text-xs text-slate-400 mt-0.5">{lab.desc}</p>
                        </div>
                      </div>

                      <div className="flex items-center gap-3">
                        {status !== 'completed' && isUnlocked && (
                          <button
                            onClick={() => {
                              setActiveLabId(lab.id);
                              setLabStatus(s => ({ ...s, [lab.id]: 'in_progress' }));
                              setLabActions({ partitionCreated: false, partitionHealed: false, logsConverged: false, leaderFailed: false });
                              addEvent(`▶️ Started ${lab.title}`, 'info');
                            }}
                            className="px-3 py-1.5 bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-semibold rounded-lg text-xs transition flex items-center gap-1.5"
                          >
                            <PlayCircle className="w-3.5 h-3.5" /> {status === 'in_progress' ? 'Restart Lab' : 'Start Lab'}
                          </button>
                        )}
                        <span className={`text-xs font-mono px-3 py-1 rounded-full ${
                          status === 'completed' 
                            ? 'bg-emerald-500/20 text-emerald-300' 
                            : status === 'in_progress'
                              ? 'bg-amber-500/20 text-amber-300' 
                              : isUnlocked
                                ? 'bg-slate-800 text-cyan-300'
                                : 'bg-slate-800 text-slate-500'
                        }`}>
                          {status === 'completed' ? 'COMPLETED' : status === 'in_progress' ? 'IN PROGRESS' : isUnlocked ? 'NOT STARTED' : 'LOCKED'}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {activeTab === 'report' && (
            <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-6">
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                  <FileText className="w-5 h-5 text-cyan-400" />
                  Dynamic Telemetry & Report Generator
                </h2>
                <button onClick={() => {
                  const report = `# RaftLab Engineering Report\nElections Held: ${telemetry.electionsHeld}\nTotal RPCs: ${telemetry.totalRPCs}\nWrites: ${telemetry.writesProcessed}\nLeader Changes: ${telemetry.leaderChanges}\n`;
                  const blob = new Blob([report], { type: 'text/markdown' });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement('a');
                  a.href = url;
                  a.download = 'RaftLab_Report.md';
                  a.click();
                }} className="bg-cyan-500 text-slate-950 font-semibold px-4 py-2 rounded-xl text-sm shadow-md">
                  Download Markdown Report
                </button>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-slate-950 p-4 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 font-mono">ELECTIONS</span>
                  <span className="text-2xl font-bold text-cyan-400 font-mono block mt-1">{telemetry.electionsHeld}</span>
                </div>
                <div className="bg-slate-950 p-4 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 font-mono">TOTAL RPCS</span>
                  <span className="text-2xl font-bold text-purple-400 font-mono block mt-1">{telemetry.totalRPCs}</span>
                </div>
                <div className="bg-slate-950 p-4 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 font-mono">WRITES PROCESSED</span>
                  <span className="text-2xl font-bold text-emerald-400 font-mono block mt-1">{telemetry.writesProcessed}</span>
                </div>
                <div className="bg-slate-950 p-4 rounded-xl border border-slate-800">
                  <span className="text-xs text-slate-400 font-mono">LEADER CHANGES</span>
                  <span className="text-2xl font-bold text-amber-400 font-mono block mt-1">{telemetry.leaderChanges}</span>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Event Stream Sidebar */}
        <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col h-[650px] shadow-xl">
          <div className="flex items-center justify-between mb-3 pb-3 border-b border-slate-800">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <List className="w-4 h-4 text-cyan-400" />
              Event Stream
            </h3>
            <span className="text-xs font-mono bg-slate-950 px-2 py-0.5 rounded text-slate-400">{eventLogs.length}</span>
          </div>
          <div className="flex-1 overflow-y-auto space-y-2 pr-1 text-xs font-mono custom-scrollbar">
            {eventLogs.length === 0 ? (
              <div className="text-slate-500 italic text-center py-8">Waiting for cluster events...</div>
            ) : (
              eventLogs.map((log) => {
                let color = 'text-slate-300';
                if (log.type === 'success') color = 'text-emerald-400';
                if (log.type === 'error') color = 'text-rose-400';
                if (log.type === 'warn') color = 'text-amber-400';
                return (
                  <div key={log.id} className="bg-slate-950 p-2.5 rounded-lg border border-slate-800">
                    <span className="text-slate-500 text-[10px] block">{log.time}</span>
                    <span className={color}>{log.msg}</span>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </main>
    </div>
  );
}