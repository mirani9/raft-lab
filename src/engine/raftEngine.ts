import { NodeData } from '../types';

export const NODE_IDS = ['A', 'B', 'C', 'D', 'E'];
export const TOTAL_NODES = NODE_IDS.length;
export const QUORUM_SIZE = Math.floor(TOTAL_NODES / 2) + 1; // 3 for 5 nodes

export function createInitialNodes(): Record<string, NodeData> {
  const nodes: Record<string, NodeData> = {};
  const baseTimeouts: Record<string, number> = {
    'A': 2500,
    'B': 4500,
    'C': 6500,
    'D': 8500,
    'E': 10500
  };

  NODE_IDS.forEach(id => {
    nodes[id] = {
      id,
      state: 'Follower',
      currentTerm: 1,
      votedFor: null,
      log: [],
      commitIndex: 0,
      lastApplied: 0,
      kvStore: {},
      electionTimer: 0,
      electionTimeout: baseTimeouts[id],
      heartbeatTimer: 0,
      nextIndex: {},
      matchIndex: {},
      votesReceived: []
    };
  });
  return nodes;
}

export function areNodesConnected(n1: string, n2: string, states: Record<string, NodeData>, partitions: Record<string, boolean>): boolean {
  if (!states[n1] || !states[n2]) return false;
  if (states[n1].state === 'Dead' || states[n2].state === 'Dead') return false;
  if (states[n1].state === 'Partitioned' || states[n2].state === 'Partitioned') return false;
  if (partitions[`${n1}-${n2}`] || partitions[`${n2}-${n1}`]) return false;
  return true;
}

export function getReachableCount(states: Record<string, NodeData>): number {
  return NODE_IDS.filter(id => states[id] && states[id].state !== 'Dead' && states[id].state !== 'Partitioned').length;
}

export function isLogUpToDate(
  candidateLastLogTerm: number, 
  candidateLastLogIndex: number, 
  recipientLastLogTerm: number, 
  recipientLastLogIndex: number
): boolean {
  if (candidateLastLogTerm !== recipientLastLogTerm) {
    return candidateLastLogTerm > recipientLastLogTerm;
  }
  return candidateLastLogIndex >= recipientLastLogIndex;
}