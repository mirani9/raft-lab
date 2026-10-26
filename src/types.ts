export type NodeState = 'Follower' | 'Candidate' | 'Leader' | 'Dead' | 'Partitioned';

export interface LogCommand {
  key: string;
  value: string;
}

export interface LogEntry {
  index: number;
  term: number;
  command: LogCommand;
}

export interface NodeData {
  id: string;
  state: NodeState;
  currentTerm: number;
  votedFor: string | null;
  log: LogEntry[];
  commitIndex: number;
  lastApplied: number;
  kvStore: Record<string, string>;
  electionTimer: number;
  electionTimeout: number;
  heartbeatTimer: number;
  nextIndex: Record<string, number>;
  matchIndex: Record<string, number>;
  votesReceived: string[];
}

export interface SimulationTelemetry {
  electionsHeld: number;
  totalRPCs: number;
  writesProcessed: number;
  conflictsResolved: number;
  leaderChanges: number;
}