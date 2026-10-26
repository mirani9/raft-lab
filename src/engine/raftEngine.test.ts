import { describe, it, expect } from 'vitest';
import { createInitialNodes, QUORUM_SIZE, getReachableCount, isLogUpToDate } from './raftEngine';

describe('Raft Consensus Engine Safety & Log Tests', () => {
  it('correctly evaluates log up-to-date safety rules', () => {
    // Candidate has higher term
    expect(isLogUpToDate(2, 5, 1, 10)).toBe(true);
    // Same term, candidate has equal or longer log
    expect(isLogUpToDate(1, 10, 1, 10)).toBe(true);
    expect(isLogUpToDate(1, 12, 1, 10)).toBe(true);
    // Candidate has lower term or shorter log at same term
    expect(isLogUpToDate(1, 4, 2, 2)).toBe(false);
    expect(isLogUpToDate(1, 5, 1, 10)).toBe(false);
  });
});