import { expect, test, vi } from 'vitest';

import {
  getPerformanceSnapshot,
  recordDbOperation,
  recordIpcSend,
  resetPerformanceMetricsForTesting,
  setDbSlowThresholdForTesting,
} from './performanceMetrics';

test('aggregates IPC payload metrics and session event rates', () => {
  resetPerformanceMetricsForTesting();

  recordIpcSend({
    type: 'messageUpdate',
    sessionId: 'session-1',
    channel: 'cowork:stream:messageUpdate',
    payload: { sessionId: 'session-1', content: 'hello' },
    windowCount: 1,
  });
  recordIpcSend({
    type: 'message',
    sessionId: 'session-1',
    channel: 'cowork:stream:message',
    payload: { sessionId: 'session-1', message: { id: 'm1', content: 'reply' } },
    windowCount: 2,
  });

  const snapshot = getPerformanceSnapshot();
  expect(snapshot.ipc.totalEvents).toBe(2);
  expect(snapshot.ipc.totalPayloadBytes).toBeGreaterThan(0);
  expect(snapshot.ipc.maxMessageUpdatePayloadBytes).toBeGreaterThan(0);
  expect(snapshot.ipc.byType.messageUpdate.count).toBe(1);
  expect(snapshot.ipc.byType.message.windowCount).toBe(2);
  expect(snapshot.ipc.sessions[0]).toMatchObject({
    sessionId: 'session-1',
    eventCount: 2,
    maxEventsPerSecond: 2,
  });
});

test('keeps a bounded slow DB operation ring buffer', () => {
  resetPerformanceMetricsForTesting();
  setDbSlowThresholdForTesting(1);
  const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

  for (let index = 0; index < 205; index += 1) {
    recordDbOperation({
      operation: 'getSession',
      sessionId: `session-${index}`,
      durationMs: 2,
      messageCount: index,
    });
  }

  const snapshot = getPerformanceSnapshot();
  expect(snapshot.db.totalOperations).toBe(205);
  expect(snapshot.db.slowOperations).toHaveLength(200);
  expect(snapshot.db.slowOperations[0].sessionId).toBe('session-5');
  expect(snapshot.db.byOperation.getSession.slowCount).toBe(205);
  expect(warnSpy).toHaveBeenCalled();

  warnSpy.mockRestore();
});
