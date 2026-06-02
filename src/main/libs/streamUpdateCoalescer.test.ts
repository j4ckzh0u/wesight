import { expect, test, vi } from 'vitest';

import { getPayloadByteSize } from './performanceMetrics';
import { type MessageUpdatePayload,StreamUpdateCoalescer } from './streamUpdateCoalescer';

test('coalesces rapid updates into one snapshot flush', () => {
  vi.useFakeTimers();
  const sent: MessageUpdatePayload[] = [];
  const coalescer = new StreamUpdateCoalescer({
    flushIntervalMs: 500,
    send: payload => sent.push(payload),
  });

  coalescer.append('session-1', 'message-1', 'a');
  coalescer.append('session-1', 'message-1', 'abc');
  vi.advanceTimersByTime(499);
  expect(sent).toHaveLength(0);

  vi.advanceTimersByTime(1);
  expect(sent).toEqual([{
    sessionId: 'session-1',
    messageId: 'message-1',
    content: 'abc',
    mode: 'snapshot',
    offset: 0,
    sequence: 0,
    finalLength: undefined,
  }]);
  vi.useRealTimers();
});

test('flushes final updates immediately', () => {
  vi.useFakeTimers();
  const sent: MessageUpdatePayload[] = [];
  const coalescer = new StreamUpdateCoalescer({
    flushIntervalMs: 500,
    send: payload => sent.push(payload),
  });

  coalescer.append('session-1', 'message-1', 'final text');
  coalescer.flushSession('session-1', 'final');

  expect(sent[0]).toMatchObject({
    content: 'final text',
    mode: 'final',
    finalLength: 10,
  });
  expect(coalescer.hasPending('session-1', 'message-1')).toBe(false);
  vi.useRealTimers();
});

test('sends appended content as delta after an initial snapshot', () => {
  const sent: MessageUpdatePayload[] = [];
  const coalescer = new StreamUpdateCoalescer({
    flushIntervalMs: 500,
    send: payload => sent.push(payload),
  });

  coalescer.append('session-1', 'message-1', 'hello');
  coalescer.flushAll();
  coalescer.append('session-1', 'message-1', 'hello world');
  coalescer.flushAll();

  expect(sent[0]).toMatchObject({
    content: 'hello',
    mode: 'snapshot',
    offset: 0,
    sequence: 0,
  });
  expect(sent[1]).toMatchObject({
    content: ' world',
    mode: 'delta',
    offset: 5,
    sequence: 1,
  });
});

test('splits oversized payloads under the configured byte cap', () => {
  const sent: MessageUpdatePayload[] = [];
  const coalescer = new StreamUpdateCoalescer({
    flushIntervalMs: 500,
    maxPayloadBytes: 250,
    send: payload => sent.push(payload),
  });

  coalescer.append('session-1', 'message-1', 'x'.repeat(1000));
  coalescer.flushAll();

  expect(sent.length).toBeGreaterThan(1);
  expect(sent.every(payload => getPayloadByteSize(payload) <= 250)).toBe(true);
  expect(sent.map(payload => payload.content).join('')).toBe('x'.repeat(1000));
});
