import { getPayloadByteSize } from './performanceMetrics';

export type MessageUpdateMode = 'snapshot' | 'delta' | 'final';

export interface MessageUpdatePayload {
  sessionId: string;
  messageId: string;
  content: string;
  mode: MessageUpdateMode;
  offset: number;
  sequence: number;
  finalLength?: number;
}

type PendingUpdate = {
  sessionId: string;
  messageId: string;
  content: string;
  timer?: ReturnType<typeof setTimeout>;
};

export interface StreamUpdateCoalescerOptions {
  flushIntervalMs?: number;
  maxPayloadBytes?: number;
  send: (payload: MessageUpdatePayload) => void;
}

const DEFAULT_FLUSH_INTERVAL_MS = 500;
const DEFAULT_MAX_PAYLOAD_BYTES = 32 * 1024;

export class StreamUpdateCoalescer {
  private readonly pending = new Map<string, PendingUpdate>();
  private readonly flushIntervalMs: number;
  private readonly maxPayloadBytes: number;
  private readonly send: (payload: MessageUpdatePayload) => void;
  private readonly lastSentContent = new Map<string, string>();
  private readonly nextSequences = new Map<string, number>();

  constructor(options: StreamUpdateCoalescerOptions) {
    this.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
    this.send = options.send;
  }

  append(sessionId: string, messageId: string, content: string): void {
    const key = this.getKey(sessionId, messageId);
    const pending = this.pending.get(key) ?? {
      sessionId,
      messageId,
      content: '',
    };
    pending.content = content;
    this.pending.set(key, pending);
    if (!pending.timer) {
      pending.timer = setTimeout(() => {
        this.flushKey(key, 'snapshot');
      }, this.flushIntervalMs);
    }
  }

  flushSession(sessionId: string, mode: MessageUpdateMode = 'snapshot'): void {
    for (const key of Array.from(this.pending.keys())) {
      if (key.startsWith(`${sessionId}:`)) {
        this.flushKey(key, mode);
      }
    }
  }

  flushAll(mode: MessageUpdateMode = 'snapshot'): void {
    for (const key of Array.from(this.pending.keys())) {
      this.flushKey(key, mode);
    }
  }

  hasPending(sessionId: string, messageId: string): boolean {
    return this.pending.has(this.getKey(sessionId, messageId));
  }

  private flushKey(key: string, mode: MessageUpdateMode): void {
    const pending = this.pending.get(key);
    if (!pending) return;
    if (pending.timer) {
      clearTimeout(pending.timer);
    }
    this.pending.delete(key);

    const { content, mode: payloadMode, offset } = this.resolveOutgoingContent(key, pending.content, mode);
    const chunks = this.splitContent(pending.sessionId, pending.messageId, content, payloadMode, offset, this.nextSequences.get(key) ?? 0);
    for (const chunk of chunks) {
      this.send(chunk);
    }
    this.lastSentContent.set(key, pending.content);
    this.nextSequences.set(key, (chunks.at(-1)?.sequence ?? -1) + 1);
  }

  private resolveOutgoingContent(
    key: string,
    content: string,
    requestedMode: MessageUpdateMode,
  ): { content: string; mode: MessageUpdateMode; offset: number } {
    const previousContent = this.lastSentContent.get(key);
    if (previousContent && content.startsWith(previousContent)) {
      const delta = content.slice(previousContent.length);
      if (delta.length > 0) {
        return {
          content: delta,
          mode: requestedMode === 'final' ? 'final' : 'delta',
          offset: previousContent.length,
        };
      }
    }

    return {
      content,
      mode: requestedMode,
      offset: 0,
    };
  }

  private splitContent(
    sessionId: string,
    messageId: string,
    content: string,
    mode: MessageUpdateMode,
    startingOffset: number,
    startingSequence: number,
  ): MessageUpdatePayload[] {
    const chunks: MessageUpdatePayload[] = [];
    let offset = 0;
    let sequence = startingSequence;

    while (offset < content.length || chunks.length === 0) {
      let nextContent = content.slice(offset);
      let payload = this.createPayload(sessionId, messageId, nextContent, mode, startingOffset + offset, sequence, startingOffset + content.length);

      while (nextContent.length > 1 && getPayloadByteSize(payload) > this.maxPayloadBytes) {
        nextContent = nextContent.slice(0, Math.max(1, Math.floor(nextContent.length / 2)));
        payload = this.createPayload(sessionId, messageId, nextContent, mode, startingOffset + offset, sequence, startingOffset + content.length);
      }

      chunks.push(payload);
      offset += nextContent.length;
      sequence += 1;
      if (nextContent.length === 0) break;
    }

    return chunks;
  }

  private createPayload(
    sessionId: string,
    messageId: string,
    content: string,
    mode: MessageUpdateMode,
    offset: number,
    sequence: number,
    finalLength: number,
  ): MessageUpdatePayload {
    return {
      sessionId,
      messageId,
      content,
      mode,
      offset,
      sequence,
      finalLength: mode === 'final' ? finalLength : undefined,
    };
  }

  private getKey(sessionId: string, messageId: string): string {
    return `${sessionId}:${messageId}`;
  }
}
