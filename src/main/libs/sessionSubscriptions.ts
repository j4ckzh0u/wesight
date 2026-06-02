export interface SubscriptionWindow {
  id: number;
  isDestroyed(): boolean;
}

export class SessionSubscriptionRegistry {
  private readonly sessionSubscribers = new Map<string, Set<number>>();
  private readonly windows = new Map<number, SubscriptionWindow>();

  subscribe(sessionId: string, window: SubscriptionWindow): void {
    this.windows.set(window.id, window);
    const subscribers = this.sessionSubscribers.get(sessionId) ?? new Set<number>();
    subscribers.add(window.id);
    this.sessionSubscribers.set(sessionId, subscribers);
  }

  unsubscribe(sessionId: string, windowId: number): void {
    const subscribers = this.sessionSubscribers.get(sessionId);
    if (!subscribers) return;
    subscribers.delete(windowId);
    if (subscribers.size === 0) {
      this.sessionSubscribers.delete(sessionId);
    }
  }

  removeWindow(windowId: number): void {
    this.windows.delete(windowId);
    for (const [sessionId, subscribers] of this.sessionSubscribers.entries()) {
      subscribers.delete(windowId);
      if (subscribers.size === 0) {
        this.sessionSubscribers.delete(sessionId);
      }
    }
  }

  getSubscribedWindows(sessionId: string): SubscriptionWindow[] {
    const subscribers = this.sessionSubscribers.get(sessionId);
    if (!subscribers) return [];
    const windows: SubscriptionWindow[] = [];
    for (const windowId of subscribers) {
      const window = this.windows.get(windowId);
      if (!window || window.isDestroyed()) {
        this.removeWindow(windowId);
        continue;
      }
      windows.push(window);
    }
    return windows;
  }

  getSnapshot(): Array<{ sessionId: string; windowIds: number[] }> {
    return Array.from(this.sessionSubscribers.entries()).map(([sessionId, subscribers]) => ({
      sessionId,
      windowIds: Array.from(subscribers).sort((a, b) => a - b),
    }));
  }
}
