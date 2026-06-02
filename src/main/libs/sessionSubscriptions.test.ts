import { expect, test } from 'vitest';

import { SessionSubscriptionRegistry, type SubscriptionWindow } from './sessionSubscriptions';

function createWindow(id: number): SubscriptionWindow & { destroyed: boolean } {
  return {
    id,
    destroyed: false,
    isDestroyed() {
      return this.destroyed;
    },
  };
}

test('subscribes, unsubscribes, and removes destroyed windows', () => {
  const registry = new SessionSubscriptionRegistry();
  const firstWindow = createWindow(1);
  const secondWindow = createWindow(2);

  registry.subscribe('session-1', firstWindow);
  registry.subscribe('session-1', secondWindow);
  registry.subscribe('session-2', secondWindow);

  expect(registry.getSubscribedWindows('session-1').map(window => window.id)).toEqual([1, 2]);
  registry.unsubscribe('session-1', 1);
  expect(registry.getSubscribedWindows('session-1').map(window => window.id)).toEqual([2]);

  secondWindow.destroyed = true;
  expect(registry.getSubscribedWindows('session-1')).toEqual([]);
  expect(registry.getSnapshot()).toEqual([]);
});
