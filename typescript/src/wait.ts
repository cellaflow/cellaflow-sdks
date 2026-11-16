/**
 * Waiting for another worker's lease, in the engine rather than by polling.
 *
 * A caller that asks with a positive `waitTimeoutMs` is held by the engine
 * until the holder commits (answered `HIT`), the lease is released or lapses
 * (the engine tries to grant it again before answering), or the wait runs out
 * (answered `IN_PROGRESS`). Every caller waiting on one key shares a single
 * watcher in the engine, so a crowd of waiters costs one storage read per tick,
 * not one per caller per second.
 */

/**
 * How long one request asks the engine to wait. The caller's total wait is the
 * loop around it, so this only bounds how long a single gRPC call stays open:
 * a dead connection is noticed within it, and no proxy idle limit is reached.
 */
export const ENGINE_WAIT_MS = 10_000;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Called on an `IN_PROGRESS` answer to a request that asked to wait, sent at
 * `sentAt` on the `performance.now()` clock. An engine that waited answers at
 * the end of its wait, so the caller can ask again at once. One that answered
 * well before the wait it was asked for did not wait (an engine that ignores
 * the field), and the caller backs off by its `retryAfterMs`, so an old engine
 * cannot turn the loop into a busy one.
 */
export async function afterInProgress(sentAt: number, retryAfterMs: bigint | undefined): Promise<void> {
  if (performance.now() - sentAt < ENGINE_WAIT_MS / 2) {
    await sleep(Number(retryAfterMs ?? 1000n));
  }
}
