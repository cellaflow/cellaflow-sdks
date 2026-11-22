/**
 * Waiting for another worker's lease, in the engine rather than by polling.
 *
 * A caller that asks with a positive `waitTimeoutMs` is held by the engine
 * until the holder commits (answered `HIT`), the lease is released or lapses
 * (the engine tries to grant it again before answering), or the wait runs out
 * (answered `IN_PROGRESS`). The engine checks the holder every 100 ms on behalf
 * of every caller waiting on the key, rather than each caller asking once a
 * second.
 */

/**
 * How long one request asks the engine to wait. The caller's total wait is the
 * loop around it, so this only bounds how long a single request stays open.
 */
export const ENGINE_WAIT_MS = 10_000;

/**
 * The deadline on a request that asks to wait: the wait, plus time for the
 * engine to answer. A connection that dies while the engine holds the request
 * fails the call by then, rather than leaving it waiting for an answer that
 * never comes.
 */
export const ENGINE_WAIT_DEADLINE_SECS = (ENGINE_WAIT_MS + 5_000) / 1000;

/**
 * The deadline on a release. A release often follows renewals or requests that
 * failed, when the connection is most likely bad, and one that hangs would hold
 * up the call, and every call queued behind it in the session. A release that
 * fails is kept as the call's unreleased lease, so giving up costs nothing.
 */
export const RELEASE_DEADLINE_SECS = 5;

/**
 * How many times a request that got no answer is asked again before the call
 * fails, counted since the last answer. The engine may have granted the lost
 * answer's lease: asking again with the same key and position waits that lease
 * out and is granted the next, so a body still runs only under a lease this
 * side was told about.
 */
export const MAX_UNANSWERED = 2;

/**
 * Bounds on a back-off the engine suggests: a zero would make the loop busy,
 * and a value past what timers hold would fire at once.
 */
const MIN_RETRY_MS = 50;
const MAX_RETRY_MS = 60_000;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Called on an `IN_PROGRESS` answer to a request that asked the engine to wait
 * `waitedMs`, sent at `sentAt` on the `performance.now()` clock. An engine that
 * waited answers at the end of its wait, so the caller can ask again at once.
 * One that answered well before the wait it was asked for did not wait (an
 * engine that ignores the field), and the caller backs off by its
 * `retryAfterMs`, so an old engine cannot turn the loop into a busy one.
 */
export async function afterInProgress(
  sentAt: number,
  waitedMs: number,
  retryAfterMs: bigint | undefined,
): Promise<void> {
  if (performance.now() - sentAt < waitedMs / 2) {
    const suggested = Number(retryAfterMs ?? 1000n);
    await sleep(Math.min(MAX_RETRY_MS, Math.max(MIN_RETRY_MS, suggested)));
  }
}
