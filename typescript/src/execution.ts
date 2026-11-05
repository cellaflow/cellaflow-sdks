import { AsyncLocalStorage } from "node:async_hooks";
import { CellaflowClient } from "./client.js";
import { CacheStatus } from "./cellaflow/v1/idempotency_pb.js";
import { LeaseHeartbeat } from "./lease.js";
import type { LeaseEvent } from "./lease.js";

const DEFAULT_TTL_MS = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;

/** Raised when the requested execution lease cannot be acquired. */
export class LeaseNotAcquired extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LeaseNotAcquired";
  }
}

/** Raised by `LeaseHandle.check()` once the lease has been lost. */
export class LeaseLostError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LeaseLostError";
  }
}

/**
 * Handle to an active execution lease.
 *
 * Node cannot interrupt a running task the way Python's asyncio can cancel one,
 * so losing a lease here is **cooperative**: the work keeps running until it
 * checks. Two ways to check, and long or irreversible work should use one:
 *
 * - `check()` throws {@link LeaseLostError} once the lease is gone.
 * - `signal` aborts, so it can be handed to `fetch` or any AbortSignal-aware API.
 */
export class LeaseHandle {
  readonly fencingToken: number;
  private readonly controller = new AbortController();
  private lostDetail?: string;
  private readonly probe?: () => void;

  /**
   * @param probe Called before every `isLost` and `check()`, so loss is judged
   *   on the clock at the moment of asking rather than on the last heartbeat.
   */
  constructor(fencingToken: number, probe?: () => void) {
    this.fencingToken = fencingToken;
    this.probe = probe;
  }

  /** Aborts when the lease is lost. Pass to `fetch`, streams, or your own loops. */
  get signal(): AbortSignal {
    return this.controller.signal;
  }

  get isLost(): boolean {
    if (!this.controller.signal.aborted) this.probe?.();
    return this.controller.signal.aborted;
  }

  /** @internal */
  markLost(detail: string): void {
    if (this.controller.signal.aborted) return;
    this.lostDetail = detail;
    this.controller.abort(new LeaseLostError(`Execution lease was lost: ${detail}`));
  }

  /**
   * Throws if the lease has been lost, or can no longer be assumed held.
   *
   * Call this before anything irreversible inside a long block. Nothing else
   * stops the work: losing a lease cannot preempt a running function in Node.
   * It judges by the clock, not only by the last heartbeat, so a process that
   * was stopped past its deadline fails here before any renewal answers.
   *
   * It cannot close the gap between the check and the write landing at the
   * other end: a write sent just before the deadline can still arrive after
   * another worker took over. Hand `signal` to the request to shorten it.
   */
  check(): void {
    if (this.isLost) {
      throw new LeaseLostError(`Execution lease was lost: ${this.lostDetail}`);
    }
  }
}

const currentLeaseStore = new AsyncLocalStorage<LeaseHandle>();

/**
 * Returns the lease held by the enclosing block.
 *
 * Lets code inside an {@link executionLease} block, or inside a
 * {@link taskLease} function which has no handle to receive, reach the fencing
 * token to pass downstream.
 */
export function currentLease(): LeaseHandle {
  const handle = currentLeaseStore.getStore();
  if (!handle) {
    throw new Error(
      "No execution lease is active. currentLease() is only valid inside an " +
        "executionLease(...) block or a taskLease(...) function.",
    );
  }
  return handle;
}

export interface ExecutionLeaseOptions {
  /** Identifies this worker. Sent as `agentId`. */
  workerId: string;
  target?: string;
  secure?: boolean;
  /**
   * How long the lease lasts without a renewal. Each renewal extends it by the
   * same amount. Defaults to 30 000.
   */
  ttlMs?: number;
  /**
   * Fallback renewal interval. The engine answers acquisition with its own
   * interval (a quarter of the TTL), and that one is used when present.
   */
  heartbeatIntervalMs?: number;
  /** Notified when the lease is lost. A notification, not a substitute for checking. */
  onLeaseLost?: (detail: string) => void;
  /**
   * Called on acquisition, every renewal outcome, loss and release, for
   * tracing. Times are wall-clock epoch milliseconds.
   */
  onEvent?: (event: LeaseEvent) => void;
  /**
   * How long to wait for a lease another worker is holding. Defaults to 0, which
   * fails immediately with {@link LeaseNotAcquired}.
   */
  waitMs?: number;
}

/**
 * Runs `fn` holding a distributed lock on `key`, renewed by a heartbeat.
 *
 * Only one worker runs the block at a time. If this process dies, the lease
 * stops being renewed and another worker takes it once the TTL elapses, which is
 * the property a plain database lock cannot offer for a holder that hangs
 * without dying.
 *
 * ```ts
 * await executionLease("task:abc-123", { workerId: "worker-1" }, async (lease) => {
 *   lease.check();
 *   await doTheWork({ signal: lease.signal });
 * });
 * ```
 *
 * Unlike the Python `async_execution_lease`, losing the lease does **not**
 * interrupt `fn`: Node has no task cancellation. The handle exposes `check()`
 * and `signal` so the work can abort itself, and anything irreversible should
 * check first.
 */
export async function executionLease<T>(
  key: string,
  options: ExecutionLeaseOptions,
  fn: (lease: LeaseHandle) => Promise<T>,
): Promise<T> {
  const {
    workerId,
    target = "localhost:50051",
    secure = false,
    ttlMs = DEFAULT_TTL_MS,
    heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS,
    onLeaseLost,
    onEvent,
    waitMs = 0,
  } = options;

  const client = new CellaflowClient({ target, secure });
  let hb: LeaseHeartbeat | undefined;
  let fencingToken = 0;

  const emit = (event: Omit<LeaseEvent, "at" | "fencingToken">) => {
    try {
      onEvent?.({ at: Date.now(), fencingToken, ...event });
    } catch {
      // A caller's tracer must not change what the lease does.
    }
  };

  try {
    const acquiredSentAt = performance.now();
    const resp = await client.checkIdempotencyCache(workerId, key, waitMs, ttlMs);

    if (resp.status === CacheStatus.HIT) {
      throw new LeaseNotAcquired(
        `'${key}' is already committed: the operation it names has completed. An ` +
          "execution lease locks work still to be done, so a committed key means " +
          "this work is finished, not that the lock is busy.",
      );
    }
    if (resp.status !== CacheStatus.ACQUIRED) {
      const holder = resp.currentHolderId ? ` (held by ${resp.currentHolderId})` : "";
      throw new LeaseNotAcquired(
        `Could not acquire execution lease '${key}'${holder}. Another worker is ` +
          "running it. Raise waitMs to wait for them, or treat this as the " +
          "signal that the work is already in hand.",
      );
    }

    fencingToken = Number(resp.fencingToken ?? 0n);
    const handle = new LeaseHandle(fencingToken, () => {
      hb?.pastDeadline();
    });
    emit({
      type: "acquired",
      expiresAtMs: resp.leaseExpiresAtMs !== undefined ? Number(resp.leaseExpiresAtMs) : undefined,
    });

    const engineInterval = Number(resp.heartbeatIntervalMs ?? 0n);
    hb = new LeaseHeartbeat({
      client,
      agentId: workerId,
      idempotencyKey: key,
      fencingToken,
      heartbeatIntervalMs: engineInterval > 0 ? engineInterval : heartbeatIntervalMs,
      // Renew for the full TTL. Asking for less made the engine hand the lease
      // on before this side noticed it was gone.
      extendMs: ttlMs,
      leaseTtlMs: ttlMs,
      acquiredSentAt,
      onEvent,
      onLeaseLost: (detail) => {
        handle.markLost(detail);
        try {
          onLeaseLost?.(detail);
        } catch {
          // A caller's callback must not mask the loss itself.
        }
      },
    });
    if (waitMs > 0) {
      // The engine may have waited before granting, so the lease can be much
      // younger than the request that won it, and a deadline counted from that
      // request would read a fresh lease as nearly expired. One renewal sent
      // now gives a deadline counted from a known time.
      await hb.renewNow();
    }
    hb.start();

    return await currentLeaseStore.run(handle, () => fn(handle));
  } finally {
    await hb?.stop();
    if (fencingToken > 0) {
      await client
        .releaseLease(workerId, key, fencingToken, "BLOCK_EXIT")
        .then(() => emit({ type: "released" }))
        .catch(() => {
          // The lease expires on its own; a failed release is not worth masking
          // whatever the block was doing.
        });
    }
    client.close();
  }
}

/**
 * Wraps a function so every call runs under an execution lease.
 *
 * The key is derived per call, so a task id argument becomes the lock:
 *
 * ```ts
 * const processOrder = taskLease(
 *   async (orderId: string) => { currentLease().check(); await ship(orderId); },
 *   { workerId: "worker-1", key: (orderId) => `order:${orderId}` },
 * );
 * ```
 */
export function taskLease<A extends unknown[], R>(
  fn: (...args: A) => Promise<R>,
  options: ExecutionLeaseOptions & { key: string | ((...args: A) => string) },
): (...args: A) => Promise<R> {
  const { key, ...leaseOptions } = options;
  return async function leasedTask(...args: A): Promise<R> {
    const resolved = typeof key === "function" ? key(...args) : key;
    return executionLease(resolved, leaseOptions, () => fn(...args));
  };
}
