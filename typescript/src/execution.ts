import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { checkApiKeyOption } from "./apikey.js";
import { CellaflowClient } from "./client.js";
import { CacheStatus } from "./cellaflow/v1/idempotency_pb.js";
import { LeaseHeartbeat } from "./lease.js";
import type { LeaseEvent } from "./lease.js";
import { RELEASE_DEADLINE_SECS } from "./wait.js";

const DEFAULT_TTL_MS = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;

/**
 * Raised when a lease cannot be acquired: by `executionLease` when another
 * worker holds it, and by `tool()` when none of the leases the engine granted
 * could be confirmed.
 */
export class LeaseNotAcquired extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LeaseNotAcquired";
  }
}

/** Raised by `LeaseHandle.check()` once the lease has been lost. */
export class LeaseLostError extends Error {
  /**
   * What a `tool()` body returned before the engine refused to record it,
   * because another worker had taken the call over. Kept for logging, and not
   * enumerable, so a logger that serialises the error does not print it.
   * Undefined for a loss found any other way.
   */
  declare readonly result?: unknown;

  constructor(message: string, options?: { result?: unknown; cause?: unknown }) {
    super(message);
    this.name = "LeaseLostError";
    if (options && "result" in options) {
      Object.defineProperty(this, "result", { value: options.result, enumerable: false });
    }
    if (options && "cause" in options) {
      Object.defineProperty(this, "cause", {
        value: options.cause,
        enumerable: false,
        writable: true,
        configurable: true,
      });
    }
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
  /**
   * The idempotency key the lease is held under: the key a `tool()` call
   * derived (or was given), or the `executionLease` key.
   *
   * A tool call's key is the same on every attempt at the same call, as long
   * as its arguments are the same, so it can be forwarded to a destination
   * that deduplicates on it; see {@link withIdempotencyKey}. Under STEP_LOCAL
   * the key includes the call's position, which calling the tool again after a
   * failure, or a resumed `durableTools` thread, can change, so prefer
   * SESSION_WIDE, SHARED, or an explicit key for forwarding.
   *
   * An `executionLease` key names a lock, which is released when the block
   * ends and taken again by the next run: forward it only when it names
   * exactly one operation (a task id), or add a run id of your own.
   *
   * A derived key carries the ids it is made of (the session or thread id, or
   * the coordination id, the agent id and the tool name, by scope), so the
   * helpers send {@link keyDigest} unless asked for the key itself.
   */
  get key(): string {
    return this.#key;
  }
  readonly #key: string;
  private readonly controller = new AbortController();
  private lostDetail?: string;
  private released = false;
  private readonly probe?: () => void;

  /**
   * @param probe Called before every `isLost` and `check()`, so loss is judged
   *   on the clock at the moment of asking rather than on the last heartbeat.
   */
  constructor(fencingToken: number, probe?: () => void, key = "") {
    this.fencingToken = fencingToken;
    this.probe = probe;
    this.#key = key;
  }

  /**
   * SHA-256 of the UTF-8 {@link key}, as 64 lowercase hex characters: the same
   * on every attempt, like the key, ASCII and within the length destinations
   * allow (Stripe takes up to 255), and without the ids the key is made of in
   * readable form. It is not a secret: anyone who can guess the inputs of a
   * key can compute its digest.
   */
  get keyDigest(): string {
    return createHash("sha256").update(this.requireKey(), "utf8").digest("hex");
  }

  /**
   * A digest for one of several requests a body sends under this lease: the
   * SHA-256 of {@link keyDigest} followed by `part` (UTF-8), as 64 lowercase
   * hex characters, the same on every attempt, and different for each `part`.
   * `part` must be a non-empty string. A destination that deduplicates refuses (Stripe) or answers from
   * its first request (others) a key reused for a different request, so each
   * request needs its own:
   *
   * ```ts
   * await stripe.customers.create(customer, { idempotencyKey: lease.keyDigestFor("customer") });
   * await stripe.paymentIntents.create(payment, { idempotencyKey: lease.keyDigestFor("payment") });
   * ```
   */
  keyDigestFor(part: string): string {
    if (typeof part !== "string" || part === "" || LONE_SURROGATE.test(part)) {
      throw new TypeError(
        "keyDigestFor: part must be a non-empty string of whole characters, so " +
          "requests without one do not all share the call's key.",
      );
    }
    // The digest is fixed-length, so no key and part can run into another's.
    return createHash("sha256").update(this.keyDigest + part, "utf8").digest("hex");
  }

  /**
   * Returns `init` for `fetch` with an `Idempotency-Key` header and this
   * lease's `signal`, so the request is deduplicated by a destination that
   * honours the header and cancelled on this side if the lease is lost. The
   * header carries {@link keyDigest}, or the key itself with `{ raw: true }`.
   * Other headers in `init` are kept and an `Idempotency-Key` already there is
   * replaced. A `signal` already in `init` is kept instead of the lease's: to
   * have both, pass `signal: AbortSignal.any([yours, lease.signal])` (Node
   * 18.17 and later), with a signal of your own that ends with the request,
   * such as `AbortSignal.timeout(...)`: `AbortSignal.any` keeps memory for each
   * call on a long-lived one. `init` is not changed.
   *
   * ```ts
   * await fetch(url, currentLease().withIdempotencyKey({ method: "POST", body }));
   * ```
   *
   * @param options.part Which of several requests this is: the header carries
   *   {@link keyDigestFor}(part), so each request has its own key.
   * @param options.raw Send {@link key} rather than its digest. The key must
   *   then be printable ASCII with no space at either end, and can be longer
   *   than some destinations allow. Not with `part`.
   * @param options.header The header's name, for a destination that uses
   *   another one (PayPal: `PayPal-Request-Id`). Defaults to `Idempotency-Key`.
   */
  withIdempotencyKey<T extends RequestInit = RequestInit>(
    init?: T,
    options: { part?: string; raw?: boolean; header?: string } = {},
  ): Omit<T, "headers" | "signal"> & { headers: Headers; signal: AbortSignal } {
    if (isRequestLike(init)) {
      // Its fields are getters, which spreading does not copy: the request
      // would go out as a bare GET.
      throw new TypeError(
        "withIdempotencyKey takes a plain init object, not a Request. Pass " +
          "fetch(request, lease.withIdempotencyKey({ headers: request.headers })): " +
          "init's headers replace the request's, so they are passed on, and " +
          "the lease's signal replaces the request's own.",
      );
    }
    // `part: undefined` given explicitly is refused below, not taken for the
    // call's own key: requests without a part would all share it.
    const hasPart = "part" in options;
    if (options.raw && hasPart) {
      throw new TypeError("withIdempotencyKey: part sends a digest, so it cannot be combined with raw.");
    }
    const value = hasPart
      ? this.keyDigestFor(options.part as string)
      : options.raw
        ? this.rawHeaderValue()
        : this.keyDigest;
    const headers = new Headers(init?.headers ?? undefined);
    headers.set(options.header ?? "Idempotency-Key", value);
    // A signal of the caller's own is kept as it is: combining two signals
    // here would have to hold a listener on a signal that may live for the
    // whole process. Pass AbortSignal.any([yours, lease.signal]) to have both.
    const signal = init?.signal ?? this.signal;
    return { ...(init ?? ({} as T)), headers, signal };
  }

  /** The key as a header value, refusing one a header cannot carry as it is. */
  private rawHeaderValue(): string {
    const key = this.requireKey();
    if (!HEADER_VALUE.test(key)) {
      throw new Error(
        "This lease's key contains characters a header cannot carry as they " +
          "are. Send its digest instead (the default).",
      );
    }
    return key;
  }

  private requireKey(): string {
    if (!this.key) {
      throw new Error(
        "This lease has no idempotency key to send: a handle made without one " +
          "would make unrelated operations look like repeats.",
      );
    }
    return this.key;
  }

  /**
   * Aborts when the lease is lost. Pass to `fetch`, streams, or your own loops.
   * Reading it checks the clock, like `isLost`, so a process resumed past its
   * deadline gets a signal that has already aborted.
   *
   * It does not abort when the block exits and the lease is released: that is
   * not a loss, and a response or stream fetched with it may still be read.
   */
  get signal(): AbortSignal {
    if (!this.released && !this.controller.signal.aborted) this.probe?.();
    return this.controller.signal;
  }

  /** True once the lease is lost, or released because its block exited. */
  get isLost(): boolean {
    if (this.released) return true;
    if (!this.controller.signal.aborted) this.probe?.();
    return this.controller.signal.aborted;
  }

  /** @internal Called as the block exits, before the lease is let go. */
  markReleased(): void {
    this.released = true;
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
    if (!this.isLost) return;
    throw new LeaseLostError(
      this.controller.signal.aborted
        ? `Execution lease was lost: ${this.lostDetail}`
        : "Execution lease was released when its block exited",
    );
  }
}

/**
 * Whether `init` is a Request rather than a plain init object, including one
 * from another realm or from the `undici` package, which `instanceof` misses.
 */
function isRequestLike(init: unknown): boolean {
  if (typeof Request !== "undefined" && init instanceof Request) return true;
  const candidate = init as { url?: unknown; clone?: unknown } | undefined;
  return typeof candidate?.url === "string" && typeof candidate?.clone === "function";
}

/** An unpaired surrogate, which UTF-8 cannot encode as it is. */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/** A header value sent as it is: printable ASCII, with no space at either end. */
const HEADER_VALUE = /^[\x21-\x7e](?:[\x20-\x7e]*[\x21-\x7e])?$/;

const currentLeaseStore = new AsyncLocalStorage<LeaseHandle | undefined>();

/**
 * Returns the lease held by the enclosing block.
 *
 * Lets code inside an {@link executionLease} block, a {@link taskLease}
 * function, or the body of a `tool()` that holds its call's lease, reach the
 * lease to check it, abort with its `signal`, or pass its fencing token
 * downstream.
 */
export function currentLease(): LeaseHandle {
  const handle = currentLeaseStore.getStore();
  if (!handle) {
    throw new Error(
      "No execution lease is active. currentLease() is only valid inside an " +
        "executionLease(...) block, a taskLease(...) function, or the body of a " +
        "tool() call that holds its lease.",
    );
  }
  return handle;
}

/** @internal Runs `fn` with `handle` as the lease `currentLease()` returns. */
export function runUnderLease<T>(handle: LeaseHandle, fn: () => T): T {
  return currentLeaseStore.run(handle, fn);
}

/**
 * @internal Runs `fn` with no lease for `currentLease()` to return, not even an
 * enclosing one: for a tool body that runs without a lease of its own.
 */
export function runWithoutLease<T>(fn: () => T): T {
  return currentLeaseStore.run(undefined, fn);
}

export interface ExecutionLeaseOptions {
  /** Identifies this worker. Sent as `agentId`. */
  workerId: string;
  target?: string;
  secure?: boolean;
  /** Authenticates with the engine. See {@link CellaflowClientOptions.apiKey}. */
  apiKey?: string | null;
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

  const client = new CellaflowClient({ target, secure, apiKey: options.apiKey });
  let hb: LeaseHeartbeat | undefined;
  let handle: LeaseHandle | undefined;
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
    const acquiredSentWallAt = Date.now();
    // A waiting request gets a deadline past its wait, so a connection that
    // dies while the engine holds it fails the block rather than hanging it.
    const resp = await client.checkIdempotencyCache(
      workerId,
      key,
      waitMs,
      ttlMs,
      undefined,
      undefined,
      waitMs > 0 ? (waitMs + 5_000) / 1000 : undefined,
    );

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
    handle = new LeaseHandle(
      fencingToken,
      () => {
        hb?.pastDeadline();
      },
      key,
    );
    const leaseHandle = handle;
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
      // An engine too old to suggest one, or a TTL under 4 ms, gives 0: renew at
      // the caller's interval, but never less often than four times per TTL.
      heartbeatIntervalMs:
        engineInterval > 0 ? engineInterval : Math.min(heartbeatIntervalMs, Math.max(1, Math.floor(ttlMs / 4))),
      // Renew for the full TTL. Asking for less made the engine hand the lease
      // on before this side noticed it was gone.
      extendMs: ttlMs,
      leaseTtlMs: ttlMs,
      acquiredSentAt,
      acquiredSentWallAt,
      // Only when the engine answered at once: after a wait the grant can be
      // much younger than the request it is counted against.
      acquiredExpiresAtMs:
        waitMs === 0 && resp.leaseExpiresAtMs !== undefined ? Number(resp.leaseExpiresAtMs) : undefined,
      onEvent,
      onLeaseLost: (detail) => {
        leaseHandle.markLost(detail);
        try {
          onLeaseLost?.(detail);
        } catch {
          // A caller's callback must not mask the loss itself.
        }
      },
    });
    // The engine may have waited before granting, so the lease can be much
    // younger than the request that won it. The block runs only if the
    // deadline that leaves has not passed.
    if (waitMs > 0) await hb.confirmGrant();
    // A lease already lost before the block starts must not run it: a block
    // that acts before its first check() would act on a lease this side has
    // already given up.
    if (leaseHandle.isLost) {
      throw new LeaseLostError(
        `Execution lease '${key}' was lost before the block started: ${hb.lostDetail}`,
      );
    }
    hb.start();

    return await currentLeaseStore.run(leaseHandle, () => fn(leaseHandle));
  } finally {
    // Before anything is awaited: work the block started and did not await can
    // still reach the handle through currentLease(), and from here on the lease
    // is not this worker's to act under.
    handle?.markReleased();
    await hb?.stop();
    if (fencingToken > 0) {
      await client
        .releaseLease(workerId, key, fencingToken, "BLOCK_EXIT", RELEASE_DEADLINE_SECS)
        .then((released) => {
          // Only a release the engine confirms: if another worker holds the
          // lease by now, there was nothing of this worker's to release.
          if (released.released) emit({ type: "released" });
        })
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
 *
 * An `apiKey` that cannot be sent as a header is refused here, when the wrapper
 * is made, not on its first call. Whether a key may go without TLS is decided
 * when each call builds its client.
 */
export function taskLease<A extends unknown[], R>(
  fn: (...args: A) => Promise<R>,
  options: ExecutionLeaseOptions & { key: string | ((...args: A) => string) },
): (...args: A) => Promise<R> {
  const { key, ...leaseOptions } = options;
  // A key that cannot be sent fails here, not on the first call.
  checkApiKeyOption(leaseOptions);
  return async function leasedTask(...args: A): Promise<R> {
    const resolved = typeof key === "function" ? key(...args) : key;
    return executionLease(resolved, leaseOptions, () => fn(...args));
  };
}
