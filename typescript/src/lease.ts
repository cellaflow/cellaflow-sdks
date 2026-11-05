import type { CellaflowClient } from "./client.js";
import { RenewFailureReason } from "./cellaflow/v1/idempotency_pb.js";

/** Why the engine refused to renew, in words a caller can act on. */
function renewFailureDetail(reason: RenewFailureReason | undefined): string {
  switch (reason) {
    case RenewFailureReason.EXPIRED:
      return "the lease had already expired";
    case RenewFailureReason.SUPERSEDED:
      return "another worker took the lease";
    case RenewFailureReason.NOT_FOUND:
      return "the engine has no record of the lease";
    case RenewFailureReason.COMPLETED:
      return "the operation was already committed";
    case RenewFailureReason.MAX_LIFETIME_EXCEEDED:
      return "the lease hit its maximum lifetime and was reclaimed";
    default:
      return "the engine refused renewal";
  }
}

export interface LeaseHeartbeatOptions {
  client: CellaflowClient;
  agentId: string;
  idempotencyKey: string;
  fencingToken: number;
  /** How often to renew. The engine suggests this on acquisition. */
  heartbeatIntervalMs: number;
  /** How long each renewal extends the lease. Defaults to 3x the interval. */
  extendMs?: number;
  /**
   * How long the acquisition granted, counted from `acquiredSentAt`. Defaults
   * to 3x the interval, which is shorter than the engine's TTL (4x), so the
   * default errs towards declaring loss early.
   */
  leaseTtlMs?: number;
  /**
   * When the acquiring request was sent, on the `performance.now()` clock.
   * Defaults to construction time, which is later than the send and so less
   * safe: pass it when you have it.
   */
  acquiredSentAt?: number;
  /**
   * How long before the computed expiry the lease stops being treated as held.
   * Defaults to one heartbeat interval, capped at half of `extendMs`.
   */
  marginMs?: number;
  /** Called once when the lease is determined to be lost. */
  onLeaseLost?: (detail: string) => void;
  /** Called on every renewal outcome, for tracing. */
  onEvent?: (event: LeaseEvent) => void;
}

/** One thing that happened to a held lease, for tracing. */
export interface LeaseEvent {
  type: "acquired" | "renewed" | "renew_failed" | "lost" | "released";
  /** Wall-clock time of the event, in epoch milliseconds. */
  at: number;
  fencingToken: number;
  /** The engine's expiry for the lease, on the engine's clock, when it sent one. */
  expiresAtMs?: number;
  detail?: string;
}

/**
 * Renews a held lease on an interval until stopped, and knows when it can no
 * longer be assumed held.
 *
 * The engine requires a caller holding a lease to renew it, so this starts as
 * soon as one is acquired and stops in a `finally`.
 *
 * Loss is judged against a deadline, not against the last answer. Each
 * confirmed renewal moves the deadline to the time the renewal was *sent* plus
 * the extension it asked for, minus a margin. The engine counts its extension
 * from when the request arrived, which is never earlier than when it was sent,
 * so the local deadline never outlives the engine's view of the lease. The
 * deadline is on the monotonic clock, which keeps running while the process is
 * stopped: a process resumed after SIGSTOP sees its lease as lost before any
 * renewal has had a chance to answer.
 *
 * One case the deadline does not see: near the engine's maximum lease lifetime
 * a renewal is granted less than it asked for, and the next renewal is refused
 * with MAX_LIFETIME_EXCEEDED. Until that answer arrives the local deadline can
 * run up to one extension past the engine's.
 */
export class LeaseHeartbeat {
  private readonly opts: Required<
    Omit<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent" | "acquiredSentAt" | "marginMs">
  > &
    Pick<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent">;
  private readonly marginMs: number;
  private timer?: NodeJS.Timeout;
  private deadlineTimer?: NodeJS.Timeout;
  private stopped = false;
  private deadline: number;
  private inFlight?: Promise<void>;

  /** Set once the lease is known to be lost. Read by `LeaseHandle.check()`. */
  lost = false;
  lostDetail?: string;

  constructor(options: LeaseHeartbeatOptions) {
    const intervalMs = options.heartbeatIntervalMs;
    this.opts = {
      ...options,
      heartbeatIntervalMs: intervalMs,
      extendMs: options.extendMs ?? intervalMs * 3,
      leaseTtlMs: options.leaseTtlMs ?? intervalMs * 3,
    };
    this.marginMs = options.marginMs ?? Math.min(intervalMs, this.opts.extendMs / 2);
    const sentAt = options.acquiredSentAt ?? performance.now();
    this.deadline = sentAt + this.opts.leaseTtlMs - this.marginMs;
  }

  /**
   * True once the deadline has passed, and declares the loss if so. Cheap
   * enough to call before every irreversible step.
   */
  pastDeadline(): boolean {
    if (this.lost) return true;
    if (this.stopped) return false;
    if (performance.now() >= this.deadline) {
      this.notifyLost(
        `no renewal was confirmed in time: the lease may already belong to another worker`,
      );
      return true;
    }
    return false;
  }

  private emit(event: Omit<LeaseEvent, "at" | "fencingToken">): void {
    try {
      this.opts.onEvent?.({ at: Date.now(), fencingToken: this.opts.fencingToken, ...event });
    } catch {
      // A caller's tracer must not take down the heartbeat.
    }
  }

  private armDeadline(): void {
    if (this.deadlineTimer) clearTimeout(this.deadlineTimer);
    if (this.stopped || this.lost) return;
    const wait = Math.max(0, this.deadline - performance.now());
    this.deadlineTimer = setTimeout(() => {
      this.deadlineTimer = undefined;
      // Timers run on a millisecond clock and can fire a fraction early against
      // performance.now(), so a timer that finds the deadline not yet passed
      // must arm again rather than stand down.
      if (!this.pastDeadline()) this.armDeadline();
    }, wait);
    this.deadlineTimer.unref?.();
  }

  start(): void {
    if (this.timer) return;
    this.armDeadline();
    this.timer = setInterval(() => {
      // Never overlap renewals: a slow RPC would otherwise queue them and each
      // would extend from a stale view of the lease.
      if (this.inFlight) return;
      this.inFlight = this.beat().finally(() => {
        this.inFlight = undefined;
      });
    }, this.opts.heartbeatIntervalMs);
    // Do not hold the event loop open on the heartbeat alone.
    this.timer.unref?.();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.deadlineTimer) {
      clearTimeout(this.deadlineTimer);
      this.deadlineTimer = undefined;
    }
    // Let an in-flight renewal settle so it cannot outlive the block that owns
    // the lease and renew something the caller has already released.
    if (this.inFlight) await this.inFlight.catch(() => {});
  }

  private notifyLost(detail: string): void {
    if (this.lost) return;
    this.lost = true;
    this.lostDetail = detail;
    void this.stop();
    this.emit({ type: "lost", detail });
    try {
      this.opts.onLeaseLost?.(detail);
    } catch {
      // A caller's callback must not take down the heartbeat.
    }
  }

  /**
   * Renews once, immediately, and waits for the answer. Used right after an
   * acquisition whose grant time is unknown, so the deadline is counted from
   * a request whose send time is known.
   */
  async renewNow(): Promise<void> {
    await this.beat(true);
  }

  private async beat(force = false): Promise<void> {
    if (this.stopped || this.lost) return;
    // A renewal that answers after the deadline cannot revive the lease: by
    // then another worker may hold it, and this one has already been told.
    // The forced renewal skips this, because its whole point is to replace a
    // deadline known to be too early.
    if (!force && this.pastDeadline()) return;
    const sentAt = performance.now();
    try {
      const resp = await this.opts.client.renewLease(
        this.opts.agentId,
        this.opts.idempotencyKey,
        this.opts.fencingToken,
        this.opts.extendMs,
        // Bound the RPC. Without a deadline a black-holed connection parks the
        // renewal forever and the lease silently ages out while we wait on it.
        Math.max(1, Math.ceil(this.opts.heartbeatIntervalMs / 1000)),
      );
      if (this.stopped || this.lost) return;
      if (!resp.renewed) {
        this.notifyLost(renewFailureDetail(resp.failureReason));
        return;
      }
      // A failed renewal is not itself proof of loss, so only a confirmed one
      // moves the deadline, and only from when it was sent.
      this.deadline = sentAt + this.opts.extendMs - this.marginMs;
      this.armDeadline();
      this.emit({
        type: "renewed",
        expiresAtMs: resp.newExpiresAtMs !== undefined ? Number(resp.newExpiresAtMs) : undefined,
      });
    } catch (err) {
      // Transient until the deadline says otherwise: the engine may still be
      // holding the lease for us, and aborting work that holds a valid lease
      // is its own failure.
      const msg = err instanceof Error ? err.message : String(err);
      this.emit({ type: "renew_failed", detail: msg });
      this.pastDeadline();
    }
  }
}
