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
  /**
   * How long each renewal extends the lease. Defaults to 4x the interval: the
   * engine suggests an interval of a quarter of the TTL, so this asks for the
   * TTL itself.
   */
  extendMs?: number;
  /**
   * How long the acquisition granted, counted from `acquiredSentAt`. Defaults
   * to 4x the interval, the TTL the engine's suggested interval implies.
   */
  leaseTtlMs?: number;
  /**
   * When the acquiring request was sent, on the `performance.now()` clock.
   * Defaults to construction time, which is later than the send and so less
   * safe: pass it when you have it.
   */
  acquiredSentAt?: number;
  /**
   * The engine's expiry for the acquisition (`leaseExpiresAtMs`), on the
   * engine's clock. Pass it only when the engine granted the lease without
   * waiting, so the grant happened right after `acquiredSentAt`. It anchors the
   * mapping that catches a renewal the engine shortened.
   */
  acquiredExpiresAtMs?: number;
  /**
   * How long before the computed expiry the lease stops being treated as held.
   * Defaults to half a heartbeat interval, capped at half of `extendMs`.
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
 * Passing the deadline is reported, but renewing carries on: the engine may
 * still hold the lease, and a caller that cannot act on the report (`tool()`
 * does not) keeps it for as long as the engine agrees. Renewing stops only when
 * the engine refuses a renewal, or on `stop()`.
 *
 * Near the engine's maximum lease lifetime a renewal is granted less than it
 * asked for. The engine's expiry on each renewal is mapped onto the local
 * clock, so a shortened renewal moves the deadline in by the shortfall.
 */
export class LeaseHeartbeat {
  private readonly opts: Required<
    Omit<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent" | "acquiredSentAt" | "acquiredExpiresAtMs" | "marginMs">
  > &
    Pick<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent">;
  private readonly marginMs: number;
  private timer?: NodeJS.Timeout;
  private deadlineTimer?: NodeJS.Timeout;
  private stopped = false;
  private deadline: number;
  private inFlight?: Promise<void>;
  /**
   * Local time minus engine time, measured on the last renewal believed to be
   * a full extension. Engine expiries plus this give a local deadline. The
   * absolute offset includes clock skew between hosts; it cancels, because only
   * differences between the engine's own expiries are used.
   */
  private engineToLocal?: number;

  /** Set once the lease is known to be lost. Read by `LeaseHandle.check()`. */
  lost = false;
  lostDetail?: string;

  constructor(options: LeaseHeartbeatOptions) {
    const intervalMs = options.heartbeatIntervalMs;
    this.opts = {
      ...options,
      heartbeatIntervalMs: intervalMs,
      extendMs: options.extendMs ?? intervalMs * 4,
      leaseTtlMs: options.leaseTtlMs ?? intervalMs * 4,
    };
    // Half an interval: a renewal sent one interval before the expiry still
    // has time to answer, so one slow or failed renewal does not cost a lease
    // the engine is still holding.
    this.marginMs = options.marginMs ?? Math.min(intervalMs / 2, this.opts.extendMs / 2);
    const sentAt = options.acquiredSentAt ?? performance.now();
    this.deadline = sentAt + this.opts.leaseTtlMs - this.marginMs;
    if (options.acquiredExpiresAtMs !== undefined && options.acquiredSentAt !== undefined) {
      this.engineToLocal = sentAt + this.opts.leaseTtlMs - options.acquiredExpiresAtMs;
    }
  }

  /**
   * The local deadline after a confirmed renewal sent at `sentAt`.
   *
   * Counted from the send, a full extension cannot outlive the engine's. But
   * near the maximum lease lifetime the engine grants less than it was asked,
   * and only its expiry says so. That expiry is mapped onto the local clock
   * through the last renewal believed full: if it lands earlier than the full
   * extension would, the renewal was shortened and the earlier time counts.
   */
  private deadlineAfterRenewal(sentAt: number, engineExpiresAtMs: number | undefined): number {
    const full = sentAt + this.opts.extendMs;
    if (engineExpiresAtMs === undefined) return full - this.marginMs;
    if (this.engineToLocal === undefined) {
      this.engineToLocal = full - engineExpiresAtMs;
      return full - this.marginMs;
    }
    const mapped = engineExpiresAtMs + this.engineToLocal;
    // Arrival jitter makes a full renewal map a little early; only a gap
    // beyond that tolerance marks it shortened. A shortened renewal must not
    // become the reference, or the shortfall would be read as skew.
    if (mapped >= full - this.marginMs / 4) {
      this.engineToLocal = full - engineExpiresAtMs;
    }
    return Math.min(full, mapped) - this.marginMs;
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
        false,
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
    // setTimeout clamps anything above 2^31-1 ms to 1 ms, which would spin; a
    // capped wait just arms again when it fires short of the deadline.
    const wait = Math.min(2 ** 31 - 1, Math.max(0, this.deadline - performance.now()));
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
    if (this.timer || this.stopped) return;
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

  /**
   * @param final True when the engine refused the lease, so renewing is over.
   *   A passed deadline is not final: the holder must stop acting, but the
   *   engine may still hold the lease, and renewing keeps it until it refuses.
   */
  private notifyLost(detail: string, final: boolean): void {
    if (final) void this.stop();
    if (this.lost) return;
    this.lost = true;
    this.lostDetail = detail;
    // onLeaseLost first: it is what marks the handle lost, so a tracer that
    // looks at the handle from the "lost" event already sees it lost.
    try {
      this.opts.onLeaseLost?.(detail);
    } catch {
      // A caller's callback must not take down the heartbeat.
    }
    this.emit({ type: "lost", detail });
  }

  /**
   * Renews once, immediately, and waits for the answer. Used right after an
   * acquisition whose grant time is unknown, so the deadline is counted from
   * a request whose send time is known.
   */
  async renewNow(): Promise<void> {
    // Through inFlight like any renewal, so it never overlaps a scheduled one
    // and stop() waits for it.
    if (this.inFlight) await this.inFlight.catch(() => {});
    if (this.stopped) return;
    this.inFlight = this.beat(true).finally(() => {
      this.inFlight = undefined;
    });
    await this.inFlight;
  }

  private async beat(force = false): Promise<void> {
    if (this.stopped) return;
    // Report a passed deadline before renewing. The forced renewal skips this,
    // because its whole point is to replace a deadline known to be too early.
    // A renewal confirmed after the deadline does not clear the report: the
    // holder was told to stop acting, and another worker may have acted.
    if (!force) this.pastDeadline();
    const sentAt = performance.now();
    try {
      const resp = await this.opts.client.renewLease(
        this.opts.agentId,
        this.opts.idempotencyKey,
        this.opts.fencingToken,
        this.opts.extendMs,
        // Bound the RPC to half an interval, so a renewal that hangs gives up
        // before the next tick is due rather than making that tick skip.
        Math.max(1, Math.floor(this.opts.heartbeatIntervalMs / 2)) / 1000,
      );
      if (this.stopped) return;
      if (!resp.renewed) {
        this.notifyLost(renewFailureDetail(resp.failureReason), true);
        return;
      }
      // A failed renewal is not itself proof of loss, so only a confirmed one
      // moves the deadline, and only from when it was sent.
      const expiresAtMs = resp.newExpiresAtMs !== undefined ? Number(resp.newExpiresAtMs) : undefined;
      this.deadline = this.deadlineAfterRenewal(sentAt, expiresAtMs);
      this.armDeadline();
      this.emit({ type: "renewed", expiresAtMs });
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
