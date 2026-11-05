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
  /** The same moment on the wall clock (`Date.now()`). */
  acquiredSentWallAt?: number;
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
 * clock, so a shortened renewal moves the deadline in by the shortfall. Two
 * cases it cannot see: a first renewal that is already shortened, and an
 * acquisition whose TTL exceeds the maximum lifetime, because neither has a
 * full renewal before it to compare against.
 */
/*
 * What this class trades off, highest priority first. Resolve new cases from
 * this order rather than one by one:
 *
 * 1. Never report the lease as held when the engine may have handed it on.
 * 2. Never report it lost while the engine certainly holds it, where avoidable.
 * 3. Events and the handle say what happened: releasing a lease is not losing it.
 * 4. Keep round trips to the engine few.
 */
export class LeaseHeartbeat {
  private readonly opts: Required<
    Omit<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent" | "acquiredSentAt" | "acquiredSentWallAt" | "marginMs">
  > &
    Pick<LeaseHeartbeatOptions, "onLeaseLost" | "onEvent">;
  /** How long before the computed expiry the lease stops being treated as held. */
  readonly marginMs: number;
  private timer?: NodeJS.Timeout;
  private deadlineTimer?: NodeJS.Timeout;
  private stopped = false;
  private deadline: number;
  /**
   * The same deadline on the wall clock. performance.now() does not advance
   * while the host is suspended, and the engine's clock does, so a woken host
   * must not trust the monotonic deadline alone. A wall clock stepped forward
   * can only make the lease read as lost early.
   */
  private wallDeadline: number;
  private inFlight?: Promise<unknown>;
  /**
   * Wall time minus engine time, for recent renewals known to have been full
   * extensions. The largest (least latency) maps an engine expiry onto the
   * wall clock. The wall clock, because it keeps running through a host
   * suspend as the engine's does, where the monotonic clock stands still.
   * Only recent ones, so a reference from before a clock step cannot linger.
   * The absolute offset includes skew between hosts; it cancels, because only
   * differences between the engine's own expiries matter.
   */
  private readonly fullOffsets: number[] = [];
  /** The latest renewal, not yet known to have been a full extension. */
  private pending?: { offset: number; expiresAtMs: number };

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
    const sentWallAt = options.acquiredSentWallAt ?? Date.now() - (performance.now() - sentAt);
    this.deadline = 0;
    this.wallDeadline = 0;
    this.setDeadline({ mono: sentAt, wall: sentWallAt }, this.opts.leaseTtlMs - this.marginMs);
  }

  /**
   * Both deadlines are counted from the same send, each on its own clock, so a
   * suspend between the send and handling the answer delays neither.
   */
  private setDeadline(sent: { mono: number; wall: number }, durationMs: number): void {
    this.deadline = sent.mono + durationMs;
    this.wallDeadline = sent.wall + durationMs;
  }

  /**
   * How long, from its send, a confirmed renewal sent at wall time `sentWallAt`
   * keeps the lease held.
   *
   * Counted from the send, a full extension cannot outlive the engine's. But
   * near the maximum lease lifetime the engine grants less than it was asked,
   * and only its expiry says so. That expiry is mapped onto the wall clock
   * through a renewal known to have been full, and the shorter time counts.
   *
   * A renewal is known to have been full once a later one expires strictly
   * later: a shortened renewal ends at the lease's fixed ceiling, which no
   * later renewal can pass. Among the last few of those, the one with the least
   * latency is the reference, so a single slow renewal cannot make later ones
   * look shortened. Until a reference exists (the first renewal), the full
   * extension counts.
   */
  private renewalDuration(sentWallAt: number, engineExpiresAtMs: number | undefined): number {
    const full = this.opts.extendMs;
    if (engineExpiresAtMs === undefined) return full - this.marginMs;
    if (this.pending && engineExpiresAtMs > this.pending.expiresAtMs) {
      this.fullOffsets.push(this.pending.offset);
      if (this.fullOffsets.length > 4) this.fullOffsets.shift();
    }
    this.pending = { offset: sentWallAt + full - engineExpiresAtMs, expiresAtMs: engineExpiresAtMs };
    if (this.fullOffsets.length === 0) return full - this.marginMs;
    const mapped = engineExpiresAtMs + Math.max(...this.fullOffsets) - sentWallAt;
    return Math.min(full, mapped) - this.marginMs;
  }

  /**
   * True once the deadline has passed, and declares the loss if so. Cheap
   * enough to call before every irreversible step.
   */
  pastDeadline(): boolean {
    if (this.lost) return true;
    if (this.stopped) return false;
    if (performance.now() >= this.deadline || Date.now() >= this.wallDeadline) {
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
    // Timers run on the monotonic clock, which stands still while the host is
    // suspended, so a woken host would not see its wall deadline until the
    // monotonic one came due. Waking at least every half interval bounds that,
    // so a signal handed to a request aborts soon after a wake. The cap also
    // keeps the wait under setTimeout's 2^31-1 ms limit.
    const cap = Math.max(1, Math.floor(this.opts.heartbeatIntervalMs / 2));
    const wait = Math.min(cap, Math.max(0, this.deadline - performance.now()));
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
    if (this.lost) {
      // Already reported on the deadline; now the engine has answered. Say so,
      // with its reason, so a tracer can tell a lease taken from one kept.
      if (final) {
        this.lostDetail = detail;
        this.emit({ type: "lost", detail });
      }
      return;
    }
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
   * a request whose send time is known. True when the renewal was confirmed.
   */
  async renewNow(): Promise<boolean> {
    // Through inFlight like any renewal, so it never overlaps a scheduled one
    // and stop() waits for it.
    if (this.inFlight) await this.inFlight.catch(() => {});
    if (this.stopped) return false;
    let confirmed = false;
    this.inFlight = this.beat(true).then((ok) => {
      confirmed = ok;
    }).finally(() => {
      this.inFlight = undefined;
    });
    await this.inFlight;
    return confirmed;
  }

  private async beat(force = false): Promise<boolean> {
    if (this.stopped) return false;
    // Report a passed deadline before renewing. The forced renewal skips this,
    // because its whole point is to replace a deadline known to be too early.
    // A renewal confirmed after the deadline does not clear the report: the
    // holder was told to stop acting, and another worker may have acted.
    if (!force) this.pastDeadline();
    const sent = { mono: performance.now(), wall: Date.now() };
    try {
      const resp = await this.opts.client.renewLease(
        this.opts.agentId,
        this.opts.idempotencyKey,
        this.opts.fencingToken,
        this.opts.extendMs,
        // Bound the RPC to half an interval, so a renewal that hangs gives up
        // before the next tick is due. Short TTLs get up to a second, never
        // more than half the TTL: a renewal is a synced write, and one that
        // takes longer than a short interval still keeps the lease.
        Math.max(
          Math.floor(this.opts.heartbeatIntervalMs / 2),
          Math.min(1000, Math.floor(this.opts.extendMs / 2)),
        ) / 1000,
      );
      if (this.stopped) return false;
      if (!resp.renewed) {
        this.notifyLost(renewFailureDetail(resp.failureReason), true);
        return false;
      }
      // A failed renewal is not itself proof of loss, so only a confirmed one
      // moves the deadline, and only from when it was sent.
      const expiresAtMs = resp.newExpiresAtMs !== undefined ? Number(resp.newExpiresAtMs) : undefined;
      this.setDeadline(sent, this.renewalDuration(sent.wall, expiresAtMs));
      this.armDeadline();
      this.emit({ type: "renewed", expiresAtMs });
      return true;
    } catch (err) {
      // Transient until the deadline says otherwise: the engine may still be
      // holding the lease for us, and aborting work that holds a valid lease
      // is its own failure. A forced renewal leaves the judgement to its caller:
      // the deadline it would be judged against is the one known to be stale.
      const msg = err instanceof Error ? err.message : String(err);
      this.emit({ type: "renew_failed", detail: msg });
      if (!force) this.pastDeadline();
      return false;
    }
  }
}
