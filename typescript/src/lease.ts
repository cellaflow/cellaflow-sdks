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
   * How long the lease survives without a confirmed renewal. Loss is declared
   * against this, not against an error count.
   */
  leaseTtlMs?: number;
  /** Called once when the lease is determined to be lost. */
  onLeaseLost?: (detail: string) => void;
}

/**
 * Renews a held lease on an interval until stopped.
 *
 * The engine requires a caller holding a lease to renew it, so this starts as
 * soon as one is acquired and stops in a `finally`.
 */
export class LeaseHeartbeat {
  private readonly opts: Required<Omit<LeaseHeartbeatOptions, "onLeaseLost">> &
    Pick<LeaseHeartbeatOptions, "onLeaseLost">;
  private timer?: NodeJS.Timeout;
  private stopped = false;
  private lastConfirmed = Date.now();
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
  }

  start(): void {
    if (this.timer) return;
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
    // Let an in-flight renewal settle so it cannot outlive the block that owns
    // the lease and renew something the caller has already released.
    if (this.inFlight) await this.inFlight.catch(() => {});
  }

  private notifyLost(detail: string): void {
    if (this.lost) return;
    this.lost = true;
    this.lostDetail = detail;
    void this.stop();
    try {
      this.opts.onLeaseLost?.(detail);
    } catch {
      // A caller's callback must not take down the heartbeat.
    }
  }

  /**
   * True once the lease can no longer be assumed held.
   *
   * A failed renewal is not itself proof of loss: the RPC failing means we could
   * not confirm the lease, while the engine may still hold it until the TTL runs
   * out. Declaring loss on an error count would abort work that still holds a
   * perfectly valid lease, so loss is declared on elapsed time since the last
   * *confirmed* renewal.
   */
  private ttlExhausted(): boolean {
    return Date.now() - this.lastConfirmed >= this.opts.leaseTtlMs;
  }

  private async beat(): Promise<void> {
    if (this.stopped || this.lost) return;
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
      if (!resp.renewed) {
        this.notifyLost(renewFailureDetail(resp.failureReason));
        return;
      }
      this.lastConfirmed = Date.now();
    } catch (err) {
      if (this.ttlExhausted()) {
        const msg = err instanceof Error ? err.message : String(err);
        this.notifyLost(
          `could not be renewed for ${this.opts.leaseTtlMs}ms (last error: ${msg})`,
        );
      }
      // Otherwise: transient. The engine may still be holding it for us.
    }
  }
}
