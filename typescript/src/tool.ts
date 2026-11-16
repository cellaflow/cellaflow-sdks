import { hasStatusCode, isCommittedElsewhere, isFailedPrecondition, isRetryable, isStaleLease } from "./errors.js";
import { CacheStatus } from "./cellaflow/v1/idempotency_pb.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";
import { getContext, type UnreleasedLease, type WorkflowContext } from "./context.js";
import { IdempotencyScope, deriveIdempotencyKey, normalizeCoordinationId } from "./idempotency.js";
import { LeaseHandle, LeaseLostError, runUnderLease } from "./execution.js";
import { LeaseHeartbeat, checkLeaseTtlMs } from "./lease.js";
import { deserialize, serialize } from "./serialization.js";

/**
 * Raised when the engine refuses a lease because another agent already owns the
 * graph position this call intends to write to.
 *
 * This is the refusal arriving *before* the side effect, which is the point: the
 * alternative is discovering the divergence at commit time, after the money has
 * moved.
 */
export class DivergentStepError extends Error {
  constructor(message: string, readonly cause?: unknown) {
    super(message);
    this.name = "DivergentStepError";
  }
}

export interface ToolOptions {
  /**
   * Overrides key derivation entirely. Supply this when the operation's identity
   * is a business fact you already have, such as `charge:${orderId}`.
   */
  idempotencyKey?: string;
  /** Identifies the calling agent. Read by AGENT_PRIVATE and STEP_LOCAL scopes. */
  agentId?: string;
  /** Defaults to the function's name. Required for anonymous functions. */
  toolName?: string;
  scope?: IdempotencyScope;
  /**
   * Restricts key derivation to these keys of the tool's single object argument.
   *
   * Use when several agents must converge on one side effect while disagreeing
   * about everything else they pass. A tool using this must take one options
   * object, because JavaScript has no named arguments to select from.
   */
  sharedOn?: readonly string[];
  /**
   * How long a call's lease lasts without a renewal, in milliseconds. A holder
   * renews it while the body runs; if the holder crashes or freezes, another
   * caller can take the call over this long after its last renewal.
   *
   * A holder that stops renewing for one TTL while its body still runs (a
   * blocked event loop, a long garbage-collection pause, a lost connection to
   * the engine) is taken over too, so set it well above the longest pause a
   * call can see.
   *
   * Overrides the `ttlMs` of the enclosing {@link durableTools} block. With
   * neither, the engine's default applies (20 000 on current engines). At least
   * 1000 (`MIN_LEASE_TTL_MS`). The engine's maximum lease lifetime caps every
   * lease, whatever is asked for here.
   */
  ttlMs?: number;
}

/**
 * A leased tool: call it like the function it wraps.
 *
 * `withCoordinationId(id)` returns the same tool for one call made under `id`
 * instead of the session's coordination id. That call is recorded under `id`,
 * and under {@link IdempotencyScope.SHARED} its key is derived from `id`, so it
 * deduplicates with every agent whose session default is `id`.
 *
 * ```ts
 * // An agent working ticket-4417 refunds the customer under the customer's id,
 * // so it converges with any agent working directly for that customer.
 * await refund.withCoordinationId(customerId)({ customerId, cents: 2500 });
 * ```
 *
 * The id is checked when `withCoordinationId` is called, so an invalid one throws
 * before the call takes a sequence number or a lease. An empty string means none:
 * the call uses the session's coordination id.
 */
export type LeasedTool<A extends unknown[], R> = ((...args: A) => Promise<R>) & {
  withCoordinationId(coordinationId: string): (...args: A) => Promise<R>;
};

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Wraps a function so the engine runs it at most once per idempotency key, and
 * so its result survives the process that produced it.
 *
 * A second caller deriving the same key does not run the body. It receives what
 * the first call returned, even if that process has since died.
 *
 * ```ts
 * const chargeCard = tool(
 *   async ({ orderId, cents }: { orderId: string; cents: number }) =>
 *     gateway.charge(orderId, cents),
 *   { toolName: "chargeCard" },
 * );
 *
 * await durableTools({ configurable: { thread_id: "ticket-4417" } }, async () => {
 *   await chargeCard({ orderId: "ORD-1", cents: 1999 });
 * });
 * ```
 *
 * Must be called inside {@link durableTools}, which supplies the session.
 */
export function tool<A extends unknown[], R>(
  fn: (...args: A) => R | Promise<R>,
  options: ToolOptions = {},
): LeasedTool<A, R> {
  const {
    idempotencyKey,
    agentId = "default",
    scope = IdempotencyScope.SESSION_WIDE,
    sharedOn,
  } = options;

  const toolName = options.toolName ?? fn.name;
  if (!toolName) {
    throw new Error(
      "tool() needs a name: it is part of the idempotency key, so two anonymous " +
        "tools would otherwise derive the same key for different work. Pass " +
        "{ toolName: '...' } or use a named function.",
    );
  }
  if (sharedOn !== undefined && scope !== IdempotencyScope.SHARED) {
    throw new Error(
      "sharedOn only applies to IdempotencyScope.SHARED. Under any other scope " +
        "the key already includes the session, so restricting the hash changes " +
        "what deduplicates without making anything converge.",
    );
  }
  if (sharedOn !== undefined && idempotencyKey) {
    throw new Error(
      "sharedOn and idempotencyKey both decide the key. Supply one: an explicit " +
        "key is already the identity of the work.",
    );
  }
  const ttlMs = checkLeaseTtlMs(options.ttlMs, `tool '${toolName}'`);

  /**
   * Runs one call. `callCoordination` is this call's own coordination id, already
   * validated, or undefined to use the session's.
   */
  async function run(args: A, callCoordination: string | undefined): Promise<R> {
    const ctx = getContext();
    // The id the call is recorded and, under SHARED, deduplicated under. The two
    // must be the same: keying on the session's id while recording the override
    // would deduplicate the call in one domain and record it in another.
    const coordinationId = callCoordination ?? ctx.coordinationId;
    // The tool's own TTL, else the block's. Undefined asks for the engine's.
    const leaseTtlMs = ttlMs ?? ctx.leaseTtlMs;

    // A cache hit may have left this counter ahead of the engine's, or left a
    // result to record at its position. Both are settled before claiming the next
    // sequence, or this commit fails the ordering check and names the wrong step.
    // Deferred to here rather than done on the hit itself so a run that ends on a
    // hit does no extra work.
    //
    // The record is written by this call or a concurrent one. Wait for it before
    // claiming a position: nothing may reach the engine ahead of it, and if it
    // fails, this call fails having claimed nothing. Waiting calls resume in the
    // order they arrived, so positions are still claimed in call order, and each
    // settles again in case anything changed while it waited. With nothing being
    // written there is no await, and the claim stays synchronous as before.
    if (!ctx.positionKnown) await ctx.ensurePosition();
    for (;;) {
      ctx.reconcileSequence();
      if (!ctx.recordingInFlight) break;
      await ctx.settleRecording();
    }
    const seq = ctx.claimPosition();
    const known: Known = {};
    try {
      return await runAt(ctx, seq, args, callCoordination, coordinationId, leaseTtlMs, known);
    } catch (err) {
      if (known.lost) {
        // The call's outcome belongs to the lease's new holder, so nothing is
        // recorded here in its name. The position goes back, so calling the
        // tool again claims it and finds that holder's result.
        ctx.giveBackPosition(seq);
        throw err;
      }
      // A failed call leaves its position empty; record the failure there so the
      // next call does not land past the gap. Not for a refused lease: another
      // writer owns that position and fills it.
      if (!(err instanceof DivergentStepError)) {
        ctx.deferFailureRecord(
          seq,
          toolName,
          err,
          callCoordination,
          agentId,
          known.key ?? "",
          known.unreleased,
          leaseTtlMs,
        );
      }
      throw err;
    } finally {
      ctx.endCall();
    }
  }

  /** Runs one call at the position it claimed. */
  async function runAt(
    ctx: WorkflowContext,
    seq: number,
    args: A,
    callCoordination: string | undefined,
    coordinationId: string | undefined,
    leaseTtlMs: number | undefined,
    known: Known,
  ): Promise<R> {

    let ikey = idempotencyKey;
    if (!ikey) {
      const kwargs =
        sharedOn !== undefined && args.length === 1 && isPlainObject(args[0])
          ? (args[0] as Record<string, unknown>)
          : {};
      if (sharedOn !== undefined && Object.keys(kwargs).length === 0) {
        throw new Error(
          `Tool '${toolName}' uses sharedOn but was not called with a single ` +
            "object argument. The names in sharedOn are read from that object, " +
            "so there is nothing to select from.",
        );
      }
      ikey = deriveIdempotencyKey({
        sessionId: ctx.sessionId,
        workflowVersion: ctx.workflowVersion,
        stepSequence: seq,
        agentId,
        toolName,
        scope,
        coordinationId,
        args: sharedOn !== undefined ? [] : args,
        kwargs,
        sharedOn,
      });
    }
    known.key = ikey;

    let fencingToken = 0;
    let intervalMs = 5000;
    let hb: LeaseHeartbeat | undefined;
    // The lease as the body sees it, through currentLease(). Only when the call
    // holds one: a hit never runs the body.
    let handle: LeaseHandle | undefined;
    // While the commit is on its way, a renewal can come back refused because
    // that very commit completed the key. That is not a loss: it is held back,
    // and applied only if the commit fails.
    let committing = false;
    let lossDuringCommit: string | undefined;
    // Once the release is on its way, a renewal refused because of it is not a
    // loss either, and is dropped.
    let releasing = false;
    // Set once the body has returned, so a takeover found afterwards can carry
    // its result.
    let returned: { result: R } | undefined;

    // Arbitrate. Loop because IN_PROGRESS means another worker holds it and the
    // right move is to wait for their result rather than to act.
    for (;;) {
      let resp;
      const acquiredSentAt = performance.now();
      const acquiredSentWallAt = Date.now();
      try {
        resp = await ctx.client.checkIdempotencyCache(
          agentId,
          ikey,
          0,
          leaseTtlMs,
          ctx.sessionId,
          // Tell the engine where this call intends to write, so a lease at an
          // already-committed position is refused before the body runs rather
          // than after.
          seq,
        );
      } catch (err) {
        if (isFailedPrecondition(err)) {
          throw new DivergentStepError(
            `Step '${toolName}' at sequence ${seq} was refused: another agent ` +
              "already owns this graph position with different inputs.",
            err,
          );
        }
        throw err;
      }

      if (resp.status === CacheStatus.HIT) {
        // A hit returns without executing. When nothing is recorded at `seq` yet,
        // the result is recorded there before the next step runs, so the history
        // matches the work. The context decides which.
        const raw = resp.cachedResult?.outputPayload;
        const envelope = (
          raw && raw.length > 0 ? deserialize(raw) : { result: undefined }
        ) as { result?: R };
        if (resp.currentSequence !== undefined) {
          ctx.takeHit(
            seq,
            toolName,
            envelope as Record<string, unknown>,
            Number(resp.currentSequence),
            callCoordination,
            agentId,
            ikey,
            leaseTtlMs,
          );
        }
        return envelope?.result as R;
      }

      if (resp.status === CacheStatus.IN_PROGRESS) {
        await sleep(Number(resp.retryAfterMs ?? 1000n));
        continue;
      }

      if (resp.status === CacheStatus.ACQUIRED) {
        fencingToken = Number(resp.fencingToken ?? 0n);
        const engineInterval = Number(resp.heartbeatIntervalMs ?? 0n);
        const suggested = engineInterval > 0 ? engineInterval : 5000;
        // Renew at least every quarter of the TTL the renewals extend by, so the
        // lease cannot lapse between them. Released engines suggest exactly that;
        // the shorter of the two covers one that suggests otherwise, and an
        // engine that suggests no interval gets the old 5 s default.
        intervalMs =
          leaseTtlMs !== undefined
            ? Math.min(suggested, Math.max(1, Math.floor(leaseTtlMs / 4)))
            : suggested;
        // Judged on the clock whenever the body asks, so a holder resumed past its
        // deadline finds the lease gone before any renewal has to answer.
        const leaseHandle = new LeaseHandle(fencingToken, () => {
          hb?.pastDeadline();
        });
        handle = leaseHandle;
        hb = new LeaseHeartbeat({
          client: ctx.client,
          agentId,
          idempotencyKey: ikey,
          fencingToken,
          heartbeatIntervalMs: intervalMs,
          // Each renewal asks for the TTL the lease was taken with. Without one,
          // the heartbeat infers the engine's default from its interval.
          extendMs: leaseTtlMs,
          leaseTtlMs,
          acquiredSentAt,
          acquiredSentWallAt,
          acquiredExpiresAtMs: resp.leaseExpiresAtMs !== undefined ? Number(resp.leaseExpiresAtMs) : undefined,
          onLeaseLost: (detail) => {
            if (releasing) return;
            if (committing) lossDuringCommit = detail;
            else leaseHandle.markLost(detail);
          },
        });
        hb.start();
      }
      break;
    }

    // What is committed, taken once: every attempt sends the same step, even if
    // work the body left running changes the object it returned.
    let committed: { result: unknown } | undefined;
    const commitOnce = (result: R) =>
      ctx.client.commitStep(
        ctx.sessionId,
        seq,
        toolName,
        StepStatus.SUCCESS,
        // The envelope is part of the cross-language contract: the Python SDK
        // commits {result} and reads .result back. A bare value here would not
        // interoperate on a shared key.
        (committed ??= deserialize(serialize({ result })) as { result: unknown }),
        ikey,
        fencingToken,
        // Only the override is sent. Without one the engine records the step
        // under the session's default, which it already holds.
        callCoordination,
      );
    /**
     * Sends the same commit again. Done: the engine holds it, either from the
     * first attempt or this one. Taken over: the refusal is about the key (a
     * stale token, or the key recorded at another position), so another worker
     * holds the call. Neither: no answer, or the position already filled,
     * which can also mean the commit landed, so the outcome stays unknown.
     */
    const resend = async (
      result: R,
    ): Promise<{ done?: boolean; takenOver?: boolean; error?: unknown }> => {
      try {
        await commitOnce(result);
        return { done: true };
      } catch (again) {
        if (isStaleLease(again) || isCommittedElsewhere(again)) return { takenOver: true, error: again };
        return { error: again };
      }
    };
    /** Concludes a takeover: the body's signal aborts, and the caller hears it. */
    const takenOver = (result: R, cause: unknown) => {
      known.lost = true;
      handle?.markLost("another worker holds the call");
      return new LeaseLostError(
        `Tool '${toolName}' lost its lease to another worker before its result ` +
          "was recorded, so the result was not recorded. Calling the tool again " +
          "finds that worker's result or waits for it, unless its key includes the " +
          "position (STEP_LOCAL).",
        { result, cause },
      );
    };

    try {
      const body = () => fn(...args);
      const result = handle ? await runUnderLease(handle, body) : await body();
      // The body is over: work it started and did not await finds the lease
      // over from here on, through the commit's round trip too.
      handle?.markReleased();
      returned = { result };
      // Committed whatever this side's clock says about the lease. The engine
      // decides: it accepts the commit while the lease is still this call's,
      // which it is unless another worker took it over, and refuses a stale
      // token. Not committing on a local judgement alone would throw away the
      // record of a side effect that ran, and calling again would run it twice.
      committing = true;
      try {
        await commitOnce(result);
      } catch (err) {
        // No answer: the commit may have landed. While the lease is still this
        // call's, the same commit sent again is certain to be recorded, and an
        // engine that already holds it answers it as done. Still committing
        // meanwhile: a renewal refused then may be this commit landing.
        const outcome =
          !isStaleLease(err) && !isCommittedElsewhere(err) && isUnanswered(err)
            ? await resend(result)
            : undefined;
        if (outcome?.done) return result;
        // The call fails from here on; losses held back count from now.
        committing = false;
        if (isStaleLease(err) || isCommittedElsewhere(err)) throw takenOver(result, err);
        if (outcome?.takenOver) throw takenOver(result, outcome.error);
        throw err;
      }
      return result;
    } catch (err) {
      handle?.markReleased();
      // A loss held back during a commit that failed counts once the call
      // fails. Not before: a resend below can still show the commit landed,
      // which is what refused the renewal.
      const applyHeldLoss = () => {
        if (lossDuringCommit !== undefined) handle?.markLost(lossDuringCommit);
      };
      // Released whatever the body threw and whatever this side's clock says:
      // the engine releases only a lease that is still this call's, so its
      // answer says whether the lease is still this call's (it may have been
      // taken over, or, on some engines, simply lapsed).
      if (fencingToken > 0 && !known.lost) {
        releasing = true;
        await ctx.client
          .releaseLease(agentId, ikey, fencingToken, "TOOL_ERROR")
          .then((released) => {
            // Not released: the lease was no longer this call's to release.
            if (!released.released) known.lost = true;
          })
          .catch(() => {
            // The lease expires on its own. Losing the release is not worth
            // masking the error that caused it. Until it does, it still claims
            // this position, so the failure record there retries the release
            // or waits for it. It lasts at most as long as the heartbeat's own
            // lease settings say, counted once the heartbeat below has stopped,
            // plus a second for the engine's clock.
            known.unreleased = {
              agentId,
              key: ikey,
              token: fencingToken,
              lapsesInMs: (hb?.lastsAtMostMs ?? intervalMs * 4) + 1000,
            };
          });
      }
      if (known.lost && returned && !(err instanceof LeaseLostError)) {
        // The commit failed and the lease would not be released. Either the
        // commit landed and only its answer was lost, which completed the key
        // under this call's own token, or another worker holds the call. The
        // same commit sent again tells them apart.
        const outcome = await resend(returned.result);
        if (outcome.done) {
          known.lost = false;
          return returned.result;
        }
        if (outcome.takenOver) throw takenOver(returned.result, outcome.error);
        // Unknown: the call is not treated as taken over, and fails as it
        // would have.
        known.lost = false;
      }
      if (known.lost) handle?.markLost("the engine would not release the lease: it is no longer this call's");
      applyHeldLoss();
      throw err;
    } finally {
      // Already marked on every path that ran the body; this covers the rest.
      handle?.markReleased();
      await hb?.stop();
    }
  }

  const leased = (async (...args: A): Promise<R> => run(args, undefined)) as LeasedTool<A, R>;
  leased.withCoordinationId = (coordinationId: string) => {
    const callCoordination = normalizeCoordinationId(coordinationId);
    return (...args: A) => run(args, callCoordination);
  };
  return leased;
}

/** What one call learned that its caller needs after it fails. */
interface Known {
  /** The idempotency key, once derived. */
  key?: string;
  /** Its own lease, if it failed and could not release it. */
  unreleased?: UnreleasedLease;
  /** Set when the call's lease was lost while its body ran. */
  lost?: boolean;
}

/** `step` and `tool` are the same mechanism, kept distinct for readability. */
export const step = tool;

/** Whether an engine call got no answer either way, rather than a refusal. */
function isUnanswered(err: unknown): boolean {
  return isRetryable(err) || !hasStatusCode(err);
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
