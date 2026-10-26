import { isFailedPrecondition } from "./errors.js";
import { CacheStatus } from "./cellaflow/v1/idempotency_pb.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";
import { getContext, type WorkflowContext } from "./context.js";
import { IdempotencyScope, deriveIdempotencyKey, normalizeCoordinationId } from "./idempotency.js";
import { LeaseHeartbeat } from "./lease.js";
import { deserialize } from "./serialization.js";

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
    for (;;) {
      ctx.reconcileSequence();
      if (!ctx.recordingInFlight) break;
      await ctx.settleRecording();
    }
    const seq = ctx.claimPosition();
    const known: { key?: string } = {};
    try {
      return await runAt(ctx, seq, args, callCoordination, coordinationId, known);
    } catch (err) {
      // A failed call leaves its position empty; record the failure there so the
      // next call does not land past the gap. Not for a refused lease: another
      // writer owns that position and fills it.
      if (!(err instanceof DivergentStepError)) {
        ctx.deferFailureRecord(seq, toolName, err, callCoordination, agentId, known.key ?? "");
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
    known: { key?: string },
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
    let hb: LeaseHeartbeat | undefined;

    // Arbitrate. Loop because IN_PROGRESS means another worker holds it and the
    // right move is to wait for their result rather than to act.
    for (;;) {
      let resp;
      try {
        resp = await ctx.client.checkIdempotencyCache(
          agentId,
          ikey,
          0,
          undefined,
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
        const intervalMs = Number(resp.heartbeatIntervalMs ?? 5000n);
        hb = new LeaseHeartbeat({
          client: ctx.client,
          agentId,
          idempotencyKey: ikey,
          fencingToken,
          heartbeatIntervalMs: intervalMs,
        });
        hb.start();
      }
      break;
    }

    try {
      const result = await fn(...args);
      await ctx.client.commitStep(
        ctx.sessionId,
        seq,
        toolName,
        StepStatus.SUCCESS,
        // The envelope is part of the cross-language contract: the Python SDK
        // commits {result} and reads .result back. A bare value here would not
        // interoperate on a shared key.
        { result },
        ikey,
        fencingToken,
        // Only the override is sent. Without one the engine records the step
        // under the session's default, which it already holds.
        callCoordination,
      );
      return result;
    } catch (err) {
      if (fencingToken > 0) {
        await ctx.client
          .releaseLease(agentId, ikey, fencingToken, "TOOL_ERROR")
          .catch(() => {
            // The lease expires on its own. Losing the release is not worth
            // masking the error that caused it.
          });
      }
      throw err;
    } finally {
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

/** `step` and `tool` are the same mechanism, kept distinct for readability. */
export const step = tool;

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
