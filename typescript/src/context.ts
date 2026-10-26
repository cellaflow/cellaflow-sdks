import { AsyncLocalStorage } from "node:async_hooks";
import type { CellaflowClient } from "./client.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";
import { CacheStatus } from "./cellaflow/v1/idempotency_pb.js";
import { recordIdempotencyKey } from "./idempotency.js";
import {
  errorCode,
  hasStatusCode,
  isFailedPrecondition,
  isNotFound,
  isRetryable,
} from "./errors.js";

/** A cache hit's result, waiting to be recorded at its position in the session. */
interface PendingHit {
  sequence: number;
  name: string;
  payload: Record<string, unknown>;
  coordinationId?: string;
  /** The agent whose call was answered by the hit. */
  agentId: string;
  /** The idempotency key the hit answered. */
  receivedKey: string;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * The session a tool call belongs to, plus the bookkeeping that keeps the local
 * sequence counter aligned with the engine's.
 */
export class WorkflowContext {
  readonly client: CellaflowClient;
  readonly sessionId: string;
  readonly workflowVersion: string;
  sequence: number;
  /**
   * Names the work several agents are collaborating on: a ticket, a task, a
   * tenant. Only {@link IdempotencyScope.SHARED} reads it, and that scope
   * requires it.
   */
  readonly coordinationId?: string;

  /**
   * The session position the engine last reported, held from a cache hit until
   * the next step consumes it. See {@link reconcileSequence}.
   */
  private reportedSequence?: number;

  /**
   * Whether this run has accounted for every position the session holds up to
   * {@link sequence}: true for a fresh session. False once the run resumes into
   * history it did not see, which a resumed `durableTools` session always does,
   * because it re-runs tools and lets the engine answer them rather than
   * replaying by position. Only while it holds can a cache hit be recorded in
   * this session without risking a second record of a step the session already
   * holds. See {@link deferHitRecord}.
   */
  historyKnown: boolean;

  private pendingHit?: PendingHit;

  /**
   * The write of {@link pendingHit}, while it is in flight. Every call waits for it
   * before claiming a position, so no later position is committed ahead of it.
   */
  private recording?: Promise<void>;

  /**
   * Calls in this session that have claimed a position and not yet returned.
   * Calls overlap whenever tools run concurrently, such as under `Promise.all`.
   * See {@link takeHit}.
   */
  private inFlight = 0;

  constructor(init: {
    client: CellaflowClient;
    sessionId: string;
    workflowVersion: string;
    sequence?: number;
    coordinationId?: string;
    historyKnown?: boolean;
  }) {
    this.client = init.client;
    this.sessionId = init.sessionId;
    this.workflowVersion = init.workflowVersion;
    this.sequence = init.sequence ?? 0;
    this.coordinationId = init.coordinationId;
    // Opt-in: a context built for a session whose history it never read must not
    // record into it.
    this.historyKnown = init.historyKnown ?? false;
  }

  /**
   * Holds a cache hit's result to be recorded at `sequence` in this session.
   *
   * A hit satisfied from another session leaves nothing at its position in this
   * one. Without a record there, the next step takes that position, so the
   * session's history no longer matches the work it did, and an SDK that
   * resumes by position cannot replay it. Recording the result keeps the history
   * the same shape as the work.
   *
   * Deferred to the next step, like {@link recordEngineSequence}: the record only
   * matters once something would otherwise take its position, so a run that ends
   * on a hit still does no extra work.
   *
   * `receivedKey` is the idempotency key the hit answered, and `agentId` the agent
   * that asked. The record is leased under its own key, derived from both. See
   * {@link recordIdempotencyKey}.
   */
  deferHitRecord(
    sequence: number,
    name: string,
    payload: Record<string, unknown>,
    coordinationId?: string,
    agentId = "default",
    receivedKey = "",
  ): void {
    this.pendingHit = { sequence, name, payload, coordinationId, agentId, receivedKey };
  }

  /**
   * Claims the next position for a call and counts it as in flight. Called after
   * {@link reconcileSequence} and {@link settleRecording}, with no await between,
   * so a pending record is always written before any later position is claimed.
   * Every call to this is paired with {@link endCall}.
   */
  claimPosition(): number {
    this.sequence += 1;
    this.inFlight += 1;
    return this.sequence;
  }

  /** Marks a call claimed by {@link claimPosition} as returned. */
  endCall(): void {
    this.inFlight -= 1;
  }

  /**
   * Decides what a cache hit at `sequence` leaves in this session.
   *
   * The hit is recorded at its position only when nothing else could have moved
   * the session meanwhile: it is the only call in flight, the run knows
   * everything the session holds, and the engine reports the session exactly one
   * position behind it. Otherwise the run adopts the position the engine
   * reported and stops recording hits: a peer in this session already wrote it,
   * the run is resuming into history it did not see, or other calls in flight
   * may commit in any order.
   */
  takeHit(
    sequence: number,
    name: string,
    payload: Record<string, unknown>,
    reported: number,
    coordinationId?: string,
    agentId = "default",
    receivedKey = "",
  ): void {
    if (this.inFlight === 1 && this.historyKnown && reported === sequence - 1) {
      this.deferHitRecord(sequence, name, payload, coordinationId, agentId, receivedKey);
      return;
    }
    // The run can no longer tell what the session holds.
    this.historyKnown = false;
    this.recordEngineSequence(reported);
  }

  /**
   * Notes the session position the engine reported alongside a cache hit.
   *
   * Held rather than applied immediately: a run whose last act is a shared tool,
   * which is the common shape, should not pay for bookkeeping it will never use.
   * {@link reconcileSequence} consumes it at the start of the next step.
   */
  recordEngineSequence(sequence: number): void {
    this.reportedSequence = sequence;
  }

  /**
   * Records a deferred hit, then adopts the position the engine reported.
   *
   * Every tool call increments this counter, but a cache hit returns *without
   * committing*. The engine's sequence therefore did not advance while the local
   * one did, and the next commit fails the ordering check one step after the
   * real cause.
   *
   * The same-session case survives on a coincidence rather than an invariant: a
   * peer's commit advances the engine by exactly the amount this caller advanced
   * locally, so the two happen to stay equal. Any asymmetry breaks it, such as a
   * hit satisfied from a *different* session, or replicas that reached a shared
   * tool after different numbers of steps.
   *
   * A no-op when the engine reported nothing, which is an older engine predating
   * the field. Behaviour then degrades to the original defect rather than to
   * something new.
   */
  reconcileSequence(): void {
    if (this.pendingHit) {
      // Started here and awaited by {@link settleRecording}, so a call still claims
      // its position synchronously, in call order, as it always has.
      this.recording ??= this.writePendingHit();
      return;
    }
    if (this.reportedSequence !== undefined) {
      this.sequence = this.reportedSequence;
      this.reportedSequence = undefined;
    }
  }

  /** Whether a deferred record is being written. */
  get recordingInFlight(): boolean {
    return this.recording !== undefined;
  }

  /**
   * Waits for a deferred record being written, by this call or a concurrent one.
   * Rejects if the write failed; the record is kept and the next call sends it
   * again.
   */
  async settleRecording(): Promise<void> {
    if (this.recording) await this.recording;
  }

  private async writePendingHit(): Promise<void> {
    const pending = this.pendingHit!;
    try {
      await this.writeRecord(pending);
      if (this.pendingHit === pending) this.pendingHit = undefined;
    } catch (err) {
      // Kept, so the next call sends it again. If the write did land, sending it
      // again finds its completed entry.
      if (isRetryable(err)) throw err;
      // The record will not be accepted: another writer in this session took the
      // position first, the engine refused the write itself, or it failed before
      // reaching the engine and would fail the same way on every retry. Read where
      // the session is before changing anything, so a failed read leaves the
      // record to be tried again.
      const position = Math.max(
        pending.sequence - 1,
        await this.enginePosition(pending.sequence),
      );
      // The run no longer knows what the session holds: it stops recording hits
      // and continues from the session's real position.
      if (this.pendingHit === pending) this.pendingHit = undefined;
      this.historyKnown = false;
      this.sequence = position;
      const reason = isFailedPrecondition(err)
        ? "another writer committed or claimed it first"
        : hasStatusCode(err)
          ? `the engine refused it (${errorCode(err)})`
          : `it could not be sent (${errorCode(err)})`;
      console.warn(
        `Cellaflow: the result of '${pending.name}' was not recorded at position ` +
          `${pending.sequence} of session ${this.sessionId}, because ${reason}. ` +
          `Continuing from position ${this.sequence}. A later resume of this ` +
          "session may not replay that step.",
      );
    } finally {
      this.recording = undefined;
    }
  }

  /**
   * Records a received result at its position, through a lease.
   *
   * A plain commit is checked only for order, not against the positions that
   * leases have claimed. A replica in this session holding a lease at the same
   * position would then have its commit refused after its side effect ran, losing
   * the record that it did. Leasing the record makes the engine decide: it refuses
   * the record at a position another lease has claimed, or refuses the later
   * lease before its side effect runs.
   */
  private async writeRecord(pending: PendingHit): Promise<void> {
    const key = recordIdempotencyKey(this.sessionId, pending.sequence, pending.receivedKey);
    let token = 0;
    for (;;) {
      const resp = await this.client.checkIdempotencyCache(
        pending.agentId,
        key,
        undefined,
        undefined,
        this.sessionId,
        pending.sequence,
      );
      // Written by an earlier attempt whose answer was lost.
      if (resp.status === CacheStatus.HIT) return;
      if (resp.status === CacheStatus.IN_PROGRESS) {
        // An earlier attempt's lease, not yet released or lapsed.
        await sleep(Number(resp.retryAfterMs ?? 1000n));
        continue;
      }
      if (resp.status !== CacheStatus.ACQUIRED) {
        throw new Error(`unexpected cache status ${resp.status} leasing the record`);
      }
      token = Number(resp.fencingToken ?? 0n);
      break;
    }
    try {
      await this.client.commitStep(
        this.sessionId,
        pending.sequence,
        pending.name,
        StepStatus.SUCCESS,
        pending.payload,
        key,
        token,
        pending.coordinationId,
      );
    } catch (err) {
      await this.client.releaseLease(pending.agentId, key, token, "TOOL_ERROR").catch(() => {
        // The lease lapses on its own. Losing the release is not worth masking the
        // error that caused it.
      });
      throw err;
    }
  }

  /**
   * The session's highest committed position at or after `fromSequence`, or 0 if
   * there is none. The engine's page cursor is the first sequence to read and is
   * inclusive, so only the tail of the session is read.
   *
   * A session the engine does not know answers NOT_FOUND rather than an empty
   * page, which means it holds nothing. NOT_FOUND after the first page means the
   * session went away mid-read, and propagates like any other failure.
   */
  private async enginePosition(fromSequence: number): Promise<number> {
    let highest = 0;
    let cursor: string | undefined = String(fromSequence);
    let firstPage = true;
    for (;;) {
      let steps: Record<string, any>[];
      let next: string | undefined;
      try {
        [steps, next] = await this.client.getGraph(this.sessionId, 1000, cursor);
      } catch (err) {
        if (firstPage && isNotFound(err)) return 0;
        throw err;
      }
      firstPage = false;
      for (const step of steps) highest = Math.max(highest, Number(step.sequence ?? 0));
      if (!next) return highest;
      cursor = next;
    }
  }
}

const storage = new AsyncLocalStorage<WorkflowContext>();

/**
 * Sessions currently open, for frameworks that dispatch a tool off the calling
 * context.
 *
 * `AsyncLocalStorage` follows `await` and `setTimeout`, which covers most
 * frameworks. It does not follow a hop through a worker thread or a native
 * callback that loses the async resource. Where exactly one session is open the
 * context is recoverable from here; where several are, there is nothing to
 * disambiguate them and the caller must bind explicitly.
 */
const openSessions = new Set<WorkflowContext>();

export function registerSession(ctx: WorkflowContext): void {
  openSessions.add(ctx);
}

export function deregisterSession(ctx: WorkflowContext): void {
  openSessions.delete(ctx);
}

/** Runs `fn` with `ctx` as the active context. */
export function runWithContext<T>(ctx: WorkflowContext, fn: () => T): T {
  return storage.run(ctx, fn);
}

/**
 * Returns the context a tool call belongs to.
 *
 * Falls back to the open-session registry when the async context was lost, but
 * only when a single session is open. Two open sessions and a lost context is
 * ambiguous, and guessing would attribute a side effect to the wrong run.
 */
export function getContext(): WorkflowContext {
  const ctx = storage.getStore();
  if (ctx) return ctx;

  if (openSessions.size === 1) {
    return openSessions.values().next().value as WorkflowContext;
  }

  if (openSessions.size === 0) {
    throw new Error(
      "No CellaFlow session is active. A leased tool must be called inside " +
        "durableTools(...), which is what binds it to a session. Without one " +
        "there is no idempotency key to derive and nothing to lease.",
    );
  }

  throw new Error(
    `The calling context was lost and ${openSessions.size} sessions are open, so ` +
      "the session this tool belongs to is ambiguous. Bind it explicitly with " +
      "session.bind(() => ...) inside the tool, or open one session at a time.",
  );
}

/** Whether a context is currently reachable, without throwing. */
export function hasContext(): boolean {
  return storage.getStore() !== undefined || openSessions.size === 1;
}
