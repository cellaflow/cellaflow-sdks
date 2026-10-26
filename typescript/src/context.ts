import { AsyncLocalStorage } from "node:async_hooks";
import type { CellaflowClient } from "./client.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";
import { isFailedPrecondition } from "./errors.js";

/** A cache hit's result, waiting to be recorded at its position in the session. */
interface PendingHit {
  sequence: number;
  name: string;
  payload: Record<string, unknown>;
  coordinationId?: string;
}

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
    this.historyKnown = init.historyKnown ?? true;
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
   */
  deferHitRecord(
    sequence: number,
    name: string,
    payload: Record<string, unknown>,
    coordinationId?: string,
  ): void {
    this.pendingHit = { sequence, name, payload, coordinationId };
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
      await this.client.commitStep(
        this.sessionId,
        pending.sequence,
        pending.name,
        StepStatus.SUCCESS,
        pending.payload,
        undefined,
        undefined,
        pending.coordinationId,
      );
      this.pendingHit = undefined;
    } catch (err) {
      // Kept on any other error, so the next call sends it again. If the write
      // did land, the engine takes an identical re-send at the same position as
      // a retry.
      if (!isFailedPrecondition(err)) throw err;
      // Another writer in this session took the position first. The run no
      // longer knows what the session holds: it stops recording hits and
      // continues from the session's real position.
      this.pendingHit = undefined;
      this.historyKnown = false;
      this.sequence = await this.enginePosition();
      console.warn(
        `Cellaflow: the result of '${pending.name}' was not recorded at position ` +
          `${pending.sequence} of session ${this.sessionId}, because another writer ` +
          `committed there first. Continuing from position ${this.sequence}. A later ` +
          "resume of this session may not replay that step.",
      );
    } finally {
      this.recording = undefined;
    }
  }

  /** The session's highest committed position, read from its history. */
  private async enginePosition(): Promise<number> {
    let highest = 0;
    let cursor: string | undefined;
    for (;;) {
      const [steps, next] = await this.client.getGraph(this.sessionId, 1000, cursor);
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
