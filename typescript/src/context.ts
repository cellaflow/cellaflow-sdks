import { AsyncLocalStorage } from "node:async_hooks";
import type { CellaflowClient } from "./client.js";
import { StepStatus } from "./cellaflow/v1/common_pb.js";

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
    this.sequence = sequence;
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
  async reconcileSequence(): Promise<void> {
    const pending = this.pendingHit;
    this.pendingHit = undefined;
    if (pending) {
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
      } catch (err) {
        const code = (err as { code?: unknown } | undefined)?.code;
        if (code !== 9 && code !== "failed_precondition") throw err;
        // Another writer in this session took the position first. The run no
        // longer knows what the session holds, so it stops recording hits and
        // the counter stays where the hit left it.
        this.historyKnown = false;
      }
      return;
    }
    if (this.reportedSequence !== undefined) {
      this.sequence = this.reportedSequence;
      this.reportedSequence = undefined;
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
