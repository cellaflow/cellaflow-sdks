import { AsyncLocalStorage } from "node:async_hooks";
import type { CellaflowClient } from "./client.js";

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

  constructor(init: {
    client: CellaflowClient;
    sessionId: string;
    workflowVersion: string;
    sequence?: number;
    coordinationId?: string;
  }) {
    this.client = init.client;
    this.sessionId = init.sessionId;
    this.workflowVersion = init.workflowVersion;
    this.sequence = init.sequence ?? 0;
    this.coordinationId = init.coordinationId;
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
   * Adopts the position the engine reported on the last cache hit.
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
