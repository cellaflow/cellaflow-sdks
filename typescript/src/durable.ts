import { createHash } from "node:crypto";
import { CellaflowClient } from "./client.js";
import type { StartSessionResponse } from "./cellaflow/v1/service_pb.js";
import {
  WorkflowContext,
  deregisterSession,
  registerSession,
  runWithContext,
} from "./context.js";
import { normalizeCoordinationId } from "./idempotency.js";

/**
 * Appended to a caller's thread id to derive the tool session, keeping it
 * distinct from whatever session a checkpointer may be using for the same
 * thread.
 */
const TOOL_SESSION_SUFFIX = "-cellaflow-tools";

/**
 * Returns the session id holding `threadId`'s leased tool calls.
 *
 * Deterministic, so a restart derives the same value and the lease taken before
 * a crash is still recognised afterwards.
 *
 * Thread ids are chosen by the application and colons are reserved by the
 * engine's key layout, so an id containing one is hashed rather than rejected:
 * `"user:123"` is an ordinary way to namespace a thread, and refusing it would
 * turn an engine storage detail into a constraint on the caller's naming.
 * Hashing keeps the one property that matters, that the same thread always
 * derives the same session, at the cost of a session id that no longer reads
 * back as the thread's name.
 */
export function toolSessionId(threadId: string): string {
  if (typeof threadId !== "string" || threadId.length === 0) {
    throw new Error(
      `threadId must be a non-empty string, got ${JSON.stringify(threadId)}. ` +
        "Every thread needs its own id: an empty one would put unrelated runs " +
        "in a single session, where they would deduplicate against each other.",
    );
  }
  if (threadId.includes(":")) {
    const digest = createHash("sha256").update(threadId, "utf8").digest("hex").slice(0, 32);
    return `lgthread-${digest}${TOOL_SESSION_SUFFIX}`;
  }
  return `${threadId}${TOOL_SESSION_SUFFIX}`;
}

/** A LangGraph-style config, or a bare thread id. */
export type ThreadRef = string | { configurable?: { thread_id?: string } };

function threadIdFrom(config: ThreadRef): string {
  if (typeof config === "string") return config;

  if (typeof config !== "object" || config === null) {
    throw new TypeError(
      "durableTools() needs the config you pass to invoke(), or a thread id " +
        `string; got ${typeof config}. Usage: ` +
        'durableTools({ configurable: { thread_id: "..." } }, fn).',
    );
  }

  const threadId = config.configurable?.thread_id;
  if (typeof threadId !== "string" || threadId.length === 0) {
    throw new Error(
      "durableTools() needs a config carrying configurable.thread_id, the same " +
        "one you pass to invoke(). The thread id is what the tool session is " +
        "derived from, so there is nothing to bind the lease to without it.",
    );
  }
  return threadId;
}

export interface DurableToolsOptions {
  workflowId?: string;
  version?: string;
  target?: string;
  secure?: boolean;
  /**
   * Names the work several agents are collaborating on: a ticket, a task, a
   * tenant. Sent to the engine, which stores it as the session's default and
   * records every step under it. Required by {@link IdempotencyScope.SHARED}.
   *
   * A later block for the same thread may omit it and adopts the stored one.
   * Naming a different one is refused by the engine, because a session's shared
   * keys would otherwise change underneath it.
   */
  coordinationId?: string;
}

/**
 * The coordination id a started session runs under.
 *
 * The engine returns the stored default, which is what lets a resumed session
 * that did not name one keep deriving the same shared keys. An engine that
 * predates the field returns nothing, and the requested value is kept, exactly
 * as before the engine recorded it.
 */
export function sessionCoordinationId(
  resp: Pick<StartSessionResponse, "coordinationId">,
  requested: string | undefined,
): string | undefined {
  return resp.coordinationId !== undefined ? resp.coordinationId : requested;
}

/** The open session, for binding a tool the framework dispatched off-context. */
export interface DurableSession {
  readonly sessionId: string;
  readonly context: WorkflowContext;
  /**
   * Re-binds the session around `fn`.
   *
   * Needed only when a framework runs a tool somewhere the async context does
   * not reach, and more than one session is open. With a single open session the
   * SDK recovers it without this.
   */
  bind<T>(fn: () => T): T;
}

/**
 * Leases every {@link tool} call made inside `fn`.
 *
 * Not tied to any framework. The contract is a session id, from a LangGraph
 * config or a bare string, and tools invoked while the callback is running.
 *
 * ```ts
 * await durableTools({ configurable: { thread_id: "ticket-4417" } }, async () => {
 *   await app.invoke({ ticket: "T-4417" }, config);
 * });
 * ```
 *
 * which is what makes a node's side effect happen at most once across a crash
 * and resume. Without it a tool either finds no session at all, or one whose id
 * is freshly generated per run, which derives a different key each time and so
 * leases nothing across the restart that matters.
 *
 * Positional replay is deliberately not seeded here. Under the default
 * `SESSION_WIDE` scope the derived key does not encode the position, so a
 * resumed call derives the same key and the engine answers from the committed
 * result. Deduplication comes from the idempotency cache, which is
 * position-independent, rather than from a counter that cannot stay aligned when
 * a framework resumes into the middle of a run.
 */
export async function durableTools<T>(
  config: ThreadRef,
  options: DurableToolsOptions,
  fn: (session: DurableSession) => Promise<T>,
): Promise<T>;
export async function durableTools<T>(
  config: ThreadRef,
  fn: (session: DurableSession) => Promise<T>,
): Promise<T>;
export async function durableTools<T>(
  config: ThreadRef,
  optionsOrFn: DurableToolsOptions | ((session: DurableSession) => Promise<T>),
  maybeFn?: (session: DurableSession) => Promise<T>,
): Promise<T> {
  const options: DurableToolsOptions =
    typeof optionsOrFn === "function" ? {} : optionsOrFn;
  const fn = (typeof optionsOrFn === "function" ? optionsOrFn : maybeFn)!;

  const {
    workflowId = "durable-tools",
    version = "1.0.0",
    target = "localhost:50051",
    secure = false,
  } = options;
  // Checked before anything is opened, so an invalid id fails here rather than
  // at the engine.
  const coordinationId = normalizeCoordinationId(options.coordinationId);

  const threadId = threadIdFrom(config);
  const sessionId = toolSessionId(threadId);

  const client = new CellaflowClient({ target, secure });
  // A resume naming a different coordination id than the stored one is refused
  // here, before the callback runs.
  const resp = await client.startSession(workflowId, version, sessionId, coordinationId);

  const ctx = new WorkflowContext({
    client,
    sessionId: resp.sessionId,
    workflowVersion: resp.version,
    sequence: 0,
    coordinationId: sessionCoordinationId(resp, coordinationId),
  });

  const session: DurableSession = {
    sessionId: ctx.sessionId,
    context: ctx,
    bind: (inner) => runWithContext(ctx, inner),
  };

  // Registered as well as bound: the async context covers frameworks that
  // dispatch tools on the calling context, and the registry covers those that
  // do not.
  registerSession(ctx);
  try {
    return await runWithContext(ctx, () => fn(session));
  } finally {
    deregisterSession(ctx);
    client.close();
  }
}
