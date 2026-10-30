import canonicalize from "canonicalize";
import { createHash } from "node:crypto";

/**
 * How widely a derived idempotency key deduplicates.
 *
 * The numeric values match the Python SDK and the engine's enum. They are part
 * of the wire contract, not an implementation detail.
 */
export enum IdempotencyScope {
  UNSPECIFIED = 0,
  /** Shared across all agents in the session. The default. */
  SESSION_WIDE = 1,
  /** Isolated to the executing agent. */
  AGENT_PRIVATE = 2,
  /** Isolated to the specific superstep / node. */
  STEP_LOCAL = 3,
  /**
   * Shared across *sessions* within a declared coordination domain.
   *
   * The only scope whose key omits `sessionId`, so agents running different
   * workflows in different sessions deduplicate one shared side effect.
   */
  SHARED = 4,
}

/** The longest coordination id the engine accepts, in UTF-8 bytes. */
export const MAX_COORDINATION_ID_BYTES = 256;

/**
 * Applies the engine's rules for a coordination id, returning it unchanged.
 *
 * `undefined`, `null` and the empty string mean none and return `undefined`. A
 * value that is not a string throws `TypeError`. Leading or trailing whitespace,
 * colons, and values over 256 UTF-8 bytes throw `Error`.
 *
 * The value is never trimmed or otherwise cleaned up. It is hashed into
 * {@link IdempotencyScope.SHARED} keys exactly as given, so a cleaned-up copy
 * would record the work under one id while deduplicating it under another.
 *
 * The engine applies the same rules, but a call's own coordination id only
 * reaches it with the commit, after the tool has run. Checking here means an
 * invalid value fails before the side effect rather than after it.
 */
export function normalizeCoordinationId(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") {
    throw new TypeError(
      `coordinationId must be a string, got ${typeof value}; pass String(...) of it ` +
        "to use it as one.",
    );
  }
  if (value === "") return undefined;
  if (value.trim() !== value) {
    throw new Error(
      `coordinationId ${JSON.stringify(value)} cannot be blank or have leading or ` +
        "trailing whitespace.",
    );
  }
  if (value.includes(":")) {
    throw new Error(`coordinationId ${JSON.stringify(value)} cannot contain colons (:).`);
  }
  if (Buffer.byteLength(value, "utf8") > MAX_COORDINATION_ID_BYTES) {
    throw new Error(`coordinationId exceeds ${MAX_COORDINATION_ID_BYTES} bytes.`);
  }
  return value;
}

/**
 * Hashes inputs with RFC 8785 Canonical JSON and SHA-256, returning the first
 * 16 bytes hex-encoded.
 *
 * Byte-identical to the Python SDK's `_hash_inputs`, which is what allows a
 * TypeScript agent and a Python agent to converge on one key under
 * {@link IdempotencyScope.SHARED}. Verified against Python reference hashes
 * covering unicode, escapes, nested objects, key reordering and exponent-form
 * numbers. Changing the canonicalisation or the digest length silently stops
 * cross-language deduplication, which fails by repeating the side effect rather
 * than by raising.
 */
export function hashInputs(args: unknown[], kwargs: Record<string, unknown>): string {
  const canon = canonicalize({ args, kwargs });
  if (canon === undefined) {
    throw new TypeError(
      "Tool arguments could not be canonicalised. Arguments contributing to an " +
        "idempotency key must be JSON-representable: no functions, symbols, " +
        "BigInt, undefined, or circular references.",
    );
  }
  return createHash("sha256").update(canon, "utf8").digest("hex").slice(0, 32);
}

export interface DeriveKeyOptions {
  sessionId: string;
  workflowVersion: string;
  stepSequence: number;
  agentId: string;
  toolName: string;
  scope: IdempotencyScope;
  coordinationId?: string;
  /** Positional arguments the tool was called with. */
  args?: unknown[];
  /** Named arguments, when the tool takes a single options object. */
  kwargs?: Record<string, unknown>;
  /**
   * Restricts the hash to the named keys of `kwargs`.
   *
   * Heterogeneous agents converge on one side effect precisely when they
   * disagree about everything else, so hashing everything they pass is the one
   * thing guaranteed to keep them apart.
   *
   * JavaScript has no named arguments, so a tool using `sharedOn` must take a
   * single options object; the names are read from it.
   */
  sharedOn?: readonly string[];
}

/** Derives the canonical idempotency key for a step or tool execution. */
export function deriveIdempotencyKey(opts: DeriveKeyOptions): string {
  const {
    sessionId,
    workflowVersion,
    stepSequence,
    agentId,
    toolName,
    scope,
    coordinationId,
    args = [],
    kwargs = {},
    sharedOn,
  } = opts;

  let inputsHash: string;
  if (sharedOn !== undefined) {
    const selected: Record<string, unknown> = {};
    for (const k of sharedOn) {
      if (Object.prototype.hasOwnProperty.call(kwargs, k)) selected[k] = kwargs[k];
    }
    inputsHash = hashInputs([], selected);
  } else {
    inputsHash = hashInputs(args, kwargs);
  }

  if (scope === IdempotencyScope.SHARED) {
    // The only scope that omits sessionId, so agents in different sessions
    // converge on one key. It also omits workflowVersion, because the whole
    // point is that *different* workflows share the operation and they will
    // not be on the same version.
    //
    // coordinationId is what keeps this from being too wide. Without it two
    // unrelated callers of sendEmail({to: X}) would deduplicate, suppressing
    // one of them silently. It is required, and the caller must choose it.
    if (!coordinationId) {
      throw new Error(
        "IdempotencyScope.SHARED requires a coordinationId naming the work being " +
          "shared: a ticket, task, or tenant id. Pass it when opening the session, " +
          'e.g. durableTools(config, { coordinationId: "ticket-4417" }, fn), or for ' +
          'one call, e.g. refund.withCoordinationId("customer-88")(...). It has no ' +
          "default: a shared one would deduplicate unrelated callers that happen to " +
          "make the same call.",
      );
    }
    return `shared:${coordinationId}:${toolName}:${inputsHash}`;
  }

  let seqPart = "session_wide";
  let agentPart = "session_wide";

  if (scope === IdempotencyScope.AGENT_PRIVATE) {
    agentPart = agentId;
  } else if (scope === IdempotencyScope.STEP_LOCAL) {
    seqPart = String(stepSequence);
    agentPart = agentId;
  }

  return `${sessionId}:${workflowVersion}:${seqPart}:${agentPart}:${toolName}:${inputsHash}`;
}

/**
 * The key a session records a result it received under, at `sequence`.
 *
 * The record is leased like any step, so the engine arbitrates its position: a
 * record is refused at a position another lease has claimed, and a later lease is
 * refused at a position the record holds. The key names the session and the
 * position, so each session records its own copy, and the received result's key,
 * so two different results contending for one position are told apart. The same
 * record sent again finds its own completed entry. Matches the Python SDK, so
 * either can find the other's record.
 */
export function recordIdempotencyKey(
  sessionId: string,
  sequence: number,
  receivedKey: string,
): string {
  const digest = createHash("sha256").update(receivedKey, "utf8").digest("hex");
  return `${sessionId}:record:${sequence}:${digest}`;
}

/**
 * The key a session records a step's failure under, at `sequence`.
 *
 * Its own key, not the step's: completing the step's key with a failure would
 * answer every later call with that key from the cache, so a retry of the tool
 * would never run again. Leased like any step, so the engine arbitrates the
 * position, as for {@link recordIdempotencyKey}. Matches the Python SDK.
 */
export function failureIdempotencyKey(
  sessionId: string,
  sequence: number,
  stepName: string,
  stepKey: string,
): string {
  const digest = createHash("sha256").update(`${stepName}\n${stepKey}`, "utf8").digest("hex");
  return `${sessionId}:failure:${sequence}:${digest}`;
}
