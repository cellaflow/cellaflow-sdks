import { Code, ConnectError } from "@connectrpc/connect";
import { ErrorInfoSchema, QuotaFailureSchema } from "./google/rpc/error_details_pb.js";

/** The gRPC status code of an engine call's error, as a number or a name. */
function codeOf(err: unknown): unknown {
  return (err as { code?: unknown } | undefined)?.code;
}

/**
 * Raised when the engine refuses a call because the Cellaflow org it runs
 * under is over its allowance, such as the free step allowance of a hosted
 * engine.
 *
 * It is final. The engine also answers RESOURCE_EXHAUSTED when a session's
 * mailbox is full, which passes, so the SDK asks again; this refusal lasts
 * until the allowance resets or the plan changes, so nothing in the SDK asks
 * again, waits for it, or records it as the failure of the step it stopped.
 *
 * It is a `ConnectError` with code `Code.ResourceExhausted`, the refusal it
 * stands for, so code that checks either still matches it. `cause` is the
 * engine's error as it arrived, and `details` and `metadata` are that error's.
 */
export class AllowanceExceededError extends ConnectError {
  /** What the org is over its allowance of, as the engine names it, such as `cellaflow.steps`. */
  declare readonly subject: string;
  /** When the allowance resets. Absent when the engine did not say, or said it in a form that does not parse. */
  declare readonly resetsAt?: Date;
  /**
   * What the engine stops while the org is over its allowance, as it names it:
   * `new_sessions_blocked` or `writes_blocked`. Absent when it did not say.
   */
  declare readonly limit?: string;

  /**
   * @param init.message The engine's message, kept at the end of this error's.
   * @param init.cause The engine's error, as it arrived.
   */
  constructor(init: {
    subject: string;
    resetsAt?: Date;
    limit?: string;
    message?: string;
    cause?: unknown;
    metadata?: ConstructorParameters<typeof ConnectError>[2];
    details?: ConnectError["details"];
  }) {
    super(
      allowanceMessage(init.subject, init.resetsAt, init.message ?? ""),
      Code.ResourceExhausted,
      init.metadata,
      undefined,
      init.cause,
    );
    // Set as Connect sets a received error's: its constructor takes only
    // details to send.
    if (init.details) this.details = init.details;
    this.subject = init.subject;
    if (init.resetsAt) this.resetsAt = init.resetsAt;
    if (init.limit) this.limit = init.limit;
    // The name stays ConnectError's, unlike this SDK's other errors: Connect
    // recognises its errors by name as well as by class, so another name would
    // make `instanceof ConnectError` false, and Connect would wrap this error
    // in an unknown one on its way out of the transport. Node's inspector still
    // shows the class.
  }

  /**
   * Only an instance of this class. The check inherited from ConnectError
   * accepts anything shaped like a ConnectError, so that one from another copy
   * of the package matches too, and so would take every ConnectError for this.
   */
  static [Symbol.hasInstance](value: unknown): boolean {
    return Function.prototype[Symbol.hasInstance].call(this, value);
  }
}

/** The plain words of an {@link AllowanceExceededError}. */
function allowanceMessage(subject: string, resetsAt: Date | undefined, engineMessage: string): string {
  let text = `The Cellaflow org is over its allowance for ${subject}, so the engine refused this call.`;
  if (resetsAt) text += ` The allowance resets at ${resetsAt.toISOString().replace(".000Z", "Z")}.`;
  if (engineMessage) text += ` The engine said: ${engineMessage}`;
  return text;
}

/** A QuotaFailure subject with this prefix is what makes a refusal an allowance refusal. */
const ALLOWANCE_SUBJECT_PREFIX = "cellaflow.";
/** The ErrorInfo that says when the allowance resets and what it stops. */
const ALLOWANCE_DOMAIN = "cellaflow.com";
const ALLOWANCE_REASON = "ALLOWANCE_EXCEEDED";

/**
 * The allowance refusal `err` is, or undefined for any other error.
 *
 * An allowance refusal is RESOURCE_EXHAUSTED whose status details hold a
 * `google.rpc.QuotaFailure` with a violation whose subject starts with
 * `cellaflow.`; the first such violation names it. A `google.rpc.ErrorInfo`
 * from domain `cellaflow.com` with reason `ALLOWANCE_EXCEEDED`, if there is
 * one, says when it resets (`resets_at`) and what it stops (`limit`). Details
 * are matched by their type's name, as Connect's `findDetails` does.
 *
 * Never throws. Anything else, including details that do not decode, is not an
 * allowance refusal, so it is the RESOURCE_EXHAUSTED it always was, and an
 * ErrorInfo that is missing, from another domain, or with a `resets_at` that
 * does not parse leaves `resetsAt` and `limit` unknown.
 */
export function allowanceRefusal(err: unknown): AllowanceExceededError | undefined {
  try {
    if (err instanceof AllowanceExceededError) return err;
    if (!(err instanceof ConnectError) || err.code !== Code.ResourceExhausted) return undefined;
    const violation = err
      .findDetails(QuotaFailureSchema)
      .flatMap((failure) => failure.violations)
      .find((v) => v.subject.startsWith(ALLOWANCE_SUBJECT_PREFIX));
    if (!violation) return undefined;
    const info = err
      .findDetails(ErrorInfoSchema)
      .find((i) => i.domain === ALLOWANCE_DOMAIN && i.reason === ALLOWANCE_REASON);
    return new AllowanceExceededError({
      subject: violation.subject,
      resetsAt: parseTimestamp(info?.metadata.resets_at),
      limit: info?.metadata.limit || undefined,
      message: err.rawMessage,
      cause: err,
      metadata: err.metadata,
      details: err.details,
    });
  } catch {
    return undefined;
  }
}

/** An RFC 3339 timestamp: date, `T`, time, optional fraction, and `Z` or an offset. */
const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/;

/**
 * An RFC 3339 timestamp as a Date, or undefined if it is not one. Stricter than
 * `Date.parse`, which also reads other formats, some in local time, and rolls
 * an impossible date such as February 30 over into the next month.
 */
function parseTimestamp(value: string | undefined): Date | undefined {
  const m = value === undefined ? null : RFC3339.exec(value);
  if (!m) return undefined;
  const [year, month, day, hour, minute, second] = m.slice(1, 7).map(Number);
  const offsetHours = Number(m[9] ?? 0);
  const offsetMinutes = Number(m[10] ?? 0);
  if (offsetHours > 23 || offsetMinutes > 59) return undefined;
  const at = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  // Date.UTC rolls a field out of range over into the next one, so a time
  // that does not read back as written was not a real one. A year before 100
  // reads back as 19xx, so it is refused too.
  if (
    at.getUTCFullYear() !== year ||
    at.getUTCMonth() !== month - 1 ||
    at.getUTCDate() !== day ||
    at.getUTCHours() !== hour ||
    at.getUTCMinutes() !== minute ||
    at.getUTCSeconds() !== second
  ) {
    return undefined;
  }
  const millis = m[7] ? Number(m[7].slice(0, 3).padEnd(3, "0")) : 0;
  const offsetMs = (m[8] === "-" ? -1 : 1) * (offsetHours * 60 + offsetMinutes) * 60_000;
  return new Date(at.getTime() + millis - offsetMs);
}

/**
 * Whether an engine call was refused with FAILED_PRECONDITION: a lease at a position
 * already owned by different inputs, or a commit at a position already taken.
 *
 * Connect and gRPC surface `FAILED_PRECONDITION` differently depending on
 * transport, so match on the code rather than the class.
 */
export function isFailedPrecondition(err: unknown): boolean {
  const code = codeOf(err);
  return code === 9 || code === "failed_precondition";
}

/**
 * Whether a commit was refused because its fencing token is stale: the lease it
 * was made under has been taken over. The engine reports it as
 * FAILED_PRECONDITION "Fenced out: stale lease", which shares its code with a
 * position already taken, so the message tells them apart.
 */
export function isStaleLease(err: unknown): boolean {
  if (!isFailedPrecondition(err)) return false;
  const message = (err as { message?: unknown } | undefined)?.message;
  return typeof message === "string" && /stale lease/i.test(message);
}

/**
 * Whether a commit was refused because its idempotency key is already committed
 * at a different position: the key's work was recorded by another worker.
 */
export function isCommittedElsewhere(err: unknown): boolean {
  if (!isFailedPrecondition(err)) return false;
  const message = (err as { message?: unknown } | undefined)?.message;
  return typeof message === "string" && /already committed at sequence/i.test(message);
}

/**
 * Whether a lease was refused because its position is already committed. No
 * lease's claim on the position explains that refusal, so it never passes by
 * waiting.
 */
export function isPositionCommitted(err: unknown): boolean {
  if (!isFailedPrecondition(err)) return false;
  const message = (err as { message?: unknown } | undefined)?.message;
  // Anchored on the engine's own wording, so an id or key quoted in another
  // refusal cannot match it.
  return typeof message === "string" && /' is already committed; the session is at \d+/.test(message);
}

/**
 * Status codes for a failure that may pass if the call is sent again. INTERNAL is
 * how the engine reports a failed storage write, which it does not retry itself,
 * and CANCELLED is the call being abandoned rather than refused.
 * RESOURCE_EXHAUSTED is a session's full mailbox, unless it is an
 * {@link AllowanceExceededError}, which {@link isRetryable} checks first.
 */
const RETRYABLE_CODES = new Set<unknown>([
  1, "canceled", "cancelled",
  4, "deadline_exceeded",
  8, "resource_exhausted",
  10, "aborted",
  13, "internal",
  14, "unavailable",
]);

/** gRPC status code names, as Connect spells them. */
const STATUS_NAMES = new Set<unknown>([
  "canceled", "cancelled", "unknown", "invalid_argument", "deadline_exceeded",
  "not_found", "already_exists", "permission_denied", "resource_exhausted",
  "failed_precondition", "aborted", "out_of_range", "unimplemented", "internal",
  "unavailable", "data_loss", "unauthenticated",
]);

/**
 * Whether an error carries a gRPC status code. Node's own network errors also
 * have a `code`, such as `"ECONNRESET"`, but that is the transport failing, not
 * the engine answering.
 */
export function hasStatusCode(err: unknown): boolean {
  const code = codeOf(err);
  return (
    (typeof code === "number" && Number.isInteger(code) && code >= 0 && code <= 16) ||
    STATUS_NAMES.has(code)
  );
}

/**
 * Node's codes for a network failure: the connection was refused, reset or timed
 * out, or the host could not be reached or resolved. Its other codes, such as
 * `ERR_INVALID_ARG_TYPE`, fail the same way every time.
 */
const NETWORK_ERROR_CODES = new Set<unknown>([
  "ECONNREFUSED", "ECONNRESET", "ECONNABORTED", "ETIMEDOUT", "EPIPE",
  "EHOSTUNREACH", "ENETUNREACH", "ENETDOWN", "ENOTFOUND", "EAI_AGAIN",
]);

/**
 * Whether an engine call failed in a way that may pass if it is sent again: one
 * of the retryable status codes, or a network failure Node reports with its own
 * code. Anything else, including an error with no code at all, was thrown before
 * the call reached the engine, such as a payload that does not serialize, and
 * fails the same way every time. So does an {@link AllowanceExceededError},
 * until the allowance resets, though its code is RESOURCE_EXHAUSTED: it is the
 * engine's answer, not a call that went unanswered.
 */
export function isRetryable(err: unknown): boolean {
  if (err instanceof AllowanceExceededError) return false;
  const code = codeOf(err);
  if (hasStatusCode(err)) return RETRYABLE_CODES.has(code);
  return NETWORK_ERROR_CODES.has(code);
}

/** Whether an engine call answered NOT_FOUND. */
export function isNotFound(err: unknown): boolean {
  const code = codeOf(err);
  return code === 5 || code === "not_found";
}

/** Why an engine call failed, for messages: its status code, or the error itself. */
export function errorCode(err: unknown): string {
  return hasStatusCode(err) ? String(codeOf(err)) : String(err);
}
