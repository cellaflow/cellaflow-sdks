/** The gRPC status code of an engine call's error, as a number or a name. */
function codeOf(err: unknown): unknown {
  return (err as { code?: unknown } | undefined)?.code;
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
 * Status codes for a failure that may pass if the call is sent again. INTERNAL is
 * how the engine reports a failed storage write, which it does not retry itself,
 * and CANCELLED is the call being abandoned rather than refused.
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
 * fails the same way every time.
 */
export function isRetryable(err: unknown): boolean {
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
