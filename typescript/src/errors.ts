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

/** Status codes for a failure that may pass if the call is sent again. */
const RETRYABLE_CODES = new Set<unknown>([
  4, "deadline_exceeded",
  8, "resource_exhausted",
  10, "aborted",
  14, "unavailable",
]);

/**
 * Whether an engine call failed in a way that may pass if it is sent again. The
 * transport reports a dropped connection or a timeout with one of these codes. An
 * error with no status code was thrown before the call reached the engine, such as
 * a payload that does not serialize, and fails the same way every time.
 */
export function isRetryable(err: unknown): boolean {
  return RETRYABLE_CODES.has(codeOf(err));
}

/** Whether an engine call answered NOT_FOUND. */
export function isNotFound(err: unknown): boolean {
  const code = codeOf(err);
  return code === 5 || code === "not_found";
}

/** Whether an error came back from the engine with a status code. */
export function hasStatusCode(err: unknown): boolean {
  return codeOf(err) !== undefined;
}

/** Why an engine call failed, for messages: its status code, or the error itself. */
export function errorCode(err: unknown): string {
  return hasStatusCode(err) ? String(codeOf(err)) : String(err);
}
