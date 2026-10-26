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
 * Whether an engine call failed in a way that may pass if it is sent again. An
 * error carrying no status code did not come back from the engine, such as a
 * dropped connection, so it counts as retryable.
 */
export function isRetryable(err: unknown): boolean {
  const code = codeOf(err);
  return code === undefined || RETRYABLE_CODES.has(code);
}

/** The status code of a failed engine call, for messages. */
export function errorCode(err: unknown): string {
  return String(codeOf(err));
}
