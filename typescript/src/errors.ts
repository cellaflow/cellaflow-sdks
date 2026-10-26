/**
 * Whether an engine call was refused with FAILED_PRECONDITION: a lease at a position
 * already owned by different inputs, or a commit at a position already taken.
 */
export function isFailedPrecondition(err: unknown): boolean {
  const code = (err as { code?: unknown } | undefined)?.code;
  return code === 9 || code === "failed_precondition";
}
