// Compiled with `tsc --noEmit` by `npm test`: how a caller catches a refusal
// because the org is over its allowance, so a missing export or a changed
// shape fails the build.
import { Code, ConnectError } from "@connectrpc/connect";
import { AllowanceExceededError, durableTools } from "../../dist/index.js";
import type { AllowanceLimit } from "../../dist/index.js";

/** True only when A and B are the same type, not merely assignable both ways. */
type Same<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

// `limit` names the two values engines send today, and still takes any other.
export const limitIsTheUnion: Same<
  AllowanceExceededError["limit"],
  "new_sessions_blocked" | "writes_blocked" | (string & {}) | undefined
> = true;
export const limitIsNotPlainString: Same<AllowanceExceededError["limit"], string | undefined> = false;
export const laterLimit: AllowanceLimit = "storage_blocked";

export async function resetsAt(threadId: string): Promise<Date | undefined> {
  try {
    await durableTools(threadId, async () => undefined);
    return undefined;
  } catch (err) {
    if (!(err instanceof AllowanceExceededError)) throw err;
    const subject: string = err.subject;
    const limit: string | undefined = err.limit;
    const writesBlocked: boolean = err.limit === "writes_blocked";
    const code: Code = err.code;
    const asConnectError: ConnectError = err;
    const cause: unknown = err.cause;
    void [subject, limit, writesBlocked, code, asConnectError, cause];
    return err.resetsAt;
  }
}
