// Compiled with `tsc --noEmit` by `npm test`: how a caller catches a refusal
// because the org is over its allowance, so a missing export or a changed
// shape fails the build.
import { Code, ConnectError } from "@connectrpc/connect";
import { AllowanceExceededError, durableTools } from "../../dist/index.js";

export async function resetsAt(threadId: string): Promise<Date | undefined> {
  try {
    await durableTools(threadId, async () => undefined);
    return undefined;
  } catch (err) {
    if (!(err instanceof AllowanceExceededError)) throw err;
    const subject: string = err.subject;
    const limit: string | undefined = err.limit;
    const code: Code = err.code;
    const asConnectError: ConnectError = err;
    const cause: unknown = err.cause;
    void [subject, limit, code, asConnectError, cause];
    return err.resetsAt;
  }
}
