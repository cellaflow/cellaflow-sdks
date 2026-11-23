// Compiled with `tsc --noEmit` by `npm test`: how a caller reads who holds a
// lease it was refused, so a missing export or a changed shape fails the build.
import { LeaseNotAcquired, executionLease } from "../../dist/index.js";
import type { LeaseHolderInfo } from "../../dist/index.js";

export async function takenBy(key: string): Promise<string | undefined> {
  try {
    await executionLease(key, { workerId: "agent-b" }, async () => undefined);
    return undefined;
  } catch (err) {
    if (!(err instanceof LeaseNotAcquired)) throw err;
    const holder: LeaseHolderInfo | undefined = err.holder;
    const engineHolderId: string | undefined = holder?.engineHolderId;
    const expires: number | undefined = holder?.expiresAtMs;
    const acquired: number | undefined = holder?.acquiredAtMs;
    const ceiling: number | undefined = holder?.hardExpiresAtMs;
    void [engineHolderId, expires, acquired, ceiling];
    return holder?.workerId;
  }
}
