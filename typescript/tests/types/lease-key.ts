// Compiled with `tsc --noEmit` by `npm test`: how a caller writes the helpers
// in TypeScript, so a signature that rejects ordinary use fails the build.
import { currentLease, tool } from "../../dist/index.js";

export const refund = tool(
  async ({ order }: { order: string }) => {
    const lease = currentLease();
    const body = JSON.stringify({ order });
    const response = await fetch("https://payments.example/refunds", lease.withIdempotencyKey({ method: "POST", body }));
    await fetch("https://payments.example/refunds", lease.withIdempotencyKey());
    await fetch(
      "https://payments.example/refunds",
      lease.withIdempotencyKey(
        { method: "POST", body, headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(10_000) },
        { raw: true, header: "PayPal-Request-Id" },
      ),
    );
    const init: RequestInit = { method: "POST" };
    const sent = lease.withIdempotencyKey(init);
    const method: string | undefined = sent.method;
    const digest: string = lease.keyDigest;
    const key: string = lease.key;
    return [response.status, method, digest, key];
  },
  { toolName: "refund" },
);
