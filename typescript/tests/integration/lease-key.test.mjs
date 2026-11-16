// A tool body that forwards its call's idempotency key, against a real engine:
// a holder killed after its request landed and before it recorded the result
// is taken over, and the new holder's request carries the same key, so a
// destination that deduplicates on it applies the effect once. Skipped unless
// CELLAFLOW_TARGET names an engine.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

import { IdempotencyScope, currentLease, durableTools, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET", timeout: 30_000 };
const dist = fileURLToPath(new URL("../../dist/index.js", import.meta.url));
const TTL_MS = 3000;

/**
 * A payments service that applies each Idempotency-Key once, and a request
 * without one every time.
 */
async function destination() {
  const applied = new Map();
  const requests = [];
  let unkeyed = 0;
  const server = createServer((req, res) => {
    const key = req.headers["idempotency-key"] ?? `unkeyed-${++unkeyed}`;
    requests.push(key);
    if (!applied.has(key)) applied.set(key, `refund-${applied.size + 1}`);
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ id: applied.get(key) }));
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { url: `http://127.0.0.1:${server.address().port}/refunds`, applied, requests, close: () => server.close() };
}

test("a holder killed after its request landed is taken over, and the destination applies the effect once", engine, async () => {
  const dest = await destination();
  const orderId = `order-${randomUUID().slice(0, 8)}`;
  const script = `
    import { IdempotencyScope, currentLease, durableTools, tool } from ${JSON.stringify(dist)};
    const refund = tool(async ({ orderId }) => {
      setInterval(() => {}, 1 << 30);
      const res = await fetch(${JSON.stringify(dest.url)}, currentLease().withIdempotencyKey({ method: "POST" }));
      await res.json();
      console.log("sent");
      await new Promise(() => {}); // killed here, before the result is recorded
    }, { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] });
    await durableTools("holder-${randomUUID().slice(0, 8)}",
      { target: ${JSON.stringify(target)}, coordinationId: ${JSON.stringify(orderId)}, ttlMs: ${TTL_MS} },
      () => refund({ orderId: ${JSON.stringify(orderId)} }));
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], { stdio: ["ignore", "pipe", "inherit"] });
  try {
    await new Promise((resolve, reject) => {
      child.stdout.on("data", (chunk) => {
        if (String(chunk).includes("sent")) resolve();
      });
      child.on("exit", (code) => reject(new Error(`the holder exited (${code}) before sending`)));
    });
    child.kill("SIGKILL");

    const refund = tool(
      async () => {
        const res = await fetch(dest.url, currentLease().withIdempotencyKey({ method: "POST" }));
        return (await res.json()).id;
      },
      { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"], ttlMs: TTL_MS },
    );
    const id = await durableTools(`taker-${randomUUID().slice(0, 8)}`, { target, coordinationId: orderId }, () =>
      refund({ orderId }),
    );
    // Two requests, one key (the call's digest), one refund: the taker got the
    // first one back.
    assert.equal(dest.requests.length, 2);
    assert.match(dest.requests[0], /^[0-9a-f]{64}$/);
    assert.equal(dest.requests[0], dest.requests[1]);
    assert.equal(dest.applied.size, 1);
    assert.equal(id, "refund-1");
  } finally {
    child.kill("SIGKILL");
    dest.close();
  }
});
