// currentLease().check() inside a tool body, against a real engine: a holder
// frozen after taking the lease, and resumed after another caller took the call
// over, does not send. Skipped unless CELLAFLOW_TARGET names an engine, for
// example:
//
//   docker run -d -p 50051:50051 -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
//     ghcr.io/cellaflow/cellaflow:0.3.0
//   CELLAFLOW_TARGET=localhost:50051 npm run test:integration
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { IdempotencyScope, durableTools, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET", timeout: 30_000 };
const dist = fileURLToPath(new URL("../../dist/index.js", import.meta.url));

const TTL_MS = 3000;

/**
 * Starts a process that takes the lease on `orderId`'s refund, then waits for
 * `goFile` before it would send: checking its lease first, as a tool body
 * should before anything irreversible. Prints what happened.
 */
function holder(orderId, goFile) {
  const script = `
    import { existsSync } from "node:fs";
    import { IdempotencyScope, currentLease, durableTools, tool } from ${JSON.stringify(dist)};
    const refund = tool(async ({ orderId }) => {
      // The heartbeat's timers do not hold the event loop open.
      setInterval(() => {}, 1 << 30);
      console.log("holding");
      while (!existsSync(${JSON.stringify(goFile)})) await new Promise((r) => setTimeout(r, 20));
      currentLease().check();
      console.log("sent");
      return "refunded-by-holder";
    }, { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] });
    try {
      await durableTools("holder-${randomUUID().slice(0, 8)}",
        { target: ${JSON.stringify(target)}, coordinationId: ${JSON.stringify(orderId)}, ttlMs: ${TTL_MS} },
        () => refund({ orderId: ${JSON.stringify(orderId)} }));
      console.log("outcome:returned");
    } catch (err) {
      console.log("outcome:" + err.name);
    }
    process.exit(0);
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  let out = "";
  const exited = new Promise((resolve) => child.on("exit", resolve));
  const ready = new Promise((resolve, reject) => {
    child.stdout.on("data", (chunk) => {
      out += String(chunk);
      if (out.includes("holding")) resolve();
    });
    child.on("exit", (code) => reject(new Error(`the holder exited (${code}) before holding the lease`)));
  });
  return { child, ready, exited, output: () => out };
}

test("a holder frozen past its lease and resumed after a takeover does not send", engine, async () => {
  const orderId = `order-${randomUUID().slice(0, 8)}`;
  const goFile = join(mkdtempSync(join(tmpdir(), "cellaflow-")), "go");
  const h = holder(orderId, goFile);
  try {
    await h.ready;
    h.child.kill("SIGSTOP");

    // Another caller takes the call over once the frozen holder's lease lapses.
    let runs = 0;
    const refund = tool(
      async () => {
        runs += 1;
        return "refunded-by-taker";
      },
      { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"], ttlMs: TTL_MS },
    );
    const taken = await durableTools(`taker-${randomUUID().slice(0, 8)}`, { target, coordinationId: orderId }, () =>
      refund({ orderId }),
    );
    assert.equal(taken, "refunded-by-taker");
    assert.equal(runs, 1);

    // The holder wakes up and reaches the point where it would send.
    writeFileSync(goFile, "");
    h.child.kill("SIGCONT");
    await h.exited;
    const out = h.output();
    assert.ok(!out.includes("sent"), out);
    assert.ok(out.includes("outcome:LeaseLostError"), out);
  } finally {
    h.child.kill("SIGKILL");
  }
});
