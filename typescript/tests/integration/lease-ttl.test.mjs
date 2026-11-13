// A tool's ttlMs against a real engine: a holder killed mid-call is taken over
// within its TTL, not the engine's 20 s default. Skipped unless CELLAFLOW_TARGET
// names an engine, for example:
//
//   docker run -d -p 50051:50051 -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
//     ghcr.io/cellaflow/cellaflow:0.3.0
//   CELLAFLOW_TARGET=localhost:50051 npm run test:integration
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { IdempotencyScope, durableTools, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET" };
const dist = fileURLToPath(new URL("../../dist/index.js", import.meta.url));

const TTL_MS = 3000;
// The engine asks for a renewal every quarter of the TTL.
const RENEW_EVERY_MS = TTL_MS / 4;

/**
 * Starts a process that takes the lease on `orderId`'s shared tool and then holds
 * it, renewing, until killed. The TTL comes from the block's `ttlMs`, so the
 * block default reaching the tool is what this measures. `ready` resolves once
 * the body is running, which is after the lease was acquired.
 */
function holder(orderId, coordinationId) {
  const script = `
    import { IdempotencyScope, durableTools, tool } from ${JSON.stringify(dist)};
    const charge = tool(async () => {
      // The heartbeat's timers do not hold the event loop open, so without this
      // the process would exit at the await below instead of holding the lease.
      setInterval(() => {}, 1 << 30);
      console.log("holding");
      await new Promise(() => {});
    }, { toolName: "charge_card", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] });
    await durableTools("holder-${randomUUID().slice(0, 8)}",
      { target: ${JSON.stringify(target)}, coordinationId: ${JSON.stringify(coordinationId)}, ttlMs: ${TTL_MS} },
      () => charge({ orderId: ${JSON.stringify(orderId)} }));
  `;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  const ready = new Promise((resolve, reject) => {
    child.stdout.on("data", (chunk) => {
      if (String(chunk).includes("holding")) resolve();
    });
    child.on("exit", (code) => reject(new Error(`the holder exited (${code}) before holding the lease`)));
  });
  return { child, ready };
}

/**
 * Kills a holder `holdMs` after it took the lease, then times how long another
 * caller waits before running the call itself.
 */
async function takeover(t, holdMs) {
  const orderId = `order-${randomUUID().slice(0, 8)}`;
  const coordinationId = orderId;
  const { child, ready } = holder(orderId, coordinationId);
  let killedAt;
  try {
    await ready;
    await new Promise((r) => setTimeout(r, holdMs));
    assert.equal(child.exitCode, null, "the holder exited before it was killed");
    assert.equal(child.signalCode, null, "the holder was stopped before it was killed");
  } finally {
    // Also when the holder never took the lease, so a failure cannot leave it running.
    child.kill("SIGKILL");
    killedAt = Date.now();
  }

  let ranAt = 0;
  const charge = tool(
    async () => {
      ranAt = Date.now();
      return "charged-here";
    },
    { toolName: "charge_card", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"], ttlMs: TTL_MS },
  );
  const result = await durableTools(`taker-${randomUUID().slice(0, 8)}`, { target, coordinationId }, () =>
    charge({ orderId }),
  );

  assert.equal(result, "charged-here");
  const waited = ranAt - killedAt;
  t.diagnostic(`took over ${waited} ms after the kill`);
  // The holder's lease lapses no later than a TTL after the kill, and a waiting
  // caller asks again about once a second.
  assert.ok(waited <= TTL_MS + 1000 + 500, `took over after ${waited} ms`);
  // And not while the lease was still live: the last renewal (or the
  // acquisition) came at most an interval before the kill.
  assert.ok(waited >= TTL_MS - 2 * RENEW_EVERY_MS, `took over after only ${waited} ms`);
}

// A TTL that did not reach the engine would leave the taker polling until the
// engine's default lapses, or its maximum lease lifetime: fail rather than hang.
const bounded = { ...engine, timeout: 30_000 };

test("a holder killed before its first renewal is taken over within its ttlMs", bounded, async (t) => {
  // Only the acquisition carried the TTL, so this is what proves it was sent.
  await takeover(t, 50);
});

test("a holder killed mid-call is taken over within its ttlMs", bounded, async (t) => {
  // Two TTLs: past the first, the holder can only still own the lease because
  // its renewals extended it.
  await takeover(t, 2 * TTL_MS);
});
