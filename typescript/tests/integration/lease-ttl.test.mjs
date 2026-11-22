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

// Below what polling takes here (at least the 600 ms the mid-call kill leaves
// after a renewal), with room for a loaded CI engine.
const TAKEN_OVER_WITHIN_MS = 500;

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
 * the body is running, which is after the lease was acquired. The process
 * prints the time each grant or renewal was confirmed, and `confirmed` lists
 * them: the lease lapses no later than a TTL after the last.
 */
function holder(orderId, coordinationId) {
  const script = `
    import { CacheStatus, CellaflowClient, IdempotencyScope, durableTools, tool } from ${JSON.stringify(dist)};
    const renew = CellaflowClient.prototype.renewLease;
    CellaflowClient.prototype.renewLease = async function (...args) {
      console.log("sending " + Date.now());
      const resp = await renew.apply(this, args);
      if (resp.renewed) console.log("confirmed " + Date.now());
      return resp;
    };
    const check = CellaflowClient.prototype.checkIdempotencyCache;
    CellaflowClient.prototype.checkIdempotencyCache = async function (...args) {
      const resp = await check.apply(this, args);
      if (resp.status === CacheStatus.ACQUIRED) console.log("confirmed " + Date.now());
      return resp;
    };
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
  const confirmed = [];
  const sent = [];
  const listeners = new Set();
  let buffered = "";
  const ready = new Promise((resolve, reject) => {
    child.stdout.on("data", (chunk) => {
      buffered += String(chunk);
      const lines = buffered.split("\n");
      buffered = lines.pop();
      for (const line of lines) {
        if (line.startsWith("sending ")) sent.push(Number(line.slice("sending ".length)));
        if (line.startsWith("confirmed ")) {
          confirmed.push(Number(line.slice("confirmed ".length)));
          for (const listener of listeners) listener();
        }
        if (line === "holding") resolve();
      }
    });
    child.on("exit", (code) => reject(new Error(`the holder exited (${code}) before holding the lease`)));
  });
  const closed = new Promise((resolve) => child.stdout.on("close", resolve));
  const nextConfirmation = () =>
    new Promise((resolve) => {
      const listener = () => {
        listeners.delete(listener);
        resolve(confirmed[confirmed.length - 1]);
      };
      listeners.add(listener);
    });
  return { child, ready, confirmed, sent, closed, nextConfirmation };
}

/**
 * Kills a holder `holdMs` after it took the lease (and, with `afterRenewalMs`,
 * that long after its next confirmed renewal), then times how long another
 * caller waits before running the call itself.
 */
async function takeover(t, holdMs, afterRenewalMs) {
  const orderId = `order-${randomUUID().slice(0, 8)}`;
  const coordinationId = orderId;
  const { child, ready, confirmed, sent, closed, nextConfirmation } = holder(orderId, coordinationId);
  let killedAt;
  try {
    await ready;
    await new Promise((r) => setTimeout(r, holdMs));
    if (afterRenewalMs !== undefined) {
      await nextConfirmation();
      await new Promise((r) => setTimeout(r, afterRenewalMs));
    }
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
  await closed;
  const waited = ranAt - killedAt;
  t.diagnostic(`took over ${waited} ms after the kill`);
  // The lease lapses no later than a TTL after the last confirmation the
  // holder printed before it was killed. A caller waiting in the engine is
  // granted it within the engine's 100 ms check of that, plus the grant and the
  // renewal that confirms a late grant. One polling once a second, started at
  // the kill, lands up to a second after the lapse.
  const lastConfirmed = Math.max(...confirmed.filter((at) => at <= killedAt));
  // A renewal sent but not confirmed before the kill may still have reached the
  // engine, as late as the kill: the lease is then counted from the kill.
  const unconfirmed = sent.some((at) => at > lastConfirmed && at <= killedAt);
  const afterLapse = ranAt - ((unconfirmed ? killedAt : lastConfirmed) + TTL_MS);
  t.diagnostic(`took over ${afterLapse} ms after the latest possible lapse`);
  assert.ok(afterLapse <= TAKEN_OVER_WITHIN_MS, `took over ${afterLapse} ms after the lapse`);
  // The holder's lease lapses no later than a TTL after the kill, and the
  // waiting caller is granted it by the engine as soon as the engine sees that
  // (it looks every 100 ms), not on its next poll a second later.
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
  // its renewals extended it. Killed 600 ms after a renewal, so the lease lapses
  // 600 ms before a caller polling once a second from the kill would ask.
  await takeover(t, 2 * TTL_MS, 600);
});
