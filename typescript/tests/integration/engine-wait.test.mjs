// Many callers waiting on one shared call against a real engine: the engine
// holds each request until the holder commits, so waiting costs about one
// request per caller, not one per caller per second. Skipped unless
// CELLAFLOW_TARGET names an engine, for example:
//
//   docker run -d -p 50051:50051 -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
//     ghcr.io/cellaflow/cellaflow:0.3.0
//   CELLAFLOW_TARGET=localhost:50051 npm run test:integration
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { CellaflowClient, IdempotencyScope, durableTools, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET", timeout: 60_000 };

const WAITERS = 100;
// Long enough that a caller polling once a second would ask three times or more.
const HOLD_MS = 3000;

test("100 callers on one shared call all get the holder's result, asking the engine about once each", engine, async (t) => {
  const orderId = `order-${randomUUID().slice(0, 8)}`;
  const coordinationId = orderId;

  // Count the requests that arbitrate the call's own key. The lease that
  // records a received result in each caller's session has a key of its own.
  const asked = [];
  const original = CellaflowClient.prototype.checkIdempotencyCache;
  CellaflowClient.prototype.checkIdempotencyCache = function (agentId, key, ...rest) {
    if (key.startsWith("shared:")) asked.push(key);
    return original.call(this, agentId, key, ...rest);
  };

  let runs = 0;
  let holding;
  const held = new Promise((resolve) => (holding = resolve));
  let finish;
  const finished = new Promise((resolve) => (finish = resolve));
  const charge = tool(
    async () => {
      runs += 1;
      holding();
      await finished;
      return `charged-by-${runs}`;
    },
    { toolName: "charge_card", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] },
  );
  const call = (name) => durableTools(`${name}-${randomUUID().slice(0, 8)}`, { target, coordinationId }, () => charge({ orderId }));

  try {
    const holder = call("holder");
    await held;
    const askedBefore = asked.length;
    const waiters = Array.from({ length: WAITERS }, (_, i) => call(`waiter-${i}`));
    await new Promise((r) => setTimeout(r, HOLD_MS));
    finish();
    const results = await Promise.all([holder, ...waiters]);

    assert.equal(runs, 1, "the body ran more than once");
    assert.deepEqual(new Set(results), new Set(["charged-by-1"]));
    const waiting = asked.length - askedBefore;
    t.diagnostic(`${WAITERS} waiters made ${waiting} requests during a ${HOLD_MS} ms hold`);
    // Polling once a second would be at least three per waiter. Waiting in the
    // engine is one, answered when the holder commits; the slack allows a few
    // that started before the engine held them.
    assert.ok(waiting <= WAITERS * 1.5, `${WAITERS} waiters made ${waiting} requests`);
  } finally {
    finish();
    CellaflowClient.prototype.checkIdempotencyCache = original;
  }
});
