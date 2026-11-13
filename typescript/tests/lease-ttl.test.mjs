// The lease TTL a tool asks for: what is sent on acquisition and on every
// renewal, where a default comes from, and what is refused. Runs against dist/
// with a scripted engine client, so no engine is needed: `npm test`.
import { test } from "node:test";
import assert from "node:assert/strict";

import {
  CacheStatus,
  IdempotencyScope,
  MIN_LEASE_TTL_MS,
  WorkflowContext,
  durableTools,
  tool,
} from "../dist/index.js";
import { serialize } from "../dist/serialization.js";
import { runWithContext } from "../dist/context.js";

/**
 * Records every acquisition and renewal. Acquisitions are answered from
 * `answers` in order, then ACQUIRED with a short heartbeat interval so a body
 * that sleeps sees several renewals.
 */
function recordingClient(answers = [], { interval = 20n, failRelease = false } = {}) {
  const acquires = [];
  const renewals = [];
  return {
    acquires,
    renewals,
    async checkIdempotencyCache(agentId, key, waitTimeoutMs, leaseTtlMs, sessionId, sequence) {
      acquires.push({ key, leaseTtlMs, sequence });
      const next = answers.shift();
      if (next) return next;
      return {
        status: CacheStatus.ACQUIRED,
        fencingToken: 7n,
        heartbeatIntervalMs: typeof interval === "function" ? interval(leaseTtlMs) : interval,
      };
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      renewals.push({ key, extendMs });
      return { renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) };
    },
    async commitStep() {
      return {};
    },
    async releaseLease() {
      if (failRelease) throw Object.assign(new Error("engine unavailable"), { code: 14 });
      return { released: true };
    },
    close() {},
  };
}

const session = (client, extra = {}) =>
  new WorkflowContext({
    client,
    sessionId: "s-ttl",
    workflowVersion: "1.0.0",
    coordinationId: "order-4417",
    historyKnown: true,
    ...extra,
  });

const slow = (name, options = {}, ms = 120) =>
  tool(
    async () => {
      // Long enough for several 20 ms heartbeats.
      await new Promise((r) => setTimeout(r, ms));
      return name;
    },
    { toolName: name, ...options },
  );

test("a tool's ttlMs is sent on acquisition and asked for on every renewal", async () => {
  const client = recordingClient();
  const charge = slow("charge_card", { ttlMs: 6000 });
  await runWithContext(session(client), () => charge());
  assert.equal(client.acquires[0].leaseTtlMs, 6000);
  assert.ok(client.renewals.length >= 2, `expected several renewals, saw ${client.renewals.length}`);
  assert.ok(client.renewals.every((r) => r.extendMs === 6000), JSON.stringify(client.renewals));
});

test("without ttlMs, the engine's default is asked for, as before", async () => {
  const client = recordingClient();
  const charge = slow("charge_card");
  await runWithContext(session(client), () => charge());
  assert.equal(client.acquires[0].leaseTtlMs, undefined);
  // The heartbeat infers the engine's TTL from its interval: four of them.
  assert.ok(client.renewals.length >= 2, `expected several renewals, saw ${client.renewals.length}`);
  assert.ok(client.renewals.every((r) => r.extendMs === 80), JSON.stringify(client.renewals));
});

test("the session's ttlMs applies to every tool, and a tool's own overrides it", async () => {
  const client = recordingClient();
  const plain = slow("plain");
  const own = slow("own", { ttlMs: 3000 });
  await runWithContext(session(client, { leaseTtlMs: 9000 }), async () => {
    await plain();
    await own();
  });
  assert.deepEqual(
    client.acquires.map((a) => a.leaseTtlMs),
    [9000, 3000],
  );
  const byKey = (i) => client.renewals.filter((r) => r.key === client.acquires[i].key);
  assert.ok(byKey(0).length > 0 && byKey(0).every((r) => r.extendMs === 9000));
  assert.ok(byKey(1).length > 0 && byKey(1).every((r) => r.extendMs === 3000));
});

test("a recorded hit's own lease asks for the TTL of the call it records", async () => {
  // The first call is answered from another session's commit, so its result is
  // recorded at its position before the next call claims one.
  const client = recordingClient([
    {
      status: CacheStatus.HIT,
      currentSequence: 0n,
      cachedResult: { outputPayload: serialize({ result: "ORD-1" }) },
    },
  ]);
  const placeOrder = tool(async () => "placed-here", {
    toolName: "place_order",
    scope: IdempotencyScope.SHARED,
    sharedOn: ["orderId"],
    ttlMs: 4000,
  });
  const next = slow("next", { ttlMs: 2000 });
  await runWithContext(session(client), async () => {
    await placeOrder({ orderId: "4417" });
    await next();
  });
  const record = client.acquires.find((a) => a.key.includes(":record:"));
  assert.ok(record, JSON.stringify(client.acquires));
  assert.equal(record.leaseTtlMs, 4000);
});

test("a recorded failure's own lease asks for the TTL of the call that failed", async () => {
  const client = recordingClient();
  const failing = tool(
    async () => {
      throw new Error("card declined");
    },
    { toolName: "charge_card", ttlMs: 5000 },
  );
  const next = slow("next");
  await runWithContext(session(client), async () => {
    await assert.rejects(failing(), /card declined/);
    await next();
  });
  const record = client.acquires.find((a) => a.key.includes(":failure:"));
  assert.ok(record, JSON.stringify(client.acquires));
  assert.equal(record.leaseTtlMs, 5000);
});

test("a ttlMs the SDK will not ask for is refused when the tool is made", () => {
  for (const bad of [0, 999, 1500.5, -1000, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => tool(async () => 1, { toolName: "charge_card", ttlMs: bad }),
      (err) => err instanceof RangeError && /tool 'charge_card'.*ttlMs/.test(err.message),
      String(bad),
    );
  }
  // A value read from the environment is a string: say so, not "got 5000".
  assert.throws(
    () => tool(async () => 1, { toolName: "charge_card", ttlMs: "5000" }),
    (err) => err instanceof RangeError && /got string "5000"/.test(err.message),
  );
  assert.equal(MIN_LEASE_TTL_MS, 1000);
  // The floor itself, and an unset value, are accepted.
  tool(async () => 1, { toolName: "a", ttlMs: MIN_LEASE_TTL_MS });
  tool(async () => 1, { toolName: "b", ttlMs: undefined });
  tool(async () => 1, { toolName: "c", ttlMs: null });
});

test("a session ttlMs the SDK will not ask for is refused before anything is opened", async () => {
  assert.throws(() => session(recordingClient(), { leaseTtlMs: 5 }), /WorkflowContext: leaseTtlMs/);
  // Nothing listens on this port: a refusal that came after connecting would be
  // a connection error, not this one.
  await assert.rejects(
    durableTools("thread-1", { target: "127.0.0.1:9", ttlMs: 10 }, async () => {}),
    (err) => err instanceof RangeError && /durableTools.*ttlMs/.test(err.message),
  );
});

test("a call that waits on another holder asks for its TTL again on every try", async () => {
  const client = recordingClient([{ status: CacheStatus.IN_PROGRESS, retryAfterMs: 1n }]);
  const charge = slow("charge_card", { ttlMs: 6000 });
  await runWithContext(session(client), () => charge());
  assert.deepEqual(
    client.acquires.map((a) => a.leaseTtlMs),
    [6000, 6000],
  );
});

test("a call under its own coordination id keeps the tool's TTL", async () => {
  const client = recordingClient();
  const charge = tool(async () => "charged", {
    toolName: "charge_card",
    scope: IdempotencyScope.SHARED,
    sharedOn: ["orderId"],
    ttlMs: 6000,
  });
  await runWithContext(session(client), () => charge.withCoordinationId("customer-88")({ orderId: "4417" }));
  assert.equal(client.acquires[0].leaseTtlMs, 6000);
});

test("a failed call's lease that could not be released lapses a TTL after its heartbeat stopped", async () => {
  const client = recordingClient([], { failRelease: true });
  const failing = tool(
    async () => {
      throw new Error("card declined");
    },
    { toolName: "charge_card", ttlMs: 6000 },
  );
  const ctx = session(client);
  await runWithContext(ctx, () => assert.rejects(failing(), /card declined/));
  // Held for the record at its position: the TTL, plus a second for the
  // engine's clock.
  assert.equal(ctx.pendingHit?.unreleased?.lapsesInMs, 7000);
});

test("renewals come at least every quarter of the TTL, even if the engine suggests fewer", async () => {
  // An engine that suggests a minute between renewals for a one-second lease
  // would let it lapse while the holder is healthy.
  const client = recordingClient([], { interval: 60_000n });
  const charge = slow("charge_card", { ttlMs: 1000 }, 700);
  await runWithContext(session(client), () => charge());
  assert.ok(client.renewals.length >= 2, `renewed ${client.renewals.length} times in 700 ms`);
});

test("a TTL past what timers can hold does not make the heartbeat renew in a loop", async () => {
  const warnings = [];
  const onWarning = (w) => warnings.push(w.name);
  process.on("warning", onWarning);
  try {
    // The engine suggests a quarter of the TTL, which overflows setInterval.
    const client = recordingClient([], { interval: (ttl) => BigInt(Math.floor(Number(ttl) / 4)) });
    const charge = slow("charge_card", { ttlMs: 2 ** 40 }, 100);
    await runWithContext(session(client), () => charge());
    assert.equal(client.renewals.length, 0, `renewed ${client.renewals.length} times in 100 ms`);
    await new Promise((r) => setImmediate(r));
    assert.ok(!warnings.includes("TimeoutOverflowWarning"), warnings.join(", "));
  } finally {
    process.off("warning", onWarning);
  }
});

test("an engine that suggests no interval gets renewals every quarter of the TTL, not a tight loop", async () => {
  const client = recordingClient([], { interval: 0n });
  const charge = slow("charge_card", { ttlMs: 1000 }, 700);
  await runWithContext(session(client), () => charge());
  const n = client.renewals.length;
  assert.ok(n >= 2 && n <= 3, `renewed ${n} times in 700 ms`);
});
