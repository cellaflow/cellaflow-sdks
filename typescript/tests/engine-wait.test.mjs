// How a tool call waits behind another worker's lease: in the engine, not by
// polling, and with the lease it is granted after a wait counted from when it
// was granted. Runs against dist/ with a scripted engine client: `npm test`.
import { test } from "node:test";
import assert from "node:assert/strict";

import { CacheStatus, WorkflowContext, currentLease, tool } from "../dist/index.js";
import { runWithContext } from "../dist/context.js";
import { ENGINE_WAIT_MS } from "../dist/wait.js";

/**
 * Moves performance.now() and Date.now() forward by whatever `advance` adds,
 * so a scripted engine can answer as if it had waited without the test
 * waiting. Returns the advance function and a restore.
 */
function fakeClocks() {
  const perf = performance.now.bind(performance);
  const wall = Date.now;
  let offset = 0;
  // Defined rather than assigned: on some Node versions performance.now is an
  // inherited read-only property, which an own property can still shadow.
  const own = Object.getOwnPropertyDescriptor(performance, "now");
  Object.defineProperty(performance, "now", { value: () => perf() + offset, configurable: true, writable: true });
  Date.now = () => wall() + offset;
  return {
    advance: (ms) => {
      offset += ms;
    },
    restore: () => {
      if (own) Object.defineProperty(performance, "now", own);
      else delete performance.now;
      Date.now = wall;
    },
  };
}

/**
 * Answers acquisitions from `answers` in order. An answer may carry `waited`,
 * the time the engine spent waiting before it, applied to the clocks.
 */
function scriptedClient(answers, { clocks, failRenewalsFor } = {}) {
  const acquires = [];
  const renewals = [];
  const releases = [];
  const order = [];
  return {
    acquires,
    renewals,
    releases,
    order,
    async checkIdempotencyCache(agentId, key, waitTimeoutMs, leaseTtlMs, sessionId, sequence) {
      acquires.push({ key, waitTimeoutMs, leaseTtlMs, sequence });
      order.push("acquire");
      const next = answers.shift();
      if (!next) throw new Error("no scripted answer left");
      const { waited, ...answer } = next;
      if (waited) clocks.advance(waited);
      return answer;
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      renewals.push({ fencingToken, extendMs });
      order.push(`renew:${fencingToken}`);
      if (failRenewalsFor === Number(fencingToken)) {
        throw Object.assign(new Error("engine unavailable"), { code: 14 });
      }
      return { renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) };
    },
    async commitStep() {
      order.push("commit");
      return {};
    },
    async releaseLease(agentId, key, fencingToken, reason) {
      releases.push({ fencingToken, reason });
      order.push(`release:${fencingToken}`);
      return { released: true };
    },
    close() {},
  };
}

const session = (client) =>
  new WorkflowContext({
    client,
    sessionId: "s-wait",
    workflowVersion: "1.0.0",
    coordinationId: "order-4417",
    historyKnown: true,
  });

const inProgress = (extra = {}) => ({ status: CacheStatus.IN_PROGRESS, retryAfterMs: 1000n, ...extra });
const acquired = (token = 7n, interval = 60_000n, extra = {}) => ({
  status: CacheStatus.ACQUIRED,
  fencingToken: token,
  heartbeatIntervalMs: interval,
  ...extra,
});

test("a call behind another holder asks the engine to wait, and asks again at once after a wait it served", async () => {
  const clocks = fakeClocks();
  try {
    const client = scriptedClient(
      [
        inProgress({ waited: ENGINE_WAIT_MS }),
        inProgress({ waited: ENGINE_WAIT_MS }),
        inProgress({ waited: ENGINE_WAIT_MS }),
        acquired(),
      ],
      { clocks },
    );
    const charge = tool(async () => "charged", { toolName: "charge_card" });
    const started = Date.now();
    const result = await runWithContext(session(client), () => charge());
    const tookMs = Date.now() - started - 3 * ENGINE_WAIT_MS;
    assert.equal(result, "charged");
    assert.ok(ENGINE_WAIT_MS > 0);
    assert.deepEqual(
      client.acquires.map((a) => a.waitTimeoutMs),
      [ENGINE_WAIT_MS, ENGINE_WAIT_MS, ENGINE_WAIT_MS, ENGINE_WAIT_MS],
    );
    // Sleeping the engine's retryAfterMs between the three would take 3 s.
    assert.ok(tookMs < 1000, `the call slept between waits the engine served: ${tookMs} ms`);
  } finally {
    clocks.restore();
  }
});

test("an engine that answers at once instead of waiting is backed off from, not polled in a busy loop", async () => {
  const client = scriptedClient([inProgress({ retryAfterMs: 100n }), inProgress({ retryAfterMs: 100n }), acquired()]);
  const charge = tool(async () => "charged", { toolName: "charge_card" });
  const started = performance.now();
  await runWithContext(session(client), () => charge());
  const tookMs = performance.now() - started;
  assert.equal(client.acquires.length, 3);
  assert.ok(tookMs >= 190, `two 100 ms back-offs took only ${tookMs} ms`);
});

test("a lease granted at once costs no extra round trip before the body", async () => {
  const client = scriptedClient([acquired()]);
  const charge = tool(async () => "charged", { toolName: "charge_card" });
  await runWithContext(session(client), () => charge());
  assert.deepEqual(client.order, ["acquire", "commit"]);
});

test("a lease granted after a long wait is counted from a renewal, so the body finds it held", async () => {
  const clocks = fakeClocks();
  try {
    // The engine waited 10 s, longer than the 3 s TTL, before granting. Counted
    // from the request, the lease would be read as lapsed on arrival.
    const client = scriptedClient([acquired(7n, 750n, { waited: 10_000 })], { clocks });
    let checked;
    const charge = tool(
      async () => {
        client.order.push("body");
        currentLease().check();
        checked = !currentLease().isLost;
        return "charged";
      },
      { toolName: "charge_card", ttlMs: 3000 },
    );
    assert.equal(await runWithContext(session(client), () => charge()), "charged");
    assert.equal(checked, true);
    assert.deepEqual(client.order.slice(0, 3), ["acquire", "renew:7", "body"]);
  } finally {
    clocks.restore();
  }
});

test("a lease granted after a long wait that no renewal confirms is handed back before the body runs", async () => {
  const clocks = fakeClocks();
  try {
    // The grant came 10 s after a request with a 3 s TTL, and the renewal sent
    // to count it from a known time fails: nothing shows it is still held, so
    // the body must not start under it. The call hands it back and asks again.
    const client = scriptedClient([acquired(7n, 750n, { waited: 10_000 }), acquired(8n, 750n)], {
      clocks,
      failRenewalsFor: 7,
    });
    const tokens = [];
    const charge = tool(
      async () => {
        client.order.push("body");
        currentLease().check();
        tokens.push(currentLease().fencingToken);
        return "charged";
      },
      { toolName: "charge_card", ttlMs: 3000 },
    );
    assert.equal(await runWithContext(session(client), () => charge()), "charged");
    assert.deepEqual(tokens, [8]);
    assert.deepEqual(client.releases[0], { fencingToken: 7, reason: "LEASE_UNCONFIRMED" });
    const body = client.order.indexOf("body");
    assert.ok(client.order.indexOf("release:7") < body, client.order.join(" "));
    assert.ok(!client.order.slice(body).includes("renew:7"), "the abandoned lease was renewed after the body began");
  } finally {
    clocks.restore();
  }
});
