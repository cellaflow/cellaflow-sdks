// When a held lease stops being treated as held. Runs against dist/ with a fake
// engine client, so no engine is needed: `npm test`.
import { test, afterEach } from "node:test";
import assert from "node:assert/strict";

import { CacheStatus, CellaflowClient, LeaseLostError, executionLease } from "../dist/index.js";
import { LeaseHeartbeat } from "../dist/lease.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// A renewal that never answers in time. It still settles, the way an RPC with a
// deadline does, so stop() is not left waiting on it forever.
const never = () => sleep(1_000).then(() => {
  throw new Error("deadline exceeded");
});

/** Blocks the event loop, the way a stopped process looks from the inside. */
function stall(ms) {
  const until = performance.now() + ms;
  while (performance.now() < until) {
    // spin
  }
}

function heartbeat(client, overrides = {}) {
  const lost = [];
  const hb = new LeaseHeartbeat({
    client,
    agentId: "a",
    idempotencyKey: "k",
    fencingToken: 1,
    heartbeatIntervalMs: 100,
    extendMs: 400,
    leaseTtlMs: 400,
    marginMs: 100,
    acquiredSentAt: performance.now(),
    onLeaseLost: (detail) => lost.push(detail),
    ...overrides,
  });
  return { hb, lost };
}

test("the lease is lost at its deadline when no renewal answers", async () => {
  const { hb, lost } = heartbeat({ renewLease: never });
  hb.start();
  try {
    await sleep(250);
    assert.equal(hb.lost, false, "still inside the deadline");
    await sleep(120); // deadline is acquiredSentAt + 400 - 100
    assert.equal(hb.lost, true);
    assert.equal(lost.length, 1);
  } finally {
    await hb.stop();
  }
});

test("a renewal extends the deadline from when it was sent, not when it answered", async () => {
  let calls = 0;
  const client = {
    async renewLease() {
      calls += 1;
      if (calls === 1) {
        await sleep(150); // sent at ~100, answers at ~250
        return { renewed: true };
      }
      return never();
    },
  };
  const { hb } = heartbeat(client);
  hb.start();
  try {
    await sleep(350);
    assert.equal(hb.lost, false, "renewal sent at ~100 holds until ~400");
    await sleep(120);
    // Counted from the answer (~250), the deadline would be ~550.
    assert.equal(hb.lost, true, "lost at ~400, counted from the send");
  } finally {
    await hb.stop();
  }
});

test("a process that was stalled past its deadline sees the loss before any timer runs", async () => {
  let renewals = 0;
  const { hb } = heartbeat({
    async renewLease() {
      renewals += 1;
      return { renewed: true };
    },
  });
  hb.start();
  try {
    stall(500);
    // No timer or renewal has run since the stall began.
    assert.equal(renewals, 0);
    assert.equal(hb.pastDeadline(), true);
    assert.equal(hb.lost, true);
  } finally {
    await hb.stop();
  }
});

test("a refused renewal is a loss, with the engine's reason", async () => {
  const { hb, lost } = heartbeat({
    async renewLease() {
      return { renewed: false, failureReason: 2 }; // SUPERSEDED
    },
  });
  hb.start();
  try {
    await sleep(150);
    assert.equal(hb.lost, true);
    assert.match(lost[0], /another worker took the lease/);
  } finally {
    await hb.stop();
  }
});

// --- executionLease, with the engine client stubbed on its prototype ---------

const original = {
  checkIdempotencyCache: CellaflowClient.prototype.checkIdempotencyCache,
  renewLease: CellaflowClient.prototype.renewLease,
  releaseLease: CellaflowClient.prototype.releaseLease,
};

afterEach(() => Object.assign(CellaflowClient.prototype, original));

function stubEngine({ renew } = {}) {
  const calls = { renew: [], release: [] };
  Object.assign(CellaflowClient.prototype, {
    async checkIdempotencyCache() {
      return {
        status: CacheStatus.ACQUIRED,
        fencingToken: 5n,
        heartbeatIntervalMs: 50n,
        leaseExpiresAtMs: 1_000n,
      };
    },
    async renewLease(agentId, key, token, extendMs) {
      calls.renew.push({ at: performance.now(), extendMs });
      return renew ? renew(calls.renew.length) : { renewed: true, newExpiresAtMs: 2_000n };
    },
    async releaseLease(agentId, key, token, reason) {
      calls.release.push(reason);
      return {};
    },
  });
  return calls;
}

test("executionLease renews for the full TTL and reports each step", async () => {
  const calls = stubEngine();
  const events = [];
  await executionLease(
    "k",
    { workerId: "w", ttlMs: 200, onEvent: (e) => events.push(e) },
    async () => sleep(130),
  );
  assert.ok(calls.renew.length >= 1);
  assert.ok(calls.renew.every((r) => r.extendMs === 200), "extends by ttlMs");
  const types = events.map((e) => e.type);
  assert.equal(types[0], "acquired");
  assert.equal(events[0].expiresAtMs, 1_000);
  assert.ok(types.includes("renewed"));
  assert.equal(types.at(-1), "released");
  assert.ok(events.every((e) => e.fencingToken === 5 && typeof e.at === "number"));
  assert.deepEqual(calls.release, ["BLOCK_EXIT"]);
});

test("after waiting for a lease, it renews once before the block runs", async () => {
  const calls = stubEngine();
  let renewalsBeforeBlock = -1;
  await executionLease("k", { workerId: "w", ttlMs: 200, waitMs: 1_000 }, async () => {
    renewalsBeforeBlock = calls.renew.length;
  });
  assert.equal(renewalsBeforeBlock, 1);
});

test("check() fails and the signal aborts once a renewal is refused", async () => {
  stubEngine({ renew: () => ({ renewed: false, failureReason: 1 }) }); // EXPIRED
  await executionLease("k", { workerId: "w", ttlMs: 200 }, async (lease) => {
    lease.check();
    await sleep(120);
    assert.equal(lease.isLost, true);
    assert.equal(lease.signal.aborted, true);
    assert.throws(() => lease.check(), LeaseLostError);
  });
});

test("check() fails after a stall past the deadline, before any renewal answers", async () => {
  const calls = stubEngine();
  await executionLease("k", { workerId: "w", ttlMs: 200 }, async (lease) => {
    lease.check();
    const before = calls.renew.length;
    stall(300);
    assert.equal(calls.renew.length, before, "no renewal ran during the stall");
    assert.throws(() => lease.check(), LeaseLostError);
  });
});
