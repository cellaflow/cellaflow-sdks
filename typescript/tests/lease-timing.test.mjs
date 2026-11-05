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
    // Wide enough that a late timer on a busy runner does not flip a result:
    // every assertion sits at least 100 ms from the deadline it tests.
    heartbeatIntervalMs: 200,
    extendMs: 800,
    leaseTtlMs: 800,
    marginMs: 200,
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
    await sleep(450);
    assert.equal(hb.lost, false, "still inside the deadline");
    await sleep(250); // deadline is acquiredSentAt + 800 - 200
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
        await sleep(300); // sent at ~200, answers at ~500
        return { renewed: true };
      }
      return never();
    },
  };
  const { hb } = heartbeat(client);
  hb.start();
  try {
    await sleep(700);
    assert.equal(hb.lost, false, "renewal sent at ~200 holds until ~800");
    await sleep(200);
    // Counted from the answer (~500), the deadline would be ~1100.
    assert.equal(hb.lost, true, "lost at ~800, counted from the send");
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
    stall(900);
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
    await sleep(300);
    assert.equal(hb.lost, true);
    assert.match(lost[0], /another worker took the lease/);
  } finally {
    await hb.stop();
  }
});

test("a missed deadline is reported but renewing goes on until the engine refuses", async () => {
  let calls = 0;
  let refuse = false;
  const events = [];
  const { hb, lost } = heartbeat({
    async renewLease() {
      calls += 1;
      if (calls === 1) await sleep(700); // answers after the deadline (~600)
      return refuse ? { renewed: false, failureReason: 2 } : { renewed: true };
    },
  }, { onEvent: (e) => events.push(e) });
  hb.start();
  try {
    await sleep(1_100);
    assert.equal(hb.lost, true, "the deadline passed without a confirmed renewal");
    assert.equal(lost.length, 1);
    const sofar = calls;
    assert.ok(sofar >= 2, "kept renewing after reporting the loss");
    refuse = true;
    await sleep(500);
    const afterRefusal = calls;
    await sleep(500);
    assert.equal(calls, afterRefusal, "a refusal stops renewing");
    assert.equal(lost.length, 1, "onLeaseLost is called once");
    const lostEvents = events.filter((e) => e.type === "lost");
    assert.equal(lostEvents.length, 2, "a 'lost' event on the deadline, and again on the refusal");
    assert.match(lostEvents[1].detail, /another worker took the lease/);
    assert.match(hb.lostDetail, /another worker took the lease/);
  } finally {
    await hb.stop();
  }
});

test("a heartbeat stopped before it started does not start", async () => {
  let calls = 0;
  const { hb } = heartbeat({
    async renewLease() {
      calls += 1;
      return { renewed: true };
    },
  });
  await hb.stop();
  hb.start();
  await sleep(500);
  assert.equal(calls, 0);
});

test("the default renewal asks for the engine's TTL, four intervals", async () => {
  const asked = [];
  const hb = new LeaseHeartbeat({
    client: {
      async renewLease(agentId, key, token, extendMs) {
        asked.push(extendMs);
        return { renewed: true };
      },
    },
    agentId: "a",
    idempotencyKey: "k",
    fencingToken: 1,
    heartbeatIntervalMs: 100,
  });
  hb.start();
  await sleep(250);
  await hb.stop();
  assert.ok(asked.length >= 1);
  assert.ok(asked.every((ms) => ms === 400));
});

test("one or two failed renewals do not cost a lease the engine still holds", async () => {
  let calls = 0;
  const { hb, lost } = heartbeat({
    async renewLease() {
      calls += 1;
      // The first two time out the way the RPC deadline (half an interval) would.
      if (calls <= 2) {
        await sleep(100);
        throw new Error("deadline exceeded");
      }
      return { renewed: true };
    },
  }, { marginMs: undefined }); // the default: half an interval
  hb.start();
  try {
    await sleep(900); // the acquisition alone would have run out at ~700
    assert.equal(hb.lost, false, "the renewal at ~600 answered before the deadline");
    assert.equal(lost.length, 0);
  } finally {
    await hb.stop();
  }
});

test("a renewal the engine shortened near its maximum lifetime moves the deadline in", async () => {
  // The engine's clock is offset from the local one, and it will not extend the
  // lease past t=2100 (its maximum lifetime).
  const offset = 3_000_000;
  const t0 = performance.now();
  const hard = t0 + offset + 2_100;
  const hb = new LeaseHeartbeat({
    client: {
      async renewLease(agentId, key, token, extendMs) {
        const now = performance.now() + offset;
        if (now >= hard) return { renewed: false, failureReason: 5 }; // MAX_LIFETIME_EXCEEDED
        return { renewed: true, newExpiresAtMs: BigInt(Math.round(Math.min(now + extendMs, hard))) };
      },
    },
    agentId: "a",
    idempotencyKey: "k",
    fencingToken: 1,
    heartbeatIntervalMs: 300,
    extendMs: 1_200,
    leaseTtlMs: 1_200,
    acquiredSentAt: t0,
  });
  hb.start();
  try {
    // Renewals at ~300 (to 1500), ~600 (to 1800, which shows the first was
    // full), ~900 (to 2100) and ~1200 (asked for 2400, got 2100). Counted from
    // the send alone the deadline would be ~2250.
    await sleep(1_850);
    assert.equal(hb.lost, false, "held until the engine's expiry minus the margin (~1950)");
    await sleep(200);
    assert.equal(hb.lost, true, "lost by ~2050, before the engine's expiry at 2100");
  } finally {
    await hb.stop();
  }
});

test("one slow renewal does not make later ones look shortened", async () => {
  const offset = -5_000_000;
  let calls = 0;
  let lastSent = 0;
  const { hb } = heartbeat({
    async renewLease(agentId, key, token, extendMs) {
      calls += 1;
      lastSent = performance.now();
      // The first renewal reaches the engine 90 ms late, just inside its 100 ms
      // timeout; the rest at once.
      if (calls === 1) await sleep(90);
      return { renewed: true, newExpiresAtMs: BigInt(Math.round(performance.now() + offset + extendMs)) };
    },
  }, { marginMs: undefined });
  hb.start();
  try {
    await sleep(1_500);
    assert.equal(hb.lost, false);
    assert.ok(calls >= 5);
    // A full renewal's deadline is its send plus 800, minus the 100 ms margin.
    // Measured against the slow renewal it would sit about 90 ms earlier.
    const lead = hb.deadline - lastSent;
    assert.ok(lead > 680, `deadline ${lead.toFixed(1)} ms after the last send`);
  } finally {
    await hb.stop();
  }
});

test("a deadline also holds on the wall clock, for a host that was suspended", async () => {
  const { hb } = heartbeat({ renewLease: never });
  const realNow = Date.now;
  try {
    assert.equal(hb.pastDeadline(), false);
    // While a host sleeps, performance.now() stands still and Date.now() does not.
    Date.now = () => realNow() + 60_000;
    assert.equal(hb.pastDeadline(), true);
  } finally {
    Date.now = realNow;
    await hb.stop();
  }
});

test("a host suspended between sending a renewal and handling its answer loses the lease", async () => {
  const realNow = Date.now;
  let calls = 0;
  const { hb } = heartbeat({
    async renewLease() {
      calls += 1;
      if (calls === 1) {
        // The lid closes while the answer is on its way: the wall clock moves
        // on a minute, the monotonic clock does not.
        Date.now = () => realNow() + 60_000;
      }
      return { renewed: true };
    },
  });
  hb.start();
  try {
    await sleep(260); // the first renewal (at ~200) has been handled
    assert.equal(hb.pastDeadline(), true, "counted from the wall-clock send, a minute has passed");
  } finally {
    Date.now = realNow;
    await hb.stop();
  }
});

test("after a wake, the lease is reported lost without a renewal or a check", async () => {
  const realNow = Date.now;
  const { hb, lost } = heartbeat({ renewLease: never }, { extendMs: 60_000, leaseTtlMs: 60_000 });
  hb.start();
  try {
    await sleep(50);
    Date.now = () => realNow() + 120_000; // woke two minutes later, on the wall clock
    await sleep(200); // half an interval is 100 ms
    assert.equal(lost.length, 1, "the deadline timer saw the wall deadline");
  } finally {
    Date.now = realNow;
    await hb.stop();
  }
});

test("renewals get a timeout of half an interval, and up to a second on short TTLs", async () => {
  const timeouts = [];
  const client = {
    async renewLease(a, k, t, e, timeoutSecs) {
      timeouts.push(timeoutSecs);
      return { renewed: true };
    },
  };
  for (const [interval, extend] of [[200, 800], [500, 2_000], [8_000, 32_000]]) {
    const hb = new LeaseHeartbeat({
      client, agentId: "a", idempotencyKey: "k", fencingToken: 1,
      heartbeatIntervalMs: interval, extendMs: extend, leaseTtlMs: extend,
    });
    await hb.renewNow();
    await hb.stop();
  }
  assert.deepEqual(timeouts, [0.4, 1, 4]);
});

test("stop() waits for a renewal made with renewNow()", async () => {
  let answered = false;
  const { hb } = heartbeat({
    async renewLease() {
      await sleep(200);
      answered = true;
      return { renewed: true };
    },
  });
  const pending = hb.renewNow();
  await sleep(20);
  await hb.stop();
  assert.equal(answered, true, "stop() returned only after the renewal settled");
  await pending;
});

// --- executionLease, with the engine client stubbed on its prototype ---------

const original = {
  checkIdempotencyCache: CellaflowClient.prototype.checkIdempotencyCache,
  renewLease: CellaflowClient.prototype.renewLease,
  releaseLease: CellaflowClient.prototype.releaseLease,
};

afterEach(() => Object.assign(CellaflowClient.prototype, original));

// The engine's clock, deliberately far from the local one: only differences
// between its timestamps may matter to the SDK.
const engineNow = () => performance.now() + 7_000_000;

function stubEngine({ renew } = {}) {
  const calls = { renew: [], release: [] };
  Object.assign(CellaflowClient.prototype, {
    async checkIdempotencyCache() {
      return {
        status: CacheStatus.ACQUIRED,
        fencingToken: 5n,
        heartbeatIntervalMs: 50n,
        leaseExpiresAtMs: BigInt(Math.round(engineNow() + 200)),
      };
    },
    async renewLease(agentId, key, token, extendMs) {
      calls.renew.push({ at: performance.now(), extendMs });
      return renew
        ? renew(calls.renew.length)
        : { renewed: true, newExpiresAtMs: BigInt(Math.round(engineNow() + extendMs)) };
    },
    async releaseLease(agentId, key, token, reason) {
      calls.release.push(reason);
      return { released: true };
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
  assert.equal(typeof events[0].expiresAtMs, "number");
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

test("the block does not run when the lease is lost before it starts", async () => {
  const calls = stubEngine({ renew: () => ({ renewed: false, failureReason: 2 }) });
  let ran = false;
  await assert.rejects(
    executionLease("k", { workerId: "w", ttlMs: 200, waitMs: 1_000 }, async () => {
      ran = true;
    }),
    LeaseLostError,
  );
  assert.equal(ran, false);
  assert.deepEqual(calls.release, ["BLOCK_EXIT"], "the lease is still released");
});

test("after the block exits, check() fails, but the signal does not abort", async () => {
  stubEngine();
  let kept;
  await executionLease("k", { workerId: "w", ttlMs: 200 }, async (lease) => {
    kept = lease;
  });
  assert.equal(kept.isLost, true);
  assert.throws(() => kept.check(), /released when its block exited/);
  // Releasing is not losing: a response fetched with the signal may still be read.
  assert.equal(kept.signal.aborted, false);
});

test("reading signal after a stall shows it aborted, without calling check()", async () => {
  stubEngine();
  await executionLease("k", { workerId: "w", ttlMs: 200 }, async (lease) => {
    stall(300);
    assert.equal(lease.signal.aborted, true);
  });
});

test("inside the 'lost' event, the handle already reads as lost", async () => {
  stubEngine({ renew: () => ({ renewed: false, failureReason: 2 }) });
  let kept;
  let lostSeenInEvent;
  await executionLease(
    "k",
    {
      workerId: "w",
      ttlMs: 200,
      onEvent: (e) => {
        if (e.type === "lost") lostSeenInEvent = kept?.isLost;
      },
    },
    async (lease) => {
      kept = lease;
      await sleep(150);
    },
  );
  assert.equal(lostSeenInEvent, true);
});

test("after a long wait, one failed renewal before the block does not lose the lease", async () => {
  // The grant arrives after more than a TTL of waiting, so a deadline counted
  // from the request has passed; the renewal after it is what counts.
  const calls = stubEngine({
    renew: (n) => {
      if (n === 1) throw new Error("connection reset");
      return { renewed: true, newExpiresAtMs: BigInt(Math.round(engineNow() + 200)) };
    },
  });
  const slowAcquire = CellaflowClient.prototype.checkIdempotencyCache;
  CellaflowClient.prototype.checkIdempotencyCache = async function (...args) {
    await sleep(300);
    return slowAcquire.apply(this, args);
  };
  let ran = false;
  await executionLease("k", { workerId: "w", ttlMs: 200, waitMs: 1_000 }, async (lease) => {
    lease.check();
    ran = true;
  });
  assert.equal(ran, true);
  assert.equal(calls.renew[0] !== undefined, true);
});

test("after a wait, if no renewal before the block is confirmed, the block does not run", async () => {
  stubEngine({
    renew: () => {
      throw new Error("unavailable");
    },
  });
  const slowAcquire = CellaflowClient.prototype.checkIdempotencyCache;
  CellaflowClient.prototype.checkIdempotencyCache = async function (...args) {
    await sleep(300);
    return slowAcquire.apply(this, args);
  };
  let ran = false;
  await assert.rejects(
    executionLease("k", { workerId: "w", ttlMs: 200, waitMs: 1_000 }, async () => {
      ran = true;
    }),
    LeaseLostError,
  );
  assert.equal(ran, false);
});

test("after a wait with no renewal confirmed, the block runs while the request's deadline holds", async () => {
  stubEngine({
    renew: () => {
      throw new Error("unavailable");
    },
  });
  let ran = false;
  await executionLease("k", { workerId: "w", ttlMs: 2_000, waitMs: 1_000 }, async (lease) => {
    lease.check();
    ran = true;
  });
  assert.equal(ran, true, "granted at once, so the deadline from the request has not passed");
});

test("'released' is reported only when the engine confirms the release", async () => {
  stubEngine();
  CellaflowClient.prototype.releaseLease = async () => ({ released: false });
  const events = [];
  await executionLease("k", { workerId: "w", ttlMs: 200, onEvent: (e) => events.push(e.type) }, async () => {});
  assert.equal(events.includes("released"), false);
});
