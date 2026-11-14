// The lease a tool body sees through currentLease(): what it holds, how a loss
// reaches it, and what tool() does with a call whose lease was lost. Runs
// against dist/ with a scripted engine client, so no engine is needed.
import { test } from "node:test";
import assert from "node:assert/strict";

import {
  CacheStatus,
  IdempotencyScope,
  LeaseLostError,
  WorkflowContext,
  currentLease,
  step,
  tool,
} from "../dist/index.js";
import { serialize } from "../dist/serialization.js";
import { runWithContext } from "../dist/context.js";

const SUPERSEDED = 2; // RenewFailureReason.SUPERSEDED

/**
 * Records acquisitions, renewals, commits and releases. Acquisitions are
 * answered from `answers`, then ACQUIRED with token 41.
 */
function scriptedClient(answers = [], { renew, commit, released = true } = {}) {
  const calls = { acquires: [], renewals: [], commits: [], releases: [] };
  return {
    calls,
    async checkIdempotencyCache(agentId, key, waitTimeoutMs, leaseTtlMs, sessionId, sequence) {
      calls.acquires.push({ key, sequence });
      const next = answers.shift();
      if (next) return next;
      return { status: CacheStatus.ACQUIRED, fencingToken: 41n, heartbeatIntervalMs: 250n };
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      calls.renewals.push({ key, fencingToken });
      if (renew) return renew();
      return { renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) };
    },
    async commitStep(sessionId, sequence, name, status, payload, key, fencingToken) {
      calls.commits.push({ sequence, name, status, key, fencingToken, payload });
      if (commit) return commit({ sequence, status });
      return {};
    },
    async releaseLease(agentId, key, fencingToken, reason) {
      calls.releases.push({ key, fencingToken, reason });
      return { released: typeof released === "function" ? released(key) : released };
    },
    close() {},
  };
}

const session = (client) =>
  new WorkflowContext({
    client,
    sessionId: "s-check",
    workflowVersion: "1.0.0",
    coordinationId: "order-4417",
    historyKnown: true,
  });

/**
 * Moves both clocks the heartbeat reads forward by `ms`, as a process stopped
 * for that long sees them when it resumes. Returns a function that undoes it.
 */
function jumpClocks(ms) {
  const perf = performance.now.bind(performance);
  const wall = Date.now;
  // Defined rather than assigned: on some Node versions performance.now is an
  // inherited read-only property, which an own property can still shadow.
  const own = Object.getOwnPropertyDescriptor(performance, "now");
  Object.defineProperty(performance, "now", { value: () => perf() + ms, configurable: true, writable: true });
  Date.now = () => wall() + ms;
  return () => {
    if (own) Object.defineProperty(performance, "now", own);
    else delete performance.now;
    Date.now = wall;
  };
}

test("currentLease() in a tool body is the call's lease, and outside one it throws as before", async () => {
  const client = scriptedClient();
  let seen;
  const charge = tool(
    async () => {
      seen = currentLease();
      seen.check();
      return { token: seen.fencingToken, aborted: seen.signal.aborted, lost: seen.isLost };
    },
    { toolName: "charge_card" },
  );
  const out = await runWithContext(session(client), () => charge());
  assert.deepEqual(out, { token: 41, aborted: false, lost: false });
  // The body's lease is over once the call returns.
  assert.equal(seen.isLost, true);
  assert.throws(() => currentLease(), /No execution lease is active/);
});

test("step gets the same lease", async () => {
  const client = scriptedClient();
  const charge = step(async () => currentLease().fencingToken, { toolName: "charge_card" });
  assert.equal(await runWithContext(session(client), () => charge()), 41);
});

test("a body that resumes past its lease's deadline gets LeaseLostError from check(), and an aborted signal", async () => {
  const client = scriptedClient();
  let caught;
  let aborted;
  const charge = tool(
    async () => {
      const restore = jumpClocks(60_000);
      try {
        const lease = currentLease();
        aborted = lease.signal.aborted;
        try {
          lease.check();
        } catch (err) {
          caught = err;
          throw err;
        }
        return "sent";
      } finally {
        restore();
      }
    },
    { toolName: "charge_card", ttlMs: 2000 },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), LeaseLostError);
  assert.ok(caught instanceof LeaseLostError, String(caught));
  assert.equal(aborted, true);
});

test("a renewal the engine refuses aborts the signal while the body still runs", async () => {
  const client = scriptedClient([], { renew: () => ({ renewed: false, failureReason: SUPERSEDED }) });
  const charge = tool(
    async () => {
      const lease = currentLease();
      await new Promise((resolve) => lease.signal.addEventListener("abort", resolve, { once: true }));
      lease.check();
      return "sent";
    },
    { toolName: "charge_card" },
  );
  // The heartbeat's timers do not hold the event loop open; this one does, and
  // fails the test if the signal never aborts.
  let giveUp;
  const timedOut = new Promise((_, reject) => {
    giveUp = setTimeout(() => reject(new Error("the signal never aborted")), 3000);
  });
  try {
    await assert.rejects(Promise.race([runWithContext(session(client), () => charge()), timedOut]), LeaseLostError);
  } finally {
    clearTimeout(giveUp);
  }
  assert.ok(client.calls.renewals.length >= 1);
});

test("a body that returns after this side judged its lease lost is still committed: the engine decides", async () => {
  // Nobody took over, so the engine accepts the commit, and the side effect the
  // body ran is recorded rather than run again by a retry.
  const client = scriptedClient();
  const charge = tool(
    async () => {
      const restore = jumpClocks(60_000);
      setImmediate(restore);
      return "refund-123";
    },
    { toolName: "charge_card", ttlMs: 2000 },
  );
  assert.equal(await runWithContext(session(client), () => charge()), "refund-123");
  assert.equal(client.calls.commits.length, 1);
  assert.equal(client.calls.commits[0].fencingToken, 41);
});

test("a body that throws after a local loss, with nobody taking over, is released and recorded as before", async () => {
  const client = scriptedClient();
  const charge = tool(
    async () => {
      const restore = jumpClocks(60_000);
      try {
        currentLease().check();
      } finally {
        restore();
      }
    },
    { toolName: "charge_card", ttlMs: 2000 },
  );
  const next = tool(async () => "next", { toolName: "next" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), LeaseLostError);
    await next();
  });
  // Released, so its position is free; the failure is recorded there, and the
  // next call lands after it.
  assert.equal(client.calls.releases.length, 1);
  assert.ok(client.calls.acquires.some((a) => a.key.includes(":failure:")));
  assert.deepEqual(
    client.calls.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "charge_card"],
      [2, "next"],
    ],
  );
});

test("a body that throws after another worker took the call over records nothing, and its position goes back", async () => {
  // The engine refuses the release: the lease is no longer this call's.
  const client = scriptedClient([], { released: (key) => !key.includes("charge_card") });
  const charge = tool(
    async () => {
      throw new LeaseLostError("taken over");
    },
    { toolName: "charge_card" },
  );
  const next = tool(async () => "next", { toolName: "next" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), LeaseLostError);
    await next();
  });
  assert.equal(client.calls.releases.length, 1);
  assert.ok(!client.calls.acquires.some((a) => a.key.includes(":failure:")), JSON.stringify(client.calls.acquires));
  assert.deepEqual(
    client.calls.commits.map((c) => [c.sequence, c.name]),
    [[1, "next"]],
  );
});

test("calling the tool again after a takeover claims the same position and takes the new holder's result", async () => {
  const hit = {
    status: CacheStatus.HIT,
    currentSequence: 0n,
    cachedResult: { outputPayload: serialize({ result: "refund-from-new-holder" }) },
  };
  const client = scriptedClient([], { released: false });
  let runs = 0;
  const refund = tool(
    async ({ order }) => {
      runs += 1;
      throw new LeaseLostError(`taken over: ${order}`);
    },
    { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["order"] },
  );
  const result = await runWithContext(session(client), async () => {
    await assert.rejects(refund({ order: "4417" }), LeaseLostError);
    client.calls.acquires.length = 0;
    const answers = [hit];
    const check = client.checkIdempotencyCache.bind(client);
    client.checkIdempotencyCache = (...a) =>
      answers.length ? (client.calls.acquires.push({ key: a[1], sequence: a[5] }), answers.shift()) : check(...a);
    return refund({ order: "4417" });
  });
  assert.equal(result, "refund-from-new-holder");
  assert.equal(runs, 1);
  assert.equal(client.calls.acquires[0].sequence, 1);
});

test("a takeover while another call is in flight leaves the position empty and stops recording", async () => {
  const client = scriptedClient([], { released: false });
  const ctx = session(client);
  const lostOne = tool(
    async () => {
      await new Promise((r) => setTimeout(r, 30));
      throw new LeaseLostError("taken over");
    },
    { toolName: "lost_one" },
  );
  const other = tool(async () => {
    await new Promise((r) => setTimeout(r, 60));
    return "other";
  }, { toolName: "other" });
  await runWithContext(ctx, async () => {
    const [a, b] = await Promise.allSettled([lostOne(), other()]);
    assert.equal(a.status, "rejected");
    assert.equal(b.status, "fulfilled");
  });
  assert.equal(ctx.historyKnown, false);
});

test("a commit the engine fences out becomes LeaseLostError, with no release", async () => {
  const stale = Object.assign(new Error("[failed_precondition] Fenced out: stale lease"), { code: 9 });
  const client = scriptedClient([], {
    commit: () => {
      throw stale;
    },
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  const err = await runWithContext(session(client), () => charge()).then(
    () => assert.fail("expected LeaseLostError"),
    (e) => e,
  );
  assert.ok(err instanceof LeaseLostError, String(err));
  assert.equal(err.result, "refund-123");
  assert.equal(err.cause, stale);
  assert.deepEqual(client.calls.releases, []);
});

test("any other commit refusal is unchanged: the lease is released and the error is the engine's", async () => {
  const taken = Object.assign(new Error("[failed_precondition] Sequence mismatch"), { code: 9 });
  const client = scriptedClient([], {
    commit: () => {
      throw taken;
    },
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client), () => charge()), (e) => e === taken);
  assert.equal(client.calls.releases.length, 1);
  assert.equal(client.calls.releases[0].reason, "TOOL_ERROR");
});

test("a body that fails with its lease held is released and recorded as before", async () => {
  const client = scriptedClient();
  const charge = tool(
    async () => {
      currentLease().check();
      throw new Error("card declined");
    },
    { toolName: "charge_card" },
  );
  const next = tool(async () => "next", { toolName: "next" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), /card declined/);
    await next();
  });
  assert.equal(client.calls.releases.length, 1);
  assert.ok(client.calls.acquires.some((a) => a.key.includes(":failure:")));
});

test("a cache hit runs no body, so no lease reaches it", async () => {
  const hit = {
    status: CacheStatus.HIT,
    currentSequence: 0n,
    cachedResult: { outputPayload: serialize({ result: "from-elsewhere" }) },
  };
  const client = scriptedClient([hit]);
  let ran = false;
  const charge = tool(
    async () => {
      ran = true;
    },
    { toolName: "charge_card", scope: IdempotencyScope.SHARED, sharedOn: ["order"] },
  );
  const next = tool(async () => currentLease().fencingToken, { toolName: "next" });
  const token = await runWithContext(session(client), async () => {
    assert.equal(await charge({ order: "4417" }), "from-elsewhere");
    return next();
  });
  assert.equal(ran, false);
  // The record written for the hit took a lease of its own; the next body sees
  // only its own.
  assert.ok(client.calls.acquires.some((a) => a.key.includes(":record:")));
  assert.equal(token, 41);
});

test("work a body started and did not await finds the lease released once the call returns", async () => {
  const client = scriptedClient();
  let later;
  const charge = tool(
    async () => {
      later = new Promise((resolve) => setTimeout(() => resolve(currentLease().isLost), 20));
      return "done";
    },
    { toolName: "charge_card" },
  );
  await runWithContext(session(client), () => charge());
  assert.equal(await later, true);
});

test("a renewal refused because the commit itself completed the key does not abort the signal after success", async () => {
  const COMPLETED = 4; // RenewFailureReason.COMPLETED
  let committed = false;
  const client = scriptedClient([], {
    renew: () => (committed ? { renewed: false, failureReason: COMPLETED } : { renewed: true }),
    commit: async () => {
      committed = true;
      // A renewal goes out and comes back refused while this answer is on its way.
      await new Promise((r) => setTimeout(r, 400));
      return {};
    },
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "charged";
    },
    { toolName: "charge_card" },
  );
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.ok(client.calls.renewals.length >= 1, "no renewal went out during the commit");
  assert.equal(lease.signal.aborted, false);
});

test("a commit refused in other words, whose release the engine also refuses, is a takeover too", async () => {
  const done = Object.assign(
    new Error("[failed_precondition] Idempotency key already committed at sequence 4, but this request specified sequence 1"),
    { code: 9 },
  );
  const client = scriptedClient([], {
    commit: () => {
      throw done;
    },
    released: false,
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  const err = await runWithContext(session(client), () => charge()).then(
    () => assert.fail("expected LeaseLostError"),
    (e) => e,
  );
  assert.ok(err instanceof LeaseLostError, String(err));
  assert.equal(err.result, "refund-123");
  assert.equal(err.cause, done);
  assert.ok(!client.calls.acquires.some((a) => a.key.includes(":failure:")));
});

test("LeaseLostError keeps its result out of what a logger serialises", () => {
  const err = new LeaseLostError("gone", { result: { card: "4242" }, cause: new Error("x") });
  assert.deepEqual(err.result, { card: "4242" });
  assert.ok(!Object.keys(err).includes("result"));
  assert.ok(!JSON.stringify(err).includes("4242"));
  assert.equal(new LeaseLostError("gone").result, undefined);
});

test("a commit that landed but whose answer was lost is not mistaken for a takeover", async () => {
  // The first commit lands and its answer is lost; the key is then complete
  // under this call's own token, so the engine refuses the release too. The
  // same commit sent again is answered as done.
  let sent = 0;
  const client = scriptedClient([], {
    commit: ({ sequence }) => {
      sent += 1;
      if (sent === 1 && sequence === 1) throw Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
      return {};
    },
    released: false,
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  const next = tool(async () => "next", { toolName: "next" });
  const out = await runWithContext(session(client), async () => [await charge(), await next()]);
  assert.deepEqual(out, ["refund-123", "next"]);
  assert.deepEqual(
    client.calls.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "charge_card"],
      [1, "charge_card"],
      [2, "next"],
    ],
  );
  // The resend is the same commit: same position, key, token and payload.
  const [first, again] = client.calls.commits;
  assert.deepEqual(again, first);
  assert.equal(again.fencingToken, 41);
});

test("a loss held back during a commit that then fails is applied", async () => {
  const COMPLETED = 4;
  let committing = false;
  const client = scriptedClient([], {
    renew: () => (committing ? { renewed: false, failureReason: COMPLETED } : { renewed: true }),
    commit: async () => {
      committing = true;
      await new Promise((r) => setTimeout(r, 400));
      throw Object.assign(new Error("[internal] storage write failed"), { code: 13 });
    },
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "charged";
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), /storage write failed/);
  assert.ok(client.calls.renewals.length >= 1);
  assert.equal(lease.signal.aborted, true);
});

test("a body's own error, after a takeover found through the release, reaches the caller unchanged", async () => {
  const declined = new Error("card declined");
  const client = scriptedClient([], { released: false });
  const charge = tool(
    async () => {
      throw declined;
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), (e) => e === declined);
  assert.ok(!client.calls.acquires.some((a) => a.key.includes(":failure:")));
});

test("a taken-over call holding the top position hands it back even with a lower call in flight", async () => {
  const client = scriptedClient([], { released: (key) => !key.includes("lost_one") });
  const ctx = session(client);
  const slowOne = tool(async () => {
    await new Promise((r) => setTimeout(r, 60));
    return "slow";
  }, { toolName: "slow_one" });
  const lostOne = tool(
    async () => {
      throw new LeaseLostError("taken over");
    },
    { toolName: "lost_one" },
  );
  const next = tool(async () => "next", { toolName: "next" });
  await runWithContext(ctx, async () => {
    const [a, b] = await Promise.allSettled([slowOne(), lostOne()]);
    assert.equal(a.status, "fulfilled");
    assert.equal(b.status, "rejected");
    await next();
  });
  assert.equal(ctx.historyKnown, true);
  assert.deepEqual(
    client.calls.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "slow_one"],
      [2, "next"],
    ],
  );
});

test("a renewal refused because of the call's own release does not abort the signal", async () => {
  const NOT_FOUND = 5;
  let released = false;
  const client = scriptedClient([], {
    renew: async () => {
      await new Promise((r) => setTimeout(r, 100));
      return released ? { renewed: false, failureReason: NOT_FOUND } : { renewed: true };
    },
  });
  const release = client.releaseLease.bind(client);
  client.releaseLease = async (...a) => {
    released = true;
    // The refused renewal comes back while the release is still on its way.
    await new Promise((r) => setTimeout(r, 200));
    return release(...a);
  };
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      // Long enough for a renewal to be on its way when the release goes out.
      await new Promise((r) => setTimeout(r, 300));
      throw new Error("card declined");
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), /card declined/);
  await new Promise((r) => setTimeout(r, 150));
  assert.ok(client.calls.renewals.length >= 1, "no renewal went out");
  assert.equal(lease.signal.aborted, false);
});

test("when neither the commit nor its resend is answered, the call fails as it would have, not as a takeover", async () => {
  const reset = () => Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  const first = reset();
  let sent = 0;
  const client = scriptedClient([], {
    commit: ({ sequence }) => {
      sent += 1;
      // The commit, its resend, and its resend after the refused release all
      // go unanswered; the failure record after them is written.
      if (sequence === 1 && sent <= 3) throw sent === 1 ? first : reset();
      return {};
    },
    released: (key) => key.includes(":failure:"),
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  const next = tool(async () => "next", { toolName: "next" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), (e) => e === first);
    await next();
  });
  // Not handed back: the position is kept and the failure goes through the
  // usual record path.
  assert.ok(client.calls.acquires.some((a) => a.key.includes(":failure:")));
});

test("a resend refused because the position is filled is not a takeover: the call fails as it would have", async () => {
  const first = Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  const mismatch = Object.assign(new Error("[failed_precondition] Sequence mismatch: expected sequence 3, but request specified 1"), { code: 9 });
  let sent = 0;
  const client = scriptedClient([], {
    commit: ({ sequence }) => {
      sent += 1;
      if (sequence !== 1) return {};
      throw sent === 1 ? first : mismatch;
    },
    released: (key) => key.includes(":failure:"),
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client), () => charge()), (e) => e === first);
});

test("a resend with no answer and no status code is not a takeover either", async () => {
  const first = Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  let sent = 0;
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      throw sent === 1 ? first : Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
    },
    released: false,
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client), () => charge()), (e) => e === first);
});

test("a resend refused because the key is recorded at another position is a takeover", async () => {
  const first = Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  const elsewhere = Object.assign(
    new Error("[failed_precondition] Idempotency key already committed at sequence 4, but this request specified sequence 1"),
    { code: 9 },
  );
  let sent = 0;
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      throw sent === 1 ? first : elsewhere;
    },
    released: false,
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client), () => charge()), (e) => e instanceof LeaseLostError && e.result === "refund-123");
});

test("a call that succeeds through the resend keeps its signal unaborted", async () => {
  const COMPLETED = 4;
  let committed = false;
  let sent = 0;
  const client = scriptedClient([], {
    renew: () => (committed ? { renewed: false, failureReason: COMPLETED } : { renewed: true }),
    commit: async () => {
      sent += 1;
      committed = true;
      if (sent === 1) {
        // The commit lands; a renewal is refused because of it; the answer is lost.
        await new Promise((r) => setTimeout(r, 400));
        throw Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
      }
      return {};
    },
    released: false,
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "charged";
    },
    { toolName: "charge_card" },
  );
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.ok(client.calls.renewals.length >= 1);
  assert.equal(lease.signal.aborted, false);
});

test("a commit with no answer while the lease is still held is sent again before anything is released", async () => {
  // The engine still holds the lease for this call, so the resend is recorded;
  // releasing first would throw the result away and a retry would run it again.
  let sent = 0;
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      if (sent === 1) throw Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
      return {};
    },
  });
  const charge = tool(async () => "refund-123", { toolName: "charge_card" });
  assert.equal(await runWithContext(session(client), () => charge()), "refund-123");
  assert.equal(client.calls.commits.length, 2);
  assert.deepEqual(client.calls.releases, []);
});

test("a resend refused as stale after a lost answer is a takeover, and the cause is that refusal", async () => {
  const first = Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  const stale = Object.assign(new Error("[failed_precondition] Fenced out: stale lease"), { code: 9 });
  let sent = 0;
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      throw sent === 1 ? first : stale;
    },
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "refund-123";
    },
    { toolName: "charge_card" },
  );
  const err = await runWithContext(session(client), () => charge()).then(
    () => assert.fail("expected LeaseLostError"),
    (e) => e,
  );
  assert.ok(err instanceof LeaseLostError, String(err));
  assert.equal(err.cause, stale);
  assert.deepEqual(client.calls.releases, []);
  // A definite takeover aborts the signal, for work the body left running.
  assert.equal(lease.signal.aborted, true);
});

test("a fenced commit aborts the signal", async () => {
  const stale = Object.assign(new Error("[failed_precondition] Fenced out: stale lease"), { code: 9 });
  const client = scriptedClient([], {
    commit: () => {
      throw stale;
    },
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "refund-123";
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), LeaseLostError);
  assert.equal(lease.signal.aborted, true);
});

test("work a body left running finds the lease over while the commit is still on its way", async () => {
  let readDuringCommit;
  let lease;
  const client = scriptedClient([], {
    commit: async () => {
      await new Promise((r) => setTimeout(r, 50));
      readDuringCommit = lease.isLost;
      return {};
    },
  });
  const charge = tool(
    async () => {
      lease = currentLease();
      return "charged";
    },
    { toolName: "charge_card" },
  );
  await runWithContext(session(client), () => charge());
  assert.equal(readDuringCommit, true);
});

test("work a body left running finds the lease over while the release is still on its way", async () => {
  let readDuringRelease;
  let lease;
  const client = scriptedClient();
  const release = client.releaseLease.bind(client);
  client.releaseLease = async (...a) => {
    await new Promise((r) => setTimeout(r, 50));
    readDuringRelease = lease.isLost;
    return release(...a);
  };
  const charge = tool(
    async () => {
      lease = currentLease();
      throw new Error("card declined");
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), /card declined/);
  assert.equal(readDuringRelease, true);
});

test("a throw whose release is refused aborts the signal", async () => {
  const client = scriptedClient([], { released: false });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      throw new Error("card declined");
    },
    { toolName: "charge_card" },
  );
  await assert.rejects(runWithContext(session(client), () => charge()), /card declined/);
  assert.equal(lease.signal.aborted, true);
});

test("a takeover found by the resend after a refused release aborts the signal too", async () => {
  const reset = () => Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
  const stale = Object.assign(new Error("[failed_precondition] Fenced out: stale lease"), { code: 9 });
  let sent = 0;
  const client = scriptedClient([], {
    // The commit and its first resend go unanswered; the resend after the
    // refused release is refused as stale.
    commit: () => {
      sent += 1;
      throw sent <= 2 ? reset() : stale;
    },
    released: false,
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "refund-123";
    },
    { toolName: "charge_card" },
  );
  const err = await runWithContext(session(client), () => charge()).then(
    () => assert.fail("expected LeaseLostError"),
    (e) => e,
  );
  assert.ok(err instanceof LeaseLostError && err.cause === stale, String(err));
  assert.equal(sent, 3);
  assert.equal(lease.signal.aborted, true);
});

test("a renewal refused while the first resend is on its way does not abort the signal of a call that succeeds", async () => {
  const COMPLETED = 4;
  let committed = false;
  let sent = 0;
  const client = scriptedClient([], {
    renew: () => (committed ? { renewed: false, failureReason: COMPLETED } : { renewed: true }),
    commit: async () => {
      sent += 1;
      committed = true;
      if (sent === 1) throw Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
      // The resend is slow; a renewal is refused because the first commit landed.
      await new Promise((r) => setTimeout(r, 400));
      return {};
    },
  });
  let lease;
  const charge = tool(
    async () => {
      lease = currentLease();
      return "charged";
    },
    { toolName: "charge_card" },
  );
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.ok(client.calls.renewals.length >= 1);
  assert.equal(lease.signal.aborted, false);
});

test("a first commit failing with no gRPC status is resent; a refusal is not", async () => {
  let sent = 0;
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      if (sent === 1) throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
      return {};
    },
  });
  const charge = tool(async () => "charged", { toolName: "charge_card" });
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.equal(sent, 2);

  const refused = Object.assign(new Error("[invalid_argument] payload too large"), { code: 3 });
  const client2 = scriptedClient([], {
    commit: () => {
      throw refused;
    },
  });
  const charge2 = tool(async () => "charged", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client2), () => charge2()), (e) => e === refused);
  assert.equal(client2.calls.commits.length, 1);
});

test("a fenced commit is sent once, and the first commit's key recorded elsewhere is a takeover at once", async () => {
  const stale = Object.assign(new Error("[failed_precondition] Fenced out: stale lease"), { code: 9 });
  const client = scriptedClient([], {
    commit: () => {
      throw stale;
    },
  });
  const charge = tool(async () => "x", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client), () => charge()), LeaseLostError);
  assert.equal(client.calls.commits.length, 1);

  const elsewhere = Object.assign(
    new Error("[failed_precondition] Idempotency key already committed at sequence 4, but this request specified sequence 1"),
    { code: 9 },
  );
  const client2 = scriptedClient([], {
    commit: () => {
      throw elsewhere;
    },
  });
  const charge2 = tool(async () => "x", { toolName: "charge_card" });
  await assert.rejects(runWithContext(session(client2), () => charge2()), (e) => e instanceof LeaseLostError && e.cause === elsewhere);
  assert.equal(client2.calls.commits.length, 1);
  assert.deepEqual(client2.calls.releases, []);
});

test("every commit attempt sends what the body returned, even if it is changed afterwards", async () => {
  let sent = 0;
  const result = { refund: "r-1" };
  const client = scriptedClient([], {
    commit: () => {
      sent += 1;
      if (sent === 1) {
        result.refund = "changed by work the body left running";
        throw Object.assign(new Error("[unavailable] connection reset"), { code: 14 });
      }
      return {};
    },
  });
  const charge = tool(async () => result, { toolName: "charge_card" });
  await runWithContext(session(client), () => charge());
  assert.deepEqual(client.calls.commits[0].payload, client.calls.commits[1].payload);
  assert.deepEqual(client.calls.commits[1].payload, { result: { refund: "r-1" } });
});

test("a first renewal the engine already shortens is judged by what it granted", async () => {
  const { LeaseHeartbeat } = await import("../dist/lease.js");
  const sentWall = Date.now();
  const sentMono = performance.now();
  let lostDetail;
  const hb = new LeaseHeartbeat({
    client: {
      // Sent 5 s after the grant, asking for 20 s, but granted only to the
      // engine's ceiling, 22 s after the grant: 17 s from the send.
      async renewLease() {
        return { renewed: true, newExpiresAtMs: BigInt(sentWall + 22_000) };
      },
    },
    agentId: "a",
    idempotencyKey: "k",
    fencingToken: 1,
    heartbeatIntervalMs: 5000,
    extendMs: 20_000,
    leaseTtlMs: 20_000,
    acquiredSentAt: sentMono,
    acquiredSentWallAt: sentWall,
    acquiredExpiresAtMs: sentWall + 20_000,
    onLeaseLost: (detail) => {
      lostDetail = detail;
    },
  });
  let restore = jumpClocks(5_000);
  try {
    assert.equal(await hb.renewNow(), true);
  } finally {
    restore();
  }
  // Held 17 s from that send, less a 2.5 s margin: until 19.5 s after the
  // grant. A full renewal would have held until 22.5 s.
  restore = jumpClocks(20_000);
  try {
    hb.pastDeadline();
  } finally {
    restore();
  }
  await hb.stop();
  assert.ok(lostDetail !== undefined, "the shortened first renewal was taken as a full one");
});
