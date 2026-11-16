// How a tool call waits behind another worker's lease: in the engine, not by
// polling, and with the lease it is granted after a wait counted from when it
// was granted. Runs against dist/ with a scripted engine client: `npm test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";

import { CacheStatus, CellaflowClient, LeaseNotAcquired, WorkflowContext, currentLease, tool } from "../dist/index.js";
import { runWithContext } from "../dist/context.js";
import { serialize } from "../dist/serialization.js";
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
    offset: () => offset,
    restore: () => {
      if (own) Object.defineProperty(performance, "now", own);
      else delete performance.now;
      Date.now = wall;
    },
  };
}

/**
 * Answers a call's acquisitions from `answers` in order, and the leases that
 * record a result or failure in the session from `recordAnswers`, then
 * ACQUIRED. An answer may carry `waited`, the time the engine spent waiting
 * before it, applied to the clocks, and `expiresInMs`, which sets the grant's
 * `leaseExpiresAtMs` on the clocks as they read when it answers. A grant made
 * a little before its answer is handled has a little less than its TTL left,
 * so a renewal sent after it expires strictly later, as on a real engine.
 */
function scriptedClient(
  answers,
  { clocks, failRenewalsFor = () => false, failReleases = false, recordAnswers = [] } = {},
) {
  const acquires = [];
  const recordAcquires = [];
  const renewals = [];
  const releases = [];
  const order = [];
  const answer = (next) => {
    const { waited, expiresInMs, throws, ...resp } = next;
    if (waited) clocks.advance(waited);
    if (throws !== undefined) throw Object.assign(new Error(`engine error ${throws}`), { code: throws });
    if (expiresInMs !== undefined) resp.leaseExpiresAtMs = BigInt(Date.now() + expiresInMs);
    return resp;
  };
  return {
    acquires,
    recordAcquires,
    renewals,
    releases,
    order,
    async checkIdempotencyCache(agentId, key, waitTimeoutMs, leaseTtlMs, sessionId, sequence, timeoutSecs) {
      if (key.includes(":record:") || key.includes(":failure:")) {
        recordAcquires.push({ key, waitTimeoutMs, timeoutSecs });
        order.push("record-acquire");
        return answer(recordAnswers.shift() ?? { status: CacheStatus.ACQUIRED, fencingToken: 99n });
      }
      acquires.push({ key, waitTimeoutMs, leaseTtlMs, sequence, timeoutSecs });
      order.push("acquire");
      const next = answers.shift();
      if (!next) throw new Error("no scripted answer left");
      return answer(next);
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      renewals.push({ fencingToken, extendMs });
      order.push(`renew:${fencingToken}`);
      if (failRenewalsFor(Number(fencingToken))) {
        throw Object.assign(new Error("engine unavailable"), { code: 14 });
      }
      return { renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) };
    },
    async commitStep(sessionId, sequence, name) {
      order.push(`commit:${sequence}:${name}`);
      return {};
    },
    async releaseLease(agentId, key, fencingToken, reason) {
      releases.push({ fencingToken, reason });
      order.push(`release:${fencingToken}`);
      const fails = typeof failReleases === "function" ? failReleases(Number(fencingToken)) : failReleases;
      if (fails) throw Object.assign(new Error("engine unavailable"), { code: 14 });
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
    // Each waiting request carries a deadline past the wait, so a connection
    // that dies while the engine holds it fails the call instead of hanging.
    assert.ok(client.acquires.every((a) => a.timeoutSecs * 1000 > ENGINE_WAIT_MS));
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
  assert.deepEqual(client.order, ["acquire", "commit:1:charge_card"]);
});

test("a lease granted after a long wait is counted from a renewal, so the body finds it held", async () => {
  const clocks = fakeClocks();
  try {
    // The engine waited 10 s, longer than the 3 s TTL, before granting. Counted
    // from the request, the lease would be read as lapsed on arrival.
    const client = scriptedClient([acquired(7n, 750n, { waited: 10_000, expiresInMs: 2900 })], { clocks });
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
    const client = scriptedClient(
      [acquired(7n, 750n, { waited: 10_000, expiresInMs: 2900 }), acquired(8n, 750n, { expiresInMs: 3000 })],
      { clocks, failRenewalsFor: (token) => token === 7 },
    );
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

test("a late grant whose renewals fail but whose deadline has not passed runs the body", async () => {
  const clocks = fakeClocks();
  try {
    // A 1 s wait leaves most of a 3 s lease, counted from the request, so the
    // body runs on it even though no renewal is confirmed.
    const client = scriptedClient([acquired(7n, 750n, { waited: 1000, expiresInMs: 2900 })], {
      clocks,
      failRenewalsFor: () => true,
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
    assert.deepEqual(tokens, [7]);
    assert.deepEqual(client.releases, []);
  } finally {
    clocks.restore();
  }
});

test("a call whose late grants no renewal ever confirms fails after three, and its failure record frees the last", async () => {
  const clocks = fakeClocks();
  try {
    const late = (token) => acquired(token, 750n, { waited: 10_000, expiresInMs: 2900 });
    const client = scriptedClient([late(7n), late(8n), late(9n), acquired(10n)], {
      clocks,
      failRenewalsFor: () => true,
      failReleases: true,
    });
    let ran = false;
    const charge = tool(
      async () => {
        ran = true;
        return "charged";
      },
      { toolName: "charge_card", ttlMs: 3000 },
    );
    const next = tool(async () => "next", { toolName: "next_step" });
    await runWithContext(session(client), async () => {
      const started = performance.now() - clocks.offset();
      await assert.rejects(charge(), (err) => err instanceof LeaseNotAcquired && /could not confirm any of 3 leases/.test(err.message));
      // Three grants, each tried three times with 100 and 200 ms between: no
      // pause after the last try, which would add 400 ms to each.
      const tookMs = performance.now() - clocks.offset() - started;
      assert.ok(tookMs < 1500, `three hand-backs took ${tookMs} ms`);
      // The failure is recorded at the call's position before the next call
      // claims one, and the lease still claiming it is released first.
      client.order.push("next");
      await next();
    });
    assert.equal(ran, false);
    assert.deepEqual(
      client.releases.slice(0, 3).map((r) => [r.fencingToken, r.reason]),
      [
        [7, "LEASE_UNCONFIRMED"],
        [8, "LEASE_UNCONFIRMED"],
        [9, "LEASE_UNCONFIRMED"],
      ],
    );
    const afterNext = client.order.slice(client.order.indexOf("next"));
    assert.ok(afterNext.includes("release:9"), afterNext.join(" "));
    assert.ok(afterNext.indexOf("release:9") < afterNext.indexOf("record-acquire"), afterNext.join(" "));
  } finally {
    clocks.restore();
  }
});

test("the lease that records a received result waits in the engine, and asks again at once after a served wait", async () => {
  const clocks = fakeClocks();
  try {
    const hit = {
      status: CacheStatus.HIT,
      currentSequence: 0n,
      cachedResult: { outputPayload: serialize({ result: "placed-elsewhere" }) },
    };
    const client = scriptedClient([hit, acquired()], {
      clocks,
      recordAnswers: [inProgress({ waited: ENGINE_WAIT_MS }), inProgress({ waited: ENGINE_WAIT_MS })],
    });
    const place = tool(async () => "placed-here", { toolName: "place_order" });
    const confirm = tool(async () => "confirmed", { toolName: "send_confirmation" });
    const started = Date.now();
    await runWithContext(session(client), async () => {
      assert.equal(await place(), "placed-elsewhere");
      assert.equal(await confirm(), "confirmed");
    });
    const tookMs = Date.now() - started - 2 * ENGINE_WAIT_MS;
    assert.equal(client.recordAcquires.length, 3);
    assert.ok(client.recordAcquires.every((a) => a.waitTimeoutMs === ENGINE_WAIT_MS));
    assert.ok(client.recordAcquires.every((a) => a.timeoutSecs * 1000 > ENGINE_WAIT_MS));
    assert.ok(tookMs < 1000, `the record slept between waits the engine served: ${tookMs} ms`);
    assert.deepEqual(
      client.order.filter((e) => e.startsWith("commit")),
      ["commit:1:place_order", "commit:2:send_confirmation"],
    );
  } finally {
    clocks.restore();
  }
});

test("a request that gets no answer is asked again, and the body runs once under the lease it is told about", async () => {
  const client = scriptedClient([{ throws: 4 }, acquired(8n)]);
  let runs = 0;
  const charge = tool(
    async () => {
      runs += 1;
      return "charged";
    },
    { toolName: "charge_card" },
  );
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.equal(runs, 1);
  assert.equal(client.acquires.length, 2);
});

test("a call whose requests go unanswered fails, and its failure record waits out a lease it was never told about", async () => {
  // The engine may have granted the last request: a lease whose token this side
  // never saw, claiming the call's position until it lapses. The failure record
  // there is refused until then, and waits rather than being dropped.
  const client = scriptedClient([{ throws: 4 }, { throws: 4 }, { throws: 4 }, acquired(9n)], {
    recordAnswers: [{ throws: 9 }],
  });
  const charge = tool(async () => "charged", { toolName: "charge_card", ttlMs: 1000 });
  const next = tool(async () => "next", { toolName: "next_step" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), (err) => err.code === 4);
    assert.equal(await next(), "next");
  });
  assert.equal(client.acquires.length, 4);
  assert.equal(client.recordAcquires.length, 2);
  // A lease without its token cannot be released, only waited out.
  assert.deepEqual(client.releases, []);
  assert.deepEqual(
    client.order.filter((e) => e.startsWith("commit")),
    ["commit:1:charge_card", "commit:2:next_step"],
  );
});

test("a lease handed back unreleased is forgotten once the engine grants the key again", async () => {
  const clocks = fakeClocks();
  try {
    // The first grant is handed back and its release fails. The engine grants
    // the key again, so that lease is gone: the failure record for the body's
    // error must not wait for it or release it.
    const client = scriptedClient(
      [acquired(7n, 750n, { waited: 10_000, expiresInMs: 2900 }), acquired(8n, 750n), acquired(9n)],
      { clocks, failRenewalsFor: (token) => token === 7, failReleases: (token) => token === 7 },
    );
    const charge = tool(
      async () => {
        throw new Error("card declined");
      },
      { toolName: "charge_card", ttlMs: 3000 },
    );
    const next = tool(async () => "next", { toolName: "next_step" });
    await runWithContext(session(client), async () => {
      await assert.rejects(charge(), /card declined/);
      client.order.push("next");
      await next();
    });
    const afterNext = client.order.slice(client.order.indexOf("next"));
    assert.ok(!afterNext.includes("release:7"), afterNext.join(" "));
    assert.deepEqual(
      client.releases.map((r) => [r.fencingToken, r.reason]),
      [
        [7, "LEASE_UNCONFIRMED"],
        [8, "TOOL_ERROR"],
      ],
    );
  } finally {
    clocks.restore();
  }
});

test("an engine suggesting no back-off is still backed off from", async () => {
  const client = scriptedClient([inProgress({ retryAfterMs: 0n }), inProgress({ retryAfterMs: 0n }), acquired()]);
  const charge = tool(async () => "charged", { toolName: "charge_card" });
  const started = performance.now();
  await runWithContext(session(client), () => charge());
  const tookMs = performance.now() - started;
  assert.ok(tookMs >= 95, `two back-offs of at least 50 ms took ${tookMs} ms`);
});

test("a waiting request that gets no answer fails at its deadline", { timeout: 10_000 }, async () => {
  // An engine whose connection stays open but never answers, as one does when
  // the network drops while the engine holds a request.
  const server = http2.createServer();
  const sessions = [];
  server.on("session", (session) => sessions.push(session));
  server.on("stream", () => {});
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const client = new CellaflowClient({ target: `127.0.0.1:${server.address().port}` });
  // If the deadline regresses, the test's own timeout fires and the finally
  // below never runs: this still shuts the server, so the file can exit.
  const guard = setTimeout(() => {
    for (const session of sessions) session.destroy();
    server.close();
  }, 9_000);
  guard.unref();
  try {
    const started = performance.now();
    await assert.rejects(
      client.checkIdempotencyCache("agent", "key", 10_000, undefined, undefined, undefined, 0.3),
      (err) => err.code === 4,
    );
    const tookMs = performance.now() - started;
    assert.ok(tookMs < 3000, `the deadline fired after ${tookMs} ms`);
  } finally {
    clearTimeout(guard);
    client.close();
    for (const session of sessions) session.destroy();
    await new Promise((resolve) => server.close(resolve));
  }
});

test("only failures since the last answer count towards giving up", async () => {
  // A long wait can meet the odd dropped connection; each answer in between
  // shows the engine is there.
  const lost = { throws: 14 };
  const client = scriptedClient([lost, inProgress({ retryAfterMs: 1n }), lost, inProgress({ retryAfterMs: 1n }), lost, acquired()]);
  const charge = tool(async () => "charged", { toolName: "charge_card" });
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.equal(client.acquires.length, 6);
});

test("the lease that records a received result is asked again after a request that got no answer", async () => {
  const hit = {
    status: CacheStatus.HIT,
    currentSequence: 0n,
    cachedResult: { outputPayload: serialize({ result: "placed-elsewhere" }) },
  };
  const client = scriptedClient([hit, acquired()], { recordAnswers: [{ throws: 4 }] });
  const place = tool(async () => "placed-here", { toolName: "place_order" });
  const confirm = tool(async () => "confirmed", { toolName: "send_confirmation" });
  await runWithContext(session(client), async () => {
    await place();
    assert.equal(await confirm(), "confirmed");
  });
  assert.equal(client.recordAcquires.length, 2);
  assert.deepEqual(
    client.order.filter((e) => e.startsWith("commit")),
    ["commit:1:place_order", "commit:2:send_confirmation"],
  );
});

test("a deadline longer than a timer can hold does not fail the call at once", { timeout: 10_000 }, async () => {
  // A server that answers UNAVAILABLE after 500 ms. A deadline that fired at
  // once would fail the call first, with DEADLINE_EXCEEDED.
  const server = http2.createServer();
  const sessions = [];
  server.on("session", (session) => sessions.push(session));
  server.on("stream", (stream) => {
    stream.on("error", () => {});
    setTimeout(() => {
      if (stream.destroyed) return;
      stream.respond({ ":status": 200, "content-type": "application/grpc" }, { waitForTrailers: true });
      stream.on("wantTrailers", () => stream.sendTrailers({ "grpc-status": "14", "grpc-message": "unavailable" }));
      stream.end();
    }, 500);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const client = new CellaflowClient({ target: `127.0.0.1:${server.address().port}` });
  try {
    const started = performance.now();
    await assert.rejects(
      client.checkIdempotencyCache("agent", "key", 10_000, undefined, undefined, undefined, 2 ** 32),
      (err) => err.code === 14,
    );
    assert.ok(performance.now() - started >= 400);
  } finally {
    client.close();
    for (const session of sessions) session.destroy();
    await new Promise((resolve) => server.close(resolve));
  }
});
