// A cache hit answered from another session is recorded at its position in the
// receiving session, so the session's history matches the work it did. Runs
// against dist/ with a scripted engine client, so no engine is needed: `npm test`.
import { test } from "node:test";
import assert from "node:assert/strict";

import { CacheStatus, IdempotencyScope, WorkflowContext, tool } from "../dist/index.js";
import { serialize } from "../dist/serialization.js";
import { runWithContext } from "../dist/context.js";

/** Answers checks from `answers` in order, and records every commit. */
function scriptedClient(
  answers,
  {
    refuseFirstCommit = false,
    refuseCode = 9,
    failFirstCommit = false,
    failCode = 14,
    history = [],
    failGraph = false,
    unknownSession = false,
  } = {},
) {
  const commits = [];
  const order = [];
  const graphCursors = [];
  let refused = false;
  let failed = false;
  return {
    commits,
    order,
    graphCursors,
    async getGraph(sessionId, limit, cursor) {
      graphCursors.push(cursor);
      if (failGraph) throw Object.assign(new Error("engine unavailable"), { code: 14 });
      if (unknownSession) throw Object.assign(new Error("session not found"), { code: 5 });
      const from = cursor === undefined ? 1 : Number(cursor);
      const steps = history.filter((sequence) => sequence >= from);
      return [steps.map((sequence) => ({ sequence: BigInt(sequence) })), undefined];
    },
    async checkIdempotencyCache() {
      order.push("check");
      const next = answers.shift();
      if (!next) throw new Error("no scripted answer left");
      return next;
    },
    async commitStep(sessionId, sequence, name, status, payload, key, token, coordinationId) {
      if (refuseFirstCommit && !refused) {
        refused = true;
        throw refuseCode === null
          ? new TypeError("cannot serialize")
          : Object.assign(new Error("refused"), { code: refuseCode });
      }
      if (failFirstCommit && !failed) {
        failed = true;
        throw Object.assign(new Error("engine unavailable"), { code: failCode });
      }
      await new Promise((r) => setTimeout(r, 5));
      order.push(`commit:${sequence}:${name}`);
      commits.push({ sequence, name, payload, key, token, coordinationId });
      return {};
    },
    async releaseLease() {},
    async renewLease() {
      return { renewed: true };
    },
    close() {},
  };
}

/** A hit from another session's commit, with this session at `now`. */
const hit = (now, result = "ORD-1") => ({
  status: CacheStatus.HIT,
  currentSequence: BigInt(now),
  cachedResult: { outputPayload: serialize({ result }) },
});
const acquired = () => ({ status: CacheStatus.ACQUIRED, fencingToken: 7n, heartbeatIntervalMs: 60_000n });

const ran = [];
const placeOrder = tool(
  async ({ orderId }) => {
    ran.push("place");
    return "placed-here";
  },
  { toolName: "place_order", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] },
);
const sendConfirmation = tool(
  async ({ orderId }) => {
    ran.push("send");
    return "emailed";
  },
  { toolName: "send_confirmation", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] },
);

function session(client, historyKnown = true) {
  return new WorkflowContext({
    client,
    sessionId: "s-1",
    workflowVersion: "1.0.0",
    coordinationId: "order-4417",
    historyKnown,
  });
}

test("a hit from another session is recorded at its position before the next step", async () => {
  ran.length = 0;
  const client = scriptedClient([hit(0), acquired()]);
  await runWithContext(session(client), async () => {
    assert.equal(await placeOrder({ orderId: "4417" }), "ORD-1");
    assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
  });

  assert.deepEqual(ran, ["send"]);
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "place_order"],
      [2, "send_confirmation"],
    ],
  );
  // A plain step: the result under the tool's name, with no lease behind it.
  const recorded = client.commits[0];
  assert.deepEqual(recorded.payload, { result: "ORD-1" });
  assert.equal(recorded.key, undefined);
  assert.equal(recorded.token, undefined);
});

test("a run ending on a hit records nothing", async () => {
  const client = scriptedClient([hit(0)]);
  await runWithContext(session(client), () => placeOrder({ orderId: "4417" }));
  assert.deepEqual(client.commits, []);
});

test("a resumed session does not record hits", async () => {
  // durableTools passes historyKnown: false on resume. The session may already
  // hold these steps, so recording them again would duplicate them.
  const client = scriptedClient([hit(2), hit(2, "emailed")]);
  await runWithContext(session(client, false), async () => {
    await placeOrder({ orderId: "4417" });
    await sendConfirmation({ orderId: "4417" });
  });
  assert.deepEqual(client.commits, []);
});

test("a session further along than the run counted is not recorded into", async () => {
  // The first hit reports 5 while the call is at 1. The second reports 5 at 6,
  // which would otherwise look recordable.
  const client = scriptedClient([hit(5), hit(5, "emailed"), acquired()]);
  const third = tool(async () => "mine", { toolName: "mine" });
  await runWithContext(session(client), async () => {
    await placeOrder({ orderId: "4417" });
    await sendConfirmation({ orderId: "4417" });
    await third();
  });
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[6, "mine"]],
  );
});

test("a position taken first stops recording and continues from the real position", async () => {
  // Another writer committed positions 1 and 2 meanwhile.
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true, history: [1, 2] });
  const ctx = session(client);
  const warn = console.warn;
  const warnings = [];
  console.warn = (msg) => warnings.push(msg);
  try {
    await runWithContext(ctx, async () => {
      await placeOrder({ orderId: "4417" });
      assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
    });
  } finally {
    console.warn = warn;
  }
  assert.equal(ctx.historyKnown, false);
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[3, "send_confirmation"]],
  );
  assert.match(warnings[0], /was not recorded at position 1/);
});

test("a record that fails to write is kept and sent again before anything else", async () => {
  const client = scriptedClient([hit(0), acquired()], { failFirstCommit: true });
  const ctx = session(client);
  await runWithContext(ctx, async () => {
    await placeOrder({ orderId: "4417" });
    // The next call fails before reaching the engine, and its body never runs.
    await assert.rejects(sendConfirmation({ orderId: "4417" }), /unavailable/);
  });
  assert.deepEqual(client.order, ["check"]);
  // Retried as is, it sends the record first, then itself at the next position.
  await runWithContext(ctx, () => sendConfirmation({ orderId: "4417" }));
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "place_order"],
      [2, "send_confirmation"],
    ],
  );
});

test("concurrent calls after a hit wait for its record and keep call order", async () => {
  const client = scriptedClient([hit(0), acquired(), acquired()]);
  const first = tool(async () => "a", { toolName: "first" });
  const second = tool(async () => "b", { toolName: "second" });
  await runWithContext(session(client), async () => {
    await placeOrder({ orderId: "4417" });
    await Promise.all([first(), second()]);
  });
  // The record lands before either call reaches the engine, and the two calls
  // keep the positions they claimed in call order.
  assert.equal(client.order[1], "commit:1:place_order");
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]).sort(),
    [
      [1, "place_order"],
      [2, "first"],
      [3, "second"],
    ],
  );
});

test("reconcileSequence stays synchronous", () => {
  const ctx = session(scriptedClient([]));
  assert.equal(ctx.reconcileSequence(), undefined);
});

// Calls in flight together. A hit is recorded only while it is the only call in
// flight. With overlap, the run behaves as it did before hits were recorded: it
// adopts the position the engine reported, and the next call claims from there.

test("two hits in flight together record nothing, and the next call claims the real position", async () => {
  const client = scriptedClient([hit(0), hit(0, "emailed"), acquired()]);
  const mine = tool(async () => "mine", { toolName: "mine" });
  const ctx = session(client);
  await runWithContext(ctx, async () => {
    await Promise.all([placeOrder({ orderId: "4417" }), sendConfirmation({ orderId: "4417" })]);
    assert.equal(await mine(), "mine");
  });
  assert.equal(ctx.historyKnown, false);
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "mine"]],
  );
});

test("a hit while another call runs is not recorded", async () => {
  let release;
  const released = new Promise((r) => (release = r));
  const slow = tool(
    async () => {
      await released;
      return "slow";
    },
    { toolName: "slow" },
  );
  const client = scriptedClient([acquired(), hit(0)]);
  await runWithContext(session(client), async () => {
    const running = slow();
    assert.equal(await placeOrder({ orderId: "4417" }), "ORD-1");
    release();
    assert.equal(await running, "slow");
  });
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "slow"]],
  );
});

test("a record the engine refuses is dropped, and the run continues from the session's tail", async () => {
  // INVALID_ARGUMENT will never pass, so keeping the record would fail every
  // later call in the session.
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true, refuseCode: 3 });
  const ctx = session(client);
  const warn = console.warn;
  const warnings = [];
  console.warn = (msg) => warnings.push(msg);
  try {
    await runWithContext(ctx, async () => {
      await placeOrder({ orderId: "4417" });
      assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
    });
  } finally {
    console.warn = warn;
  }
  // Only the tail from the record's position was read.
  assert.deepEqual(client.graphCursors, ["1"]);
  assert.equal(ctx.historyKnown, false);
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "send_confirmation"]],
  );
  assert.match(warnings[0], /the engine refused it \(3\)/);
});

test("a failed position read keeps the record for the next call", async () => {
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true, failGraph: true });
  const ctx = session(client);
  await runWithContext(ctx, async () => {
    await placeOrder({ orderId: "4417" });
    await assert.rejects(sendConfirmation({ orderId: "4417" }), /unavailable/);
  });
  assert.equal(ctx.historyKnown, true);
  assert.equal(ctx.sequence, 1);
  await runWithContext(ctx, () => sendConfirmation({ orderId: "4417" }));
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [
      [1, "place_order"],
      [2, "send_confirmation"],
    ],
  );
});

test("a record that cannot be sent is dropped rather than retried forever", async () => {
  // An error with no status code was thrown before the record reached the engine,
  // and fails the same way every time.
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true, refuseCode: null });
  const ctx = session(client);
  const warn = console.warn;
  const warnings = [];
  console.warn = (msg) => warnings.push(msg);
  try {
    await runWithContext(ctx, async () => {
      await placeOrder({ orderId: "4417" });
      assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
    });
  } finally {
    console.warn = warn;
  }
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "send_confirmation"]],
  );
  assert.match(warnings[0], /it could not be sent \(TypeError: cannot serialize\)/);
});

test("a session the engine does not know reads as empty when the record is refused", async () => {
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true, refuseCode: 3, unknownSession: true });
  const ctx = session(client);
  const warn = console.warn;
  console.warn = () => {};
  try {
    await runWithContext(ctx, async () => {
      await placeOrder({ orderId: "4417" });
      assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
    });
  } finally {
    console.warn = warn;
  }
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "send_confirmation"]],
  );
});

test("a context built by hand does not record hits", async () => {
  const client = scriptedClient([hit(0), acquired()]);
  const ctx = new WorkflowContext({ client, sessionId: "s-hand", workflowVersion: "1.0.0", coordinationId: "order-4417" });
  await runWithContext(ctx, async () => {
    await placeOrder({ orderId: "4417" });
    await sendConfirmation({ orderId: "4417" });
  });
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[1, "send_confirmation"]],
  );
});

for (const [label, failCode] of [
  ["the engine's failed storage write (INTERNAL)", 13],
  ["a reset connection", "ECONNRESET"],
]) {
  test(`a record that fails with ${label} is kept and sent again`, async () => {
    const client = scriptedClient([hit(0), acquired()], { failFirstCommit: true, failCode });
    const ctx = session(client);
    await runWithContext(ctx, async () => {
      await placeOrder({ orderId: "4417" });
      await assert.rejects(sendConfirmation({ orderId: "4417" }), /unavailable/);
    });
    assert.equal(ctx.historyKnown, true);
    assert.deepEqual(client.graphCursors, []);
    await runWithContext(ctx, () => sendConfirmation({ orderId: "4417" }));
    assert.deepEqual(
      client.commits.map((c) => [c.sequence, c.name]),
      [
        [1, "place_order"],
        [2, "send_confirmation"],
      ],
    );
  });
}
