// A cache hit answered from another session is recorded at its position in the
// receiving session, so the session's history matches the work it did. Runs
// against dist/ with a scripted engine client, so no engine is needed: `npm test`.
import { test } from "node:test";
import assert from "node:assert/strict";

import { CacheStatus, IdempotencyScope, WorkflowContext, tool } from "../dist/index.js";
import { serialize } from "../dist/serialization.js";
import { runWithContext } from "../dist/context.js";

/** Answers checks from `answers` in order, and records every commit. */
function scriptedClient(answers, { refuseFirstCommit = false } = {}) {
  const commits = [];
  let refused = false;
  return {
    commits,
    async checkIdempotencyCache() {
      const next = answers.shift();
      if (!next) throw new Error("no scripted answer left");
      return next;
    },
    async commitStep(sessionId, sequence, name, status, payload, key, token, coordinationId) {
      if (refuseFirstCommit && !refused) {
        refused = true;
        throw Object.assign(new Error("sequence already committed"), { code: 9 });
      }
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

function session(client, historyKnown) {
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

test("a position taken first stops recording hits instead of failing", async () => {
  ran.length = 0;
  const client = scriptedClient([hit(0), acquired()], { refuseFirstCommit: true });
  const ctx = session(client);
  await runWithContext(ctx, async () => {
    await placeOrder({ orderId: "4417" });
    assert.equal(await sendConfirmation({ orderId: "4417" }), "emailed");
  });
  assert.equal(ctx.historyKnown, false);
  assert.deepEqual(
    client.commits.map((c) => [c.sequence, c.name]),
    [[2, "send_confirmation"]],
  );
});
