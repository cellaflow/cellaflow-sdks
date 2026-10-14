// The coordination id: a session default, adopted on resume, overridable per
// call, and the input to every SHARED key. Runs against dist/ with a fake engine
// client, so no engine is needed: `npm test`.
import { test, afterEach } from "node:test";
import assert from "node:assert/strict";

import {
  CellaflowClient,
  CacheStatus,
  IdempotencyScope,
  MAX_COORDINATION_ID_BYTES,
  WorkflowContext,
  durableTools,
  normalizeCoordinationId,
  sessionCoordinationId,
  tool,
} from "../dist/index.js";
import { runWithContext } from "../dist/context.js";

/** Records what a tool sends to the engine and answers like a fresh engine. */
function fakeClient() {
  const calls = { keys: [], commits: [] };
  return {
    calls,
    async checkIdempotencyCache(_agentId, idempotencyKey) {
      calls.keys.push(idempotencyKey);
      return { status: CacheStatus.ACQUIRED, fencingToken: 7n, heartbeatIntervalMs: 60_000n };
    },
    async commitStep(sessionId, sequence, name, status, payload, key, token, coordinationId) {
      calls.commits.push({ sequence, name, key, coordinationId });
      return {};
    },
    async releaseLease() {},
    async renewLease() {
      return { renewed: true };
    },
    close() {},
  };
}

function sessionUnder(coordinationId, client = fakeClient()) {
  const ctx = new WorkflowContext({ client, sessionId: "s-1", workflowVersion: "1.0.0", coordinationId });
  return { ctx, client };
}

const ran = [];
const refund = tool(
  async ({ customer, cents }) => {
    ran.push({ customer, cents });
    return `refunded ${customer}`;
  },
  { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["customer"] },
);

afterEach(() => {
  ran.length = 0;
});

test("the session's coordination id keys shared calls, and the commit leaves it to the engine", async () => {
  const { ctx, client } = sessionUnder("ticket-4417");
  await runWithContext(ctx, () => refund({ customer: "customer-88", cents: 10 }));

  assert.ok(client.calls.keys[0].startsWith("shared:ticket-4417:refund:"), client.calls.keys[0]);
  assert.equal(client.calls.commits[0].coordinationId, undefined);
});

test("a call can override the coordination id", async () => {
  const { ctx, client } = sessionUnder("ticket-4417");
  const result = await runWithContext(ctx, () =>
    refund.withCoordinationId("customer-88")({ customer: "customer-88", cents: 10 }),
  );

  assert.equal(result, "refunded customer-88");
  assert.deepEqual(ran, [{ customer: "customer-88", cents: 10 }], "the tool receives only its own arguments");
  assert.ok(client.calls.keys[0].startsWith("shared:customer-88:refund:"), client.calls.keys[0]);
  assert.equal(client.calls.commits[0].coordinationId, "customer-88");
});

test("an override converges with a session whose default is that id", async () => {
  // One agent works a ticket and refunds the customer under the customer's id;
  // another works for the customer directly. They disagree on the amount, which
  // sharedOn leaves out of the key.
  const ticket = sessionUnder("ticket-4417");
  await runWithContext(ticket.ctx, () =>
    refund.withCoordinationId("customer-88")({ customer: "customer-88", cents: 10 }),
  );
  const customer = sessionUnder("customer-88");
  await runWithContext(customer.ctx, () => refund({ customer: "customer-88", cents: 25 }));

  assert.equal(ticket.client.calls.keys[0], customer.client.calls.keys[0]);
});

test("an empty override uses the session's coordination id", async () => {
  const { ctx, client } = sessionUnder("ticket-4417");
  await runWithContext(ctx, () => refund.withCoordinationId("")({ customer: "customer-88", cents: 10 }));

  assert.ok(client.calls.keys[0].startsWith("shared:ticket-4417:refund:"), client.calls.keys[0]);
  assert.equal(client.calls.commits[0].coordinationId, undefined);
});

for (const bad of ["ticket:4417", " t-1", "t-1 ", "\tt-1", "x".repeat(MAX_COORDINATION_ID_BYTES + 1)]) {
  test(`an invalid override ${JSON.stringify(bad.slice(0, 12))} fails before anything runs`, async () => {
    const { ctx, client } = sessionUnder("ticket-4417");
    await runWithContext(ctx, async () => {
      assert.throws(() => refund.withCoordinationId(bad));
      assert.deepEqual(ran, []);
      assert.deepEqual(client.calls.keys, [], "no lease may be taken");
      // And the failed call must not have consumed a sequence.
      await refund({ customer: "customer-88", cents: 10 });
    });
    assert.deepEqual(client.calls.commits.map((c) => c.sequence), [1]);
  });
}

test("normalizeCoordinationId applies the engine's rules", () => {
  assert.equal(normalizeCoordinationId(undefined), undefined);
  assert.equal(normalizeCoordinationId(null), undefined);
  assert.equal(normalizeCoordinationId(""), undefined);
  const longest = "x".repeat(MAX_COORDINATION_ID_BYTES);
  assert.equal(normalizeCoordinationId(longest), longest);
  // The limit is in UTF-8 bytes, as the engine counts it, not characters.
  assert.throws(() => normalizeCoordinationId("é".repeat(MAX_COORDINATION_ID_BYTES / 2 + 1)));
  assert.throws(() => normalizeCoordinationId(4417), TypeError);
  assert.throws(() => normalizeCoordinationId({ id: "t-1" }), TypeError);
});

test("a started session adopts the engine's stored coordination id", () => {
  assert.equal(sessionCoordinationId({ coordinationId: "ticket-4417" }, undefined), "ticket-4417");
  assert.equal(sessionCoordinationId({ coordinationId: "ticket-4417" }, "ticket-4417"), "ticket-4417");
  // An engine without the field returns nothing; the requested value is kept.
  assert.equal(sessionCoordinationId({ coordinationId: undefined }, "ticket-4417"), "ticket-4417");
  assert.equal(sessionCoordinationId({ coordinationId: undefined }, undefined), undefined);
});

/** Replaces the client's transport-level calls, returning what they received. */
function withInnerClient(responses) {
  const client = new CellaflowClient({ target: "localhost:1" });
  const sent = {};
  client.client = Object.fromEntries(
    Object.entries(responses).map(([method, response]) => [
      method,
      async (req) => {
        sent[method] = req;
        return response;
      },
    ]),
  );
  return { client, sent };
}

test("the client sends a coordination id only when there is one", async () => {
  const { client, sent } = withInnerClient({ startSession: {}, commitStep: {} });

  await client.startSession("wf", "1.0.0", "s-1", "ticket-4417");
  assert.equal(sent.startSession.coordinationId, "ticket-4417");
  await client.startSession("wf", "1.0.0", "s-1");
  assert.equal(sent.startSession.coordinationId, undefined);

  await client.commitStep("s-1", 1, "a", 1, { result: null }, "k", 7, "customer-88");
  assert.equal(sent.commitStep.stepResult.coordinationId, "customer-88");
  await client.commitStep("s-1", 2, "a", 1, { result: null }, "k", 7);
  assert.equal(sent.commitStep.stepResult.coordinationId, undefined);
});

test("getGraph returns each step's coordination id", async () => {
  const { client } = withInnerClient({
    getGraph: {
      steps: [
        { sequence: 1n, name: "a", status: 1, outputPayload: new Uint8Array(), coordinationId: "t-1" },
        { sequence: 2n, name: "b", status: 1, outputPayload: new Uint8Array() },
      ],
      nextCursor: "",
    },
  });
  const [steps] = await client.getGraph("s-1");
  assert.deepEqual(steps.map((s) => s.coordinationId), ["t-1", undefined]);
});

/**
 * durableTools builds its own client, so these tests stand in for the engine by
 * replacing the client's methods on its prototype for the duration of one test.
 */
function fakeEngine(startResponse) {
  const proto = CellaflowClient.prototype;
  const saved = {};
  const calls = { starts: [], keys: [], commits: [] };
  const fake = fakeClient();
  const replace = (name, fn) => {
    saved[name] = proto[name];
    proto[name] = fn;
  };
  replace("startSession", async (workflowId, version, sessionId, coordinationId) => {
    calls.starts.push({ sessionId, coordinationId });
    if (startResponse instanceof Error) throw startResponse;
    return { sessionId, version, ...startResponse };
  });
  replace("checkIdempotencyCache", async (...args) => {
    calls.keys.push(args[1]);
    return fake.checkIdempotencyCache(...args);
  });
  replace("commitStep", async (...args) => {
    calls.commits.push({ coordinationId: args[7] });
    return {};
  });
  return { calls, restore: () => Object.assign(proto, saved) };
}

test("durableTools sends the coordination id and runs under it", async () => {
  const engine = fakeEngine({ coordinationId: "ticket-4417" });
  try {
    await durableTools("thread-1", { coordinationId: "ticket-4417" }, async (session) => {
      assert.equal(session.context.coordinationId, "ticket-4417");
      await refund({ customer: "customer-88", cents: 10 });
    });
    assert.equal(engine.calls.starts[0].coordinationId, "ticket-4417");
    assert.ok(engine.calls.keys[0].startsWith("shared:ticket-4417:refund:"));
  } finally {
    engine.restore();
  }
});

test("a resumed durableTools block that names none adopts the stored id", async () => {
  const engine = fakeEngine({ coordinationId: "ticket-4417" });
  try {
    await durableTools("thread-1", async (session) => {
      assert.equal(session.context.coordinationId, "ticket-4417");
    });
    assert.equal(engine.calls.starts[0].coordinationId, undefined);
  } finally {
    engine.restore();
  }
});

test("against an engine without the field, the requested id is kept", async () => {
  const engine = fakeEngine({});
  try {
    await durableTools("thread-1", { coordinationId: "ticket-4417" }, async (session) => {
      assert.equal(session.context.coordinationId, "ticket-4417");
    });
  } finally {
    engine.restore();
  }
});

test("a resume refused by the engine never runs the callback", async () => {
  const refused = Object.assign(new Error("Coordination mismatch"), { code: 9 });
  const engine = fakeEngine(refused);
  let ranCallback = false;
  try {
    await assert.rejects(
      durableTools("thread-1", { coordinationId: "ticket-9999" }, async () => {
        ranCallback = true;
      }),
      /Coordination mismatch/,
    );
    assert.equal(ranCallback, false);
  } finally {
    engine.restore();
  }
});

test("an invalid durableTools coordination id fails before the session starts", async () => {
  const engine = fakeEngine({});
  try {
    await assert.rejects(durableTools("thread-1", { coordinationId: "ticket:4417" }, async () => {}));
    assert.deepEqual(engine.calls.starts, []);
  } finally {
    engine.restore();
  }
});
