// The SDK against a real Cellaflow engine. Skipped unless CELLAFLOW_TARGET names
// one, for example:
//
//   docker run -d -p 50051:50051 -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
//     ghcr.io/cellaflow/cellaflow:0.2.0
//   CELLAFLOW_TARGET=localhost:50051 npm run test:integration
//
// These check what a stubbed engine cannot: that the engine accepts the records
// the SDK writes, and that a resumed session continues where it is.
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { CellaflowClient, IdempotencyScope, durableTools, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET" };
const unique = () => randomUUID().slice(0, 8);

/** A session's committed steps, as "position:name:ok|FAILED". */
async function history(sessionId) {
  const [steps] = await new CellaflowClient({ target }).getGraph(sessionId);
  return steps.map((s) => `${s.sequence}:${s.name}:${s.status === 3 ? "FAILED" : "ok"}`);
}

test("durableTools records a shared result, and a resume adds nothing", engine, async () => {
  const runs = { place: 0, send: 0 };
  const coordinationId = `order-${unique()}`;
  const placeOrder = tool(async ({ orderId }) => { runs.place += 1; return "ORD-1"; },
    { toolName: "place_order", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] });
  const sendConfirmation = tool(async ({ orderId }) => { runs.send += 1; return "emailed"; },
    { toolName: "send_confirmation", scope: IdempotencyScope.SHARED, sharedOn: ["orderId"] });
  const options = { target, coordinationId };

  await durableTools(`other-${unique()}`, options, () => placeOrder({ orderId: "4417" }));
  const thread = `agent-${unique()}`;
  let sessionId;
  runs.place = 0;
  await durableTools(thread, options, async (session) => {
    sessionId = session.sessionId;
    await placeOrder({ orderId: "4417" });
    await sendConfirmation({ orderId: "4417" });
  });
  assert.deepEqual(runs, { place: 0, send: 1 });
  const first = await history(sessionId);
  assert.deepEqual(first, ["1:place_order:ok", "2:send_confirmation:ok"]);

  runs.send = 0;
  await durableTools(thread, options, async () => {
    await placeOrder({ orderId: "4417" });
    await sendConfirmation({ orderId: "4417" });
  });
  assert.deepEqual(runs, { place: 0, send: 0 });
  assert.deepEqual(await history(sessionId), first);
});

test("a caught failure is recorded at its position", engine, async () => {
  const charge = tool(async () => { throw new Error("insufficient funds"); }, { toolName: "charge" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  let sessionId;
  await durableTools(`caught-${unique()}`, { target }, async (session) => {
    sessionId = session.sessionId;
    await assert.rejects(charge(), /insufficient funds/);
    assert.equal(await notify(), "notified");
  });
  assert.deepEqual(await history(sessionId), ["1:charge:FAILED", "2:notify:ok"]);
});

test("a resumed session runs a tool it never reached after what it holds", engine, async () => {
  const reserveSeat = tool(async (flight) => `seat on ${flight}`, { toolName: "reserve_seat" });
  const chargeCard = tool(async (amount) => `charged ${amount}`, { toolName: "charge_card" });
  const thread = `resume-${unique()}`;
  let sessionId;
  await durableTools(thread, { target }, async (session) => {
    sessionId = session.sessionId;
    await reserveSeat("BA117");
  });
  await durableTools(thread, { target }, async () => {
    assert.equal(await chargeCard(450), "charged 450");
  });
  assert.deepEqual(await history(sessionId), ["1:reserve_seat:ok", "2:charge_card:ok"]);
});
