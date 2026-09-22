import {
  durableTools, tool, IdempotencyScope, deriveIdempotencyKey, hashInputs,
  executionLease, taskLease, currentLease, LeaseNotAcquired, toolSessionId,
} from "../dist/index.js";
import assert from "node:assert/strict";

const rid = () => Math.random().toString(16).slice(2, 8);
let pass = 0, fail = 0;
const t = async (name, fn) => {
  try { await fn(); console.log(`  PASS  ${name}`); pass++; }
  catch (e) { console.log(`  FAIL  ${name}\n        ${e.message}`); fail++; }
};

// 1. Key derivation matches the Python reference hashes byte for byte.
await t("hashInputs matches Python rfc8785 reference", () => {
  assert.equal(hashInputs([], { order_id: "ORD-1", cents: 1999 }), "8004e5b025abe0bd702530cdfb094eb3");
  assert.equal(hashInputs([1, "two", true, null], {}), "bb3e04d2f8f90d9fea32e9b3eedee98f");
  assert.equal(hashInputs([], { unicode: "café ☕", esc: "line\nbreak\ttab" }), "08b350545fc4bdc8ae9212bfa27c7c21");
});

await t("SHARED scope omits session and requires coordinationId", () => {
  const k = deriveIdempotencyKey({
    sessionId: "s1", workflowVersion: "v9", stepSequence: 3, agentId: "a",
    toolName: "charge", scope: IdempotencyScope.SHARED, coordinationId: "ticket-1",
    kwargs: { amount: 40 },
  });
  assert.ok(k.startsWith("shared:ticket-1:charge:"), k);
  assert.ok(!k.includes("s1") && !k.includes("v9"));
  assert.throws(() => deriveIdempotencyKey({
    sessionId: "s1", workflowVersion: "v", stepSequence: 1, agentId: "a",
    toolName: "t", scope: IdempotencyScope.SHARED, kwargs: {},
  }), /requires a coordinationId/);
});

await t("toolSessionId hashes colons, is deterministic", () => {
  assert.equal(toolSessionId("t1"), "t1-cellaflow-tools");
  assert.equal(toolSessionId("user:123"), toolSessionId("user:123"));
  assert.ok(!toolSessionId("user:123").includes("user:123"));
});

// 2. tool() end to end: runs once, second call returns the value not the envelope.
await t("tool runs once; a second call is served from the record", async () => {
  const thread = `th-${rid()}`;
  let calls = 0;
  const charge = tool(async ({ orderId }) => { calls++; return { receipt: `r-${orderId}` }; }, { toolName: "charge" });
  await durableTools(thread, async () => {
    const a = await charge({ orderId: "O-1" });
    const b = await charge({ orderId: "O-1" });
    assert.deepEqual(a, { receipt: "r-O-1" });
    assert.deepEqual(b, { receipt: "r-O-1" }, "second call must unwrap {result}");
  });
  assert.equal(calls, 1, `body ran ${calls} times, expected 1`);
});

// 3. Survives process boundaries: a new session on the same thread id sees the record.
await t("a fresh session on the same thread id does not re-run", async () => {
  const thread = `th-${rid()}`;
  let calls = 0;
  const send = tool(async ({ to }) => { calls++; return { sent: to }; }, { toolName: "send" });
  await durableTools(thread, async () => { await send({ to: "x" }); });
  await durableTools(thread, async () => {
    const r = await send({ to: "x" });
    assert.deepEqual(r, { sent: "x" });
  });
  assert.equal(calls, 1, `body ran ${calls} times across two sessions, expected 1`);
});

// 4. Different arguments are different work.
await t("different arguments derive different keys and both run", async () => {
  const thread = `th-${rid()}`;
  let calls = 0;
  const f = tool(async ({ n }) => { calls++; return n * 2; }, { toolName: "double" });
  await durableTools(thread, async () => {
    assert.equal(await f({ n: 1 }), 2);
    assert.equal(await f({ n: 2 }), 4);
  });
  assert.equal(calls, 2);
});

// 5. execution lease: second holder refused.
await t("executionLease refuses a second holder", async () => {
  const key = `lock:${rid()}`;
  await executionLease(key, { workerId: "w1" }, async (lease) => {
    assert.ok(lease.fencingToken > 0);
    lease.check();
    await assert.rejects(
      executionLease(key, { workerId: "w2" }, async () => "never"),
      (e) => e instanceof LeaseNotAcquired,
    );
  });
});

// 6. currentLease reachable inside taskLease.
await t("taskLease exposes currentLease()", async () => {
  const run = taskLease(async (id) => { currentLease().check(); return `done-${id}`; },
    { workerId: "w1", key: (id) => `task:${id}:${rid()}` });
  assert.equal(await run("abc"), "done-abc");
});

// 7. The lease is released, so the key is takeable again.
await t("lease is released on block exit", async () => {
  const key = `lock:${rid()}`;
  await executionLease(key, { workerId: "w1" }, async () => {});
  await executionLease(key, { workerId: "w2" }, async (l) => { assert.ok(l.fencingToken > 0); });
});

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
