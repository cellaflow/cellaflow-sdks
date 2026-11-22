// Who holds a lease that executionLease was refused: LeaseNotAcquired carries
// the holder the engine reported, and nothing when it reported none. Runs
// against dist/ with the client's engine calls replaced, so no engine is needed.
import { test } from "node:test";
import assert from "node:assert/strict";

import { CacheStatus, LeaseNotAcquired, executionLease, taskLease } from "../dist/index.js";
import { CellaflowClient } from "../dist/client.js";

/** Answers every acquisition with `response`, for the length of `fn`. */
async function withEngineAnswer(response, fn) {
  const orig = CellaflowClient.prototype.checkIdempotencyCache;
  CellaflowClient.prototype.checkIdempotencyCache = async () => response;
  try {
    return await fn();
  } finally {
    CellaflowClient.prototype.checkIdempotencyCache = orig;
  }
}

const refusal = (answer) =>
  withEngineAnswer(answer, () =>
    executionLease("task:4417", { workerId: "agent-b" }, async () => assert.fail("the body ran")).then(
      () => assert.fail("the lease was granted"),
      (err) => err,
    ),
  );

const inProgress = (holder) => ({
  status: CacheStatus.IN_PROGRESS,
  leaseExpiresAtMs: 1_763_800_030_000n,
  ...(holder ? { currentHolder: holder } : {}),
});

test("a refusal names the worker holding the lease", async () => {
  const err = await refusal(
    inProgress({
      agentId: "agent-a",
      holderId: "holder-1",
      acquiredAtMs: 1_763_800_000_000n,
      expiresAtMs: 1_763_800_030_000n,
      hardExpiresAtMs: 1_763_800_600_000n,
    }),
  );
  assert.ok(err instanceof LeaseNotAcquired);
  assert.deepEqual(err.holder, {
    workerId: "agent-a",
    holderId: "holder-1",
    acquiredAtMs: 1_763_800_000_000,
    expiresAtMs: 1_763_800_030_000,
    hardExpiresAtMs: 1_763_800_600_000,
  });
  assert.ok(Object.isFrozen(err.holder));
});

test("the message names the worker holding the lease", async () => {
  const err = await refusal(
    inProgress({ agentId: "agent-a", holderId: "holder-1", acquiredAtMs: 1n, expiresAtMs: 2n, hardExpiresAtMs: 0n }),
  );
  assert.equal(
    err.message,
    "Could not acquire execution lease 'task:4417' (held by agent-a). Another worker is running it. " +
      "Raise waitMs to wait for them, or treat this as the signal that the work is already in hand.",
  );
});

test("what the engine did not record is absent, not empty or 0", async () => {
  const err = await refusal(
    inProgress({ agentId: "", holderId: "holder-1", acquiredAtMs: 0n, expiresAtMs: 2n, hardExpiresAtMs: 0n }),
  );
  assert.deepEqual(err.holder, { holderId: "holder-1", expiresAtMs: 2 });
  // No worker to name, so no "(held by ...)", and never the engine's opaque id.
  assert.match(err.message, /^Could not acquire execution lease 'task:4417'\. Another worker/);
  assert.equal("workerId" in err.holder, false);
  assert.equal("acquiredAtMs" in err.holder, false);
  assert.equal("hardExpiresAtMs" in err.holder, false);
});

test("an engine that reports no holder gives no holder", async () => {
  const err = await refusal(inProgress(undefined));
  assert.ok(err instanceof LeaseNotAcquired);
  assert.equal(err.holder, undefined);
  assert.equal("holder" in err, false);
  assert.doesNotMatch(err.message, /held by/);
});

test("a committed key has no holder", async () => {
  // A holder in the answer too, so this fails if a HIT were read for one.
  const err = await refusal({
    status: CacheStatus.HIT,
    currentHolder: { agentId: "agent-a", holderId: "holder-1", acquiredAtMs: 1n, expiresAtMs: 2n, hardExpiresAtMs: 0n },
  });
  assert.ok(err instanceof LeaseNotAcquired);
  assert.match(err.message, /already committed/);
  assert.equal("holder" in err, false);
});

test("taskLease's refusal carries the holder too", async () => {
  const run = taskLease(async () => assert.fail("the body ran"), { workerId: "agent-b", key: (id) => `task:${id}` });
  const err = await withEngineAnswer(
    inProgress({ agentId: "agent-a", holderId: "holder-1", acquiredAtMs: 1n, expiresAtMs: 2n, hardExpiresAtMs: 0n }),
    () => run("4417").then(() => assert.fail("the lease was granted"), (e) => e),
  );
  assert.ok(err instanceof LeaseNotAcquired);
  assert.equal(err.holder?.workerId, "agent-a");
});

test("LeaseNotAcquired can be built without a holder, as before", () => {
  const err = new LeaseNotAcquired("busy");
  assert.equal(err.message, "busy");
  assert.equal(err.name, "LeaseNotAcquired");
  assert.equal("holder" in err, false);
});
