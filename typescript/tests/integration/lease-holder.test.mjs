// Who holds a lease, against a real engine: a second worker refused while the
// first holds the lease is told the first worker's id, and once the first
// releases it a third worker takes the lease with nothing to refuse. Skipped
// unless CELLAFLOW_TARGET names an engine.
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { LeaseNotAcquired, executionLease } from "../../dist/index.js";

const target = process.env.CELLAFLOW_TARGET;
const engine = { skip: target ? false : "needs an engine: set CELLAFLOW_TARGET", timeout: 30_000 };

test("a refused worker learns who holds the lease", engine, async () => {
  const key = `task:${randomUUID()}`;
  const first = `agent-a-${randomUUID().slice(0, 8)}`;

  const refusal = await executionLease(key, { workerId: first, target, ttlMs: 10_000 }, () =>
    executionLease(key, { workerId: "agent-b", target }, async () => assert.fail("granted a held lease")).then(
      () => assert.fail("granted a held lease"),
      (err) => err,
    ),
  );

  assert.ok(refusal instanceof LeaseNotAcquired, String(refusal));
  const { holder } = refusal;
  assert.ok(holder, "the refusal names no holder");
  assert.equal(holder.workerId, first);
  assert.ok(holder.engineHolderId);
  // Times are on the engine's clock, which need not be this one, so they are
  // checked against each other rather than against Date.now().
  assert.ok(holder.acquiredAtMs > 0, String(holder.acquiredAtMs));
  // The holder's own lease: its 10 s TTL (plus a renewal, every 2.5 s, on a
  // slow runner), not the refused worker's 30 s default, under the ceiling.
  const ttl = holder.expiresAtMs - holder.acquiredAtMs;
  assert.ok(ttl >= 9_000 && ttl <= 14_000, String(ttl));
  assert.ok(holder.hardExpiresAtMs > holder.expiresAtMs, String(holder.hardExpiresAtMs));

  // Released when the block exited, so the key is free again.
  const token = await executionLease(key, { workerId: "agent-c", target }, async (lease) => lease.fencingToken);
  assert.ok(token > 0);
});
