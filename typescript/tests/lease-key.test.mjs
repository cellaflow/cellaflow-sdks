// The idempotency key a tool body sees through currentLease(): the same key the
// call's lease was taken under, on every attempt, and the helpers for sending
// it to a destination that deduplicates. Runs against dist/ with a scripted
// engine client, so no engine is needed.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import { CacheStatus, IdempotencyScope, LeaseHandle, WorkflowContext, currentLease, tool } from "../dist/index.js";
import { runWithContext } from "../dist/context.js";

function scriptedClient(token = 41n) {
  const acquires = [];
  return {
    acquires,
    async checkIdempotencyCache(agentId, key) {
      acquires.push(key);
      return { status: CacheStatus.ACQUIRED, fencingToken: token, heartbeatIntervalMs: 250n };
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      return { renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) };
    },
    async commitStep() {
      return {};
    },
    async releaseLease() {
      return { released: true };
    },
    close() {},
  };
}

const session = (client, sessionId = "s-key") =>
  new WorkflowContext({ client, sessionId, workflowVersion: "1.0.0", coordinationId: "order-4417", historyKnown: true });

for (const [label, options, args] of [
  ["session-wide", {}, ["4417"]],
  ["step-local", { scope: IdempotencyScope.STEP_LOCAL }, ["4417"]],
  ["shared with sharedOn", { scope: IdempotencyScope.SHARED, sharedOn: ["order"] }, [{ order: "4417", note: "x" }]],
  ["agent-private", { scope: IdempotencyScope.AGENT_PRIVATE }, ["4417"]],
  ["explicit", { idempotencyKey: "refund:4417" }, ["4417"]],
]) {
  test(`currentLease().key is the key the lease was taken under: ${label}`, async () => {
    const client = scriptedClient();
    const refund = tool(async () => currentLease().key, { toolName: "refund", ...options });
    const key = await runWithContext(session(client), () => refund(...args));
    assert.equal(key, client.acquires[0]);
    assert.ok(key.length > 0);
    if (options.idempotencyKey) assert.equal(key, "refund:4417");
  });
}

test("two attempts at the same call see the same key and different fencing tokens", async () => {
  // A shared call: the holder in one session, and the worker that takes it
  // over in another, after the first holder's lease lapsed.
  const seen = [];
  const refund = tool(
    async ({ order }) => {
      const lease = currentLease();
      seen.push({ key: lease.key, token: lease.fencingToken });
      return order;
    },
    { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["order"] },
  );
  await runWithContext(session(scriptedClient(41n), "holder"), () => refund({ order: "4417" }));
  await runWithContext(session(scriptedClient(42n), "taker"), () => refund({ order: "4417" }));
  assert.equal(seen[0].key, seen[1].key);
  assert.notEqual(seen[0].token, seen[1].token);
});

test("keyDigest is the SHA-256 of the key, in hex", () => {
  const lease = new LeaseHandle(1, undefined, "shared:order-4417:refund:abc");
  assert.equal(lease.keyDigest, createHash("sha256").update("shared:order-4417:refund:abc").digest("hex"));
  assert.match(lease.keyDigest, /^[0-9a-f]{64}$/);
});

test("withIdempotencyKey sends the key's digest, keeps other headers, and leaves init alone", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  const init = { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" };
  const out = lease.withIdempotencyKey(init);
  assert.equal(out.method, "POST");
  assert.equal(out.body, "{}");
  assert.equal(out.headers.get("Idempotency-Key"), lease.keyDigest);
  assert.equal(out.headers.get("Content-Type"), "application/json");
  assert.equal(out.signal, lease.signal);
  assert.deepEqual(init.headers, { "Content-Type": "application/json" });
  assert.equal(init.signal, undefined);

  const fromHeaders = lease.withIdempotencyKey({ headers: new Headers([["X-Trace", "t-1"]]) });
  assert.equal(fromHeaders.headers.get("X-Trace"), "t-1");
  const fromPairs = lease.withIdempotencyKey({ headers: [["X-Trace", "t-2"]] });
  assert.equal(fromPairs.headers.get("X-Trace"), "t-2");
  assert.equal(lease.withIdempotencyKey().headers.get("Idempotency-Key"), lease.keyDigest);
  // One already there is replaced.
  const replaced = lease.withIdempotencyKey({ headers: { "Idempotency-Key": "mine" } });
  assert.equal(replaced.headers.get("Idempotency-Key"), lease.keyDigest);
});

test("withIdempotencyKey can send the key itself, under another header name", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  const out = lease.withIdempotencyKey({}, { raw: true, header: "PayPal-Request-Id" });
  assert.equal(out.headers.get("PayPal-Request-Id"), "refund:4417");
  assert.equal(out.headers.get("Idempotency-Key"), null);
});

test("a digest can be sent for a key a header could not carry", () => {
  // A coordination id outside ASCII cannot go in a header as it is.
  const lease = new LeaseHandle(1, undefined, "shared:\u8ba2\u5355-4417:refund:abc");
  assert.match(lease.withIdempotencyKey().headers.get("Idempotency-Key"), /^[0-9a-f]{64}$/);
  assert.throws(() => lease.withIdempotencyKey({}, { raw: true }), /cannot carry/);
});

for (const [label, bad] of [
  ["a line break", "a\r\nb"],
  ["a leading space", " padded"],
  ["a trailing space", "padded "],
  ["a tab", "tab\there"],
  ["Latin-1", "caf\u00e9"],
]) {
  test(`a raw key with ${label} is refused rather than sent altered`, () => {
    const lease = new LeaseHandle(1, undefined, bad);
    assert.throws(() => lease.withIdempotencyKey({}, { raw: true }), /cannot carry/);
    assert.match(lease.withIdempotencyKey().headers.get("Idempotency-Key"), /^[0-9a-f]{64}$/);
  });
}

test("a raw key with an inner space is sent as it is", () => {
  const lease = new LeaseHandle(1, undefined, "refund 4417");
  assert.equal(lease.withIdempotencyKey({}, { raw: true }).headers.get("Idempotency-Key"), "refund 4417");
});

test("the key cannot be reassigned", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  assert.throws(() => {
    lease.key = "other";
  }, TypeError);
  assert.equal(lease.key, "refund:4417");
});

test("a signal already in init is kept as it is, and can be combined by the caller", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  const mine = new AbortController();
  assert.equal(lease.withIdempotencyKey({ signal: mine.signal }).signal, mine.signal);
  assert.equal(lease.withIdempotencyKey({ method: "POST" }).signal, lease.signal);
  if (AbortSignal.any) {
    const both = lease.withIdempotencyKey({ signal: AbortSignal.any([mine.signal, lease.signal]) });
    lease.markLost("taken over");
    assert.equal(both.signal.aborted, true);
  }
});

test("a handle with no key refuses to send one", () => {
  const lease = new LeaseHandle(1);
  assert.throws(() => lease.keyDigest, /no idempotency key/);
  assert.throws(() => lease.withIdempotencyKey(), /no idempotency key/);
});

test("a call under its own coordination id sees the key derived from it", async () => {
  const client = scriptedClient();
  const refund = tool(async () => currentLease().key, {
    toolName: "refund",
    scope: IdempotencyScope.SHARED,
    sharedOn: ["order"],
  });
  const key = await runWithContext(session(client), () => refund.withCoordinationId("customer-88")({ order: "4417" }));
  assert.equal(key, client.acquires[0]);
  assert.ok(key.includes("customer-88"), key);
});

test("an executionLease's handle carries its key", async () => {
  const { executionLease } = await import("../dist/index.js");
  const { CellaflowClient } = await import("../dist/client.js");
  const orig = {
    check: CellaflowClient.prototype.checkIdempotencyCache,
    renew: CellaflowClient.prototype.renewLease,
    release: CellaflowClient.prototype.releaseLease,
  };
  CellaflowClient.prototype.checkIdempotencyCache = async () => ({
    status: CacheStatus.ACQUIRED,
    fencingToken: 7n,
    heartbeatIntervalMs: 1000n,
  });
  CellaflowClient.prototype.renewLease = async () => ({ renewed: true });
  CellaflowClient.prototype.releaseLease = async () => ({ released: true });
  try {
    const key = await executionLease("task:abc-123", { workerId: "w-1" }, async (lease) => lease.key);
    assert.equal(key, "task:abc-123");
  } finally {
    CellaflowClient.prototype.checkIdempotencyCache = orig.check;
    CellaflowClient.prototype.renewLease = orig.renew;
    CellaflowClient.prototype.releaseLease = orig.release;
  }
});

for (const [label, key, digest] of [
  ["an ASCII key", "refund:4417", "393b2ae43cab6d289df9fc2c73f49ffb273b85ed5f2ca410005df7df803d210f"],
  ["a CJK key", "shared:\u8ba2\u5355-4417:refund:abc", "84e2e882b60d5980b7021b9db7d656b8d8f7a28ddee9343c77675e88c7f24443"],
]) {
  // Titles stay ASCII: Node 18.14's test runner cannot read others.
  test(`the digest of ${label} matches the Python SDK's`, () => {
    // Pinned values, shared with the Python tests: agents in either language
    // send the same digest for the same key.
    assert.equal(new LeaseHandle(1, undefined, key).keyDigest, digest);
  });
}

test("a Request passed as init is refused, since spreading it would drop its method and body", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  assert.throws(
    () => lease.withIdempotencyKey(new Request("https://payments.example/refunds", { method: "POST", body: "{}" })),
    TypeError,
  );
});

test("a body run without a lease of its own sees no enclosing lease", async () => {
  const { executionLease } = await import("../dist/index.js");
  const { CellaflowClient } = await import("../dist/client.js");
  const orig = {
    check: CellaflowClient.prototype.checkIdempotencyCache,
    renew: CellaflowClient.prototype.renewLease,
    release: CellaflowClient.prototype.releaseLease,
  };
  CellaflowClient.prototype.checkIdempotencyCache = async () => ({
    status: CacheStatus.ACQUIRED,
    fencingToken: 7n,
    heartbeatIntervalMs: 1000n,
  });
  CellaflowClient.prototype.renewLease = async () => ({ renewed: true });
  CellaflowClient.prototype.releaseLease = async () => ({ released: true });
  try {
    // An answer this SDK does not know runs the body without a lease.
    const client = scriptedClient();
    client.checkIdempotencyCache = async () => ({ status: 0 });
    const charge = tool(
      async () => {
        // After an await too: the body's whole async run is outside any lease.
        await new Promise((r) => setTimeout(r, 5));
        try {
          return currentLease().key;
        } catch {
          return "no lease";
        }
      },
      { toolName: "charge_card" },
    );
    const seen = await executionLease("lock:account-7", { workerId: "w-1" }, () =>
      runWithContext(session(client), () => charge()),
    );
    assert.equal(seen, "no lease");
  } finally {
    CellaflowClient.prototype.checkIdempotencyCache = orig.check;
    CellaflowClient.prototype.renewLease = orig.renew;
    CellaflowClient.prototype.releaseLease = orig.release;
  }
});

test("each of several requests gets its own key, the same on every attempt", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  const again = new LeaseHandle(2, undefined, "refund:4417");
  assert.equal(lease.keyDigestFor("payment"), again.keyDigestFor("payment"));
  assert.notEqual(lease.keyDigestFor("payment"), lease.keyDigestFor("customer"));
  assert.notEqual(lease.keyDigestFor("payment"), lease.keyDigest);
  // Pinned, shared with the Python tests.
  assert.equal(lease.keyDigestFor("payment"), "531f4ed05fa09544dfd9db3ebea89affec6e983821c53a6914edec69592d5b14");
  assert.equal(
    lease.withIdempotencyKey({}, { part: "payment" }).headers.get("Idempotency-Key"),
    lease.keyDigestFor("payment"),
  );
  assert.throws(() => lease.withIdempotencyKey({}, { part: "payment", raw: true }), TypeError);
});

test("the advice for a Request keeps the request's own headers", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  const request = new Request("https://payments.example/refunds", {
    method: "POST",
    headers: { Authorization: "Bearer t", "Content-Type": "application/json" },
    body: "{}",
  });
  const init = lease.withIdempotencyKey({ headers: request.headers });
  assert.equal(init.headers.get("Authorization"), "Bearer t");
  assert.equal(init.headers.get("Content-Type"), "application/json");
  assert.equal(init.headers.get("Idempotency-Key"), lease.keyDigest);
  // A Request-like object from another realm or package is refused too.
  assert.throws(() => lease.withIdempotencyKey({ url: "https://x", clone() {} }), TypeError);
});

test("a missing, empty or malformed part is refused rather than taken for the call's key", () => {
  const lease = new LeaseHandle(1, undefined, "refund:4417");
  for (const bad of [undefined, "", 7, "\uD800"]) {
    assert.throws(() => lease.keyDigestFor(bad), TypeError, String(bad));
    assert.throws(() => lease.withIdempotencyKey({}, { part: bad }), TypeError, String(bad));
  }
  // A whole surrogate pair is fine, and pinned with the Python tests.
  assert.equal(lease.keyDigestFor("\uD83D\uDE00"), "390afccc84e99e8dd21ced1c6182206223f4781427658e5ab1a44c5ad1939d5f");
  assert.equal(
    lease.withIdempotencyKey({}, { part: "payment", header: "PayPal-Request-Id" }).headers.get("PayPal-Request-Id"),
    lease.keyDigestFor("payment"),
  );
});
