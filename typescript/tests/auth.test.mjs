// Sending an API key: what reaches the engine, where it comes from, and where it
// must never appear. Runs against an in-process gRPC server, so no engine is
// needed.
import { test, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { Code, ConnectError } from "@connectrpc/connect";

import { CellaflowClient, resolveApiKey } from "../dist/client.js";
import { durableTools } from "../dist/durable.js";
import { executionLease, taskLease } from "../dist/execution.js";
import { WorkflowEngineService } from "../dist/cellaflow/v1/service_connect.js";

const KEY = "cf_test_" + "k".repeat(43);
const received = [];
let expected = KEY;

function refuseWithoutTheKey(ctx) {
  const header = ctx.requestHeader.get("authorization");
  received.push(header);
  if (header !== `Bearer ${expected}`) {
    throw new ConnectError("Invalid or missing API key / authorization token", Code.Unauthenticated);
  }
}

const server = http2.createServer(
  connectNodeAdapter({
    routes(router) {
      router.service(WorkflowEngineService, {
        startSession(req, ctx) {
          refuseWithoutTheKey(ctx);
          return { sessionId: req.sessionId || "s", version: req.version };
        },
        // Lease calls are only ever refused here; what matters is the header.
        checkIdempotencyCache(_req, ctx) {
          refuseWithoutTheKey(ctx);
          throw new ConnectError("no leases here", Code.Unavailable);
        },
      });
    },
  }),
);
// Open HTTP/2 sessions keep the process alive after close(), so they are
// destroyed too, which also ends the clients' side of them.
const sessions = new Set();
server.on("session", (session) => {
  sessions.add(session);
  session.on("close", () => sessions.delete(session));
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const target = `127.0.0.1:${server.address().port}`;
after(() => {
  for (const session of sessions) session.destroy();
  server.close();
});

// Every variable a test sets, so none leaks into the next.
const ENV = [
  "CELLAFLOW_API_KEY",
  "CELLAFLOW_ALLOW_PLAINTEXT_KEY",
  "CELLAFLOW_AUTH_TOKEN",
  "AUTH_TOKEN",
  "API_KEY",
];
beforeEach(() => {
  received.length = 0;
  expected = KEY;
  for (const name of ENV) delete process.env[name];
});

test("the key is sent as a bearer token on every call", async () => {
  const client = new CellaflowClient({ target, apiKey: KEY });
  await client.startSession("wf", "1.0.0", "a");
  await client.startSession("wf", "1.0.0", "b");
  assert.deepEqual(received, [`Bearer ${KEY}`, `Bearer ${KEY}`]);
});

test("without a key nothing is sent, and a secured engine refuses", async () => {
  await assert.rejects(new CellaflowClient({ target }).startSession("wf", "1.0.0"), (err) => {
    assert.equal(err.code, Code.Unauthenticated);
    return true;
  });
  assert.deepEqual(received, [null]);
});

test("the key is read from the environment", async () => {
  process.env.CELLAFLOW_API_KEY = KEY;
  await new CellaflowClient({ target }).startSession("wf", "1.0.0");
  assert.deepEqual(received, [`Bearer ${KEY}`]);
});

test("only CELLAFLOW_API_KEY is read", () => {
  // The engine's variables configure the token it checks against, not a caller's
  // key, and one named API_KEY often holds another service's secret.
  for (const name of ["CELLAFLOW_AUTH_TOKEN", "AUTH_TOKEN", "API_KEY"]) {
    process.env[name] = "not-this-one";
  }
  assert.equal(resolveApiKey(), undefined);
  process.env.CELLAFLOW_API_KEY = "the-key";
  assert.equal(resolveApiKey(), "the-key");
  assert.equal(resolveApiKey("passed"), "passed");
});

test("a blank key means none", () => {
  process.env.CELLAFLOW_API_KEY = "  ";
  assert.equal(resolveApiKey(), undefined);
  process.env.CELLAFLOW_API_KEY = "the-key";
  // An empty option means no key, even with the variable set.
  assert.equal(resolveApiKey(""), undefined);
  assert.equal(resolveApiKey("   "), undefined);
});

for (const bad of ["MARKER\n", "MAR\tKER", "MAR\x7fKER", "MARKER\x00", "MARKÉR"]) {
  test(`a key that cannot be a header is refused without showing it (${JSON.stringify(bad)})`, () => {
    assert.throws(
      () => new CellaflowClient({ target, apiKey: bad }),
      (err) => !String(err.message).includes("MAR") && String(err.message).includes("the apiKey option"),
    );
  });
}

test("a refused key names the variable it came from", () => {
  process.env.CELLAFLOW_API_KEY = "from-a-secret-file\n";
  assert.throws(() => new CellaflowClient({ target }), /from CELLAFLOW_API_KEY/);
});

test("every entry point sends its key", async () => {
  const refused = () => true;
  const options = { target, apiKey: KEY, workerId: "w" };
  await assert.rejects(taskLease(async () => {}, { ...options, key: "task:1" })(), refused);
  await assert.rejects(executionLease("task:1", options, async () => {}), refused);
  // The key is right, so the session opens.
  await durableTools("thread-1", { target, apiKey: KEY }, async () => {});
  assert.equal(received.length, 3);
  assert.ok(received.every((header) => header === `Bearer ${KEY}`));
});

test("the key never appears in a refusal or in the client's own properties", async () => {
  expected = "the-right-key";
  const client = new CellaflowClient({ target, apiKey: KEY });
  let refusal;
  try {
    await client.startSession("wf", "1.0.0");
  } catch (err) {
    refusal = err;
  }
  assert.ok(refusal);
  for (const text of [String(refusal), JSON.stringify(refusal), JSON.stringify(client)]) {
    assert.ok(!text.includes(KEY));
  }
});

test("a key is not sent without TLS to another host unless allowed", () => {
  // Forgetting TLS must not be how a key crosses the network, as forgetting a
  // token cannot be how the engine serves an open port.
  const refusal = (err) =>
    /refused-host\.example\.com:50051 without TLS/.test(err.message) &&
    /CELLAFLOW_ALLOW_PLAINTEXT_KEY/.test(err.message) &&
    !err.message.includes(KEY);
  assert.throws(() => new CellaflowClient({ target: "refused-host.example.com:50051", apiKey: KEY }), refusal);
  // The key from the environment is held to the same rule.
  process.env.CELLAFLOW_API_KEY = KEY;
  assert.throws(() => new CellaflowClient({ target: "refused-host.example.com:50051" }), refusal);
  // No key, nothing to protect; TLS, nothing to refuse.
  delete process.env.CELLAFLOW_API_KEY;
  new CellaflowClient({ target: "refused-host.example.com:50051" });
  new CellaflowClient({ target: "refused-host.example.com:50051", secure: true, apiKey: KEY });
});

test("the variable allows it the way the engine reads its opt-out, and the option decides over it", (t) => {
  t.mock.method(console, "warn", () => {});
  for (const value of ["true", "TRUE", "1", "yes", " true "]) {
    process.env.CELLAFLOW_ALLOW_PLAINTEXT_KEY = value;
    new CellaflowClient({ target: "allowed.example.com:50051", apiKey: KEY });
  }
  for (const value of ["", "false", "0", "no", "on"]) {
    process.env.CELLAFLOW_ALLOW_PLAINTEXT_KEY = value;
    assert.throws(() => new CellaflowClient({ target: "allowed.example.com:50051", apiKey: KEY }), /without TLS/);
  }
  process.env.CELLAFLOW_ALLOW_PLAINTEXT_KEY = "true";
  assert.throws(
    () => new CellaflowClient({ target: "allowed.example.com:50051", apiKey: KEY, allowPlaintextKey: false }),
    /without TLS/,
  );
});

test("an allowed plaintext key warns once per target", (t) => {
  const warnings = [];
  t.mock.method(console, "warn", (message) => warnings.push(message));
  // Each test uses its own hosts: a target warns once per process, and nothing
  // resets that between tests.
  const options = { target: "warns-once.example.com:50051", apiKey: KEY, allowPlaintextKey: true };
  new CellaflowClient(options);
  new CellaflowClient(options);
  new CellaflowClient({ target: "localhost:50051", apiKey: KEY });
  new CellaflowClient({ target: "tls.example.com:50051", secure: true, apiKey: KEY });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /warns-once\.example\.com:50051 without TLS/);
  assert.ok(!warnings[0].includes(KEY));
});

test("a local target in any form is allowed without a warning", (t) => {
  const warnings = [];
  t.mock.method(console, "warn", (message) => warnings.push(message));
  // A bare `::1` is not a URL Connect accepts, so it is not a target here.
  for (const local of [
    "localhost:50051",
    "127.0.0.1:50051",
    "[::1]:50051",
    "[0:0:0:0:0:0:0:1]:50051",
    "LOCALHOST:50051",
    "localhost.:50051",
    "127.0.0.2:50051",
    "[::ffff:127.0.0.1]:50051",
  ]) {
    new CellaflowClient({ target: local, apiKey: KEY });
  }
  assert.deepEqual(warnings, []);
});

test("another host is refused, in any form, judged as the transport parses it", () => {
  for (const remote of [
    "[::ffff:10.0.0.5]:50051",
    "[2001:db8::1]:50051",
    // The URL's host is evil.com; `localhost:50051` is userinfo.
    "localhost:50051@evil.com",
    "localhost@evil.com:50051",
  ]) {
    assert.throws(() => new CellaflowClient({ target: remote, apiKey: KEY }), /without TLS/, remote);
  }
});

test("taskLease refuses a key it could not send when it is made, not on the first call", () => {
  const fn = async () => {};
  assert.throws(() => taskLease(fn, { key: "task:1", workerId: "w", apiKey: "MARKER\n" }), /the apiKey option/);
  assert.throws(
    () => taskLease(fn, { key: "task:1", workerId: "w", target: "remote.example.com:50051", apiKey: KEY }),
    /without TLS/,
  );
  // A blank key means none, so there is nothing to refuse.
  taskLease(fn, { key: "task:1", workerId: "w", target: "remote.example.com:50051", apiKey: " " });
});

test("the key helpers are exported from the package", async () => {
  const sdk = await import("../dist/index.js");
  assert.equal(sdk.resolveApiKey, resolveApiKey);
  assert.equal(sdk.API_KEY_ENV_VAR, "CELLAFLOW_API_KEY");
  assert.equal(sdk.ALLOW_PLAINTEXT_KEY_ENV_VAR, "CELLAFLOW_ALLOW_PLAINTEXT_KEY");
  // The checks themselves are not public API.
  assert.equal(sdk.checkApiKey, undefined);
  assert.equal(sdk.checkApiKeyOption, undefined);
});
