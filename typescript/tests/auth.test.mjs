// Sending an API key: what reaches the engine, where it comes from, and where it
// must never appear. Runs against an in-process gRPC server, so no engine is
// needed.
import { test, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { Code, ConnectError } from "@connectrpc/connect";

import { CellaflowClient, resolveApiKey } from "../dist/client.js";
import { WorkflowEngineService } from "../dist/cellaflow/v1/service_connect.js";

const KEY = "cf_test_" + "k".repeat(43);
const received = [];
let expected = KEY;

const server = http2.createServer(
  connectNodeAdapter({
    routes(router) {
      router.service(WorkflowEngineService, {
        startSession(req, ctx) {
          const header = ctx.requestHeader.get("authorization");
          received.push(header);
          if (header !== `Bearer ${expected}`) {
            throw new ConnectError("Invalid or missing API key / authorization token", Code.Unauthenticated);
          }
          return { sessionId: req.sessionId || "s", version: req.version };
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

const ENV = ["CELLAFLOW_API_KEY", "CELLAFLOW_AUTH_TOKEN"];
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

test("the variables are read in the engine's order, and a blank key means none", () => {
  process.env.CELLAFLOW_API_KEY = "from-sdk-var";
  assert.equal(resolveApiKey(), "from-sdk-var");
  process.env.CELLAFLOW_AUTH_TOKEN = "from-engine-var";
  assert.equal(resolveApiKey(), "from-engine-var");
  assert.equal(resolveApiKey("passed"), "passed");
  assert.equal(resolveApiKey(""), undefined);
  assert.equal(resolveApiKey("   "), undefined);
  process.env.CELLAFLOW_AUTH_TOKEN = "  ";
  assert.equal(resolveApiKey(), "from-sdk-var");
});

test("generic variables are never read", () => {
  process.env.API_KEY = "someone-elses-secret";
  process.env.AUTH_TOKEN = "someone-elses-secret";
  try {
    assert.equal(resolveApiKey(), undefined);
  } finally {
    delete process.env.API_KEY;
    delete process.env.AUTH_TOKEN;
  }
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

test("sending a key without TLS to another host warns once", (t) => {
  const warnings = [];
  t.mock.method(console, "warn", (message) => warnings.push(message));
  new CellaflowClient({ target: "engine.example.com:50051", apiKey: KEY });
  new CellaflowClient({ target: "engine.example.com:50051", apiKey: KEY });
  new CellaflowClient({ target: "localhost:50051", apiKey: KEY });
  new CellaflowClient({ target: "engine.example.com:50051", secure: true, apiKey: KEY });
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /engine\.example\.com:50051 without TLS/);
  assert.ok(!warnings[0].includes(KEY));
});

test("a local target in any form gets no warning", (t) => {
  const warnings = [];
  t.mock.method(console, "warn", (message) => warnings.push(message));
  // A bare `::1` is not a URL Connect accepts, so it is not a target here.
  for (const local of ["localhost:50051", "127.0.0.1:50051", "[::1]:50051"]) {
    new CellaflowClient({ target: local, apiKey: KEY });
  }
  assert.deepEqual(warnings, []);
});
