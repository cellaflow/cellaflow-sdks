// The SDK against a real engine that requires a token. Skipped unless
// CELLAFLOW_AUTH_TARGET names such an engine and CELLAFLOW_AUTH_TEST_KEY is its
// token, for example:
//
//   docker run -d -p 50052:50051 -e CELLAFLOW_API_KEY=s3cret \
//     ghcr.io/cellaflow/cellaflow:0.2.0
//   CELLAFLOW_AUTH_TARGET=localhost:50052 CELLAFLOW_AUTH_TEST_KEY=s3cret \
//     npm run test:integration
import { test, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Code } from "@connectrpc/connect";

import { durableTools, executionLease, tool } from "../../dist/index.js";

const target = process.env.CELLAFLOW_AUTH_TARGET;
const key = process.env.CELLAFLOW_AUTH_TEST_KEY;
const secured = {
  skip: target && key ? false : "needs an engine that requires a token: set CELLAFLOW_AUTH_TARGET and CELLAFLOW_AUTH_TEST_KEY",
};
const unique = () => randomUUID().slice(0, 8);
const reserveSeat = tool(async (flight) => `seat on ${flight}`, { toolName: "reserve_seat" });

beforeEach(() => {
  delete process.env.CELLAFLOW_API_KEY;
});

test("durableTools runs with the right key", secured, async () => {
  await durableTools(`auth-${unique()}`, { target, apiKey: key }, async () => {
    assert.equal(await reserveSeat("BA117"), "seat on BA117");
  });
});

for (const [label, apiKey] of [["a wrong key", "wrong-key"], ["no key", undefined]]) {
  test(`durableTools is refused with ${label}`, secured, async () => {
    await assert.rejects(
      durableTools(`auth-${unique()}`, { target, apiKey }, async () => {}),
      (err) => {
        assert.equal(err.code, Code.Unauthenticated);
        assert.ok(!String(err).includes(key));
        return true;
      },
    );
  });
}

test("the key is read from the environment", secured, async () => {
  process.env.CELLAFLOW_API_KEY = key;
  await durableTools(`auth-${unique()}`, { target }, async () => {
    assert.equal(await reserveSeat("BA117"), "seat on BA117");
  });
});

test("an execution lease runs with the right key", secured, async () => {
  await executionLease(`task:${unique()}`, { workerId: "w-1", target, apiKey: key }, async (lease) => {
    lease.check();
  });
});
