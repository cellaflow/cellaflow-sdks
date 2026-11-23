// An engine refusing calls because the org is over its allowance: how the
// refusal is recognised from its status details, and that it is final
// everywhere the SDK calls the engine, never asked again, never taken for an
// answer that was lost, and never recorded as a step's failure. A full
// session mailbox, the other RESOURCE_EXHAUSTED, still passes as before. Runs
// against dist/ with scripted engine clients and in-process gRPC servers, so no
// engine is needed.
import { test, after } from "node:test";
import assert from "node:assert/strict";
import http2 from "node:http2";
import { inspect } from "node:util";
import { create, toBinary } from "@bufbuild/protobuf";
import { BinaryWriter, WireType } from "@bufbuild/protobuf/wire";
import { Code, ConnectError } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";

import {
  AllowanceExceededError,
  CacheStatus,
  CellaflowClient,
  StepStatus,
  WorkflowContext,
  durableTools,
  executionLease,
  tool,
} from "../dist/index.js";
import { runWithContext } from "../dist/context.js";
import { allowanceRefusal, hasStatusCode, isRetryable } from "../dist/errors.js";
import { LeaseHeartbeat } from "../dist/lease.js";
import { serialize } from "../dist/serialization.js";
import { MAX_UNANSWERED } from "../dist/wait.js";
import { WorkflowEngineService } from "../dist/cellaflow/v1/service_pb.js";
import { ErrorInfoSchema, QuotaFailureSchema } from "../dist/google/rpc/error_details_pb.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const RESETS_AT = "2026-12-01T00:00:00Z";
const RESETS_AT_MS = Date.UTC(2026, 11, 1);

/** A QuotaFailure detail as Connect hands it over from the wire: its type's name and its bytes. */
const quotaFailure = (...subjects) => ({
  type: "google.rpc.QuotaFailure",
  value: toBinary(QuotaFailureSchema, create(QuotaFailureSchema, { violations: subjects.map((subject) => ({ subject })) })),
});

/** An ErrorInfo detail, from the allowance's domain and with its reason unless told otherwise. */
const errorInfo = ({ domain = "cellaflow.com", reason = "ALLOWANCE_EXCEEDED", ...metadata } = {}) => ({
  type: "google.rpc.ErrorInfo",
  value: toBinary(ErrorInfoSchema, create(ErrorInfoSchema, { domain, reason, metadata })),
});

/** RESOURCE_EXHAUSTED as Connect raises it from the wire, carrying `details`. */
function exhausted(details = [], message = "steps allowance exhausted", code = Code.ResourceExhausted) {
  const err = new ConnectError(message, code);
  err.details = details;
  return err;
}

const allowanceDetails = () => [quotaFailure("cellaflow.steps"), errorInfo({ resets_at: RESETS_AT, limit: "writes_blocked" })];

/** What the SDK's client raises when the engine refuses a call over the allowance. */
const overAllowance = () => allowanceRefusal(exhausted(allowanceDetails()));

/** A session's full mailbox: RESOURCE_EXHAUSTED without the details, which passes. */
const mailboxFull = () => new ConnectError("session mailbox is full", Code.ResourceExhausted);

/** The engine unavailable: no answer either way. */
const unavailable = () => new ConnectError("engine unavailable", Code.Unavailable);

// --- Recognising the refusal ---------------------------------------------

test("an allowance refusal is recognised from its status details", () => {
  const raw = exhausted(allowanceDetails());
  const err = allowanceRefusal(raw);
  assert.ok(err instanceof AllowanceExceededError);
  assert.equal(err.subject, "cellaflow.steps");
  assert.equal(err.resetsAt.getTime(), RESETS_AT_MS);
  assert.equal(err.limit, "writes_blocked");
  assert.equal(err.cause, raw);
  // Says what happened in plain words, and keeps the engine's own.
  assert.match(err.message, /org is over its allowance for cellaflow\.steps/);
  assert.match(err.message, /resets at 2026-12-01T00:00:00Z/);
  assert.match(err.message, /steps allowance exhausted/);
  // The received details and metadata stay readable on it.
  assert.equal(err.details, raw.details);
  assert.equal(err.findDetails(QuotaFailureSchema)[0].violations[0].subject, "cellaflow.steps");
  // Converting it again changes nothing.
  assert.equal(allowanceRefusal(err), err);
});

test("an allowance refusal is still a ConnectError with code RESOURCE_EXHAUSTED", () => {
  const err = overAllowance();
  assert.ok(err instanceof ConnectError);
  assert.equal(err.code, Code.ResourceExhausted);
  assert.equal(err.code, 8);
  assert.equal(ConnectError.from(err), err);
  // Named ConnectError, for Connect's own check; Node's inspector shows the class.
  assert.equal(err.name, "ConnectError");
  assert.match(inspect(err), /AllowanceExceededError/);
});

test("a ConnectError is not taken for an allowance refusal", () => {
  // The class's instanceof check is its own: ConnectError's accepts anything
  // shaped like one, which would include every ConnectError.
  assert.equal(mailboxFull() instanceof AllowanceExceededError, false);
  assert.equal(exhausted(allowanceDetails()) instanceof AllowanceExceededError, false);
});

test("RESOURCE_EXHAUSTED without a cellaflow. quota violation is not an allowance refusal", () => {
  const plain = [
    ["no details", exhausted([])],
    ["a quota of another service", exhausted([quotaFailure("projects/123/quota")])],
    ["a subject without the dot", exhausted([quotaFailure("cellaflowsteps")])],
    ["a subject with the prefix inside it", exhausted([quotaFailure("x.cellaflow.steps")])],
    ["a subject in another case", exhausted([quotaFailure("Cellaflow.steps")])],
    ["an empty QuotaFailure", exhausted([quotaFailure()])],
    ["only the ErrorInfo", exhausted([errorInfo({ resets_at: RESETS_AT })])],
    ["another detail type", exhausted([{ type: "google.rpc.RetryInfo", value: new Uint8Array() }])],
  ];
  for (const [what, err] of plain) {
    assert.equal(allowanceRefusal(err), undefined, what);
    // Exactly as before: a full mailbox, which may pass.
    assert.equal(isRetryable(err), true, what);
  }
});

test("the same details under another status code are not an allowance refusal", () => {
  for (const code of [Code.FailedPrecondition, Code.Unavailable, Code.PermissionDenied]) {
    assert.equal(allowanceRefusal(exhausted(allowanceDetails(), "refused", code)), undefined, String(code));
  }
});

test("only a ConnectError is read, and reading never throws", () => {
  for (const value of [undefined, null, "resource_exhausted", 8, { code: 8, details: allowanceDetails() }, new Error("x")]) {
    assert.equal(allowanceRefusal(value), undefined);
  }
  const brokenDetails = exhausted([null]);
  assert.equal(allowanceRefusal(brokenDetails), undefined);
  const throwingDetails = exhausted();
  Object.defineProperty(throwingDetails, "details", {
    get() {
      throw new Error("no details for you");
    },
  });
  assert.equal(allowanceRefusal(throwingDetails), undefined);
  const throwingFind = exhausted(allowanceDetails());
  throwingFind.findDetails = () => {
    throw new Error("cannot find");
  };
  assert.equal(allowanceRefusal(throwingFind), undefined);
});

test("details that do not decode are not an allowance refusal", () => {
  const garbage = new Uint8Array([0xff, 0xff, 0xff, 0xff]);
  const undecodable = exhausted([{ type: "google.rpc.QuotaFailure", value: garbage }]);
  assert.equal(allowanceRefusal(undecodable), undefined);
  assert.equal(isRetryable(undecodable), true);
  // An ErrorInfo that does not decode only leaves what it would have said unknown.
  const err = allowanceRefusal(exhausted([quotaFailure("cellaflow.steps"), { type: "google.rpc.ErrorInfo", value: garbage }]));
  assert.ok(err instanceof AllowanceExceededError);
  assert.equal(err.resetsAt, undefined);
  assert.equal(err.limit, undefined);
});

test("an ErrorInfo from another domain, or with another reason, says nothing about the allowance", () => {
  for (const info of [
    errorInfo({ domain: "googleapis.com", resets_at: RESETS_AT, limit: "writes_blocked" }),
    errorInfo({ reason: "RATE_LIMITED", resets_at: RESETS_AT, limit: "writes_blocked" }),
  ]) {
    const err = allowanceRefusal(exhausted([quotaFailure("cellaflow.steps"), info]));
    assert.ok(err instanceof AllowanceExceededError);
    assert.equal(err.resetsAt, undefined);
    assert.equal(err.limit, undefined);
    assert.doesNotMatch(err.message, /resets at/);
  }
  // The one from the allowance's domain is read, wherever it is among them.
  const err = allowanceRefusal(
    exhausted([
      errorInfo({ domain: "googleapis.com", resets_at: "2030-01-01T00:00:00Z" }),
      quotaFailure("cellaflow.steps"),
      errorInfo({ resets_at: RESETS_AT, limit: "new_sessions_blocked" }),
    ]),
  );
  assert.equal(err.resetsAt.getTime(), RESETS_AT_MS);
  assert.equal(err.limit, "new_sessions_blocked");
});

test("resets_at and limit, present, absent and unparsable", () => {
  const read = (metadata) => allowanceRefusal(exhausted([quotaFailure("cellaflow.steps"), errorInfo(metadata)]));

  const absent = read({});
  assert.equal(absent.resetsAt, undefined);
  assert.equal(absent.limit, undefined);
  assert.equal("resetsAt" in absent, false);
  assert.equal("limit" in absent, false);
  assert.doesNotMatch(absent.message, /resets at/);
  assert.equal(read({ limit: "" }).limit, undefined);
  assert.equal(read({ limit: "writes_blocked" }).limit, "writes_blocked");

  const parsed = [
    [RESETS_AT, RESETS_AT_MS],
    ["2026-12-01T00:00:00.5Z", RESETS_AT_MS + 500],
    ["2026-12-01T00:00:00.123456789Z", RESETS_AT_MS + 123],
    ["2026-12-01T01:30:00+01:30", RESETS_AT_MS],
    ["2026-11-30T19:00:00-05:00", RESETS_AT_MS],
    ["2026-12-01t00:00:00z", RESETS_AT_MS],
    ["2028-02-29T00:00:00Z", Date.UTC(2028, 1, 29)],
  ];
  for (const [value, ms] of parsed) {
    const err = read({ resets_at: value, limit: "writes_blocked" });
    assert.equal(err.resetsAt?.getTime(), ms, value);
    assert.equal(err.limit, "writes_blocked", value);
  }

  const unparsable = [
    "",
    "soon",
    "2026-12-01",
    "December 1, 2026",
    "1796083200",
    " 2026-12-01T00:00:00Z",
    "2026-12-01 00:00:00Z",
    "2026-12-01T00:00:00",
    "2026-02-30T00:00:00Z",
    "2027-02-29T00:00:00Z",
    "2026-13-01T00:00:00Z",
    "2026-12-01T24:00:00Z",
    "2026-12-01T23:60:00Z",
    "2026-12-01T23:59:60Z",
    "2026-12-01T00:00:00+24:00",
    "2026-12-01T00:00:00+01:60",
    "0099-12-01T00:00:00Z",
  ];
  for (const value of unparsable) {
    const err = read({ resets_at: value, limit: "writes_blocked" });
    assert.ok(err instanceof AllowanceExceededError, value);
    assert.equal(err.resetsAt, undefined, value);
    assert.equal(err.limit, "writes_blocked", value);
  }
});

test("the first cellaflow. violation names the refusal", () => {
  const one = allowanceRefusal(exhausted([quotaFailure("projects/1/quota", "cellaflow.storage", "cellaflow.steps")]));
  assert.equal(one.subject, "cellaflow.storage");
  const across = allowanceRefusal(exhausted([quotaFailure("projects/1/quota"), quotaFailure("cellaflow.steps", "cellaflow.storage")]));
  assert.equal(across.subject, "cellaflow.steps");
});

test("details are matched by their type's name, sent or received", () => {
  // Built on this side, as a server or a test raises it.
  const sent = new ConnectError("steps allowance exhausted", Code.ResourceExhausted, undefined, [
    { desc: QuotaFailureSchema, value: { violations: [{ subject: "cellaflow.steps" }] } },
    { desc: ErrorInfoSchema, value: { domain: "cellaflow.com", reason: "ALLOWANCE_EXCEEDED", metadata: { limit: "writes_blocked" } } },
  ]);
  const err = allowanceRefusal(sent);
  assert.equal(err.subject, "cellaflow.steps");
  assert.equal(err.limit, "writes_blocked");
});

test("an allowance refusal is final, and a full mailbox still may pass", () => {
  assert.equal(isRetryable(overAllowance()), false);
  assert.equal(isRetryable(mailboxFull()), true);
  // Both are the engine's answers.
  assert.equal(hasStatusCode(overAllowance()), true);
  assert.equal(hasStatusCode(mailboxFull()), true);
});

// --- Every call site, with a scripted engine -----------------------------

/**
 * A scripted engine client. Each kind of call answers from its own entry in
 * `script`: a list, answered in order and then as a healthy engine, or a
 * function called for every call. An Error is thrown, not returned. Calls are
 * recorded in order; the leases and commits of the SDK's own records are
 * counted apart from the calls' own.
 */
function scriptedClient(script = {}) {
  const calls = [];
  const answer = (kind, healthy) => {
    const entry = script[kind];
    const next = typeof entry === "function" ? entry() : entry?.shift();
    if (next === undefined) return healthy();
    if (next instanceof Error) throw next;
    return next;
  };
  const isRecord = (key) => typeof key === "string" && (key.includes(":record:") || key.includes(":failure:"));
  return {
    calls,
    count: (kind) => calls.filter((c) => c.kind === kind).length,
    of: (kind) => calls.filter((c) => c.kind === kind),
    async checkIdempotencyCache(agentId, key, waitTimeoutMs, leaseTtlMs, sessionId, sequence) {
      const kind = isRecord(key) ? "recordAcquire" : "acquire";
      calls.push({ kind, key, sequence });
      if (kind === "acquire" && script.acquireDelayMs) await sleep(script.acquireDelayMs);
      return answer(kind, () => ({ status: CacheStatus.ACQUIRED, fencingToken: 7n, heartbeatIntervalMs: 60_000n }));
    },
    async commitStep(sessionId, sequence, name, status, payload, key) {
      const kind = isRecord(key) ? "recordCommit" : "commit";
      calls.push({ kind, sequence, name, status, key });
      return answer(kind, () => ({}));
    },
    async renewLease(agentId, key, fencingToken, extendMs) {
      calls.push({ kind: "renew", fencingToken });
      return answer("renew", () => ({ renewed: true, newExpiresAtMs: BigInt(Date.now() + extendMs) }));
    },
    async releaseLease(agentId, key, fencingToken, reason) {
      calls.push({ kind: "release", fencingToken, reason });
      return answer("release", () => ({ released: true }));
    },
    async getGraph() {
      calls.push({ kind: "getGraph" });
      return answer("getGraph", () => [[], undefined]);
    },
    close() {},
  };
}

const session = (client, init = {}) =>
  new WorkflowContext({ client, sessionId: "s-allowance", workflowVersion: "1.0.0", historyKnown: true, ...init });

const isAllowance = (err) => {
  assert.ok(err instanceof AllowanceExceededError, `expected an allowance refusal, got ${err}`);
  assert.equal(err.subject, "cellaflow.steps");
  return true;
};

/** Runs `fn` with console.warn captured, returning what it warned. */
async function warnings(fn) {
  const warned = [];
  const warn = console.warn;
  console.warn = (...args) => warned.push(args.join(" "));
  try {
    await fn();
  } finally {
    console.warn = warn;
  }
  return warned;
}

/** The steps a scripted client committed, as [sequence, name], records included. */
const committed = (client) =>
  client.calls.filter((c) => c.kind === "commit" || c.kind === "recordCommit").map((c) => [c.sequence, c.name]);

test("a tool call refused over the allowance fails at once, and the next call takes its position", async () => {
  const client = scriptedClient({ acquire: [overAllowance()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  let ran = false;
  const warned = await warnings(() =>
    runWithContext(session(client), async () => {
      const started = performance.now();
      await assert.rejects(charge(), isAllowance);
      // Asking again after no answer waits 200 ms first.
      assert.ok(performance.now() - started < 150, "the refusal waited before failing");
      ran = true;
      assert.equal(await notify(), "notified");
    }),
  );
  assert.ok(ran);
  // Asked once. Nothing is recorded at position 1, which notify takes.
  assert.deepEqual(client.of("acquire").map((c) => c.sequence), [1, 1]);
  assert.equal(client.count("recordAcquire"), 0);
  assert.deepEqual(committed(client), [[1, "notify"]]);
  assert.deepEqual(warned, []);
});

test("a tool call that finds the mailbox full still asks again", async () => {
  const client = scriptedClient({ acquire: [mailboxFull(), mailboxFull()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.equal(client.count("acquire"), MAX_UNANSWERED + 1);
});

test("a commit refused over the allowance is not sent again, and nothing is recorded", async () => {
  const client = scriptedClient({ commit: [overAllowance()] });
  let runs = 0;
  const charge = tool(async () => { runs += 1; return "charged"; }, { toolName: "charge" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), isAllowance);
    // Once the allowance allows, calling it again runs it at the same position.
    assert.equal(await charge(), "charged");
  });
  assert.equal(runs, 2);
  assert.deepEqual(client.of("release").map((c) => c.reason), ["TOOL_ERROR"]);
  assert.equal(client.count("recordAcquire"), 0);
  assert.deepEqual(committed(client), [[1, "charge"], [1, "charge"]]);
});

test("a commit refused over the allowance is not sent again when the release is refused too", async () => {
  // The lease would not be released. For a commit that got no answer, the same
  // commit sent again tells whether it landed; a refused one did not.
  const client = scriptedClient({ commit: [overAllowance()], release: [{ released: false }] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), isAllowance);
    assert.equal(await notify(), "notified");
  });
  assert.equal(client.count("commit"), 2);
  assert.deepEqual(committed(client), [[1, "charge"], [1, "notify"]]);
});

test("a commit that finds the mailbox full is still sent again", async () => {
  const client = scriptedClient({ commit: [mailboxFull()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.deepEqual(committed(client), [[1, "charge"], [1, "charge"]]);
});

test("a refusal raised inside a tool's body is not recorded as the tool's failure", async () => {
  // The body's own engine call is refused: an inner tool here. Recording the
  // refusal as the outer step's failure would replay it after the allowance is
  // back; neither call leaves anything at its position.
  const client = scriptedClient({ acquire: [undefined, overAllowance()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  const book = tool(async () => `booked:${await charge()}`, { toolName: "book" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(book(), isAllowance);
    assert.equal(await notify(), "notified");
  });
  assert.deepEqual(client.of("acquire").map((c) => [c.sequence]), [[1], [2], [1]]);
  assert.deepEqual(client.of("release").map((c) => c.reason), ["TOOL_ERROR"]);
  assert.equal(client.count("recordAcquire"), 0);
  assert.deepEqual(committed(client), [[1, "notify"]]);
});

test("a body that throws the refusal itself is not recorded either, while its other errors are", async () => {
  const client = scriptedClient();
  const refused = tool(async () => { throw overAllowance(); }, { toolName: "refused" });
  const declined = tool(async () => { throw new Error("card declined"); }, { toolName: "declined" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(refused(), isAllowance);
    await assert.rejects(declined(), /card declined/);
    assert.equal(await notify(), "notified");
  });
  // The refusal left position 1 to the next call; the decline is recorded there.
  const failure = client.of("recordCommit");
  assert.deepEqual(failure.map((c) => [c.sequence, c.name, c.status]), [[1, "declined", StepStatus.FAILED]]);
  assert.deepEqual(committed(client), [[1, "declined"], [2, "notify"]]);
});

test("reading a resumed session's position over the allowance fails the call before it claims anything", async () => {
  const client = scriptedClient({ getGraph: [overAllowance()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  const ctx = session(client, { historyKnown: false, positionKnown: false });
  const warned = await warnings(() => runWithContext(ctx, () => assert.rejects(charge(), isAllowance)));
  assert.equal(client.count("getGraph"), 1);
  assert.equal(client.count("acquire"), 0);
  assert.deepEqual(warned, []);
});

test("reading a resumed session's position into a full mailbox still lets the call go ahead", async () => {
  const client = scriptedClient({ getGraph: [mailboxFull()] });
  const charge = tool(async () => "charged", { toolName: "charge" });
  const ctx = session(client, { historyKnown: false, positionKnown: false });
  const warned = await warnings(async () => {
    assert.equal(await runWithContext(ctx, () => charge()), "charged");
  });
  assert.equal(client.count("acquire"), 1);
  assert.equal(warned.length, 1);
});

const hit = (result) => ({
  status: CacheStatus.HIT,
  currentSequence: 0n,
  cachedResult: { outputPayload: serialize({ result }) },
});

test("a received result's record refused over the allowance is kept, and not asked again or waited for", async () => {
  const client = scriptedClient({ acquire: [hit("placed-elsewhere")], recordAcquire: [overAllowance()] });
  const place = tool(async () => "placed-here", { toolName: "place_order" });
  const confirm = tool(async () => "confirmed", { toolName: "send_confirmation" });
  const warned = await warnings(() =>
    runWithContext(session(client), async () => {
      assert.equal(await place(), "placed-elsewhere");
      const started = performance.now();
      await assert.rejects(confirm(), isAllowance);
      assert.ok(performance.now() - started < 150, "the refusal waited before failing");
      // The call claimed nothing, and the record is still owed: written by
      // the next call, once the allowance allows, before it claims position 2.
      assert.equal(client.count("acquire"), 1);
      assert.equal(await confirm(), "confirmed");
    }),
  );
  assert.equal(client.count("recordAcquire"), 2);
  assert.deepEqual(committed(client), [[1, "place_order"], [2, "send_confirmation"]]);
  assert.deepEqual(warned, []);
});

test("a received result's record that finds the mailbox full is still asked again", async () => {
  const client = scriptedClient({
    acquire: [hit("placed-elsewhere")],
    recordAcquire: [mailboxFull(), mailboxFull()],
  });
  const place = tool(async () => "placed-here", { toolName: "place_order" });
  const confirm = tool(async () => "confirmed", { toolName: "send_confirmation" });
  await runWithContext(session(client), async () => {
    await place();
    assert.equal(await confirm(), "confirmed");
  });
  assert.equal(client.count("recordAcquire"), MAX_UNANSWERED + 1);
  assert.deepEqual(committed(client), [[1, "place_order"], [2, "send_confirmation"]]);
});

test("a record whose commit is refused over the allowance is kept for the next call", async () => {
  const client = scriptedClient({ acquire: [hit("placed-elsewhere")], recordCommit: [overAllowance()] });
  const place = tool(async () => "placed-here", { toolName: "place_order" });
  const confirm = tool(async () => "confirmed", { toolName: "send_confirmation" });
  const warned = await warnings(() =>
    runWithContext(session(client), async () => {
      await place();
      await assert.rejects(confirm(), isAllowance);
      assert.equal(await confirm(), "confirmed");
    }),
  );
  assert.equal(client.count("recordCommit"), 2);
  assert.deepEqual(committed(client), [[1, "place_order"], [1, "place_order"], [2, "send_confirmation"]]);
  assert.deepEqual(warned, []);
});

test("a failure record whose lease's release is refused over the allowance keeps both, without waiting", async () => {
  // The failed call's own release gets no answer, so its lease still claims
  // position 1. The record tries the release again before leasing itself;
  // that is refused over the allowance, which fails the next call at once.
  const client = scriptedClient({ release: [unavailable(), overAllowance()] });
  const charge = tool(async () => { throw new Error("boom"); }, { toolName: "charge" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), /boom/);
    const started = performance.now();
    await assert.rejects(notify(), isAllowance);
    assert.ok(performance.now() - started < 150, "the refusal waited before failing");
    assert.equal(client.count("recordAcquire"), 0);
    // Once the allowance allows, the release goes through and the record is written.
    assert.equal(await notify(), "notified");
  });
  assert.equal(client.count("release"), 3);
  assert.deepEqual(committed(client), [[1, "charge"], [2, "notify"]]);
});

test("a failure record whose lease's release finds the mailbox full still goes on to lease the record", async () => {
  const client = scriptedClient({ release: [unavailable(), mailboxFull()] });
  const charge = tool(async () => { throw new Error("boom"); }, { toolName: "charge" });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), /boom/);
    assert.equal(await notify(), "notified");
  });
  assert.equal(client.count("recordAcquire"), 1);
  assert.deepEqual(committed(client), [[1, "charge"], [2, "notify"]]);
});

test("a renewal refused over the allowance is not sent again, and the lease lasts until its deadline", async () => {
  // A renewal the engine would grant, if it were asked again.
  const client = scriptedClient({ renew: [overAllowance()] });
  const events = [];
  let lostDetail;
  const hb = new LeaseHeartbeat({
    client,
    agentId: "agent",
    idempotencyKey: "key",
    fencingToken: 1,
    heartbeatIntervalMs: 20,
    extendMs: 400,
    leaseTtlMs: 400,
    onEvent: (event) => events.push(event),
    onLeaseLost: (detail) => {
      lostDetail = detail;
    },
  });
  try {
    hb.start();
    await sleep(150);
    assert.equal(client.count("renew"), 1);
    assert.equal(hb.lost, false);
    assert.match(events.find((e) => e.type === "renew_failed").detail, /over its allowance for cellaflow\.steps/);
    await sleep(400);
    assert.equal(hb.lost, true);
    assert.match(lostDetail, /over its allowance for cellaflow\.steps/);
    assert.equal(client.count("renew"), 1);
  } finally {
    await hb.stop();
  }
});

test("a renewal that finds the mailbox full is still sent again", async () => {
  const client = scriptedClient({ renew: () => mailboxFull() });
  const hb = new LeaseHeartbeat({
    client,
    agentId: "agent",
    idempotencyKey: "key",
    fencingToken: 1,
    heartbeatIntervalMs: 20,
    extendMs: 400,
    leaseTtlMs: 400,
  });
  try {
    hb.start();
    await sleep(150);
    assert.ok(client.count("renew") >= 3, `${client.count("renew")} renewals`);
  } finally {
    await hb.stop();
  }
});

test("a late grant whose renewal is refused over the allowance is handed back, and the call fails", async () => {
  // A grant that comes back after half a renewal interval is confirmed by a
  // renewal before the body runs. Refused over the allowance, every renewal
  // will be: the body does not run, the lease goes back, and nothing is asked again.
  const client = scriptedClient({
    acquireDelayMs: 10,
    acquire: [{ status: CacheStatus.ACQUIRED, fencingToken: 7n, heartbeatIntervalMs: 4n }],
    renew: [overAllowance()],
  });
  let ran = false;
  const charge = tool(async () => { ran = true; return "charged"; }, { toolName: "charge", ttlMs: 60_000 });
  const notify = tool(async () => "notified", { toolName: "notify" });
  await runWithContext(session(client), async () => {
    await assert.rejects(charge(), isAllowance);
    assert.equal(await notify(), "notified");
  });
  assert.equal(ran, false);
  assert.equal(client.count("renew"), 1);
  assert.deepEqual(client.of("release").map((c) => c.reason), ["LEASE_UNCONFIRMED"]);
  assert.deepEqual(client.of("acquire").map((c) => c.sequence), [1, 1]);
  assert.deepEqual(committed(client), [[1, "notify"]]);
});

test("a late grant whose renewal finds the mailbox full still tries again before the body runs", async () => {
  const client = scriptedClient({
    acquireDelayMs: 10,
    acquire: [{ status: CacheStatus.ACQUIRED, fencingToken: 7n, heartbeatIntervalMs: 4n }],
    renew: [mailboxFull(), mailboxFull()],
  });
  const charge = tool(async () => "charged", { toolName: "charge", ttlMs: 60_000 });
  assert.equal(await runWithContext(session(client), () => charge()), "charged");
  assert.ok(client.count("renew") >= 3);
  assert.equal(client.count("acquire"), 1);
});

// --- Over the real transport ---------------------------------------------

/** What an engine over the allowance raises, as the engine sends it. */
const engineRefusal = () =>
  new ConnectError("steps allowance exhausted", Code.ResourceExhausted, undefined, [
    { desc: QuotaFailureSchema, value: { violations: [{ subject: "cellaflow.steps" }] } },
    {
      desc: ErrorInfoSchema,
      value: {
        domain: "cellaflow.com",
        reason: "ALLOWANCE_EXCEEDED",
        metadata: { resets_at: RESETS_AT, limit: "writes_blocked" },
      },
    },
  ]);

/**
 * An in-process engine. `respond(method)` decides each call: it throws to
 * refuse, or returns undefined to answer as a healthy engine. Calls are
 * recorded by method name.
 */
const served = [];
let respond = () => undefined;
const engine = http2.createServer(
  connectNodeAdapter({
    routes(router) {
      const method = (name, healthy) => (req) => {
        served.push(name);
        return respond(name, req) ?? healthy(req);
      };
      router.service(WorkflowEngineService, {
        startSession: method("startSession", (req) => ({ sessionId: req.sessionId || "s", version: req.version })),
        commitStep: method("commitStep", () => ({})),
        getGraph: method("getGraph", () => ({ steps: [] })),
        checkIdempotencyCache: method("checkIdempotencyCache", () => ({
          status: CacheStatus.ACQUIRED,
          fencingToken: 7n,
          heartbeatIntervalMs: 60_000n,
        })),
        renewLease: method("renewLease", (req) => ({ renewed: true, newExpiresAtMs: BigInt(Date.now()) + req.extendMs })),
        releaseLease: method("releaseLease", () => ({ released: true })),
      });
    },
  }),
);
// Open HTTP/2 sessions keep the process alive after close(), so they are
// destroyed too. Unreferenced, so the process can exit on a Node whose runner
// never calls a file's top-level after() (18.14).
const engineSessions = new Set();
engine.on("session", (s) => {
  s.unref();
  engineSessions.add(s);
  s.on("close", () => engineSessions.delete(s));
});
await new Promise((resolve) => engine.listen(0, "127.0.0.1", resolve));
engine.unref();
const target = `127.0.0.1:${engine.address().port}`;
after(() => {
  for (const s of engineSessions) s.destroy();
  engine.close();
});

/** Sets how the engine answers for one test, and clears what it served. */
function engineAnswers(fn) {
  served.length = 0;
  respond = fn;
}

/** The refusal as the SDK's client raises it, with every field the engine sent. */
const isEngineRefusal = (err) => {
  assert.ok(err instanceof AllowanceExceededError, `expected an allowance refusal, got ${err}`);
  assert.ok(err instanceof ConnectError);
  assert.equal(err.code, Code.ResourceExhausted);
  assert.equal(err.subject, "cellaflow.steps");
  assert.equal(err.resetsAt.getTime(), RESETS_AT_MS);
  assert.equal(err.limit, "writes_blocked");
  assert.match(err.message, /over its allowance for cellaflow\.steps.*resets at 2026-12-01T00:00:00Z.*steps allowance exhausted/);
  assert.ok(err.cause instanceof ConnectError);
  assert.equal(err.cause instanceof AllowanceExceededError, false);
  return true;
};

test("every engine call raises the refusal as AllowanceExceededError over the real transport", async () => {
  engineAnswers(() => {
    throw engineRefusal();
  });
  const client = new CellaflowClient({ target });
  const calls = {
    startSession: () => client.startSession("wf", "1.0.0", "s-1"),
    commitStep: () => client.commitStep("s-1", 1, "charge", StepStatus.SUCCESS, { result: 1 }),
    getGraph: () => client.getGraph("s-1"),
    checkIdempotencyCache: () => client.checkIdempotencyCache("agent", "key", 0, 1000, "s-1", 1, 5),
    renewLease: () => client.renewLease("agent", "key", 7, 1000, 5),
    releaseLease: () => client.releaseLease("agent", "key", 7, "TOOL_ERROR", 5),
  };
  for (const [name, call] of Object.entries(calls)) {
    await assert.rejects(call(), isEngineRefusal, name);
  }
  assert.deepEqual(served, Object.keys(calls));
});

test("a full mailbox over the real transport is the RESOURCE_EXHAUSTED it always was", async () => {
  engineAnswers(() => {
    throw new ConnectError("session mailbox is full", Code.ResourceExhausted);
  });
  const client = new CellaflowClient({ target });
  await assert.rejects(client.checkIdempotencyCache("agent", "key"), (err) => {
    assert.ok(err instanceof ConnectError);
    assert.equal(err instanceof AllowanceExceededError, false);
    assert.equal(err.code, Code.ResourceExhausted);
    assert.equal(isRetryable(err), true);
    return true;
  });
});

test("a session the engine refuses to start over the allowance fails before the callback runs", async () => {
  engineAnswers((name) => {
    if (name === "startSession") throw engineRefusal();
  });
  let ran = false;
  await assert.rejects(
    durableTools("thread-1", { target }, async () => {
      ran = true;
    }),
    isEngineRefusal,
  );
  assert.equal(ran, false);
  assert.deepEqual(served, ["startSession"]);
});

test("an execution lease refused over the allowance fails at once and runs nothing", async () => {
  engineAnswers((name) => {
    if (name === "checkIdempotencyCache") throw engineRefusal();
  });
  let ran = false;
  await assert.rejects(
    executionLease("task:1", { workerId: "worker-1", target }, async () => {
      ran = true;
    }),
    isEngineRefusal,
  );
  assert.equal(ran, false);
  // Nothing was granted, so nothing is released.
  assert.deepEqual(served, ["checkIdempotencyCache"]);
});

test("an execution lease whose renewal is refused over the allowance does not start its block", async () => {
  // After a wait the grant is confirmed by a renewal; refused over the
  // allowance, it is not tried again, and the lease is released.
  engineAnswers((name) => {
    if (name === "renewLease") throw engineRefusal();
  });
  let ran = false;
  await assert.rejects(
    executionLease("task:1", { workerId: "worker-1", target, waitMs: 1000 }, async () => {
      ran = true;
    }),
    isEngineRefusal,
  );
  assert.equal(ran, false);
  assert.deepEqual(served, ["checkIdempotencyCache", "renewLease", "releaseLease"]);
});

test("an execution lease whose renewal finds the mailbox full still tries again, and runs", async () => {
  engineAnswers((name) => {
    if (name === "renewLease") throw new ConnectError("session mailbox is full", Code.ResourceExhausted);
  });
  const result = await executionLease("task:1", { workerId: "worker-1", target, waitMs: 1000 }, async () => "done");
  assert.equal(result, "done");
  assert.equal(served.filter((name) => name === "renewLease").length, 3);
});

/** A google.rpc.Status, encoded by hand as an engine's gRPC library sends it. */
function statusDetailsBin(code, message, details) {
  const w = new BinaryWriter();
  w.tag(1, WireType.Varint).int32(code);
  w.tag(2, WireType.LengthDelimited).string(message);
  for (const { typeUrl, value } of details) {
    w.tag(3, WireType.LengthDelimited).fork();
    w.tag(1, WireType.LengthDelimited).string(typeUrl);
    w.tag(2, WireType.LengthDelimited).bytes(value);
    w.join();
  }
  // Binary metadata is base64, sent without padding.
  return Buffer.from(w.finish()).toString("base64").replace(/=+$/, "");
}

/** Serves every call with gRPC trailers written by hand, then runs `fn(client)`. */
async function withRawEngine(trailers, fn) {
  const server = http2.createServer();
  const sessions = [];
  server.on("session", (s) => sessions.push(s));
  server.on("stream", (stream) => {
    stream.on("error", () => {});
    stream.resume();
    stream.respond({ ":status": 200, "content-type": "application/grpc" }, { waitForTrailers: true });
    stream.on("wantTrailers", () => stream.sendTrailers(trailers));
    stream.end();
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const client = new CellaflowClient({ target: `127.0.0.1:${server.address().port}` });
  try {
    await fn(client);
  } finally {
    client.close();
    for (const s of sessions) s.destroy();
    await new Promise((resolve) => server.close(resolve));
  }
}

test("a refusal written as gRPC trailers is read by its details' type names", async () => {
  const quota = toBinary(QuotaFailureSchema, create(QuotaFailureSchema, { violations: [{ subject: "cellaflow.steps" }] }));
  const info = toBinary(
    ErrorInfoSchema,
    create(ErrorInfoSchema, {
      domain: "cellaflow.com",
      reason: "ALLOWANCE_EXCEEDED",
      metadata: { resets_at: RESETS_AT, limit: "writes_blocked" },
    }),
  );
  const trailers = {
    "grpc-status": "8",
    "grpc-message": encodeURIComponent("steps allowance exhausted"),
    "grpc-status-details-bin": statusDetailsBin(8, "steps allowance exhausted", [
      // Any type URL prefix: only the name after the last slash is matched.
      { typeUrl: "example.com/types/google.rpc.QuotaFailure", value: quota },
      { typeUrl: "type.googleapis.com/google.rpc.ErrorInfo", value: info },
    ]),
  };
  await withRawEngine(trailers, (client) => assert.rejects(client.renewLease("agent", "key", 7, 1000, 5), isEngineRefusal));
});

test("a refusal whose details do not decode is a plain RESOURCE_EXHAUSTED over the real transport", async () => {
  const trailers = {
    "grpc-status": "8",
    "grpc-message": "steps allowance exhausted",
    "grpc-status-details-bin": statusDetailsBin(8, "steps allowance exhausted", [
      { typeUrl: "type.googleapis.com/google.rpc.QuotaFailure", value: new Uint8Array([0xff, 0xff, 0xff, 0xff]) },
    ]),
  };
  await withRawEngine(trailers, (client) =>
    assert.rejects(client.checkIdempotencyCache("agent", "key"), (err) => {
      assert.equal(err instanceof AllowanceExceededError, false);
      assert.equal(err.code, Code.ResourceExhausted);
      assert.equal(isRetryable(err), true);
      return true;
    }),
  );
});
