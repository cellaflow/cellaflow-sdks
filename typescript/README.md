# @cellaflow/sdk

The official TypeScript/Node.js SDK for the Cellaflow workflow engine.

Cellaflow is the concurrency and durability layer for AI agent execution. It makes concurrent AI agent state transitions and external actions safe despite crashes, retries, and stale state.

## Installation

```bash
npm install @cellaflow/sdk
```

## Quick Start

```typescript
import { CellaflowClient, StepStatus } from "@cellaflow/sdk";

// 1. Initialize the client
const client = new CellaflowClient({
  target: "localhost:50051",
  secure: false, // Set to true if your engine is behind HTTPS/TLS
});

async function main() {
  // 2. Start a workflow session
  const session = await client.startSession("my-agent-workflow", "1.0");
  const sessionId = session.sessionId;

  console.log("Started session:", sessionId);

  // 3. Commit a step to the session
  const result = await client.commitStep(
    sessionId,
    1,                       // Step sequence
    "fetch_data",            // Step name
    StepStatus.COMPLETED,    // Outcome
    { data: "example" }      // Payload (auto-serialized via MessagePack)
  );

  console.log("Step committed.");

  // 4. Retrieve the session graph
  const [graph, nextCursor] = await client.getGraph(sessionId);
  console.log("Graph:", graph);
}

main().catch(console.error);
```

## Authenticating with the engine

An engine started with a token refuses calls without it. Start the engine with the token in `CELLAFLOW_API_KEY`, set the same variable in the SDK's environment, and every call sends it, with no code change. Or pass `apiKey` to `CellaflowClient`, `durableTools`, `executionLease` or `taskLease`:

```typescript
await durableTools(threadId, { target: "engine.example.com:50051", secure: true, apiKey }, async () => {
  // ...
});
```

- **Where it comes from:** `apiKey` if given, else `CELLAFLOW_API_KEY`. A blank value means no key. That is the only variable the SDK reads. It never reads the engine's older name, `CELLAFLOW_AUTH_TOKEN`, or the generic `AUTH_TOKEN` and `API_KEY`: a variable that generic often holds another service's secret.
- **On the engine:** the engine reads its token from `CELLAFLOW_AUTH_TOKEN` before `CELLAFLOW_API_KEY`, and a blank `CELLAFLOW_AUTH_TOKEN` counts as set: the engine then has no token at all. Leave `CELLAFLOW_AUTH_TOKEN` unset there, not blank (Compose writes a blank one for `${VAR}` when the variable is unset), and set only `CELLAFLOW_API_KEY`.
- **Refused early:** a key that cannot be sent as a header (surrounding whitespace, or a character outside printable ASCII) is refused when the client is built, naming where it came from.
- **How it's sent:** as `authorization: Bearer <key>` on every call.
- **Without TLS:** a key is never sent in plaintext to anything but this machine unless you allow it. With `secure: false` and another host as the target, the client refuses, naming the target, so forgetting TLS cannot be how a key crosses the network. On a network you trust (a Compose network, a Kubernetes service), set `CELLAFLOW_ALLOW_PLAINTEXT_KEY=true` (or, building a `CellaflowClient` yourself, pass `allowPlaintextKey: true`); each target then warns once. The target is judged as the transport parses it, and Node's HTTP/2 client uses no proxy, so the host checked is the host the key goes to.
- **Your own CA:** `secure: true` trusts Node's root certificates. For an engine whose certificate comes from your own CA, or is self-signed, start Node with `NODE_EXTRA_CA_CERTS=/path/to/ca.pem`, which adds it to them.

## Coordinating agents across sessions

A coordination id names the work several agents share: a ticket, a customer, a release. Give it to `durableTools`, and a tool declared with `IdempotencyScope.SHARED` deduplicates with every agent working under the same id, even in other sessions, other workflows, or the Python SDK.

```typescript
import { durableTools, tool, IdempotencyScope } from "@cellaflow/sdk";

const refund = tool(
  async ({ customer, cents }: { customer: string; cents: number }) =>
    gateway.refund(customer, cents),
  { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["customer"] },
);

await durableTools("billing-run-7", { coordinationId: "customer-88" }, async () => {
  await refund({ customer: "customer-88", cents: 2500 }); // runs once per customer
});
```

**The engine records it.** The coordination id is sent when the session starts. The engine stores it as the session's default and records every step under it, and `getGraph` returns each step's `coordinationId`.

**Resuming keeps it.** A later `durableTools` block for the same thread may omit `coordinationId` and adopts the stored one, so its shared keys do not change. Naming a different one is refused by the engine before the callback runs.

**Overriding it for one call.** An agent working one thing sometimes acts on another: a ticket agent refunding the customer. `withCoordinationId` runs a single call under a different id. That call is recorded under it, and its shared key is derived from it, so it converges with agents whose session default is that id.

```typescript
await durableTools("ticket-4417", { coordinationId: "ticket-4417" }, async () => {
  await refund.withCoordinationId("customer-88")({ customer: "customer-88", cents: 2500 });
});
```

**Rules.** A coordination id may not contain colons, may not have leading or trailing whitespace, and may not exceed 256 bytes; an empty string means none. Invalid values throw before anything runs, and are never trimmed. `normalizeCoordinationId` applies the same check.

## How long a tool's lease lasts

While a tool's body runs, the call holds a lease, renewed every quarter of its TTL. If the process crashes or freezes, another caller can take the call over once the lease lapses: within one TTL of the holder's last renewal. By default that is the engine's TTL, 20 s on current engines. Set `ttlMs` to change it, for every tool in a `durableTools` block or for one tool:

```typescript
const charge = tool(chargeCard, { toolName: "charge_card", ttlMs: 6_000 });
const sendReceipt = tool(emailReceipt, { toolName: "send_receipt" });

await durableTools("ticket-4417", { ttlMs: 10_000 }, async () => {
  await charge(order);      // 6 s: the tool's own ttlMs wins
  await sendReceipt(order); // 10 s: the block's
});
```

A shorter TTL means a faster takeover, and more renewals while a call runs. A holder that stops renewing for one TTL while its body still runs (a blocked event loop, a long garbage-collection pause, a lost connection to the engine) is taken over too, so set the TTL well above the longest pause a call can see. The lease that records a call's shared result or failure in the session is not renewed, so the TTL also bounds how long that one write may take. The TTL that decides a takeover is the holder's: a waiting caller's own `ttlMs` does not shorten it. A waiting caller asks again about once a second, so it takes over up to a second after the lease lapses. `ttlMs` must be a whole number of milliseconds, at least 1000 (`MIN_LEASE_TTL_MS`); anything else makes `tool` throw a `RangeError` when the tool is created, and makes `durableTools` reject with one before it connects. The engine's maximum lease lifetime (`CELLAFLOW_MAX_LEASE_LIFETIME_MS`, an hour by default) caps every lease, whatever is asked for.

## Checking the lease inside a tool

A tool's body can reach its call's lease through `currentLease()`, the same handle `executionLease` and `taskLease` give. Check it right before anything irreversible, and hand its `signal` to the request:

```typescript
import { IdempotencyScope, currentLease, tool } from "@cellaflow/sdk";

const refund = tool(
  async ({ order, amount }: { order: string; amount: number }) => {
    const lease = currentLease();
    lease.check(); // throws LeaseLostError if the lease is gone
    return await payments.refund(order, amount, { signal: lease.signal });
  },
  { toolName: "refund", scope: IdempotencyScope.SHARED, sharedOn: ["order"] },
);
```

This is what stops a holder that froze (a long garbage-collection pause, a stopped process, a suspended laptop or VM) and woke up after another caller took the call over. The lease is judged on the clock, so `check()` throws as soon as the holder resumes past its lease's deadline, before any renewal has to answer, and the request never leaves. `signal` aborts once the lease is judged lost: when it is read, or when the deadline timer fires, so a request given it is cancelled on this side.

The clock only tells the body to stop. What happens to the call is up to the engine, which accepts a commit only while the lease is still this call's, that is, unless another worker has taken it over:

- **The body returns:** its result is committed. If another worker holds the call now (in another session, or before it has committed), the engine refuses it, and the caller gets `LeaseLostError` with the body's return value on its `result`, for logging. If nobody took over, the commit stands, so a body that finished just as its deadline passed is recorded, not run again. A commit that gets no answer is sent once more before the lease is released, and again if the engine then refuses the release; only a refusal about the call's key counts as a takeover.
- **The body throws** (including `LeaseLostError` from `check()`): the lease is released. If the engine releases it, the lease was still this call's, and the failure is recorded as for any failing tool. If it refuses, the lease is no longer this call's (another worker took it over, or it lapsed): nothing is recorded at the call's position in its name, and the position is handed back, so calling the same tool again finds the other worker's result, waits for it, or, if the lease only lapsed, runs the body again. (When a later position was claimed meanwhile, the position stays empty instead, and the run stops recording, as it does for any call that overlaps.)

The check covers a request that has not left. A request already on its way when the holder froze can still arrive, and so can one whose effect landed just before a crash, ahead of the commit. For those, give the destination a way to recognise a repeat, such as an idempotency key it deduplicates on.

The clock can only judge what it can see: keep `ttlMs` below the engine's maximum lease lifetime (`CELLAFLOW_MAX_LEASE_LIFETIME_MS`), or a holder frozen past that limit can still pass `check()` when it wakes.

Inside a tool body, `currentLease()` is the tool call's lease, even when the tool is called from inside an `executionLease` block or a `taskLease` function. To pass the enclosing lease's fencing token downstream from a tool, read it before calling the tool. A cache hit never runs the body, so it never sees a lease; outside any lease, `currentLease()` throws, as before.

## Execution leases

`executionLease` runs a block while holding a lock on a key, renewed by a heartbeat. One worker holds it at a time; if that worker dies, the lease stops being renewed and the next worker gets it once the TTL runs out, with a higher fencing token.

```typescript
import { executionLease, LeaseNotAcquired } from "@cellaflow/sdk";

await executionLease("task:repo#42", { workerId: "worker-1", ttlMs: 30_000 }, async (lease) => {
  const result = await doTheWork();
  lease.check(); // throws LeaseLostError if the lease is gone
  await postResult(result, { signal: lease.signal });
});
```

**Options.** `workerId` (required), `target` (default `localhost:50051`), `secure`, `ttlMs` (default 30 000; each renewal extends by the same amount), `waitMs` (how long to wait for a lease another worker holds; default 0, which throws `LeaseNotAcquired` straight away), `onLeaseLost`, and `onEvent`. The engine answers acquisition with its own renewal interval, a quarter of the TTL; `heartbeatIntervalMs` is used only when it does not.

**Losing the lease.** Node cannot preempt a running function, so loss is cooperative. `check()` throws `LeaseLostError` and `signal` aborts once the lease can no longer be assumed held. That is judged on the clock: each confirmed renewal sets a deadline counted from when it was sent, minus half a renewal interval, so the holder gives up no later than the engine would hand the lease on. A renewal the engine shortens, near its maximum lease lifetime, moves the deadline in by the shortfall. One or two failed renewals in a row do not lose a lease the engine still holds. A process stopped past its deadline fails `check()` as soon as it resumes, and so does a host woken from sleep. Once the block has exited and the lease is released, `check()` fails and `isLost` is true, but `signal` does not abort: releasing is not losing, and a response fetched with it can still be read. Neither can recall a write that already left: a request sent just before the deadline can still land after another worker took over, which is why `signal` is worth passing to it.

**Tracing.** `onEvent` receives `{ type, at, fencingToken, expiresAtMs?, detail? }` for `acquired`, `renewed`, `renew_failed`, `lost` and `released`. `at` is wall-clock epoch milliseconds; `expiresAtMs` is on the engine's clock.

**A lock, not a record.** `executionLease` records no result, so the lease alone does not tell the next worker whether the work already happened; check the destination first, or use `tool()`, which leases and records the result. A key that is already committed throws `LeaseNotAcquired`.

## Features

- **Built on Connect-ES**: Uses HTTP/2 for high performance and strict gRPC semantics.
- **Safe Serialization**: Strictly uses MessagePack for state payloads to mitigate Remote Code Execution (RCE) risks associated with unvalidated JSON parsing.
- **Idempotency Locks**: Built-in methods for leasing and fencing distributed locks (`checkIdempotencyCache`, `renewLease`, `releaseLease`).

## Requirements

- Node.js 18.14.1 or higher.

## License

Apache 2.0
