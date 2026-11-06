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

An engine started with a token (`CELLAFLOW_AUTH_TOKEN`) refuses calls without it. Set the token in the environment as `CELLAFLOW_API_KEY`, and every call sends it, with no code change. Or pass `apiKey` to `CellaflowClient`, `durableTools`, `executionLease` or `taskLease`:

```typescript
await durableTools(threadId, { target: "engine.example.com:50051", secure: true, apiKey }, async () => {
  // ...
});
```

- **Where it comes from:** `apiKey` if given, else `CELLAFLOW_API_KEY`. A blank value means no key. That is the only variable the SDK reads: the engine's `CELLAFLOW_AUTH_TOKEN` configures the token it checks against, and a variable as generic as `API_KEY` often holds another service's secret.
- **Refused early:** a key that cannot be sent as a header (surrounding whitespace, or a character outside printable ASCII) is refused when the client is built, naming where it came from.
- **How it's sent:** as `authorization: Bearer <key>` on every call.
- **Without TLS:** sending a key to a host other than localhost with `secure: false` warns once per target, since anyone on the path can read it.

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

- Node.js 18.0.0 or higher.

## License

Apache 2.0
