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

## Features

- **Built on Connect-ES**: Uses HTTP/2 for high performance and strict gRPC semantics.
- **Safe Serialization**: Strictly uses MessagePack for state payloads to mitigate Remote Code Execution (RCE) risks associated with unvalidated JSON parsing.
- **Idempotency Locks**: Built-in methods for leasing and fencing distributed locks (`checkIdempotencyCache`, `renewLease`, `releaseLease`).

## Requirements

- Node.js 18.0.0 or higher.

## License

Apache 2.0
