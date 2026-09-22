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

## Features

- **Built on Connect-ES**: Uses HTTP/2 for high performance and strict gRPC semantics.
- **Safe Serialization**: Strictly uses MessagePack for state payloads to mitigate Remote Code Execution (RCE) risks associated with unvalidated JSON parsing.
- **Idempotency Locks**: Built-in methods for leasing and fencing distributed locks (`checkIdempotencyCache`, `renewLease`, `releaseLease`).

## Requirements

- Node.js 18.0.0 or higher.

## License

Apache 2.0
