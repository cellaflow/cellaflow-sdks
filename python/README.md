# Cellaflow Python SDK

The official Python SDK for the [CellaFlow Engine](https://github.com/theblueskies/cellaflow) — providing durable execution, deterministic replay recovery, and swarm-safe concurrency primitives for AI workflows and agent graphs.

---

## Features

- 🔄 **Durable Execution & Transparent Replay**: Workflows survive process restarts and infrastructure crashes without re-executing completed steps.
- ⚡ **Zero-Friction Decorators**: Annotate standard Python functions with `@workflow`, `@step`, and `@tool` (supporting both `def` and `async def`).
- 🛡️ **Deterministic Idempotency**: Automatic input hashing via RFC 8785 Canonical JSON and SHA-256 guarantees cross-language determinism across multi-agent swarms.
- 🔒 **Background Lease Management**: Non-blocking heartbeat management keeps engine locks alive and eliminates split-brain execution using fencing tokens.
- 📦 **Secure Serialization**: Strictly uses MessagePack for all state payloads to optimize throughput and mitigate remote code execution (RCE) attack vectors.

---

## Installation

```bash
pip install cellaflow
```

---

## Quickstart

```python
from cellaflow import workflow, step, tool, IdempotencyScope

# 1. Define tools with automatic caching and idempotency
@tool(scope=IdempotencyScope.SCOPE_SESSION_WIDE)
def search_web(query: str) -> dict:
    """Simulates an expensive API or web search tool."""
    print(f"Executing web search for: {query}")
    return {"query": query, "results": ["CellaFlow Overview", "Durable Execution Docs"]}

# 2. Define intermediate steps
@step
def summarize_results(data: dict) -> str:
    return f"Processed {len(data['results'])} items for '{data['query']}'"

# 3. Define the orchestrating workflow
# By default connects to localhost:50051. You can pass a custom endpoint:
# @workflow(version="1.0.0", target="engine.mycompany.com:50051", secure=True)
@workflow(version="1.0.0")
def research_workflow(topic: str) -> str:
    search_data = search_web(topic)
    summary = summarize_results(search_data)
    return summary

if __name__ == "__main__":
    result = research_workflow("autonomous agent architectures")
    print("Result:", result)
```

---

## Architecture & How It Works

### 1. Transparent Replay Recovery

If a worker crashes mid-workflow, restarting the workflow with the same session automatically recovers state from the CellaFlow engine's durable event log:

```mermaid
flowchart LR
    classDef app fill:#e1f5fe,stroke:#0288d1,stroke-width:2px,color:#01579b
    classDef engine fill:#ede7f6,stroke:#512da8,stroke-width:2px,color:#311b92
    classDef crash fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c

    subgraph Client[" 🐍 Python Application "]
        W["@workflow Orchestrator"]:::app
        S1["@step 1: Query API"]:::app
        S2["@step 2: Process Data"]:::app
        Crash["💥 Process Crashes Mid-Run"]:::crash
    end

    subgraph Storage[" ⚙️ CellaFlow Engine "]
        Log[("💾 RocksDB Event Graph")]:::engine
        Replay["🔄 On-Demand Replay Engine"]:::engine
    end

    W --> S1
    S1 -->|"1. Commit Result"| Log
    S1 --> S2
    S2 -.-> Crash
    Crash ==>|"Restart Workflow"| Replay
    Replay -->|"2. Instant Cache Replay (0ms)"| S1
    Replay -->|"3. Resume Live Execution"| S2
```

---

### 2. Idempotency & Lease Lifecycle

For external tool calls and multi-agent coordination, `@tool` prevents duplicate tool calls, single-flights in-progress work, and maintains active lock heartbeats:

```mermaid
flowchart TD
    classDef check fill:#e0f7fa,stroke:#00838f,stroke-width:2px,color:#004d40
    classDef hit fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef lease fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
    classDef commit fill:#ede7f6,stroke:#512da8,stroke-width:2px,color:#311b92

    In["Function Call: @tool(*args, **kwargs)"] --> Hash["🔑 RFC 8785 Canonical JSON + SHA-256 Hashing"]
    Hash --> Query{"Check Idempotency Cache"}:::check

    Query -->|"⚡ Cache HIT"| Cached["Return Cached StepResult<br/>(Skip Execution)"]:::hit
    
    Query -->|"🔒 Cache ACQUIRED"| Lease["Acquire Fencing Token & Lease"]:::lease
    
    subgraph Execution[" ⚙️ Active Tool Execution "]
        Lease --> HB["🔄 Background LeaseHeartbeat<br/>(Periodic RenewLease)"]:::lease
        Lease --> Run["🛠️ Run User Function Body"]:::lease
    end

    Run --> Done["Stop Heartbeat & CommitStep(fencing_token)"]:::commit
    Done --> DB[("💾 Persist Result to RocksDB")]:::commit
```

---

## Core Concepts

### `@workflow(version="1.0.0", target="localhost:50051", secure=False)`
Marks the entry point for a durable workflow execution. 
- Automatically initializes the underlying gRPC client and execution context.
- Manages replay state when resuming from a crashed or interrupted session.

### `@step` and `@tool`
Decorators for atomic units of execution within a workflow.
- **Idempotency**: Generates a deterministic key based on the function name, session, version, sequence, agent ID, and hashed inputs.
- **Replay Interception**: If a step was already completed in the session history, the SDK returns the cached output immediately without re-running the function body.
- **Lease Heartbeating**: Spawns a background task/thread to keep the engine lease refreshed until the function returns.

### `IdempotencyScope`
Controls how cache hits are shared across the execution graph:
* `IdempotencyScope.SCOPE_SESSION_WIDE` *(Default)*: Results are cached and shared across all agents in the session.
* `IdempotencyScope.SCOPE_AGENT_PRIVATE`: Isolates cache hits to the executing agent.
* `IdempotencyScope.SCOPE_STEP_LOCAL`: Strictly isolates cache hits to the current superstep/turn.

---

## Development Setup

To get up and running with the Python SDK for local development:

1. **Create and activate a virtual environment**:
   ```bash
   cd python
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install the SDK in editable mode with development dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

### Generating Protobufs and Typing Stubs (`.pyi`)

We use `grpcio-tools` and `mypy-protobuf` to generate Python stubs from `.proto` definitions:

```bash
./scripts/generate_protos.sh
```

This compiles protos from `../proto/cellaflow/v1/*.proto` and outputs `*_pb2.py`, `*_pb2_grpc.py`, and `*.pyi` typing stubs into `src/cellaflow/v1/`.

### Running Tests and Linters

```bash
# Run unit tests
pytest

# Formatting & Linting
black src tests
flake8 src tests
mypy src tests
```

---

## License

Apache 2.0 or MIT (see repository root for details).
