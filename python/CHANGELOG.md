# Changelog

All notable changes to the CellaFlow Python SDK.

## 0.6.0

### Added & Changed

- **Framework-agnostic `durable_tools` and top-level export**:
  `durable_tools` and `tool_session_id` have moved from `cellaflow.langgraph` to `cellaflow.durable` and are now exported directly from the top-level `cellaflow` package:

  ```python
  from cellaflow import durable_tools, tool
  ```

  `durable_tools` requires only a session identifier and tools invoked within the block; it has no dependency on LangGraph. Moving it decouples CellaFlow's leased-execution primitive from any specific framework, making it natively usable with AutoGen, CrewAI, LlamaIndex, the OpenAI Agents SDK, or standalone Python applications.
  - Backward compatibility is preserved: `cellaflow.langgraph` continues to re-export `durable_tools` and `tool_session_id`.
  - `CellaflowSaver` remains in `cellaflow.langgraph` and is now lazily imported on first access via module `__getattr__` (with a `TYPE_CHECKING` guard). Importing `cellaflow` no longer eagerly loads LangGraph and its serialization machinery into environments that do not use it.

- **Off-context executor dispatch recovery**:
  Frameworks that dispatch tool execution via thread-pool executors such as `loop.run_in_executor` (including AutoGen 0.7.5 and CrewAI 1.9.3's async path) do not propagate Python `contextvars.ContextVar` across the thread boundary, which previously caused wrapped `@tool` calls to fail with `RuntimeError: No active workflow context found`.
  - The SDK now maintains a thread-safe registry of active `durable_tools` blocks. When `get_context()` detects an unset `ContextVar`:
    - Exactly one active session in the process: automatically resolves and rebinds the workflow context.
    - Zero active sessions: raises `RuntimeError` as before.
    - Two or more concurrent active sessions: raises `AmbiguousContextError` identifying the conflicting sessions rather than erroneously attributing side effects or leases across session boundaries.
  - Added `WorkflowContext.bind()` as an explicit escape hatch for concurrent multi-session applications requiring manual context propagation.

### Fixed

- **Protobuf runtime mismatch & toolchain pinning (`CEL-148`)**:
  Fixed an issue where `cellaflow==0.5.0` declared `protobuf>=4.25.0` while its generated gRPC stubs asserted a runtime of at least `7.35.1` at import, causing resolver confusion and immediate runtime crashes on `import cellaflow`.
  - Pinned development tools to `grpcio-tools==1.71.0` (emitting gencode `5.29.0`) and `mypy-protobuf==3.6.0` (avoiding bundled protobuf 6.x gencode guards).
  - Aligned the declared runtime floor in `pyproject.toml` to `protobuf>=5.29.0`.
  - Restored co-installation compatibility with agent frameworks restricted to protobuf 5.x, such as `autogen-core 0.7.5` (`protobuf~=5.29.3`) and `crewai 1.9.3` (`protobuf<6.0`).
  - Fixed `.github/workflows/publish-python-sdk.yml` to build wheels using the pinned dev toolchain instead of unpinned packages, eliminating release drift from CI.
  - Added `tests/test_protobuf_floor.py` to continuously validate that the declared floor covers the gencode assertions in every generated protobuf stub.

### Documentation & Examples

- **Irreversible Tool Crash Benchmark (`examples/crash_benchmark`)**:
  Added an automated, ledger-adjudicated benchmark harness comparing four side-effect mitigation guards (`no-guard`, `pg_advisory_lock`, `claim-first` idempotency keys, and CellaFlow leased tools) under six real-world failure injection scenarios: invocation redelivery, crash before checkpoint, crash between side effect and checkpoint, stalled owner / lease expiration, multi-agent convergence, and owner worker death.

## 0.5.0

### Added

- **`shared_on` for cross-session convergence (`CEL-145`)**:
  When heterogeneous agents across different sessions or workflows converge on a shared side effect (such as issuing a refund for a customer ticket), they frequently reason their way to different secondary arguments (e.g. diverging refund amounts, notes, or metadata). Previously, `SCOPE_SHARED` hashed all arguments into the idempotency key, causing each agent to derive a distinct key and execute duplicate side effects.

  The `@tool` and `@step` decorators now accept `shared_on: Optional[Sequence[str]] = None`. When specified, only the named arguments contribute to the derived idempotency key:

  ```python
  @tool(
      tool_name="issue_refund",
      scope=IdempotencyScope.SCOPE_SHARED,
      shared_on=["ticket_id"],
  )
  def issue_refund(ticket_id: str, amount_cents: int) -> dict: ...
  ```

  Positional arguments are bound and default parameter values are applied before extracting the target values. Arguments not named in `shared_on` can diverge freely across callers without splitting the key.

### Breaking changes

- **`SCOPE_SHARED` requires explicit identification of shared work**: Decorating a tool or step with `IdempotencyScope.SCOPE_SHARED` without declaring `shared_on=[...]` (or an explicit static `idempotency_key`) now raises `ValueError` at decoration time rather than failing in production or silently deriving colliding/divergent keys.
- **Signature validation for `shared_on`**: Parameter names passed to `shared_on` are validated against the function's signature at decoration time; any parameter names not present in the signature raise `ValueError` immediately to catch typos at import.

### Behaviour changes

- **Deliberate key collapse across heterogeneous agents**: Declaring `shared_on` intentionally collapses keys. The winning agent executes the side effect with its arguments; losing agents receive the winning execution's result computed from arguments they did not supply. If callers must agree on disputed values, compute the disputed value in its own leased step before executing the shared tool.

### Documentation & Examples

- **At-Most-Once Proof (`examples/at_most_once_proof`)**: Added an automated, runnable two-arm harness demonstrating that while LangGraph checkpointers protect workflow state, tool-level side effects require leases (`durable_tools`) to prevent duplicate execution during process crashes.
- **Runnable Quickstart Agent (`examples/quickstart`)**: Added a verified, runnable `research_agent.py` CLI script and append-only ledger demonstration showing durable replay and side-effect suppression against a live engine.
- **Swarm Demo Convergence (`examples/multi_agent_idempotency`)**: Updated scenario 4 with divergent agent refund amounts to showcase multi-agent `shared_on` convergence and trade-off disclosure.

## 0.4.0

### Fixed

- **`durable_tools` in multi-node LangGraph workflows**: Dropped positional replay within `durable_tools` so graphs with multiple tool-bearing nodes resume correctly across failures without sequence mismatches.
- **Config validation in `durable_tools`**: Added robust handling for LangGraph config formats, accepting bare string thread IDs, hashing thread IDs containing colons to ensure valid session IDs, and rejecting empty thread IDs.

## 0.3.0

The first release since `0.2.1`, covering six tickets. Two of them change behaviour you may
currently be relying on — read **Behaviour changes** before upgrading.

### Added

**`IdempotencyScope.SCOPE_SHARED` — deduplicate across sessions.** The four existing scopes all
narrow the derived key from the session default, so every agent sharing a result had to be in the
same session. `SCOPE_SHARED` is the first that widens past it: agents running *different*
workflows, on *different* versions, in *different* sessions converge on one execution.

```python
@tool(tool_name="issue_refund", scope=IdempotencyScope.SCOPE_SHARED)
def issue_refund(ticket_id: str, amount_cents: int) -> dict: ...

support_agent("T-4417", 2499, _coordination_id="refund-T-4417")
fraud_detector("T-4417", 2499, _coordination_id="refund-T-4417")   # one refund, not two
```

`_coordination_id` names the work being shared — a ticket, a release, a tenant — and is passed at
the call site because it belongs to the collaboration rather than to the tool. It is **required**
for `SCOPE_SHARED` and has no default; a default would silently deduplicate unrelated callers that
happen to make the same call, suppressing one of them with no error anywhere.

Three things must match for two agents to converge, and one is a footgun: **`tool_name` defaults
to the function name.** Agents whose functions are named differently must pin it to the same
explicit value or they will never share. The other two are the coordination id and the arguments.

### Behaviour changes

Two guarantees the SDK enforces at runtime. Both surface as exceptions, so a workflow that
violates either will stop rather than continue on an unverified result.

**Replayed steps are verified before being returned.** Replay is positional: the step at sequence
*N* is answered from what was committed at sequence *N*. The SDK compares step names before
returning a recorded result and raises `NondeterministicWorkflowError` when they do not match, so
a resumed run that took a different path stops rather than continuing on another step's result.

If you see this, it is a bug in your workflow, not the engine: a resumed run took a different path
from the one it is recovering, almost always by branching on a value computed outside a step. Move
whatever the branch tests inside a `@step` so its result is replayed too.

**Divergent replicas are refused before they execute.** When several replicas of one agent reason
their way to *different* arguments for the same step, they derive different idempotency keys, so
nothing about the operation makes them contend. The SDK tells the engine which graph position it
intends to write, and a replica whose position is already taken raises `DivergentStepError`
**before** its function body runs — so only one replica reaches the side effect.

The durable fix is in the workflow rather than at the call site: wrap the value the replicas
disagree about in its own leased step, and they converge on it before reaching the step that acts
on it.

```python
@tool
def decide_amount(ticket_id: str) -> int:
    return llm.decide(ticket_id)       # nondeterministic, but leased -> one winner

@tool
def issue_refund(ticket_id: str, amount_cents: int) -> dict:
    return gateway.charge(...)         # every replica now sees identical arguments
```

### Fixed

**Callers adopt the engine's reported sequence after a cache hit.** A caller taking a cache hit
returns without committing, so its local counter would otherwise advance while the session's did
not. The SDK reads the position the engine reports on the response and re-aligns to it.

**Concurrent LangGraph savers on one thread.** `CellaflowSaver` commits against a sequence
refreshed from the engine and retries on contention, so concurrent replicas writing to one
`thread_id` all land. Measured at 100 concurrent savers.

**Lease renewal failures explain themselves.** When a renewal is denied the step is still running
and is about to attempt a commit that will be rejected, so the heartbeat logs what happened and
what to do about it rather than an enum ordinal.

### Compatibility

- **Requires no engine upgrade.** The new request fields are optional; an older engine ignores
  them. Verified against a released engine image.
- **`DivergentStepError` only fires against an engine that arbitrates graph positions.** Against an
  older engine the call succeeds and the divergence is caught at commit, as it was before.
- **Not a breaking API change**, with one exception: `derive_idempotency_key` gained a required
  positional `coordination_id` before `*args`. It is not exported from the `cellaflow` package
  namespace and is not intended as public API, but code importing it directly from
  `cellaflow.idempotency` must add the argument.

## 0.2.1 and earlier

Not recorded here. See the git history.
