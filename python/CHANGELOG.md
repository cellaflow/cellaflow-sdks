# Changelog

All notable changes to the CellaFlow Python SDK.

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
