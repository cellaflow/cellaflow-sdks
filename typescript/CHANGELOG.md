# Changelog

All notable changes to the CellaFlow TypeScript SDK.

## 0.7.1

### Added

- **The high-level API.** `0.7.0` shipped the transport only: `CellaflowClient` and
  the generated types. Everything above it now exists, matching the Python SDK's
  surface.

  - `tool` / `step` — wraps a function so the engine runs it at most once per
    idempotency key, and so its result survives the process that produced it. A
    second caller deriving the same key receives what the first returned, even if
    that process has since died.
  - `durableTools` — opens the session a leased tool belongs to, from a LangGraph
    config or a bare thread id. Callback-based rather than a context manager.
  - `executionLease` / `taskLease` — a distributed lock with liveness
    heartbeating, so a holder that dies stops renewing and another worker takes
    over once the TTL elapses.
  - `currentLease`, `LeaseHandle`, `LeaseNotAcquired`, `LeaseLostError`.
  - `deriveIdempotencyKey`, `hashInputs`, `IdempotencyScope`, `toolSessionId`,
    `WorkflowContext`, `DivergentStepError`.

- **Cross-language coordination.** `IdempotencyScope.SHARED` now works between
  runtimes. Key derivation is byte-identical to the Python SDK: RFC 8785 canonical
  JSON, SHA-256, first 16 bytes. Verified against Python reference hashes covering
  unicode, escapes, nested objects, key reordering and exponent-form numbers, and
  end to end against a running engine, where a Python agent and a TypeScript
  agent proposing different amounts for the same refund converge on one.

  This was not possible on `0.7.0`: without exported key derivation, a TypeScript
  caller could not reach the same key without reimplementing the canonicalisation
  by hand and getting it exactly right.

### Notes

- Purely additive. Nothing was removed or changed, so `^0.7.0` picks this up and
  existing code continues to work untouched.

- **Two places this cannot match the Python SDK**, documented in the API rather
  than silently approximated:

  - Python's `async_execution_lease` cancels the calling task when the lease is
    lost. Node has no task cancellation, so `LeaseHandle` exposes `check()` and
    an `AbortSignal` and losing a lease is cooperative. Anything irreversible
    inside a long block should check first.
  - Python binds `*args`/`**kwargs` against the function signature so `shared_on`
    can select named parameters. JavaScript has no named arguments, so `sharedOn`
    selects keys from a single object argument, and a tool using it must take one.

- **Not included:** the LangGraph checkpointer. It needs `@langchain/langgraph`
  as a peer dependency, and the Python implementation has a known defect under
  concurrent read-modify-write that should be fixed before it is copied into a
  second language.

- The Python and TypeScript SDKs will be aligned at `0.8.0` on the next release.
  Until then, matching version numbers do not imply matching feature sets:
  Python `0.7.0` had the high-level API and TypeScript `0.7.0` did not.

## 0.7.0

### Added

- Initial TypeScript/Node.js SDK. `CellaflowClient` over Connect RPC, covering
  `startSession`, `commitStep`, `getGraph`, `checkIdempotencyCache`, `renewLease`
  and `releaseLease`, with MessagePack serialization for state payloads.
