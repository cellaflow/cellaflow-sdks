# Changelog

All notable changes to the CellaFlow TypeScript SDK.

## Unreleased

### Added

- **A call's failure is recorded at its position.** When a caller catches a leased tool's error and
  carries on, the failure is recorded at the call's position as a step with status `FAILED`,
  carrying the error's name and message, and the next call takes the following position. It is
  written when the next call claims a position, so a failure nothing follows, or one while another
  call of the session is in flight, is not recorded. `durableTools` resumes by running tools
  again rather than by replaying positions, so the record fills the position; it is not replayed.
  The Python SDK records failures the same way and, in a `@workflow`, also replays them. The
  message keeps its first 4 KB and the stack its last 16 KB, and recording never replaces the
  error the tool threw, even one whose fields cannot be read.

### Fixed

- **A session's history includes shared results it received.** When a `SHARED` tool was answered
  from another session's result, nothing was recorded at its position in the receiving session, so
  the next step took that position and the session's history left the shared step out. The result
  is now recorded at its own position, under the tool's name, when the next step starts. It is
  leased under its own key like any step, so the engine refuses it at a position another lease in
  the session has claimed. A run that ends on the shared tool still makes no extra call. Recording happens only
  while no other call of the session is in flight. A resumed `durableTools` session, one further
  along than the run counted, and calls running concurrently in one session (under `Promise.all`,
  say) keep the previous behaviour, so a resume never writes a second record of a step. Matches the
  Python SDK.

  If writing the record fails with an error that may pass on a retry, it is kept and sent again
  before the next step claims a position. If another writer in the session committed or claimed the
  position first, or the engine does not accept the record, the run logs a warning and records no
  further received results for the rest of the run.

## 0.8.0

### Added & Changed

- **The coordination id is sent to the engine and recorded on every step.**
  `coordinationId` on `durableTools` is now sent when the session starts. The engine stores it
  as the session's default and records every step under it. A later block for the same thread
  that omits it adopts the stored value, so `SHARED` tools keep deriving the same keys; naming a
  different one is refused by the engine before the callback runs.
- **A call can run under a different coordination id.** A leased tool has
  `withCoordinationId(id)`, which runs one call under `id`:

  ```typescript
  await refund.withCoordinationId("customer-88")({ customer: "customer-88", cents: 2500 });
  ```

  That call is recorded under `id`, and its `SHARED` key is derived from `id`. The key format is
  unchanged; its coordination input is now the call's id if given, otherwise the session's.
- **Coordination ids are validated.** A coordination id may not contain colons, may not have
  leading or trailing whitespace, may not exceed 256 bytes, and must be a string. An empty string
  means none. Invalid values throw before the session starts or, for a single call, before it
  takes a sequence number or a lease. Values are never trimmed. `normalizeCoordinationId` and
  `MAX_COORDINATION_ID_BYTES` are exported.
- `CellaflowClient.startSession` and `commitStep` take an optional `coordinationId`, and
  `getGraph` returns each step's `coordinationId`. `sessionCoordinationId` and the `LeasedTool`
  type are exported.
- `npm test` builds the package and runs the tests under `tests/*.test.mjs`, which need no engine.

### Compatibility

- **Breaking:** a `coordinationId` containing a colon, with leading or trailing whitespace, over
  256 bytes, or not a string was accepted before and now throws.
- Against an engine without the field, the requested coordination id is kept and keys are derived
  exactly as before.

### Packaging

- The published package includes the license text, and its homepage and project links point to
  cellaflow.com.

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
