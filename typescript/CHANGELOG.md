# Changelog

All notable changes to the Cellaflow TypeScript SDK.

## Unreleased

### Added

- **A lease TTL for tools.** `tool(fn, { ttlMs })`, and `durableTools(thread, { ttlMs })` as the
  default for every tool in the block, set how long a call's lease lasts without a renewal, and so
  how soon another caller can take over a call whose process crashed or froze: one TTL after the
  holder's last renewal, plus up to a second for the waiting caller to ask again. A tool's own
  `ttlMs` overrides the block's, and `step` takes it too. Renewals ask for the same TTL, and the
  lease that records a call's shared result or failure in the session uses the TTL of that call.
  That lease is not renewed, so the TTL also bounds how long the record's write may take. Without
  `ttlMs` nothing changes: the engine's default applies, 20 s on current engines. A holder that
  stops renewing for one TTL while its body still runs is taken over too, so set it well above the
  longest pause a call can see. `ttlMs` must be a whole number of milliseconds, at least 1000
  (`MIN_LEASE_TTL_MS`, exported): otherwise `tool` throws a `RangeError` when the tool is created,
  and `durableTools` rejects with one before it connects. The engine's maximum lease lifetime still
  caps every lease.

### Fixed

- **An `executionLease` TTL above about 99 days made its heartbeat renew in a tight loop:** the
  renewal interval, a quarter of the TTL, overflowed Node's timers, which fire at once for anything
  over 2^31 - 1 ms. The heartbeat now caps the interval, so a tool's `ttlMs` gets the same cap.
- **A renewal that hung could hold up the return of a tool or an `executionLease` block for half a
  renewal interval** (7.5 minutes for an hour-long TTL), because stopping the heartbeat waits for
  the renewal in flight. A renewal
  is now abandoned after at most 10 s, as in the Python SDK.

## 0.10.0

### Behaviour changes

Code written against 0.9.0 can see these:

- **The exported protobuf messages are types, not classes.** The SDK now uses Connect 2 and
  protobuf-es 2. `StepResult`, `CheckCacheResponse`, `RenewLeaseResponse` and the other exported
  messages are plain object types, each with a `...Schema` export beside it.
  `StartSessionResponse` and `CommitStepResponse`, which `startSession` and `commitStep` return,
  are now exported with their schemas too. Code that called `new StepResult(...)`, tested
  `instanceof`, or called `toJson()` or `toBinary()` on a response builds and converts them with
  `create`, `toJson` and `toBinary` from `@bufbuild/protobuf`, passing the schema, as in
  `toJson(CommitStepResponseSchema, response)`. Those come from **version 2** of
  `@bufbuild/protobuf`: add `@bufbuild/protobuf@^2` to your own dependencies to import them, since
  an app that depends on version 1 gets that version's module, which has none of them. Field names
  and types are unchanged, so code that only reads fields from what the client returns is
  unaffected. The enums (`StepStatus`, `CacheStatus` and the others) are unchanged.
- **`JSON.stringify` on a response throws.** Responses with 64-bit fields (sequence numbers,
  fencing tokens) hold them as `bigint`, which `JSON.stringify` refuses with `TypeError: Do not
  know how to serialize a BigInt`; Connect 1's messages converted themselves. A line that logs a
  response this way now throws after the call succeeded. Use `toJson(<Schema>, response)` instead.
  Responses also carry a `$typeName` field, which shows up in `Object.keys`, spreads and
  deep-equality checks.
- **Node.js 18.14.1 or later,** up from 18.0.0, which Connect 2 requires.

### Compatibility

- Works with engine 0.2.0 and later. CI's integration tests run against `ghcr.io/cellaflow/cellaflow:0.2.0`,
  an engine started both without a key and with one. CI also runs the tests on Node.js 18.14.1, the
  lowest version supported.

### Added

- **An API key, for an engine that requires a token.** `CellaflowClient`, `durableTools`,
  `executionLease` and `taskLease` take `apiKey`, sent as `authorization: Bearer <key>` on every
  call. Without it, the key is read from `CELLAFLOW_API_KEY`, so existing code works against a
  secured engine with no change; a blank value means no key, and no other variable is read. Before
  this, the SDK could only talk to an engine running unauthenticated. The SDK never puts the key in
  an error message. A key that cannot be a header is refused, naming where it came from, and
  `taskLease` refuses one when it is made. A key is not sent without TLS to a host other than this
  machine: that is refused unless `CELLAFLOW_ALLOW_PLAINTEXT_KEY=true` or `allowPlaintextKey: true`
  allows it, for a network you trust, and an allowed one warns once per target.

### Fixed

- **`npm install` reported four vulnerabilities (two high) through the SDK.** Connect 1's Node
  transport depended on `undici` 5, which pulls in an affected `@fastify/busboy`. Connect 2's does
  not depend on `undici`, and an install of the SDK reports none. The affected code parses
  multipart form uploads, which the SDK never does. The SDK stays on Connect 2.0, the last line
  that supports Node 18.

## 0.9.0

0.8.1 was prepared but never published; its changes are part of 0.9.0.

### Behaviour changes

Code written against 0.8.0 can see these:

- **A session's history has more entries:** the result of a shared tool answered from another
  session, at its position in the receiving session, and calls with status `FAILED`.
- **An execution lease is treated as lost sooner:** no later than the engine treats it as expired,
  counted from when each renewal was sent. `check()` fails once the block has exited, and
  `executionLease` throws `LeaseLostError` rather than run its block on a lease already lost.
- **`tool()` renewals ask for the full lease lifetime,** four heartbeat intervals rather than
  three.
- **A resumed `durableTools` session reads its history at its first call,** one paged read, to
  continue after the positions it holds.

### Compatibility

- Verified against engine 0.2.0 (`ghcr.io/cellaflow/cellaflow:0.2.0`), which CI's integration
  tests run against. Older engines are untested.

### Added

- **A call's failure is recorded at its position.** When a caller catches a leased tool's error and
  carries on, the failure is recorded at the call's position as a step with status `FAILED`,
  carrying the error's name and message, and the next call takes the following position. It is
  written when the next call claims a position, so a failure nothing follows, one while another
  call of the session is in flight, or one in a session whose history the run did not read (a
  resumed `durableTools` session), is not recorded. After one that overlapped another call, the
  run stops recording, so no later failure is recorded above the position it left empty. If the failed call's lease could not be
  released, the record retries the release before each attempt, and otherwise waits for the lease
  to lapse. `durableTools` resumes by running tools
  again rather than by replaying positions, so the record fills the position; it is not replayed.
  The Python SDK records failures the same way and, in a `@workflow`, also replays them. The
  message keeps its first 4 KB and the stack its last 16 KB, and recording never replaces the
  error the tool threw, even one whose fields cannot be read.

- **`onEvent` on `executionLease`**, called on acquisition, every renewal outcome, loss and
  release, with a wall-clock time, the fencing token and, where the engine sent one, its expiry.
  For tracing; it cannot change what the lease does.

### Fixed

- **A resumed `durableTools` session can run a tool it had not reached.** A resumed session counted
  positions from 0, so its first call that was not answered from the cache (the node the thread
  had not reached yet, typically) claimed position 1, which the session already held, and the
  engine refused it with `DivergentStepError` before its body ran. A resumed session now continues
  after the last position it holds, read from its history at its first call, or taken from a
  cache hit that reports it. A session the engine reports as recovered but whose history it cannot
  find is an error, not a session at position 0; a read that fails with an error that may pass
  lets the call go ahead as before, and the next call reads again.

- **An execution lease is treated as lost no later than the engine treats it as expired.** Two
  things let a holder keep going after another worker had taken its lease. Renewals asked the
  engine for three heartbeat intervals (22.5 s at the default 30 s TTL) while loss was only declared
  after the full TTL, and only when a heartbeat tick came round. And `check()` and `signal` reflected
  the last renewal's answer, so a process resumed after a stop passed `check()` until its next
  renewal came back refused. Renewals now ask for the full TTL. Each confirmed renewal sets a
  deadline counted from when the renewal was sent, minus half a heartbeat interval; `check()` and
  `isLost` compare against it on every call, on the monotonic clock and on the wall clock (the
  monotonic clock stands still while a host is suspended), and `signal` aborts when it passes. A
  holder stopped past its deadline fails `check()` the moment it resumes, before any renewal
  answers. After waiting for a lease (`waitMs` above 0), a renewal is made before the block runs,
  since the engine may have granted the lease well after the request was sent. A failed attempt is
  retried with a short backoff; if none is confirmed, the deadline counted from the request still
  bounds the lease from below, so the block runs only while that has not passed. Both deadlines are
  counted from the send, each on its own clock, so a suspend while a renewal is answered delays
  neither, and the deadline timer wakes at least every half interval, so a woken host's `signal`
  aborts soon after it wakes.

  A passed deadline is reported but does not stop renewing: the engine may still hold the lease,
  and renewing keeps it until the engine refuses a renewal. `tool()`, which cannot act on a loss,
  keeps its lease for as long as the engine agrees, and its renewals now also ask for the full TTL
  (four heartbeat intervals) rather than three. If the engine then refuses a renewal, `onEvent`
  receives a second `lost` event carrying the engine's reason.

  Near the engine's maximum lease lifetime a renewal is granted less than it asked for. The
  engine's expiry on each renewal is mapped onto the wall clock through a recent renewal known to
  have been full (a later one expired strictly later) and with the least latency, so a shortened one
  moves the deadline in by the shortfall and one slow renewal does not distort the rest. Only
  differences between the engine's own timestamps are used, so clock skew between hosts does not
  matter. A first renewal that is already shortened, or a TTL above the engine's maximum lifetime,
  is not detected.

  Renewals time out after half an interval (short TTLs get up to a second, since a renewal is a
  synced write), so one that hangs gives up before the next is due, and
  the half-interval margin leaves a renewal sent one interval before expiry time to answer: one or
  two failed renewals in a row no longer lose a lease the engine still holds. Reading `signal`
  checks the clock, as `isLost` and `check()` do. In an `onEvent` callback for `lost`, the handle
  already reads as lost.

- **`executionLease` does not run its block on a lease already lost**, for example when the
  renewal after a wait is refused. It throws `LeaseLostError` instead.
- **`check()` fails once the block has exited**, so work the block started without awaiting cannot
  act under a lease that has been released. `signal` does not abort on release, only on loss: a
  response fetched with it can still be read after the block returns. `released` is reported only
  when the engine confirms the release.

  `check()` still cannot close the gap between the check and the write landing at the other end:
  hand `signal` to the request to narrow it.

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
