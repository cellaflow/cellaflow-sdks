# Changelog

All notable changes to the Cellaflow Python SDK.

## Unreleased

### Added

- **An org over its allowance gets a clear, final error.** A hosted engine refuses an org that has
  used up its allowance (its free steps, say) with RESOURCE_EXHAUSTED and status details saying so.
  The SDK raises that as `AllowanceExceeded` (exported), at once, wherever the engine's error
  surfaced before: a `@workflow` or `durable_tools` session start, a `@tool` or `@step` call,
  `execution_lease`, `async_execution_lease`, `@task_lease`, `CellaflowSaver`, and every
  `CellaflowClient` request, `check_idempotency_cache_future` included. It carries `subject`, what
  the allowance is for (`cellaflow.steps` today); `resets_at`, when it resets, as a UTC `datetime`;
  and `limit`, what is blocked until then (`new_sessions_blocked` or `writes_blocked`). The last two
  are `None` when the engine did not say. Its message says the org is over its allowance, for what,
  and when it resets, then the engine's own message. It is a `grpc.RpcError`, so `except
  grpc.RpcError` still catches it: `code()` is RESOURCE_EXHAUSTED, `details()` the engine's message
  and `trailing_metadata()` what the engine sent, and the engine's error is its `__cause__`. Like
  gRPC's own error for a failed call it is a `grpc.Call` too, answering `initial_metadata()`,
  `is_active()`, `time_remaining()`, `cancel()` and `add_callback()` as that call does, or as a
  finished call would once there is none. It can be pickled. A `resets_at` that is not an RFC 3339
  time, names a year before 100, or falls past the year 9999 in UTC, reads as `None`, and the
  message prints the reset to the millisecond, both as the TypeScript SDK does. See "When the org is
  over its allowance" in the README.
- **`CellaflowClient.check_idempotency_cache_future` returns a wrapper of the call.** It is still a
  `grpc.Future` and a `grpc.Call`, and fails with `AllowanceExceeded` where the call failed with
  an allowance refusal; everything else is passed through to the call (`code()`, `details()`,
  metadata, `is_active()`, `time_remaining()`, `cancel()`, `add_callback()`, and done callbacks,
  which receive the wrapper).

### Behaviour changes

- **An allowance refusal is not retried.** Every RESOURCE_EXHAUSTED counted as an error that may
  pass, so an over-allowance `@tool` or `@step` call was asked for twice more and then failed with
  the engine's error, kept as a lease that might have been granted, and the failure recorded as a
  workflow returned was retried for up to 45 s. Now the call fails at once, with nothing kept to
  wait out, and no lease request, commit, renewal or history read the engine refused this way is
  sent again; nothing loops or sleeps on the refusal. A refused release, and the lease on a record
  the SDK keeps pending, are sent again once, by the next call. A commit that got no answer and
  whose resend is refused fails with the refusal, not with the first attempt's error, including the
  resend made when the engine says the lease is no longer the call's; when the first attempt failed
  with a definite error, that error is still the step's failure, recorded as before. A refusal
  raised by a client other than `CellaflowClient` (a stub of your own, or a test double, raising the
  engine's error as it arrives) is recognised the same way. A plain RESOURCE_EXHAUSTED, such as a
  session's full mailbox, is retried as before.
- **A refused renewal stops the renewals, not the lease.** The engine still holds the lease until
  its deadline, so a body that finishes in time commits as usual. At the deadline the lease is lost
  as usual: `on_lease_lost` runs, an `async_execution_lease` caller is cancelled, and `check()` or
  `cancel_on_loss()` raise `LeaseLostError` ("Execution lease was lost: the engine refused to renew
  it because the Cellaflow org is over its allowance for cellaflow.steps, and its deadline passed.
  The lease may already belong to another worker.", as in the TypeScript SDK) with the
  `AllowanceExceeded` as its `__cause__`. That is decided when the loss is reported: a holder that
  had already stalled past its deadline gets the plain loss, even if a renewal is refused after. A
  `@tool` or `@step` whose body meets that loss fails with the `AllowanceExceeded` itself. A call
  whose lease the engine granted after a wait, and refused the renewal that confirms it, hands the
  lease back and fails with the refusal without running its body, rather than asking for another
  lease.
- **A refused release is kept, and sent again later.** A step's lease the engine would not release
  for the allowance is kept with its token, as one whose release went unanswered is. The next call
  or failure record sends the release again before writing the record: while the allowance is still
  exceeded that call fails with the refusal at once, rather than wait for the lease to lapse, and
  once it is back the release goes through. A lease that has lapsed by then no longer holds the
  record up: its refused release is dropped and the record is written.
- **A step stopped by an allowance refusal is not recorded as failed**, even when the workflow
  catches the refusal, so a resume does not raise the refusal again after the allowance has reset.
  The same holds for a step whose body raised an error made from a refusal (`raise
  OrderFailed(...) from refusal`, up to eight causes deep) or an exception group holding one: the
  caller still gets that error as it was raised, but nothing is recorded. If the refusal came
  after the body ran, when its result was to be recorded, calling the step again runs the body
  again. A session stays resumable if the refusal ends the run, or the workflow calls the same step
  again after the reset; one that goes on to a different step gives it the refused step's
  position, and resuming it after the reset fails with `ReplayDivergedError`, so start a new
  session for that work.
- **Where the next step lands after a refusal.** As in the TypeScript SDK, the refused step's
  position goes to the next call when no later one has been claimed meanwhile, even with lower calls
  still running (a step that called it, which the refusal then stops too). A position below a later
  one is vacated instead: the counter steps down through it when the later one is given back too, or
  a call claims it again, and while it stays empty the run records nothing into the session. A
  failure left unrecorded meanwhile stops recording for good. A refused step whose lease could not
  be released (its release unanswered or refused) leaves that release owed at its position, given
  back or vacated alike: the next call to claim the position sends it first, so it never lands
  behind a lease that still claims the position. Answered, the call goes on at once; refused for the
  allowance, it fails at once with the refusal; unanswered, the call waits, sending the release
  again about once a second, until it is answered or the lease lapses (at most the lease's TTL plus
  a few seconds). An async call can be cancelled while it waits; a sync call waits until then. A
  call whose lease grant got no answer and whose next request was then refused leaves that lease
  owed too, though no release can be sent for it: the next call waits until the lease would lapse,
  looking again about once a second. A step whose commit got no answer, and whose resend was
  refused, gives its position back too, but unless the engine then released the lease, which shows
  the commit did not land, the run also stops recording into the session. A taken-over step that is
  refused is handed back at the top, as a takeover is in the TypeScript SDK, and otherwise leaves
  its position empty and stops recording, since the new holder may fill it. A takeover's failure is
  still recorded at its position, and a process exit gives its position back, as before. A cache
  hit's result or a caught failure waiting to be recorded when the next step starts is kept but not
  retried: that step raises the refusal, and the first call after the reset writes the record. One
  recorded as a workflow returns is dropped at once with a warning naming the allowance, and a
  re-run runs that step again.
- **`CellaflowSaver` raises a session start the engine refuses for the allowance**, where it logged
  any refused start and went on to the commit.

### Compatibility

- Defines what the SDK reads ahead of the engine: no engine sends an allowance refusal yet, and
  against one that does not, nothing changes. The TypeScript SDK reads the same details the same
  way. They are decoded with protobuf alone, in a descriptor pool of the SDK's own, so the package
  needs no new dependency, and an application that also uses `googleapis-common-protos` does not
  find the `google.rpc` types defined twice.

## 0.13.0

### Behaviour changes

- **`CheckCacheResponse.current_holder_id` is removed.** It was the engine's opaque id for the
  holder's grant of the lease, which `CellaflowClient.check_idempotency_cache` returned on
  `IN_PROGRESS`. The same value is now `current_holder.engine_holder_id`, and
  `current_holder.agent_id` is the worker that acquired the lease. Code that reads or assigns
  `current_holder_id` raises `AttributeError`, and `HasField("current_holder_id")` or
  `CheckCacheResponse(current_holder_id=...)`, in a test double for instance, raises `ValueError`.

### Compatibility

- Tested with engine 0.5.0: CI's integration tests run against `ghcr.io/cellaflow/cellaflow:0.5.0`.
- `holder` on `LeaseNotAcquired` needs engine 0.5.0 or later; with an older engine it is always
  `None`, and everything else works as before. Engine 0.5.0 also refuses a `worker_id` (or a step's
  agent id) longer than 1024 bytes or containing control characters.

### Added

- **A refused lease says who holds it.** When `execution_lease`, `async_execution_lease` or
  `@task_lease` is refused because another worker holds the lease, `LeaseNotAcquired` carries
  `holder`, a `LeaseHolderInfo` (exported, a frozen dataclass): `worker_id`, the id that worker
  acquired the lease with; `engine_holder_id`, the engine's id for that grant of the lease;
  `acquired_at_ms`; `expires_at_ms`, when the lease lapses unless renewed; and `hard_expires_at_ms`,
  its ceiling. Times are epoch milliseconds on the engine's clock. The `worker_id` is what that
  worker said it was, not an authenticated identity. `worker_id`, `acquired_at_ms` and
  `hard_expires_at_ms` are `None` rather than empty or 0 when the engine did not record them, and
  `holder` itself is `None` for a key that is already completed, for a `@tool` or `@step` call whose
  granted leases could not be confirmed, and when the engine is too old to report it.
  `LeaseNotAcquired` is still a `RuntimeError`, takes the same positional arguments, and its message
  is unchanged. A test double standing in for the engine's answer (a `MagicMock`, specced or not)
  still gives a refusal, with `holder` as `None`. When a step raises the refusal and the workflow catches it, the recorded failure
  keeps the holder, so a resumed run reads the same one.

## 0.12.0

### Compatibility

- Tested with engine 0.3.0: CI's integration tests run against `ghcr.io/cellaflow/cellaflow:0.3.0`.

### Changed

- **A step waits for another agent's lease in the engine, not by polling.** A `@tool` or `@step`
  call that finds the key held asks the engine to wait (up to 10 s per request, asking again until
  it has an answer). It gets the holder's result the moment the holder commits, or the lease the
  moment it is released or lapses, instead of up to a second later. The engine checks the holder
  every 100 ms for all the callers waiting on one key, and each sends a new request about once
  every 10 s, instead of once a second. An `async` step waits on the gRPC call itself
  (`CellaflowClient.check_idempotency_cache_future`), holding neither the event loop nor a thread,
  so any number of waiters cannot delay the renewals of a call that holds a lease. A call
  cancelled during the wait lets its request finish (within its 15 s deadline) and releases any
  lease it is granted. A request that fails without an answer (no answer within 15 s, the engine
  unavailable, a dropped connection) is asked again, and after three in a row the call fails, so a
  lost connection to the engine is noticed. A proxy between the SDK and the engine must let a
  request run for at least 15 s. An engine that answers at once without waiting is asked again
  after its `retry_after_ms`, as before. The lease that records a shared result or failure in the
  session still asks without waiting: it is requested under the session's lock, and a held record
  is rare. A test double standing in for `CellaflowClient` now receives `wait_timeout_ms`
  and `timeout` on `check_idempotency_cache`.
- **A lease granted after a wait is counted from when it was granted.** The lease can be much younger
  than the request that won it, so the call sends a renewal before the body starts and counts the
  lease from that. A grant that comes back within half a renewal interval (an eighth of the TTL)
  costs no extra request. If no renewal confirms the lease and the time counted from the request has
  already passed, the body does not start under it: the call hands the lease back and asks again.
  After three such leases the call fails with `LeaseNotAcquired`, saying why, instead of asking
  forever.

### Added

- `CellaflowClient.check_idempotency_cache_future`: the same request as a `grpc.Future`, which can
  be cancelled, for a caller that waits without holding a thread.

## 0.11.0

### Compatibility

- Tested with engine 0.3.0: CI's integration tests run against `ghcr.io/cellaflow/cellaflow:0.3.0`.

### Added

- **A lease TTL for steps and tools.** `@tool(lease_ttl_ms=...)` and `@step(lease_ttl_ms=...)`,
  and `durable_tools(..., lease_ttl_ms=...)` as the default for every call in the block, set how
  long a call's lease lasts without a renewal, and so how soon another caller can take over a call
  whose process crashed or froze: one TTL after the holder's last renewal, plus up to a second for
  the waiting caller to ask again. A step's own value overrides the block's. Renewals ask for the
  same TTL and come at least every quarter of it, and the lease that records a call's shared result
  or failure in the session uses the TTL of that call. That lease is not renewed, so the TTL also
  bounds how long the record's write may take. Without `lease_ttl_ms` nothing changes: the engine's
  default applies, 20 s on current engines. A holder that stops renewing for one TTL while its body
  still runs is taken over too, so set it well above the longest pause a call can see. It must be a
  whole number of milliseconds, from 1000 to 2^53 - 1 (`MIN_LEASE_TTL_MS` and `MAX_LEASE_TTL_MS`,
  exported), the same range as the TypeScript SDK's `ttlMs`: otherwise the decorator or
  `durable_tools` raises `ValueError`, or `TypeError` for a value that is not an `int`, before
  anything connects. The engine's maximum lease lifetime still caps every lease.

- **A step's body can check its lease.** `current_lease()` inside a `@tool` or `@step` body returns
  the call's lease, as it does inside `execution_lease` and `@task_lease`. A holder that froze and
  woke up after another caller took the call over fails `check()` as soon as it resumes, so it does
  not send. `LeaseHandle.cancel_on_loss()` cancels the awaits inside it when the lease is lost and
  raises `LeaseLostError` in their place. See "Checking the lease inside a step" in the README. The
  clock only tells the body to stop; whether the call's result is recorded is still the engine's
  decision.
- **A step body can send its call's idempotency key to the destination.** `current_lease().key` is
  the key the call's lease was taken under, the same on every attempt at the same call, and
  `key_digest` its SHA-256 (64 lowercase hex characters). `idempotency_headers()` returns an
  `Idempotency-Key` header carrying the digest (or the key, with `raw=True`) to merge into a request,
  so a destination that deduplicates applies a request repeated after a crash once.
  `key_digest_for(part)` and `idempotency_headers(part=...)` give each of several requests its own
  key, the same as the TypeScript SDK's. Execution-lease handles carry their key too. See "Sending
  the call's key to the destination" in the README.

### Changed

- **A lease is judged on the clock.** `LeaseHandle.check()` and `is_lost` now fail once a TTL has
  passed since the last confirmed renewal was sent, less half a renewal interval, without waiting
  for a renewal to fail. This applies to `execution_lease`, `async_execution_lease` and
  `@task_lease` too: a holder resumed past its deadline now fails `check()` straight away, and an
  `async_execution_lease` caller is cancelled then, where before it ran until a renewal was
  refused.
- **A step body nested in `execution_lease` or `@task_lease` now gets the step call's lease from
  `current_lease()`**, not the enclosing one. Code that passes `current_lease().fencing_token` to a
  downstream fence from inside a step should read the enclosing lease before calling the step. A
  body the SDK runs without a lease of its own sees no lease at all.
- **A step whose commit the engine refuses because another worker holds the call now ends with
  `LeaseLostError`** (the body's return value on its `result`, the refusal as its `__cause__`)
  instead of the engine's FAILED_PRECONDITION. The failure is recorded at the step's position as
  before, so a resumed `@workflow` replays it there.
- **A step commit that gets no answer is sent again** before the call's lease is released, and once
  more if the engine then refuses the release. An engine that already holds the commit answers it as
  done, and while the lease is still the call's a resend is recorded, so a call whose commit answer
  was dropped once returns its result, where before it failed with the transport error (and, if the
  commit had not reached the engine, a retry ran the body again). An engine that stays unreachable
  still fails the call, as before. An interrupt (Ctrl-C, `SystemExit`) is never resent.
- **`execution_lease` and `async_execution_lease` handles read as released once their block ends**,
  so work the block left running finds `is_lost` true and `check()` raising, without the lease
  being reported lost, `on_lease_lost` running, or the caller being cancelled after the block.

### Fixed

- **A lease heartbeat whose renewal interval was too long for Python's timers stopped with
  `OverflowError`,** so the lease was never renewed: from a TTL of about 199 days on Windows, and
  over a thousand years elsewhere. The heartbeat now caps its interval at 2^31 - 1 ms, as the
  TypeScript SDK does.
- **A renewal that hung could hold up a step's return for up to a quarter of a long TTL** (15
  minutes for an hour-long `execution_lease`). A renewal is now abandoned after at most 10 s.
- **With a long `lease_ttl_ms`, an engine outage could hold up a workflow's return for as long as
  the TTL.** A workflow that caught a step's failure and returned waits to record the failure, and
  when the step's lease could not be released, it waited for that lease to lapse even while the
  engine was unreachable. It still waits out a lease the engine reports holding the position, but
  gives up after 30 s of errors in a row (a hung call counts), drops the record with a warning, and
  returns, so a re-run runs the step again. At the engine's default TTL the wait is about 27 s, so
  nothing changes there.

## 0.10.0

### Compatibility

- Works with engine 0.2.0 and later. CI's integration tests run against `ghcr.io/cellaflow/cellaflow:0.2.0`,
  an engine started both without a key and with one.
- Requires grpcio 1.71.0 or later (see Fixed).

### Fixed

- **The package declared `grpcio>=1.60.0`, but needs 1.71.0.** The generated code checks the
  installed grpcio at import, so with anything from 1.60 to 1.70 the install succeeded and
  `import cellaflow` then failed. The floor is now 1.71.0, and CI runs the tests at the lowest
  grpcio and protobuf the package declares.

### Added

- **An API key, for an engine that requires a token.** `CellaflowClient`, `@workflow`,
  `durable_tools`, `execution_lease`, `async_execution_lease`, `task_lease` and `CellaflowSaver`
  take `api_key`, sent as `authorization: Bearer <key>` on every call, streaming calls included.
  Without it, the key is read from `CELLAFLOW_API_KEY`, so existing code works against a secured
  engine with no change; a blank value means no key, and no other variable is read. Before this,
  the SDK could only talk to an engine running unauthenticated. The SDK never puts the key in a log
  message, an exception message or a `repr`; grpc's own stack frames hold the header, so turn off
  local-variable capture in error reporting wherever the key matters. A key that cannot be a header
  (outside printable ASCII, or with surrounding whitespace) is refused before a connection is
  opened, naming where it came from, and a decorator refuses one when it is applied.
  `CellaflowSaver` refuses an `api_key` given beside a `client`, which would have ignored it. A
  key is not sent without TLS to a host other than this machine: that is refused unless
  `CELLAFLOW_ALLOW_PLAINTEXT_KEY=true` or `allow_plaintext_key=True` allows it, for a network you
  trust, and an allowed one logs a warning once per target. A key sent without TLS to this machine goes
  on a channel that bypasses `https_proxy` and on which gRPC checks the peer really is this
  machine.

## 0.9.0

### Behaviour changes

Code written against 0.8.0 can see these:

- **A resumed `@workflow` raises a caught step failure again instead of running the step.** When
  the first run caught a step's exception and carried on, the failure is recorded at its position,
  and a resume raises it there, so the workflow takes the branch it took. A failure the workflow
  let propagate is still not recorded, so a resume still runs that step again.
- **A recorded cancellation makes a resume refuse,** with `FailureNotReplayableError`, rather than
  guess what cancelled the step. Put a timeout inside the step's body instead: it records a plain
  `TimeoutError`, which replays.
- **A refusal stops the run even if the workflow catches it.** After `ReplayDivergedError` or
  `FailureNotReplayableError`, every later step raises it again before its body runs, and the
  workflow raises it when it ends.
- **A session's history has more entries:** the result of a shared step answered from another
  session, at its position in the receiving session, and steps with status `STEP_STATUS_FAILED`.
- **A workflow that returns right after a caught failure waits for its record before returning.**
  Normally one round trip; up to the failed step's lease lifetime (about 27 seconds by default)
  when that lease could not be released.
- **A resumed `durable_tools` session reads its history at its first call,** one paged read, to
  continue after the positions it holds.

### Compatibility

- Verified against engine 0.2.0 (`ghcr.io/cellaflow/cellaflow:0.2.0`), which CI's integration
  tests run against. Older engines are untested.

### Added

- **A step's failure is recorded at its position, and replayed.** When a workflow catches a step's
  exception and carries on, the failure is recorded at the step's position as a step with status
  `STEP_STATUS_FAILED`, and the next step takes the following position. A resumed `@workflow`
  raises the same exception there instead of running the step again, so it takes the same branch.

  The exception comes back as an instance of its own class, with its arguments and attributes, so
  `except` clauses and `isinstance` checks behave as they did. Rebuilding it runs no code the
  record can choose: the class is looked up only among modules already loaded, nothing is
  imported, and the class's own `__new__` and `__init__` are not called. A class that only
  compiled code in an extension module can construct, such as pydantic's `ValidationError`, cannot
  be rebuilt that way, so its resume is refused. Its class must be defined
  at module level in a module the workflow imports, as it is when an `except` clause names it. A
  library the step imports inside its own body is not imported on resume, since the step does not
  run; every run warns when such a class is recorded, and a resume that cannot find it names the
  import to add, after which the same session resumes.
  Otherwise the resume stops with `FailureNotReplayableError` rather than run the step again.
  The original run logs a warning when the failure is recorded if the class cannot be found by name,
  is defined in `__main__`, or has values that cannot be recorded, such as an HTTP response
  object; each comes back as a placeholder, and using it raises `AttributeError` (Python's
  `AttributeError.obj` is replayed the same way, without a warning). A placeholder prints as the
  value did, so an exception whose argument was not recorded keeps its message. The run also
  warns about anything else a resume would refuse, because the record is rebuilt once as a resume
  would rebuild it. These warnings come when the
  failure is recorded, so a failure the workflow lets propagate says nothing. Tuples,
  enums and subclasses of plain types are recorded, the last two as their plain value, as are the
  fields Python keeps outside `args` (`OSError.errno`, `ImportError.name` and the like) and fields
  in a class's `__slots__`. An
  exception group comes back with its members, every one of them keeping at least its type and
  message, so `except*` behaves the same; a group too large to record that way (over 2,000 members
  or 512 KB) is refused on resume, with a warning when it happens. An engine call's gRPC
  error comes back as a `grpc.RpcError` with the same `code()` and `details()`; a subclass of
  `grpc.RpcError` defined outside gRPC comes back as itself. A record is kept
  within a commit's size: a value over 64 KB is left out, and the traceback is trimmed. Recording a
  failure never replaces the step's own exception, and neither does a lease that cannot be
  released after it.

  The exception is recorded as the step raised it, before the workflow's handler runs, so a
  handler that changes it finds the original values on replay too; the record keeps what was
  captured, not the exception, so the failed step's frames are released.

  A failure the workflow lets propagate is not recorded, so resuming retries the step, unless a
  step runs while it propagates (a cleanup in a `finally`): that step needs the next position, so
  the failure is recorded to fill it, and a resume ends with the same exception. The run warns when
  it ends that way, and says to start a new session to run the step again. A failure
  while another step of the session is in flight is not recorded either, and the run then stops
  recording, so no later failure is recorded above the position it left empty; nor is one in a run that has
  not read the session's history (a resumed `durable_tools` thread). A record still unwritten when
  the workflow returns is written before the workflow returns its result, because a record lost
  after the run reported its result would let a re-run run the step again. That costs one write's
  round trip at the end of such a run; in an async workflow, a caller that cancels the workflow
  meanwhile still has the write finish on its worker thread. Errors that may pass are retried,
  and every engine call writing a record has a deadline of at most 10 seconds, shortened to what
  is left of the time allowed. If the failed step's lease could not be released, the record
  retries the release, or waits for the lease to lapse (allowing for a renewal still on its way),
  rather than be refused at a position that lease still claims; otherwise it keeps trying for 5
  seconds. A record that still cannot be written, or that the engine refuses, is dropped with a
  warning, and a re-run runs the step again. In an async
  workflow, records are written on a worker thread, so the event loop is not held up.

  A cancelled async step (`asyncio.CancelledError`) is recorded the same way, with how long it ran,
  and its lease is released, so the workflow's next step lands at the right position. A resume
  refuses at it with `FailureNotReplayableError` rather than replay it: what the workflow saw
  depended on what cancelled the step (`asyncio.wait_for` turns its own cancellation into
  `TimeoutError`, one from outside arrives as `CancelledError`), which the record cannot say, and
  waiting to be cancelled again cannot reliably tell either, since earlier steps replay instantly
  and move every timer. A timeout inside the step's body (`return await asyncio.wait_for(call(),
  5)`) records a plain `TimeoutError` instead, which replays exactly. A cancellation the workflow
  carries on past is recorded even when nothing follows it, so re-running a finished session
  refuses there rather than run the cancelled step again; read the session's history to see how it
  ended. A `CancelledError` raised out of a synchronous step is that step's own exception, and
  is recorded and replayed as one.

  A refusal stops the run even when the workflow catches it: every later step raises it again
  before its body runs, and the workflow raises it when it ends, so a broad `except` around the
  refused step cannot carry the run down a branch nothing checked.

- **Two kinds of `NondeterministicWorkflowError`.** `ReplayDivergedError` means the resumed run
  called a different step than its history records at a position; its message names the session,
  the position, both steps, the last failure before them, and the way on.
  `FailureNotReplayableError` means a recorded failure or cancellation cannot be reproduced as it
  happened. Both subclass `NondeterministicWorkflowError`, so code that catches it still does.

### Fixed

- **A resumed `durable_tools` thread can run a tool it had not reached.** A resumed session counted
  positions from 0, so its first call that was not answered from the cache (the node the thread
  had not reached yet, typically) claimed position 1, which the session already held, and the
  engine refused it with `DivergentStepError` before its body ran. A resumed session now continues
  after the last position it holds, read from its history at its first call, or taken from a
  cache hit that reports it. A session the engine reports as recovered but whose history it cannot
  find is an error, not a session at position 0; a read that fails with an error that may pass
  lets the call go ahead as before, and the next call reads again.

- **A session that received a shared result can be resumed.** When a `SCOPE_SHARED` step was
  answered from another session's result, nothing was recorded at its position in the receiving
  session, so the next step took that position. Resuming the session then found the next step where
  the shared one was expected and raised `NondeterministicWorkflowError`. The shared result is now
  recorded at its own position, under the tool's name, when the next step starts. It is leased
  under its own key like any step, so the engine refuses it at a position another lease in the
  session has claimed. A workflow that ends on the shared step still makes no extra call.

  Recording happens only while the run has accounted for everything the session holds and no other
  step of the session is in flight. A `durable_tools` thread that resumes, a run that finds the
  session further along than it counted, and steps running concurrently in one session (on threads
  or as `asyncio` tasks) keep the previous behaviour, so a resume never writes a second record of a
  step.

  If writing the record fails with an error that may pass on a retry, it is kept and sent again
  before the next step claims a position. If another writer in the session committed or claimed the
  position first, or the engine does not accept the record, the run logs a warning and records no
  further received results for the rest of the run.

- **A resumed `@workflow` reads its whole history, however long,** following the engine's pages.
- **Steps on several threads in one session claim distinct positions.** Claiming a position is now
  a single locked step, so tools a framework runs on a thread pool inside one `durable_tools` block
  never claim the same position.

## 0.8.0

### Added & Changed

- **The coordination domain is sent to the engine and recorded on every step.**
  `_coordination_id` on a `@workflow` call, and `coordination_id` on `durable_tools`, are now sent
  with `StartSession`. The engine stores the value as the session's default coordination domain and
  records every step under it.

- **A resumed session adopts its stored domain.** Resuming with `_session_id` and no
  `_coordination_id` now picks up the domain the session was started with, so `SCOPE_SHARED` tools
  keep deriving the same keys. Resuming with a different domain, or naming one on a session started
  without one, is refused by the engine with `FAILED_PRECONDITION`.

- **A step can override the domain for one call.** Pass `_coordination_id` when calling a `@step`
  or `@tool`:

  ```python
  refund(customer, 25, _coordination_id=customer)
  ```

  The call is recorded under that domain and, under `SCOPE_SHARED`, keyed by it, so it converges
  with other agents using the same domain whatever their session default. The key format is
  unchanged; its coordination input is now the call's override if given, otherwise the session's.

- **Coordination ids are validated.** A coordination id may not contain colons, may not have
  leading or trailing whitespace, and may not exceed 256 bytes. An empty string means none. Invalid
  values raise `ValueError` before the session starts or, for an override, before the step runs,
  where previously any string was accepted. A value that is not a string, such as an int or a
  `uuid.UUID`, raises `TypeError`; pass `str()` of it. Values are never trimmed.

- `CellaflowClient.start_session` and `commit_step` take an optional `coordination_id`, and
  `get_graph` returns each step's `coordination_id`.

Engines that do not support the field ignore it. Keys are derived exactly as before against them,
from the value the caller passed.

### Packaging

- The published package includes the license text, and its homepage and project links point to
  cellaflow.com.

## 0.7.0

### Added & Changed

- **Distributed Execution Lease Primitive**:
  Added native Python SDK support for the `task_execution_lease` primitive, which acts as a distributed lock with liveness heartbeating.
  - Added synchronous `execution_lease` and asynchronous `async_execution_lease` context managers.
  - Added a first-class `@task_lease` decorator for wrapping execution entry points.
  - Implements lease loss preemption: if the heartbeat fails and the lease expires while the main thread/task is running, it aborts execution (e.g., by injecting `asyncio.CancelledError` or invoking a callback).
  - Yields a fencing token accessible downstream to enforce the lock.

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

- **Protobuf runtime mismatch & toolchain pinning**:
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

- **`shared_on` for cross-session convergence**:
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
