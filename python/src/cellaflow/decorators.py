import functools
import inspect
import uuid
import asyncio
import time
import logging
from typing import Any, Callable, Dict, Optional, Sequence, TypeVar, cast

import grpc

from cellaflow.client import CellaflowClient
from cellaflow.context import (
    WorkflowContext,
    set_context,
    reset_context,
    get_context,
    session_coordination_id,
)
from cellaflow.failures import ReplayedFailureError, rebuild
from cellaflow.history import iter_session_steps
from cellaflow.idempotency import (
    derive_idempotency_key,
    normalize_coordination_id,
    IdempotencyScope,
)
from cellaflow.lease import LeaseHeartbeat
from cellaflow.serialization import deserialize
from cellaflow.v1.common_pb2 import STEP_STATUS_FAILED, STEP_STATUS_SUCCESS
from cellaflow.v1.idempotency_pb2 import (
    CACHE_STATUS_HIT,
    CACHE_STATUS_ACQUIRED,
    CACHE_STATUS_IN_PROGRESS,
)

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def workflow(
    version: str = "1.0.0", target: str = "localhost:50051", secure: bool = False
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            # Pop _session_id if provided for recovery, else generate new
            session_id = kwargs.pop("_session_id", str(uuid.uuid4()))
            # Identifies the shared work, not the session. Agents in
            # different sessions pass the same value to converge on one key.
            coordination_id = normalize_coordination_id(
                kwargs.pop("_coordination_id", None)
            )

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__,
                version=version,
                session_id=session_id,
                coordination_id=coordination_id,
            )

            replayed_steps = {}
            if resp.is_recovered:
                # Every page: a step left out would run again instead of
                # replaying. The engine just reported the session as recovered, so
                # NOT_FOUND here is a failure, not an empty history.
                steps = iter_session_steps(
                    client, resp.session_id, limit=1000, missing_ok=False
                )
                for step_info in steps:
                    # Keep the name, not just the payload. Replay is
                    # keyed on position, and without the name there is no way to
                    # tell a legitimate replay from a different step landing on
                    # the same index.
                    replayed_steps[step_info["sequence"]] = {
                        "name": step_info["name"],
                        "payload": step_info["output_payload"],
                        "status": step_info.get("status"),
                    }

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
                workflow_version=resp.version,
                sequence=0,
                replayed_steps=replayed_steps,
                coordination_id=session_coordination_id(resp, coordination_id),
                # A fresh session holds nothing, and a recovered one is replayed
                # from the start, so the run accounts for every position.
                history_known=True,
            )
            token = set_context(ctx)
            try:
                result = await func(*args, **kwargs)
                # A failure the workflow caught, with no step after it to record
                # it. Only on a normal return: a failure the workflow lets
                # propagate stays unrecorded, so resuming retries the step.
                await ctx.record_pending_failure_async()
                return result
            finally:
                reset_context(token)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            session_id = kwargs.pop("_session_id", str(uuid.uuid4()))
            # Identifies the shared work, not the session. Agents in
            # different sessions pass the same value to converge on one key.
            coordination_id = normalize_coordination_id(
                kwargs.pop("_coordination_id", None)
            )

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__,
                version=version,
                session_id=session_id,
                coordination_id=coordination_id,
            )

            replayed_steps = {}
            if resp.is_recovered:
                # Every page: a step left out would run again instead of
                # replaying. The engine just reported the session as recovered, so
                # NOT_FOUND here is a failure, not an empty history.
                steps = iter_session_steps(
                    client, resp.session_id, limit=1000, missing_ok=False
                )
                for step_info in steps:
                    # Keep the name, not just the payload. Replay is
                    # keyed on position, and without the name there is no way to
                    # tell a legitimate replay from a different step landing on
                    # the same index.
                    replayed_steps[step_info["sequence"]] = {
                        "name": step_info["name"],
                        "payload": step_info["output_payload"],
                        "status": step_info.get("status"),
                    }

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
                workflow_version=resp.version,
                sequence=0,
                replayed_steps=replayed_steps,
                coordination_id=session_coordination_id(resp, coordination_id),
                # A fresh session holds nothing, and a recovered one is replayed
                # from the start, so the run accounts for every position.
                history_known=True,
            )
            token = set_context(ctx)
            try:
                result = func(*args, **kwargs)
                # See the async wrapper.
                ctx.record_pending_failure()
                return result
            finally:
                reset_context(token)

        if inspect.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        return cast(F, sync_wrapper)

    return decorator


class DivergentStepError(RuntimeError):
    """Raised when the engine refuses a lease because its position is taken.

    Two replicas of one agent reached the same step with *different* arguments.
    Different arguments hash to different idempotency keys, so from the engine's
    view these are unrelated operations and neither blocks the other. The
    divergence is only detectable against the graph position they both target.

    Another writer holds that position: it committed there, or holds a live lease
    that claimed it. This call is refused *before* running its tool body, so only
    one replica reaches the side effect. Without the claim, the refusal came only
    at commit time, after the irreversible action had already happened.

    The fix is in the workflow, not the call: whatever the replicas disagreed
    about must itself be a leased step, so they converge on one value before
    reaching the step that acts on it.
    """


class NondeterministicWorkflowError(RuntimeError):
    """A resumed run cannot be replayed against its recorded history.

    Replay is positional: the step at sequence N is answered from whatever was
    committed at sequence N. That is only sound if the resumed run performs the
    same steps in the same order, which is why the workflow body must be
    deterministic given the session's inputs: all nondeterminism belongs inside
    step bodies, never in the code deciding what to call.

    The base of `ReplayDivergedError` and `FailureNotReplayableError`; catch this
    to handle both. Raised as itself only for a record that cannot be verified.
    """


class ReplayDivergedError(NondeterministicWorkflowError):
    """The resumed run called a different step than its history records here.

    The run went a different way than it did the first time: the code changed
    since then, or it branched on something that was not recorded. Continuing
    would hand back another step's result, so the run stops before this step
    does anything. Nothing ran twice; to go on, start a new session.
    """


class FailureNotReplayableError(NondeterministicWorkflowError):
    """A recorded failure or cancellation cannot be reproduced as it happened.

    For example, the exception's class cannot be found in the resumed process,
    or the record was written by another SDK. Raising something else instead
    could send the run down a different branch, so it stops.
    """


#: How long a replayed cancellation waits, as a multiple of the time the step ran
#: before it was cancelled the first time, plus a fixed allowance. Long enough
#: that whatever cancelled it then (a `wait_for` timer) does so again first.
_CANCEL_WAIT_FACTOR = 2.0
_CANCEL_WAIT_EXTRA_S = 0.25
#: The most a replay waits, for a timeout around several steps, beyond what the
#: step's own run time calls for. Earlier steps replay instantly, so that timeout
#: fires later than it did; a wait without a bound would hold a resumed run for
#: as long as the original run had been going.
_CANCEL_WAIT_RUN_CAP_S = 120.0


def _cancellation(recorded: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The recorded cancellation, if this record is one."""
    if recorded.get("status") != STEP_STATUS_FAILED:
        return None
    payload = recorded.get("payload") or {}
    cancelled = payload.get("cancelled")
    return cancelled if isinstance(cancelled, dict) else None


def _check_recorded_name(ctx: WorkflowContext, seq: int, expected_name: str) -> None:
    """Raises unless the step recorded at `seq` is `expected_name`."""
    recorded_name = ctx.replayed_steps[seq].get("name")

    # Fail closed on an unverifiable record. Every step this SDK commits carries a
    # name -- `@step` falls back to `func.__name__`, the LangGraph saver builds
    # one, and engine-generated timer events carry their own -- so an empty name
    # means the record cannot be checked, not that checking is unnecessary.
    # Replaying it would be positional replay, which is what this guards against.
    if not recorded_name:
        raise NondeterministicWorkflowError(
            f"Cannot verify replay at sequence {seq}: the recorded step has no "
            f"name, so there is no way to confirm it is {expected_name!r} rather "
            f"than a different step at the same position. Every step this SDK "
            f"commits records its name, so this history did not come from it."
        )
    if recorded_name != expected_name:
        raise ReplayDivergedError(
            _divergence_message(ctx, seq, expected_name, recorded_name)
        )


def _divergence_message(
    ctx: WorkflowContext, seq: int, expected_name: str, recorded_name: str
) -> str:
    lines = [
        f"Session {ctx.session_id} cannot resume at position {seq}. The resumed "
        f"run called {expected_name!r}, but the history records "
        f"{recorded_name!r} there."
    ]
    # The last failure or cancellation before here is usually where the paths
    # parted: the code handled it differently this time.
    earlier = sorted(p for p in ctx.replayed_steps if p < seq)
    for position in reversed(earlier):
        record = ctx.replayed_steps[position]
        if record.get("status") != STEP_STATUS_FAILED:
            continue
        name = record.get("name")
        if _cancellation(record) is not None:
            what = "was cancelled"
        else:
            error = (record.get("payload") or {}).get("error") or {}
            kind = str(error.get("type", "")).rpartition(":")[2] or "an exception"
            what = f"failed with {kind}"
        lines.append(
            f"Position {position} ({name!r}) {what} on the first run; after it, "
            f"the code took a different branch than it did then."
        )
        break
    lines.append(
        "Common causes: the workflow code changed since the first run (a step "
        "added, removed or reordered, or a different timeout), or it branched on "
        "something not recorded (the clock, a random number, a model's answer "
        "outside a step). Nothing ran twice. To go on, start a new session for "
        "this work."
    )
    return "\n".join(lines)


async def _replay_cancellation(ctx: WorkflowContext, cancelled: Dict[str, Any]) -> None:
    """Reproduces a cancellation: waits to be cancelled, as the step did then.

    What the workflow saw depended on what cancelled the step, not on the step:
    `asyncio.wait_for` turns its own cancellation into `TimeoutError`, while a
    `task.cancel()` arrives as `CancelledError`. So the replay does not raise
    anything at once. It waits, and whatever cancelled the step the first time
    cancels it again, producing the same exception. If nothing does within twice
    the time the step ran then (and no sooner than the run is as far in as it was
    when the cancellation came, waiting at most two minutes for that), the
    cancellation came from outside the code (a person pressing Cancel), and the
    replay delivers it itself.
    """
    elapsed = float(cancelled.get("elapsed_s") or 0.0)
    wait = elapsed * _CANCEL_WAIT_FACTOR + _CANCEL_WAIT_EXTRA_S
    since_start = cancelled.get("since_run_start_s")
    if since_start is not None:
        # A timeout set around several steps started before this one, and earlier
        # steps replay instantly, so it fires later relative to this step than it
        # did the first time. It started no earlier than the run did, so by the
        # time this replayed run is as far in as the first one was when the
        # cancellation came, it has fired.
        # Only this wait is capped; the step's own is not, so a single step
        # under a long `wait_for` still replays its timeout.
        deadline = ctx.run_started + float(since_start) * 1.25 + _CANCEL_WAIT_EXTRA_S
        wait = max(wait, min(deadline - time.monotonic(), _CANCEL_WAIT_RUN_CAP_S))
    await asyncio.sleep(wait)
    raise asyncio.CancelledError()


def _replay(ctx: WorkflowContext, seq: int, expected_name: str) -> Any:
    """Returns the recorded result at `seq`, or raises if it is a different step.

    Replay is positional, so the step recorded at a sequence is only the right
    answer if the resumed run reached that sequence by the same path. Matching on
    the step name establishes that before the recorded result is returned.

    A record whose name is empty is treated as unverifiable and rejected rather
    than replayed.
    """
    _check_recorded_name(ctx, seq, expected_name)
    recorded = ctx.replayed_steps[seq]
    payload = recorded.get("payload")
    if _cancellation(recorded) is not None:
        # Only an async step is ever recorded as cancelled, and an async step
        # replays it through `_replay_cancellation`. Reaching it here means the
        # step is now synchronous, which cannot be cancelled the same way.
        raise FailureNotReplayableError(
            f"Cannot replay position {seq} ({expected_name!r}) of session "
            f"{ctx.session_id}: it was cancelled on the first run, as an async "
            f"step, and is now a synchronous one. Start a new session for this "
            f"work."
        )
    if recorded.get("status") == STEP_STATUS_FAILED:
        # The step failed the first time, and the workflow carried on past it.
        # Raise the same exception, so the resumed run takes the same branch.
        try:
            failure = rebuild((payload or {}).get("error", {}), expected_name, seq)
        except ReplayedFailureError as exc:
            raise FailureNotReplayableError(str(exc)) from None
        raise failure
    if payload is None:
        return None
    return payload.get("result")


async def _replay_async(ctx: WorkflowContext, seq: int, expected_name: str) -> Any:
    """`_replay` for an async step, which can also replay a cancellation."""
    _check_recorded_name(ctx, seq, expected_name)
    cancelled = _cancellation(ctx.replayed_steps[seq])
    if cancelled is not None:
        await _replay_cancellation(ctx, cancelled)
    return _replay(ctx, seq, expected_name)


def _release_after_failure(
    ctx: WorkflowContext, agent_id: str, ikey: str, fencing_token: int, name: str
) -> None:
    """Releases the lease of a step that failed or was cancelled, so the step
    that takes this position next is not refused until it lapses."""
    try:
        ctx.client.release_lease(
            agent_id=agent_id,
            idempotency_key=ikey,
            fencing_token=fencing_token,
            reason="TOOL_ERROR",
        )
    except Exception:
        # The lease lapses on its own. Losing the release is not worth
        # replacing the error that caused it.
        logger.warning(
            "Cellaflow: could not release the lease on %r after it failed; it "
            "lapses on its own.",
            name,
            exc_info=True,
        )


def _note_failure(
    ctx: WorkflowContext,
    seq: int,
    name: str,
    exc: BaseException,
    call_coordination: Optional[str],
    agent_id: str,
    ikey: Optional[str],
    elapsed_s: float,
) -> None:
    """Holds a live step's failure or cancellation to be recorded at its position.

    Not for a refused lease: another writer owns that position and fills it.
    A cancellation (`asyncio.CancelledError`) is recorded with how long the step
    ran, so a replay can wait to be cancelled the same way; see
    `_replay_cancellation`. A process exit (`KeyboardInterrupt`, `SystemExit`)
    is not something a workflow carries on past, so the step gives its position
    back instead.
    """
    try:
        if isinstance(exc, DivergentStepError):
            return
        if isinstance(exc, asyncio.CancelledError):
            ctx.defer_cancellation_record(
                seq,
                name,
                elapsed_s,
                call_coordination,
                agent_id=agent_id,
                step_key=ikey or "",
                since_run_start_s=time.monotonic() - ctx.run_started,
            )
            return
        if not isinstance(exc, Exception):
            ctx.give_back_position(seq)
            return
        ctx.defer_failure_record(
            seq, name, exc, call_coordination, agent_id=agent_id, step_key=ikey or ""
        )
    except Exception:
        # Noting the failure must never replace the step's own exception, which
        # is on its way to the workflow.
        logger.warning(
            "Cellaflow: could not hold the failure of %r at position %d for "
            "recording.",
            name,
            seq,
            exc_info=True,
        )


def _take_hit(
    ctx: WorkflowContext,
    resp: Any,
    seq: int,
    name: str,
    call_coordination: Optional[str],
    agent_id: str,
    ikey: str,
) -> Any:
    """Returns a cache hit's result and keeps this session's history in step.

    A hit returns without executing. When nothing is recorded at `seq` yet, the
    result is recorded there before the next step runs, so a resumed session
    replays it like any other step. `WorkflowContext.take_hit` decides which.
    """
    payload: Dict[str, Any] = {"result": None}
    if resp.cached_result and resp.cached_result.output_payload:
        payload = deserialize(resp.cached_result.output_payload)

    if resp.HasField("current_sequence"):
        ctx.take_hit(
            seq,
            name,
            payload,
            resp.current_sequence,
            call_coordination,
            agent_id=agent_id,
            received_key=ikey,
        )
    return payload.get("result")


def _pop_call_coordination(kwargs: Dict[str, Any]) -> Optional[str]:
    """Takes a step's own coordination domain out of its call arguments.

    Removed before the step's arguments are hashed or passed to it, and
    validated before the sequence advances or a lease is taken, so an invalid
    value fails without leaving anything behind.
    """
    return normalize_coordination_id(kwargs.pop("_coordination_id", None))


def _validate_shared_scope(
    func: Callable[..., Any],
    scope: IdempotencyScope,
    shared_on: Optional[Sequence[str]],
    idempotency_key: Optional[str],
) -> None:
    """Refuses a SCOPE_SHARED tool that has not said what identifies its work.

    Every other scope can derive a safe key on its own. This one cannot, and the
    failure is silent: heterogeneous agents converge on a shared side effect
    exactly when they disagree about their arguments, so hashing all of them
    splits the key and every agent performs the side effect. Measured at five
    agents, five charges.

    An agent cannot detect the divergence locally either -- it sees only its own
    arguments -- so this has to be the caller's decision, made once, at import.
    """
    if shared_on is not None:
        params = inspect.signature(func).parameters
        unknown = [n for n in shared_on if n not in params]
        if unknown:
            raise ValueError(
                f"shared_on names {unknown} which are not parameters of "
                f"{func.__name__}(). Known parameters: {list(params)}."
            )
        return

    if scope == IdempotencyScope.SCOPE_SHARED and not idempotency_key:
        raise ValueError(
            f"{func.__name__}() uses SCOPE_SHARED without saying which of its "
            f"arguments identify the shared work.\n\n"
            f"Agents sharing a side effect will not agree on every argument -- "
            f"that is what makes them different agents -- and hashing all of "
            f"them gives each one its own key, so each performs the side "
            f"effect. Name the identifying arguments instead:\n\n"
            f"    @tool(tool_name=..., scope=IdempotencyScope.SCOPE_SHARED,\n"
            f'          shared_on=["ticket_id"])\n\n'
            f"Agents may then disagree about everything else and still converge "
            f"on one execution. Note the trade-off this accepts: the agents that "
            f"lose receive a result computed from arguments they did not supply."
        )


def step(
    func: Optional[F] = None,
    *,
    idempotency_key: Optional[str] = None,
    agent_id: str = "default",
    tool_name: Optional[str] = None,
    scope: IdempotencyScope = IdempotencyScope.SCOPE_SESSION_WIDE,
    shared_on: Optional[Sequence[str]] = None,
) -> Any:
    if func is None:
        return functools.partial(
            step,
            idempotency_key=idempotency_key,
            agent_id=agent_id,
            tool_name=tool_name,
            scope=scope,
            shared_on=shared_on,
        )

    actual_tool_name = tool_name or func.__name__
    _validate_shared_scope(func, scope, shared_on, idempotency_key)

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # A step may name its own coordination domain, overriding the
        # session's for this call only. It is recorded with the step and, under
        # SCOPE_SHARED, is what the key is derived from, so the call is
        # deduplicated under the same domain it is recorded under.
        call_coordination = _pop_call_coordination(kwargs)
        coordination_id = call_coordination or ctx.coordination_id
        # A cache hit may have left this counter ahead of the engine's, or left
        # a result to record at its position. Both are settled before claiming
        # the next sequence, or this commit fails the ordering check and names
        # the wrong step. Deferred to here rather than done on the hit itself so
        # a workflow that ends on a hit does no extra work.
        seq = await ctx.begin_call_async()
        # Set once the step runs live rather than replaying, so only a live
        # failure is recorded; `ikey` once it is known.
        live = False
        ikey: Optional[str] = None
        started = time.monotonic()
        try:
            if seq in ctx.replayed_steps:
                return await _replay_async(ctx, seq, actual_tool_name)
            live = True

            # Derive idempotency key
            ikey = idempotency_key
            if not ikey:
                if shared_on is not None:
                    # Bind first: the caller may have passed the identifying
                    # arguments positionally, and only names can select them.
                    bound = inspect.signature(func).bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    ikey = derive_idempotency_key(
                        ctx.session_id,
                        ctx.workflow_version,
                        seq,
                        agent_id,
                        actual_tool_name,
                        scope,
                        coordination_id,
                        _shared_on=shared_on,
                        **bound.arguments,
                    )
                else:
                    ikey = derive_idempotency_key(
                        ctx.session_id,
                        ctx.workflow_version,
                        seq,
                        agent_id,
                        actual_tool_name,
                        scope,
                        coordination_id,
                        *args,
                        **kwargs,
                    )

            committed = False
            fencing_token = 0
            hb: Optional[LeaseHeartbeat] = None

            while True:
                try:
                    resp = ctx.client.check_idempotency_cache(
                        agent_id=agent_id,
                        idempotency_key=ikey,
                        session_id=ctx.session_id,
                        # Tell the engine where this call intends to write,
                        # so a lease at an already-committed position is refused
                        # before the tool body runs rather than after.
                        sequence=seq,
                    )
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        raise DivergentStepError(
                            f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                            f"{exc.details()}"
                        ) from exc
                    raise
                if resp.status == CACHE_STATUS_HIT:
                    return _take_hit(
                        ctx,
                        resp,
                        seq,
                        actual_tool_name,
                        call_coordination,
                        agent_id,
                        ikey,
                    )
                elif resp.status == CACHE_STATUS_IN_PROGRESS:
                    retry_ms = resp.retry_after_ms or 1000
                    logger.info(
                        "Step %s is in progress by another worker. Sleeping %d ms...",
                        actual_tool_name,
                        retry_ms,
                    )
                    await asyncio.sleep(retry_ms / 1000.0)
                elif resp.status == CACHE_STATUS_ACQUIRED:
                    fencing_token = resp.fencing_token or 0
                    interval_ms = resp.heartbeat_interval_ms or 5000
                    hb = LeaseHeartbeat(
                        client=ctx.client,
                        agent_id=agent_id,
                        idempotency_key=ikey,
                        fencing_token=fencing_token,
                        heartbeat_interval_ms=interval_ms,
                    )
                    hb.start_async()
                    break
                else:
                    break

            try:
                result = await func(*args, **kwargs)
                ctx.client.commit_step(
                    session_id=ctx.session_id,
                    sequence=seq,
                    name=actual_tool_name,
                    status=STEP_STATUS_SUCCESS,
                    output_payload={"result": result},
                    idempotency_key=ikey,
                    idempotency_fencing_token=fencing_token,
                    # Omitted when the call names none, so the engine records the
                    # session's default.
                    coordination_id=call_coordination,
                )
                committed = True
                return result
            except BaseException as e:
                # Cancellation too: a lease left held would refuse the step that
                # takes this position next until it lapsed.
                if fencing_token > 0:
                    _release_after_failure(
                        ctx, agent_id, ikey, fencing_token, actual_tool_name
                    )
                raise e
            finally:
                if hb:
                    if committed:
                        # Nothing is awaited between the commit and the return,
                        # so a cancellation cannot land on a committed step.
                        hb.stop_soon()
                    else:
                        await hb.stop_async()
        except BaseException as exc:
            if live:
                _note_failure(
                    ctx,
                    seq,
                    actual_tool_name,
                    exc,
                    call_coordination,
                    agent_id,
                    ikey,
                    time.monotonic() - started,
                )
            raise
        finally:
            ctx.end_call()

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # A step may name its own coordination domain, overriding the
        # session's for this call only. It is recorded with the step and, under
        # SCOPE_SHARED, is what the key is derived from, so the call is
        # deduplicated under the same domain it is recorded under.
        call_coordination = _pop_call_coordination(kwargs)
        coordination_id = call_coordination or ctx.coordination_id
        # A cache hit may have left this counter ahead of the engine's, or left
        # a result to record at its position. Both are settled before claiming
        # the next sequence, or this commit fails the ordering check and names
        # the wrong step. Deferred to here rather than done on the hit itself so
        # a workflow that ends on a hit does no extra work.
        seq = ctx.begin_call()
        # Set once the step runs live rather than replaying, so only a live
        # failure is recorded; `ikey` once it is known.
        live = False
        ikey: Optional[str] = None
        started = time.monotonic()
        try:
            if seq in ctx.replayed_steps:
                return _replay(ctx, seq, actual_tool_name)
            live = True

            # Derive idempotency key
            ikey = idempotency_key
            if not ikey:
                if shared_on is not None:
                    # Bind first: the caller may have passed the identifying
                    # arguments positionally, and only names can select them.
                    bound = inspect.signature(func).bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    ikey = derive_idempotency_key(
                        ctx.session_id,
                        ctx.workflow_version,
                        seq,
                        agent_id,
                        actual_tool_name,
                        scope,
                        coordination_id,
                        _shared_on=shared_on,
                        **bound.arguments,
                    )
                else:
                    ikey = derive_idempotency_key(
                        ctx.session_id,
                        ctx.workflow_version,
                        seq,
                        agent_id,
                        actual_tool_name,
                        scope,
                        coordination_id,
                        *args,
                        **kwargs,
                    )

            fencing_token = 0
            hb: Optional[LeaseHeartbeat] = None

            while True:
                try:
                    resp = ctx.client.check_idempotency_cache(
                        agent_id=agent_id,
                        idempotency_key=ikey,
                        session_id=ctx.session_id,
                        # Tell the engine where this call intends to write,
                        # so a lease at an already-committed position is refused
                        # before the tool body runs rather than after.
                        sequence=seq,
                    )
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        raise DivergentStepError(
                            f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                            f"{exc.details()}"
                        ) from exc
                    raise
                if resp.status == CACHE_STATUS_HIT:
                    return _take_hit(
                        ctx,
                        resp,
                        seq,
                        actual_tool_name,
                        call_coordination,
                        agent_id,
                        ikey,
                    )
                elif resp.status == CACHE_STATUS_IN_PROGRESS:
                    retry_ms = resp.retry_after_ms or 1000
                    logger.info(
                        "Step %s is in progress by another worker. Sleeping %d ms...",
                        actual_tool_name,
                        retry_ms,
                    )
                    time.sleep(retry_ms / 1000.0)
                elif resp.status == CACHE_STATUS_ACQUIRED:
                    fencing_token = resp.fencing_token or 0
                    interval_ms = resp.heartbeat_interval_ms or 5000
                    hb = LeaseHeartbeat(
                        client=ctx.client,
                        agent_id=agent_id,
                        idempotency_key=ikey,
                        fencing_token=fencing_token,
                        heartbeat_interval_ms=interval_ms,
                    )
                    hb.start_sync()
                    break
                else:
                    break

            try:
                result = func(*args, **kwargs)
                ctx.client.commit_step(
                    session_id=ctx.session_id,
                    sequence=seq,
                    name=actual_tool_name,
                    status=STEP_STATUS_SUCCESS,
                    output_payload={"result": result},
                    idempotency_key=ikey,
                    idempotency_fencing_token=fencing_token,
                    # Omitted when the call names none, so the engine records the
                    # session's default.
                    coordination_id=call_coordination,
                )
                return result
            except BaseException as e:
                # Cancellation too: a lease left held would refuse the step that
                # takes this position next until it lapsed.
                if fencing_token > 0:
                    _release_after_failure(
                        ctx, agent_id, ikey, fencing_token, actual_tool_name
                    )
                raise e
            finally:
                if hb:
                    hb.stop_sync()
        except BaseException as exc:
            if live:
                _note_failure(
                    ctx,
                    seq,
                    actual_tool_name,
                    exc,
                    call_coordination,
                    agent_id,
                    ikey,
                    time.monotonic() - started,
                )
            raise
        finally:
            ctx.end_call()

    if inspect.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    return cast(F, sync_wrapper)


def tool(
    func: Optional[F] = None,
    *,
    idempotency_key: Optional[str] = None,
    agent_id: str = "default",
    tool_name: Optional[str] = None,
    scope: IdempotencyScope = IdempotencyScope.SCOPE_SESSION_WIDE,
    shared_on: Optional[Sequence[str]] = None,
) -> Any:
    """
    Decorator for tool steps that require idempotency tracking.
    Functionally identical to @step for now, but semantically distinct.
    """
    return step(
        func=func,
        idempotency_key=idempotency_key,
        agent_id=agent_id,
        tool_name=tool_name,
        scope=scope,
        shared_on=shared_on,
    )
