import functools
import inspect
import uuid
import asyncio
import time
import threading
import logging
from typing import (
    Any,
    Callable,
    Dict,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)

import grpc

from cellaflow.client import CellaflowClient, _check_api_key_argument
from cellaflow.context import (
    _RETRYABLE_CODES,
    WorkflowContext,
    set_context,
    reset_context,
    get_context,
    session_coordination_id,
)
from cellaflow.failures import ReplayedFailureError, rebuild
from cellaflow.history import GRAPH_PAGE, iter_session_steps
from cellaflow.idempotency import (
    derive_idempotency_key,
    normalize_coordination_id,
    IdempotencyScope,
)
from cellaflow.execution import (
    LeaseHandle,
    LeaseLostError,
    LeaseNotAcquired,
    _current_lease,
)
from cellaflow.lease import LeaseHeartbeat, check_lease_ttl_ms
from cellaflow.serialization import deserialize, serialize
from cellaflow.v1.common_pb2 import STEP_STATUS_FAILED, STEP_STATUS_SUCCESS
from cellaflow.v1.idempotency_pb2 import (
    CACHE_STATUS_HIT,
    CACHE_STATUS_ACQUIRED,
    CACHE_STATUS_IN_PROGRESS,
)

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def workflow(
    version: str = "1.0.0",
    target: str = "localhost:50051",
    secure: bool = False,
    api_key: Optional[str] = None,
) -> Callable[[F], F]:
    """Makes a function a durable workflow: its `@step` calls are recorded in a
    session, and a run with the same `_session_id` resumes from them.

    `api_key` authenticates with the engine; when omitted it is read from
    `CELLAFLOW_API_KEY`. An `api_key` that cannot be sent as a header is refused
    here, when the decorator is applied; whether a key may go without TLS is
    decided when each call builds its client. See `CellaflowClient`.
    """
    _check_api_key_argument(api_key)

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

            client = CellaflowClient(target=target, secure=secure, api_key=api_key)
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
                    client, resp.session_id, limit=GRAPH_PAGE, missing_ok=False
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
                try:
                    result = await func(*args, **kwargs)
                except BaseException as exc:
                    ctx.note_run_ended(exc)
                    # A refusal the workflow's own code caught still ends it.
                    ctx.raise_if_refused(cause=exc)
                    raise
                # A failure the workflow caught, with no step after it to record
                # it. Only on a normal return: a failure the workflow lets
                # propagate stays unrecorded, so resuming retries the step.
                ctx.raise_if_refused()
                if ctx.has_pending_failure():
                    # Written before returning, off the event loop.
                    await asyncio.to_thread(ctx.record_pending_failure)
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

            client = CellaflowClient(target=target, secure=secure, api_key=api_key)
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
                    client, resp.session_id, limit=GRAPH_PAGE, missing_ok=False
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
                try:
                    result = func(*args, **kwargs)
                except BaseException as exc:
                    ctx.note_run_ended(exc)
                    # A refusal the workflow's own code caught still ends it.
                    ctx.raise_if_refused(cause=exc)
                    raise
                # See the async wrapper.
                ctx.raise_if_refused()
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
    # The last failure before here is usually where the paths parted: the code
    # handled it differently this time. (A cancellation before here cannot be:
    # a resume refuses at it, so never gets this far.)
    earlier = sorted(p for p in ctx.replayed_steps if p < seq)
    for position in reversed(earlier):
        record = ctx.replayed_steps[position]
        if record.get("status") != STEP_STATUS_FAILED:
            continue
        name = record.get("name")
        error = (record.get("payload") or {}).get("error") or {}
        kind = str(error.get("type", "")).rpartition(":")[2] or "an exception"
        lines.append(
            f"Position {position} ({name!r}) failed with {kind} on the first run; "
            f"after it, the code took a different branch than it did then."
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


def _refuse(ctx: WorkflowContext, refusal: NondeterministicWorkflowError) -> NoReturn:
    """Raises a replay refusal, after recording it so the run stops there."""
    ctx.refuse(refusal)
    raise refusal from None


def _replay(ctx: WorkflowContext, seq: int, expected_name: str) -> Any:
    """Returns the recorded result at `seq`, or raises if it is a different step.

    Replay is positional, so the step recorded at a sequence is only the right
    answer if the resumed run reached that sequence by the same path. Matching on
    the step name establishes that before the recorded result is returned.

    A record whose name is empty is treated as unverifiable and rejected rather
    than replayed.

    A refusal also stops the rest of the run: the workflow called this step
    inside its own `try`, since that is why a failure was recorded here, and a
    broad `except` there would otherwise catch the refusal and carry on down a
    guessed branch. See `WorkflowContext.refuse`.
    """
    try:
        _check_recorded_name(ctx, seq, expected_name)
    except NondeterministicWorkflowError as refusal:
        ctx.refuse(refusal)
        raise
    return _replay_recorded(ctx, seq, expected_name)


def _replay_recorded(ctx: WorkflowContext, seq: int, expected_name: str) -> Any:
    """`_replay` once the recorded name has been checked.

    Only the replay's own refusals go through `ctx.refuse`. A recorded failure
    raised again is the step's own exception, even one that happens to be a
    `NondeterministicWorkflowError` (from a workflow the step drives, say):
    the workflow caught it the first time, and may again.
    """
    recorded = ctx.replayed_steps[seq]
    payload = recorded.get("payload")
    if _cancellation(recorded) is not None:
        # Inside the step a cancellation always looks the same, but what the
        # workflow saw depended on what cancelled it: a `wait_for` around the
        # step turns its own into `TimeoutError`, while one from outside arrives
        # as `CancelledError`. The record cannot say which, and waiting for the
        # same thing to happen again cannot either: steps before this one replay
        # instantly, so timers around them fire at different moments, and one
        # that did not fire the first time can fire now. So the replay refuses
        # rather than guess.
        _refuse(ctx, FailureNotReplayableError(
            f"Cannot replay position {seq} ({expected_name!r}) of session "
            f"{ctx.session_id}: it was cancelled on the first run, and a replay "
            f"cannot tell what cancelled it (a timeout around the step, or "
            f"something outside the run), so it cannot raise what the workflow "
            f"saw then. Put a timeout inside the step's body instead "
            f"(`return await asyncio.wait_for(call(), 5)`), which records the "
            f"TimeoutError itself and replays it. Nothing has run twice; start a "
            f"new session for this work."
        ))
    if recorded.get("status") == STEP_STATUS_FAILED:
        # The step failed the first time, and the workflow carried on past it.
        # Raise the same exception, so the resumed run takes the same branch.
        try:
            failure = rebuild((payload or {}).get("error", {}), expected_name, seq)
        except ReplayedFailureError as exc:
            _refuse(ctx, FailureNotReplayableError(str(exc)))
        raise failure
    if payload is None:
        return None
    return payload.get("result")


def _release_after_failure(
    ctx: WorkflowContext,
    agent_id: str,
    ikey: str,
    fencing_token: int,
    name: str,
    hb: LeaseHeartbeat,
) -> Tuple[Optional[Dict[str, Any]], Optional[bool]]:
    """Releases the lease of a step that failed or was cancelled, so the step
    that takes this position next is not refused until it lapses. Returns the
    unreleased lease, if any (below), and the engine's answer: False when the
    lease was no longer this call's to release, None when there was none.

    If the release fails, returns the lease, for the failure record to retry the
    release or wait for the lapse: the lease still claims the step's position,
    so the engine would refuse the record there. When it lapses is counted from
    the moment the record is held, after the heartbeat has stopped, because a
    renewal still in flight now can extend it.
    """
    try:
        resp = ctx.client.release_lease(
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
        # A renewal already on its way when the heartbeat is told to stop can
        # still reach the engine, up to its RPC timeout from now, and extend
        # the lease a full lifetime from there.
        return {
            "agent_id": agent_id,
            "key": ikey,
            "token": fencing_token,
            "lapses_in_s": hb.rpc_timeout_sec + hb.lease_ttl_ms / 1000.0 + 1.0,
        }, None
    return None, bool(getattr(resp, "released", True))


def _note_failure(
    ctx: WorkflowContext,
    seq: int,
    name: str,
    exc: BaseException,
    call_coordination: Optional[str],
    agent_id: str,
    ikey: Optional[str],
    started: float,
    failed_at: float,
    unreleased: Optional[Dict[str, Any]] = None,
    is_async: bool = True,
    lease_ttl_ms: Optional[int] = None,
) -> None:
    """Holds a live step's failure or cancellation to be recorded at its position.

    `started` and `failed_at` are when the step started and when it failed, on
    the monotonic clock: a cancellation records the time between, without the
    release and heartbeat stop after it.

    Not for a refused lease: another writer owns that position and fills it.
    An async step's cancellation (`asyncio.CancelledError`) is recorded as a
    cancellation, which fills the position, and which a resume refuses at; see
    `_replay_recorded`. Raised out of a synchronous step, it is the step's own
    exception (an `asyncio.run` inside it, say), recorded and replayed like any
    other. A process exit (`KeyboardInterrupt`, `SystemExit`) is not something a
    workflow carries on past, so the step gives its position back instead.
    """
    try:
        if isinstance(exc, DivergentStepError):
            return
        if isinstance(exc, asyncio.CancelledError) and is_async:
            ctx.defer_cancellation_record(
                seq,
                name,
                failed_at - started,
                call_coordination,
                agent_id=agent_id,
                step_key=ikey or "",
                source=exc,
                unreleased=unreleased,
                lease_ttl_ms=lease_ttl_ms,
            )
            return
        if not isinstance(exc, (Exception, asyncio.CancelledError)):
            ctx.give_back_position(seq)
            return
        ctx.defer_failure_record(
            seq,
            name,
            exc,
            call_coordination,
            agent_id=agent_id,
            step_key=ikey or "",
            unreleased=unreleased,
            lease_ttl_ms=lease_ttl_ms,
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


class _CallLease:
    """What one call knows about its lease while it runs.

    The lease's clock only tells the body to stop. Whether the call's result is
    recorded is the engine's decision: it accepts a commit or a release only
    while the lease is still this call's. `lost` is set from its answers, never
    from the clock alone. The held-back and dropped renewal refusals only
    decide whether the handle reads as lost or merely released afterwards, for
    work the body left running; no outcome depends on them.
    """

    def __init__(self) -> None:
        self.handle: Optional[LeaseHandle] = None
        # While the commit is on its way, a renewal can come back refused
        # because that very commit completed the key: not a loss. It is held
        # back, and applied only if the commit fails.
        self.committing = False
        self.held_loss = False
        # Once the release is on its way, a renewal refused because of it is
        # not a loss either, and is dropped.
        self.releasing = False
        # Set when the engine showed the lease is no longer this call's.
        self.lost = False
        self.returned = False
        self.result: Any = None
        # The payload committed, taken once from the result.
        self.payload: Dict[str, Any] = {}

    def on_lost(self) -> None:
        if self.releasing:
            return
        if self.committing:
            self.held_loss = True
            return
        if self.handle is not None:
            self.handle.is_lost = True

    def mark_taken_over(self) -> None:
        """The engine showed another worker holds the call: the handle reads as
        lost, not merely released, for work the body left running."""
        self.lost = True
        if self.handle is not None:
            self.handle.is_lost = True

    def apply_held_loss(self) -> None:
        """A loss held back during a commit that failed counts once the call
        fails. Not before: a resend can still show the commit landed, which is
        what refused the renewal."""
        if self.held_loss and self.handle is not None:
            self.handle.is_lost = True


#: How long one arbitration request asks the engine to wait while another
#: worker holds the call. The engine answers the moment that worker commits, or
#: grants the lease the moment it is released or lapses, and every caller
#: waiting on the key shares one watcher there instead of each polling. The
#: call's total wait is the loop around it, so this only bounds how long one
#: request stays open: a dead connection is noticed within it.
_ENGINE_WAIT_MS = 10_000

#: The deadline on a request that asks to wait, in seconds: the wait, plus time
#: for the engine to answer. A connection that dies while the engine holds the
#: request fails the call by then, rather than leaving it waiting for an answer
#: that never comes.
_ENGINE_WAIT_DEADLINE_S = (_ENGINE_WAIT_MS + 5_000) / 1000.0


def _retry_after_s(resp: Any) -> float:
    """The back-off an engine that did not wait suggests, bounded: a zero would
    make the loop busy."""
    return float(min(60_000, max(50, resp.retry_after_ms or 1000))) / 1000.0


def _answered_at_once(sent: Tuple[float, float]) -> bool:
    """Whether an IN_PROGRESS answer came back well before the wait it asked
    for. An engine that waited answers at the end of its wait, so the call can
    ask again at once; one that answered sooner did not wait (an engine that
    ignores the field), and is asked again only after its `retry_after_ms`, so
    an old engine cannot turn the loop into a busy one."""
    return time.monotonic() - sent[0] < _ENGINE_WAIT_MS / 2000.0


def _granted_late(sent: Tuple[float, float], interval_ms: int) -> bool:
    """Whether a grant came back more than half an interval after its request,
    so after a wait in the engine. Such a lease can be much younger than the
    request and is counted from a renewal instead (`confirm_grant`). Up to
    that, counting from the request costs the lease less than its margin."""
    return time.monotonic() - sent[0] > interval_ms / 2000.0


#: How many granted leases a call hands back, unconfirmed, before it fails
#: rather than asking again. A healthy engine needs none: a lease released at
#: once is granted again at once, so the next grant is not late.
_MAX_HAND_BACKS = 3

#: The deadline on a hand-back made without the lease's heartbeat at hand.
_HAND_BACK_TIMEOUT_S = 5.0

#: How many times a call asks again after a request that got no answer, before
#: it fails. The engine may have granted the lost answer's lease: asking again
#: with the same key and position waits that lease out and is granted the next,
#: so the body still runs only under a lease this side was told about.
_MAX_UNANSWERED = 2

#: The engine's default lease TTL, for a call that asked for none.
_ENGINE_DEFAULT_TTL_MS = 20_000


def _lost_answer(agent_id: str, ikey: str, ttl_ms: Optional[int]) -> Dict[str, Any]:
    """The unreleased lease of a request whose answer never came: the engine
    may have granted it, a lease nobody here knows the token of, which claims
    the call's position until it lapses. Kept for the failure record there to
    wait out; without its token it cannot be released."""
    return {
        "agent_id": agent_id,
        "key": ikey,
        "token": 0,
        "lapses_in_s": (ttl_ms or _ENGINE_DEFAULT_TTL_MS) / 1000.0 + 1.0,
    }


def _hand_back(
    ctx: WorkflowContext,
    agent_id: str,
    ikey: str,
    fencing_token: int,
    reason: str,
    hb: Optional[LeaseHeartbeat] = None,
) -> Optional[Dict[str, Any]]:
    """Releases a lease whose body never started. It lapses on its own if the
    release fails, and asking again waits that out. Until it does, it still
    claims the call's position, so it is returned, as `_release_after_failure`
    returns one, for a failure record there to retry the release or wait for
    the lapse."""
    try:
        ctx.client.release_lease(
            agent_id=agent_id,
            idempotency_key=ikey,
            fencing_token=fencing_token,
            reason=reason,
            # Bounded: a hand-back often follows renewals that failed, when
            # the connection is most likely bad.
            timeout=hb.rpc_timeout_sec if hb is not None else _HAND_BACK_TIMEOUT_S,
        )
    except Exception:
        logger.info("Could not hand back the lease on %s; it lapses on its own", ikey)
        if hb is None:
            return None
        return _held(agent_id, ikey, fencing_token, hb)
    return None


def _held(
    agent_id: str, ikey: str, fencing_token: int, hb: LeaseHeartbeat
) -> Dict[str, Any]:
    """A lease of this call's that may not have been released, as the failure
    record there needs it: to retry the release, or wait for the lapse."""
    return {
        "agent_id": agent_id,
        "key": ikey,
        "token": fencing_token,
        "lapses_in_s": hb.rpc_timeout_sec + hb.lease_ttl_ms / 1000.0 + 1.0,
    }


def _hand_back_later(
    ctx: WorkflowContext,
    agent_id: str,
    ikey: str,
    fencing_token: int,
    hb: LeaseHeartbeat,
) -> Dict[str, Any]:
    """Hands a lease back from another thread, for a call being cancelled,
    and returns it as unreleased, as a failed release would leave it."""
    threading.Thread(
        target=_hand_back,
        args=(ctx, agent_id, ikey, fencing_token, "CANCELLED", hb),
        daemon=True,
    ).start()
    return _held(agent_id, ikey, fencing_token, hb)


def _unconfirmed(name: str, handed_back: int, hb: LeaseHeartbeat) -> LeaseNotAcquired:
    """The error for a call none of whose granted leases could be confirmed:
    the engine grants leases but will not renew them, and waiting longer only
    repeats that."""
    detail = hb.lost_detail or "no renewal was confirmed"
    return LeaseNotAcquired(
        f"Step '{name}' could not confirm any of {handed_back} leases the engine "
        f"granted it, so its body did not run: {detail}."
    )


async def _arbitrate_async(
    ctx: WorkflowContext,
    agent_id: str,
    ikey: str,
    ttl_ms: Optional[int],
    seq: int,
) -> Any:
    """Asks for the lease without holding the event loop or a thread while the
    engine waits: the request is a gRPC future, and its answer is handed to the
    loop when it comes. A thread per waiting call would fill the loop's default
    executor, which the heartbeats of calls already holding a lease renew
    through, so a crowd of waiters could make a live holder lose its lease.

    A call cancelled while it waits cancels the request. If the engine granted
    the lease before that took effect, nothing will run the body under it, so
    it is handed back rather than left to lapse.

    A client that does not return a `grpc.Future` (a test double) is asked from
    a worker thread instead."""
    kwargs: Dict[str, Any] = dict(
        agent_id=agent_id,
        idempotency_key=ikey,
        wait_timeout_ms=_ENGINE_WAIT_MS,
        lease_ttl_ms=ttl_ms,
        session_id=ctx.session_id,
        # Tell the engine where this call intends to write, so a lease at an
        # already-committed position is refused before the tool body runs
        # rather than after.
        sequence=seq,
        timeout=_ENGINE_WAIT_DEADLINE_S,
    )
    send = getattr(ctx.client, "check_idempotency_cache_future", None)
    request = send(**kwargs) if send is not None else None
    if not isinstance(request, grpc.Future):
        return await asyncio.to_thread(ctx.client.check_idempotency_cache, **kwargs)

    loop = asyncio.get_running_loop()
    answer: "asyncio.Future[Any]" = loop.create_future()

    def hand_back_if_granted(done: Any) -> None:
        if done.cancelled() or done.exception() is not None:
            return
        resp = done.result()
        if resp.status == CACHE_STATUS_ACQUIRED:
            # Off the loop: a release is a blocking call.
            threading.Thread(
                target=_hand_back,
                args=(ctx, agent_id, ikey, resp.fencing_token or 0, "CANCELLED"),
                daemon=True,
            ).start()

    def deliver(done: Any) -> None:
        # On the loop.
        if answer.done():
            # The call was cancelled while it waited.
            hand_back_if_granted(done)
        elif done.cancelled():
            answer.cancel()
        elif done.exception() is not None:
            answer.set_exception(done.exception())
        else:
            answer.set_result(done.result())

    def on_done(done: Any) -> None:
        # On a gRPC thread, or on the loop if the call had already finished.
        try:
            loop.call_soon_threadsafe(deliver, done)
        except RuntimeError:
            # The loop has closed, so nothing will use a grant.
            hand_back_if_granted(done)

    request.add_done_callback(on_done)
    try:
        return await answer
    except asyncio.CancelledError:
        if not request.cancel() and answer.done() and not answer.cancelled():
            # The answer reached the loop just before the cancellation did.
            hand_back_if_granted(answer)
        raise


def _lease_for_body(
    ctx: WorkflowContext,
    call: _CallLease,
    agent_id: str,
    ikey: str,
    fencing_token: int,
    interval_ms: int,
    ttl_ms: Optional[int],
    sent: Tuple[float, float],
    expires_at_ms: int = 0,
) -> Tuple[LeaseHandle, LeaseHeartbeat]:
    """The heartbeat for a call's lease, and the handle its body sees through
    `current_lease()`. The handle judges loss on the heartbeat's clock whenever
    it is asked, so a holder resumed past its deadline finds the lease gone
    before any renewal answers. `sent` is when the acquisition was sent."""
    hb: Optional[LeaseHeartbeat] = None

    def probe() -> bool:
        return hb is not None and hb.past_deadline()

    handle = LeaseHandle(fencing_token, probe=probe, key=ikey)
    call.handle = handle
    hb = LeaseHeartbeat(
        client=ctx.client,
        agent_id=agent_id,
        idempotency_key=ikey,
        fencing_token=fencing_token,
        heartbeat_interval_ms=interval_ms,
        on_lease_lost=call.on_lost,
        # None infers the engine's TTL from its interval.
        lease_ttl_ms=ttl_ms,
        acquired_sent_at=sent,
        acquired_expires_at_ms=expires_at_ms,
    )
    return handle, hb


def _is_stale_lease(exc: BaseException) -> bool:
    """Whether a commit was refused because its fencing token is stale: the
    engine's FAILED_PRECONDITION "Fenced out: stale lease", which shares its code
    with a position already taken, so the message tells them apart."""
    if not isinstance(exc, grpc.RpcError):
        return False
    if exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
        return False
    return "stale lease" in (exc.details() or "").lower()


def _unanswered(exc: BaseException) -> bool:
    """Whether a call got no answer either way: a transport error, or one that
    may pass, rather than a refusal."""
    if not isinstance(exc, grpc.RpcError):
        return True
    return exc.code() in _RETRYABLE_CODES


def _is_committed_elsewhere(exc: BaseException) -> bool:
    """Whether a commit was refused because its idempotency key is already
    committed at a different position: the key's work was recorded by another
    worker."""
    if not isinstance(exc, grpc.RpcError):
        return False
    if exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
        return False
    return "already committed at sequence" in (exc.details() or "").lower()


def _taken_over(name: str, result: Any) -> LeaseLostError:
    return LeaseLostError(
        f"Step '{name}' lost its lease to another worker before its result was "
        "recorded, so the result was not recorded. Calling the step again finds "
        "that worker's result or waits for it, unless its key includes the "
        "position (SCOPE_STEP_LOCAL).",
        result=result,
    )


def _commit(
    ctx: WorkflowContext,
    call: _CallLease,
    seq: int,
    name: str,
    result: Any,
    ikey: str,
    fencing_token: int,
    call_coordination: Optional[str],
) -> None:
    """Commits a body's result, whatever the lease's clock says.

    The engine decides: it accepts the commit while the lease is still this
    call's, which it is unless another worker took it over, and refuses a stale
    token. Not committing on a local judgement alone would throw away the
    record of a side effect that ran, and calling again would run it twice. A
    refusal about the key (a stale token, or the key recorded at another
    position) raises `LeaseLostError` with the result attached. A commit that
    gets no answer is sent once more before anything is released: while the
    lease is still this call's, that one is recorded, and an engine that
    already holds the first answers it as done.
    """
    # Taken once, so every attempt sends the same step, even if work the body
    # left running changes the object it returned. First: a result that cannot
    # be recorded fails here, before anything is sent or noted as returned.
    payload = deserialize(serialize({"result": result}))
    call.returned, call.result, call.payload = True, result, payload
    call.committing = True
    try:
        _send_commit(ctx, call, seq, name, ikey, fencing_token, call_coordination)
    except Exception as exc:
        if _is_stale_lease(exc) or _is_committed_elsewhere(exc):
            call.committing = False
            call.mark_taken_over()
            raise _taken_over(name, result) from exc
        if not _unanswered(exc):
            call.committing = False
            raise
        try:
            _send_commit(
                ctx, call, seq, name, ikey, fencing_token, call_coordination
            )
            return
        except Exception as again:
            call.committing = False
            if _is_stale_lease(again) or _is_committed_elsewhere(again):
                call.mark_taken_over()
                raise _taken_over(name, result) from again
            raise exc from None
    except BaseException:
        call.committing = False
        raise


def _send_commit(
    ctx: WorkflowContext,
    call: _CallLease,
    seq: int,
    name: str,
    ikey: str,
    fencing_token: int,
    call_coordination: Optional[str],
) -> None:
    ctx.client.commit_step(
        session_id=ctx.session_id,
        sequence=seq,
        name=name,
        status=STEP_STATUS_SUCCESS,
        output_payload=call.payload,
        idempotency_key=ikey,
        idempotency_fencing_token=fencing_token,
        # Omitted when the call names none, so the engine records the
        # session's default.
        coordination_id=call_coordination,
    )


def _resend_commit(
    ctx: WorkflowContext,
    call: _CallLease,
    seq: int,
    name: str,
    ikey: str,
    fencing_token: int,
    call_coordination: Optional[str],
) -> bool:
    """Decides a failed commit whose lease the engine would not release.

    Either the commit landed and only its answer was lost, which completed the
    key under this call's own token, or another worker holds the call. The same
    commit sent again tells them apart: the engine answers an identical commit
    it already holds as done, and refuses a stale token. True when it was done;
    `LeaseLostError` when the refusal is about the key (a stale token, or the
    key recorded at another position), so another worker holds the call; False
    for anything else (no answer, or the position already filled, which can
    also mean the commit landed), so the call fails as it would have.
    """
    try:
        _send_commit(ctx, call, seq, name, ikey, fencing_token, call_coordination)
    except Exception as again:
        if not _is_stale_lease(again) and not _is_committed_elsewhere(again):
            return False
        call.apply_held_loss()
        call.mark_taken_over()
        raise _taken_over(name, call.result) from again
    return True


def _take_hit(
    ctx: WorkflowContext,
    resp: Any,
    seq: int,
    name: str,
    call_coordination: Optional[str],
    agent_id: str,
    ikey: str,
    lease_ttl_ms: Optional[int] = None,
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
            lease_ttl_ms=lease_ttl_ms,
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
    lease_ttl_ms: Optional[int] = None,
) -> Any:
    """Records a function's result as a step of the running workflow.

    `lease_ttl_ms` is how long the call's lease lasts without a renewal, in
    milliseconds, and so how soon another caller can take the call over if this
    process crashes or freezes: one TTL after the last renewal, which a waiting
    caller is told within about 100 ms. Renewals come every quarter of it.
    None takes the session's (`durable_tools(..., lease_ttl_ms=...)`), or
    else the engine's default, 20 s on current engines. A holder that stops
    renewing for one TTL while its body still runs is taken over too, so set it
    well above the longest pause a call can see. At least `MIN_LEASE_TTL_MS`
    (1000), at most `MAX_LEASE_TTL_MS` (2^53 - 1) and an `int`, or the decorator
    raises `ValueError` (or `TypeError`); the engine's maximum lease lifetime
    caps it.
    """
    if func is None:
        return functools.partial(
            step,
            idempotency_key=idempotency_key,
            agent_id=agent_id,
            tool_name=tool_name,
            scope=scope,
            shared_on=shared_on,
            lease_ttl_ms=lease_ttl_ms,
        )

    actual_tool_name = tool_name or func.__name__
    _validate_shared_scope(func, scope, shared_on, idempotency_key)
    # Checked here rather than above, so the decorator form names the step too.
    lease_ttl_ms = check_lease_ttl_ms(lease_ttl_ms, f"step {actual_tool_name!r}")

    def _ttl_for(ctx: WorkflowContext) -> Optional[int]:
        """The step's own TTL, or else the session's."""
        return lease_ttl_ms if lease_ttl_ms is not None else ctx.lease_ttl_ms

    def _interval_for(resp: Any, ttl_ms: Optional[int]) -> int:
        """How often to renew: what the engine suggests, but at least every
        quarter of the TTL the renewals extend by, so the lease cannot lapse
        between them. Released engines suggest exactly that."""
        interval_ms: int = resp.heartbeat_interval_ms or 5000
        if ttl_ms is not None:
            interval_ms = min(interval_ms, max(1, ttl_ms // 4))
        return interval_ms

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # A replay that refused stops the run, even where the workflow caught
        # the refusal: no later step may run on a guessed branch.
        ctx.raise_if_refused()
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
        # A lease the step could not release after failing, kept with its
        # failure record. See `_release_after_failure`.
        unreleased: Optional[Dict[str, Any]] = None
        started = time.monotonic()
        # When the step failed, before releasing its lease and stopping its
        # heartbeat, which a replayed cancellation must not count.
        failed_at: Optional[float] = None
        # Whether the failure path already stopped the heartbeat.
        stopped = False
        # The call's lease as the engine answers about it.
        call = _CallLease()
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
            # The lease as the body sees it through current_lease(). Only when
            # the call holds one: a hit never runs the body.
            handle: Optional[LeaseHandle] = None
            ttl_ms = _ttl_for(ctx)

            handed_back = 0
            unanswered = 0
            while True:
                sent = (time.monotonic(), time.time())
                try:
                    resp = await _arbitrate_async(ctx, agent_id, ikey, ttl_ms, seq)
                except asyncio.CancelledError:
                    # The engine may have granted the request before its
                    # cancellation took effect, and the grant may not have come
                    # back to be handed back.
                    unreleased = _lost_answer(agent_id, ikey, ttl_ms)
                    raise
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        raise DivergentStepError(
                            f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                            f"{exc.details()}"
                        ) from exc
                    if exc.code() in _RETRYABLE_CODES:
                        # Kept until an answer, so a failure or cancellation
                        # before one leaves the record waiting it out.
                        unreleased = _lost_answer(agent_id, ikey, ttl_ms)
                        if unanswered < _MAX_UNANSWERED:
                            unanswered += 1
                            await asyncio.sleep(0.2 * unanswered)
                            continue
                    raise
                # An answer: only failures since the last one count.
                unanswered = 0
                if resp.status == CACHE_STATUS_HIT:
                    return _take_hit(
                        ctx,
                        resp,
                        seq,
                        actual_tool_name,
                        call_coordination,
                        agent_id,
                        ikey,
                        ttl_ms,
                    )
                elif resp.status == CACHE_STATUS_IN_PROGRESS:
                    # After a wait the engine served, ask again at once.
                    if _answered_at_once(sent):
                        retry_s = _retry_after_s(resp)
                        logger.info(
                            "Step %s is in progress by another worker. "
                            "Sleeping %d ms...",
                            actual_tool_name,
                            retry_s * 1000,
                        )
                        await asyncio.sleep(retry_s)
                elif resp.status == CACHE_STATUS_ACQUIRED:
                    # A lease handed back below and not released has lapsed or
                    # been released by now, or the engine would not grant the
                    # key again.
                    unreleased = None
                    fencing_token = resp.fencing_token or 0
                    interval_ms = _interval_for(resp, ttl_ms)
                    late = _granted_late(sent, interval_ms)
                    handle, hb = _lease_for_body(
                        ctx,
                        call,
                        agent_id,
                        ikey,
                        fencing_token,
                        interval_ms,
                        ttl_ms,
                        sent,
                        # Read against the request's send time, so only for a
                        # grant that answered at once.
                        0 if late else resp.lease_expires_at_ms,
                    )
                    if late:
                        try:
                            await asyncio.to_thread(hb.confirm_grant)
                        except asyncio.CancelledError:
                            # The body will not run under this lease. Until the
                            # release lands it claims this position, so the
                            # cancellation's record there retries it first.
                            hb.stop_sync()
                            unreleased = _hand_back_later(
                                ctx, agent_id, ikey, fencing_token, hb
                            )
                            call.handle = None
                            raise
                    if handle.is_lost:
                        # Neither the renewal nor the request can show the
                        # lease is still held, so the body must not start under
                        # it: it would act on a lease this side has already
                        # given up. Nothing has run, so hand the lease back and
                        # ask again, which finds the call's result, waits behind
                        # its holder, or grants a lease this side can count on.
                        # Off the loop: a release can take its whole deadline,
                        # and other calls' renewals run on this loop. Counted
                        # as unreleased until it says otherwise.
                        unreleased = _held(agent_id, ikey, fencing_token, hb)
                        unreleased = await asyncio.to_thread(
                            _hand_back,
                            ctx,
                            agent_id,
                            ikey,
                            fencing_token,
                            "LEASE_UNCONFIRMED",
                            hb,
                        )
                        handed_back += 1
                        unconfirmed = _unconfirmed(actual_tool_name, handed_back, hb)
                        call.handle = None
                        handle, hb, fencing_token = None, None, 0
                        if handed_back >= _MAX_HAND_BACKS:
                            raise unconfirmed
                        continue
                    hb.start_async()
                    break
                else:
                    break

            try:
                # The body sees the call's own lease. Without one (an answer
                # this SDK does not know) it sees none, not an enclosing one,
                # so no other lease's key is forwarded in this call's name.
                lease_var = _current_lease.set(handle)
                try:
                    result = await func(*args, **kwargs)
                finally:
                    _current_lease.reset(lease_var)
                # The body is over: work it started and left running finds the
                # lease over from here on, through the commit's round trip too.
                if handle is not None:
                    handle._mark_released()
                _commit(
                    ctx,
                    call,
                    seq,
                    actual_tool_name,
                    result,
                    ikey,
                    fencing_token,
                    call_coordination,
                )
                return result
            except BaseException as e:
                failed_at = time.monotonic()
                if handle is not None:
                    handle._mark_released()
                if hb:
                    # Told to stop before the release, so it starts no renewal
                    # after it. Not awaited: an await is where a cancellation
                    # lands, and would replace the step's own exception.
                    hb.stop_soon()
                    stopped = True
                # Released whatever the body raised and whatever the lease's
                # clock says, cancellation too: a lease left held would refuse
                # the step that takes this position next until it lapsed. The
                # engine releases only a lease that is still this call's, so its
                # answer says whether another worker took the call over.
                if fencing_token > 0 and hb is not None and not call.lost:
                    call.releasing = True
                    unreleased, released = _release_after_failure(
                        ctx, agent_id, ikey, fencing_token, actual_tool_name, hb
                    )
                    if released is False:
                        # For a body that returned, the commit may have landed
                        # and refused the release itself: the resend below
                        # decides. For one that raised, it is no longer ours.
                        if call.returned:
                            call.lost = True
                        else:
                            call.mark_taken_over()
                # Only for an error: an interrupt (Ctrl-C, SystemExit) is on its
                # way out and must not be swallowed by a resend that succeeds.
                if (
                    call.lost
                    and call.returned
                    and isinstance(e, Exception)
                    and not isinstance(e, LeaseLostError)
                ):
                    if _resend_commit(
                        ctx,
                        call,
                        seq,
                        actual_tool_name,
                        ikey,
                        fencing_token,
                        call_coordination,
                    ):
                        return call.result
                call.apply_held_loss()
                raise e
            finally:
                if handle is not None:
                    # Work the body started and left running finds the lease
                    # over from here on.
                    handle._mark_released()
                if hb and not stopped:
                    # Not awaited after a commit either: a cancellation landing
                    # there would turn a committed step into a cancelled one.
                    hb.stop_soon()
        except BaseException as exc:
            # Recorded at its position even when another worker took the call
            # over: a resumed @workflow replays by position, and finds the
            # failure there as the run saw it. Calling the step again lands
            # one position on and finds the other worker's result as a hit.
            if live:
                _note_failure(
                    ctx,
                    seq,
                    actual_tool_name,
                    exc,
                    call_coordination,
                    agent_id,
                    ikey,
                    started,
                    failed_at or time.monotonic(),
                    unreleased,
                    lease_ttl_ms=_ttl_for(ctx),
                )
            raise
        finally:
            ctx.end_call()

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # A replay that refused stops the run, even where the workflow caught
        # the refusal: no later step may run on a guessed branch.
        ctx.raise_if_refused()
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
        # A lease the step could not release after failing, kept with its
        # failure record. See `_release_after_failure`.
        unreleased: Optional[Dict[str, Any]] = None
        started = time.monotonic()
        # When the step failed, before releasing its lease and stopping its
        # heartbeat, which a replayed cancellation must not count.
        failed_at: Optional[float] = None
        # Whether the failure path already stopped the heartbeat.
        stopped = False
        # The call's lease as the engine answers about it.
        call = _CallLease()
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
            # The lease as the body sees it through current_lease(). Only when
            # the call holds one: a hit never runs the body.
            handle: Optional[LeaseHandle] = None
            ttl_ms = _ttl_for(ctx)

            handed_back = 0
            unanswered = 0
            while True:
                sent = (time.monotonic(), time.time())
                try:
                    resp = ctx.client.check_idempotency_cache(
                        agent_id=agent_id,
                        idempotency_key=ikey,
                        # Wait in the engine while another worker holds the
                        # call; see _ENGINE_WAIT_MS.
                        wait_timeout_ms=_ENGINE_WAIT_MS,
                        lease_ttl_ms=ttl_ms,
                        session_id=ctx.session_id,
                        # Tell the engine where this call intends to write,
                        # so a lease at an already-committed position is refused
                        # before the tool body runs rather than after.
                        sequence=seq,
                        timeout=_ENGINE_WAIT_DEADLINE_S,
                    )
                except grpc.RpcError as exc:
                    if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        raise DivergentStepError(
                            f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                            f"{exc.details()}"
                        ) from exc
                    if exc.code() in _RETRYABLE_CODES:
                        # Kept until an answer, so a failure before one leaves
                        # the record waiting it out.
                        unreleased = _lost_answer(agent_id, ikey, ttl_ms)
                        if unanswered < _MAX_UNANSWERED:
                            unanswered += 1
                            time.sleep(0.2 * unanswered)
                            continue
                    raise
                # An answer: only failures since the last one count.
                unanswered = 0
                if resp.status == CACHE_STATUS_HIT:
                    return _take_hit(
                        ctx,
                        resp,
                        seq,
                        actual_tool_name,
                        call_coordination,
                        agent_id,
                        ikey,
                        ttl_ms,
                    )
                elif resp.status == CACHE_STATUS_IN_PROGRESS:
                    # After a wait the engine served, ask again at once.
                    if _answered_at_once(sent):
                        retry_s = _retry_after_s(resp)
                        logger.info(
                            "Step %s is in progress by another worker. "
                            "Sleeping %d ms...",
                            actual_tool_name,
                            retry_s * 1000,
                        )
                        time.sleep(retry_s)
                elif resp.status == CACHE_STATUS_ACQUIRED:
                    # A lease handed back below and not released has lapsed or
                    # been released by now, or the engine would not grant the
                    # key again.
                    unreleased = None
                    fencing_token = resp.fencing_token or 0
                    interval_ms = _interval_for(resp, ttl_ms)
                    late = _granted_late(sent, interval_ms)
                    handle, hb = _lease_for_body(
                        ctx,
                        call,
                        agent_id,
                        ikey,
                        fencing_token,
                        interval_ms,
                        ttl_ms,
                        sent,
                        # Read against the request's send time, so only for a
                        # grant that answered at once.
                        0 if late else resp.lease_expires_at_ms,
                    )
                    if late:
                        hb.confirm_grant()
                    if handle.is_lost:
                        # Neither the renewal nor the request can show the
                        # lease is still held, so the body must not start under
                        # it: it would act on a lease this side has already
                        # given up. Nothing has run, so hand the lease back and
                        # ask again, which finds the call's result, waits behind
                        # its holder, or grants a lease this side can count on.
                        unreleased = _hand_back(
                            ctx, agent_id, ikey, fencing_token, "LEASE_UNCONFIRMED", hb
                        )
                        handed_back += 1
                        unconfirmed = _unconfirmed(actual_tool_name, handed_back, hb)
                        call.handle = None
                        handle, hb, fencing_token = None, None, 0
                        if handed_back >= _MAX_HAND_BACKS:
                            raise unconfirmed
                        continue
                    hb.start_sync()
                    break
                else:
                    break

            try:
                # The body sees the call's own lease. Without one (an answer
                # this SDK does not know) it sees none, not an enclosing one,
                # so no other lease's key is forwarded in this call's name.
                lease_var = _current_lease.set(handle)
                try:
                    result = func(*args, **kwargs)
                finally:
                    _current_lease.reset(lease_var)
                # The body is over: work it started and left running finds the
                # lease over from here on, through the commit's round trip too.
                if handle is not None:
                    handle._mark_released()
                _commit(
                    ctx,
                    call,
                    seq,
                    actual_tool_name,
                    result,
                    ikey,
                    fencing_token,
                    call_coordination,
                )
                return result
            except BaseException as e:
                failed_at = time.monotonic()
                if handle is not None:
                    handle._mark_released()
                if hb:
                    # Stopped before the release, so it renews nothing after it.
                    hb.stop_sync()
                    stopped = True
                # Released whatever the body raised and whatever the lease's
                # clock says, cancellation too: a lease left held would refuse
                # the step that takes this position next until it lapsed. The
                # engine releases only a lease that is still this call's, so its
                # answer says whether another worker took the call over.
                if fencing_token > 0 and hb is not None and not call.lost:
                    call.releasing = True
                    unreleased, released = _release_after_failure(
                        ctx, agent_id, ikey, fencing_token, actual_tool_name, hb
                    )
                    if released is False:
                        # For a body that returned, the commit may have landed
                        # and refused the release itself: the resend below
                        # decides. For one that raised, it is no longer ours.
                        if call.returned:
                            call.lost = True
                        else:
                            call.mark_taken_over()
                # Only for an error: an interrupt (Ctrl-C, SystemExit) is on its
                # way out and must not be swallowed by a resend that succeeds.
                if (
                    call.lost
                    and call.returned
                    and isinstance(e, Exception)
                    and not isinstance(e, LeaseLostError)
                ):
                    if _resend_commit(
                        ctx,
                        call,
                        seq,
                        actual_tool_name,
                        ikey,
                        fencing_token,
                        call_coordination,
                    ):
                        return call.result
                call.apply_held_loss()
                raise e
            finally:
                if handle is not None:
                    # Work the body started and left running finds the lease
                    # over from here on.
                    handle._mark_released()
                if hb and not stopped:
                    hb.stop_sync()
        except BaseException as exc:
            # Recorded at its position even when another worker took the call
            # over: a resumed @workflow replays by position, and finds the
            # failure there as the run saw it. Calling the step again lands
            # one position on and finds the other worker's result as a hit.
            if live:
                _note_failure(
                    ctx,
                    seq,
                    actual_tool_name,
                    exc,
                    call_coordination,
                    agent_id,
                    ikey,
                    started,
                    failed_at or time.monotonic(),
                    unreleased,
                    is_async=False,
                    lease_ttl_ms=_ttl_for(ctx),
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
    lease_ttl_ms: Optional[int] = None,
) -> Any:
    """
    Decorator for tool steps that require idempotency tracking.
    Functionally identical to @step for now, but semantically distinct.
    `lease_ttl_ms` is as for `step`.
    """
    if func is None:
        return functools.partial(
            tool,
            idempotency_key=idempotency_key,
            agent_id=agent_id,
            tool_name=tool_name,
            scope=scope,
            shared_on=shared_on,
            lease_ttl_ms=lease_ttl_ms,
        )
    # Checked here too, so a refusal names a tool as a tool, and in the order
    # `step` checks them.
    _validate_shared_scope(func, scope, shared_on, idempotency_key)
    check_lease_ttl_ms(lease_ttl_ms, f"tool {tool_name or func.__name__!r}")
    return step(
        func=func,
        idempotency_key=idempotency_key,
        agent_id=agent_id,
        tool_name=tool_name,
        scope=scope,
        shared_on=shared_on,
        lease_ttl_ms=lease_ttl_ms,
    )
