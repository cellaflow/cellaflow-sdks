import asyncio
import contextlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import contextvars
import logging
import threading
import time

import grpc

from cellaflow.client import CellaflowClient
from cellaflow.history import GRAPH_PAGE, latest_session_sequence
from cellaflow.failures import (
    _MAX_GROUP_TOTAL,
    _MAX_RECORD_BYTES,
    capture,
    findable,
    incomplete_group,
    modules_at_first_run,
    rebuild,
    type_location,
    unrecorded,
)
from cellaflow.idempotency import failure_idempotency_key, record_idempotency_key
from cellaflow.lease import check_lease_ttl_ms
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2

logger = logging.getLogger(__name__)


def _identity(exc: Optional[BaseException]) -> Optional[Tuple[int, type, str]]:
    """An exception's identity without a reference to it, which would keep its
    traceback, and every frame in it, alive. Exceptions cannot be weakly
    referenced, and Python reuses a freed exception's id for the next one of the
    same size, so the class and message are kept with it: a new exception of
    the same class at the same address is told apart by what it says."""
    if exc is None:
        return None
    try:
        message = str(exc)[:200]
    except Exception:
        message = ""
    return (id(exc), type(exc), message)


class _RecordHeld(Exception):
    """A live lease holds a hit's record. Wait `retry_after_s` and ask again."""

    def __init__(self, retry_after_s: float) -> None:
        super().__init__(f"the record is leased; retry in {retry_after_s}s")
        self.retry_after_s = retry_after_s


#: The longest any one engine call writing a record may take. A write that
#: cannot finish in time is dropped rather than hold up the run.
_RECORD_RPC_TIMEOUT_S = 10.0
#: Marks a refusal raised again as a copy, so the workflow's end can tell it
#: from an exception the workflow raised from the refusal.
_REFUSAL_COPY = "_cellaflow_refusal_copy"
#: How long the last failure record keeps trying when the failed step's own
#: unreleased lease is not what holds it up: for someone else's lease on its
#: position, or for an engine error that may pass.
_FINAL_HELD_WAIT_S = 5.0
#: How long the last failure record keeps retrying errors that may pass, in a
#: row, before it is dropped. Longer than the default lease lapses it may wait
#: for (about 27 s at the engine's 20 s TTL), so those runs wait as they always
#: have, while a long `lease_ttl_ms` cannot hold the return through an outage.
_FINAL_ERROR_WAIT_S = 30.0
#: Between tries of the last failure record after an error that may pass.
_FINAL_RETRY_S = 0.5
#: How long after its estimated lapse a failed step's unreleased lease is still
#: waited for, for clock and renewal skew.
_LAPSE_GRACE_S = 1.0


#: Failures writing a hit's record that may succeed if sent again. The record is
#: kept for the next step to retry; any other failure drops it. INTERNAL is how
#: the engine reports a failed storage write, which it does not retry itself, and
#: CANCELLED is the call being abandoned rather than refused.
_RETRYABLE_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.CANCELLED,
    }
)


@dataclass
class WorkflowContext:
    client: CellaflowClient
    session_id: str
    workflow_version: str
    sequence: int = 0
    # Map sequence number to the recorded step at that position, as
    # {"name": str, "payload": dict}. The name is retained so replay can
    # verify it is returning *this* step's result and not whatever happens to sit
    # at the same index.
    replayed_steps: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    # The session's default coordination domain: the ticket, task or tenant
    # several agents are collaborating on, as the engine stored it. Steps that
    # do not name their own are recorded under it, and SCOPE_SHARED derives its
    # keys from it.
    coordination_id: Optional[str] = None
    # The session position the engine last reported, held from a cache hit
    # until the next step consumes it. See `reconcile_sequence`.
    _reported_sequence: Optional[int] = None
    # Whether this run has accounted for every position the session holds up to
    # `sequence`: true for a fresh session, and for one `@workflow` replayed from
    # the start. False once the run resumes into history it did not replay, which
    # is what `durable_tools` and any hit-based resume do. Only while it holds can
    # a cache hit be recorded in this session without risking a second record of
    # a step the session already holds. See `defer_hit_record`.
    #
    # False unless the caller says otherwise: a context built for a session whose
    # history it never read must not record into it.
    history_known: bool = False
    # Whether `sequence` is where the session is. False for a resumed
    # `durable_tools` session, which does not replay from the start: its first
    # call reads the session's position, unless a cache hit has reported it by
    # then. See `_ensure_position`.
    position_known: bool = True
    # How long the leases of this session's steps last without a renewal, in
    # milliseconds, unless a step sets its own. None asks for the engine's
    # default. See `step`'s `lease_ttl_ms`.
    lease_ttl_ms: Optional[int] = None
    # A cache hit's result, waiting to be recorded at its position in this
    # session. Written when the next step starts. See `defer_hit_record`.
    _pending_hit: Optional[Dict[str, Any]] = None
    # Steps of this session that have claimed a position and not yet returned.
    # Calls in one session can overlap: asyncio tasks, or a framework running
    # tools on a thread pool. See `take_hit`.
    _in_flight: int = 0
    # The refusal that stopped this run's replay, if one did. Every later step
    # raises it again, and so does the workflow's end. See `refuse`.
    _refusal: Optional[BaseException] = field(default=None, repr=False, compare=False)
    # When the last failure record must be written or given up. See
    # `record_pending_failure` and `_rpc_timeout`.
    _rpc_deadline: Optional[float] = field(default=None, repr=False, compare=False)
    # The modules loaded when the first run in this process started. A failure
    # whose class comes from a module loaded later was most likely imported by a
    # step, and a resume does not run steps, so the run warns. Taken once per
    # process, so every run warns, not only the first. See
    # `_warn_if_not_replayable`.
    _modules_at_start: "frozenset[str]" = field(
        default_factory=lambda: modules_at_first_run(), repr=False, compare=False
    )
    # The failures this run recorded, by the identity of their exception, with
    # the step's name and position. See `note_run_ended`.
    _recorded_failures: List[Tuple[Tuple[int, type, str], str, int, bool]] = field(
        default_factory=list, repr=False, compare=False
    )
    # The write of a pending record that async steps are waiting on. See
    # `begin_call_async`.
    _settling: "Optional[asyncio.Task[None]]" = field(
        default=None, repr=False, compare=False
    )
    # Guards the position counter, the reported position, the pending record and
    # the in-flight count, which every call in the session reads and writes, on
    # whichever thread it runs.
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        check_lease_ttl_ms(self.lease_ttl_ms, "WorkflowContext")

    def begin_call(self) -> int:
        """Counts a step as in flight and claims the next position for it.

        Claiming is one locked step, so calls on different threads never claim
        the same position, and a pending record is always written before any
        later position is claimed. Every call to this is paired with `end_call`.

        If the pending record's lease is held by another live lease, the call
        waits for it without holding the lock, then tries again. Every other
        call in the session finds the same record pending and waits too, so
        none claims a later position before it lands.
        """
        if not self.position_known:
            self._ensure_position()
        while True:
            position, wait = self._try_claim()
            if position is not None:
                return position
            time.sleep(wait)

    async def begin_call_async(self) -> int:
        """`begin_call` for an async step: waits without blocking the event loop.

        A pending record is written on a worker thread first, because writing it
        is engine calls and encoding that would stall every other task on the
        loop. Every call that finds it pending waits on the same write, and
        they resume in the order they arrived, so positions are still claimed in
        call order, as a resume claims them. The claim itself stays on the loop:
        it is quick, and a claim made on a thread that a cancellation abandoned
        would leave a position counted that no step owns.
        """
        if not self.position_known:
            await asyncio.to_thread(self._ensure_position)
        loop = asyncio.get_running_loop()
        while True:
            if self._pending_hit is not None:
                settling = self._settling
                if (
                    settling is None
                    or settling.done()
                    or settling.get_loop() is not loop
                ):
                    settling = loop.create_task(self._settle_async())
                    self._settling = settling
                # Shielded: one caller being cancelled does not stop the write
                # the others are waiting on.
                await asyncio.shield(settling)
            position, wait = self._try_claim()
            if position is not None:
                return position
            await asyncio.sleep(wait)

    def _ensure_position(self) -> None:
        """Moves the counter to where the session is, before a resumed session's
        first call claims a position.

        Counted from 0, a call that is not a cache hit took position 1, which
        the session already holds, and the engine refused it as divergent before
        its body ran. Not needed once a cache hit has reported the session's
        position, which the claim adopts.

        A history the engine cannot find is an error, not a session at 0: the
        engine reported this session as recovered. A read that fails with an
        error that may pass is not: the call goes ahead as it did before, which
        a cache hit needs no position for, and the next call reads again.
        """
        with self._lock:
            if self.position_known or self._reported_sequence is not None:
                self.position_known = True
                return
        try:
            position = latest_session_sequence(
                self.client, self.session_id, limit=GRAPH_PAGE, missing_ok=False
            )
        except grpc.RpcError as exc:
            if exc.code() not in _RETRYABLE_CODES:
                raise
            logger.warning(
                "Cellaflow: could not read where session %s is (%s); this call "
                "goes ahead without it, and the next one reads it again.",
                self.session_id,
                exc.code().name,
            )
            return
        with self._lock:
            if not self.position_known:
                if self._reported_sequence is None:
                    self.sequence = max(self.sequence, position)
                self.position_known = True

    async def _settle_async(self) -> None:
        """Writes the pending record on a worker thread, waiting out a held one."""
        while self._pending_hit is not None:
            wait = await asyncio.to_thread(self._settle_pending)
            if not wait:
                return
            await asyncio.sleep(wait)

    def _settle_pending(self) -> float:
        """Writes a pending record, or says how long to wait before trying again."""
        self._prepare_pending()
        with self._lock:
            return self._reconcile_or_wait()

    def _try_claim(self) -> Tuple[Optional[int], float]:
        """One attempt to claim a position: the position, or how long to wait.

        Settling the pending record and claiming are one locked step, so no
        other call can hold a new record in between.
        """
        self._prepare_pending()
        with self._lock:
            wait = self._reconcile_or_wait()
            if wait:
                return None, wait
            self.sequence += 1
            self._in_flight += 1
            return self.sequence, 0.0

    def _reconcile_or_wait(self) -> float:
        """`reconcile_sequence`, or how long a held record says to wait. Under
        the lock."""
        try:
            self.reconcile_sequence()
        except _RecordHeld as held:
            return held.retry_after_s
        return 0.0

    def end_call(self) -> None:
        """Marks a step claimed by `begin_call` as returned."""
        with self._lock:
            self._in_flight -= 1

    def take_hit(
        self,
        sequence: int,
        name: str,
        payload: Dict[str, Any],
        reported: int,
        coordination_id: Optional[str],
        agent_id: str = "default",
        received_key: str = "",
        lease_ttl_ms: Optional[int] = None,
    ) -> None:
        """Decides what a cache hit at `sequence` leaves in this session.

        The hit is recorded at its position only when nothing else could have
        moved the session meanwhile: it is the only call in flight and no later
        position was claimed while it waited, the run knows everything the session
        holds, and the engine reports the session exactly one position behind it.
        Otherwise the run adopts the position the engine
        reported and stops recording hits: a peer in this session already wrote
        it, the run is resuming into history it did not replay, or other calls in
        flight may commit in any order.
        """
        with self._lock:
            behind_by_one = reported == sequence - 1
            # No other call is in flight now, and none claimed a later position
            # while this one waited: one that did and then failed without
            # committing leaves the counter past `sequence`.
            alone = self._in_flight == 1 and self.sequence == sequence
            if alone and self.history_known and behind_by_one:
                self.defer_hit_record(
                    sequence,
                    name,
                    payload,
                    coordination_id,
                    agent_id,
                    received_key,
                    lease_ttl_ms,
                )
                return
            # The run can no longer tell what the session holds.
            self.history_known = False
            self.record_engine_sequence(reported)

    def record_engine_sequence(self, sequence: int) -> None:
        """Notes the session position the engine reported alongside a cache hit.

        Held rather than applied immediately: a workflow whose last act is a
        shared tool — the common shape — should not pay for bookkeeping it will
        never use. `reconcile_sequence` consumes it at the start of the next
        step.
        """
        self._reported_sequence = sequence

    def defer_hit_record(
        self,
        sequence: int,
        name: str,
        payload: Dict[str, Any],
        coordination_id: Optional[str],
        agent_id: str = "default",
        received_key: str = "",
        lease_ttl_ms: Optional[int] = None,
    ) -> None:
        """Holds a cache hit's result to be recorded at `sequence` in this session.

        A hit satisfied from another session leaves nothing at its position in
        this one. Without a record there, the next step takes that position, and
        a later resume of this session finds that step where the shared one was
        expected and cannot replay. Recording the result keeps the session's
        history the same shape as the work it did.

        Deferred to the next step, like `record_engine_sequence`: the record only
        matters once something would otherwise take its position, so a workflow
        that ends on a hit still does no extra work.

        `received_key` is the idempotency key the hit answered, and `agent_id`
        the agent that asked. The record is leased by `agent_id`, under its own
        key derived from the session, `sequence` and `received_key`, for
        `lease_ttl_ms`: the TTL of the call it records. See
        `record_idempotency_key`.
        """
        self._pending_hit = {
            "kind": "result",
            "status": common_pb2.StepStatus.STEP_STATUS_SUCCESS,
            "sequence": sequence,
            "name": name,
            "payload": payload,
            "coordination_id": coordination_id,
            "agent_id": agent_id,
            "received_key": received_key,
            "lease_ttl_ms": lease_ttl_ms,
        }

    def defer_failure_record(
        self,
        sequence: int,
        name: str,
        exc: BaseException,
        coordination_id: Optional[str],
        agent_id: str = "default",
        step_key: str = "",
        unreleased: Optional[Dict[str, Any]] = None,
        lease_ttl_ms: Optional[int] = None,
    ) -> None:
        """Holds a step's failure to be recorded at `sequence` in this session.

        A step that fails leaves its position empty. A workflow that catches the
        failure and carries on would otherwise put its next step one position
        past the gap, where the engine refuses the commit after that step's side
        effect has run. Recording the failure fills the position, and lets a
        resumed run raise the same exception there instead of running the step
        again, so the workflow takes the same branch. See `failures.rebuild`.

        Written when the next step claims a position, or when the workflow
        returns: only once the run has moved past the failure. A failure the
        workflow lets propagate is not recorded, so resuming retries the step.

        Only while the step is the session's only call in flight and no later
        position was claimed meanwhile, as for a hit. Otherwise nothing is
        recorded, as before.
        """
        # Captured now, before the workflow's handler runs: a handler that
        # changes the exception (`e.attempts += 1`) must find on replay what it
        # found the first time. Capturing also lets go of the exception, whose
        # traceback keeps every frame of the failed step alive. Skipped when the
        # failure could not be held anyway. The warnings about replaying it wait
        # until it is recorded; see `_prepare_pending`.
        with self._lock:
            if not self._can_hold(sequence):
                return
        recorded = capture(exc)  # never raises
        self._hold_failure(
            sequence,
            name,
            {"error": recorded},
            coordination_id,
            agent_id,
            step_key,
            source=exc,
            unreleased=unreleased,
            lease_ttl_ms=lease_ttl_ms,
        )

    def defer_cancellation_record(
        self,
        sequence: int,
        name: str,
        elapsed_s: float,
        coordination_id: Optional[str],
        agent_id: str = "default",
        step_key: str = "",
        source: Optional[BaseException] = None,
        unreleased: Optional[Dict[str, Any]] = None,
        lease_ttl_ms: Optional[int] = None,
    ) -> None:
        """Holds a step's cancellation to be recorded at `sequence`.

        The record fills the position, so the workflow's next step lands where
        it should. A resume refuses at it rather than replay it, because what
        the workflow saw depended on what cancelled the step, which the record
        cannot say; see `decorators._replay_recorded`. Recorded with how long
        the step ran, for whoever reads the history. Written and guarded exactly
        as a failure is; see `defer_failure_record`.
        """
        cancelled = {"format": 1, "sdk": "python", "elapsed_s": round(elapsed_s, 3)}
        self._hold_failure(
            sequence,
            name,
            {"cancelled": cancelled},
            coordination_id,
            agent_id,
            step_key,
            source=source,
            unreleased=unreleased,
            lease_ttl_ms=lease_ttl_ms,
        )

    def _hold_failure(
        self,
        sequence: int,
        name: str,
        payload: Dict[str, Any],
        coordination_id: Optional[str],
        agent_id: str,
        step_key: str,
        source: Optional[BaseException] = None,
        unreleased: Optional[Dict[str, Any]] = None,
        lease_ttl_ms: Optional[int] = None,
    ) -> None:
        """Holds a failure-kind record, if nothing claimed a later position.

        `source` is the exception, kept only as its identity and class: to warn
        about replaying it when it is recorded, and to recognise it if the
        workflow later ends by raising it. See `note_run_ended`.

        `unreleased` is the step's own lease, if it could not be released: it
        still claims this position, so the record retries the release or waits
        for it to lapse. It lives and goes with the record. See `_write_record`.
        """
        if unreleased is not None:
            # Counted from now: the step's heartbeat has been told to stop, and
            # `lapses_in_s` already allows for a renewal still on its way. See
            # `decorators._release_after_failure`.
            unreleased = dict(unreleased)
            unreleased["lapses_at"] = time.monotonic() + unreleased["lapses_in_s"]
        with self._lock:
            if not self._can_hold(sequence):
                return
            self._pending_hit = {
                "kind": "failure",
                "status": common_pb2.StepStatus.STEP_STATUS_FAILED,
                "sequence": sequence,
                "name": name,
                "payload": payload,
                "source": _identity(source),
                "unreleased": unreleased,
                "warn": source is not None and "error" in payload,
                "coordination_id": coordination_id,
                "agent_id": agent_id,
                "received_key": step_key,
                "lease_ttl_ms": lease_ttl_ms,
            }

    def _can_hold(self, sequence: int) -> bool:
        """Whether a failure at `sequence` can be held for recording. Under the
        lock.

        As for a hit: only while the step is the session's only call in flight
        and no later position was claimed meanwhile, and only in a run that knows
        everything the session holds (not a resumed `durable_tools` thread, which
        would record at a position the session already filled).

        A failure that overlapped another call is not recorded, so its position
        stays empty, and the run stops recording, as it does for a hit: a later
        failure held above that empty position would be written out of order,
        and the step after it would claim a position the engine then refuses
        after its side effect ran.
        """
        if not self.history_known:
            return False
        if self._in_flight == 1 and self.sequence == sequence:
            return True
        self.history_known = False
        return False

    def _prepare_pending(self) -> None:
        """Warns about what a resume could not reproduce of a held failure.

        Here, once, just before its record is written, because only now is the
        failure certain to be recorded: one the workflow lets propagate says
        nothing. Outside the lock, because the check rebuilds the exception.
        """
        pending = self._pending_hit
        if pending is None or not pending.get("warn"):
            return
        with self._lock:
            first = self._pending_hit is pending and pending.get("warn")
            if first:
                pending["warn"] = False
        if first:
            try:
                self._warn_if_not_replayable(
                    pending["source"][1],
                    pending["name"],
                    pending["sequence"],
                    pending["payload"]["error"],
                )
            except Exception:
                pass  # a warning is never worth failing a step over

    def _warn_if_not_replayable(
        self, cls: type, name: str, sequence: int, recorded: Dict[str, Any]
    ) -> None:
        """Says now, while the failure happens, what a resume could not reproduce."""
        if "grpc" in recorded:
            return
        if not findable(cls):
            logger.warning(
                "Cellaflow: %s raised by %r is not defined at module level, so a "
                "resumed run cannot raise it again and will refuse to resume past "
                "position %d. Define exceptions a step raises at module level.",
                cls.__qualname__,
                name,
                sequence,
            )
            return
        module = type_location(cls).partition(":")[0]
        if module not in self._modules_at_start and module != "__main__":
            logger.warning(
                "Cellaflow: %s raised by %r at position %d comes from module %s, "
                "which was first imported during this run, most likely by a step. "
                "A resume does not run steps, so it may not be imported then, and "
                "the resume would refuse at this position. Add `import %s` where "
                "the workflow is defined.",
                cls.__qualname__,
                name,
                sequence,
                module,
                module,
            )
        if cls.__module__ == "__main__":
            logger.warning(
                "Cellaflow: %s raised by %r is defined in the script being run "
                "(__main__). A run resumed from another entry point will not find "
                "it and will refuse to resume past position %d. Define exceptions "
                "a step raises in an importable module.",
                cls.__qualname__,
                name,
                sequence,
            )
        if incomplete_group(recorded):
            logger.warning(
                "Cellaflow: the exception group %s raised by %r at position %d is "
                "too large to record (over %d members, or over %d KB even with "
                "each member reduced to its type and message). A resumed run will "
                "refuse to resume past this position rather than raise a "
                "different group.",
                cls.__qualname__,
                name,
                sequence,
                _MAX_GROUP_TOTAL,
                _MAX_RECORD_BYTES // 1024,
            )
            return
        try:
            # The same rebuild a resume does, so anything that would refuse
            # there (a class only its extension module can construct, say) is
            # said now rather than discovered on resume.
            rebuild(recorded, name, sequence)
        except Exception as error:
            logger.warning(
                "Cellaflow: %s raised by %r at position %d cannot be raised again "
                "by a resumed run, which will refuse to resume past this position. "
                "%s",
                cls.__qualname__,
                name,
                sequence,
                error,
            )
            return
        missing = unrecorded(recorded)
        if missing:
            logger.warning(
                "Cellaflow: %s raised by %r at position %d has values that cannot "
                "be recorded (%s). A resumed run raises it with a placeholder for "
                "each, and using one raises AttributeError.",
                cls.__qualname__,
                name,
                sequence,
                ", ".join(missing),
            )

    def refuse(self, refusal: BaseException) -> None:
        """Records that this run's replay refused to go on.

        The refusal is raised at a step the workflow called inside its own
        `try` (a failure was recorded there because the workflow caught one),
        so a broad `except` can catch it and carry on down a branch nothing
        checked. From here every step raises it again before its body runs,
        and the workflow raises it at its end however it ends.
        """
        with self._lock:
            if self._refusal is None:
                self._refusal = refusal

    def raise_if_refused(self, cause: Optional[BaseException] = None) -> None:
        """Raises the refusal recorded by `refuse`, if any.

        Raised as a fresh copy whose cause is the original, so its traceback
        still shows where the replay refused, without the original gaining
        frames each time. `cause` is the exception the workflow is ending with,
        if it is: the refusal replaces it, even one the workflow raised from the
        refusal (`except Exception: raise PaymentFailed(...)`), because that
        outcome is one the first run never produced. A cancellation or process
        exit (anything not an `Exception`) is not replaced; it keeps
        propagating as itself.
        """
        refusal = self._refusal
        if refusal is None:
            return
        if cause is not None:
            if not isinstance(cause, Exception):
                return
            if cause is refusal or getattr(cause, _REFUSAL_COPY, False):
                return  # already the refusal, or a copy of it
        again = type(refusal)(*refusal.args)
        setattr(again, _REFUSAL_COPY, True)
        raise again from refusal

    def note_run_ended(self, exc: BaseException) -> None:
        """Warns when the workflow ends by raising a failure that was recorded.

        A failure the workflow lets propagate is not recorded, so a resume runs
        the step again. But a step that runs while it propagates (a cleanup in a
        `finally`, or in an `except` that re-raises) needs the next position, so
        the failure is recorded to fill it, and the history then holds both. A
        resume replays them and ends with the same exception: it cannot run the
        failed step again, because a later position is already filled.
        """
        chain: List[Optional[Tuple[int, type, str]]] = []
        link: Optional[BaseException] = exc
        while link is not None and len(chain) < 20:
            chain.append(_identity(link))
            link = link.__cause__ or link.__context__
        for source, name, sequence, cancelled in self._recorded_failures:
            if source in chain and cancelled:
                logger.warning(
                    "Cellaflow: the workflow ended with %r cancelled at position %d "
                    "of session %s. A step ran after the cancellation, so it was "
                    "recorded, and a resume of this session refuses at that "
                    "position: a replay cannot tell what cancelled it. To run it "
                    "again, start a new session.",
                    name,
                    sequence,
                    self.session_id,
                )
                return
            if source in chain:
                logger.warning(
                    "Cellaflow: the workflow ended with %s from %r at position %d "
                    "of session %s. A step ran after that failure, so it was "
                    "recorded, and resuming this session raises it again rather "
                    "than running %r again. To run it again, start a new session.",
                    source[1].__name__,
                    name,
                    sequence,
                    self.session_id,
                    name,
                )
                return

    def give_back_position(self, sequence: int) -> None:
        """Returns the position of a step a process exit interrupted.

        A `KeyboardInterrupt` or `SystemExit` is not something a workflow carries
        on past, so it is not recorded. If the code does carry on anyway, its next
        step takes this position rather than landing one past an empty one. Only
        while no later position was claimed meanwhile; if the step did commit
        before the interruption reached it, the next step's lease at that position
        is refused before its body runs.
        """
        with self._lock:
            if self._in_flight == 1 and self.sequence == sequence:
                self.sequence = sequence - 1

    def has_pending_failure(self) -> bool:
        """Whether a failure is held, waiting to be recorded."""
        pending = self._pending_hit
        return pending is not None and pending.get("kind") == "failure"

    def record_pending_failure(self) -> None:
        """Records a failure the workflow caught and finished after, before the
        workflow returns.

        A failure followed by a later step is recorded when that step starts;
        this covers one followed by none. It is written before the result is
        returned, not in the background: a record lost after the run reported
        its result would let a re-run run the step again, whose side effect
        could repeat, and which could then take a different branch. That costs
        one write's round trip at the end of such a run, and, when the failed
        step's lease could not be released, up to that lease's lapse. In an
        async workflow the write runs on a worker thread; if the caller cancels
        the workflow meanwhile, the write still finishes there.

        A lease holding the position is waited out, until the failed step's own
        unreleased lease lapses or for `_FINAL_HELD_WAIT_S`, whichever is later.
        Errors that may pass are retried for at most `_FINAL_ERROR_WAIT_S` in a
        row, so an engine that is down does not hold the return for as long as
        a long lease TTL. Every engine call is bounded by what is left of the
        time, and of the error budget. A record that still cannot be written,
        or that the engine refuses, is dropped with a warning, and a re-run
        runs the step again, as before failures were recorded.
        """
        pending = self._pending_hit
        if pending is None or pending.get("kind") != "failure":
            return
        deadline = time.monotonic() + _FINAL_HELD_WAIT_S
        unreleased = pending.get("unreleased")
        if unreleased is not None:
            deadline = max(deadline, unreleased["lapses_at"] + _LAPSE_GRACE_S)
        # When the run of errors that may pass began, if one is under way.
        failing_since: Optional[float] = None
        try:
            last_try = False
            while self._pending_hit is pending:
                tried_at = time.monotonic()
                # A call that hangs counts against the error budget it would
                # start, so a stalled engine is dropped on time too.
                budget_from = failing_since if failing_since is not None else tried_at
                self._rpc_deadline = min(deadline, budget_from + _FINAL_ERROR_WAIT_S)
                try:
                    wait = self._settle_pending()
                    reason = "a lease still holds its position"
                    limit = deadline
                    if failing_since is not None:
                        # An answer ends the run of errors, and the last try
                        # it was given.
                        failing_since, last_try = None, False
                except Exception as exc:
                    code = exc.code() if isinstance(exc, grpc.RpcError) else None
                    if code is None or code not in _RETRYABLE_CODES:
                        self._drop_final_record(
                            pending, f"{type(exc).__name__}: {exc}"
                        )
                        return
                    # It may have landed despite the error (a deadline that
                    # passed after the commit): the next try finds it written.
                    wait, reason = _FINAL_RETRY_S, f"{code.name}: {exc}"
                    if failing_since is None:
                        failing_since = tried_at
                    limit = min(deadline, failing_since + _FINAL_ERROR_WAIT_S)
                if not wait:
                    return  # written, or refused and dropped by the write itself
                remaining = limit - time.monotonic()
                if last_try or remaining <= 0:
                    self._drop_final_record(pending, reason)
                    return
                if wait >= remaining:
                    # Use what is left, then try once more.
                    wait, last_try = remaining, True
                time.sleep(wait)
        finally:
            self._rpc_deadline = None

    def _drop_final_record(self, pending: Dict[str, Any], reason: str) -> None:
        with self._lock:
            if self._pending_hit is pending:
                self._pending_hit = None
        logger.warning(
            "Cellaflow: the failure of %r at position %d of session %s was not "
            "recorded (%s). The workflow's result stands; a re-run of this session "
            "runs that step again.",
            pending["name"],
            pending["sequence"],
            self.session_id,
            reason,
        )

    def _rpc_timeout(self) -> float:
        """The deadline for one engine call writing a record: the usual one, or
        what is left of the last failure record's time, if less."""
        if self._rpc_deadline is None:
            return _RECORD_RPC_TIMEOUT_S
        left = self._rpc_deadline - time.monotonic()
        return max(0.5, min(_RECORD_RPC_TIMEOUT_S, left))

    def reconcile_sequence(self) -> None:
        """Records a deferred hit, then adopts the position the engine reported.

        `@step` increments this counter before every step, but a cache hit
        returns *without committing*. The engine's sequence therefore did not
        advance while the local one did, and the next commit fails the ordering
        check — one step after the real cause.

        The same-session case survives on a coincidence rather than an
        invariant: a peer's commit advances the engine by exactly the amount
        this caller advanced locally, so the two happen to stay equal. Any
        asymmetry breaks it — a hit satisfied from a *different* session, or
        replicas that reached a shared tool after different numbers of steps.

        The SDK cannot tell those apart locally, because whether the engine
        advanced depends on *whose* commit satisfied the hit. The engine reports
        it on the response instead, so this costs nothing beyond a field read.

        A no-op when the engine reported nothing — an older engine predating the
        field. Behaviour then degrades to the original defect rather than to
        something new.
        """
        pending = self._pending_hit
        if pending is not None:
            try:
                self._write_record(pending)
            except _RecordHeld:
                # Kept, and waited out by the caller. See `begin_call`.
                raise
            except Exception as exc:
                code = exc.code() if isinstance(exc, grpc.RpcError) else None
                if code in _RETRYABLE_CODES:
                    # Kept, so the step that hit this is retried with it. If the
                    # write did land, sending it again finds its completed entry.
                    raise
                # The record will not be accepted: another writer in this session
                # took the position first, the engine refused the write itself, or
                # it failed before reaching the engine and would fail the same way
                # on every retry. Read where the session is before changing
                # anything, so a failed read leaves the record to be tried again.
                position = max(
                    pending["sequence"] - 1,
                    latest_session_sequence(
                        self.client,
                        self.session_id,
                        from_sequence=pending["sequence"],
                        limit=GRAPH_PAGE,
                        timeout=self._rpc_timeout(),
                    ),
                )
                # The run no longer knows what the session holds: it stops
                # recording hits and continues from the session's real position.
                self._pending_hit = None
                self.history_known = False
                self.sequence = position
                if code == grpc.StatusCode.FAILED_PRECONDITION:
                    reason = "another writer committed or claimed it first"
                elif code is not None:
                    reason = f"the engine refused it ({code.name})"
                else:
                    reason = f"it could not be sent ({type(exc).__name__}: {exc})"
                logger.warning(
                    "Cellaflow: the %s of %r was not recorded at position %d "
                    "of session %s, because %s. Continuing from position %d. A "
                    "later resume of this session may not replay that step.",
                    "failure" if pending.get("kind") == "failure" else "result",
                    pending["name"],
                    pending["sequence"],
                    self.session_id,
                    reason,
                    self.sequence,
                )
                return
            if pending.get("kind") == "failure" and pending.get("source"):
                cancelled = "cancelled" in (pending.get("payload") or {})
                self._recorded_failures.append(
                    (pending["source"], pending["name"], pending["sequence"], cancelled)
                )
            self._pending_hit = None
            return
        if self._reported_sequence is not None:
            self.sequence = self._reported_sequence
            self._reported_sequence = None

    def _write_record(self, pending: Dict[str, Any]) -> None:
        """Records a received result at its position, through a lease.

        A plain commit is checked only for order, not against the positions that
        leases have claimed. A replica in this session holding a lease at the same
        position would then have its commit refused after its side effect ran,
        losing the record that it did. Leasing the record makes the engine decide:
        it refuses the record at a position another lease has claimed, or refuses
        the later lease before its side effect runs.
        """
        if pending.get("kind") == "failure":
            key = failure_idempotency_key(
                self.session_id,
                pending["sequence"],
                pending["name"],
                pending["received_key"],
            )
        else:
            key = record_idempotency_key(
                self.session_id, pending["sequence"], pending["received_key"]
            )
        agent_id = pending["agent_id"]
        unreleased = pending.get("unreleased")
        if unreleased is not None:
            # The failed step's own lease still claims this position, so the
            # engine would refuse the record. Try the release again first.
            try:
                self.client.release_lease(
                    agent_id=unreleased["agent_id"],
                    idempotency_key=unreleased["key"],
                    fencing_token=unreleased["token"],
                    reason="TOOL_ERROR",
                    timeout=self._rpc_timeout(),
                )
                pending["unreleased"] = None
            except Exception as exc:
                code = exc.code() if isinstance(exc, grpc.RpcError) else None
                if code not in _RETRYABLE_CODES:
                    # Already released or lapsed: nothing is left to free.
                    pending["unreleased"] = None
        try:
            resp = self.client.check_idempotency_cache(
                agent_id=agent_id,
                idempotency_key=key,
                # The TTL of the call the record is for. The record's lease is
                # not renewed, so this also bounds how long the write may take.
                lease_ttl_ms=pending.get("lease_ttl_ms"),
                session_id=self.session_id,
                sequence=pending["sequence"],
                timeout=self._rpc_timeout(),
            )
        except grpc.RpcError as exc:
            unreleased = pending.get("unreleased")
            if (
                unreleased is not None
                and exc.code() == grpc.StatusCode.FAILED_PRECONDITION
            ):
                remaining = unreleased["lapses_at"] + _LAPSE_GRACE_S - time.monotonic()
                if remaining > 0:
                    # Refused because of that lease, which lapses on its own:
                    # wait for it, as for any held record.
                    raise _RecordHeld(min(1.0, remaining)) from None
                pending["unreleased"] = None
            raise
        if resp.status == idempotency_pb2.CACHE_STATUS_HIT:
            # Written by an earlier attempt whose answer was lost.
            return
        if resp.status == idempotency_pb2.CACHE_STATUS_IN_PROGRESS:
            # A live lease on this record: an earlier attempt that could not
            # release it, or a replica writing the same record. It commits or
            # lapses, so the caller waits and asks again, outside the lock.
            raise _RecordHeld((resp.retry_after_ms or 1000) / 1000.0)
        if resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
            raise RuntimeError(
                f"unexpected cache status {resp.status} leasing the record"
            )
        token = resp.fencing_token or 0
        try:
            self.client.commit_step(
                session_id=self.session_id,
                sequence=pending["sequence"],
                name=pending["name"],
                status=pending["status"],
                output_payload=pending["payload"],
                idempotency_key=key,
                idempotency_fencing_token=token,
                coordination_id=pending["coordination_id"],
                timeout=self._rpc_timeout(),
            )
        except Exception:
            try:
                self.client.release_lease(
                    agent_id=agent_id,
                    idempotency_key=key,
                    fencing_token=token,
                    reason="TOOL_ERROR",
                    timeout=self._rpc_timeout(),
                )
            except Exception:
                # The lease lapses on its own. Losing the release is not worth
                # masking the error that caused it.
                pass
            raise

    @contextlib.contextmanager
    def bind(self) -> Iterator["WorkflowContext"]:
        """Re-establishes this context on the current thread or task.

        Needed when an agent framework dispatches a tool somewhere the caller's
        context does not reach -- `loop.run_in_executor` rather than
        `asyncio.to_thread`, say. With a single open session `get_context` can
        infer it; with several it cannot, and this is how the caller says which
        one the work belongs to:

            with durable_tools(config) as session:
                ...

            def my_tool(order_id):          # called by the framework
                with session.bind():
                    return charge(order_id)
        """
        token = set_context(self)
        try:
            yield self
        finally:
            reset_context(token)


def session_coordination_id(
    resp: service_pb2.StartSessionResponse, requested: Optional[str]
) -> Optional[str]:
    """The coordination domain a started or resumed session works under.

    The engine returns the value stored with the session, which is what lets a
    resumed agent that did not pass one adopt it and keep deriving the same
    shared keys as its peers. An engine that does not return the field keeps
    the requested value.
    """
    if resp.HasField("coordination_id"):
        return resp.coordination_id
    return requested


_current_context: contextvars.ContextVar[WorkflowContext] = contextvars.ContextVar(
    "workflow_context"
)

#: Sessions with an open `durable_tools` block, newest last. Consulted only when
#: the ContextVar is unset.
#:
#: Agent frameworks do not agree on how a tool is dispatched. Some call it on the
#: calling context; some hand it to `asyncio.to_thread`, which copies the context;
#: and some use `loop.run_in_executor`, which does not. Measured: LlamaIndex and
#: the OpenAI Agents SDK propagate, CrewAI propagates on its sync path only, and
#: AutoGen does not propagate at all. On those paths the ContextVar a caller set
#: is simply not visible, and a `@tool` would fail despite being correctly wrapped.
#:
#: The ContextVar stays authoritative wherever it survives -- it is what keeps
#: concurrent sessions in one process apart. This list is the fallback for where
#: it does not, and it deliberately refuses to guess when more than one session
#: is open.
_open_sessions: List["WorkflowContext"] = []
_open_sessions_lock = threading.Lock()


def _register_session(context: WorkflowContext) -> None:
    with _open_sessions_lock:
        _open_sessions.append(context)


def _deregister_session(context: WorkflowContext) -> None:
    with _open_sessions_lock:
        for i in range(len(_open_sessions) - 1, -1, -1):
            if _open_sessions[i] is context:
                del _open_sessions[i]
                return


def get_context() -> WorkflowContext:
    try:
        return _current_context.get()
    except LookupError:
        pass

    with _open_sessions_lock:
        candidates = list(_open_sessions)

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise RuntimeError(
            "No active workflow context found. "
            "Are you calling a @step inside a @workflow, or a @tool inside "
            "durable_tools()?"
        )

    # Two or more sessions are open and the framework dispatched this tool off
    # the calling context, so there is nothing left to say which one it belongs
    # to. Picking either would attribute a side effect -- and its lease -- to the
    # wrong session, which is worse than refusing.
    sessions = ", ".join(c.session_id for c in candidates)
    raise RuntimeError(
        f"Ambiguous workflow context: {len(candidates)} sessions are open "
        f"({sessions}) and this call arrived without one.\n\n"
        "The agent framework dispatched this tool onto a thread that does not "
        "carry the caller's context -- `loop.run_in_executor` does this, unlike "
        "`asyncio.to_thread`. With a single open session that is recoverable; "
        "with several it is not.\n\n"
        "Bind the session explicitly around the call:\n\n"
        "    with durable_tools(config) as session:\n"
        "        ...\n"
        "    # inside the tool the framework dispatched:\n"
        "    with session.bind():\n"
        "        charge(order_id)"
    )


def set_context(context: WorkflowContext) -> contextvars.Token[WorkflowContext]:
    return _current_context.set(context)


def reset_context(token: contextvars.Token[WorkflowContext]) -> None:
    _current_context.reset(token)
