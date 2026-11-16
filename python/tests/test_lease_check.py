"""The lease a step's body sees through current_lease(): what it holds, how a
loss reaches it, and what the decorator does with a call whose lease was lost.
Uses a scripted engine client, so no engine is needed."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import asyncio
import threading
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import grpc
import pytest

from cellaflow import (
    IdempotencyScope,
    LeaseLostError,
    current_lease,
    execution_lease,
    step,
    tool,
)
from cellaflow.context import WorkflowContext
from cellaflow.execution import LeaseHandle
from cellaflow.lease import LeaseHeartbeat
from cellaflow.serialization import serialize
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2


class _Refused(grpc.RpcError):  # type: ignore[misc]
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code, self._details = code, details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class ScriptedClient:
    """Records acquisitions, renewals, commits and releases. Acquisitions are
    answered from `answers`, then ACQUIRED with token 41 and a 250 ms interval."""

    def __init__(
        self,
        answers: Optional[List[Any]] = None,
        renew: Optional[Callable[[], Any]] = None,
        commit: Optional[Callable[[Dict[str, Any]], Any]] = None,
        released: Any = True,
        interval: int = 250,
    ) -> None:
        self.answers = list(answers or [])
        self.renew = renew
        self.commit = commit
        self.released = released
        self.interval = interval
        self.acquires: List[Dict[str, Any]] = []
        self.renewals: List[Dict[str, Any]] = []
        self.commits: List[Dict[str, Any]] = []
        self.releases: List[Dict[str, Any]] = []

    def check_idempotency_cache(self, **kw: Any) -> Any:
        self.acquires.append(kw)
        if self.answers:
            return self.answers.pop(0)
        return idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
            fencing_token=41,
            heartbeat_interval_ms=self.interval,
        )

    def renew_lease(self, **kw: Any) -> Any:
        self.renewals.append(kw)
        if self.renew is not None:
            return self.renew()
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit_step(self, **kw: Any) -> Any:
        self.commits.append(kw)
        if self.commit is not None:
            return self.commit(kw)
        return service_pb2.CommitStepResponse()

    def release_lease(self, **kw: Any) -> Any:
        self.releases.append(kw)
        released = self.released
        if callable(released):
            released = released(kw["idempotency_key"])
        return idempotency_pb2.ReleaseLeaseResponse(released=bool(released))

    def close(self) -> None:
        pass


def session(client: ScriptedClient) -> WorkflowContext:
    return WorkflowContext(
        client=client,  # type: ignore[arg-type]
        session_id="s-check",
        workflow_version="1.0.0",
        coordination_id="order-4417",
        history_known=True,
    )


class _ShiftedTime:
    """`time` as the lease module sees it, moved forward by `offset` seconds, as
    a process stopped for that long sees its clocks when it resumes."""

    def __init__(self) -> None:
        self.offset = 0.0

    def monotonic(self) -> float:
        return time.monotonic() + self.offset

    def time(self) -> float:
        return time.time() + self.offset


@pytest.fixture
def clocks(monkeypatch: Any) -> Iterator[_ShiftedTime]:
    shifted = _ShiftedTime()
    monkeypatch.setattr("cellaflow.lease.time", shifted)
    yield shifted


SUPERSEDED = idempotency_pb2.RENEW_FAILURE_REASON_SUPERSEDED
_HIT = idempotency_pb2.CheckCacheResponse(
    status=idempotency_pb2.CACHE_STATUS_HIT,
    current_sequence=0,
    cached_result=common_pb2.StepResult(
        output_payload=serialize({"result": "from-new-holder"})
    ),
)


def test_current_lease_in_a_body_is_the_calls_lease_and_outside_raises() -> None:
    client = ScriptedClient()
    seen: List[LeaseHandle] = []

    @tool
    def charge_card() -> Any:
        lease = current_lease()
        seen.append(lease)
        lease.check()
        return [lease.fencing_token, lease.is_lost]

    with session(client).bind():
        assert charge_card() == [41, False]
    # The body's lease is over once the call returns.
    assert seen[0].is_lost
    with pytest.raises(LookupError, match="No execution lease is active"):
        current_lease()


@pytest.mark.asyncio
async def test_an_async_step_body_sees_its_lease_too() -> None:
    client = ScriptedClient()

    @step
    async def charge_card() -> int:
        await asyncio.sleep(0)
        return current_lease().fencing_token

    with session(client).bind():
        assert await charge_card() == 41
    with pytest.raises(LookupError):
        current_lease()


def test_a_body_resumed_past_its_deadline_fails_check_before_any_renewal(
    clocks: _ShiftedTime,
) -> None:
    client = ScriptedClient()
    caught: List[BaseException] = []

    @tool(lease_ttl_ms=2000)
    def charge_card() -> str:
        clocks.offset = 60.0
        try:
            current_lease().check()
        except LeaseLostError as exc:
            caught.append(exc)
            raise
        return "sent"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            charge_card()
    assert caught and client.renewals == []


def test_a_healthy_lease_is_never_judged_lost() -> None:
    # Renewals keep moving the deadline: a body that runs for several TTLs
    # still holds its lease.
    client = ScriptedClient()

    @tool(lease_ttl_ms=1000)
    def charge_card() -> str:
        for _ in range(25):
            current_lease().check()
            time.sleep(0.1)
        return "sent"

    with session(client).bind():
        assert charge_card() == "sent"
    assert len(client.renewals) >= 8
    assert client.commits and client.releases == []


def test_a_renewal_the_engine_refuses_makes_check_raise() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    client = ScriptedClient(renew=lambda: refused)

    @tool
    def charge_card() -> str:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not current_lease().is_lost:
            time.sleep(0.02)
        current_lease().check()
        return "sent"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            charge_card()
    # Nothing was sent; the lease is released, and the engine's answer decides.
    assert client.commits == [] and len(client.releases) == 1


@pytest.mark.asyncio
async def test_cancel_on_loss_cancels_the_await_with_lease_lost_error() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    client = ScriptedClient(renew=lambda: refused)
    reached: List[str] = []

    @tool
    async def refund() -> str:
        lease = current_lease()
        lease.check()
        async with lease.cancel_on_loss():
            await asyncio.sleep(5)
            reached.append("sent")
        return "refunded"

    started = time.monotonic()
    with session(client).bind():
        with pytest.raises(LeaseLostError, match="cancel_on_loss"):
            await refund()
    assert reached == []
    assert time.monotonic() - started < 3
    # The task is not left cancelled: the cancellation became LeaseLostError.
    task = asyncio.current_task()
    assert task is not None
    if hasattr(task, "cancelling"):
        assert task.cancelling() == 0
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_cancel_on_loss_leaves_a_healthy_await_alone() -> None:
    client = ScriptedClient()

    @tool
    async def refund() -> str:
        async with current_lease().cancel_on_loss():
            await asyncio.sleep(0.3)
        return "refunded"

    with session(client).bind():
        assert await refund() == "refunded"
    # A loss after the block must not cancel the task later.
    await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_cancel_on_loss_passes_through_a_cancellation_that_is_not_a_loss() -> (
    None
):
    client = ScriptedClient()

    @tool
    async def refund() -> str:
        async with current_lease().cancel_on_loss():
            await asyncio.sleep(5)
        return "refunded"

    with session(client).bind():
        running = asyncio.ensure_future(refund())
        await asyncio.sleep(0.05)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running


def test_a_body_returning_after_a_local_loss_is_still_committed(
    clocks: _ShiftedTime,
) -> None:
    # Nobody took over, so the engine accepts the commit, and the side effect
    # the body ran is recorded rather than run again by a retry.
    client = ScriptedClient()

    @tool(lease_ttl_ms=2000)
    def charge_card() -> str:
        clocks.offset = 60.0
        return "refund-123"

    with session(client).bind():
        assert charge_card() == "refund-123"
    assert len(client.commits) == 1
    assert client.commits[0]["idempotency_fencing_token"] == 41


def test_a_body_raising_after_a_local_loss_is_released_and_recorded(
    clocks: _ShiftedTime,
) -> None:
    client = ScriptedClient()

    @tool(lease_ttl_ms=2000)
    def charge_card() -> str:
        clocks.offset = 60.0
        current_lease().check()
        return "sent"

    @tool
    def nxt() -> str:
        clocks.offset = 0.0
        return "next"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            charge_card()
        nxt()
    assert len(client.releases) == 1
    assert any(":failure:" in a["idempotency_key"] for a in client.acquires)
    assert [(c["sequence"], c["name"]) for c in client.commits] == [
        (1, "charge_card"),
        (2, "nxt"),
    ]


def test_a_bodys_own_error_after_a_takeover_reaches_the_caller_unchanged() -> None:
    client = ScriptedClient(released=False)
    declined = ValueError("card declined")

    @tool
    def charge_card() -> str:
        raise declined

    ctx = session(client)
    with ctx.bind():
        with pytest.raises(ValueError) as raised:
            charge_card()
    assert raised.value is declined
    # Recorded at its position as any failure is, once the run moves on.
    assert ctx.has_pending_failure()


def test_a_commit_whose_answer_was_lost_is_not_mistaken_for_a_takeover() -> None:
    # The first commit lands and its answer is lost; the key is complete under
    # this call's own token, so the release is refused too. The same commit
    # sent again is answered as done.
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(kw["sequence"])
        if len(sent) == 1:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        return "refund-123"

    @tool
    def nxt() -> str:
        return "next"

    with session(client).bind():
        assert charge_card() == "refund-123"
        assert nxt() == "next"
    assert sent == [1, 1, 2]
    # The resend is the same commit: same position, key, token and payload.
    assert client.commits[1] == client.commits[0]
    assert client.commits[1]["idempotency_fencing_token"] == 41


def test_an_unanswered_commit_and_resend_fail_as_before_not_as_a_takeover() -> None:
    first = _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(kw["sequence"])
        # The commit, its resend, and its resend after the refused release
        # all go unanswered; the failure record after them is written.
        if kw["sequence"] == 1 and len(sent) <= 3:
            raise first if len(sent) == 1 else _Refused(
                grpc.StatusCode.UNAVAILABLE, "connection reset"
            )
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit, released=lambda key: ":failure:" in key)

    @tool
    def charge_card() -> str:
        return "refund-123"

    @tool
    def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(grpc.RpcError) as raised:
            charge_card()
        nxt()
    assert raised.value is first
    assert any(":failure:" in a["idempotency_key"] for a in client.acquires)


def test_a_commit_refused_in_other_words_with_a_refused_release_is_a_takeover() -> (
    None
):
    done = _Refused(
        grpc.StatusCode.FAILED_PRECONDITION,
        "Idempotency key already committed at sequence 4, but this request "
        "specified sequence 1: one key cannot cover two graph positions",
    )

    def commit(kw: Dict[str, Any]) -> Any:
        raise done

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert lost.value.result == "refund-123" and lost.value.__cause__ is done


def test_a_loss_held_back_during_a_failing_commit_is_applied() -> None:
    completed = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_COMPLETED
    )
    state = {"committing": False}
    seen: List[LeaseHandle] = []

    def renew() -> Any:
        if state["committing"]:
            return completed
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit(kw: Dict[str, Any]) -> Any:
        state["committing"] = True
        time.sleep(0.5)  # a renewal goes out and is refused meanwhile
        raise _Refused(grpc.StatusCode.INTERNAL, "storage write failed")

    client = ScriptedClient(renew=renew, commit=commit)

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "charged"

    with session(client).bind():
        with pytest.raises(grpc.RpcError):
            charge_card()
    assert seen[0]._lost


def test_a_renewal_refused_because_the_commit_completed_is_not_a_loss() -> None:
    completed = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_COMPLETED
    )
    state = {"committing": False}
    seen: List[LeaseHandle] = []

    def renew() -> Any:
        if state["committing"]:
            return completed
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit(kw: Dict[str, Any]) -> Any:
        state["committing"] = True
        time.sleep(0.5)
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(renew=renew, commit=commit)

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "charged"

    with session(client).bind():
        assert charge_card() == "charged"
    assert not seen[0]._lost


def test_a_commit_the_engine_fences_out_becomes_lease_lost_error() -> None:
    stale = _Refused(grpc.StatusCode.FAILED_PRECONDITION, "Fenced out: stale lease")

    def commit(kw: Dict[str, Any]) -> Any:
        raise stale

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert lost.value.result == "refund-123"
    assert lost.value.__cause__ is stale
    assert client.releases == []


def test_any_other_commit_refusal_is_unchanged() -> None:
    taken = _Refused(grpc.StatusCode.FAILED_PRECONDITION, "Sequence mismatch")

    def commit(kw: Dict[str, Any]) -> Any:
        raise taken

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(grpc.RpcError) as refused:
            charge_card()
    assert refused.value is taken
    assert len(client.releases) == 1


def test_a_body_failing_with_its_lease_held_is_released_and_recorded() -> None:
    client = ScriptedClient()

    @tool
    def charge_card() -> str:
        current_lease().check()
        raise ValueError("card declined")

    @tool
    def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(ValueError, match="card declined"):
            charge_card()
        nxt()
    assert len(client.releases) == 1
    assert any(":failure:" in a["idempotency_key"] for a in client.acquires)


def test_a_cache_hit_runs_no_body_so_no_lease_reaches_it() -> None:
    client = ScriptedClient([_HIT])
    ran: List[bool] = []

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order"])
    def refund(order: str) -> None:
        ran.append(True)

    @tool
    def nxt() -> int:
        return current_lease().fencing_token

    with session(client).bind():
        assert refund("4417") == "from-new-holder"
        assert nxt() == 41
    assert ran == []
    assert any(":record:" in a["idempotency_key"] for a in client.acquires)


def test_work_left_running_finds_the_lease_released() -> None:
    client = ScriptedClient()
    later: List[bool] = []
    done = threading.Event()

    @tool
    def charge_card() -> str:
        lease = current_lease()

        def after() -> None:
            time.sleep(0.05)
            later.append(lease.is_lost)
            done.set()

        threading.Thread(target=after).start()
        return "done"

    with session(client).bind():
        charge_card()
    assert done.wait(2)
    assert later == [True]


def test_execution_lease_check_fails_for_a_holder_resumed_past_its_deadline(
    monkeypatch: Any, clocks: _ShiftedTime
) -> None:
    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    with execution_lease("task:1", worker_id="w-1", ttl_ms=2000) as lease:
        lease.check()
        clocks.offset = 60.0
        with pytest.raises(LeaseLostError):
            lease.check()
    assert client.renewals == []


def test_lease_lost_error_without_a_result() -> None:
    err = LeaseLostError("gone")
    assert not err.has_result and err.result is None
    assert LeaseLostError("gone", result=None).has_result


def test_check_raises_while_another_thread_is_still_declaring_the_loss(
    clocks: _ShiftedTime,
) -> None:
    # The heartbeat thread (here, another thread asking) finds the deadline
    # passed and is slow to finish declaring it. A check() meanwhile must not
    # pass.
    import logging

    class Slow(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            time.sleep(0.5)

    slow = Slow()
    log = logging.getLogger("cellaflow.lease")
    log.addHandler(slow)
    client = ScriptedClient()
    outcome: List[str] = []
    try:

        @tool(lease_ttl_ms=2000)
        def charge_card() -> str:
            lease = current_lease()
            clocks.offset = 60.0
            other = threading.Thread(target=lambda: lease.is_lost)
            other.start()
            time.sleep(0.05)  # the other thread is inside the log call now
            try:
                lease.check()
                outcome.append("sent")
            except LeaseLostError:
                outcome.append("refused")
            other.join()
            return "done"

        with session(client).bind():
            charge_card()
    finally:
        log.removeHandler(slow)
    assert outcome == ["refused"]


def test_an_execution_lease_is_released_not_lost_once_its_block_ends(
    monkeypatch: Any, clocks: _ShiftedTime
) -> None:
    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    notified: List[str] = []
    with execution_lease(
        "task:1",
        worker_id="w-1",
        ttl_ms=2000,
        on_lease_lost=lambda: notified.append("lost"),
    ) as lease:
        lease.check()
    clocks.offset = 60.0
    assert lease.is_lost
    with pytest.raises(LeaseLostError, match="released"):
        lease.check()
    assert notified == []


@pytest.mark.asyncio
async def test_an_async_execution_lease_does_not_cancel_its_caller_after_the_block(
    monkeypatch: Any, clocks: _ShiftedTime
) -> None:
    from cellaflow import async_execution_lease

    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    async with async_execution_lease("task:1", worker_id="w-1", ttl_ms=2000) as lease:
        pass
    clocks.offset = 60.0
    assert lease.is_lost
    await asyncio.sleep(0.05)  # no cancellation lands here


@pytest.mark.asyncio
async def test_a_check_from_a_worker_thread_cancels_the_caller_on_its_loop(
    monkeypatch: Any, clocks: _ShiftedTime
) -> None:
    from cellaflow import async_execution_lease

    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    asyncio.get_running_loop().set_debug(True)
    with pytest.raises((asyncio.CancelledError, LeaseLostError)):
        async with async_execution_lease(
            "task:1", worker_id="w-1", ttl_ms=2000
        ) as lease:
            clocks.offset = 60.0
            with pytest.raises(LeaseLostError):
                await asyncio.to_thread(lease.check)
            await asyncio.sleep(5)


@pytest.mark.asyncio
async def test_cancel_on_loss_follows_the_deadline_without_a_renewal_answer(
    clocks: _ShiftedTime,
) -> None:
    # No renewal is due for 15 s, so only the block's own watch on the
    # deadline can notice in time.
    client = ScriptedClient(interval=15_000)

    @tool(lease_ttl_ms=60_000)
    async def refund() -> str:
        loop = asyncio.get_running_loop()
        # The deadline passes while the request is in flight, and no renewal
        # is due for another 15 s.
        loop.call_later(0.1, lambda: setattr(clocks, "offset", 120.0))
        async with current_lease().cancel_on_loss():
            await asyncio.sleep(5)
        return "refunded"

    started = time.monotonic()
    with session(client).bind():
        with pytest.raises(LeaseLostError):
            await refund()
    assert time.monotonic() - started < 2


@pytest.mark.asyncio
async def test_cancel_on_loss_raises_at_once_for_a_loss_found_at_entry() -> None:
    client = ScriptedClient()
    reached: List[str] = []

    @tool(lease_ttl_ms=2000)
    async def refund() -> str:
        lease = current_lease()
        # Lost after the entry check passed and before the block registered.
        real_check = lease.check

        def check_then_lose() -> None:
            real_check()
            lease.is_lost = True

        lease.check = check_then_lose  # type: ignore[method-assign]
        async with lease.cancel_on_loss():
            reached.append("inside")
        return "refunded"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            await refund()
    assert reached == []
    await asyncio.sleep(0.05)  # and no cancellation lands later


@pytest.mark.asyncio
async def test_cancel_on_loss_raises_even_when_the_cancellation_is_swallowed() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    client = ScriptedClient(renew=lambda: refused)

    async def stubborn() -> None:
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass  # a library that swallows cancellation

    @tool
    async def refund() -> str:
        async with current_lease().cancel_on_loss():
            await stubborn()
        return "refunded"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            await refund()


@pytest.mark.skipif(
    not hasattr(asyncio.Task, "uncancel"), reason="cancellation counts arrived in 3.11"
)
@pytest.mark.asyncio
async def test_nested_cancel_on_loss_leaves_no_cancellation_behind() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    client = ScriptedClient(renew=lambda: refused)

    @tool
    async def refund() -> str:
        lease = current_lease()
        async with lease.cancel_on_loss():
            async with lease.cancel_on_loss():
                await asyncio.sleep(5)
        return "refunded"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            await refund()
    task = asyncio.current_task()
    assert task is not None
    assert getattr(task, "cancelling")() == 0


@pytest.mark.skipif(
    not hasattr(asyncio.Task, "uncancel"), reason="cancellation counts arrived in 3.11"
)
@pytest.mark.asyncio
async def test_a_cancellation_from_elsewhere_at_the_same_time_still_cancels() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    client = ScriptedClient(renew=lambda: refused)

    @tool
    async def refund() -> str:
        lease = current_lease()
        task = asyncio.current_task()
        assert task is not None

        def both() -> None:
            lease.is_lost = True
            task.cancel()

        asyncio.get_running_loop().call_later(0.05, both)
        async with lease.cancel_on_loss():
            await asyncio.sleep(5)
        return "refunded"

    with session(client).bind():
        running = asyncio.ensure_future(refund())
        with pytest.raises(asyncio.CancelledError):
            await running


def test_a_stopped_heartbeat_no_longer_judges_the_lease(
    clocks: _ShiftedTime,
) -> None:
    notified: List[str] = []
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=500,
        lease_ttl_ms=2000,
        on_lease_lost=lambda: notified.append("lost"),
    )
    hb.stop_sync()
    clocks.offset = 60.0
    assert hb.past_deadline() is False
    assert notified == []


def test_a_renewal_the_engine_shortens_near_its_ceiling_moves_the_deadline_in() -> (
    None
):
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=5000,
        lease_ttl_ms=20_000,
    )
    sent = 1_000.0  # wall seconds
    # Two full renewals, five seconds apart: the second expires later, so the
    # first is known to have been full and becomes the reference.
    assert hb._renewal_duration(sent, int((sent + 20) * 1000)) == 20.0
    assert hb._renewal_duration(sent + 5, int((sent + 25) * 1000)) == 20.0
    # Then the engine grants only up to its ceiling, 8 s after this send.
    shortened = hb._renewal_duration(sent + 10, int((sent + 18) * 1000))
    assert shortened == pytest.approx(8.0)


def test_the_heartbeat_judges_by_what_the_engine_granted_near_its_ceiling() -> None:
    # Renewals keep succeeding, but past its ceiling the engine grants no
    # more: the lease is judged lost by then, not a full TTL later.
    ceiling_ms = int(time.time() * 1000) + 800
    count = [0]

    def renew() -> Any:
        count[0] += 1
        now_ms = int(time.time() * 1000)
        expires = now_ms + 2000 if count[0] <= 2 else min(now_ms + 2000, ceiling_ms)
        return idempotency_pb2.RenewLeaseResponse(
            renewed=True, new_expires_at_ms=expires
        )

    hb = LeaseHeartbeat(
        client=ScriptedClient(renew=renew),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=200,
        lease_ttl_ms=2000,
    )
    hb.start_sync()
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not hb.past_deadline():
            time.sleep(0.05)
        lost_after = time.monotonic() - (deadline - 3)
    finally:
        hb.stop_sync()
    assert hb.lost
    assert lost_after < 1.6, lost_after


def test_a_resend_refused_on_position_is_not_a_takeover() -> None:
    first = _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
    mismatch = _Refused(
        grpc.StatusCode.FAILED_PRECONDITION,
        "Sequence mismatch: expected sequence 3, but request specified 1",
    )
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(kw["sequence"])
        if kw["sequence"] != 1:
            return service_pb2.CommitStepResponse()
        raise first if len(sent) == 1 else mismatch

    client = ScriptedClient(commit=commit, released=lambda key: ":failure:" in key)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(grpc.RpcError) as raised:
            charge_card()
    assert raised.value is first


def test_a_resend_refused_as_committed_elsewhere_is_a_takeover() -> None:
    first = _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
    elsewhere = _Refused(
        grpc.StatusCode.FAILED_PRECONDITION,
        "Idempotency key already committed at sequence 4, but this request "
        "specified sequence 1: one key cannot cover two graph positions",
    )
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        raise first if len(sent) == 1 else elsewhere

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert lost.value.result == "refund-123"


def test_a_call_that_succeeds_through_the_resend_is_not_marked_lost() -> None:
    completed = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_COMPLETED
    )
    state = {"committed": False}
    sent: List[int] = []
    seen: List[LeaseHandle] = []

    def renew() -> Any:
        if state["committed"]:
            return completed
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        state["committed"] = True
        if len(sent) == 1:
            time.sleep(0.5)  # a renewal is refused because of this commit
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(renew=renew, commit=commit, released=False)

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "charged"

    with session(client).bind():
        assert charge_card() == "charged"
    assert client.renewals
    assert not seen[0]._lost


def test_a_body_raising_after_a_takeover_is_recorded_where_it_ran() -> None:
    # A resumed @workflow replays by position, so the failure stays at the
    # position the run saw it, and the next step lands after it.
    client = ScriptedClient(released=lambda key: "charge_card" not in key)

    @tool
    def charge_card() -> str:
        raise LeaseLostError("taken over")

    @tool
    def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            charge_card()
        nxt()
    assert [(c["sequence"], c["name"]) for c in client.commits] == [
        (1, "charge_card"),
        (2, "nxt"),
    ]
    assert client.commits[0]["status"] == common_pb2.StepStatus.STEP_STATUS_FAILED


@pytest.mark.asyncio
async def test_an_async_body_raising_after_a_takeover_is_recorded_where_it_ran() -> (
    None
):
    client = ScriptedClient(released=lambda key: "charge_card" not in key)

    @tool
    async def charge_card() -> str:
        raise LeaseLostError("taken over")

    @tool
    async def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            await charge_card()
        await nxt()
    assert [(c["sequence"], c["name"]) for c in client.commits] == [
        (1, "charge_card"),
        (2, "nxt"),
    ]


def test_calling_again_after_a_takeover_takes_the_other_workers_result() -> None:
    client = ScriptedClient(released=lambda key: ":failure:" in key)
    runs: List[str] = []

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order"])
    def refund(order: str) -> str:
        runs.append(order)
        raise LeaseLostError("taken over")

    hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        current_sequence=1,
        cached_result=common_pb2.StepResult(
            output_payload=serialize({"result": "from-new-holder"})
        ),
    )
    ctx = session(client)
    with ctx.bind():
        with pytest.raises(LeaseLostError):
            refund("4417")
        # Next: the lease for the failure record at 1, then the call again,
        # answered with the other worker's result.
        client.answers = [
            idempotency_pb2.CheckCacheResponse(
                status=idempotency_pb2.CACHE_STATUS_ACQUIRED, fencing_token=9
            ),
            hit,
        ]
        assert refund("4417") == "from-new-holder"
    assert runs == ["4417"]
    # The failure at 1; the other worker's result is the call's at 2, recorded
    # there when the run's next step starts.
    assert [(c["sequence"], c["name"], c["status"]) for c in client.commits] == [
        (1, "refund", common_pb2.StepStatus.STEP_STATUS_FAILED),
    ]
    assert ctx._pending_hit is not None and ctx._pending_hit["sequence"] == 2


def test_renewals_go_on_after_the_clock_judges_the_lease_lost(
    clocks: _ShiftedTime,
) -> None:
    # The engine may still hold the lease; a body that does not check keeps it
    # for as long as the engine agrees, so its result can be recorded.
    client = ScriptedClient()

    @tool(lease_ttl_ms=1000)
    def charge_card() -> str:
        clocks.offset = 60.0  # judged lost at the next heartbeat
        before = len(client.renewals)
        time.sleep(0.8)
        return f"{len(client.renewals) - before}"

    with session(client).bind():
        renewed_after = int(charge_card())
    assert renewed_after >= 2
    assert len(client.commits) == 1


def test_an_interrupt_during_a_commit_is_not_swallowed_by_a_resend() -> None:
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) == 1:
            raise KeyboardInterrupt()
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(KeyboardInterrupt):
            charge_card()
    assert sent == [1]


@pytest.mark.asyncio
async def test_an_async_commit_whose_answer_was_lost_is_resent() -> None:
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(kw["sequence"])
        if len(sent) == 1:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit, released=False)

    @tool
    async def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        assert await charge_card() == "refund-123"
    assert sent == [1, 1]


def test_a_first_renewal_the_engine_already_shortens_is_noticed() -> None:
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=5000,
        lease_ttl_ms=20_000,
        acquired_sent_at=(time.monotonic(), 1_000.0),
        acquired_expires_at_ms=1_020_000,
    )
    # The first renewal expires later than the grant, so the grant is the
    # reference; the second is cut short to the ceiling.
    assert hb._renewal_duration(1_005.0, 1_025_000) == 20.0
    assert hb._renewal_duration(1_010.0, 1_018_000) == pytest.approx(8.0)


def test_the_deadline_follows_the_wall_clock_too(monkeypatch: Any) -> None:
    # A suspended host: the monotonic clock may stand still while the wall
    # clock, like the engine's, runs on.
    class WallOnly:
        def monotonic(self) -> float:
            return time.monotonic()

        def time(self) -> float:
            return time.time() + 60.0

    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=500,
        lease_ttl_ms=2000,
    )
    assert hb.past_deadline() is False
    monkeypatch.setattr("cellaflow.lease.time", WallOnly())
    assert hb.past_deadline() is True


def test_the_deadline_counts_from_the_send_less_half_an_interval(
    clocks: _ShiftedTime,
) -> None:
    sent = (time.monotonic() - 1.0, time.time() - 1.0)
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=1000,
        lease_ttl_ms=4000,
        acquired_sent_at=sent,
    )
    # Sent a second ago: 4 s, less a 0.5 s margin, ends 2.5 s from now.
    clocks.offset = 2.4
    assert hb.past_deadline() is False
    clocks.offset = 2.6
    assert hb.past_deadline() is True


def test_the_loss_is_reported_once(clocks: _ShiftedTime) -> None:
    notified: List[str] = []
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=500,
        lease_ttl_ms=2000,
        on_lease_lost=lambda: notified.append("lost"),
    )
    clocks.offset = 60.0
    assert hb.past_deadline() and hb.past_deadline()
    assert notified == ["lost"]


def test_the_handle_reads_released_while_the_commit_is_on_its_way() -> None:
    seen: List[bool] = []
    holder: List[LeaseHandle] = []

    def commit(kw: Dict[str, Any]) -> Any:
        seen.append(holder[0].is_lost)
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        holder.append(current_lease())
        return "charged"

    with session(client).bind():
        charge_card()
    assert seen == [True]
    assert not holder[0]._lost


def test_a_late_loss_after_an_execution_lease_block_is_ignored(
    monkeypatch: Any,
) -> None:
    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    real = LeaseHeartbeat
    built: List[LeaseHeartbeat] = []

    def capture(**kw: Any) -> LeaseHeartbeat:
        built.append(real(**kw))
        return built[-1]

    monkeypatch.setattr("cellaflow.execution.LeaseHeartbeat", capture)
    notified: List[str] = []
    with execution_lease(
        "task:1",
        worker_id="w-1",
        ttl_ms=2000,
        on_lease_lost=lambda: notified.append("lost"),
    ) as lease:
        pass
    # A renewal refused after the block ended reaches the heartbeat's callback.
    assert built[0].on_lease_lost is not None
    built[0].on_lease_lost()
    assert notified == []
    assert not lease._lost


def test_a_slow_renewal_counts_from_when_it_was_sent() -> None:
    # The engine counts its extension from when the renewal arrived, never
    # earlier than it was sent, so the deadline counts from the send: a renewal
    # answered 0.6 s later moves it less than its answer time would suggest.
    def renew() -> Any:
        time.sleep(0.6)
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    hb = LeaseHeartbeat(
        client=ScriptedClient(renew=renew),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=250,
        lease_ttl_ms=1000,
    )
    started = time.monotonic()
    hb.start_sync()
    try:
        # First renewal sent at about 0.15 s and answered at 0.75 s: held until
        # about 1.03 s counted from the send, 1.63 s from the answer. The next
        # is answered at about 1.5 s.
        time.sleep(max(0.0, 1.3 - (time.monotonic() - started)))
        lost = hb.past_deadline()
    finally:
        hb.stop_sync()
    assert lost


def test_an_unanswered_commit_is_resent_before_the_lease_is_released() -> None:
    # While the lease is still the call's, the resend is recorded; releasing
    # first would throw the result away, and a retry would run the body again.
    sent: List[int] = []
    runs: List[str] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) == 1:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        runs.append("charged")
        return "refund-123"

    with session(client).bind():
        assert charge_card() == "refund-123"
    assert sent == [1, 1] and runs == ["charged"]
    assert client.releases == []


def test_every_commit_attempt_sends_what_the_body_returned() -> None:
    sent: List[bytes] = []
    result = {"refund": "r-1"}

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(serialize(kw["output_payload"]))  # as sent, at the time
        if len(sent) == 1:
            result["refund"] = "changed by work the body left running"
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> Dict[str, str]:
        return result

    with session(client).bind():
        charge_card()
    assert sent[0] == sent[1] == serialize({"result": {"refund": "r-1"}})


def test_a_resend_refused_as_stale_names_that_refusal_as_its_cause() -> None:
    first = _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
    stale = _Refused(grpc.StatusCode.FAILED_PRECONDITION, "Fenced out: stale lease")
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        raise first if len(sent) == 1 else stale

    client = ScriptedClient(commit=commit)
    seen: List[LeaseHandle] = []

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "refund-123"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert lost.value.__cause__ is stale
    assert client.releases == []
    # A definite takeover reads as lost, not merely released.
    assert seen[0]._lost


def test_lease_lost_errors_result_is_not_kept_in_the_history() -> None:
    from cellaflow.failures import capture, rebuild

    err = LeaseLostError("taken over", result="x" * 100_000)
    recorded = capture(err)
    assert "x" * 1000 not in repr(recorded)
    replayed = rebuild(recorded, "refund", 1)
    assert isinstance(replayed, LeaseLostError)
    assert replayed.has_result is False and replayed.result is None


def test_the_heartbeat_reports_a_passed_deadline_on_its_own() -> None:
    # Nobody asks: the heartbeat's own tick notices, before renewing.
    notified: List[str] = []
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=200,
        lease_ttl_ms=1000,
        acquired_sent_at=(time.monotonic() - 5.0, time.time() - 5.0),
        on_lease_lost=lambda: notified.append("lost"),
    )
    hb.start_sync()
    try:
        deadline = time.monotonic() + 2
        while not notified and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        hb.stop_sync()
    assert notified == ["lost"]


@pytest.mark.asyncio
async def test_the_async_heartbeat_reports_a_passed_deadline_on_its_own() -> None:
    notified: List[str] = []
    hb = LeaseHeartbeat(
        client=ScriptedClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=200,
        lease_ttl_ms=1000,
        acquired_sent_at=(time.monotonic() - 5.0, time.time() - 5.0),
        on_lease_lost=lambda: notified.append("lost"),
    )
    hb.start_async()
    try:
        for _ in range(100):
            if notified:
                break
            await asyncio.sleep(0.02)
    finally:
        await hb.stop_async()
    assert notified == ["lost"]


def test_a_refused_renewal_keeps_the_lease_lost_however_the_clock_reads() -> None:
    refused = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=SUPERSEDED
    )
    hb = LeaseHeartbeat(
        client=ScriptedClient(renew=lambda: refused),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=200,
        lease_ttl_ms=60_000,
    )
    hb.start_sync()
    try:
        deadline = time.monotonic() + 2
        while not hb.lost and time.monotonic() < deadline:
            time.sleep(0.02)
    finally:
        hb.stop_sync()
    assert hb.past_deadline() is True


def test_the_step_passes_the_grant_and_its_send_time_to_the_heartbeat(
    monkeypatch: Any,
) -> None:
    built: List[Dict[str, Any]] = []
    real = LeaseHeartbeat

    def capture_kw(**kw: Any) -> LeaseHeartbeat:
        built.append(kw)
        return real(**kw)

    monkeypatch.setattr("cellaflow.decorators.LeaseHeartbeat", capture_kw)
    grant = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=41,
        heartbeat_interval_ms=250,
        lease_expires_at_ms=1_234_567,
    )
    client = ScriptedClient([grant])
    before = time.time()

    @tool
    def charge_card() -> str:
        return "charged"

    with session(client).bind():
        charge_card()
    kw = built[0]
    assert kw["acquired_expires_at_ms"] == 1_234_567
    assert before <= kw["acquired_sent_at"][1] <= time.time()


def test_execution_leases_pass_the_grant_to_the_heartbeat(monkeypatch: Any) -> None:
    built: List[Dict[str, Any]] = []
    real = LeaseHeartbeat

    def capture_kw(**kw: Any) -> LeaseHeartbeat:
        built.append(kw)
        return real(**kw)

    monkeypatch.setattr("cellaflow.execution.LeaseHeartbeat", capture_kw)
    grant = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=41,
        heartbeat_interval_ms=250,
        lease_expires_at_ms=7_654_321,
    )
    client = ScriptedClient([grant])
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    with execution_lease("task:1", worker_id="w-1", ttl_ms=2000):
        pass
    assert built[0]["acquired_expires_at_ms"] == 7_654_321
    assert built[0]["acquired_sent_at"] is not None


@pytest.mark.asyncio
async def test_a_late_loss_after_an_async_execution_lease_block_does_not_cancel(
    monkeypatch: Any,
) -> None:
    from cellaflow import async_execution_lease

    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    real = LeaseHeartbeat
    built: List[LeaseHeartbeat] = []

    def capture_hb(**kw: Any) -> LeaseHeartbeat:
        built.append(real(**kw))
        return built[-1]

    monkeypatch.setattr("cellaflow.execution.LeaseHeartbeat", capture_hb)
    async with async_execution_lease("task:1", worker_id="w-1", ttl_ms=2000):
        pass
    assert built[0].on_lease_lost is not None
    built[0].on_lease_lost()
    await asyncio.sleep(0.05)  # no cancellation lands in unrelated work


@pytest.mark.asyncio
async def test_a_loss_after_cancel_on_loss_has_exited_cancels_nothing() -> None:
    client = ScriptedClient()
    holder: List[LeaseHandle] = []

    @tool
    async def refund() -> str:
        lease = current_lease()
        holder.append(lease)
        async with lease.cancel_on_loss():
            await asyncio.sleep(0)
        # Exited: a loss now must not cancel this task later.
        lease.is_lost = True
        await asyncio.sleep(0.05)
        return "refunded"

    with session(client).bind():
        assert await refund() == "refunded"


def test_a_takeover_found_after_a_refused_release_names_that_refusal() -> None:
    stale = _Refused(grpc.StatusCode.FAILED_PRECONDITION, "Fenced out: stale lease")
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        # The commit and its first resend go unanswered; the resend after the
        # refused release is refused as stale.
        if len(sent) <= 2:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        raise stale

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        return "refund-123"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert len(sent) == 3
    assert lost.value.__cause__ is stale


@pytest.mark.asyncio
async def test_a_loss_as_cancel_on_loss_exits_cancels_nothing_later() -> None:
    client = ScriptedClient()

    @tool
    async def refund() -> str:
        lease = current_lease()
        async with lease.cancel_on_loss():
            # Lost, and the block exits before the loop gets to the
            # cancellation that was scheduled for it.
            lease.is_lost = True
        await asyncio.sleep(0.05)  # unrelated work: must not be cancelled
        return "refunded"

    with session(client).bind():
        assert await refund() == "refunded"


def test_a_result_that_cannot_be_recorded_fails_before_anything_is_sent() -> None:
    # Even when the engine will not release the lease, nothing is committed in
    # place of the result.
    client = ScriptedClient(released=False)

    @tool
    def charge_card() -> Any:
        return (1, 2)  # a tuple cannot be recorded

    with session(client).bind():
        with pytest.raises(TypeError):
            charge_card()
    assert client.commits == []


def test_a_refused_release_after_a_commit_that_landed_leaves_the_handle_released() -> (
    None
):
    sent: List[int] = []
    seen: List[LeaseHandle] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) <= 2:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit, released=False)

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "charged"

    with session(client).bind():
        assert charge_card() == "charged"
    assert len(sent) == 3
    assert not seen[0]._lost


def test_a_definite_refusal_of_the_commit_is_not_resent() -> None:
    refused = _Refused(grpc.StatusCode.INVALID_ARGUMENT, "payload too large")

    def commit(kw: Dict[str, Any]) -> Any:
        raise refused

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "charged"

    with session(client).bind():
        with pytest.raises(grpc.RpcError):
            charge_card()
    assert len([c for c in client.commits if c["name"] == "charge_card"]) == 1


def test_a_commit_failing_without_a_grpc_status_is_resent() -> None:
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) == 1:
            raise ConnectionResetError("socket closed")
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "charged"

    with session(client).bind():
        assert charge_card() == "charged"
    assert len(sent) == 2 and client.releases == []


def test_a_key_recorded_elsewhere_at_the_first_commit_is_a_takeover_at_once() -> None:
    elsewhere = _Refused(
        grpc.StatusCode.FAILED_PRECONDITION,
        "Idempotency key already committed at sequence 4, but this request "
        "specified sequence 1: one key cannot cover two graph positions",
    )

    def commit(kw: Dict[str, Any]) -> Any:
        raise elsewhere

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "charged"

    with session(client).bind():
        with pytest.raises(LeaseLostError) as lost:
            charge_card()
    assert lost.value.__cause__ is elsewhere
    assert len(client.commits) == 1 and client.releases == []


def test_a_key_recorded_elsewhere_at_the_early_resend_is_a_takeover_at_once() -> None:
    elsewhere = _Refused(
        grpc.StatusCode.FAILED_PRECONDITION,
        "Idempotency key already committed at sequence 4, but this request "
        "specified sequence 1: one key cannot cover two graph positions",
    )
    sent: List[int] = []

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) == 1:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        raise elsewhere

    client = ScriptedClient(commit=commit)

    @tool
    def charge_card() -> str:
        return "charged"

    with session(client).bind():
        with pytest.raises(LeaseLostError):
            charge_card()
    assert len(sent) == 2 and client.releases == []


def test_a_renewal_refused_during_the_early_resend_is_held_back() -> None:
    completed = idempotency_pb2.RenewLeaseResponse(
        renewed=False, failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_COMPLETED
    )
    sent: List[int] = []
    seen: List[LeaseHandle] = []

    def renew() -> Any:
        if sent:
            return completed
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit(kw: Dict[str, Any]) -> Any:
        sent.append(1)
        if len(sent) == 1:
            raise _Refused(grpc.StatusCode.UNAVAILABLE, "connection reset")
        time.sleep(0.5)  # a renewal is refused while the resend is on its way
        return service_pb2.CommitStepResponse()

    client = ScriptedClient(renew=renew, commit=commit)

    @tool
    def charge_card() -> str:
        seen.append(current_lease())
        return "charged"

    with session(client).bind():
        assert charge_card() == "charged"
    assert client.renewals
    assert not seen[0]._lost


def test_a_group_member_recorded_by_type_lists_no_unrecorded_result() -> None:
    from cellaflow.failures import _minimal

    member = _minimal(LeaseLostError("taken over", result="x" * 10), 0)
    assert "result" not in member["unavailable_attrs"]
    assert "has_result" not in member["unavailable_attrs"]


@pytest.mark.asyncio
async def test_work_left_running_in_cancel_on_loss_hears_the_lease_was_released() -> (
    None
):
    client = ScriptedClient()
    outcome: List[str] = []

    @tool
    async def refund() -> str:
        lease = current_lease()

        async def background() -> None:
            try:
                async with lease.cancel_on_loss():
                    await asyncio.sleep(5)
            except LeaseLostError as exc:
                outcome.append(str(exc))

        asyncio.get_running_loop().create_task(background())
        await asyncio.sleep(0)
        return "refunded"

    with session(client).bind():
        assert await refund() == "refunded"
    for _ in range(40):
        if outcome:
            break
        await asyncio.sleep(0.05)
    assert outcome and "released" in outcome[0]
