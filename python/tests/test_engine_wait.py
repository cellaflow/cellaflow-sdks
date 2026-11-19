"""How a step waits behind another worker's lease: in the engine, not by
polling, without holding the event loop, and with a lease granted after a wait
counted from when it was granted. Uses a scripted engine client, so no engine is
needed."""

import asyncio
import concurrent.futures
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import grpc
import pytest

import cellaflow.decorators
import cellaflow.lease
from cellaflow import current_lease, step
from cellaflow.client import CellaflowClient
from cellaflow.context import WorkflowContext
from cellaflow.decorators import _ENGINE_WAIT_MS
from cellaflow.lease import LeaseHeartbeat
from cellaflow.v1 import idempotency_pb2, service_pb2


class Unavailable(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "engine unavailable"


class DeadlineExceeded(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.DEADLINE_EXCEEDED

    def details(self) -> str:
        return "deadline exceeded"


class Refused(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self) -> str:
        return "the position is claimed by another lease"


class Clocks:
    """The `time` module as the SDK sees it, with both clocks moved forward by
    `advance`, so a scripted engine can answer as if it had waited without the
    test waiting. Sleeps are real."""

    def __init__(self) -> None:
        self.offset = 0.0

    def monotonic(self) -> float:
        return time.monotonic() + self.offset

    def time(self) -> float:
        return time.time() + self.offset

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def advance(self, seconds: float) -> None:
        self.offset += seconds


@pytest.fixture
def clocks(monkeypatch: pytest.MonkeyPatch) -> Clocks:
    fake = Clocks()
    monkeypatch.setattr(cellaflow.decorators, "time", fake)
    monkeypatch.setattr(cellaflow.lease, "time", fake)
    return fake


class Answer:
    """One scripted answer to an acquisition: the response, the time the engine
    spends waiting before it on the faked clocks, real time spent blocking, and
    how long the grant has left when it answers (`lease_expires_at_ms`, on the
    faked clocks). A grant made a little before its answer is handled has a
    little less than its TTL left, so a renewal sent after it expires strictly
    later, as on a real engine."""

    def __init__(
        self,
        resp: Any,
        waited_s: float = 0.0,
        blocks_s: float = 0.0,
        expires_in_ms: Optional[int] = None,
        raises: Optional[BaseException] = None,
    ) -> None:
        self.raises = raises
        self.resp = resp
        self.waited_s = waited_s
        self.blocks_s = blocks_s
        self.expires_in_ms = expires_in_ms


class Request(grpc.Future):  # type: ignore[misc]
    """A scripted request as a `grpc.Future`, which is what an async step waits
    on without a thread."""

    def __init__(self, future: "concurrent.futures.Future[Any]") -> None:
        self._future = future

    def cancel(self) -> bool:
        return self._future.cancel()

    def cancelled(self) -> bool:
        return self._future.cancelled()

    def running(self) -> bool:
        return self._future.running()

    def done(self) -> bool:
        return self._future.done()

    def result(self, timeout: Optional[float] = None) -> Any:
        return self._future.result(timeout)

    def exception(self, timeout: Optional[float] = None) -> Any:
        return self._future.exception(timeout)

    def traceback(self, timeout: Optional[float] = None) -> Any:
        return None

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        self._future.add_done_callback(lambda _: fn(self))


class WaitClient(CellaflowClient):
    """A scripted engine. Async steps wait on its `check_idempotency_cache_future`
    as they do on a real client's; nothing of the real client is set up."""

    def __init__(
        self,
        answers: List[Answer],
        clocks: Optional[Clocks] = None,
        fail_renewals: Callable[[int], bool] = lambda token: False,
        fail_releases: bool = False,
        record_answers: Optional[List[Answer]] = None,
        renew_blocks_s: float = 0.0,
        refuse_renewals: Callable[[int], bool] = lambda token: False,
    ) -> None:
        self.requests: List[Any] = []
        self.record_answers = list(record_answers or [])
        self.renew_blocks_s = renew_blocks_s
        self.refuse_renewals = refuse_renewals
        self.answers = list(answers)
        self.clocks = clocks
        self.fail_renewals = fail_renewals
        self.fail_releases = fail_releases
        self.acquires: List[Dict[str, Any]] = []
        self.record_acquires: List[Dict[str, Any]] = []
        self.releases: List[Dict[str, Any]] = []
        self.order: List[str] = []
        self._lock = threading.Lock()

    def note(self, event: str) -> None:
        with self._lock:
            self.order.append(event)

    def check_idempotency_cache(self, **kw: Any) -> Any:  # type: ignore[override]
        key = kw["idempotency_key"]
        if ":record:" in key or ":failure:" in key:
            self.record_acquires.append(kw)
            self.note("record-acquire")
            if self.record_answers:
                record = self.record_answers.pop(0)
                if record.raises is not None:
                    raise record.raises
                return record.resp
            return acquired(99)
        self.acquires.append(kw)
        self.note("acquire")
        answer = self.answers.pop(0)
        if answer.raises is not None:
            raise answer.raises
        if answer.blocks_s:
            time.sleep(answer.blocks_s)
        if answer.waited_s:
            assert self.clocks is not None
            self.clocks.advance(answer.waited_s)
        resp = idempotency_pb2.CheckCacheResponse()
        resp.CopyFrom(answer.resp)
        if answer.expires_in_ms is not None:
            now = self.clocks.time() if self.clocks else time.time()
            resp.lease_expires_at_ms = int(now * 1000) + answer.expires_in_ms
        return resp

    def check_idempotency_cache_future(  # type: ignore[override]
        self, **kw: Any
    ) -> Any:
        future: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
        self.requests.append(future)

        def answer() -> None:
            try:
                resp = self.check_idempotency_cache(**kw)
            except BaseException as exc:
                if not future.cancelled():
                    future.set_exception(exc)
                return
            if not future.cancelled():
                future.set_result(resp)

        threading.Thread(target=answer, daemon=True).start()
        return Request(future)

    def get_graph(self, **kw: Any) -> Any:  # type: ignore[override]
        # Nothing committed: the scripted engine keeps no history.
        return [], None

    def renew_lease(self, **kw: Any) -> Any:  # type: ignore[override]
        self.note(f"renew:{kw['fencing_token']}")
        if self.renew_blocks_s:
            time.sleep(self.renew_blocks_s)
        if self.fail_renewals(kw["fencing_token"]):
            raise Unavailable()
        if self.refuse_renewals(kw["fencing_token"]):
            return idempotency_pb2.RenewLeaseResponse(renewed=False)
        now = self.clocks.time() if self.clocks else time.time()
        return idempotency_pb2.RenewLeaseResponse(
            renewed=True, new_expires_at_ms=int(now * 1000) + kw["extend_ms"]
        )

    def commit_step(self, **kw: Any) -> Any:  # type: ignore[override]
        self.note(f"commit:{kw['sequence']}:{kw['name']}")
        return service_pb2.CommitStepResponse()

    def release_lease(self, **kw: Any) -> Any:  # type: ignore[override]
        self.releases.append(kw)
        self.note(f"release:{kw['fencing_token']}")
        if self.fail_releases:
            raise Unavailable()
        return idempotency_pb2.ReleaseLeaseResponse(released=True)

    def close(self) -> None:
        pass


async def until(condition: Callable[[], bool], within_s: float = 5.0) -> None:
    """Waits for a condition rather than a fixed time, so a slow runner does
    not move what the test interrupts."""
    deadline = time.monotonic() + within_s
    while not condition():
        assert time.monotonic() < deadline, "the condition never held"
        await asyncio.sleep(0.005)


def session(client: WaitClient) -> WorkflowContext:
    return WorkflowContext(
        client=client,
        session_id="s-wait",
        workflow_version="1.0.0",
        coordination_id="order-4417",
        history_known=True,
    )


def in_progress(retry_after_ms: int = 1000) -> Any:
    return idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS, retry_after_ms=retry_after_ms
    )


def acquired(token: int = 7, interval_ms: int = 60_000) -> Any:
    return idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=token,
        heartbeat_interval_ms=interval_ms,
    )


def body_that_checks(client: WaitClient, tokens: List[int]) -> Callable[[], str]:
    def charge_card() -> str:
        client.note("body")
        lease = current_lease()
        lease.check()
        tokens.append(lease.fencing_token)
        return "charged"

    return charge_card


def test_a_waiting_step_asks_the_engine_to_wait_and_again_at_once_after_a_wait(
    clocks: Clocks,
) -> None:
    waited = _ENGINE_WAIT_MS / 1000.0
    client = WaitClient(
        [
            Answer(in_progress(), waited_s=waited),
            Answer(in_progress(), waited_s=waited),
            Answer(in_progress(), waited_s=waited),
            Answer(acquired()),
        ],
        clocks,
    )
    charge = step(lambda: "charged", tool_name="charge_card")
    started = time.monotonic()
    with session(client).bind():
        assert charge() == "charged"
    # Sleeping the engine's retry_after_ms between the three would take 3 s.
    assert time.monotonic() - started < 1.0
    assert _ENGINE_WAIT_MS > 0
    assert [a["wait_timeout_ms"] for a in client.acquires] == [_ENGINE_WAIT_MS] * 4
    # Each waiting request carries a deadline past the wait, so a connection
    # that dies while the engine holds it fails the call instead of hanging.
    assert all(a["timeout"] * 1000 > _ENGINE_WAIT_MS for a in client.acquires)


@pytest.mark.asyncio
async def test_an_async_waiting_step_asks_the_engine_to_wait_and_asks_again_at_once(
    clocks: Clocks,
) -> None:
    waited = _ENGINE_WAIT_MS / 1000.0
    client = WaitClient(
        [
            Answer(in_progress(), waited_s=waited),
            Answer(in_progress(), waited_s=waited),
            Answer(acquired()),
        ],
        clocks,
    )

    async def charge_card() -> str:
        return "charged"

    charge = step(charge_card)
    started = time.monotonic()
    with session(client).bind():
        assert await charge() == "charged"
    assert time.monotonic() - started < 1.0
    assert [a["wait_timeout_ms"] for a in client.acquires] == [_ENGINE_WAIT_MS] * 3
    assert all(a["timeout"] * 1000 > _ENGINE_WAIT_MS for a in client.acquires)


def test_an_engine_that_answers_at_once_is_backed_off_from() -> None:
    client = WaitClient(
        [Answer(in_progress(100)), Answer(in_progress(100)), Answer(acquired())]
    )
    charge = step(lambda: "charged", tool_name="charge_card")
    started = time.monotonic()
    with session(client).bind():
        charge()
    assert len(client.acquires) == 3
    assert time.monotonic() - started >= 0.19


@pytest.mark.asyncio
async def test_an_async_engine_that_answers_at_once_is_backed_off_from() -> None:
    client = WaitClient(
        [Answer(in_progress(100)), Answer(in_progress(100)), Answer(acquired())]
    )

    async def charge_card() -> str:
        return "charged"

    charge = step(charge_card)
    started = time.monotonic()
    with session(client).bind():
        await charge()
    assert time.monotonic() - started >= 0.19


@pytest.mark.asyncio
async def test_an_async_step_waiting_in_the_engine_leaves_the_event_loop_free() -> None:
    # The engine holds the request for half a second. Called on the loop, that
    # would stop every other coroutine for as long.
    client = WaitClient([Answer(acquired(), blocks_s=0.5)])
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    async def charge_card() -> str:
        return "charged"

    charge = step(charge_card)
    tick = asyncio.ensure_future(ticker())
    try:
        with session(client).bind():
            assert await charge() == "charged"
    finally:
        tick.cancel()
    assert ticks >= 20, f"the loop ran {ticks} ticks during a 0.5 s wait"


def test_a_lease_granted_at_once_costs_no_extra_request_before_the_body() -> None:
    # A 750 ms interval: a grant is late after 375 ms, so a threshold far off
    # that would show here.
    client = WaitClient([Answer(acquired(7, 750))])
    tokens: List[int] = []
    charge = step(body_that_checks(client, tokens), lease_ttl_ms=3000)
    with session(client).bind():
        charge()
    assert client.order == ["acquire", "body", "commit:1:charge_card"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_lease_granted_after_a_long_wait_is_counted_from_a_renewal(
    clocks: Clocks, asynchronous: bool
) -> None:
    # The engine waited 10 s, longer than the 3 s TTL, before granting. Counted
    # from the request, the lease would be read as lapsed on arrival.
    client = WaitClient(
        [Answer(acquired(7, 750), waited_s=10.0, expires_in_ms=2900)], clocks
    )
    tokens: List[int] = []
    body = body_that_checks(client, tokens)
    with session(client).bind():
        if asynchronous:

            async def charge_card() -> str:
                return body()

            assert await step(charge_card, lease_ttl_ms=3000)() == "charged"
        else:
            charge = step(body, lease_ttl_ms=3000)
            assert charge() == "charged"
    assert tokens == [7]
    assert client.order[:3] == ["acquire", "renew:7", "body"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_late_grant_no_renewal_confirms_is_handed_back_before_the_body(
    clocks: Clocks, asynchronous: bool
) -> None:
    # The grant came 10 s after a request with a 3 s TTL, and the renewal sent
    # to count it from a known time fails: nothing shows it is still held, so
    # the body must not start under it. The call hands it back and asks again.
    client = WaitClient(
        [
            Answer(acquired(7, 750), waited_s=10.0, expires_in_ms=2900),
            Answer(acquired(8, 750), expires_in_ms=3000),
        ],
        clocks,
        fail_renewals=lambda token: token == 7,
    )
    tokens: List[int] = []
    body = body_that_checks(client, tokens)
    with session(client).bind():
        if asynchronous:

            async def charge_card() -> str:
                return body()

            assert await step(charge_card, lease_ttl_ms=3000)() == "charged"
        else:
            assert step(body, lease_ttl_ms=3000)() == "charged"
    assert tokens == [8]
    assert client.releases[0]["fencing_token"] == 7
    assert client.releases[0]["reason"] == "LEASE_UNCONFIRMED"
    body_at = client.order.index("body")
    assert client.order.index("release:7") < body_at
    assert "renew:7" not in client.order[body_at:]


@pytest.mark.asyncio
async def test_a_call_cancelled_while_waiting_lets_its_request_finish() -> None:
    # Cancelling the request could throw away a grant the engine made just
    # before the cancellation reached it, leaving the key held for a TTL. The
    # request finishes instead, and its grant is handed back.
    client = WaitClient([Answer(acquired(9), blocks_s=1.0)])

    async def charge_card() -> str:
        client.note("body")
        return "charged"

    charge = step(charge_card)
    with session(client).bind():
        task = asyncio.ensure_future(charge())
        await until(lambda: bool(client.requests))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not client.requests[0].cancelled()
    await until(lambda: bool(client.releases))
    assert [(r["fencing_token"], r["reason"]) for r in client.releases] == [
        (9, "CANCELLED")
    ]
    assert "body" not in client.order


def test_a_grant_that_answers_after_the_loop_closed_is_handed_back() -> None:
    # asyncio.run cancels the waiting call and closes its loop before the
    # engine answers: the grant is handed back from the answering thread.
    client = WaitClient([Answer(acquired(9), blocks_s=0.3)])

    async def charge_card() -> str:
        return "charged"

    async def main() -> None:
        asyncio.ensure_future(step(charge_card)())
        await until(lambda: bool(client.requests))

    with session(client).bind():
        asyncio.run(main())
    deadline = time.monotonic() + 2.0
    while not client.releases and time.monotonic() < deadline:
        time.sleep(0.02)
    assert [(r["fencing_token"], r["reason"]) for r in client.releases] == [
        (9, "CANCELLED")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_late_grant_whose_renewals_fail_inside_its_deadline_runs_the_body(
    clocks: Clocks, asynchronous: bool
) -> None:
    # A 1 s wait leaves most of a 3 s lease, counted from the request, so the
    # body runs on it even though no renewal is confirmed.
    client = WaitClient(
        [Answer(acquired(7, 750), waited_s=1.0, expires_in_ms=2900)],
        clocks,
        fail_renewals=lambda token: True,
    )
    tokens: List[int] = []
    body = body_that_checks(client, tokens)
    with session(client).bind():
        if asynchronous:

            async def charge_card() -> str:
                return body()

            assert await step(charge_card, lease_ttl_ms=3000)() == "charged"
        else:
            assert step(body, lease_ttl_ms=3000)() == "charged"
    assert tokens == [7]
    assert client.releases == []


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_step_no_renewal_confirms_fails_after_three_grants_and_frees_the_last(
    clocks: Clocks, asynchronous: bool
) -> None:
    def late(token: int) -> Answer:
        return Answer(acquired(token, 750), waited_s=10.0, expires_in_ms=2900)

    client = WaitClient(
        [late(7), late(8), late(9), Answer(acquired(10))],
        clocks,
        fail_renewals=lambda token: True,
        fail_releases=True,
    )
    ran = False

    def charge_card() -> str:
        nonlocal ran
        ran = True
        return "charged"

    async def charge_card_async() -> str:
        return charge_card()

    def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(RuntimeError, match="could not confirm any of 3 leases"):
            if asynchronous:
                charge = step(
                    charge_card_async, tool_name="charge_card", lease_ttl_ms=3000
                )
                await charge()
            else:
                step(charge_card, lease_ttl_ms=3000)()
        # The failure is recorded at the call's position before the next call
        # claims one, and the lease still claiming it is released first.
        client.note("next")
        step(nxt)()
    assert not ran
    assert [(r["fencing_token"], r["reason"]) for r in client.releases[:3]] == [
        (7, "LEASE_UNCONFIRMED"),
        (8, "LEASE_UNCONFIRMED"),
        (9, "LEASE_UNCONFIRMED"),
    ]
    after = client.order[client.order.index("next"):]
    assert "release:9" in after, after
    assert after.index("release:9") < after.index("record-acquire"), after


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_request_that_gets_no_answer_is_asked_again(asynchronous: bool) -> None:
    client = WaitClient([Answer(None, raises=DeadlineExceeded()), Answer(acquired(8))])
    runs = 0

    def charge_card() -> str:
        nonlocal runs
        runs += 1
        return "charged"

    async def charge_card_async() -> str:
        return charge_card()

    with session(client).bind():
        if asynchronous:
            assert await step(charge_card_async, tool_name="charge_card")() == "charged"
        else:
            assert step(charge_card)() == "charged"
    assert runs == 1
    assert len(client.acquires) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_unanswered_requests_fail_the_call_and_its_record_waits_out_the_lease(
    asynchronous: bool,
) -> None:
    # The engine may have granted the last request: a lease whose token this
    # side never saw, claiming the call's position until it lapses. The failure
    # record there is refused until then, and waits rather than being dropped.
    lost = [Answer(None, raises=DeadlineExceeded()) for _ in range(3)]
    client = WaitClient(
        [*lost, Answer(acquired(9))],
        record_answers=[Answer(None, raises=Refused())],
    )

    def charge_card() -> str:
        return "charged"

    async def charge_card_async() -> str:
        return charge_card()

    def next_step() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(grpc.RpcError):
            if asynchronous:
                charge = step(
                    charge_card_async, tool_name="charge_card", lease_ttl_ms=1000
                )
                await charge()
            else:
                step(charge_card, lease_ttl_ms=1000)()
        assert step(next_step)() == "next"
    assert len(client.acquires) == 4
    assert len(client.record_acquires) == 2
    # A lease without its token cannot be released, only waited out.
    assert client.releases == []
    assert [e for e in client.order if e.startswith("commit")] == [
        "commit:1:charge_card",
        "commit:2:next_step",
    ]


def test_a_renewal_the_engine_refuses_after_a_late_grant_hands_the_lease_back(
    clocks: Clocks,
) -> None:
    client = WaitClient(
        [
            Answer(acquired(7, 750), waited_s=10.0, expires_in_ms=2900),
            Answer(acquired(8)),
        ],
        clocks,
        refuse_renewals=lambda token: token == 7,
    )
    tokens: List[int] = []
    with session(client).bind():
        assert step(body_that_checks(client, tokens), lease_ttl_ms=3000)() == "charged"
    assert tokens == [8]
    # Refused once, not tried again: a refusal is an answer.
    assert client.order.count("renew:7") == 1
    assert client.releases[0]["reason"] == "LEASE_UNCONFIRMED"


def test_confirm_grant_stops_trying_once_the_lease_could_have_lapsed() -> None:
    # Each renewal takes 0.5 s and fails. With a 1 s TTL, after two the lease
    # could have lapsed even counted from when the grant arrived, so there is no
    # third.
    client = WaitClient([], fail_renewals=lambda token: True, renew_blocks_s=0.5)
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=250,
        lease_ttl_ms=1000,
    )
    hb.confirm_grant()
    assert client.order == ["renew:7", "renew:7"]


@pytest.mark.asyncio
async def test_a_call_cancelled_while_its_late_grant_is_confirmed_hands_it_back(
    clocks: Clocks,
) -> None:
    client = WaitClient(
        [Answer(acquired(7, 750), waited_s=10.0, expires_in_ms=2900)],
        clocks,
        renew_blocks_s=0.3,
    )

    async def charge_card() -> str:
        client.note("body")
        return "charged"

    charge = step(charge_card, lease_ttl_ms=3000)
    with session(client).bind():
        task = asyncio.ensure_future(charge())
        await until(lambda: "renew:7" in client.order)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    deadline = time.monotonic() + 2.0
    while not client.releases and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert [(r["fencing_token"], r["reason"]) for r in client.releases] == [
        (7, "CANCELLED")
    ]
    assert "body" not in client.order


def test_only_failures_since_the_last_answer_count_towards_giving_up() -> None:
    # A long wait can meet the odd dropped connection; each answer in between
    # shows the engine is there.
    def lost() -> Answer:
        return Answer(None, raises=Unavailable())

    client = WaitClient(
        [lost(), Answer(in_progress(1)), lost(), Answer(in_progress(1)), lost()]
        + [Answer(acquired())]
    )
    with session(client).bind():
        assert step(lambda: "charged", tool_name="charge_card")() == "charged"
    assert len(client.acquires) == 6


@pytest.mark.asyncio
async def test_a_grant_reaching_the_loop_just_before_a_cancel_is_handed_back() -> None:
    # The answer is handed to the loop, and the cancellation lands before the
    # call resumes to take it: the request can no longer be cancelled.
    client = WaitClient([Answer(acquired(9), blocks_s=1.0)])
    loop = asyncio.get_running_loop()

    async def charge_card() -> str:
        client.note("body")
        return "charged"

    with session(client).bind():
        task = asyncio.ensure_future(step(charge_card)())
        await until(lambda: bool(client.requests))
        client.requests[0].add_done_callback(
            lambda _: loop.call_soon_threadsafe(task.cancel)
        )
        with pytest.raises(asyncio.CancelledError):
            await task
    await until(lambda: bool(client.releases))
    assert [(r["fencing_token"], r["reason"]) for r in client.releases] == [
        (9, "CANCELLED")
    ]
    assert "body" not in client.order


@pytest.mark.asyncio
async def test_a_call_cancelled_after_a_lost_answer_leaves_its_record_waiting() -> None:
    # The unanswered request may have been granted. The cancellation's record
    # at the call's position is refused until that lease lapses, and waits
    # rather than being dropped.
    client = WaitClient(
        [Answer(None, raises=Unavailable()), Answer(acquired(9))],
        record_answers=[Answer(None, raises=Refused())],
    )

    async def charge_card() -> str:
        return "charged"

    async def next_step() -> str:
        return "next"

    with session(client).bind():
        task = asyncio.ensure_future(step(charge_card, lease_ttl_ms=1000)())
        await until(lambda: len(client.acquires) == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await step(next_step)() == "next"
    assert len(client.record_acquires) == 2
    assert [e for e in client.order if e.startswith("commit")] == [
        "commit:1:charge_card",
        "commit:2:next_step",
    ]


def test_confirm_grant_does_nothing_once_told_to_stop() -> None:
    client = WaitClient([])
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=250,
        lease_ttl_ms=1000,
    )
    hb.stop_sync()
    hb.confirm_grant()
    assert client.order == []


@pytest.mark.asyncio
async def test_an_async_grant_answered_at_once_costs_no_extra_request() -> None:
    client = WaitClient([Answer(acquired(7, 750))])

    async def charge_card() -> str:
        client.note("body")
        return "charged"

    with session(client).bind():
        await step(charge_card, lease_ttl_ms=3000)()
    assert client.order == ["acquire", "body", "commit:1:charge_card"]


class Committed(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self) -> str:
        return "Sequence 1 in session 's-wait' is already committed"


def test_a_record_refused_at_a_committed_position_skips_the_lease_wait() -> (
    None
):
    # The lost answer's lease may still claim the position, but a committed
    # position never takes the record whatever claims it: waiting is wasted.
    lost = [Answer(None, raises=Unavailable()) for _ in range(3)]
    client = WaitClient(
        [*lost, Answer(acquired(9))],
        record_answers=[Answer(None, raises=Committed())],
    )

    def charge_card() -> str:
        return "charged"

    def next_step() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(grpc.RpcError):
            step(charge_card, lease_ttl_ms=1000)()
        started = time.monotonic()
        step(next_step)()
        assert time.monotonic() - started < 1.0
    assert len(client.record_acquires) == 1


class BlockingOnly(WaitClient):
    """A client that overrides only the blocking request, as instrumentation or
    a fake built on `CellaflowClient` does."""

    check_idempotency_cache_future = (
        CellaflowClient.check_idempotency_cache_future  # type: ignore[assignment]
    )


@pytest.mark.asyncio
async def test_an_async_step_uses_a_clients_own_blocking_request() -> None:
    # The inherited future would go straight to the stub, bypassing the
    # override (and, here, failing: the stub was never set up).
    client = BlockingOnly([Answer(acquired(7))])

    async def charge_card() -> str:
        return "charged"

    with session(client).bind():
        assert await step(charge_card)() == "charged"
    assert len(client.acquires) == 1
