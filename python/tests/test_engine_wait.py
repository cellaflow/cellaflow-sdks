"""How a step waits behind another worker's lease: in the engine, not by
polling, without holding the event loop, and with a lease granted after a wait
counted from when it was granted. Uses a scripted engine client, so no engine is
needed."""

import asyncio
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import grpc
import pytest

import cellaflow.decorators
import cellaflow.lease
from cellaflow import current_lease, step
from cellaflow.context import WorkflowContext
from cellaflow.decorators import _ENGINE_WAIT_MS
from cellaflow.v1 import idempotency_pb2, service_pb2


class Unavailable(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "engine unavailable"


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
    ) -> None:
        self.resp = resp
        self.waited_s = waited_s
        self.blocks_s = blocks_s
        self.expires_in_ms = expires_in_ms


class WaitClient:
    def __init__(
        self,
        answers: List[Answer],
        clocks: Optional[Clocks] = None,
        fail_renewals: Callable[[int], bool] = lambda token: False,
        fail_releases: bool = False,
    ) -> None:
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

    def check_idempotency_cache(self, **kw: Any) -> Any:
        key = kw["idempotency_key"]
        if ":record:" in key or ":failure:" in key:
            self.record_acquires.append(kw)
            self.note("record-acquire")
            return acquired(99)
        self.acquires.append(kw)
        self.note("acquire")
        answer = self.answers.pop(0)
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

    def renew_lease(self, **kw: Any) -> Any:
        self.note(f"renew:{kw['fencing_token']}")
        if self.fail_renewals(kw["fencing_token"]):
            raise Unavailable()
        now = self.clocks.time() if self.clocks else time.time()
        return idempotency_pb2.RenewLeaseResponse(
            renewed=True, new_expires_at_ms=int(now * 1000) + kw["extend_ms"]
        )

    def commit_step(self, **kw: Any) -> Any:
        self.note("commit")
        return service_pb2.CommitStepResponse()

    def release_lease(self, **kw: Any) -> Any:
        self.releases.append(kw)
        self.note(f"release:{kw['fencing_token']}")
        if self.fail_releases:
            raise Unavailable()
        return idempotency_pb2.ReleaseLeaseResponse(released=True)

    def close(self) -> None:
        pass


def session(client: WaitClient) -> WorkflowContext:
    return WorkflowContext(
        client=client,  # type: ignore[arg-type]
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
    client = WaitClient([Answer(acquired())])
    tokens: List[int] = []
    charge = step(body_that_checks(client, tokens))
    with session(client).bind():
        charge()
    assert client.order == ["acquire", "body", "commit"]


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
async def test_a_call_cancelled_while_waiting_hands_back_a_later_grant() -> None:
    # The engine grants the lease after the caller gave up. Nothing will run the
    # body under it, so it is released rather than left to lapse.
    client = WaitClient([Answer(acquired(9), blocks_s=0.3)])

    async def charge_card() -> str:
        return "charged"

    charge = step(charge_card)
    with session(client).bind():
        task = asyncio.ensure_future(charge())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    deadline = time.monotonic() + 2.0
    while not client.releases and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert client.releases == [
        {
            "agent_id": client.releases[0]["agent_id"],
            "idempotency_key": client.acquires[0]["idempotency_key"],
            "fencing_token": 9,
            "reason": "CANCELLED",
        }
    ]
    assert "body" not in client.order


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
