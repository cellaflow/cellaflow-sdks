"""The lease TTL a step asks for: what is sent on acquisition and on every
renewal, where a default comes from, and what is refused. Uses a scripted engine
client, so no engine is needed."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional

import grpc
import pytest

import cellaflow
from cellaflow import MAX_LEASE_TTL_MS, MIN_LEASE_TTL_MS, durable_tools, step, tool
from cellaflow.context import WorkflowContext
from cellaflow.idempotency import IdempotencyScope
from cellaflow.lease import LeaseHeartbeat
from cellaflow.serialization import serialize
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2


class Unavailable(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "engine unavailable"


class RecordingClient:
    """Records every acquisition and renewal. Acquisitions are answered from
    `answers` in order, then ACQUIRED with a short heartbeat interval so a body
    that sleeps sees several renewals."""

    def __init__(
        self,
        answers: Optional[List[Any]] = None,
        interval: Any = 20,
        fail_release: bool = False,
        down_for_records: bool = False,
    ) -> None:
        self.answers = list(answers or [])
        self.interval = interval
        self.fail_release = fail_release
        self.down_for_records = down_for_records
        self.acquires: List[Dict[str, Any]] = []
        self.renewals: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def check_idempotency_cache(self, **kw: Any) -> Any:
        self.acquires.append(kw)
        if self.down_for_records and ":failure:" in kw["idempotency_key"]:
            raise Unavailable()
        if self.answers:
            return self.answers.pop(0)
        ttl = kw.get("lease_ttl_ms")
        interval = self.interval(ttl) if callable(self.interval) else self.interval
        return idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
            fencing_token=7,
            heartbeat_interval_ms=interval,
        )

    def renew_lease(self, **kw: Any) -> Any:
        with self._lock:
            self.renewals.append(kw)
        return idempotency_pb2.RenewLeaseResponse(renewed=True)

    def commit_step(self, **kw: Any) -> Any:
        return service_pb2.CommitStepResponse()

    def release_lease(self, **kw: Any) -> Any:
        if self.fail_release:
            raise Unavailable()
        return idempotency_pb2.ReleaseLeaseResponse(released=True)

    def close(self) -> None:
        pass

    def renewals_for(self, key: str) -> List[Dict[str, Any]]:
        return [r for r in self.renewals if r["idempotency_key"] == key]


def session(client: RecordingClient, **extra: Any) -> WorkflowContext:
    return WorkflowContext(
        client=client,  # type: ignore[arg-type]
        session_id="s-ttl",
        workflow_version="1.0.0",
        coordination_id="order-4417",
        history_known=True,
        **extra,
    )


def slow(name: str, seconds: float = 0.35, **options: Any) -> Any:
    def body() -> str:
        # Long enough for several renewals at the heartbeat's 0.1 s floor.
        time.sleep(seconds)
        return name

    body.__name__ = name
    return step(body, **options)


def test_a_steps_ttl_is_sent_on_acquisition_and_asked_for_on_every_renewal() -> None:
    client = RecordingClient()
    charge = slow("charge_card", lease_ttl_ms=6000)
    with session(client).bind():
        charge()
    assert client.acquires[0]["lease_ttl_ms"] == 6000
    assert len(client.renewals) >= 2, client.renewals
    assert all(r["extend_ms"] == 6000 for r in client.renewals), client.renewals


@pytest.mark.asyncio
async def test_an_async_steps_ttl_is_sent_and_renewed_the_same_way() -> None:
    client = RecordingClient()

    @tool(lease_ttl_ms=6000)
    async def charge_card() -> str:
        await asyncio.sleep(0.35)
        return "charged"

    with session(client).bind():
        await charge_card()
    assert client.acquires[0]["lease_ttl_ms"] == 6000
    assert len(client.renewals) >= 2, client.renewals
    assert all(r["extend_ms"] == 6000 for r in client.renewals), client.renewals


def test_without_a_ttl_the_engines_default_is_asked_for_as_before() -> None:
    client = RecordingClient()
    charge = slow("charge_card")
    with session(client).bind():
        charge()
    assert client.acquires[0]["lease_ttl_ms"] is None
    # The heartbeat infers the engine's TTL from its interval: four of them.
    assert len(client.renewals) >= 2, client.renewals
    assert all(r["extend_ms"] == 80 for r in client.renewals), client.renewals


def test_the_sessions_ttl_applies_to_every_step_and_a_step_overrides_it() -> None:
    client = RecordingClient()
    plain = slow("plain")
    own = slow("own", lease_ttl_ms=3000)
    with session(client, lease_ttl_ms=9000).bind():
        plain()
        own()
    assert [a["lease_ttl_ms"] for a in client.acquires] == [9000, 3000]
    first = client.renewals_for(client.acquires[0]["idempotency_key"])
    second = client.renewals_for(client.acquires[1]["idempotency_key"])
    assert first and all(r["extend_ms"] == 9000 for r in first)
    assert second and all(r["extend_ms"] == 3000 for r in second)


def test_durable_tools_passes_its_ttl_to_every_tool_in_the_block(
    monkeypatch: Any,
) -> None:
    client = RecordingClient()

    def start_session(**kw: Any) -> Any:
        return service_pb2.StartSessionResponse(
            session_id=kw["session_id"], version="1.0.0", is_recovered=False
        )

    client.start_session = start_session  # type: ignore[attr-defined]
    monkeypatch.setattr("cellaflow.durable.CellaflowClient", lambda **kw: client)

    @tool
    def charge_card() -> str:
        return "charged"

    @tool(lease_ttl_ms=2000)
    def send_receipt() -> str:
        return "sent"

    with durable_tools("thread-1", lease_ttl_ms=7000):
        charge_card()
        send_receipt()
    assert [a["lease_ttl_ms"] for a in client.acquires] == [7000, 2000]


def test_a_recorded_hits_lease_asks_for_the_ttl_of_the_call_it_records() -> None:
    # The first call is answered from another session's commit, so its result is
    # recorded at its position before the next call claims one.
    hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        current_sequence=0,
        cached_result=common_pb2.StepResult(
            output_payload=serialize({"result": "ORD-1"})
        ),
    )
    client = RecordingClient([hit])

    @tool(
        scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order_id"], lease_ttl_ms=4000
    )
    def place_order(order_id: str) -> str:
        return "placed-here"

    nxt = slow("next", 0.01, lease_ttl_ms=2000)
    with session(client).bind():
        assert place_order("4417") == "ORD-1"
        nxt()
    record = next(a for a in client.acquires if ":record:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 4000


def test_a_recorded_failures_lease_asks_for_the_ttl_of_the_failed_call() -> None:
    client = RecordingClient()

    @tool(lease_ttl_ms=5000)
    def charge_card() -> str:
        raise ValueError("card declined")

    nxt = slow("next", 0.01)
    with session(client).bind():
        with pytest.raises(ValueError, match="card declined"):
            charge_card()
        nxt()
    record = next(a for a in client.acquires if ":failure:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 5000


@pytest.mark.parametrize(
    "bad, error",
    [
        (0, ValueError),
        (999, ValueError),
        (-1000, ValueError),
        (2**53, ValueError),
        (2**64 - 1, ValueError),
        (1500.5, TypeError),
        (float("inf"), TypeError),
        (True, TypeError),
        ("5000", TypeError),
    ],
)
def test_a_ttl_the_sdk_will_not_ask_for_is_refused_when_the_step_is_made(
    bad: Any, error: type
) -> None:
    with pytest.raises(error, match=r"tool 'charge_card'.*lease_ttl_ms"):

        @tool(lease_ttl_ms=bad)
        def charge_card() -> None:
            pass

    with pytest.raises(error, match=r"tool 'charge_card'.*lease_ttl_ms"):
        tool(lambda: None, tool_name="charge_card", lease_ttl_ms=bad)
    with pytest.raises(error, match=r"step 'charge_card'.*lease_ttl_ms"):

        @step(lease_ttl_ms=bad)
        def charge_card() -> None:  # noqa: F811
            pass


def test_a_string_ttl_says_it_was_a_string() -> None:
    # A value read from the environment is a string: say so, not "got 5000".
    with pytest.raises(TypeError, match=r"got str '5000'"):
        tool(
            lambda: None,
            tool_name="charge_card",
            lease_ttl_ms="5000",  # type: ignore[arg-type]
        )


def test_the_floor_and_an_unset_ttl_are_accepted() -> None:
    assert MIN_LEASE_TTL_MS == 1000
    # The largest TypeScript can send exactly, so both SDKs accept the same range.
    assert MAX_LEASE_TTL_MS == 2**53 - 1
    assert {"MIN_LEASE_TTL_MS", "MAX_LEASE_TTL_MS"} <= set(cellaflow.__all__)
    tool(lambda: None, tool_name="a", lease_ttl_ms=MIN_LEASE_TTL_MS)
    tool(lambda: None, tool_name="d", lease_ttl_ms=MAX_LEASE_TTL_MS)
    tool(lambda: None, tool_name="b", lease_ttl_ms=None)
    tool(lambda: None, tool_name="c")


def test_a_session_ttl_the_sdk_will_not_ask_for_is_refused_before_anything_opens(
    monkeypatch: Any,
) -> None:
    with pytest.raises(ValueError, match="WorkflowContext: lease_ttl_ms"):
        session(RecordingClient(), lease_ttl_ms=5)

    def no_client(**kw: Any) -> None:
        raise AssertionError("connected before refusing the TTL")

    monkeypatch.setattr("cellaflow.durable.CellaflowClient", no_client)
    with pytest.raises(ValueError, match="durable_tools: lease_ttl_ms"):
        with durable_tools("thread-1", lease_ttl_ms=10):
            pass


def test_a_call_that_waits_on_another_holder_asks_for_its_ttl_on_every_try() -> None:
    busy = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS, retry_after_ms=1
    )
    client = RecordingClient([busy])
    charge = slow("charge_card", 0.01, lease_ttl_ms=6000)
    with session(client).bind():
        charge()
    assert [a["lease_ttl_ms"] for a in client.acquires] == [6000, 6000]


def test_a_call_under_its_own_coordination_id_keeps_the_steps_ttl() -> None:
    client = RecordingClient()

    @tool(
        scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order_id"], lease_ttl_ms=6000
    )
    def charge_card(order_id: str) -> str:
        return "charged"

    with session(client).bind():
        charge_card("4417", _coordination_id="customer-88")
    assert client.acquires[0]["lease_ttl_ms"] == 6000


def test_an_unreleased_failed_lease_is_waited_for_by_the_steps_ttl() -> None:
    client = RecordingClient(fail_release=True)

    @tool(lease_ttl_ms=6000)
    def charge_card() -> str:
        raise ValueError("card declined")

    ctx = session(client)
    with ctx.bind():
        with pytest.raises(ValueError, match="card declined"):
            charge_card()
    pending = ctx._pending_hit
    assert pending is not None and pending["unreleased"] is not None
    # A renewal still on its way (its RPC timeout, 1 s here), the TTL, and a
    # second for the engine's clock.
    assert pending["unreleased"]["lapses_in_s"] == pytest.approx(8.0)


def test_renewals_come_at_least_every_quarter_of_the_ttl() -> None:
    # An engine that suggests a minute between renewals for a one-second lease
    # would let it lapse while the holder is healthy.
    client = RecordingClient(interval=60_000)
    charge = slow("charge_card", 0.7, lease_ttl_ms=1000)
    with session(client).bind():
        charge()
    assert len(client.renewals) >= 2, f"renewed {len(client.renewals)} times"


def test_an_engine_that_suggests_no_interval_gets_renewals_by_the_ttl() -> None:
    client = RecordingClient(interval=0)
    charge = slow("charge_card", 0.7, lease_ttl_ms=1000)
    with session(client).bind():
        charge()
    assert 2 <= len(client.renewals) <= 5, f"renewed {len(client.renewals)} times"


def test_a_ttl_past_what_timers_can_hold_does_not_break_the_heartbeat() -> None:
    errors: List[Optional[BaseException]] = []
    previous = threading.excepthook

    def record(args: Any) -> None:
        errors.append(args.exc_value)

    threading.excepthook = record
    try:
        # The engine suggests a quarter of the TTL, which no timer can wait.
        client = RecordingClient(interval=lambda ttl: ttl // 4)
        charge = slow("charge_card", 0.2, lease_ttl_ms=MAX_LEASE_TTL_MS)
        with session(client).bind():
            charge()
    finally:
        threading.excepthook = previous
    assert errors == []
    assert client.renewals == []


def test_the_heartbeat_caps_its_interval() -> None:
    hb = LeaseHeartbeat(
        client=RecordingClient(),  # type: ignore[arg-type]
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=2**51,
        lease_ttl_ms=MAX_LEASE_TTL_MS,
    )
    assert hb.interval_sec <= (2**31 - 1) / 1000.0
    # The TTL itself is what renewals still ask for.
    assert hb.extend_ms == MAX_LEASE_TTL_MS
    # And a hung renewal is abandoned well before a quarter of the TTL.
    assert hb.rpc_timeout_sec == 10.0


# The async wrapper is a copy of the sync one, so each path is checked there too.


def _shared_hit() -> Any:
    return idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        current_sequence=0,
        cached_result=common_pb2.StepResult(
            output_payload=serialize({"result": "ORD-1"})
        ),
    )


@pytest.mark.asyncio
async def test_an_async_recorded_hits_lease_asks_for_the_ttl_of_the_call() -> None:
    client = RecordingClient([_shared_hit()])

    @tool(
        scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order_id"], lease_ttl_ms=4000
    )
    async def place_order(order_id: str) -> str:
        return "placed-here"

    @tool(lease_ttl_ms=2000)
    async def nxt() -> str:
        return "next"

    with session(client).bind():
        assert await place_order("4417") == "ORD-1"
        await nxt()
    record = next(a for a in client.acquires if ":record:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 4000


@pytest.mark.asyncio
async def test_an_async_recorded_failures_lease_asks_for_the_ttl_of_the_call() -> None:
    client = RecordingClient()

    @tool(lease_ttl_ms=5000)
    async def charge_card() -> str:
        raise ValueError("card declined")

    @tool
    async def nxt() -> str:
        return "next"

    with session(client).bind():
        with pytest.raises(ValueError, match="card declined"):
            await charge_card()
        await nxt()
    record = next(a for a in client.acquires if ":failure:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 5000


@pytest.mark.asyncio
async def test_a_recorded_cancellations_lease_asks_for_the_ttl_of_the_call() -> None:
    client = RecordingClient()

    @tool(lease_ttl_ms=7000)
    async def charge_card() -> str:
        await asyncio.sleep(10)
        return "charged"

    @tool
    async def nxt() -> str:
        return "next"

    with session(client).bind():
        running = asyncio.ensure_future(charge_card())
        await asyncio.sleep(0.05)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        await nxt()
    record = next(a for a in client.acquires if ":failure:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 7000


@pytest.mark.asyncio
async def test_async_renewals_come_at_least_every_quarter_of_the_ttl() -> None:
    client = RecordingClient(interval=60_000)

    @tool(lease_ttl_ms=1000)
    async def charge_card() -> str:
        await asyncio.sleep(0.7)
        return "charged"

    with session(client).bind():
        await charge_card()
    assert len(client.renewals) >= 2, f"renewed {len(client.renewals)} times"


def test_an_engine_outage_does_not_hold_the_workflows_return_for_the_ttl(
    monkeypatch: Any,
) -> None:
    # The failed step's lease could not be released, so its record may wait
    # for that lease to lapse: a minute here. But the engine is down, not
    # refusing the record, and waiting out a minute of outage would hold the
    # workflow's return for as long as the TTL.
    monkeypatch.setattr("cellaflow.context._FINAL_ERROR_WAIT_S", 0.3)
    client = RecordingClient(fail_release=True, down_for_records=True)

    @tool(lease_ttl_ms=60_000)
    def charge_card() -> str:
        raise ValueError("card declined")

    ctx = session(client)
    with ctx.bind():
        with pytest.raises(ValueError, match="card declined"):
            charge_card()
    assert ctx.has_pending_failure()
    started = time.monotonic()
    ctx.record_pending_failure()
    assert time.monotonic() - started < 2.0
    assert not ctx.has_pending_failure()


def test_a_blocks_ttl_reaches_the_records_of_its_steps() -> None:
    hit = _shared_hit()
    client = RecordingClient([hit])

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order_id"])
    def place_order(order_id: str) -> str:
        return "placed-here"

    @tool
    def charge_card() -> str:
        raise ValueError("card declined")

    nxt = slow("next", 0.01)
    with session(client, lease_ttl_ms=9000).bind():
        assert place_order("4417") == "ORD-1"
        with pytest.raises(ValueError, match="card declined"):
            charge_card()
        nxt()
    by_kind = {
        kind: next(a for a in client.acquires if kind in a["idempotency_key"])
        for kind in (":record:", ":failure:")
    }
    assert by_kind[":record:"]["lease_ttl_ms"] == 9000
    assert by_kind[":failure:"]["lease_ttl_ms"] == 9000


@pytest.mark.asyncio
async def test_an_async_blocks_ttl_reaches_the_records_of_its_steps() -> None:
    client = RecordingClient([_shared_hit()])

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order_id"])
    async def place_order(order_id: str) -> str:
        return "placed-here"

    @tool
    async def charge_card() -> str:
        raise ValueError("card declined")

    @tool
    async def nxt() -> str:
        return "next"

    with session(client, lease_ttl_ms=9000).bind():
        assert await place_order("4417") == "ORD-1"
        with pytest.raises(ValueError, match="card declined"):
            await charge_card()
        await nxt()
    record = next(a for a in client.acquires if ":record:" in a["idempotency_key"])
    failure = next(a for a in client.acquires if ":failure:" in a["idempotency_key"])
    assert record["lease_ttl_ms"] == 9000
    assert failure["lease_ttl_ms"] == 9000


@pytest.mark.parametrize("is_async", [False, True])
def test_the_heartbeat_is_asked_for_a_quarter_of_the_ttl(
    monkeypatch: Any, is_async: bool
) -> None:
    # Pinned exactly, not by counting renewals: an engine suggesting a minute
    # for a one-second lease gets 250 ms.
    seen: List[int] = []
    real = LeaseHeartbeat

    def spy(**kw: Any) -> Any:
        seen.append(kw["heartbeat_interval_ms"])
        return real(**kw)

    monkeypatch.setattr("cellaflow.decorators.LeaseHeartbeat", spy)
    client = RecordingClient(interval=60_000)
    if is_async:

        @tool(lease_ttl_ms=1000)
        async def charge_card() -> str:
            return "charged"

        with session(client).bind():
            asyncio.run(charge_card())
    else:
        charge = slow("charge_card", 0.01, lease_ttl_ms=1000)
        with session(client).bind():
            charge()
    assert seen == [250]


# The last failure record of a run: what an outage may hold up, and what not.


class _Err(grpc.RpcError):  # type: ignore[misc]
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


class _ScriptedEngine:
    """The failed step's lease could not be released; the record's lease is
    answered per `script`: "held" (that lease still holds the position), "down"
    (unavailable), or granted."""

    def __init__(self, script: List[str]) -> None:
        self.script = script
        self.commits: List[int] = []
        self.timeouts: List[float] = []

    def release_lease(self, **kw: Any) -> Any:
        raise _Err(grpc.StatusCode.UNAVAILABLE)

    def check_idempotency_cache(self, **kw: Any) -> Any:
        self.timeouts.append(kw["timeout"])
        nxt = self.script.pop(0) if self.script else "ok"
        if nxt == "held":
            raise _Err(grpc.StatusCode.FAILED_PRECONDITION)
        if nxt == "down":
            raise _Err(grpc.StatusCode.UNAVAILABLE)
        return idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED, fencing_token=9
        )

    def commit_step(self, **kw: Any) -> Any:
        self.commits.append(kw["sequence"])
        return service_pb2.CommitStepResponse()


def _held_failure(engine: _ScriptedEngine, lapses_in_s: float) -> WorkflowContext:
    ctx = WorkflowContext(
        client=engine,  # type: ignore[arg-type]
        session_id="s",
        workflow_version="1",
        sequence=1,
        history_known=True,
    )
    ctx._in_flight = 1
    ctx.defer_failure_record(
        1,
        "charge",
        ValueError("card declined"),
        None,
        step_key="k",
        unreleased={
            "agent_id": "a",
            "key": "k",
            "token": 7,
            "lapses_in_s": lapses_in_s,
        },
    )
    ctx.end_call()
    return ctx


def test_a_held_position_is_waited_out_past_the_error_budget(monkeypatch: Any) -> None:
    monkeypatch.setattr("cellaflow.context._FINAL_HELD_WAIT_S", 0.3)
    monkeypatch.setattr("cellaflow.context._FINAL_ERROR_WAIT_S", 0.3)
    engine = _ScriptedEngine(["held", "held"])  # two 1 s waits, past both budgets
    _held_failure(engine, lapses_in_s=5.0).record_pending_failure()
    assert engine.commits == [1]


def test_answers_between_errors_restart_the_error_budget(monkeypatch: Any) -> None:
    monkeypatch.setattr("cellaflow.context._FINAL_ERROR_WAIT_S", 0.6)
    monkeypatch.setattr("cellaflow.context._FINAL_RETRY_S", 0.25)
    # Each run of errors is shorter than the budget; together they are longer.
    engine = _ScriptedEngine(
        ["down", "down", "held", "down", "down", "held", "down", "down"]
    )
    _held_failure(engine, lapses_in_s=10.0).record_pending_failure()
    assert engine.commits == [1]


def test_an_answer_after_the_error_budgets_last_try_is_waited_out(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr("cellaflow.context._FINAL_ERROR_WAIT_S", 0.3)
    engine = _ScriptedEngine(["down", "held"])
    _held_failure(engine, lapses_in_s=5.0).record_pending_failure()
    assert engine.commits == [1]


def test_an_engine_call_is_bounded_by_the_error_budget(monkeypatch: Any) -> None:
    # A call that hangs to its deadline must not outlast the budget it starts:
    # the lease's lapse, a minute away, is not the bound.
    monkeypatch.setattr("cellaflow.context._FINAL_ERROR_WAIT_S", 2.0)
    engine = _ScriptedEngine(["down"] * 10)
    _held_failure(engine, lapses_in_s=60.0).record_pending_failure()
    assert engine.commits == []
    assert engine.timeouts and max(engine.timeouts) <= 2.05, engine.timeouts


@pytest.mark.parametrize("decorator", [step, tool])
def test_a_bad_scope_is_reported_before_a_bad_ttl(decorator: Any) -> None:
    # Both are wrong; @tool and @step name the same one.
    with pytest.raises(ValueError) as refused:

        @decorator(shared_on=["no_such_argument"], lease_ttl_ms=5)
        def charge_card(order_id: str) -> None:
            pass

    assert "lease_ttl_ms" not in str(refused.value)
