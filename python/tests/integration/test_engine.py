"""The SDK against a real engine: recording, replay and positions."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import asyncio
import threading
import time
from typing import Any, Dict, List

import grpc
import pytest

from cellaflow import (
    CellaflowClient,
    DivergentStepError,
    FailureNotReplayableError,
    IdempotencyScope,
    durable_tools,
    step,
    tool,
    workflow,
)
from cellaflow.idempotency import record_idempotency_key
from cellaflow.v1 import common_pb2, idempotency_pb2

from tests.integration.conftest import history
from tests.integration.errors import PaymentDeclined


def call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Calls a workflow with `_session_id` or `_coordination_id`, which its
    signature does not declare."""
    return fn(*args, **kwargs)


SUCCESS = common_pb2.StepStatus.STEP_STATUS_SUCCESS


# --- Replay -----------------------------------------------------------------


def test_a_resumed_workflow_replays_its_steps_without_running_them(
    target: str, unique: str
) -> None:
    """Including a shared step answered from another session's result, which
    is recorded at its position in the receiving session."""
    runs: Dict[str, int] = {"price": 0, "place": 0, "send": 0}
    domain = f"order-{unique}"

    @tool(
        tool_name="place_order",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def place_order(order_id: str) -> str:
        runs["place"] += 1
        return "ORD-1"

    @tool(
        tool_name="send_confirmation",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def send_confirmation(order_id: str) -> str:
        runs["send"] += 1
        return "emailed"

    @step
    def price_cart(order_id: str) -> int:
        runs["price"] += 1
        return 2499

    @workflow(version="1.0.0", target=target)
    def other_agent(order_id: str) -> str:
        return place_order(order_id)  # type: ignore[no-any-return]

    @workflow(version="1.0.0", target=target)
    def agent(order_id: str) -> str:
        price_cart(order_id)
        place_order(order_id)
        return send_confirmation(order_id)  # type: ignore[no-any-return]

    call(other_agent, "4417", _coordination_id=domain)
    session = f"agent-{unique}"
    runs.update(price=0, place=0, send=0)
    call(agent, "4417", _session_id=session, _coordination_id=domain)
    assert runs == {"price": 1, "place": 0, "send": 1}
    first = history(target, session)
    assert first == [
        (1, "price_cart", "ok"),
        (2, "place_order", "ok"),
        (3, "send_confirmation", "ok"),
    ]

    runs.update(price=0, place=0, send=0)
    for _ in range(2):
        call(agent, "4417", _session_id=session, _coordination_id=domain)
    assert runs == {"price": 0, "place": 0, "send": 0}
    assert history(target, session) == first


def test_a_caught_failure_is_recorded_and_replayed(target: str, unique: str) -> None:
    ran: List[str] = []

    @step
    def flaky() -> str:
        ran.append("flaky")
        raise PaymentDeclined("insufficient funds")

    @step
    def charge(reason: str) -> str:
        ran.append("charge")
        return f"handled: {reason}"

    @workflow(version="1.0.0", target=target)
    def agent() -> str:
        try:
            flaky()
        except PaymentDeclined as e:
            return charge(e.reason)  # type: ignore[no-any-return]
        return "no failure"

    session = f"caught-{unique}"
    assert call(agent, _session_id=session) == "handled: insufficient funds"
    assert ran == ["flaky", "charge"]
    recorded = [(1, "flaky", "FAILED"), (2, "charge", "ok")]
    assert history(target, session) == recorded

    ran.clear()
    assert call(agent, _session_id=session) == "handled: insufficient funds"
    assert ran == []
    assert history(target, session) == recorded


def test_a_retry_loop_replays_its_failed_attempt(target: str, unique: str) -> None:
    ran: List[str] = []

    @step
    def charge(card: str) -> str:
        ran.append("charge")
        if ran.count("charge") == 1:
            raise PaymentDeclined("gateway timeout")
        return "ch_123"

    @step
    def receipt(charge_id: str) -> str:
        ran.append("receipt")
        return f"receipt for {charge_id}"

    @workflow(version="1.0.0", target=target)
    def checkout(card: str) -> str:
        charge_id = ""
        for attempt in range(3):
            try:
                charge_id = charge(card)
                break
            except PaymentDeclined:
                if attempt == 2:
                    raise
        return receipt(charge_id)  # type: ignore[no-any-return]

    session = f"retry-{unique}"
    assert call(checkout, "4111", _session_id=session) == "receipt for ch_123"
    recorded = [(1, "charge", "FAILED"), (2, "charge", "ok"), (3, "receipt", "ok")]
    assert history(target, session) == recorded

    ran.clear()
    assert call(checkout, "4111", _session_id=session) == "receipt for ch_123"
    assert ran == []


def test_a_recorded_cancellation_refuses_the_resume(target: str, unique: str) -> None:
    """A replay cannot tell what cancelled the step, so it refuses rather than
    guess, and runs nothing."""
    ran: List[str] = []

    @step
    async def charge() -> str:
        ran.append("charge")
        await asyncio.sleep(5)
        return "charged"

    @step
    async def fallback() -> str:
        ran.append("fallback")
        return "fallback"

    @workflow(version="1.0.0", target=target)
    async def checkout() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.3)
        except asyncio.TimeoutError:
            return await fallback()  # type: ignore[no-any-return]

    session = f"cancel-{unique}"
    run: Any = checkout
    assert asyncio.run(run(_session_id=session)) == "fallback"
    assert history(target, session) == [(1, "charge", "FAILED"), (2, "fallback", "ok")]

    ran.clear()
    with pytest.raises(FailureNotReplayableError, match="cannot tell what cancel"):
        asyncio.run(run(_session_id=session))
    assert ran == []


def test_the_last_failure_record_is_committed_before_the_workflow_returns(
    target: str, unique: str
) -> None:
    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @workflow(version="1.0.0", target=target)
    def checkout() -> str:
        try:
            charge()
        except PaymentDeclined:
            return "declined"
        return "charged"

    session = f"last-{unique}"
    assert checkout(_session_id=session) == "declined"  # type: ignore[call-arg]
    # Read at once, by a client of its own: the record is already committed.
    assert history(target, session) == [(1, "charge", "FAILED")]


# --- durable_tools ------------------------------------------------------------


def test_durable_tools_records_a_shared_result_and_a_resume_adds_nothing(
    target: str, unique: str
) -> None:
    runs = {"place": 0, "send": 0}
    domain = f"order-{unique}"

    @tool(
        tool_name="place_order",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def place_order(order_id: str) -> str:
        runs["place"] += 1
        return "ORD-1"

    @tool(
        tool_name="send_confirmation",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def send_confirmation(order_id: str) -> str:
        runs["send"] += 1
        return "emailed"

    with durable_tools(
        {"configurable": {"thread_id": f"other-{unique}"}},
        target=target,
        coordination_id=domain,
    ):
        place_order("4418")

    thread = {"configurable": {"thread_id": f"agent-{unique}"}}
    runs.update(place=0, send=0)
    with durable_tools(thread, target=target, coordination_id=domain) as ctx:
        place_order("4418")
        send_confirmation("4418")
        session = ctx.session_id
    assert runs == {"place": 0, "send": 1}
    first = history(target, session)
    assert first == [(1, "place_order", "ok"), (2, "send_confirmation", "ok")]

    runs.update(place=0, send=0)
    with durable_tools(thread, target=target, coordination_id=domain):
        place_order("4418")
        send_confirmation("4418")
    assert runs == {"place": 0, "send": 0}
    assert history(target, session) == first


def test_a_resumed_thread_runs_a_tool_it_never_reached(
    target: str, unique: str
) -> None:
    """Its first call is not a cache hit, so it claims a position: the one
    after what the session holds, not position 1."""

    @tool(tool_name="reserve_seat")
    def reserve_seat(flight: str) -> str:
        return f"seat on {flight}"

    @tool(tool_name="charge_card")
    def charge_card(amount: int) -> str:
        return f"charged {amount}"

    thread = {"configurable": {"thread_id": f"resume-{unique}"}}
    with durable_tools(thread, target=target) as ctx:
        reserve_seat("BA117")
        session = ctx.session_id
    with durable_tools(thread, target=target):
        assert charge_card(450) == "charged 450"
    assert history(target, session) == [
        (1, "reserve_seat", "ok"),
        (2, "charge_card", "ok"),
    ]


def test_a_resumed_thread_runs_a_failed_tool_again_at_the_next_position(
    target: str, unique: str
) -> None:
    attempts: List[int] = []

    @tool(tool_name="charge_card")
    def charge_card(amount: int) -> str:
        attempts.append(amount)
        if len(attempts) == 1:
            raise PaymentDeclined("declined")
        return f"charged {amount}"

    @tool(tool_name="notify")
    def notify(message: str) -> str:
        return message

    thread = {"configurable": {"thread_id": f"retry-{unique}"}}
    with durable_tools(thread, target=target) as ctx:
        try:
            charge_card(450)
        except PaymentDeclined:
            notify("card declined, will retry")
        session = ctx.session_id
    with durable_tools(thread, target=target):
        assert charge_card(450) == "charged 450"
    assert history(target, session) == [
        (1, "charge_card", "FAILED"),
        (2, "notify", "ok"),
        (3, "charge_card", "ok"),
    ]


# --- Two writers in one session -----------------------------------------------


def _shared_order(target: str, domain: str, unique: str) -> Any:
    @tool(
        tool_name="place_order",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def place_order(order_id: str) -> str:
        return "ORD-1"

    with durable_tools(
        {"configurable": {"thread_id": f"peer-{unique}"}},
        target=target,
        coordination_id=domain,
    ):
        place_order("4417")
    return place_order


def test_a_received_result_is_not_recorded_where_another_replica_leased(
    target: str, unique: str
) -> None:
    """The record is leased, so the engine refuses it at a position another
    lease in the session claimed, and that lease's commit goes through after
    its side effect."""
    domain = f"order-{unique}"
    place_order = _shared_order(target, domain, unique)

    @tool(tool_name="mine")
    def mine() -> str:
        return "mine"

    raw = CellaflowClient(target=target)
    thread = {"configurable": {"thread_id": f"rep-{unique}"}}
    with durable_tools(thread, target=target, coordination_id=domain) as s:
        place_order("4417")  # a hit, held to be recorded at position 1
        key = f"replica-2-{unique}"
        lease = raw.check_idempotency_cache(
            agent_id="replica-2",
            idempotency_key=key,
            session_id=s.session_id,
            sequence=1,
        )
        assert lease.status == idempotency_pb2.CACHE_STATUS_ACQUIRED
        with pytest.raises(DivergentStepError):
            mine()
        raw.commit_step(
            session_id=s.session_id,
            sequence=1,
            name="side_effect",
            status=SUCCESS,
            output_payload={"result": "done"},
            idempotency_key=key,
            idempotency_fencing_token=lease.fencing_token,
        )
        session = s.session_id
    assert history(target, session) == [(1, "side_effect", "ok")]


def test_the_next_step_waits_while_a_replica_writes_the_same_record(
    target: str, unique: str
) -> None:
    domain = f"order-{unique}"
    place_order = _shared_order(target, domain, unique)

    @tool(tool_name="mine")
    def mine() -> str:
        return "mine"

    raw = CellaflowClient(target=target)
    thread = {"configurable": {"thread_id": f"held-{unique}"}}
    with durable_tools(thread, target=target, coordination_id=domain) as s:
        place_order("4417")
        assert s._pending_hit is not None
        key = record_idempotency_key(s.session_id, 1, s._pending_hit["received_key"])
        lease = raw.check_idempotency_cache(
            agent_id="replica-2",
            idempotency_key=key,
            session_id=s.session_id,
            sequence=1,
        )
        assert lease.status == idempotency_pb2.CACHE_STATUS_ACQUIRED

        def commit_later() -> None:
            time.sleep(1.5)
            raw.commit_step(
                session_id=s.session_id,
                sequence=1,
                name="place_order",
                status=SUCCESS,
                output_payload={"result": "ORD-1"},
                idempotency_key=key,
                idempotency_fencing_token=lease.fencing_token,
            )

        writer = threading.Thread(target=commit_later)
        writer.start()
        started = time.monotonic()
        assert mine() == "mine"
        assert time.monotonic() - started >= 1.0
        writer.join()
        session = s.session_id
    assert history(target, session) == [(1, "place_order", "ok"), (2, "mine", "ok")]


# --- What the SDK relies on the engine for ------------------------------------


def test_an_unreleased_lease_refuses_another_key_at_its_position_until_it_lapses(
    target: str, unique: str
) -> None:
    """A failed step's lease that could not be released still claims its
    position, so its failure record waits for the lapse. The SDK's estimate of
    that lapse (`lease_ttl_ms` from the heartbeat interval) must cover it."""
    raw = CellaflowClient(target=target)
    session = f"unreleased-{unique}"
    raw.start_session(workflow_id="unreleased", version="1.0.0", session_id=session)
    step_lease = raw.check_idempotency_cache(
        agent_id="default",
        idempotency_key=f"{session}:step",
        session_id=session,
        sequence=1,
    )
    assert step_lease.status == idempotency_pb2.CACHE_STATUS_ACQUIRED
    interval_s = (step_lease.heartbeat_interval_ms or 5000) / 1000.0

    with pytest.raises(grpc.RpcError) as refused:
        raw.check_idempotency_cache(
            agent_id="default",
            idempotency_key=f"{session}:failure:1:x",
            session_id=session,
            sequence=1,
        )
    assert refused.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    started = time.monotonic()
    while True:
        try:
            record = raw.check_idempotency_cache(
                agent_id="default",
                idempotency_key=f"{session}:failure:1:x",
                session_id=session,
                sequence=1,
            )
            break
        except grpc.RpcError:
            assert time.monotonic() - started < interval_s * 4 + 5
            time.sleep(1)
    assert record.status == idempotency_pb2.CACHE_STATUS_ACQUIRED
