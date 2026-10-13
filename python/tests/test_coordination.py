"""The coordination domain: a session default, adopted on resume, overridable
per call, and the input to every SCOPE_SHARED key."""

from typing import Any, Callable, List, Optional, cast
from unittest.mock import MagicMock

import grpc
import pytest

from cellaflow import durable_tools
from cellaflow.client import CellaflowClient
from cellaflow.decorators import tool, workflow
from cellaflow.idempotency import (
    MAX_COORDINATION_ID_BYTES,
    IdempotencyScope,
    normalize_coordination_id,
)
from cellaflow.serialization import serialize
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2


@pytest.fixture
def stub(monkeypatch: Any) -> MagicMock:
    s = MagicMock()
    s.CheckIdempotencyCache.return_value = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=7,
        heartbeat_interval_ms=60000,
    )
    s.CommitStep.return_value = service_pb2.CommitStepResponse(next_sequence=2)
    s.GetGraph.return_value = service_pb2.GetGraphResponse()
    s.StartSession.return_value = started()
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: s,
    )
    return s


def started(
    coordination_id: Optional[str] = None, recovered: bool = False
) -> service_pb2.StartSessionResponse:
    resp = service_pb2.StartSessionResponse(
        session_id="s-1", version="1.0.0", is_recovered=recovered
    )
    if coordination_id is not None:
        resp.coordination_id = coordination_id
    return resp


def keys(stub: MagicMock) -> List[str]:
    return [c[0][0].idempotency_key for c in stub.CheckIdempotencyCache.call_args_list]


def committed(stub: MagicMock) -> List[common_pb2.StepResult]:
    return [c[0][0].step_result for c in stub.CommitStep.call_args_list]


def start_request(stub: MagicMock) -> service_pb2.StartSessionRequest:
    return stub.StartSession.call_args[0][0]  # type: ignore[no-any-return]


ran: List[Any] = []


@tool(
    tool_name="refund", scope=IdempotencyScope.SCOPE_SHARED, shared_on=["customer"]
)  # type: ignore[untyped-decorator]
def refund(customer: str, amount: int) -> str:
    ran.append((customer, amount))
    return f"refunded {customer}"


@workflow(version="1.0.0")
def _support_agent(customer: str, amount: int, domain: Optional[str] = None) -> str:
    if domain is None:
        return refund(customer, amount)  # type: ignore[no-any-return]
    result = refund(customer, amount, _coordination_id=domain)
    return result  # type: ignore[no-any-return]


# Typed loosely so calls can pass the `_session_id` and `_coordination_id`
# keywords the decorator consumes.
support_agent: Callable[..., str] = _support_agent


@pytest.fixture(autouse=True)
def clear_ran() -> None:
    ran.clear()


def test_the_session_default_is_sent_and_keys_shared_calls(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    support_agent("customer-88", 10, _coordination_id="ticket-4417")

    assert start_request(stub).coordination_id == "ticket-4417"
    assert keys(stub)[0].startswith("shared:ticket-4417:refund:")
    # The step names no domain of its own, so the commit omits it and the
    # engine records the session default.
    assert not committed(stub)[0].HasField("coordination_id")


def test_a_resumed_session_adopts_the_stored_domain(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417", recovered=True)

    support_agent("customer-88", 10, _session_id="s-1")

    assert not start_request(stub).HasField("coordination_id")
    assert keys(stub)[0].startswith("shared:ticket-4417:refund:")


def test_an_engine_returning_no_domain_keeps_the_requested_one(
    stub: MagicMock,
) -> None:
    stub.StartSession.return_value = started(None)

    support_agent("customer-88", 10, _coordination_id="ticket-4417")

    assert keys(stub)[0].startswith("shared:ticket-4417:refund:")


class _Refused(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self) -> str:
        return (
            "Coordination mismatch for session s-1: pinned to ticket-4417, "
            "but request specified ticket-9999"
        )


def test_a_resume_under_another_domain_is_an_error(stub: MagicMock) -> None:
    stub.StartSession.side_effect = _Refused()

    with pytest.raises(grpc.RpcError) as exc:
        support_agent(
            "customer-88", 10, _session_id="s-1", _coordination_id="ticket-9999"
        )

    assert exc.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert ran == [], "the workflow body must not run under a refused session"


def test_a_call_can_override_the_domain(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    support_agent("customer-88", 10, "customer-88", _coordination_id="ticket-4417")

    assert ran == [("customer-88", 10)], "the override must not reach the tool"
    assert keys(stub)[0].startswith("shared:customer-88:refund:")
    assert committed(stub)[0].coordination_id == "customer-88"


def test_an_override_converges_with_a_session_using_that_domain(
    stub: MagicMock,
) -> None:
    # One agent works a ticket and refunds the customer under the customer's
    # domain; another works under the customer's domain directly. They disagree
    # on the amount, which shared_on leaves out of the key.
    stub.StartSession.return_value = started("ticket-4417")
    support_agent("customer-88", 10, "customer-88", _coordination_id="ticket-4417")

    stub.StartSession.return_value = started("customer-88")
    support_agent("customer-88", 25, _coordination_id="customer-88")

    first, second = keys(stub)
    assert first == second


def test_an_empty_override_inherits_the_default(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    support_agent("customer-88", 10, "", _coordination_id="ticket-4417")

    assert keys(stub)[0].startswith("shared:ticket-4417:refund:")
    assert not committed(stub)[0].HasField("coordination_id")


@pytest.mark.parametrize(
    "bad",
    ["ticket:4417", " t-1", "t-1 ", "\tt-1", "x" * (MAX_COORDINATION_ID_BYTES + 1)],
)
def test_an_invalid_override_fails_before_anything_runs(
    stub: MagicMock, bad: str
) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    @workflow(version="1.0.0")
    def agent() -> str:
        with pytest.raises(ValueError):
            refund("customer-88", 10, _coordination_id=bad)
        assert ran == [], "an invalid override must fail before the tool body"
        stub.CheckIdempotencyCache.assert_not_called()
        # And the failed call must not have consumed a sequence.
        return refund("customer-88", 10)  # type: ignore[no-any-return]

    cast(Any, agent)(_coordination_id="ticket-4417")

    assert [s.sequence for s in committed(stub)] == [1]


@pytest.mark.parametrize(
    "bad",
    ["ticket:4417", " t-1", "t-1 ", "x" * (MAX_COORDINATION_ID_BYTES + 1)],
)
@pytest.mark.asyncio
async def test_an_invalid_async_override_fails_before_anything_runs(
    stub: MagicMock, bad: str
) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    @tool(
        tool_name="notify", scope=IdempotencyScope.SCOPE_SHARED, shared_on=["who"]
    )  # type: ignore[untyped-decorator]
    async def notify(who: str) -> str:
        ran.append(who)
        return f"notified {who}"

    @workflow(version="1.0.0")
    async def agent() -> str:
        with pytest.raises(ValueError):
            await notify("customer-88", _coordination_id=bad)
        assert ran == [], "an invalid override must fail before the tool body"
        stub.CheckIdempotencyCache.assert_not_called()
        # And the failed call must not have consumed a sequence.
        result = await notify("customer-88")
        return result  # type: ignore[no-any-return]

    await cast(Any, agent)(_coordination_id="ticket-4417")

    assert [s.sequence for s in committed(stub)] == [1]


def test_an_invalid_session_default_fails_before_the_session_starts(
    stub: MagicMock,
) -> None:
    with pytest.raises(ValueError):
        support_agent("customer-88", 10, _coordination_id="ticket:4417")

    stub.StartSession.assert_not_called()


@pytest.mark.asyncio
async def test_an_async_call_can_override_the_domain(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417")

    @tool(
        tool_name="notify", scope=IdempotencyScope.SCOPE_SHARED, shared_on=["who"]
    )  # type: ignore[untyped-decorator]
    async def notify(who: str) -> str:
        return f"notified {who}"

    @workflow(version="1.0.0")
    async def agent() -> str:
        result = await notify("customer-88", _coordination_id="customer-88")
        return result  # type: ignore[no-any-return]

    assert (
        await cast(Any, agent)(_coordination_id="ticket-4417") == "notified customer-88"
    )
    assert keys(stub)[0].startswith("shared:customer-88:notify:")
    assert committed(stub)[0].coordination_id == "customer-88"


def test_durable_tools_sends_and_adopts_the_domain(stub: MagicMock) -> None:
    stub.StartSession.return_value = started("ticket-4417")
    with durable_tools("thread-1", coordination_id="ticket-4417") as ctx:
        assert start_request(stub).coordination_id == "ticket-4417"
        assert ctx.coordination_id == "ticket-4417"

    # A later block for the same thread may omit it and adopts the stored one.
    with durable_tools("thread-1") as ctx:
        assert not start_request(stub).HasField("coordination_id")
        assert ctx.coordination_id == "ticket-4417"


def test_get_graph_returns_each_steps_domain(stub: MagicMock) -> None:
    payload = serialize({"result": None})
    stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=[
            common_pb2.StepResult(
                sequence=1, name="a", output_payload=payload, coordination_id="t-1"
            ),
            common_pb2.StepResult(sequence=2, name="b", output_payload=payload),
        ]
    )

    steps, _ = CellaflowClient().get_graph("s-1")

    assert [s["coordination_id"] for s in steps] == ["t-1", None]


def test_normalize_matches_the_engine_rules() -> None:
    assert normalize_coordination_id(None) is None
    assert normalize_coordination_id("") is None
    longest = "x" * MAX_COORDINATION_ID_BYTES
    assert normalize_coordination_id(longest) == longest
    # The limit is in UTF-8 bytes, as the engine counts it, not characters.
    with pytest.raises(ValueError):
        normalize_coordination_id("é" * (MAX_COORDINATION_ID_BYTES // 2 + 1))
    with pytest.raises(TypeError):
        normalize_coordination_id(cast(Any, 4417))
