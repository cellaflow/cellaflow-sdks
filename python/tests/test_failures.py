"""A step's failure is recorded at its position, and raised again on replay.

A workflow that catches a step's exception and carries on must not leave the
step's position empty, or its next step runs and is refused at commit. And a
resumed run must raise the same exception at the same position, so the workflow
takes the same branch.
"""

import asyncio
import sys
from typing import Any, Callable, List, Tuple
from unittest.mock import MagicMock

import grpc
import pytest

from cellaflow.decorators import (
    DivergentStepError,
    NondeterministicWorkflowError,
    step,
    workflow,
)
from cellaflow.failures import capture
from cellaflow.serialization import deserialize, serialize
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2


class PaymentDeclined(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class HttpFailure(Exception):
    """Passes Exception a different argument from the ones it takes."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body


class WithResponse(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.response = object()  # cannot be recorded


@pytest.fixture
def mock_stub() -> MagicMock:
    stub = MagicMock()
    stub.CheckIdempotencyCache.return_value = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=456,
        heartbeat_interval_ms=10000,
    )
    stub.CommitStep.side_effect = lambda req, **_: service_pb2.CommitStepResponse(
        session_id=req.session_id, next_sequence=req.step_result.sequence + 1
    )
    return stub


@pytest.fixture
def mock_client(mock_stub: MagicMock, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: mock_stub,
    )


def _fresh(mock_stub: MagicMock, session_id: str) -> None:
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id=session_id, version="1.0.0", is_recovered=False
    )


def _commits(mock_stub: MagicMock) -> List[Tuple[int, str, int]]:
    """Every commit as (position, step name, status)."""
    reqs = [c[0][0] for c in mock_stub.CommitStep.call_args_list]
    return [
        (r.step_result.sequence, r.step_result.name, r.step_result.status)
        for r in reqs
    ]


FAILED = common_pb2.StepStatus.STEP_STATUS_FAILED
SUCCESS = common_pb2.StepStatus.STEP_STATUS_SUCCESS


def _checkout(ran: List[str]) -> Callable[..., Any]:
    @step
    def charge(card: str) -> str:
        ran.append("charge")
        raise PaymentDeclined("insufficient funds")

    @step
    def notify(message: str) -> str:
        ran.append("notify")
        return message

    @workflow(version="1.0.0")
    def checkout(card: str) -> str:
        try:
            charge(card)
        except PaymentDeclined as e:
            return notify(f"declined: {e.reason}")  # type: ignore[no-any-return]
        return "charged"

    return checkout


# --- Recording -----------------------------------------------------------------


def test_a_caught_failure_is_recorded_and_the_next_step_takes_the_next_position(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The failure fills its position, so the next step commits where the engine
    expects it, instead of one past an empty position."""
    _fresh(mock_stub, "sess-caught")
    ran: List[str] = []

    assert _checkout(ran)("4111") == "declined: insufficient funds"
    assert ran == ["charge", "notify"]
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]

    failure = mock_stub.CommitStep.call_args_list[0][0][0]
    assert failure.idempotency_key.startswith("sess-caught:failure:1:")
    error = deserialize(failure.step_result.output_payload)["error"]
    assert error["type"] == f"{__name__}:PaymentDeclined"
    assert error["attrs"] == {"reason": "insufficient funds"}


def test_a_failure_the_workflow_lets_propagate_is_not_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The run ended on it, so resuming should retry the step, not replay it."""
    _fresh(mock_stub, "sess-propagate")

    @step
    def charge() -> str:
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    def checkout() -> str:
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(PaymentDeclined):
        checkout()
    mock_stub.CommitStep.assert_not_called()


def test_a_caught_failure_with_no_later_step_is_recorded_on_return(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The workflow finished past the failure, so a resume must replay it."""
    _fresh(mock_stub, "sess-return")

    @step
    def charge() -> str:
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge()
        except PaymentDeclined:
            return "fallback"
        return "charged"

    assert checkout() == "fallback"
    assert _commits(mock_stub) == [(1, "charge", FAILED)]


def test_an_async_workflow_records_a_caught_failure_on_return(
    mock_stub: MagicMock, mock_client: None
) -> None:
    _fresh(mock_stub, "sess-async-return")

    @step
    async def charge() -> str:
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    async def checkout() -> str:
        try:
            await charge()
        except PaymentDeclined:
            return "fallback"
        return "charged"

    assert asyncio.run(checkout()) == "fallback"
    assert _commits(mock_stub) == [(1, "charge", FAILED)]


def test_a_refused_lease_is_not_recorded_as_a_failure(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Another writer owns that position and fills it."""
    _fresh(mock_stub, "sess-divergent")

    class Claimed(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.FAILED_PRECONDITION

        def details(self) -> str:
            return "position claimed"

    mock_stub.CheckIdempotencyCache.side_effect = Claimed()

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge()
        except DivergentStepError:
            return "refused"
        return "charged"

    assert checkout() == "refused"
    mock_stub.CommitStep.assert_not_called()


def test_a_failure_while_another_call_runs_is_not_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The other call may commit before or after it, so its position cannot be
    known to be the next one. Nothing is recorded, as before. `slow` is released
    only once `charge`'s failure has been handled, so it is in flight throughout,
    whatever the event loop's scheduling."""
    _fresh(mock_stub, "sess-overlap")
    release = asyncio.Event()

    @step
    async def slow() -> str:
        await release.wait()
        return "slow"

    @step
    async def charge() -> str:
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    async def checkout() -> List[Any]:
        async def charge_then_release() -> Any:
            try:
                return await charge()
            except PaymentDeclined as e:
                return e
            finally:
                release.set()

        return list(await asyncio.gather(slow(), charge_then_release()))

    results = asyncio.run(checkout())
    assert results[0] == "slow"
    assert isinstance(results[1], PaymentDeclined)
    assert _commits(mock_stub) == [(1, "slow", SUCCESS)]


def test_a_failure_after_the_other_call_finished_is_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Once the other call has committed, the failed call's position is the next
    one, so recording it there is safe."""
    _fresh(mock_stub, "sess-after")
    slow_done = asyncio.Event()

    @step
    async def slow() -> str:
        return "slow"

    @step
    async def charge() -> str:
        await slow_done.wait()
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    async def checkout() -> str:
        async def slow_then_signal() -> str:
            try:
                result: str = await slow()
                return result
            finally:
                slow_done.set()

        async def charge_caught() -> str:
            try:
                await charge()
            except PaymentDeclined:
                pass
            return "fallback"

        await asyncio.gather(slow_then_signal(), charge_caught())
        return "done"

    assert asyncio.run(checkout()) == "done"
    assert _commits(mock_stub) == [(1, "slow", SUCCESS), (2, "charge", FAILED)]


# --- Replay --------------------------------------------------------------------


def _history(mock_stub: MagicMock, session_id: str, steps: List[Any]) -> None:
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id=session_id, version="1.0.0", is_recovered=True
    )
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=steps, next_cursor=""
    )


def _failed(sequence: int, name: str, exc: BaseException) -> Any:
    try:
        raise exc
    except BaseException as live:
        recorded = capture(live)
    return common_pb2.StepResult(
        sequence=sequence,
        name=name,
        status=FAILED,
        output_payload=serialize({"error": recorded}),
    )


def _succeeded(sequence: int, name: str, result: Any) -> Any:
    return common_pb2.StepResult(
        sequence=sequence,
        name=name,
        status=SUCCESS,
        output_payload=serialize({"result": result}),
    )


def test_a_resumed_workflow_raises_the_recorded_failure_and_takes_the_same_branch(
    mock_stub: MagicMock, mock_client: None
) -> None:
    _history(
        mock_stub,
        "sess-replay",
        [
            _failed(1, "charge", PaymentDeclined("insufficient funds")),
            _succeeded(2, "notify", "declined: insufficient funds"),
        ],
    )
    ran: List[str] = []

    result = _checkout(ran)("4111", _session_id="sess-replay")
    assert result == "declined: insufficient funds"
    assert ran == []
    mock_stub.CheckIdempotencyCache.assert_not_called()
    mock_stub.CommitStep.assert_not_called()


def test_a_replayed_failure_is_an_instance_of_the_original_class(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Rebuilt without calling its constructor, so a class whose constructor
    takes different arguments from the ones it passes to Exception comes back
    intact."""
    _history(mock_stub, "sess-http", [_failed(1, "fetch", HttpFailure(402, "no"))])
    caught: List[HttpFailure] = []

    @step
    def fetch() -> str:
        raise AssertionError("a recorded failure must replay, not run")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            fetch()
        except HttpFailure as e:
            caught.append(e)
        return "done"

    assert wf(_session_id="sess-http") == "done"  # type: ignore[call-arg]
    (e,) = caught
    assert type(e) is HttpFailure
    assert (e.status, e.body, str(e)) == (402, "no", "HTTP 402")


def test_an_attribute_that_was_not_recorded_cannot_be_read_on_replay(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Rather than quietly reading as something else, so the run cannot diverge
    on it unnoticed."""
    _history(mock_stub, "sess-attr", [_failed(1, "fetch", WithResponse("boom"))])
    caught: List[WithResponse] = []

    @step
    def fetch() -> str:
        raise AssertionError("must replay")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            fetch()
        except WithResponse as e:
            caught.append(e)
        return "done"

    wf(_session_id="sess-attr")  # type: ignore[call-arg]
    (e,) = caught
    assert type(e) is WithResponse and str(e) == "boom"
    response: Any = e.response
    assert "not recorded" in repr(response)
    for use in (
        lambda: response.status_code,
        lambda: bool(response),
        lambda: response == 200,
        lambda: str(response),
        lambda: response["body"],
        lambda: list(response),
        lambda: response(),
    ):
        with pytest.raises(AttributeError, match="not available on replay"):
            use()


def test_a_failure_whose_class_cannot_be_imported_refuses_the_resume(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """A class defined inside a function has no importable name. The run is
    warned when it fails, and a resume refuses rather than running the step."""

    class Local(Exception):
        pass

    _fresh(mock_stub, "sess-local")

    @step
    def charge() -> str:
        raise Local("x")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Local:
            return "fallback"
        return "charged"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "fallback"
    assert "is not defined at module level" in caplog.text

    recorded = mock_stub.CommitStep.call_args_list[0][0][0].step_result
    _history(mock_stub, "sess-local", [recorded])
    with pytest.raises(NondeterministicWorkflowError, match="not defined in any"):
        wf(_session_id="sess-local")  # type: ignore[call-arg]


# --- Capture and rebuild -------------------------------------------------------


def _round_trip(exc: BaseException) -> BaseException:
    from cellaflow.failures import rebuild

    try:
        raise exc
    except BaseException as live:
        recorded = deserialize(serialize({"e": capture(live)}))["e"]
    return rebuild(recorded, "step", 1)


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad"),
        KeyError("k"),
        FileNotFoundError(2, "No such file", "/x"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"),
        TimeoutError("t"),
        asyncio.CancelledError(),
        HttpFailure(402, "no"),
    ],
)
def test_a_rebuilt_exception_matches_the_original(exc: BaseException) -> None:
    rebuilt = _round_trip(exc)
    assert type(rebuilt) is type(exc)
    assert rebuilt.args == exc.args
    assert (str(rebuilt), repr(rebuilt)) == (str(exc), repr(exc))
    if isinstance(exc, OSError):
        assert getattr(rebuilt, "filename") == exc.filename


def test_an_argument_that_cannot_be_recorded_cannot_be_used_on_replay() -> None:
    from cellaflow.failures import UnavailableOnReplay

    rebuilt = _round_trip(ValueError("bad", object()))
    assert rebuilt.args[0] == "bad"
    placeholder = rebuilt.args[1]
    assert isinstance(placeholder, UnavailableOnReplay)
    assert "not available on replay" in repr(placeholder)
    with pytest.raises(AttributeError, match="not available on replay"):
        placeholder.anything


def test_a_location_that_is_not_an_exception_class_is_refused() -> None:
    from cellaflow.failures import ReplayedFailureError, rebuild

    with pytest.raises(ReplayedFailureError, match="not defined in any module"):
        rebuild({"type": "os:path", "args": []}, "step", 4)
    with pytest.raises(ReplayedFailureError, match="not defined in any module"):
        rebuild({"type": "no.such.module:Thing", "args": []}, "step", 4)


class OwnNew(Exception):
    """Its own __new__ and __init__ have side effects. Replay never runs them."""

    calls: List[str] = []

    def __new__(cls, *args: Any) -> "OwnNew":
        OwnNew.calls.append("new")
        return super().__new__(cls, *args)

    def __init__(self, *args: Any) -> None:
        OwnNew.calls.append("init")
        super().__init__(*args)


def test_a_class_s_own_new_and_init_never_run_on_replay() -> None:
    """Rebuilding runs no code the recorded data can choose: only Python's own
    exception machinery, which stores fields."""
    from cellaflow.failures import rebuild

    OwnNew.calls.clear()
    rebuilt = rebuild({"type": f"{__name__}:OwnNew", "args": ["x"]}, "step", 1)
    assert type(rebuilt) is OwnNew and rebuilt.args == ("x",)
    assert OwnNew.calls == []


# --- Review fixes --------------------------------------------------------------


def test_replay_never_imports_a_module(tmp_path: Any, monkeypatch: Any) -> None:
    """The recorded type names a class; replay looks only among modules already
    loaded. A module it would have to import is refused, and its code never runs."""
    from cellaflow.failures import ReplayedFailureError, rebuild

    (tmp_path / "side_effect_on_import.py").write_text(
        "import builtins\nbuiltins.CELLAFLOW_IMPORTED = True\n"
        "class Boom(Exception):\n    pass\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    import builtins

    with pytest.raises(ReplayedFailureError, match="not defined in any module"):
        rebuild({"type": "side_effect_on_import:Boom", "args": []}, "step", 1)
    assert not hasattr(builtins, "CELLAFLOW_IMPORTED")


def test_a_cancellation_is_recorded_with_how_long_the_step_ran(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The workflow carried on past it, so it is recorded at its position, and
    the next step takes the following one. The step's lease is released."""
    _fresh(mock_stub, "sess-cancel")

    @step
    async def charge() -> str:
        await asyncio.sleep(5)
        return "charged"

    @step
    async def fallback() -> str:
        return "fallback"

    @workflow(version="1.0.0")
    async def checkout() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.05)
        except asyncio.TimeoutError:
            return await fallback()  # type: ignore[no-any-return]

    assert asyncio.run(checkout()) == "fallback"
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "fallback", SUCCESS)]
    mock_stub.ReleaseLease.assert_called_once()
    first = mock_stub.CommitStep.call_args_list[0][0][0]
    record = deserialize(first.step_result.output_payload)
    assert 0.04 <= record["cancelled"]["elapsed_s"] < 1.0


class Unavailable(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return "engine restarting"


def test_an_engine_error_replays_with_its_code_and_details() -> None:
    from cellaflow.failures import ReplayedRpcError

    rebuilt = _round_trip(Unavailable())
    assert isinstance(rebuilt, grpc.RpcError) and isinstance(rebuilt, ReplayedRpcError)
    assert rebuilt.code() == grpc.StatusCode.UNAVAILABLE
    assert rebuilt.details() == "engine restarting"


class WithStatus(Exception):
    def __init__(self, status: Any) -> None:
        super().__init__(f"status {int(status)}")
        self.status = status


def test_tuples_enums_and_plain_subclasses_are_recorded() -> None:
    """Tuples come back as tuples. An enum or a subclass of a plain type comes
    back as its plain value, so comparisons with it still hold."""
    from http import HTTPStatus

    key_error = _round_trip(KeyError(("a", 1)))
    assert key_error.args == (("a", 1),) and str(key_error) == "('a', 1)"

    with_status = _round_trip(WithStatus(HTTPStatus.NOT_FOUND))
    assert isinstance(with_status, WithStatus)
    assert with_status.status == 404


def test_values_that_cannot_be_recorded_are_named_when_the_failure_happens(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    _fresh(mock_stub, "sess-unrecorded")

    @step
    def fetch() -> str:
        raise WithResponse("boom")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            fetch()
        except WithResponse:
            return "fallback"
        return "fetched"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "fallback"
    assert "cannot be recorded (response)" in caplog.text


def test_a_class_defined_in_main_is_warned_about(
    mock_stub: MagicMock, mock_client: None, caplog: Any, monkeypatch: Any
) -> None:
    """Found by the process that raised it, but not by a resume started from
    another entry point."""
    import __main__

    class InMain(Exception):
        pass

    InMain.__module__ = "__main__"
    InMain.__qualname__ = "InMain"
    monkeypatch.setattr(__main__, "InMain", InMain, raising=False)
    _fresh(mock_stub, "sess-main")

    @step
    def charge() -> str:
        raise InMain("x")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except InMain:
            return "fallback"
        return "charged"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "fallback"
    assert "(__main__)" in caplog.text


def test_a_failure_recorded_by_another_sdk_is_refused() -> None:
    from cellaflow.failures import ReplayedFailureError, rebuild

    recorded = {"sdk": "typescript", "type": "PaymentDeclined", "message": "x"}
    with pytest.raises(ReplayedFailureError, match="recorded by the typescript SDK"):
        rebuild(recorded, "charge", 1)


# --- Replaying a cancellation --------------------------------------------------


def _cancelled(sequence: int, name: str, elapsed_s: float) -> Any:
    return common_pb2.StepResult(
        sequence=sequence,
        name=name,
        status=FAILED,
        output_payload=serialize(
            {"cancelled": {"format": 1, "sdk": "python", "elapsed_s": elapsed_s}}
        ),
    )


def test_a_replayed_timeout_takes_the_same_branch(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The replay waits rather than raising, so the workflow's own wait_for times
    out again and the code sees TimeoutError, as it did the first time."""
    import time

    _history(
        mock_stub,
        "sess-timeout",
        [_cancelled(1, "charge", 0.05), _succeeded(2, "fallback", "fallback")],
    )
    ran: List[str] = []

    @step
    async def charge() -> str:
        ran.append("charge")
        return "charged"

    @step
    async def fallback() -> str:
        ran.append("fallback")
        return "live fallback"

    @workflow(version="1.0.0")
    async def checkout() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.05)
        except asyncio.TimeoutError:
            return await fallback()  # type: ignore[no-any-return]

    started = time.monotonic()
    result = asyncio.run(checkout(_session_id="sess-timeout"))  # type: ignore[call-arg]
    assert result == "fallback"
    assert ran == []
    assert 0.04 <= time.monotonic() - started < 1.0
    mock_stub.CommitStep.assert_not_called()


def test_a_replayed_outside_cancel_is_delivered_by_the_replay(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The first run was cancelled from outside the code (a Cancel button). Nothing
    repeats that on replay, so after waiting as long as the step ran, the replay
    cancels it itself, and the workflow takes the same branch."""
    _history(
        mock_stub,
        "sess-button",
        [_cancelled(1, "issue_refund", 0.05), _succeeded(2, "escalate", "escalated")],
    )
    ran: List[str] = []

    @step
    async def issue_refund() -> str:
        ran.append("issue_refund")
        return "refunded"

    @step
    async def escalate() -> str:
        ran.append("escalate")
        return "live escalate"

    @workflow(version="1.0.0")
    async def handle() -> str:
        task = asyncio.ensure_future(issue_refund())
        try:
            return await task  # type: ignore[no-any-return]
        except asyncio.CancelledError:
            return await escalate()  # type: ignore[no-any-return]

    result = asyncio.run(handle(_session_id="sess-button"))  # type: ignore[call-arg]
    assert result == "escalated"
    assert ran == []


def test_a_divergence_after_a_cancellation_says_where_the_paths_parted(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The code changed since the first run and now calls a different step after
    the timeout. The error names the session, the position, both steps and the
    cancellation before them, and the way on."""
    from cellaflow import NondeterministicWorkflowError, ReplayDivergedError

    _history(
        mock_stub,
        "sess-changed",
        [_cancelled(1, "charge", 0.05), _succeeded(2, "escalate", "escalated")],
    )

    @step
    async def charge() -> str:
        return "charged"

    @step
    async def send_reply() -> str:
        return "sent"

    @workflow(version="1.0.0")
    async def handle() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.05)
        except asyncio.TimeoutError:
            return await send_reply()  # type: ignore[no-any-return]

    with pytest.raises(ReplayDivergedError) as excinfo:
        asyncio.run(handle(_session_id="sess-changed"))  # type: ignore[call-arg]
    assert isinstance(excinfo.value, NondeterministicWorkflowError)
    message = str(excinfo.value)
    assert "Session sess-changed cannot resume at position 2" in message
    assert "'send_reply'" in message and "'escalate'" in message
    assert "Position 1 ('charge') was cancelled on the first run" in message
    assert "start a new session" in message


def test_a_cancellation_cannot_replay_on_a_step_that_is_now_synchronous(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow import FailureNotReplayableError

    _history(mock_stub, "sess-sync", [_cancelled(1, "charge", 0.05)])

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def handle() -> str:
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(FailureNotReplayableError, match="now a synchronous one"):
        handle(_session_id="sess-sync")  # type: ignore[call-arg]


def test_an_unreplayable_failure_is_a_failure_not_replayable_error(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow import FailureNotReplayableError, NondeterministicWorkflowError

    recorded = common_pb2.StepResult(
        sequence=1,
        name="charge",
        status=FAILED,
        output_payload=serialize({"error": {"sdk": "typescript", "type": "X"}}),
    )
    _history(mock_stub, "sess-ts", [recorded])

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def handle() -> str:
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(FailureNotReplayableError) as excinfo:
        handle(_session_id="sess-ts")  # type: ignore[call-arg]
    assert isinstance(excinfo.value, NondeterministicWorkflowError)


# --- Review fixes, round two ----------------------------------------------------


def test_a_timeout_around_several_steps_replays_its_branch(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The timeout started before the first step, which took most of it. On replay
    that step is instant, so the timeout fires later relative to the cancelled
    step than it did. The replay waits until the run is as far in as it was."""
    _history(
        mock_stub,
        "sess-scope",
        [
            _succeeded(1, "prepare", "ready"),
            common_pb2.StepResult(
                sequence=2,
                name="charge",
                status=FAILED,
                output_payload=serialize(
                    {
                        "cancelled": {
                            "format": 1,
                            "sdk": "python",
                            "elapsed_s": 0.1,
                            "since_run_start_s": 1.0,
                        }
                    }
                ),
            ),
            _succeeded(3, "fallback", "fallback"),
        ],
    )

    @step
    async def prepare() -> str:
        return "ready"

    @step
    async def charge() -> str:
        return "charged"

    @step
    async def fallback() -> str:
        return "live fallback"

    @workflow(version="1.0.0")
    async def checkout() -> str:
        async def both() -> str:
            await prepare()
            return await charge()  # type: ignore[no-any-return]

        try:
            return await asyncio.wait_for(both(), 1.0)
        except asyncio.TimeoutError:
            return await fallback()  # type: ignore[no-any-return]

    result = asyncio.run(checkout(_session_id="sess-scope"))  # type: ignore[call-arg]
    assert result == "fallback"


class FetchFailed(OSError):
    """Sets its OSError fields in its own constructor, which replay never runs."""

    def __init__(self, path: str) -> None:
        super().__init__(2, "missing", path)


def test_an_oserror_subclass_keeps_errno_and_strerror() -> None:
    import errno

    rebuilt = _round_trip(FetchFailed("/data/x"))
    assert isinstance(rebuilt, FetchFailed)
    assert rebuilt.errno == errno.ENOENT and rebuilt.strerror == "missing"
    assert str(rebuilt) == str(FetchFailed("/data/x"))


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="exception groups arrived in Python 3.11"
)
def test_an_exception_group_replays_with_its_members() -> None:
    import builtins

    group_cls = getattr(builtins, "ExceptionGroup")
    group = group_cls("batch failed", [ValueError("a"), PaymentDeclined("no funds")])
    rebuilt = _round_trip(group)
    assert type(rebuilt) is group_cls
    assert rebuilt.message == "batch failed"  # type: ignore[attr-defined]
    members = rebuilt.exceptions  # type: ignore[attr-defined]
    assert [type(m) for m in members] == [ValueError, PaymentDeclined]
    assert members[1].reason == "no funds"
    matched, rest = rebuilt.split(PaymentDeclined)  # type: ignore[attr-defined]
    assert matched is not None and rest is not None


class Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError("str() is broken")


def test_an_exception_that_cannot_be_inspected_still_reaches_the_workflow(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Recording the failure must never replace the step's own exception."""
    _fresh(mock_stub, "sess-unprintable")

    @step
    def charge() -> str:
        raise Unprintable()

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Unprintable:
            return "caught the real one"
        return "charged"

    assert wf() == "caught the real one"
    assert _commits(mock_stub) == [(1, "charge", FAILED)]


class Huge(Exception):
    def __init__(self) -> None:
        super().__init__("response too big")
        self.body = b"x" * (2 * 1024 * 1024)


def test_a_huge_value_is_left_out_so_the_record_fits() -> None:
    recorded = capture(Huge())
    assert "body" in recorded["unavailable_attrs"]
    assert len(serialize({"e": recorded})) < 600 * 1024


def test_a_failed_release_does_not_replace_the_steps_exception(
    mock_stub: MagicMock, mock_client: None
) -> None:
    _fresh(mock_stub, "sess-release")

    class Unavailable(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.UNAVAILABLE

    mock_stub.ReleaseLease.side_effect = Unavailable()

    @step
    def charge() -> str:
        raise PaymentDeclined("insufficient funds")

    @workflow(version="1.0.0")
    def wf() -> str:
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(PaymentDeclined):
        wf()


def test_a_failure_record_in_another_shape_is_not_replayable(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow import FailureNotReplayableError

    recorded = common_pb2.StepResult(
        sequence=1,
        name="charge",
        status=FAILED,
        output_payload=serialize({"error": "timeout"}),
    )
    _history(mock_stub, "sess-shape", [recorded])

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def wf() -> str:
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(FailureNotReplayableError, match="not in a form"):
        wf(_session_id="sess-shape")  # type: ignore[call-arg]


class NoSubclasses(Exception):
    def __init_subclass__(cls, **kwargs: Any) -> None:
        raise TypeError("NoSubclasses may not be subclassed")


def test_a_missing_field_keeps_the_exact_class_and_runs_none_of_its_code() -> None:
    """No subclass is made for it, so a class that refuses subclasses, or that
    `except` matches by exact type, replays as itself."""
    from cellaflow.failures import UnavailableOnReplay, rebuild

    recorded = {
        "type": f"{__name__}:NoSubclasses",
        "args": ["x"],
        "unavailable_attrs": ["response"],
    }
    exc = rebuild(recorded, "step", 1)
    assert type(exc) is NoSubclasses
    assert isinstance(vars(exc)["response"], UnavailableOnReplay)


def test_a_failure_that_propagates_is_never_captured(
    mock_stub: MagicMock, mock_client: None, caplog: Any, monkeypatch: Any
) -> None:
    """Nothing is recorded for it, so nothing is captured and nothing is said
    about how it would replay."""
    from cellaflow.failures import capture as real_capture

    captured: List[Any] = []

    def counting_capture(exc: BaseException) -> Any:
        captured.append(exc)
        return real_capture(exc)

    monkeypatch.setattr("cellaflow.context.capture", counting_capture)
    _fresh(mock_stub, "sess-propagate-quiet")

    class Local(Exception):
        pass

    @step
    def charge() -> str:
        raise Local("x")

    @workflow(version="1.0.0")
    def wf() -> str:
        return charge()  # type: ignore[no-any-return]

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        with pytest.raises(Local):
            wf()
    assert captured == []
    assert "not defined at module level" not in caplog.text


# --- Review fixes, round three --------------------------------------------------


def _group_cls() -> Any:
    import builtins

    return getattr(builtins, "ExceptionGroup")


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="exception groups arrived in Python 3.11"
)
def test_every_member_of_a_wide_group_keeps_its_type() -> None:
    """Members past the first fifty keep their type and message, so `except*`
    matches the replayed group exactly as it matched the original."""
    members: List[Exception] = [ValueError(str(i)) for i in range(55)]
    members.append(PaymentDeclined("no funds"))
    rebuilt = _round_trip(_group_cls()("batch", members))
    replayed = rebuilt.exceptions  # type: ignore[attr-defined]
    assert len(replayed) == 56
    assert type(replayed[-1]) is PaymentDeclined
    assert str(replayed[-1]) == "no funds"
    assert replayed[-1].args == ("no funds",)
    matched, rest = rebuilt.split(PaymentDeclined)  # type: ignore[attr-defined]
    assert matched is not None and len(rest.exceptions) == 55


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="exception groups arrived in Python 3.11"
)
def test_a_group_too_big_in_full_keeps_its_members_by_type() -> None:
    members = [WithPayload("x" * 60_000) for _ in range(10)]
    recorded = capture(_group_cls()("batch", members))
    assert not recorded["group"].get("incomplete")
    rebuilt = _round_trip(_group_cls()("batch", members))
    replayed = rebuilt.exceptions  # type: ignore[attr-defined]
    assert [type(m) for m in replayed] == [WithPayload] * 10
    with pytest.raises(AttributeError, match="not available on replay"):
        replayed[0].payload.upper()


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="exception groups arrived in Python 3.11"
)
def test_a_group_that_cannot_be_recorded_refuses_the_resume_and_says_so(
    mock_stub: MagicMock, mock_client: None, caplog: Any, monkeypatch: Any
) -> None:
    """Rather than replay a smaller group, which could take a different branch."""
    from cellaflow.failures import ReplayedFailureError, rebuild

    monkeypatch.setattr("cellaflow.failures._MAX_GROUP_TOTAL", 3)
    group = _group_cls()("batch", [ValueError(str(i)) for i in range(4)])
    recorded = capture(group)
    assert recorded["group"]["incomplete"] is True
    with pytest.raises(ReplayedFailureError, match="too large to record"):
        rebuild(deserialize(serialize({"e": recorded}))["e"], "step", 1)

    _fresh(mock_stub, "sess-big-group")

    @step
    def fan_out() -> str:
        raise _group_cls()("batch", [ValueError(str(i)) for i in range(4)])

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            fan_out()
        except Exception:  # `except*` would not parse on Python 3.10
            return "fallback"
        return "done"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "fallback"
    assert "too large to record" in caplog.text
    assert "refuse to resume past this position" in caplog.text


class WithPayload(Exception):
    def __init__(self, payload: str) -> None:
        super().__init__("payload too big")
        self.payload = payload


def test_an_attribute_errors_object_is_not_warned_about(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """Python fills `AttributeError.obj` with the live object, which can never be
    recorded and which handlers almost never read. It replays as a placeholder,
    without a warning on every `AttributeError` a step raises."""
    _fresh(mock_stub, "sess-obj")

    @step
    def lookup() -> str:
        return object().missing  # type: ignore[attr-defined,no-any-return]

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            lookup()
        except AttributeError:
            return "fallback"
        return "found"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "fallback"
    assert "cannot be recorded" not in caplog.text

    try:
        object().missing  # type: ignore[attr-defined]
    except AttributeError as live:
        rebuilt = _round_trip(live)
    assert type(rebuilt) is AttributeError
    assert rebuilt.name == "missing"
    with pytest.raises(AttributeError, match="not available on replay"):
        bool(rebuilt.obj)


def _cancelled_at(seq: int, elapsed_s: float, since_run_start_s: float) -> Any:
    return common_pb2.StepResult(
        sequence=seq,
        name="charge",
        status=FAILED,
        output_payload=serialize(
            {
                "cancelled": {
                    "format": 1,
                    "sdk": "python",
                    "elapsed_s": elapsed_s,
                    "since_run_start_s": since_run_start_s,
                }
            }
        ),
    )


def test_the_wait_for_a_timeout_around_earlier_steps_is_capped(
    mock_stub: MagicMock, mock_client: None, monkeypatch: Any
) -> None:
    """A cancellation that came three hours into the original run does not hold
    the resumed run for three hours."""
    import time

    monkeypatch.setattr("cellaflow.decorators._CANCEL_WAIT_RUN_CAP_S", 0.2)
    _history(mock_stub, "sess-cap", [_cancelled_at(1, 0.01, 3 * 3600.0)])

    @step
    async def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    async def wf() -> str:
        return await charge()  # type: ignore[no-any-return]

    async def run() -> None:
        run_wf: Any = wf
        await asyncio.wait_for(run_wf(_session_id="sess-cap"), 5.0)

    started = time.monotonic()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(run())
    assert time.monotonic() - started < 2.0


def test_the_steps_own_wait_is_not_capped(
    mock_stub: MagicMock, mock_client: None, monkeypatch: Any
) -> None:
    """A single step under a long `wait_for` still waits for that timeout."""
    import time

    monkeypatch.setattr("cellaflow.decorators._CANCEL_WAIT_RUN_CAP_S", 0.0)
    _history(mock_stub, "sess-own", [_cancelled_at(1, 0.3, 0.3)])

    @step
    async def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    async def wf() -> str:
        return await charge()  # type: ignore[no-any-return]

    started = time.monotonic()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(wf(_session_id="sess-own"))  # type: ignore[call-arg]
    assert time.monotonic() - started >= 0.8  # 0.3 * 2 + 0.25


def test_a_timeout_that_expires_during_the_commit_does_not_cancel_a_committed_step(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The commit blocks the loop, so a timeout that expires meanwhile is handled
    at the step's next `await`. After a commit there is none: the step returns
    its result, and nothing records it as cancelled at its own position."""
    import time

    _fresh(mock_stub, "sess-late-timeout")
    answer = mock_stub.CommitStep.side_effect

    def slow_commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == "charge":
            time.sleep(0.3)  # past the 0.1 s timeout below
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = slow_commit

    @step
    async def charge() -> str:
        return "charged"

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            outcome = await asyncio.wait_for(charge(), 0.1)
        except asyncio.TimeoutError:
            outcome = "timed out"
        await notify()
        return outcome  # type: ignore[no-any-return]

    assert asyncio.run(wf()) == "charged"
    assert _commits(mock_stub) == [(1, "charge", SUCCESS), (2, "notify", SUCCESS)]
