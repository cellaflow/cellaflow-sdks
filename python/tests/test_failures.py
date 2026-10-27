"""A step's failure is recorded at its position, and raised again on replay.

A workflow that catches a step's exception and carries on must not leave the
step's position empty, or its next step runs and is refused at commit. And a
resumed run must raise the same exception at the same position, so the workflow
takes the same branch.
"""

import asyncio
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
    assert isinstance(e, WithResponse) and str(e) == "boom"
    with pytest.raises(AttributeError, match="not available on replay"):
        e.response


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


def test_a_cancelled_step_gives_its_position_back(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A cancellation is not recorded: replayed as a raised exception it would not
    arrive the way it did (wait_for turns a real one into TimeoutError). The
    position is handed back and the lease released, so the next step takes the
    position instead of landing past an empty one."""
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
    assert _commits(mock_stub) == [(1, "fallback", SUCCESS)]
    mock_stub.ReleaseLease.assert_called_once()


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
