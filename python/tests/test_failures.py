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

    value = object()
    rebuilt = _round_trip(ValueError("bad", value))
    assert rebuilt.args[0] == "bad"
    placeholder = rebuilt.args[1]
    assert isinstance(placeholder, UnavailableOnReplay)
    # Prints as it did, so the exception's message is unchanged.
    assert repr(placeholder) == repr(value)
    assert str(rebuilt) == str(ValueError("bad", value))
    with pytest.raises(AttributeError, match="not available on replay"):
        placeholder.anything


def test_a_location_that_is_not_an_exception_class_is_refused() -> None:
    from cellaflow.failures import ReplayedFailureError, rebuild

    with pytest.raises(ReplayedFailureError, match="not defined in any module"):
        rebuild({"type": "os:path", "args": []}, "step", 4)
    with pytest.raises(ReplayedFailureError, match="has not imported"):
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

    with pytest.raises(ReplayedFailureError, match="has not imported"):
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
    """gRPC's own error classes have internal constructors, so one replays as
    an equivalent RpcError with the same code and details."""
    from grpc.aio import AioRpcError, Metadata

    from cellaflow.failures import ReplayedRpcError

    error = AioRpcError(
        grpc.StatusCode.UNAVAILABLE, Metadata(), Metadata(), "engine restarting"
    )
    rebuilt = _round_trip(error)
    assert isinstance(rebuilt, grpc.RpcError) and isinstance(rebuilt, ReplayedRpcError)
    assert rebuilt.code() == grpc.StatusCode.UNAVAILABLE
    assert rebuilt.details() == "engine restarting"


def test_a_users_own_rpc_error_class_replays_as_itself() -> None:
    """So an `except` clause naming it still matches on replay."""
    rebuilt = _round_trip(Unavailable())
    assert type(rebuilt) is Unavailable
    assert rebuilt.code() == grpc.StatusCode.UNAVAILABLE


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


def test_a_recorded_cancellation_refuses_on_a_synchronous_step_too(
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

    with pytest.raises(FailureNotReplayableError, match="cannot tell what cancel"):
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


def test_a_failure_that_propagates_is_neither_recorded_nor_warned_about(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """Nothing is recorded for it, so nothing is said about how it would replay,
    and a resume runs the step again."""
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
    mock_stub.CommitStep.assert_not_called()
    assert "not defined at module level" not in caplog.text
    assert "resuming this session raises it again" not in caplog.text


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


class Code(int):
    pass


class Label(str):
    pass


class Ratio(float):
    pass


class Blob(bytes):
    pass


class WithFields(Exception):
    def __init__(self) -> None:
        super().__init__("fields")
        self.code = Code(7)
        self.label = Label("eu")
        self.ratio = Ratio(0.5)
        self.raw = Blob(b"\x00\x01")
        self.meta = {"tries": [1, (2, 3)], 4: "four"}
        self.handle = {"conn": object()}
        self.keyed = {(1, 2): "tuple key"}


def test_subclasses_dicts_and_nested_tuples_are_recorded_as_plain_values() -> None:
    rebuilt: Any = _round_trip(WithFields())
    assert type(rebuilt) is WithFields
    assert rebuilt.code == 7 and type(rebuilt.code) is int
    assert rebuilt.label == "eu" and type(rebuilt.label) is str
    assert rebuilt.ratio == 0.5 and type(rebuilt.ratio) is float
    assert rebuilt.raw == b"\x00\x01" and type(rebuilt.raw) is bytes
    assert rebuilt.meta == {"tries": [1, (2, 3)], 4: "four"}
    with pytest.raises(AttributeError, match="not available on replay"):
        rebuilt.handle["conn"]
    with pytest.raises(AttributeError, match="not available on replay"):
        rebuilt.keyed[(1, 2)]


def test_a_replayed_rpc_error_answers_the_rest_of_the_rpc_error_interface() -> None:
    from cellaflow.failures import ReplayedRpcError

    error = ReplayedRpcError(grpc.StatusCode.UNAVAILABLE, "engine restarting")
    assert error.trailing_metadata() == ()
    assert error.debug_error_string() == "replayed UNAVAILABLE: engine restarting"


def test_a_placeholder_refuses_every_use_but_repr() -> None:
    from cellaflow.failures import UnavailableOnReplay

    placeholder: Any = UnavailableOnReplay("Fetch.response")
    assert "Fetch.response" in repr(placeholder)
    for use in (
        lambda: len(placeholder),
        lambda: 1 in placeholder,
        lambda: int(placeholder),
        lambda: float(placeholder),
        lambda: hash(placeholder),
        lambda: placeholder != 1,
        lambda: placeholder < 1,
        lambda: setattr(placeholder, "status", 200),
    ):
        with pytest.raises(AttributeError, match="not available on replay"):
            use()


class LooksLikeAGroup(Exception):
    """Has what a group's record is made from, so the shape can be checked on
    Python 3.10, which has no exception groups."""

    def __init__(self, message: str, exceptions: List[BaseException]) -> None:
        super().__init__(message)
        self.message = message
        self.exceptions = exceptions


def test_a_groups_members_beyond_the_first_keep_their_type_message_and_small_args(
    monkeypatch: Any,
) -> None:
    from cellaflow import failures

    monkeypatch.setattr(failures, "_MAX_GROUP_MEMBERS", 1)
    first = WithPayload("x")
    later = ValueError("small", "y" * 5000)
    later.note = "an attribute"  # type: ignore[attr-defined]
    group = failures._group(LooksLikeAGroup("batch", [first, later]), 0, full=True)
    assert group["message"] == "batch"
    full, minimal = group["exceptions"]
    assert full["attrs"]["payload"] == "x"
    assert minimal["type"] == "builtins:ValueError"
    assert minimal["message"].startswith("('small', 'yyy")
    assert minimal["args"] == ["small", None] and minimal["unavailable_args"] == [1]
    assert minimal["unavailable_attrs"] == ["note"]

    reduced = failures._group(LooksLikeAGroup("batch", [first]), 0, full=False)
    assert reduced["exceptions"][0]["unavailable_attrs"] == ["payload"]


def test_a_member_that_cannot_be_printed_is_kept_by_its_type() -> None:
    from cellaflow import failures

    member = failures._minimal(Unprintable(), 1)
    assert member["type"] == f"{__name__}:Unprintable" and member["message"] == ""


# --- Review fixes, round four ---------------------------------------------------


class Response:
    """Stands for an HTTP client's response object, which cannot be recorded."""

    status = 503

    def __str__(self) -> str:
        return "503 Service Unavailable"


class HTTPFailed(Exception):
    pass


def test_an_exception_whose_argument_was_not_recorded_prints_as_it_did() -> None:
    """A handler that formats the exception reads what it read the first time;
    only using the value itself raises."""
    rebuilt: Any = _round_trip(HTTPFailed(Response()))
    assert type(rebuilt) is HTTPFailed
    assert str(rebuilt) == "503 Service Unavailable"
    assert f"charge failed: {rebuilt}" == "charge failed: 503 Service Unavailable"
    with pytest.raises(AttributeError, match="not available on replay"):
        rebuilt.args[0].status

    original = HTTPFailed("upstream", Response())
    assert str(_round_trip(original)) == str(original)


class WithData(Exception):
    pass


def test_a_dict_that_looks_like_a_recorded_tuple_comes_back_as_a_dict() -> None:
    exc = WithData("x")
    exc.data = {"__cellaflow_tuple__": [1, 2]}  # type: ignore[attr-defined]
    exc.other = {"__cellaflow_dict__": {"a": 1}}  # type: ignore[attr-defined]
    rebuilt: Any = _round_trip(exc)
    assert rebuilt.data == {"__cellaflow_tuple__": [1, 2]}
    assert rebuilt.other == {"__cellaflow_dict__": {"a": 1}}


def test_an_exception_only_its_extension_module_can_build_is_refused_and_warned(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """pydantic's ValidationError is constructed by compiled code, which a replay
    does not run. The run says so when the failure is recorded."""
    pydantic = pytest.importorskip("pydantic")
    from cellaflow.failures import ReplayedFailureError

    Order = pydantic.create_model("Order", quantity=(int, ...))

    try:
        Order(quantity="many")
    except pydantic.ValidationError as live:
        with pytest.raises(ReplayedFailureError, match="compiled code"):
            _round_trip(live)

    _fresh(mock_stub, "sess-pydantic")

    @step
    def parse() -> Any:
        return Order(quantity="many")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            parse()
        except pydantic.ValidationError:
            return "asked again"
        return "parsed"

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "asked again"
    assert "cannot be raised again by a resumed run" in caplog.text
    assert "compiled code" in caplog.text


def _caught_then_returns(raise_in_step: BaseException) -> Callable[..., Any]:
    @step
    def charge() -> str:
        raise raise_in_step

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Exception:
            pass
        return "done"

    return wf


def test_a_record_that_cannot_be_written_after_the_workflow_returns_keeps_its_result(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """One attempt, then the record is dropped: the workflow's result never
    waits on, or is replaced by, the bookkeeping."""
    _fresh(mock_stub, "sess-end")
    answer = mock_stub.CommitStep.side_effect

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.status == FAILED:
            raise Unavailable()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    wf = _caught_then_returns(PaymentDeclined("no funds"))
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "done"
    assert mock_stub.CommitStep.call_count == 1
    assert "The workflow's result stands" in caplog.text


def test_a_record_held_by_a_lease_is_not_waited_for_after_the_workflow_returns(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """A caller with a timeout would otherwise lose a finished run to a record
    waiting for a lease to lapse."""
    import time

    _fresh(mock_stub, "sess-end-held")
    acquired = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if ":failure:" in req.idempotency_key:
            return idempotency_pb2.CheckCacheResponse(
                status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS, retry_after_ms=5000
            )
        return acquired

    mock_stub.CheckIdempotencyCache.side_effect = check
    started = time.monotonic()
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert _caught_then_returns(PaymentDeclined("no funds"))() == "done"
    assert time.monotonic() - started < 1.0
    assert "a lease still holds its position" in caplog.text


def test_a_failure_is_not_recorded_by_a_run_that_does_not_know_the_history() -> None:
    """As for a hit: a resumed `durable_tools` thread counts positions from 0, so
    it would record at a position the session already filled."""
    from cellaflow.context import WorkflowContext

    for known in (False, True):
        ctx = WorkflowContext(
            client=MagicMock(),
            session_id="s",
            workflow_version="1",
            sequence=1,
            replayed_steps={},
            history_known=known,
        )
        ctx._in_flight = 1
        ctx.defer_failure_record(1, "charge", ValueError("x"), None)
        assert (ctx._pending_hit is not None) is known


class Claimed(grpc.RpcError):  # type: ignore[misc]
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self) -> str:
        return "position claimed"


def _failure_then_next(mock_stub: MagicMock, session_id: str) -> Callable[..., Any]:
    _fresh(mock_stub, session_id)

    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except PaymentDeclined:
            return notify()  # type: ignore[no-any-return]
        return "charged"

    return wf


def test_a_release_that_failed_is_tried_again_before_the_failure_is_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The failed step's lease would otherwise still claim its position, the
    record would be refused there, and the next step refused as divergent."""
    releases = [Unavailable()]

    def release(req: Any, **kwargs: Any) -> Any:
        if releases:
            raise releases.pop()
        return idempotency_pb2.ReleaseLeaseResponse()

    mock_stub.ReleaseLease.side_effect = release
    assert _failure_then_next(mock_stub, "sess-release")() == "notified"
    assert mock_stub.ReleaseLease.call_count == 2
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def test_a_record_waits_for_an_unreleased_lease_to_lapse(
    mock_stub: MagicMock, mock_client: None
) -> None:
    mock_stub.ReleaseLease.side_effect = Unavailable()
    acquired = mock_stub.CheckIdempotencyCache.return_value
    refusals = [Claimed()]

    def check(req: Any, **kwargs: Any) -> Any:
        if ":failure:" in req.idempotency_key and refusals:
            raise refusals.pop()
        return acquired

    mock_stub.CheckIdempotencyCache.side_effect = check
    assert _failure_then_next(mock_stub, "sess-lapse")() == "notified"
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def test_an_async_workflow_writes_failure_records_off_the_event_loop(
    mock_stub: MagicMock, mock_client: None
) -> None:
    import threading

    _fresh(mock_stub, "sess-off-loop")
    answer = mock_stub.CommitStep.side_effect
    threads: List[Tuple[str, bool]] = []

    def commit(req: Any, **kwargs: Any) -> Any:
        on_main = threading.current_thread() is threading.main_thread()
        threads.append((req.step_result.name, on_main))
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit

    @step
    async def charge() -> str:
        raise PaymentDeclined("no funds")

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            await charge()
        except PaymentDeclined:
            pass
        await notify()
        try:
            await charge()
        except PaymentDeclined:
            pass
        return "done"

    assert asyncio.run(wf()) == "done"
    # Each failure record off the loop: at the next step, and after the return.
    assert threads == [("charge", False), ("notify", True), ("charge", False)]


def _lazy_module(name: str) -> Any:
    """A module created the way `import` inside a step loads a library: during
    the run, after the workflow started."""
    import types

    module = types.ModuleType(name)
    exec("class LazyError(Exception):\n    pass\n", module.__dict__)
    module.LazyError.__module__ = name
    sys.modules[name] = module
    return module


def test_an_exception_from_a_module_a_step_imported_is_warned_about(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """A resume does not run the step, so it does not run its import either.
    The run says so while it can still be fixed."""
    _fresh(mock_stub, "sess-lazy")

    @step
    def charge() -> str:
        lazy = _lazy_module("lazy_payments")
        raise lazy.LazyError("card declined")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Exception:
            return "fallback"
        return "charged"

    try:
        with caplog.at_level("WARNING", logger="cellaflow.context"):
            assert wf() == "fallback"
    finally:
        sys.modules.pop("lazy_payments", None)
    assert "first imported during this run" in caplog.text
    assert "Add `import lazy_payments` where the workflow is defined" in caplog.text


def test_a_module_loaded_before_the_run_is_not_warned_about(
    mock_stub: MagicMock, mock_client: None, caplog: Any, monkeypatch: Any
) -> None:
    _fresh(mock_stub, "sess-eager")
    lazy = _lazy_module("eager_payments")
    # As if this were the process's first run, so the snapshot holds the module.
    monkeypatch.setattr("cellaflow.failures._first_run_modules", None)

    @step
    def charge() -> str:
        raise lazy.LazyError("card declined")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Exception:
            return "fallback"
        return "charged"

    try:
        with caplog.at_level("WARNING", logger="cellaflow.context"):
            assert wf() == "fallback"
    finally:
        sys.modules.pop("eager_payments", None)
    assert "first imported during this run" not in caplog.text


def test_a_resume_names_the_import_a_missing_module_needs() -> None:
    """Nothing is imported on replay, so the refusal says which import to add.
    Once it is there, the same session resumes."""
    from cellaflow.failures import ReplayedFailureError, rebuild

    lazy = _lazy_module("unloaded_payments")
    try:
        recorded = capture(lazy.LazyError("card declined"))
    finally:
        sys.modules.pop("unloaded_payments", None)
    with pytest.raises(ReplayedFailureError) as refused:
        rebuild(recorded, "charge", 1)
    assert "Add `import unloaded_payments` where the workflow is defined" in str(
        refused.value
    )
    assert "resumes from here" in str(refused.value)

    _lazy_module("unloaded_payments")
    try:
        assert str(rebuild(recorded, "charge", 1)) == "card declined"
    finally:
        sys.modules.pop("unloaded_payments", None)


# --- Review fixes, round five ---------------------------------------------------


class RateLimited(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.attempts = 0


def test_a_failure_is_recorded_as_the_step_raised_it_not_as_the_handler_left_it(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A handler that changes the exception must find on replay what it found
    the first time, or it can take a different branch."""
    _fresh(mock_stub, "sess-mutated")

    @step
    def fetch() -> str:
        raise RateLimited("slow down")

    @step
    def backoff(attempts: int) -> int:
        return attempts

    @workflow(version="1.0.0")
    def wf() -> int:
        try:
            fetch()
        except RateLimited as e:
            e.attempts += 1
            return backoff(e.attempts)  # type: ignore[no-any-return]
        return 0

    assert wf() == 1
    first = mock_stub.CommitStep.call_args_list[0][0][0]
    record = deserialize(first.step_result.output_payload)["error"]
    assert record["attrs"]["attempts"] == 0


def test_a_held_failure_does_not_keep_the_failed_steps_frames_alive(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The record holds what was captured, not the exception, whose traceback
    would keep every local of the failed step alive until the next step."""
    import gc
    import weakref

    _fresh(mock_stub, "sess-frames")

    class Buffer:
        pass

    refs: List[Any] = []
    alive_while_held: List[bool] = []

    @step
    def download() -> str:
        buffer = Buffer()  # a large response, say
        refs.append(weakref.ref(buffer))
        raise PaymentDeclined("bad response")

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            download()
        except PaymentDeclined:
            pass
        gc.collect()
        alive_while_held.append(refs[0]() is not None)
        return notify()  # type: ignore[no-any-return]

    assert wf() == "notified"
    assert alive_while_held == [False]
    assert _commits(mock_stub) == [(1, "download", FAILED), (2, "notify", SUCCESS)]


def test_a_failure_a_cleanup_step_followed_says_a_resume_will_raise_it_again(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """The cleanup needs the next position, so the failure is recorded to fill
    the one before it, and a resume replays both and ends the same way. The run
    says so, and what to do instead."""
    _fresh(mock_stub, "sess-finally")

    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @step
    def release_hold() -> str:
        return "released"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        finally:
            release_hold()

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        with pytest.raises(PaymentDeclined):
            wf()
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "release_hold", SUCCESS)]
    assert "the workflow ended with PaymentDeclined from 'charge'" in caplog.text
    assert "To run it again, start a new session" in caplog.text


def test_an_interrupted_run_with_a_cleanup_step_says_the_same(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """Ctrl-C reaches an async step as a cancellation. With a cleanup step after
    it, the cancellation is recorded, and the run says what a resume will do."""
    _fresh(mock_stub, "sess-interrupted")

    @step
    async def long_task() -> str:
        await asyncio.sleep(5)
        return "done"

    @step
    async def release_hold() -> str:
        return "released"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            return await long_task()  # type: ignore[no-any-return]
        finally:
            await release_hold()

    async def interrupt() -> None:
        task = asyncio.ensure_future(wf())
        await asyncio.sleep(0.1)
        task.cancel()  # what asyncio.run does on Ctrl-C
        await task

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(interrupt())
    assert "the workflow ended with 'long_task' cancelled at position 1" in caplog.text
    assert "a resume of this session refuses at that position" in caplog.text


def test_every_run_warns_about_a_module_a_step_imported_not_only_the_first(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """The snapshot is taken once per process, so a second run, which finds the
    module already loaded, still warns."""
    _fresh(mock_stub, "sess-lazy-twice")

    @step
    def charge() -> str:
        lazy = sys.modules.get("lazy_twice") or _lazy_module("lazy_twice")
        raise lazy.LazyError("card declined")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Exception:
            return "fallback"
        return "charged"

    try:
        with caplog.at_level("WARNING", logger="cellaflow.context"):
            assert wf() == "fallback"
            assert wf() == "fallback"
    finally:
        sys.modules.pop("lazy_twice", None)
    assert caplog.text.count("Add `import lazy_twice`") == 2


def test_a_new_exception_at_a_recorded_ones_address_is_not_mistaken_for_it(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """Python reuses a freed exception's address for the next one of the same
    size, so the workflow's own ValueError can sit where a recorded one was."""
    from cellaflow.context import _identity

    _fresh(mock_stub, "sess-reused")

    @step
    def parse() -> str:
        raise ValueError("not a number")

    @step
    def note() -> str:
        return "noted"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            parse()
        except ValueError:
            note()
        raise ValueError("bad input")

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        with pytest.raises(ValueError, match="bad input"):
            wf()
    assert "resuming this session raises it again" not in caplog.text

    # The reuse itself, made certain: a recorded failure whose address and class
    # match the exception the workflow ends with, but whose message does not.
    from cellaflow.context import WorkflowContext

    ending = ValueError("bad input")
    ctx = WorkflowContext(client=MagicMock(), session_id="s", workflow_version="1")
    ctx._recorded_failures.append(
        ((id(ending), ValueError, "not a number"), "x", 1, False)
    )
    caplog.clear()
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        ctx.note_run_ended(ending)
    assert "resuming this session raises it again" not in caplog.text
    ctx._recorded_failures.append(
        (_identity(ending), "x", 1, False)  # type: ignore[arg-type]
    )
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        ctx.note_run_ended(ending)
    assert "resuming this session raises it again" in caplog.text


def test_a_release_retried_by_the_record_is_timed_from_the_heartbeat_stopping(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The lease's lapse is counted from when the record is held, after the
    heartbeat stopped, so a renewal in flight at the failed release cannot
    outlast the wait."""
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=MagicMock(),
        session_id="s",
        workflow_version="1",
        sequence=1,
        replayed_steps={},
        history_known=True,
    )
    ctx._in_flight = 1
    lease = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 21.0}
    import time

    before = time.monotonic()
    ctx.defer_failure_record(1, "charge", ValueError("x"), None, unreleased=lease)
    held = ctx._pending_hit
    assert held is not None and held["unreleased"]["lapses_at"] >= before + 21.0
    assert "lapses_at" not in lease  # the caller's copy is left alone


@pytest.mark.background_records
def test_a_workflows_result_does_not_wait_for_its_last_record(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The record is written on its own thread, after the result is returned,
    and `wait_for_record_writes` waits for it."""
    import threading
    import time

    from cellaflow.context import wait_for_record_writes

    _fresh(mock_stub, "sess-background")
    answer = mock_stub.CommitStep.side_effect
    writing = threading.Event()

    def slow_commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.status == FAILED:
            writing.set()
            time.sleep(0.5)
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = slow_commit
    started = time.monotonic()
    assert _caught_then_returns(PaymentDeclined("no funds"))() == "done"
    assert time.monotonic() - started < 0.4
    assert writing.wait(timeout=2)
    wait_for_record_writes()
    assert _commits(mock_stub) == [(1, "charge", FAILED)]


@pytest.mark.background_records
def test_a_workflow_with_nothing_to_record_starts_no_writer(
    mock_stub: MagicMock, mock_client: None
) -> None:
    import threading

    _fresh(mock_stub, "sess-nothing")

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def wf() -> str:
        return charge()  # type: ignore[no-any-return]

    before = {t.name for t in threading.enumerate()}
    assert wf() == "charged"
    assert "cellaflow-record" not in {t.name for t in threading.enumerate()} - before


def test_steps_failing_together_stop_recording_rather_than_skip_a_position(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The first failure overlaps the other step, so it is not recorded and its
    position stays empty. The second must not be recorded above that gap: the
    run stops recording, as for a hit, and later failures are not held."""
    _fresh(mock_stub, "sess-gather")

    @step
    async def a() -> str:
        await asyncio.sleep(0.01)  # b has claimed its position by now
        raise PaymentDeclined("a")

    @step
    async def b() -> str:
        await asyncio.sleep(0.05)
        raise PaymentDeclined("b")

    @workflow(version="1.0.0")
    async def wf() -> bool:
        await asyncio.gather(a(), b(), return_exceptions=True)
        from cellaflow.context import get_context

        ctx = get_context()
        return bool(ctx.history_known or ctx._pending_hit is not None)

    run_wf: Any = wf
    assert asyncio.run(run_wf()) is False
    assert not any(status == FAILED for _, _, status in _commits(mock_stub))


def test_a_cancellation_records_how_long_the_step_ran_not_its_cleanup(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The release after a failure can be slow; it is not part of how long the
    step ran before it was cancelled, which a replay waits to reproduce."""
    import time

    _fresh(mock_stub, "sess-slow-release")

    def slow_release(req: Any, **kwargs: Any) -> Any:
        time.sleep(0.4)
        return idempotency_pb2.ReleaseLeaseResponse()

    mock_stub.ReleaseLease.side_effect = slow_release

    @step
    async def charge() -> str:
        await asyncio.sleep(5)
        return "charged"

    @step
    async def fallback() -> str:
        return "fallback"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.1)
        except asyncio.TimeoutError:
            return await fallback()  # type: ignore[no-any-return]

    assert asyncio.run(wf()) == "fallback"
    first = mock_stub.CommitStep.call_args_list[0][0][0]
    record = deserialize(first.step_result.output_payload)["cancelled"]
    assert record["elapsed_s"] < 0.3


# --- Review fixes, round seven --------------------------------------------------


def test_a_recorded_cancellation_refuses_the_resume_and_says_how_to_fix_it(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A replay cannot tell what cancelled the step, a timeout around it or
    something outside the run, so it refuses at once rather than guess or wait."""
    import time

    from cellaflow import FailureNotReplayableError

    _history(mock_stub, "sess-cancel-refused", [_cancelled_at(1, 0.05, 30.0)])
    ran: List[str] = []

    @step
    async def charge() -> str:
        ran.append("charge")
        return "charged"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            return await asyncio.wait_for(charge(), 10)
        except asyncio.TimeoutError:
            return "timed out"

    run_wf: Any = wf
    started = time.monotonic()
    with pytest.raises(FailureNotReplayableError, match="inside the step's body"):
        asyncio.run(run_wf(_session_id="sess-cancel-refused"))
    assert time.monotonic() - started < 1.0
    assert ran == []


class NotFound(Exception):
    pass


def test_a_refusal_the_workflow_catches_still_stops_the_run(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A recorded failure exists because the workflow caught it, so the refusal
    is raised inside the workflow's own `try`. A broad `except` catching it must
    not let the run carry on: the next step refuses before its body runs."""
    from cellaflow import FailureNotReplayableError

    failed = capture(NotFound("x"))
    failed["type"] = "no_such_module:NotFound"
    _history(
        mock_stub,
        "sess-swallowed",
        [
            common_pb2.StepResult(
                sequence=1,
                name="charge",
                status=FAILED,
                output_payload=serialize({"error": failed}),
            )
        ],
    )
    ran: List[str] = []

    @step
    def charge() -> str:
        ran.append("charge")
        return "charged"

    @step
    def fallback() -> str:
        ran.append("fallback")
        return "fell back"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except Exception:
            return fallback()  # type: ignore[no-any-return]

    with pytest.raises(FailureNotReplayableError):
        wf(_session_id="sess-swallowed")  # type: ignore[call-arg]
    assert ran == []


def test_a_refusal_swallowed_with_no_step_after_it_ends_the_run(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow import ReplayDivergedError

    _history(mock_stub, "sess-swallowed-end", [_succeeded(1, "warmup", "ok")])

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except Exception:
            pass
        return "carried on"

    with pytest.raises(ReplayDivergedError):
        wf(_session_id="sess-swallowed-end")  # type: ignore[call-arg]


def test_a_cancellation_raised_out_of_a_synchronous_step_replays_as_itself(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """There it is the step's own exception (an `asyncio.run` inside the step,
    say), so it is recorded as a failure, and replays."""
    _fresh(mock_stub, "sess-sync-cancel")

    @step
    def fetch() -> str:
        raise asyncio.CancelledError("inner run cancelled")

    @step
    def fallback() -> str:
        return "fell back"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            return fetch()  # type: ignore[no-any-return]
        except asyncio.CancelledError:
            return fallback()  # type: ignore[no-any-return]

    assert wf() == "fell back"
    first = mock_stub.CommitStep.call_args_list[0][0][0]
    record = deserialize(first.step_result.output_payload)
    assert "error" in record and "cancelled" not in record
    rebuilt = _round_trip(asyncio.CancelledError("inner run cancelled"))
    assert type(rebuilt) is asyncio.CancelledError


class RetryLater(Exception):
    __slots__ = ("retry_after", "reason")
    retry_after: int
    reason: str

    def __init__(self, retry_after: int) -> None:
        super().__init__("retry later")
        self.retry_after = retry_after


def test_fields_kept_in_slots_are_recorded_and_replayed() -> None:
    """`vars` does not show them, so they were dropped without a warning."""
    from cellaflow.failures import UnavailableOnReplay

    recorded = capture(RetryLater(30))
    assert recorded["attrs"]["retry_after"] == 30
    rebuilt: Any = _round_trip(RetryLater(30))
    assert type(rebuilt) is RetryLater and rebuilt.retry_after == 30
    with pytest.raises(AttributeError):
        rebuilt.reason  # never set, so not set on replay either
    assert not isinstance(getattr(rebuilt, "retry_after"), UnavailableOnReplay)


@pytest.mark.background_records
def test_a_new_run_of_a_session_waits_for_its_last_record(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Otherwise it could read the history before the record lands, and put its
    own next step on that position."""
    import threading
    import time

    _fresh(mock_stub, "sess-rerun")
    order: List[str] = []
    answer = mock_stub.CommitStep.side_effect

    def slow_commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.status == FAILED:
            time.sleep(0.3)
            order.append("record written")
        return answer(req, **kwargs)

    def start(req: Any, **kwargs: Any) -> Any:
        order.append("session started")
        return service_pb2.StartSessionResponse(
            session_id="sess-rerun", version="1.0.0", is_recovered=False
        )

    mock_stub.CommitStep.side_effect = slow_commit
    mock_stub.StartSession.side_effect = start
    wf = _caught_then_returns(PaymentDeclined("no funds"))
    wf(_session_id="sess-rerun")
    writers = [t for t in threading.enumerate() if t.name == "cellaflow-record"]
    assert writers and all(t.daemon for t in writers)
    wf(_session_id="sess-rerun")
    assert order[:3] == ["session started", "record written", "session started"]


# --- Review fixes, round eight --------------------------------------------------


def test_a_cancellation_nothing_follows_is_not_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """No position needs filling, and a resume refuses at a recorded
    cancellation: recording it would only stop the finished session from being
    replayed, even to read its result."""
    _fresh(mock_stub, "sess-cancel-last")

    @step
    async def charge() -> str:
        await asyncio.sleep(5)
        return "charged"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.05)
        except asyncio.TimeoutError:
            return "timed out"

    assert asyncio.run(wf()) == "timed out"
    assert _commits(mock_stub) == []


def test_waiting_for_record_writes_has_one_deadline_for_all() -> None:
    import threading
    import time

    from cellaflow import context

    stalled = [
        threading.Thread(target=time.sleep, args=(1.0,), daemon=True) for _ in range(3)
    ]
    for writer in stalled:
        writer.start()
    context._writers["sess-stalled"] = set(stalled)
    try:
        started = time.monotonic()
        context.wait_for_record_writes(0.3, session_id="sess-stalled")
        assert time.monotonic() - started < 0.6
    finally:
        del context._writers["sess-stalled"]


@pytest.mark.background_records
def test_a_writer_that_cannot_start_never_costs_the_workflows_result(
    mock_stub: MagicMock, mock_client: None, monkeypatch: Any, caplog: Any
) -> None:
    import threading

    from cellaflow import context

    original = threading.Thread.start

    def refuse(self: threading.Thread) -> None:
        if self.name == "cellaflow-record":
            raise RuntimeError("can't start new thread")
        original(self)

    monkeypatch.setattr(threading.Thread, "start", refuse)
    _fresh(mock_stub, "sess-no-thread")
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert _caught_then_returns(PaymentDeclined("no funds"))() == "done"
    assert "could not start writing the failure" in caplog.text
    assert "sess-no-thread" not in context._writers


def test_a_steps_own_nondeterministic_error_replays_without_stopping_the_run(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A step that drives another workflow can fail with a replay error of its
    own. The workflow caught it the first time; replaying it is not a refusal
    of this run."""
    from cellaflow import ReplayDivergedError

    recorded = capture(ReplayDivergedError("the inner workflow diverged"))
    _history(
        mock_stub,
        "sess-inner",
        [
            common_pb2.StepResult(
                sequence=1,
                name="run_inner",
                status=FAILED,
                output_payload=serialize({"error": recorded}),
            )
        ],
    )
    ran: List[str] = []

    @step
    def run_inner() -> str:
        ran.append("run_inner")
        return "inner done"

    @step
    def report() -> str:
        ran.append("report")
        return "reported"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            return run_inner()  # type: ignore[no-any-return]
        except ReplayDivergedError:
            return report()  # type: ignore[no-any-return]

    assert wf(_session_id="sess-inner") == "reported"  # type: ignore[call-arg]
    assert ran == ["report"]


def test_a_failed_steps_exception_is_not_replaced_while_its_heartbeat_stops(
    mock_stub: MagicMock, mock_client: None, monkeypatch: Any
) -> None:
    """Stopping the heartbeat is not awaited, so a timeout that expires then
    cannot land in the step and replace its own exception with a cancellation."""

    async def slow_stop(self: Any) -> None:
        await asyncio.sleep(0.3)

    monkeypatch.setattr("cellaflow.lease.LeaseHeartbeat.stop_async", slow_stop)
    _fresh(mock_stub, "sess-stop")

    @step
    async def charge() -> str:
        await asyncio.sleep(0.05)
        raise PaymentDeclined("no funds")

    @step
    async def fallback() -> str:
        return "fell back"

    @workflow(version="1.0.0")
    async def wf() -> str:
        try:
            return await asyncio.wait_for(charge(), 0.15)
        except PaymentDeclined:
            return await fallback()  # type: ignore[no-any-return]

    assert asyncio.run(wf()) == "fell back"
    first = mock_stub.CommitStep.call_args_list[0][0][0]
    assert "error" in deserialize(first.step_result.output_payload)
