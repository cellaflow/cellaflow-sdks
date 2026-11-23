"""An org over its allowance is refused with RESOURCE_EXHAUSTED and status
details saying so. The SDK raises that as `AllowanceExceeded`, at once,
wherever the engine's error would have surfaced: it is never sent again, never
waited out as an answer that may have been lost, and never recorded as a step's
failure. A plain RESOURCE_EXHAUSTED, such as a session's full mailbox, is
retried as before."""

import asyncio
import builtins
import collections
import copy
import pickle
import sys
import threading
import time
from concurrent import futures
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock

import grpc
import pytest

import cellaflow
import cellaflow.decorators
import cellaflow.lease
from cellaflow import (
    AllowanceExceeded,
    CellaflowClient,
    LeaseLostError,
    async_execution_lease,
    current_lease,
    durable_tools,
    execution_lease,
    step,
    task_lease,
    tool,
    workflow,
)
from cellaflow.allowance import (
    STATUS_DETAILS_KEY,
    _carries_refusal,
    _is_allowance_refusal,
    _refusal_from,
    _rfc3339,
)
from cellaflow.client import _RequestFuture
from cellaflow.context import WorkflowContext, _may_pass, get_context
from cellaflow.decorators import _hand_back, _hand_back_later, _unanswered
from cellaflow.lease import LeaseHeartbeat
from cellaflow.serialization import serialize
from cellaflow.v1 import common_pb2, idempotency_pb2, service_pb2
from tests.test_engine_wait import Answer, Clocks, WaitClient, acquired, session

# --- The status details, written out by hand ----------------------------------
#
# Byte by byte rather than with the SDK's own message classes, so the decoder is
# tested against the wire format rather than against itself.


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        low, n = n & 0x7F, n >> 7
        if not n:
            out.append(low)
            return bytes(out)
        out.append(low | 0x80)


def _field(number: int, payload: bytes) -> bytes:
    """A length-delimited field."""
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def _text(number: int, text: str) -> bytes:
    return _field(number, text.encode("utf-8"))


def detail(type_name: str, value: bytes, prefix: str = "type.googleapis.com/") -> bytes:
    """A `google.protobuf.Any`."""
    return _text(1, prefix + type_name) + _field(2, value)


def quota_failure(*subjects: str, prefix: str = "type.googleapis.com/") -> bytes:
    violations = (_field(1, _text(1, s) + _text(2, "over")) for s in subjects)
    return detail("google.rpc.QuotaFailure", b"".join(violations), prefix)


def error_info(
    domain: str = "cellaflow.com",
    reason: str = "ALLOWANCE_EXCEEDED",
    prefix: str = "type.googleapis.com/",
    **metadata: str,
) -> bytes:
    entries = (_field(3, _text(1, k) + _text(2, v)) for k, v in metadata.items())
    value = _text(1, reason) + _text(2, domain) + b"".join(entries)
    return detail("google.rpc.ErrorInfo", value, prefix)


def status(*details: bytes, message: str = "free step allowance used up") -> bytes:
    """A `google.rpc.Status` with code RESOURCE_EXHAUSTED (8)."""
    code = _varint(1 << 3) + _varint(8)
    return code + _text(2, message) + b"".join(_field(3, d) for d in details)


RESETS_AT = "2026-12-01T00:00:00Z"
RESET = datetime(2026, 12, 1, tzinfo=timezone.utc)
MESSAGE = "free step allowance used up"


def allowance_details() -> bytes:
    return status(
        quota_failure("cellaflow.steps"),
        error_info(resets_at=RESETS_AT, limit="writes_blocked"),
    )


class EngineError(grpc.RpcError):  # type: ignore[misc]
    """An engine call's error as the client receives it, trailer included."""

    def __init__(
        self,
        code: grpc.StatusCode = grpc.StatusCode.RESOURCE_EXHAUSTED,
        details: str = MESSAGE,
        trailer: Tuple[Any, ...] = (),
    ) -> None:
        super().__init__(details)
        self._code, self._details, self._trailer = code, details, trailer

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> Tuple[Any, ...]:
        return self._trailer


def over_allowance(raw: Optional[bytes] = None) -> EngineError:
    """The engine refusing an org over its allowance."""
    raw = allowance_details() if raw is None else raw
    return EngineError(trailer=((STATUS_DETAILS_KEY, raw),))


def mailbox_full() -> EngineError:
    """A plain RESOURCE_EXHAUSTED: a session's full mailbox, which passes."""
    return EngineError(details="session mailbox is full")


def refusal() -> AllowanceExceeded:
    """What the client raises for `over_allowance()`."""
    found = _refusal_from(over_allowance())
    assert found is not None
    return found


def refused_with(raw: bytes) -> AllowanceExceeded:
    found = _refusal_from(over_allowance(raw))
    assert found is not None
    return found


# --- Reading the details --------------------------------------------------------


def test_an_allowance_refusal_is_read_with_everything_it_says() -> None:
    error = over_allowance()
    refused = _refusal_from(error)
    assert isinstance(refused, AllowanceExceeded)
    assert refused.subject == "cellaflow.steps"
    assert refused.resets_at == RESET
    assert refused.resets_at is not None
    assert refused.resets_at.utcoffset() == timedelta(0)
    assert refused.limit == "writes_blocked"
    assert refused.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert refused.details() == MESSAGE
    assert refused.trailing_metadata() == error.trailing_metadata()
    assert refused.__cause__ is error
    text = str(refused)
    assert "The Cellaflow org is over its allowance for cellaflow.steps" in text
    assert "resets at 2026-12-01T00:00:00Z" in text
    assert text.endswith(f"The engine said: {MESSAGE}")


@pytest.mark.parametrize(
    "error",
    [
        EngineError(),
        EngineError(trailer=(("x-request-id", "abc"),)),
        EngineError(grpc.StatusCode.UNAVAILABLE, trailer=over_allowance()._trailer),
        EngineError(
            grpc.StatusCode.FAILED_PRECONDITION, trailer=over_allowance()._trailer
        ),
        over_allowance(b"\xff\xff\xff\xff"),
        over_allowance(allowance_details()[:-5]),
        EngineError(
            trailer=((STATUS_DETAILS_KEY, allowance_details().decode("latin-1")),)
        ),
        over_allowance(status()),
        over_allowance(status(error_info(resets_at=RESETS_AT))),
        over_allowance(
            status(
                quota_failure(
                    "storage.googleapis.com/x",
                    "cellaflow",
                    "Cellaflow.steps",
                    "xcellaflow.steps",
                    "",
                )
            )
        ),
        over_allowance(status(quota_failure())),
        over_allowance(
            status(
                detail("google.rpc.QuotaFailureX", _field(1, _text(1, "cellaflow.x")))
            )
        ),
        over_allowance(
            status(
                detail("google.rpc.QuotaFailure/", _field(1, _text(1, "cellaflow.x")))
            )
        ),
        over_allowance(
            status(detail("google.rpc.ErrorInfo", _field(1, _text(1, "cellaflow.x"))))
        ),
    ],
    ids=[
        "no trailer",
        "other trailers only",
        "the details on UNAVAILABLE",
        "the details on FAILED_PRECONDITION",
        "bytes that do not decode",
        "details cut short",
        "details that are not bytes",
        "no details",
        "only the ErrorInfo",
        "no subject starting with cellaflow.",
        "a QuotaFailure without violations",
        "another type with a longer name",
        "a type URL ending in a slash",
        "a violation under another type",
    ],
)
def test_anything_else_is_the_error_it_was(error: grpc.RpcError) -> None:
    assert _refusal_from(error) is None


def test_reading_never_raises() -> None:
    class NoCode(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            raise RuntimeError("no code")

    class OddTrailer(EngineError):
        def trailing_metadata(self) -> Tuple[Any, ...]:
            return (object(),)

    assert _refusal_from(NoCode()) is None
    assert _refusal_from(OddTrailer()) is None
    assert _refusal_from(grpc.RpcError()) is None
    assert _refusal_from(ValueError("not an engine error")) is None
    refused = refusal()
    assert _refusal_from(refused) is refused


@pytest.mark.parametrize(
    "raw",
    [
        status(quota_failure("cellaflow.steps")),
        status(
            quota_failure("cellaflow.steps"),
            error_info(domain="example.com", resets_at=RESETS_AT, limit="x"),
        ),
        status(
            quota_failure("cellaflow.steps"),
            error_info(reason="QUOTA", resets_at=RESETS_AT, limit="x"),
        ),
        status(quota_failure("cellaflow.steps"), error_info()),
        status(quota_failure("cellaflow.steps"), error_info(limit="")),
        status(
            quota_failure("cellaflow.steps"),
            detail("google.rpc.ErrorInfo", b"\xff\xff\xff"),
        ),
    ],
    ids=[
        "no ErrorInfo",
        "an ErrorInfo from another domain",
        "an ErrorInfo with another reason",
        "an ErrorInfo without metadata",
        "an empty limit",
        "an ErrorInfo that does not decode",
    ],
)
def test_an_error_info_it_cannot_use_leaves_the_reset_and_limit_unknown(
    raw: bytes,
) -> None:
    refused = refused_with(raw)
    assert refused.subject == "cellaflow.steps"
    assert refused.resets_at is None
    assert refused.limit is None
    assert "resets" not in str(refused)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2026-12-01T00:00:00Z", RESET),
        ("2026-12-01t00:00:00z", RESET),
        ("2026-12-01T01:30:00+01:30", RESET),
        ("2026-11-30T19:00:00-05:00", RESET),
        ("2026-12-01T00:00:00.5Z", RESET + timedelta(microseconds=500_000)),
        ("2026-12-01T00:00:00.123456789Z", RESET + timedelta(microseconds=123_456)),
    ],
)
def test_resets_at_is_read_as_utc(text: str, expected: datetime) -> None:
    refused = refused_with(
        status(quota_failure("cellaflow.steps"), error_info(resets_at=text))
    )
    assert refused.resets_at == expected
    assert refused.resets_at is not None
    assert refused.resets_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "tomorrow",
        "1764547200",
        "2026-12-01",
        "2026-12-01T00:00:00",
        "2026-12-01 00:00:00Z",
        " 2026-12-01T00:00:00Z",
        "2026-12-01T00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-02-30T00:00:00Z",
        "2026-12-31T23:59:60Z",
        "2026-12-01T24:00:00Z",
        "2026-12-01T00:00:00+24:00",
        "2026-12-01T00:00:00+05:60",
        "0000-12-01T00:00:00Z",
        "２０２６-12-01T00:00:00Z",
    ],
)
def test_a_reset_that_does_not_parse_is_unknown_and_the_rest_still_reads(
    text: str,
) -> None:
    refused = refused_with(
        status(
            quota_failure("cellaflow.steps"),
            error_info(resets_at=text, limit="new_sessions_blocked"),
        )
    )
    assert refused.resets_at is None
    assert refused.limit == "new_sessions_blocked"


@pytest.mark.parametrize(
    "prefix", ["type.googleapis.com/", "", "https://example.com/types/", "a/b/c/"]
)
def test_a_detail_is_matched_by_the_name_after_the_last_slash(prefix: str) -> None:
    refused = refused_with(
        status(
            quota_failure("cellaflow.steps", prefix=prefix),
            error_info(prefix=prefix, resets_at=RESETS_AT, limit="writes_blocked"),
        )
    )
    assert refused.subject == "cellaflow.steps"
    assert refused.resets_at == RESET
    assert refused.limit == "writes_blocked"


@pytest.mark.parametrize(
    "raw,subject",
    [
        (
            status(
                quota_failure("other.thing", "cellaflow.storage", "cellaflow.steps")
            ),
            "cellaflow.storage",
        ),
        (
            status(
                quota_failure("other.thing"),
                quota_failure("cellaflow.steps"),
                quota_failure("cellaflow.storage"),
            ),
            "cellaflow.steps",
        ),
        (
            status(
                detail("google.rpc.QuotaFailure", b"\xff\xff\xff"),
                quota_failure("cellaflow.steps"),
            ),
            "cellaflow.steps",
        ),
    ],
    ids=["within one QuotaFailure", "across several", "after one that does not decode"],
)
def test_the_first_violation_about_a_cellaflow_allowance_counts(
    raw: bytes, subject: str
) -> None:
    assert refused_with(raw).subject == subject


def test_the_first_error_info_about_the_allowance_counts() -> None:
    refused = refused_with(
        status(
            error_info(domain="example.com", resets_at="2030-01-01T00:00:00Z"),
            quota_failure("cellaflow.steps"),
            error_info(resets_at=RESETS_AT, limit="writes_blocked"),
            error_info(resets_at="2031-01-01T00:00:00Z", limit="new_sessions_blocked"),
        )
    )
    assert refused.resets_at == RESET
    assert refused.limit == "writes_blocked"


def test_the_details_are_read_without_touching_protobufs_default_pool() -> None:
    """So an application that also imports `google.rpc` from
    googleapis-common-protos does not find those files defined twice."""
    import sys

    from google.protobuf import descriptor_pool

    refusal()
    for file, module in (
        ("google/rpc/status.proto", "google.rpc.status_pb2"),
        ("google/rpc/error_details.proto", "google.rpc.error_details_pb2"),
    ):
        if module in sys.modules:
            continue  # imported by something else in this process
        with pytest.raises(KeyError):
            descriptor_pool.Default().FindFileByName(file)


# --- The exception --------------------------------------------------------------


def test_it_is_exported_and_still_a_grpc_rpc_error() -> None:
    assert cellaflow.AllowanceExceeded is AllowanceExceeded
    assert "AllowanceExceeded" in cellaflow.__all__
    try:
        raise refusal()
    except grpc.RpcError as caught:
        assert isinstance(caught, AllowanceExceeded)
        assert caught.code() == grpc.StatusCode.RESOURCE_EXHAUSTED


def test_it_pickles_without_the_call_it_came_from() -> None:
    # gRPC's own metadata type cannot be pickled, and neither can a live call.
    metadatum = collections.namedtuple("metadatum", "key value")
    trailer = (metadatum(STATUS_DETAILS_KEY, allowance_details()),)

    class LiveCall(EngineError):
        def __init__(self, trailer: Tuple[Any, ...]) -> None:
            super().__init__(trailer=trailer)
            self.lock = threading.Lock()

    original = LiveCall(trailer)
    refused = _refusal_from(original)
    assert refused is not None
    copy = pickle.loads(pickle.dumps(refused))
    assert type(copy) is AllowanceExceeded
    assert (copy.subject, copy.resets_at, copy.limit) == (
        "cellaflow.steps",
        RESET,
        "writes_blocked",
    )
    assert copy.details() == MESSAGE
    assert copy.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert copy.trailing_metadata() == ((STATUS_DETAILS_KEY, allowance_details()),)
    assert str(copy) == str(refused)
    assert copy.__cause__ is None
    # The original keeps its own trailer.
    assert refused.trailing_metadata() is trailer


def test_its_message_leaves_out_what_the_engine_did_not_say() -> None:
    bare = AllowanceExceeded(subject="cellaflow.steps")
    assert str(bare) == (
        "The Cellaflow org is over its allowance for cellaflow.steps, so the "
        "engine refused this call."
    )
    assert bare.debug_error_string() == "RESOURCE_EXHAUSTED: "


# --- The client -----------------------------------------------------------------


@pytest.fixture
def mock_stub(monkeypatch: Any) -> MagicMock:
    stub = MagicMock()
    stub.StartSession.side_effect = lambda req, **_: service_pb2.StartSessionResponse(
        session_id=req.session_id or "s", version=req.version
    )
    stub.CheckIdempotencyCache.return_value = acquired(456, 10_000)
    stub.CommitStep.side_effect = lambda req, **_: service_pb2.CommitStepResponse(
        session_id=req.session_id, next_sequence=req.step_result.sequence + 1
    )
    stub.GetGraph.return_value = service_pb2.GetGraphResponse()
    stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(released=True)
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: stub,
    )
    return stub


REQUESTS: List[Tuple[str, Callable[[CellaflowClient], Any]]] = [
    ("StartSession", lambda c: c.start_session("wf", "1.0.0")),
    (
        "CommitStep",
        lambda c: c.commit_step("s", 1, "charge", common_pb2.STEP_STATUS_SUCCESS, {}),
    ),
    ("GetGraph", lambda c: c.get_graph("s")),
    ("CheckIdempotencyCache", lambda c: c.check_idempotency_cache("a", "k")),
    ("RenewLease", lambda c: c.renew_lease("a", "k", 7, 1000)),
    ("ReleaseLease", lambda c: c.release_lease("a", "k", 7)),
]


@pytest.mark.parametrize("method,send", REQUESTS, ids=[m for m, _ in REQUESTS])
def test_every_request_raises_an_allowance_refusal_as_allowance_exceeded(
    mock_stub: MagicMock, method: str, send: Callable[[CellaflowClient], Any]
) -> None:
    error = over_allowance()
    getattr(mock_stub, method).side_effect = error
    with pytest.raises(AllowanceExceeded) as refused:
        send(CellaflowClient())
    assert refused.value.__cause__ is error
    assert refused.value.subject == "cellaflow.steps"
    assert getattr(mock_stub, method).call_count == 1


@pytest.mark.parametrize("method,send", REQUESTS, ids=[m for m, _ in REQUESTS])
def test_every_other_error_is_raised_as_it_was(
    mock_stub: MagicMock, method: str, send: Callable[[CellaflowClient], Any]
) -> None:
    error = mailbox_full()
    getattr(mock_stub, method).side_effect = error
    with pytest.raises(grpc.RpcError) as raised:
        send(CellaflowClient())
    assert raised.value is error


def test_a_stub_that_returns_no_future_is_left_as_it_is(mock_stub: MagicMock) -> None:
    """A test double's answer stays recognisable as not a future, so an async
    step asks it on a thread instead."""
    answer = object()
    mock_stub.CheckIdempotencyCache.future.return_value = answer
    assert CellaflowClient().check_idempotency_cache_future("a", "k") is answer


# --- Over the real transport ----------------------------------------------------


class Engine:
    """An engine stand-in on a real gRPC server that refuses every call it is
    told to refuse: as an org over its allowance, or, with no details, as a
    plain RESOURCE_EXHAUSTED. Everything else is granted."""

    def __init__(self) -> None:
        self.refuse = {"StartSession", "CheckIdempotencyCache"}
        self.details: Optional[bytes] = allowance_details()
        self.calls: List[str] = []

    def answer(self, name: str, request: Any, context: Any) -> Any:
        self.calls.append(name)
        if name in self.refuse:
            if self.details is not None:
                context.set_trailing_metadata(((STATUS_DETAILS_KEY, self.details),))
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, MESSAGE)
        if name == "StartSession":
            return service_pb2.StartSessionResponse(
                session_id=request.session_id or "s", version=request.version
            )
        if name == "CheckIdempotencyCache":
            return acquired(9, 60_000)
        if name == "CommitStep":
            return service_pb2.CommitStepResponse(session_id=request.session_id)
        if name == "ReleaseLease":
            return idempotency_pb2.ReleaseLeaseResponse(released=True)
        raise AssertionError(name)


@pytest.fixture
def engine() -> Iterator[Tuple[str, Engine]]:
    stand_in = Engine()
    methods: Dict[str, Tuple[Any, Any]] = {
        "StartSession": (
            service_pb2.StartSessionRequest,
            service_pb2.StartSessionResponse,
        ),
        "CommitStep": (service_pb2.CommitStepRequest, service_pb2.CommitStepResponse),
        "CheckIdempotencyCache": (
            idempotency_pb2.CheckCacheRequest,
            idempotency_pb2.CheckCacheResponse,
        ),
        "ReleaseLease": (
            idempotency_pb2.ReleaseLeaseRequest,
            idempotency_pb2.ReleaseLeaseResponse,
        ),
    }

    def handler(name: str) -> Callable[[Any, Any], Any]:
        return lambda request, context: stand_in.answer(name, request, context)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "cellaflow.v1.WorkflowEngineService",
                {
                    name: grpc.unary_unary_rpc_method_handler(
                        handler(name),
                        request_deserializer=request.FromString,
                        response_serializer=response.SerializeToString,
                    )
                    for name, (request, response) in methods.items()
                },
            ),
        )
    )
    port = server.add_insecure_port("localhost:0")
    server.start()
    try:
        yield f"localhost:{port}", stand_in
    finally:
        server.stop(None)


def test_an_engines_refusal_raises_allowance_exceeded_through_the_client(
    engine: Tuple[str, Engine],
) -> None:
    target, _ = engine
    client = CellaflowClient(target=target)
    with pytest.raises(grpc.RpcError) as caught:
        client.start_session("wf", "1.0.0")
    refused = caught.value
    assert isinstance(refused, AllowanceExceeded)
    assert (refused.subject, refused.resets_at, refused.limit) == (
        "cellaflow.steps",
        RESET,
        "writes_blocked",
    )
    assert refused.details() == MESSAGE
    assert (STATUS_DETAILS_KEY, allowance_details()) in [
        tuple(m) for m in refused.trailing_metadata()
    ]
    # The transport's own error, which holds the call, is its cause, and is
    # left out of a pickle.
    assert isinstance(refused.__cause__, grpc.RpcError)
    assert not isinstance(refused.__cause__, AllowanceExceeded)
    copy = pickle.loads(pickle.dumps(refused))
    assert (copy.subject, copy.resets_at, copy.limit) == (
        refused.subject,
        refused.resets_at,
        refused.limit,
    )
    client.close()


def test_a_plain_resource_exhausted_comes_through_the_client_as_it_was(
    engine: Tuple[str, Engine],
) -> None:
    target, stand_in = engine
    stand_in.details = None
    client = CellaflowClient(target=target)
    with pytest.raises(grpc.RpcError) as caught:
        client.start_session("wf", "1.0.0")
    assert not isinstance(caught.value, AllowanceExceeded)
    assert caught.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    client.close()


def test_the_future_request_fails_with_allowance_exceeded_too(
    engine: Tuple[str, Engine],
) -> None:
    target, _ = engine
    client = CellaflowClient(target=target)
    request = client.check_idempotency_cache_future("a", "k", timeout=5.0)
    assert isinstance(request, grpc.Future)
    seen: List[Any] = []
    done = threading.Event()

    def on_done(future: Any) -> None:
        seen.append(future)
        done.set()

    request.add_done_callback(on_done)
    with pytest.raises(AllowanceExceeded) as refused:
        request.result()
    assert done.wait(5.0)
    assert seen == [request]
    # The same error however it is asked for.
    assert request.exception() is request.exception()
    assert request.exception() is refused.value
    assert request.done() and not request.cancelled() and not request.running()
    # The call's own attributes pass through.
    assert request.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert request.traceback() is not None
    client.close()


def test_a_plain_error_on_the_future_request_is_the_calls_own(
    engine: Tuple[str, Engine],
) -> None:
    target, stand_in = engine
    stand_in.details = None
    client = CellaflowClient(target=target)
    request = client.check_idempotency_cache_future("a", "k", timeout=5.0)
    with pytest.raises(grpc.RpcError) as caught:
        request.result()
    assert not isinstance(caught.value, AllowanceExceeded)
    assert caught.value is request.exception()
    assert caught.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    client.close()


def test_the_future_request_answers_as_the_call_does(
    engine: Tuple[str, Engine],
) -> None:
    target, stand_in = engine
    stand_in.refuse = set()
    client = CellaflowClient(target=target)
    request = client.check_idempotency_cache_future("a", "k", timeout=5.0)
    assert request.result().fencing_token == 9
    assert request.exception() is None
    assert not request.cancel()
    client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("allowance", [True, False])
async def test_an_async_step_waiting_on_the_future_fails_at_once(
    engine: Tuple[str, Engine], allowance: bool
) -> None:
    target, stand_in = engine
    if not allowance:
        stand_in.details = None
    client = CellaflowClient(target=target)
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )

    async def charge() -> str:
        return "charged"

    with ctx.bind():
        with pytest.raises(grpc.RpcError) as caught:
            await step(charge)()
    asks = stand_in.calls.count("CheckIdempotencyCache")
    if allowance:
        assert isinstance(caught.value, AllowanceExceeded)
        assert asks == 1
        assert not ctx.has_pending_failure()
    else:
        assert not isinstance(caught.value, AllowanceExceeded)
        assert asks == 3
    client.close()


# --- Every call site ------------------------------------------------------------


FAILED = common_pb2.StepStatus.STEP_STATUS_FAILED
SUCCESS = common_pb2.StepStatus.STEP_STATUS_SUCCESS


def _commits(mock_stub: MagicMock) -> List[Tuple[int, str, int]]:
    reqs = [c[0][0] for c in mock_stub.CommitStep.call_args_list]
    return [
        (r.step_result.sequence, r.step_result.name, r.step_result.status) for r in reqs
    ]


def _keys(mock: MagicMock) -> List[str]:
    return [c[0][0].idempotency_key for c in mock.call_args_list]


def _asks_for(mock_stub: MagicMock, name: str) -> int:
    return sum(f":{name}:" in k for k in _keys(mock_stub.CheckIdempotencyCache))


def _refuse_step(
    mock_stub: MagicMock, name: str, error: Callable[[], BaseException]
) -> None:
    """Refuses every request for the step `name`'s lease with `error()`."""
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if f":{name}:" in req.idempotency_key:
            raise error()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check


@pytest.fixture
def lost_answers(monkeypatch: Any) -> List[str]:
    """The keys `_lost_answer` was asked to keep a possibly granted lease for."""
    kept: List[str] = []
    original = cellaflow.decorators._lost_answer

    def spy(agent_id: str, ikey: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        kept.append(ikey)
        return original(agent_id, ikey, *args, **kwargs)

    monkeypatch.setattr(cellaflow.decorators, "_lost_answer", spy)
    return kept


def _refused_then_carries_on(asynchronous: bool) -> Callable[..., Any]:
    """A workflow that catches a refused step and carries on with another."""
    if asynchronous:

        @tool
        async def charge() -> str:
            return "charged"

        @tool
        async def notify() -> str:
            return "notified"

        @workflow(version="1.0.0")
        async def checkout_async() -> str:
            try:
                await charge()
            except grpc.RpcError:
                return await notify()  # type: ignore[no-any-return]
            return "charged"

        return checkout_async

    @step
    def charge_sync() -> str:
        return "charged"

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge_sync()
        except grpc.RpcError:
            return notify_sync()  # type: ignore[no-any-return]
        return "charged"

    return checkout


async def _run(wf: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = wf(*args, **kwargs)
    return await result if asyncio.iscoroutine(result) else result


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_lease_fails_the_step_at_once_and_is_not_recorded(
    mock_stub: MagicMock, lost_answers: List[str], asynchronous: bool
) -> None:
    name = "charge" if asynchronous else "charge_sync"
    notify = "notify" if asynchronous else "notify_sync"
    _refuse_step(mock_stub, name, over_allowance)
    assert await _run(_refused_then_carries_on(asynchronous)) == "notified"
    assert _asks_for(mock_stub, name) == 1
    assert lost_answers == []
    # Nothing recorded: the next step takes the refused step's position.
    assert _commits(mock_stub) == [(1, notify, SUCCESS)]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_full_mailbox_is_still_asked_again_and_recorded(
    mock_stub: MagicMock, lost_answers: List[str], asynchronous: bool
) -> None:
    name = "charge" if asynchronous else "charge_sync"
    notify = "notify" if asynchronous else "notify_sync"
    _refuse_step(mock_stub, name, mailbox_full)
    assert await _run(_refused_then_carries_on(asynchronous)) == "notified"
    assert _asks_for(mock_stub, name) == 3
    assert len(lost_answers) == 3
    assert _commits(mock_stub) == [(1, name, FAILED), (2, notify, SUCCESS)]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_session_start_fails_the_workflow_at_once(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    mock_stub.StartSession.side_effect = over_allowance()
    with pytest.raises(AllowanceExceeded):
        await _run(_refused_then_carries_on(asynchronous))
    assert mock_stub.StartSession.call_count == 1
    assert mock_stub.CheckIdempotencyCache.call_count == 0


def test_a_refused_durable_tools_session_start_raises_at_once(
    mock_stub: MagicMock,
) -> None:
    mock_stub.StartSession.side_effect = over_allowance()
    with pytest.raises(AllowanceExceeded):
        with durable_tools("thread-1"):
            pass  # pragma: no cover
    assert mock_stub.StartSession.call_count == 1


@pytest.mark.parametrize("allowance", [True, False])
def test_a_resumed_threads_position_read_refused_fails_the_call(
    mock_stub: MagicMock, allowance: bool
) -> None:
    mock_stub.StartSession.side_effect = None
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="thread-1-cellaflow-tools", version="1.0.0", is_recovered=True
    )
    mock_stub.GetGraph.side_effect = over_allowance() if allowance else mailbox_full()

    @tool
    def charge() -> str:
        return "charged"

    with durable_tools("thread-1"):
        if allowance:
            with pytest.raises(AllowanceExceeded):
                charge()
            assert mock_stub.CheckIdempotencyCache.call_count == 0
        else:
            # An error that may pass: the call goes ahead, as before.
            assert charge() == "charged"
    assert mock_stub.GetGraph.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["sync", "async", "task_lease"])
async def test_a_refused_execution_lease_raises_at_once(
    mock_stub: MagicMock, form: str
) -> None:
    mock_stub.CheckIdempotencyCache.side_effect = over_allowance()
    ran: List[str] = []
    with pytest.raises(AllowanceExceeded):
        if form == "sync":
            with execution_lease("task:1", worker_id="w-1"):
                ran.append("body")  # pragma: no cover
        elif form == "async":
            async with async_execution_lease("task:1", worker_id="w-1"):
                ran.append("body")  # pragma: no cover
        else:

            @task_lease("task:1", worker_id="w-1")
            def process() -> None:
                ran.append("body")  # pragma: no cover

            process()
    assert ran == []
    assert mock_stub.CheckIdempotencyCache.call_count == 1
    assert mock_stub.ReleaseLease.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_commit_is_not_sent_again_nor_recorded(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """Not sent again by the commit, nor by the resend that follows a release
    the engine said was no longer this call's."""
    name = "charge" if asynchronous else "charge_sync"
    answer = mock_stub.CommitStep.side_effect

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == name:
            raise over_allowance()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=False
    )
    assert await _run(_refused_then_carries_on(asynchronous)) == "notified"
    attempts = [c for c in _commits(mock_stub) if c[1] == name]
    assert attempts == [(1, name, SUCCESS)]
    assert _commits(mock_stub)[-1][0] == 1
    assert mock_stub.ReleaseLease.call_count == 1


def test_a_full_mailbox_on_the_commit_is_sent_again(mock_stub: MagicMock) -> None:
    answer = mock_stub.CommitStep.side_effect
    failures = [mailbox_full()]

    def commit(req: Any, **kwargs: Any) -> Any:
        if failures:
            raise failures.pop()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    assert _refused_then_carries_on(False)() == "charged"
    assert _commits(mock_stub) == [(1, "charge_sync", SUCCESS)] * 2


class PaymentDeclined(Exception):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refusal_raised_inside_a_step_is_not_recorded_and_a_rerun_runs_it(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """From an inner workflow's session start here; an inner step's refusal
    propagates the same way."""
    ran: List[str] = []
    resumed = [False]

    def start(req: Any, **kwargs: Any) -> Any:
        if req.workflow_id == "fulfil":
            raise over_allowance()
        return service_pb2.StartSessionResponse(
            session_id=req.session_id, version=req.version, is_recovered=resumed[0]
        )

    mock_stub.StartSession.side_effect = start

    @workflow(version="1.0.0")
    def fulfil() -> str:
        return "fulfilled"  # pragma: no cover

    if asynchronous:

        @step
        async def ship_async() -> str:
            ran.append("ship")
            return fulfil()

        @workflow(version="1.0.0")
        async def order_async() -> str:
            try:
                return await ship_async()  # type: ignore[no-any-return]
            except AllowanceExceeded:
                return "later"

        wf: Callable[..., Any] = order_async
    else:

        @step
        def ship() -> str:
            ran.append("ship")
            return fulfil()

        @workflow(version="1.0.0")
        def order() -> str:
            try:
                return ship()  # type: ignore[no-any-return]
            except AllowanceExceeded:
                return "later"

        wf = order

    assert await _run(wf, _session_id="order-1") == "later"
    assert _commits(mock_stub) == []
    # Resumed: nothing was recorded at the step's position, so it runs again.
    resumed[0] = True
    assert await _run(wf, _session_id="order-1") == "later"
    assert mock_stub.GetGraph.call_count == 1
    assert ran == ["ship", "ship"]


def _held_hit(name: str) -> Any:
    """A cache hit for a session one position behind it: recorded at the next
    step's start."""
    return idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        cached_result=common_pb2.StepResult(
            sequence=1, name=name, output_payload=serialize({"result": "shared"})
        ),
        current_sequence=0,
    )


@pytest.mark.parametrize("kind", ["result", "failure"])
def test_a_refused_record_is_kept_for_after_the_reset_and_not_retried_meanwhile(
    mock_stub: MagicMock, kind: str
) -> None:
    granted = mock_stub.CheckIdempotencyCache.return_value
    marker = ":record:" if kind == "result" else ":failure:"
    refusals = [over_allowance()]

    def check(req: Any, **kwargs: Any) -> Any:
        if marker in req.idempotency_key and refusals:
            raise refusals.pop()
        if kind == "result" and ":lookup:" in req.idempotency_key:
            return _held_hit("lookup")
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check
    outcomes: List[str] = []

    @step
    def lookup() -> str:
        if kind == "failure":
            raise PaymentDeclined("no funds")
        return "looked up"  # pragma: no cover

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            outcomes.append(lookup())
        except PaymentDeclined:
            outcomes.append("declined")
        try:
            notify()
        except AllowanceExceeded:
            outcomes.append("over allowance")
        # The allowance has reset.
        return notify()  # type: ignore[no-any-return]

    assert wf() == "notified"
    assert outcomes[1:] == ["over allowance"]
    # Asked once while refused, once more after: nothing looped on it.
    assert sum(marker in k for k in _keys(mock_stub.CheckIdempotencyCache)) == 2
    status = SUCCESS if kind == "result" else FAILED
    assert _commits(mock_stub) == [(1, "lookup", status), (2, "notify", SUCCESS)]


def test_the_last_record_refused_is_dropped_at_once_naming_the_allowance(
    mock_stub: MagicMock, caplog: Any
) -> None:
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if ":failure:" in req.idempotency_key:
            raise over_allowance()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check

    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except PaymentDeclined:
            pass
        return "done"

    started = time.monotonic()
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "done"
    assert time.monotonic() - started < 2.0
    assert sum(":failure:" in k for k in _keys(mock_stub.CheckIdempotencyCache)) == 1
    assert "was not recorded" in caplog.text
    assert "over its allowance for cellaflow.steps" in caplog.text
    assert _commits(mock_stub) == []


def test_the_last_record_is_still_retried_after_a_full_mailbox(
    mock_stub: MagicMock, monkeypatch: Any
) -> None:
    monkeypatch.setattr("cellaflow.context._FINAL_RETRY_S", 0.01)
    granted = mock_stub.CheckIdempotencyCache.return_value
    failures = [mailbox_full(), mailbox_full()]

    def check(req: Any, **kwargs: Any) -> Any:
        if ":failure:" in req.idempotency_key and failures:
            raise failures.pop()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check

    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except PaymentDeclined:
            pass
        return "done"

    assert wf() == "done"
    assert sum(":failure:" in k for k in _keys(mock_stub.CheckIdempotencyCache)) == 3
    assert _commits(mock_stub) == [(1, "charge", FAILED)]


def _refuse_releases(mock_stub: MagicMock, name: str) -> List[bool]:
    """Refuses every release of the step `name`'s lease for the allowance while
    the returned flag is set. Clearing it brings the allowance back."""
    refusing = [True]

    def release(req: Any, **kwargs: Any) -> Any:
        if f":{name}:" in req.idempotency_key and refusing[0]:
            raise over_allowance()
        return idempotency_pb2.ReleaseLeaseResponse(released=True)

    mock_stub.ReleaseLease.side_effect = release
    return refusing


def _releases_of(mock_stub: MagicMock, name: str) -> List[Any]:
    return [
        c[0][0]
        for c in mock_stub.ReleaseLease.call_args_list
        if f":{name}:" in c[0][0].idempotency_key
    ]


def test_a_refused_release_is_kept_and_sent_again_once_the_allowance_is_back(
    mock_stub: MagicMock,
) -> None:
    """The failed step's lease still claims its position, so its failure record
    sends the release again before it is written. Refused again, the next call
    fails with the refusal at once rather than waiting out the lease (about 40
    s here). Once the allowance is back the release goes through, token and
    all, and the record is written."""
    refusing = _refuse_releases(mock_stub, "charge")
    took: List[float] = []

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
            pass
        started = time.monotonic()
        try:
            notify()
        except AllowanceExceeded:
            took.append(time.monotonic() - started)
        refusing[0] = False
        return notify()  # type: ignore[no-any-return]

    assert wf() == "notified"
    assert len(took) == 1 and took[0] < 1.0
    assert [r.fencing_token for r in _releases_of(mock_stub, "charge")] == [456] * 3
    # The record's lease is asked for only once the release went through.
    assert sum(":failure:" in k for k in _keys(mock_stub.CheckIdempotencyCache)) == 1
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def test_a_refused_release_of_a_lease_that_has_lapsed_no_longer_blocks(
    mock_stub: MagicMock,
) -> None:
    _refuse_releases(mock_stub, "charge")

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
            # As if the lease's lifetime had passed since.
            pending = get_context()._pending_hit
            assert pending is not None
            assert pending["unreleased"]["token"] == 456
            pending["unreleased"]["lapses_at"] = time.monotonic() - 1.0
        return notify()  # type: ignore[no-any-return]

    assert wf() == "notified"
    assert len(_releases_of(mock_stub, "charge")) == 2
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def test_the_last_record_behind_a_refused_release_is_dropped_without_waiting(
    mock_stub: MagicMock, caplog: Any
) -> None:
    _refuse_releases(mock_stub, "charge")

    @step
    def charge() -> str:
        raise PaymentDeclined("no funds")

    @workflow(version="1.0.0")
    def wf() -> str:
        try:
            charge()
        except PaymentDeclined:
            pass
        return "done"

    started = time.monotonic()
    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "done"
    assert time.monotonic() - started < 2.0
    assert len(_releases_of(mock_stub, "charge")) == 2
    assert "over its allowance for cellaflow.steps" in caplog.text
    assert _commits(mock_stub) == []


def test_a_lease_handed_back_and_refused_for_the_allowance_keeps_its_token() -> None:
    hb = LeaseHeartbeat(
        client=MagicMock(),
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=5000,
    )
    for error in (refusal(), over_allowance(), mailbox_full()):
        client = MagicMock()
        client.release_lease.side_effect = error
        ctx = WorkflowContext(client=client, session_id="s", workflow_version="1")
        held = _hand_back(ctx, "a", "k", 7, "LEASE_UNCONFIRMED", hb)
        assert held is not None
        assert held["token"] == 7
        assert _hand_back_later(ctx, "a", "k", 7, hb)["token"] == 7


# --- The heartbeat --------------------------------------------------------------


def _heartbeat(client: Any, lost: List[str]) -> LeaseHeartbeat:
    return LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        # Renews every 0.1 s; the lease lasts 0.8 s.
        heartbeat_interval_ms=200,
        on_lease_lost=lambda: lost.append("lost"),
    )


def _wait_for(condition: Callable[[], bool], within_s: float = 3.0) -> None:
    deadline = time.monotonic() + within_s
    while not condition():
        assert time.monotonic() < deadline, "the condition never held"
        time.sleep(0.01)


async def _until(condition: Callable[[], bool], within_s: float = 3.0) -> None:
    deadline = time.monotonic() + within_s
    while not condition():
        assert time.monotonic() < deadline, "the condition never held"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_renewal_refused_for_the_allowance_leaves_the_lease_to_its_deadline(
    asynchronous: bool,
) -> None:
    """Renewing stops at the refusal. The lease is not lost then: the engine
    holds it until its deadline, which the heartbeat goes on watching, so the
    loss is reported on time, once."""
    client = MagicMock()
    refused = refusal()
    client.renew_lease.side_effect = refused
    lost_at: List[float] = []
    started = time.monotonic()
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        # Renews every 0.1 s; the lease lasts 0.8 s, so its deadline is 0.7 s
        # from now, half an interval early.
        heartbeat_interval_ms=200,
        on_lease_lost=lambda: lost_at.append(time.monotonic() - started),
    )
    if asynchronous:
        hb.start_async()
        await _until(lambda: client.renew_lease.call_count == 1)
        await asyncio.sleep(max(0.0, 0.4 - (time.monotonic() - started)))
        assert not hb.lost and lost_at == []
        await _until(lambda: bool(lost_at))
        await asyncio.sleep(0.3)
        assert hb._async_task is not None and hb._async_task.done()
        await hb.stop_async()
    else:
        hb.start_sync()
        _wait_for(lambda: client.renew_lease.call_count == 1)
        time.sleep(max(0.0, 0.4 - (time.monotonic() - started)))
        assert not hb.lost and lost_at == []
        _wait_for(lambda: bool(lost_at))
        time.sleep(0.3)
        assert hb._sync_thread is not None and not hb._sync_thread.is_alive()
        hb.stop_sync()
    assert hb.allowance_refusal is refused
    assert client.renew_lease.call_count == 1
    assert len(lost_at) == 1 and 0.65 <= lost_at[0] < 1.0
    assert hb.lost_detail is not None
    assert "over its allowance for cellaflow.steps" in hb.lost_detail


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_renewal_that_finds_a_full_mailbox_is_tried_again(
    asynchronous: bool,
) -> None:
    client = MagicMock()
    client.renew_lease.side_effect = mailbox_full()
    lost: List[str] = []
    hb = _heartbeat(client, lost)
    if asynchronous:
        hb.start_async()
        await asyncio.sleep(0.45)
        await hb.stop_async()
    else:
        hb.start_sync()
        time.sleep(0.45)
        hb.stop_sync()
    assert client.renew_lease.call_count >= 2
    assert lost == []
    assert hb.allowance_refusal is None


class RefusesRenewals(WaitClient):
    """Refuses every renewal of lease 7 because the org is over its allowance."""

    def renew_lease(self, **kw: Any) -> Any:  # type: ignore[override]
        if kw["fencing_token"] == 7:
            self.note("renew:7")
            raise refusal()
        return super().renew_lease(**kw)


@pytest.fixture
def clocks(monkeypatch: pytest.MonkeyPatch) -> Clocks:
    fake = Clocks()
    monkeypatch.setattr(cellaflow.decorators, "time", fake)
    monkeypatch.setattr(cellaflow.lease, "time", fake)
    return fake


def test_confirming_a_grant_stops_at_an_allowance_refusal() -> None:
    client = RefusesRenewals([])
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=250,
        lease_ttl_ms=10_000,
    )
    hb.confirm_grant()
    assert client.order == ["renew:7"]
    # Not lost: the call hands the lease back and fails with the refusal.
    assert not hb.lost
    assert isinstance(hb.allowance_refusal, AllowanceExceeded)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("waited_s", [1.0, 10.0])
async def test_a_late_grant_whose_renewal_is_refused_fails_the_call_with_it(
    clocks: Clocks, asynchronous: bool, waited_s: float
) -> None:
    """The lease is handed back, and the call fails with the refusal rather
    than asking for another lease the engine would refuse the same way. After
    a 1 s wait the 3 s lease is not yet past its deadline, so only the refusal
    stops the body; after 10 s the deadline has passed as well."""
    client = RefusesRenewals(
        [
            Answer(acquired(7, 750), waited_s=waited_s, expires_in_ms=2900),
            Answer(acquired(8)),
        ],
        clocks,
    )

    def charge_card() -> str:
        client.note("body")
        return "charged"

    async def charge_card_async() -> str:
        return charge_card()

    with session(client).bind() as ctx:
        with pytest.raises(AllowanceExceeded):
            if asynchronous:
                await step(charge_card_async, lease_ttl_ms=3000)()
            else:
                step(charge_card, lease_ttl_ms=3000)()
        assert not ctx.has_pending_failure()
    assert len(client.acquires) == 1
    assert client.order.count("renew:7") == 1
    assert [(r["fencing_token"], r["reason"]) for r in client.releases] == [
        (7, "LEASE_UNCONFIRMED")
    ]
    assert "body" not in client.order


# --- The LangGraph saver --------------------------------------------------------


def _saver() -> Any:
    from cellaflow.langgraph import CellaflowSaver

    return CellaflowSaver(client=CellaflowClient())


def test_the_saver_raises_a_refused_session_start(mock_stub: MagicMock) -> None:
    mock_stub.StartSession.side_effect = over_allowance()
    with pytest.raises(AllowanceExceeded):
        _saver()._commit_with_retry("thread-1", "checkpoint:1", {"record_type": "x"})
    assert mock_stub.StartSession.call_count == 1
    assert mock_stub.CommitStep.call_count == 0


def test_the_saver_still_goes_on_after_a_session_start_that_failed_otherwise(
    mock_stub: MagicMock,
) -> None:
    mock_stub.StartSession.side_effect = mailbox_full()
    assert _saver()._commit_with_retry("thread-1", "checkpoint:1", {}) == 1


def test_the_saver_raises_a_refused_commit_at_once(mock_stub: MagicMock) -> None:
    mock_stub.CommitStep.side_effect = over_allowance()
    with pytest.raises(AllowanceExceeded):
        _saver()._commit_with_retry("thread-1", "checkpoint:1", {})
    assert mock_stub.CommitStep.call_count == 1


def test_the_saver_raises_a_refused_position_read_after_a_conflict(
    mock_stub: MagicMock,
) -> None:
    mock_stub.CommitStep.side_effect = EngineError(
        grpc.StatusCode.FAILED_PRECONDITION, "sequence 1 is taken"
    )
    reads: List[Any] = [service_pb2.GetGraphResponse(), over_allowance()]

    def get_graph(req: Any, **kwargs: Any) -> Any:
        answer = reads.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    mock_stub.GetGraph.side_effect = get_graph
    with pytest.raises(AllowanceExceeded):
        _saver()._commit_with_retry("thread-1", "checkpoint:1", {})
    assert mock_stub.CommitStep.call_count == 1


# --- A renewal refused while the lease is held ----------------------------------


def _renewals_refused(mock_stub: MagicMock, interval_ms: int = 250) -> None:
    """Grants every lease and refuses every renewal for the allowance."""
    mock_stub.CheckIdempotencyCache.return_value = acquired(456, interval_ms)
    mock_stub.RenewLease.side_effect = over_allowance()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_body_that_finishes_before_the_deadline_still_commits(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """A 1 s lease renewed every 0.15 s, its deadline 0.875 s in: the body
    outlasts a renewal, which is refused, and still commits."""
    _renewals_refused(mock_stub)

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    async def charge() -> str:
        await asyncio.sleep(0.4)
        current_lease().check()
        return "charged"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        return await charge()  # type: ignore[no-any-return]

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    def charge_sync() -> str:
        time.sleep(0.4)
        current_lease().check()
        return "charged"

    @workflow(version="1.0.0")
    def checkout() -> str:
        return charge_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "charged"
    name = "charge" if asynchronous else "charge_sync"
    assert mock_stub.RenewLease.call_count == 1
    assert _commits(mock_stub) == [(1, name, SUCCESS)]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_body_that_outlives_the_deadline_fails_with_the_refusal(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """The lease holds until its deadline, and the body's check passes before
    it. After it, the lease's loss is raised as the refusal behind it, which is
    not recorded: the position goes back to the next call."""
    _renewals_refused(mock_stub)
    checks: List[str] = []
    caught: List[BaseException] = []

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    async def charge() -> str:
        await asyncio.sleep(0.3)
        lease = current_lease()
        lease.check()
        checks.append("held")
        async with lease.cancel_on_loss():
            await asyncio.sleep(5)
        return "charged"  # pragma: no cover

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            return await charge()  # type: ignore[no-any-return]
        except AllowanceExceeded as e:
            caught.append(e)
            return await notify()  # type: ignore[no-any-return]

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    def charge_sync() -> str:
        time.sleep(0.3)
        current_lease().check()
        checks.append("held")
        time.sleep(0.8)
        current_lease().check()
        return "charged"  # pragma: no cover

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge_sync()  # type: ignore[no-any-return]
        except AllowanceExceeded as e:
            caught.append(e)
            return notify_sync()  # type: ignore[no-any-return]

    started = time.monotonic()
    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    # Cancelled at the deadline, not after the body's 5 s.
    assert time.monotonic() - started < 3.0
    assert checks == ["held"]
    assert len(caught) == 1 and isinstance(caught[0], AllowanceExceeded)
    assert caught[0].subject == "cellaflow.steps"
    assert mock_stub.RenewLease.call_count == 1
    next_step = "notify" if asynchronous else "notify_sync"
    assert _commits(mock_stub) == [(1, next_step, SUCCESS)]
    assert mock_stub.ReleaseLease.call_count == 1


def test_an_execution_lease_whose_renewal_is_refused_is_lost_at_its_deadline(
    mock_stub: MagicMock,
) -> None:
    """Its deadline is 0.9 s in (a 1 s TTL less half a 0.2 s interval)."""
    _renewals_refused(mock_stub, interval_ms=200)
    lost_at: List[float] = []
    started = time.monotonic()
    with execution_lease(
        "task:1",
        worker_id="w-1",
        ttl_ms=1000,
        heartbeat_interval_ms=200,
        on_lease_lost=lambda: lost_at.append(time.monotonic() - started),
    ) as lease:
        _wait_for(lambda: mock_stub.RenewLease.call_count == 1)
        # Still held: the engine keeps it until its deadline.
        lease.check()
        _wait_for(lambda: bool(lost_at))
        with pytest.raises(LeaseLostError) as lost:
            lease.check()
    assert isinstance(lost.value.__cause__, AllowanceExceeded)
    assert "over its allowance for cellaflow.steps" in str(lost.value)
    assert 0.8 <= lost_at[0] < 1.5
    assert mock_stub.RenewLease.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("preempt", [False, True])
async def test_an_async_execution_lease_refused_its_renewal_is_lost_at_its_deadline(
    mock_stub: MagicMock, preempt: bool
) -> None:
    _renewals_refused(mock_stub, interval_ms=200)
    lost_at: List[float] = []
    raised: List[BaseException] = []
    started = time.monotonic()

    async def run() -> None:
        try:
            async with async_execution_lease(
                "task:1",
                worker_id="w-1",
                ttl_ms=1000,
                heartbeat_interval_ms=200,
                on_lease_lost=lambda: lost_at.append(time.monotonic() - started),
                preempt=preempt,
            ) as lease:
                await asyncio.sleep(0.4)
                lease.check()
                if preempt:
                    await asyncio.sleep(5)
                await _until(lambda: bool(lost_at))
                lease.check()
        except (asyncio.CancelledError, LeaseLostError) as e:
            raised.append(e)

    await asyncio.wait_for(asyncio.ensure_future(run()), timeout=4.0)
    assert 0.8 <= lost_at[0] < 1.5
    assert mock_stub.RenewLease.call_count == 1
    if preempt:
        # Cancelled when the deadline passed, not at the refusal.
        assert isinstance(raised[0], asyncio.CancelledError)
    else:
        assert isinstance(raised[0], LeaseLostError)
        assert isinstance(raised[0].__cause__, AllowanceExceeded)


# --- A refused resend, a wrapped refusal, a raw one -----------------------------


def test_a_resent_commit_refused_for_the_allowance_raises_the_refusal(
    mock_stub: MagicMock,
) -> None:
    """The first commit gets no answer, and the resend is refused: the call
    fails with the refusal, not the first attempt's error, and nothing is
    recorded."""
    answer = mock_stub.CommitStep.side_effect
    failures: List[BaseException] = [over_allowance(), mailbox_full()]

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == "charge_sync" and failures:
            raise failures.pop()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    caught: List[BaseException] = []

    @step
    def charge_sync() -> str:
        return "charged"

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge_sync()  # type: ignore[no-any-return]
        except grpc.RpcError as e:
            caught.append(e)
            return notify_sync()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    assert isinstance(caught[0], AllowanceExceeded)
    assert _commits(mock_stub) == [
        (1, "charge_sync", SUCCESS),
        (1, "charge_sync", SUCCESS),
        (1, "notify_sync", SUCCESS),
    ]


class OrderFailed(Exception):
    pass


def _wrapping(shape: str) -> BaseException:
    """An error a step body makes from a refusal, in the given shape."""
    if shape == "cause":
        try:
            raise refusal()
        except AllowanceExceeded as e:
            try:
                raise OrderFailed("could not order") from e
            except OrderFailed as wrapped:
                return wrapped
    if shape == "cause of a cause":
        inner = OrderFailed("inner")
        inner.__cause__ = refusal()
        outer = OrderFailed("outer")
        outer.__cause__ = inner
        return outer
    group: Any = getattr(builtins, "ExceptionGroup")
    if shape == "group":
        made: BaseException = group("several", [ValueError("x"), refusal()])
        return made
    nested = group("inner", [OrderFailed("x"), over_allowance()])
    made = group("outer", [ValueError("y"), nested])
    return made


GROUPS = sys.version_info >= (3, 11)


@pytest.mark.parametrize(
    "shape",
    [
        "cause",
        "cause of a cause",
        pytest.param(
            "group", marks=pytest.mark.skipif(not GROUPS, reason="needs 3.11")
        ),
        pytest.param(
            "nested group of a raw refusal",
            marks=pytest.mark.skipif(not GROUPS, reason="needs 3.11"),
        ),
    ],
)
def test_an_error_made_from_a_refusal_is_raised_as_it_was_but_not_recorded(
    mock_stub: MagicMock, shape: str
) -> None:
    caught: List[BaseException] = []

    @step
    def charge() -> str:
        raise _wrapping(shape)

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except BaseException as e:
            caught.append(e)
            return notify()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    # What the caller sees is the body's own error.
    assert not isinstance(caught[0], AllowanceExceeded)
    # Not recorded, and the position went back.
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


def test_an_error_raised_while_handling_a_refusal_is_recorded(
    mock_stub: MagicMock,
) -> None:
    """Only an explicit cause counts: an error raised in an `except` without
    `from` is not made from the refusal."""

    @step
    def charge() -> str:
        try:
            raise refusal()
        except AllowanceExceeded:
            raise OrderFailed("something else went wrong")

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except OrderFailed:
            return notify()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def _chain(links: int) -> BaseException:
    """A refusal `links` explicit causes below the error raised."""
    error: BaseException = refusal()
    for i in range(links):
        wrapper = OrderFailed(str(i))
        wrapper.__cause__ = error
        error = wrapper
    return error


def _nested(levels: int) -> BaseException:
    """A refusal `levels` exception groups down."""
    group: Any = getattr(builtins, "ExceptionGroup")
    error: BaseException = refusal()
    for _ in range(levels):
        error = group("g", [error])
    return error


def test_a_refusal_eight_causes_down_is_found_and_nine_is_not() -> None:
    """The bound the TypeScript SDK's `carriesAllowanceRefusal` uses."""
    assert _carries_refusal(_chain(0))
    assert _carries_refusal(_chain(8))
    assert not _carries_refusal(_chain(9))


@pytest.mark.skipif(not GROUPS, reason="needs 3.11")
def test_a_refusal_eight_groups_down_is_found_and_nine_is_not() -> None:
    assert _carries_refusal(_nested(8))
    assert not _carries_refusal(_nested(9))
    # Causes and groups count as one depth between them.
    wrapper = OrderFailed("wrapper")
    wrapper.__cause__ = _nested(7)
    assert _carries_refusal(wrapper)
    wrapper.__cause__ = _nested(8)
    assert not _carries_refusal(wrapper)


@pytest.mark.skipif(not GROUPS, reason="needs 3.11")
def test_at_most_64_errors_are_looked_at() -> None:
    group: Any = getattr(builtins, "ExceptionGroup")

    def wide(before: int) -> BaseException:
        made: BaseException = group(
            "wide", [ValueError(str(i)) for i in range(before)] + [refusal()]
        )
        return made

    # The group itself, 62 members, and the refusal: 64 errors.
    assert _carries_refusal(wide(62))
    assert not _carries_refusal(wide(63))
    # A group's members beyond its 64th are not looked at, even when the
    # errors before it are one error repeated, which counts once.
    same = OrderFailed("the same")
    assert _carries_refusal(group("many", [same] * 63 + [refusal()]))
    assert not _carries_refusal(group("many", [same] * 64 + [refusal()]))


def test_a_cycle_or_an_attribute_that_raises_ends_the_search() -> None:
    a, b = OrderFailed("a"), OrderFailed("b")
    a.__cause__, b.__cause__ = b, a
    assert not _carries_refusal(a)

    def unreadable(self: BaseException) -> BaseException:
        raise RuntimeError("cannot read the cause")

    # A class whose `__cause__` raises when read.
    unreadable_cause = type(
        "Unreadable", (Exception,), {"__cause__": property(unreadable)}
    )
    assert not _carries_refusal(unreadable_cause())
    if GROUPS:
        # Only what is behind it is hidden: the search goes on past it.
        group: Any = getattr(builtins, "ExceptionGroup")
        assert _carries_refusal(group("g", [unreadable_cause(), refusal()]))
    context = OrderFailed("context only")
    context.__context__ = refusal()
    assert not _carries_refusal(context)
    assert not _carries_refusal(mailbox_full())


def test_a_raw_refusal_is_recognised_wherever_it_comes_from() -> None:
    raw = over_allowance()
    assert _is_allowance_refusal(raw)
    assert _is_allowance_refusal(refusal())
    assert not _is_allowance_refusal(mailbox_full())
    assert not _is_allowance_refusal(ValueError("x"))
    assert not _may_pass(raw)
    assert not _unanswered(raw)
    assert _may_pass(mailbox_full())


class RawRefusals(WaitClient):
    """A client of its own that raises the engine's error as it arrives,
    without making it `AllowanceExceeded`."""

    def renew_lease(self, **kw: Any) -> Any:  # type: ignore[override]
        self.note(f"renew:{kw['fencing_token']}")
        raise over_allowance()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_raw_refusal_from_another_client_is_final_too(
    asynchronous: bool,
) -> None:
    client = RawRefusals([Answer(None, raises=over_allowance())])

    def charge_card() -> str:
        return "charged"  # pragma: no cover

    async def charge_card_async() -> str:
        return charge_card()  # pragma: no cover

    with session(client).bind() as ctx:
        with pytest.raises(grpc.RpcError) as raised:
            if asynchronous:
                await step(charge_card_async)()
            else:
                step(charge_card)()
        assert not ctx.has_pending_failure()
    assert _is_allowance_refusal(raised.value)
    assert len(client.acquires) == 1


def test_a_raw_refused_renewal_stops_the_heartbeat_with_the_refusal() -> None:
    client = RawRefusals([])
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=250,
        lease_ttl_ms=10_000,
    )
    hb.confirm_grant()
    assert client.order == ["renew:7"]
    assert isinstance(hb.allowance_refusal, AllowanceExceeded)


# --- resets_at, as the TypeScript SDK reads and prints it -----------------------


@pytest.mark.parametrize(
    "moment,printed",
    [
        (RESET, "2026-12-01T00:00:00Z"),
        (RESET + timedelta(milliseconds=500), "2026-12-01T00:00:00.500Z"),
        (RESET + timedelta(microseconds=123_456), "2026-12-01T00:00:00.123Z"),
        (RESET + timedelta(microseconds=999), "2026-12-01T00:00:00Z"),
        (datetime(100, 1, 1, tzinfo=timezone.utc), "0100-01-01T00:00:00Z"),
        (
            datetime(2026, 12, 1, 1, 0, tzinfo=timezone(timedelta(hours=1))),
            "2026-12-01T00:00:00Z",
        ),
    ],
)
def test_the_reset_is_printed_as_the_typescript_sdk_prints_it(
    moment: datetime, printed: str
) -> None:
    refused = AllowanceExceeded(subject="cellaflow.steps", resets_at=moment)
    assert f"The allowance resets at {printed}." in str(refused)
    assert _rfc3339(moment) == printed
    # The datetime keeps its full precision.
    assert refused.resets_at == moment


@pytest.mark.parametrize(
    "text,expected",
    [
        ("0099-12-31T23:59:59Z", None),
        ("0001-01-01T00:00:00Z", None),
        ("0100-01-01T00:00:00Z", datetime(100, 1, 1, tzinfo=timezone.utc)),
        (
            "0100-01-01T00:30:00+01:00",
            datetime(99, 12, 31, 23, 30, tzinfo=timezone.utc),
        ),
    ],
)
def test_a_year_below_100_is_refused_as_typescript_refuses_it(
    text: str, expected: Optional[datetime]
) -> None:
    refused = refused_with(
        status(quota_failure("cellaflow.steps"), error_info(resets_at=text))
    )
    assert refused.resets_at == expected


# --- The future's wrapper -------------------------------------------------------


def test_the_future_wrapper_can_be_copied_and_made_without_a_call() -> None:
    call = MagicMock()
    call.code.return_value = grpc.StatusCode.OK
    copied = copy.copy(_RequestFuture(call))
    assert copied.code() == grpc.StatusCode.OK
    bare = _RequestFuture.__new__(_RequestFuture)
    assert getattr(bare, "code", None) is None
    assert not hasattr(bare, "__setstate_custom__")
