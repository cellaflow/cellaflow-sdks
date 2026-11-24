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
import gc
import itertools
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
import cellaflow.context
import cellaflow.decorators
import cellaflow.lease
from cellaflow import (
    MAX_LEASE_TTL_MS,
    AllowanceExceeded,
    CellaflowClient,
    LeaseHandle,
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
        "9999-12-31T23:00:00-05:00",
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


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "release", ["released", "not released", "refused", "unanswered"]
)
async def test_a_resent_commit_refused_for_the_allowance_raises_the_refusal(
    mock_stub: MagicMock, release: str, asynchronous: bool
) -> None:
    """The first commit gets no answer, and the resend is refused: the call
    fails with the refusal, not the first attempt's error, nothing is recorded,
    and the position is given back. Released, the lease was still the call's,
    so the first commit did not land, and the run goes on recording. Not
    released, or the release refused or unanswered, it may have landed: the run stops
    recording, and the next call at the position is refused its lease before
    its body runs if it did."""
    name = "charge" if asynchronous else "charge_sync"
    answer = mock_stub.CommitStep.side_effect
    failures: List[BaseException] = [over_allowance(), mailbox_full()]

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == name and failures:
            raise failures.pop()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    if release == "not released":
        mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
            released=False
        )
    elif release in ("refused", "unanswered"):
        # Once: the release owed at the position is sent again, and goes
        # through, before the next call claims it.
        failure = (
            over_allowance()
            if release == "refused"
            else EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
        )
        mock_stub.ReleaseLease.side_effect = [
            failure,
            idempotency_pb2.ReleaseLeaseResponse(released=True),
        ]
    caught: List[BaseException] = []
    known: List[bool] = []

    @step
    async def charge() -> str:
        return "charged"

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            return await charge()  # type: ignore[no-any-return]
        except grpc.RpcError as e:
            caught.append(e)
            known.append(get_context().history_known)
            return await notify()  # type: ignore[no-any-return]

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
            known.append(get_context().history_known)
            return notify_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    assert isinstance(caught[0], AllowanceExceeded)
    assert known == [release == "released"]
    next_step = "notify" if asynchronous else "notify_sync"
    assert _commits(mock_stub) == [
        (1, name, SUCCESS),
        (1, name, SUCCESS),
        (1, next_step, SUCCESS),
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
    # Without a call, asking fails plainly instead of recursing.
    with pytest.raises(AttributeError):
        bare.code()
    assert getattr(bare, "debug_error_string", None) is None
    assert not hasattr(bare, "__setstate_custom__")


# --- Positions given back, as the TypeScript SDK gives them back ----------------


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_step_inside_another_gives_both_positions_back(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """`book` (position 1) calls `charge` (position 2), which is refused. The
    refusal passes up through `book`, and each gives its position back as it
    goes, the top one first, so the step the workflow calls next lands at 1
    with the history still known."""
    _refuse_step(mock_stub, "charge" if asynchronous else "charge_sync", over_allowance)
    known: List[bool] = []

    @step
    async def charge() -> str:
        return "charged"  # pragma: no cover

    @step
    async def book() -> str:
        return await charge()  # type: ignore[no-any-return]

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            return await book()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            known.append(get_context().history_known)
            return await notify()  # type: ignore[no-any-return]

    @step
    def charge_sync() -> str:
        return "charged"  # pragma: no cover

    @step
    def book_sync() -> str:
        return charge_sync()  # type: ignore[no-any-return]

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return book_sync()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            known.append(get_context().history_known)
            return notify_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    assert known == [True]
    next_step = "notify" if asynchronous else "notify_sync"
    assert _commits(mock_stub) == [(1, next_step, SUCCESS)]


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["call order", "reverse order"])
async def test_concurrent_refused_calls_give_their_positions_back_in_either_order(
    mock_stub: MagicMock, order: str
) -> None:
    """`first` claims 1 and `second` claims 2, and both are refused. Refused in
    call order, `first` cannot give 1 back past `second`, so it is vacated, and
    the run records nothing meanwhile; when `second` gives 2 back, the counter
    steps down past 1 as well, and recording resumes. In reverse order each
    gives its own back in turn. Either way the next call takes 1."""
    gates = {"first": threading.Event(), "second": threading.Event()}
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        for name, gate in gates.items():
            if f":{name}:" in req.idempotency_key:
                assert gate.wait(5.0)
                raise over_allowance()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check

    @step
    async def first() -> str:
        return "first"  # pragma: no cover

    @step
    async def second() -> str:
        return "second"  # pragma: no cover

    @step
    async def notify() -> str:
        return "notified"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-concurrent",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        a = asyncio.ensure_future(first())
        await _until(lambda: ctx.sequence == 1)
        b = asyncio.ensure_future(second())
        await _until(lambda: ctx.sequence == 2)
        settle = [("first", a), ("second", b)]
        if order == "reverse order":
            settle.reverse()
        recording: List[bool] = []
        for name, task in settle:
            gates[name].set()
            with pytest.raises(AllowanceExceeded):
                await task
            recording.append(ctx.history_known)
        assert recording == ([False, True] if order == "call order" else [True, True])
        assert ctx.sequence == 0
        assert not ctx._vacated
        assert ctx.history_known
        assert await notify() == "notified"
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


@pytest.mark.asyncio
async def test_a_call_refused_below_one_that_commits_leaves_its_position_empty(
    mock_stub: MagicMock,
) -> None:
    """`first` (1) is refused while `second` (2) is still running, and then
    `second` commits. Position 1 can never be given back, so it stays vacated,
    and the run records nothing into the session from then on."""
    gates = {"first": threading.Event(), "second": threading.Event()}
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if ":first:" in req.idempotency_key:
            assert gates["first"].wait(5.0)
            raise over_allowance()
        if ":second:" in req.idempotency_key:
            assert gates["second"].wait(5.0)
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check

    @step
    async def first() -> str:
        return "first"  # pragma: no cover

    @step
    async def second() -> str:
        return "second"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-below",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        a = asyncio.ensure_future(first())
        await _until(lambda: ctx.sequence == 1)
        b = asyncio.ensure_future(second())
        await _until(lambda: ctx.sequence == 2)
        gates["first"].set()
        with pytest.raises(AllowanceExceeded):
            await a
        assert ctx._vacated == {1} and ctx.sequence == 2
        assert not ctx.history_known
        gates["second"].set()
        assert await b == "second"
    assert _commits(mock_stub) == [(2, "second", SUCCESS)]
    assert ctx._vacated == {1}
    assert not ctx.history_known


def _fresh_context() -> WorkflowContext:
    return WorkflowContext(
        client=MagicMock(), session_id="s", workflow_version="1", history_known=True
    )


def test_a_vacated_position_is_cleared_when_a_call_claims_it() -> None:
    """Through the public methods only, as the TypeScript test does: the
    engine's report moves the counter back below a call still in flight, so
    that call, refused, vacates a position above the counter, which the next
    claim then fills, and recording resumes."""
    ctx = _fresh_context()
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    ctx.record_engine_sequence(0)
    assert ctx.begin_call() == 1
    ctx.give_back_refused_position(2)
    ctx.end_call()
    assert not ctx.history_known
    assert ctx.begin_call() == 2
    assert ctx.history_known


def test_adopting_the_engines_position_drops_vacated_ones_for_good() -> None:
    ctx = _fresh_context()
    assert [ctx.begin_call() for _ in range(3)] == [1, 2, 3]
    ctx.give_back_refused_position(2)
    ctx.end_call()
    ctx.record_engine_sequence(5)
    assert ctx.begin_call() == 6
    assert not ctx.history_known
    # Nothing vacated: adopting the engine's position keeps the history known.
    other = _fresh_context()
    other.record_engine_sequence(5)
    assert other.begin_call() == 6
    assert other.history_known


# --- A refused resend after a commit with no answer -----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_resend_refused_after_the_lease_was_said_to_be_gone_raises_it(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """Commit unanswered, resent and unanswered again, the release answered as
    no longer the call's, and the resend that decides it refused for the
    allowance: the caller gets the refusal. The first commit may have landed,
    so nothing is recorded, the position is given back, and the run stops recording
    into the session."""
    name = "charge" if asynchronous else "charge_sync"
    answer = mock_stub.CommitStep.side_effect
    failures: List[BaseException] = [
        over_allowance(),
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"),
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"),
    ]

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == name and failures:
            raise failures.pop()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=False
    )
    caught: List[BaseException] = []
    known: List[bool] = []

    @step
    async def charge() -> str:
        return "charged"

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            return await charge()  # type: ignore[no-any-return]
        except grpc.RpcError as e:
            caught.append(e)
            known.append(get_context().history_known)
            return await notify()  # type: ignore[no-any-return]

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
            known.append(get_context().history_known)
            return notify_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    assert isinstance(caught[0], AllowanceExceeded)
    assert known == [False]
    commits = _commits(mock_stub)
    assert commits[:3] == [(1, name, SUCCESS)] * 3
    assert all(status != FAILED for _, _, status in commits)
    assert commits[-1][0] == 1
    assert mock_stub.ReleaseLease.call_count == 1


def test_a_resend_refused_after_a_definite_commit_failure_raises_that_failure(
    mock_stub: MagicMock,
) -> None:
    """The first commit failed with a definite error, so it did not land, and
    the engine would not release the lease. The resend that tells a landed
    commit from a takeover is refused for the allowance, but the step's real
    failure is the commit's error: the call fails with it, and it is recorded,
    as before."""
    answer = mock_stub.CommitStep.side_effect
    definite = EngineError(grpc.StatusCode.INVALID_ARGUMENT, "not that")
    failures: List[BaseException] = [over_allowance(), definite]

    def commit(req: Any, **kwargs: Any) -> Any:
        if req.step_result.name == "charge_sync" and failures:
            raise failures.pop()
        return answer(req, **kwargs)

    mock_stub.CommitStep.side_effect = commit
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=False
    )
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
    assert caught == [definite]
    assert _commits(mock_stub)[-2:] == [
        (1, "charge_sync", FAILED),
        (2, "notify_sync", SUCCESS),
    ]


# --- The refusal is the loss's cause only when it is the reason -----------------


def test_a_refusal_after_a_loss_for_another_reason_is_not_its_cause() -> None:
    """The holder stalled past its deadline, so the lease was lost on the
    clock; a renewal refused for the allowance only after that does not make
    the refusal the cause."""
    client = MagicMock()
    client.renew_lease.side_effect = refusal()
    long_ago = (time.monotonic() - 5.0, time.time() - 5.0)
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=200,
        acquired_sent_at=long_ago,
    )
    handle = LeaseHandle(
        7, probe=hb.past_deadline, key="k", refusal=lambda: hb.loss_refusal
    )
    with pytest.raises(LeaseLostError) as stalled:
        handle.check()
    hb.start_sync()
    _wait_for(lambda: client.renew_lease.call_count == 1)
    hb.stop_sync()
    assert hb.allowance_refusal is not None
    assert hb.loss_refusal is None
    with pytest.raises(LeaseLostError) as later:
        handle.check()
    for lost in (stalled.value, later.value):
        assert lost.__cause__ is None
        assert "allowance" not in str(lost)


def _stalled_then_refused(mock_stub: MagicMock, interval_ms: int) -> List[str]:
    """The first renewal hangs past the lease's deadline, then is refused for
    the allowance. Returns a list that holds "refused" once it was."""
    mock_stub.CheckIdempotencyCache.return_value = acquired(456, interval_ms)
    refused: List[str] = []

    def renew(req: Any, **kwargs: Any) -> Any:
        time.sleep(1.0)
        refused.append("refused")
        raise over_allowance()

    mock_stub.RenewLease.side_effect = renew
    return refused


def test_an_execution_lease_lost_before_its_renewal_was_refused_is_a_plain_loss(
    mock_stub: MagicMock,
) -> None:
    """Deadline 0.9 s in; the renewal sent at 0.1 s answers at 1.1 s."""
    refused = _stalled_then_refused(mock_stub, 200)
    with execution_lease(
        "task:1", worker_id="w-1", ttl_ms=1000, heartbeat_interval_ms=200
    ) as lease:
        time.sleep(0.95)
        with pytest.raises(LeaseLostError) as first:
            lease.check()
        _wait_for(lambda: bool(refused))
        time.sleep(0.05)
        with pytest.raises(LeaseLostError) as later:
            lease.check()
    for lost in (first.value, later.value):
        assert lost.__cause__ is None
        assert "allowance" not in str(lost)


def test_a_step_lost_before_its_renewal_was_refused_fails_with_the_loss(
    mock_stub: MagicMock,
) -> None:
    """The step's own loss came first, so the call fails with `LeaseLostError`,
    recorded as any failure is, not with the refusal that came after."""
    refused = _stalled_then_refused(mock_stub, 250)

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    def charge() -> str:
        time.sleep(0.95)
        lease = current_lease()
        assert lease.is_lost
        _wait_for(lambda: bool(refused))
        time.sleep(0.05)
        lease.check()
        return "charged"  # pragma: no cover

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except LeaseLostError as e:
            assert e.__cause__ is None
            return notify()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    assert _commits(mock_stub) == [(1, "charge", FAILED), (2, "notify", SUCCESS)]


def test_a_loss_caused_by_the_refusal_says_so_in_the_shared_words() -> None:
    for subject, what in (("cellaflow.steps", " for cellaflow.steps"), ("", "")):
        refused = AllowanceExceeded(subject=subject)
        handle = LeaseHandle(7, probe=lambda: True, key="k", refusal=lambda: refused)
        with pytest.raises(LeaseLostError) as lost:
            handle.check()
        assert str(lost.value) == (
            "Execution lease was lost: the engine refused to renew it because the "
            f"Cellaflow org is over its allowance{what}, and its deadline passed. "
            "The lease may already belong to another worker."
        )
        assert lost.value.__cause__ is refused
    assert str(AllowanceExceeded()) == (
        "The Cellaflow org is over its allowance, so the engine refused this call."
    )


# --- The heartbeat wakes at the deadline ----------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_after_a_refusal_the_heartbeat_wakes_at_the_deadline_not_the_interval(
    asynchronous: bool, caplog: Any
) -> None:
    """Renewals every 0.9 s for a 2 s lease: the first, at 0.9 s, is refused,
    and the deadline is 1.5 s in. Waking only at the next interval would report
    the loss at 1.8 s."""
    client = MagicMock()
    client.renew_lease.side_effect = refusal()
    lost_at: List[float] = []
    started = time.monotonic()
    hb = LeaseHeartbeat(
        client=client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=1000,
        lease_ttl_ms=2000,
        on_lease_lost=lambda: lost_at.append(time.monotonic() - started),
    )
    with caplog.at_level("WARNING", logger="cellaflow.lease"):
        if asynchronous:
            hb.start_async()
            await _until(lambda: bool(lost_at), within_s=4.0)
            await hb.stop_async()
        else:
            hb.start_sync()
            _wait_for(lambda: bool(lost_at), within_s=4.0)
            hb.stop_sync()
    assert client.renew_lease.call_count == 1
    assert 1.4 <= lost_at[0] < 1.65
    assert "is not renewed any more and lasts until its deadline: " in caplog.text


# --- grpc.Call, as the errors and futures it replaces are -----------------------


class FinishedCall(grpc.RpcError, grpc.Call):  # type: ignore[misc]
    """A finished call that answers everything, unlike gRPC's own error."""

    def initial_metadata(self) -> Any:
        return (("x-initial", "1"),)

    def trailing_metadata(self) -> Any:
        return ()

    def code(self) -> Any:
        return grpc.StatusCode.RESOURCE_EXHAUSTED

    def details(self) -> str:
        return MESSAGE

    def is_active(self) -> bool:
        return True

    def time_remaining(self) -> Optional[float]:
        return 1.5

    def cancel(self) -> bool:
        return True

    def add_callback(self, callback: Any) -> bool:
        callback()
        return True


def test_the_refusal_is_a_grpc_call_answering_as_its_call_does() -> None:
    refused = AllowanceExceeded(MESSAGE, subject="cellaflow.steps")
    refused.__cause__ = FinishedCall()
    called: List[str] = []
    assert isinstance(refused, grpc.Call) and isinstance(refused, grpc.RpcError)
    assert refused.initial_metadata() == (("x-initial", "1"),)
    assert refused.is_active() is True
    assert refused.time_remaining() == 1.5
    assert refused.cancel() is True
    assert refused.add_callback(lambda: called.append("cb")) is True
    assert called == ["cb"]
    # Pickled, there is no call: it answers as a finished one.
    copy_ = pickle.loads(pickle.dumps(refused))
    assert isinstance(copy_, grpc.Call)
    assert copy_.initial_metadata() == ()
    assert copy_.is_active() is False
    assert copy_.time_remaining() is None
    assert copy_.cancel() is False
    assert copy_.add_callback(lambda: called.append("again")) is False
    assert called == ["cb"]
    # From a client whose error is not a `grpc.Call`, likewise.
    plain = refusal()
    assert (plain.initial_metadata(), plain.is_active(), plain.cancel()) == (
        (),
        False,
        False,
    )


def test_a_refusal_through_the_real_transport_answers_as_grpcs_error_does(
    engine: Tuple[str, Engine],
) -> None:
    """gRPC's own failed call leaves `is_active`, `time_remaining` and
    `add_callback` unimplemented; the refusal answers them as a finished call."""
    target, _ = engine
    client = CellaflowClient(target=target)
    with pytest.raises(AllowanceExceeded) as caught:
        client.start_session("wf", "1.0.0")
    refused = caught.value
    assert isinstance(refused, grpc.Call)
    assert refused.initial_metadata() == refused.__cause__.initial_metadata()
    assert refused.is_active() is False
    assert refused.time_remaining() is None
    assert refused.cancel() is False
    assert refused.add_callback(lambda: None) is False
    client.close()


def test_the_future_wrapper_is_a_grpc_call_passing_everything_through() -> None:
    call = MagicMock()
    call.initial_metadata.return_value = (("i", "1"),)
    call.trailing_metadata.return_value = (("t", "2"),)
    call.code.return_value = grpc.StatusCode.OK
    call.details.return_value = "fine"
    call.is_active.return_value = True
    call.time_remaining.return_value = 2.5
    call.add_callback.return_value = True
    call.cancel.return_value = True
    wrapped = _RequestFuture(call)
    assert isinstance(wrapped, grpc.Call) and isinstance(wrapped, grpc.Future)
    assert wrapped.initial_metadata() == (("i", "1"),)
    assert wrapped.trailing_metadata() == (("t", "2"),)
    assert wrapped.code() == grpc.StatusCode.OK
    assert wrapped.details() == "fine"
    assert wrapped.is_active() is True
    assert wrapped.time_remaining() == 2.5
    callback = lambda: None  # noqa: E731
    assert wrapped.add_callback(callback) is True
    call.add_callback.assert_called_once_with(callback)
    assert wrapped.cancel() is True


def test_the_real_future_is_a_grpc_call(engine: Tuple[str, Engine]) -> None:
    target, _ = engine
    client = CellaflowClient(target=target)
    request = client.check_idempotency_cache_future("a", "k", timeout=5.0)
    assert isinstance(request, grpc.Call) and isinstance(request, grpc.Future)
    with pytest.raises(AllowanceExceeded):
        request.result()
    assert request.is_active() is False
    assert request.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert request.details() == MESSAGE
    assert request.initial_metadata() is not None
    assert (STATUS_DETAILS_KEY, allowance_details()) in [
        tuple(m) for m in request.trailing_metadata()
    ]
    remaining = request.time_remaining()
    assert remaining is None or remaining >= 0
    assert request.add_callback(lambda: None) is False
    client.close()


# --- Recording never resumes over a dropped failure -----------------------------


@pytest.mark.asyncio
async def test_a_failure_dropped_while_a_position_is_vacated_stops_recording_for_good(
    mock_stub: MagicMock,
) -> None:
    """Calls claim 1, 2 and 3. Call 2 is refused below the top, so 2 is vacated
    and recording waits. Call 1 then fails with an ordinary error, which is not
    recorded. Call 3 is refused at the top, and the counter steps down through
    2 to 1. Position 1 is empty, so recording must not resume."""
    gates = {"second": threading.Event(), "third": threading.Event()}
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        for name, gate in gates.items():
            if f":{name}:" in req.idempotency_key:
                assert gate.wait(5.0)
                raise over_allowance()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check
    go = asyncio.Event()
    entered: List[str] = []

    @step
    async def first() -> str:
        entered.append("first")
        await go.wait()
        raise PaymentDeclined("no funds")

    @step
    async def second() -> str:
        return "second"  # pragma: no cover

    @step
    async def third() -> str:
        return "third"  # pragma: no cover

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-dropped",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        one = asyncio.ensure_future(first())
        await _until(lambda: entered == ["first"])
        two = asyncio.ensure_future(second())
        await _until(lambda: ctx.sequence == 2)
        three = asyncio.ensure_future(third())
        await _until(lambda: ctx.sequence == 3)
        gates["second"].set()
        with pytest.raises(AllowanceExceeded):
            await two
        assert ctx._vacated == {2}
        go.set()
        with pytest.raises(PaymentDeclined):
            await one
        assert not ctx.has_pending_failure()
        gates["third"].set()
        with pytest.raises(AllowanceExceeded):
            await three
        assert ctx.sequence == 1 and not ctx._vacated
        assert not ctx.history_known
    assert _commits(mock_stub) == []


# --- Everything but a refusal gives its position back as before -----------------


def _context_with(sequence: int, in_flight: int) -> WorkflowContext:
    ctx = WorkflowContext(
        client=MagicMock(), session_id="s", workflow_version="1", history_known=True
    )
    ctx.sequence, ctx._in_flight = sequence, in_flight
    return ctx


def _note(ctx: WorkflowContext, seq: int, exc: BaseException) -> None:
    cellaflow.decorators._note_failure(
        ctx, seq, "charge", exc, None, "default", "k", 0.0, 0.0, is_async=False
    )


@pytest.mark.parametrize("exit_", [KeyboardInterrupt(), SystemExit(1)])
def test_a_process_exit_gives_its_position_back_only_when_alone(
    exit_: BaseException,
) -> None:
    """As before the allowance changes: at the top with a lower call in flight,
    or below the top, the position is neither given back nor vacated, and
    recording goes on."""
    for sequence, in_flight, seq in ((2, 2, 2), (2, 2, 1)):
        ctx = _context_with(sequence, in_flight)
        _note(ctx, seq, exit_)
        assert (ctx.sequence, ctx._vacated, ctx.history_known) == (2, set(), True)
    alone = _context_with(2, 1)
    _note(alone, 2, exit_)
    assert alone.sequence == 1 and alone.history_known


def test_a_takeover_below_the_top_is_handled_as_before() -> None:
    """Not given back nor vacated: the failure cannot be held, so its position
    stays empty and the run stops recording, as it always did."""
    ctx = _context_with(2, 2)
    taken = LeaseLostError("lost its lease to another worker", result="charged")
    _note(ctx, 1, taken)
    assert ctx.sequence == 2 and not ctx._vacated
    assert not ctx.history_known and not ctx.has_pending_failure()


def test_a_takeover_below_the_top_then_a_refusal_at_the_top() -> None:
    """The takeover's position is not vacated, so the refusal's hand-back does
    not step through it, and the history stays unknown."""
    ctx = _fresh_context()
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    _note(ctx, 1, LeaseLostError("lost its lease to another worker", result="r"))
    ctx.end_call()
    _note(ctx, 2, refusal())
    ctx.end_call()
    assert ctx.sequence == 1
    assert not ctx.history_known


@pytest.mark.parametrize("exit_", [KeyboardInterrupt(), SystemExit(1)])
def test_a_process_exit_below_the_top_then_a_refusal_at_the_top(
    exit_: BaseException,
) -> None:
    """A process exit below the top is handled as before: not given back, nor
    vacated, so the refusal at the top does not step through its position."""
    ctx = _fresh_context()
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    _note(ctx, 1, exit_)
    ctx.end_call()
    _note(ctx, 2, refusal())
    ctx.end_call()
    assert ctx.sequence == 1


# --- Smaller things -------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_async_execution_lease_lost_before_its_renewal_was_refused_is_plain(
    mock_stub: MagicMock,
) -> None:
    refused = _stalled_then_refused(mock_stub, 200)
    raised: List[LeaseLostError] = []
    async with async_execution_lease(
        "task:1",
        worker_id="w-1",
        ttl_ms=1000,
        heartbeat_interval_ms=200,
        preempt=False,
    ) as lease:
        await asyncio.sleep(0.95)
        for _ in range(2):
            try:
                lease.check()
            except LeaseLostError as e:
                raised.append(e)
            await _until(lambda: bool(refused))
            await asyncio.sleep(0.05)
    assert len(raised) == 2
    for lost in raised:
        assert lost.__cause__ is None
        assert "allowance" not in str(lost)


def test_a_loss_detail_leaves_out_an_empty_subject() -> None:
    hb = LeaseHeartbeat(
        client=MagicMock(),
        agent_id="a",
        idempotency_key="k",
        fencing_token=7,
        heartbeat_interval_ms=200,
        acquired_sent_at=(time.monotonic() - 5.0, time.time() - 5.0),
    )
    hb.allowance_refusal = AllowanceExceeded()
    assert hb.past_deadline()
    assert hb.lost_detail == (
        "no renewal was confirmed in time, because the engine refused them: the "
        "org is over its allowance. The lease may already belong to another worker"
    )


# --- A refused call's lease is released before its position is taken again -----


def _releases_for(mock_stub: MagicMock, name: str) -> List[Any]:
    return [
        c[0][0]
        for c in mock_stub.ReleaseLease.call_args_list
        if f":{name}:" in c[0][0].idempotency_key
    ]


def _release_answers(mock_stub: MagicMock, name: str, answers: List[Any]) -> None:
    """Answers the releases of step `name`'s lease in turn (an exception is
    raised), and every other release as released."""

    def release(req: Any, **kwargs: Any) -> Any:
        if f":{name}:" in req.idempotency_key and answers:
            answer = answers.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return answer
        return idempotency_pb2.ReleaseLeaseResponse(released=True)

    mock_stub.ReleaseLease.side_effect = release


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_steps_unanswered_release_is_sent_again_before_its_position(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """The body's own call is refused, and the TOOL_ERROR release gets no
    answer, so the lease still claims position 1. The next step, a different
    one, sends the release again before it claims 1, and commits there."""
    name = "charge" if asynchronous else "charge_sync"
    _release_answers(
        mock_stub, name, [EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")]
    )

    @step
    async def charge() -> str:
        raise refusal()

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            return await charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            return await notify()  # type: ignore[no-any-return]

    @step
    def charge_sync() -> str:
        raise refusal()

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge_sync()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            return notify_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    assert [r.fencing_token for r in _releases_for(mock_stub, name)] == [456, 456]
    next_step = "notify" if asynchronous else "notify_sync"
    assert _commits(mock_stub) == [(1, next_step, SUCCESS)]


class GuardFirstEngine:
    """An engine model whose allowance guard sits at the top of the lease
    request, before the cache lookup: it refuses even a key a live lease still
    holds. Leases lapse on the clock, and the session is strictly ordered: a
    lease at a position another live lease claims is refused as divergent, and
    a commit anywhere but the next position is refused. The first grant to step
    `lose_grant_of` is made but its answer lost, and the org goes over its
    allowance at that moment."""

    def __init__(self, mock_stub: MagicMock, lose_grant_of: str) -> None:
        self.over = False
        self.committed = 0
        self.leases: Dict[str, Tuple[Optional[int], float]] = {}
        self.divergent: List[int] = []
        self.lose_grant_of: Optional[str] = lose_grant_of
        self.tokens = itertools.count(100)
        mock_stub.CheckIdempotencyCache.side_effect = self.check
        mock_stub.CommitStep.side_effect = self.commit
        mock_stub.ReleaseLease.side_effect = self.release

    def check(self, req: Any, **kwargs: Any) -> Any:
        if self.over:
            raise over_allowance()
        now = time.monotonic()
        self.leases = {k: v for k, v in self.leases.items() if v[1] > now}
        key = req.idempotency_key
        position = req.sequence if req.HasField("sequence") else None
        for holder, (claimed, _) in self.leases.items():
            if position is not None and claimed == position and holder != key:
                self.divergent.append(position)
                raise EngineError(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"position {position} is claimed by another lease",
                )
        ttl_s = (req.lease_ttl_ms or 20_000) / 1000.0
        self.leases[key] = (position, now + ttl_s)
        if self.lose_grant_of and f":{self.lose_grant_of}:" in key:
            self.lose_grant_of = None
            self.over = True
            raise EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
        return acquired(next(self.tokens), 250)

    def commit(self, req: Any, **kwargs: Any) -> Any:
        position = req.step_result.sequence
        if position != self.committed + 1:
            raise EngineError(grpc.StatusCode.FAILED_PRECONDITION, "Sequence mismatch")
        self.committed = position
        self.leases.pop(req.idempotency_key, None)
        return service_pb2.CommitStepResponse(session_id=req.session_id)

    def release(self, req: Any, **kwargs: Any) -> Any:
        released = self.leases.pop(req.idempotency_key, None) is not None
        return idempotency_pb2.ReleaseLeaseResponse(released=released)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_lost_grant_then_a_refused_retry_leaves_the_session_in_order(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """The grant at position 1 is made but its answer lost, and the retry is
    refused by a guard that does not look the key up first, so the refusal
    proves nothing about that lease. It is owed at 1, and once the block lifts
    the next call waits for it to lapse (1 s TTL) rather than be refused as
    divergent behind it; the session then commits in order."""
    name = "charge" if asynchronous else "charge_sync"
    engine = GuardFirstEngine(mock_stub, name)
    waited: List[float] = []

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    async def charge() -> str:
        return "charged"  # pragma: no cover

    @step
    async def notify() -> str:
        return "notified"

    @step
    async def audit() -> str:
        return "audited"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            await charge()
        except AllowanceExceeded:
            engine.over = False
        started = time.monotonic()
        await notify()
        waited.append(time.monotonic() - started)
        return await audit()  # type: ignore[no-any-return]

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    def charge_sync() -> str:
        return "charged"  # pragma: no cover

    @step
    def notify_sync() -> str:
        return "notified"

    @step
    def audit_sync() -> str:
        return "audited"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge_sync()
        except AllowanceExceeded:
            engine.over = False
        started = time.monotonic()
        notify_sync()
        waited.append(time.monotonic() - started)
        return audit_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "audited"
    assert engine.divergent == []
    assert engine.committed == 2
    suffix = "" if asynchronous else "_sync"
    assert [c[:2] for c in _commits(mock_stub)] == [
        (1, "notify" + suffix),
        (2, "audit" + suffix),
    ]
    # Waited out in passes, about as long as the lease could still live.
    assert 1.0 <= waited[0] < 3.0


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_an_owed_release_refused_for_the_allowance_fails_the_next_call_at_once(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    name = "charge" if asynchronous else "charge_sync"
    _release_answers(mock_stub, name, [over_allowance(), over_allowance()])
    outcomes: List[Any] = []

    @step
    async def charge() -> str:
        raise refusal()

    @step
    async def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            await charge()
        except AllowanceExceeded:
            pass
        started = time.monotonic()
        try:
            await notify()
        except AllowanceExceeded:
            ctx = get_context()
            outcomes.append((time.monotonic() - started, ctx.sequence, dict(ctx._owed)))
        # The allowance is back.
        return await notify()  # type: ignore[no-any-return]

    @step
    def charge_sync() -> str:
        raise refusal()

    @step
    def notify_sync() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge_sync()
        except AllowanceExceeded:
            pass
        started = time.monotonic()
        try:
            notify_sync()
        except AllowanceExceeded:
            ctx = get_context()
            outcomes.append((time.monotonic() - started, ctx.sequence, dict(ctx._owed)))
        return notify_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "notified"
    took, sequence, owed = outcomes[0]
    # At once, without claiming the position, and the release still owed.
    assert took < 1.0 and sequence == 0 and list(owed) == [1]
    assert len(_releases_for(mock_stub, name)) == 3
    next_step = "notify" if asynchronous else "notify_sync"
    assert _commits(mock_stub) == [(1, next_step, SUCCESS)]


def test_an_owed_release_unanswered_again_is_sent_again_after_a_second(
    mock_stub: MagicMock,
) -> None:
    """Sent again before the claim and unanswered again: the call waits a
    second, sends it once more, and goes on once it is answered, well before
    the lease would lapse."""
    unavailable = EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
    _release_answers(mock_stub, "charge", [unavailable, unavailable])
    waited: List[float] = []

    @step(lease_ttl_ms=1000)  # type: ignore[untyped-decorator]
    def charge() -> str:
        raise refusal()

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            started = time.monotonic()
            done: str = notify()
            waited.append(time.monotonic() - started)
            return done

    assert checkout() == "notified"
    assert 0.9 <= waited[0] < 2.0
    assert len(_releases_for(mock_stub, "charge")) == 3
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


@pytest.mark.asyncio
async def test_an_owed_release_on_a_vacated_position_is_settled_when_it_is_claimed(
    mock_stub: MagicMock,
) -> None:
    """`first` (1) is refused while `second` (2) runs, and its release gets no
    answer, so 1 is vacated with its release owed. `second` is refused at the
    top, and the counter steps down through 1. The next call sends the owed
    release, then claims 1."""
    _release_answers(
        mock_stub, "first", [EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")]
    )
    first_goes, second_goes = asyncio.Event(), threading.Event()
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if ":second:" in req.idempotency_key:
            assert second_goes.wait(5.0)
            raise over_allowance()
        return granted

    mock_stub.CheckIdempotencyCache.side_effect = check
    entered: List[str] = []

    @step
    async def first() -> str:
        entered.append("first")
        await first_goes.wait()
        raise refusal()

    @step
    async def second() -> str:
        return "second"  # pragma: no cover

    @step
    async def notify() -> str:
        return "notified"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-owed",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        one = asyncio.ensure_future(first())
        await _until(lambda: entered == ["first"])
        two = asyncio.ensure_future(second())
        await _until(lambda: ctx.sequence == 2)
        first_goes.set()
        with pytest.raises(AllowanceExceeded):
            await one
        assert ctx._vacated == {1} and list(ctx._owed) == [1]
        second_goes.set()
        with pytest.raises(AllowanceExceeded):
            await two
        assert ctx.sequence == 0
        assert await notify() == "notified"
        assert ctx._owed == {}
        assert ctx.history_known
    assert len(_releases_for(mock_stub, "first")) == 2
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


# --- The base hand-back steps down too, and a takeover is not vacated -----------


def test_a_process_exit_handed_back_alone_steps_down_through_vacated_positions() -> (
    None
):
    """Refused at 1 while 2 runs, so 1 is vacated; then 2 hands back alone (a
    process exit), and the counter steps down through 1. The next call
    claims 1."""
    ctx = _fresh_context()
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    _note(ctx, 1, refusal())
    ctx.end_call()
    _note(ctx, 2, SystemExit(1))
    ctx.end_call()
    assert ctx.begin_call() == 1
    assert ctx.history_known


def test_a_taken_over_call_refused_is_handed_back_as_a_takeover() -> None:
    """Below the top its position is not vacated, since the takeover's new
    holder may fill it, and the run stops recording; at the top it is handed
    back."""

    def note(ctx: WorkflowContext, seq: int) -> None:
        cellaflow.decorators._note_failure(
            ctx, seq, "charge", refusal(), None, "d", "k", 0.0, 0.0, taken_over=True
        )

    below = _fresh_context()
    assert (below.begin_call(), below.begin_call()) == (1, 2)
    note(below, 1)
    assert below.sequence == 2 and not below._vacated
    assert not below.history_known
    top = _fresh_context()
    assert (top.begin_call(), top.begin_call()) == (1, 2)
    note(top, 2)
    assert top.sequence == 1 and top.history_known


def test_a_step_taken_over_and_refused_hands_its_position_back(
    mock_stub: MagicMock,
) -> None:
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=False
    )

    @step
    def charge() -> str:
        raise refusal()

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            return notify()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


# --- Resuming after a refusal (the reviewer's probe) ----------------------------


def _session_history(mock_stub: MagicMock, refusing: List[bool]) -> Dict[str, Any]:
    """An engine stand-in that keeps the session's history, refuses `charge`
    while `refusing[0]` is set, and reports the session recovered once
    `state["recovered"]` is."""
    state: Dict[str, Any] = {"history": [], "recovered": False}
    granted = mock_stub.CheckIdempotencyCache.return_value

    def check(req: Any, **kwargs: Any) -> Any:
        if ":charge:" in req.idempotency_key and refusing[0]:
            raise over_allowance()
        return granted

    def commit(req: Any, **kwargs: Any) -> Any:
        state["history"].append(req.step_result)
        return service_pb2.CommitStepResponse(session_id=req.session_id)

    mock_stub.CheckIdempotencyCache.side_effect = check
    mock_stub.CommitStep.side_effect = commit
    mock_stub.StartSession.side_effect = lambda req, **_: (
        service_pb2.StartSessionResponse(
            session_id=req.session_id,
            version=req.version,
            is_recovered=state["recovered"],
        )
    )
    mock_stub.GetGraph.side_effect = lambda req, **_: service_pb2.GetGraphResponse(
        steps=list(state["history"])
    )
    return state


def test_a_caught_refusal_then_a_different_step_cannot_be_resumed(
    mock_stub: MagicMock,
) -> None:
    from cellaflow import ReplayDivergedError

    refusing = [True]
    state = _session_history(mock_stub, refusing)

    @step
    def charge() -> str:
        return "charged"

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            return notify()  # type: ignore[no-any-return]

    assert checkout(_session_id="s1") == "notified"  # type: ignore[call-arg]
    assert [(s.sequence, s.name) for s in state["history"]] == [(1, "notify")]
    refusing[0], state["recovered"] = False, True
    with pytest.raises(ReplayDivergedError):
        checkout(_session_id="s1")  # type: ignore[call-arg]


def test_a_refusal_that_ended_the_run_resumes_cleanly(mock_stub: MagicMock) -> None:
    refusing = [True]
    state = _session_history(mock_stub, refusing)

    @step
    def lookup() -> str:
        return "found"

    @step
    def charge() -> str:
        return "charged"

    @workflow(version="1.0.0")
    def checkout() -> str:
        lookup()
        return charge()  # type: ignore[no-any-return]

    with pytest.raises(AllowanceExceeded):
        checkout(_session_id="s2")  # type: ignore[call-arg]
    refusing[0], state["recovered"] = False, True
    assert checkout(_session_id="s2") == "charged"  # type: ignore[call-arg]
    assert [(s.sequence, s.name) for s in state["history"]] == [
        (1, "lookup"),
        (2, "charge"),
    ]


# --- Owed releases, as the TypeScript SDK settles them --------------------------


class StrictEngine:
    """An engine model that orders a session strictly, as the engine does: a
    lease at a position another live lease claims is refused as divergent, a
    commit anywhere but the next position is refused, and a release frees the
    position its lease claimed. Releases of step `fail_release_of`'s lease fail
    with the errors in `release_failures` first."""

    def __init__(self, mock_stub: MagicMock, fail_release_of: str) -> None:
        self.committed = 0
        self.claims: Dict[int, str] = {}
        self.tokens = itertools.count(100)
        self.fail_release_of = fail_release_of
        self.release_failures: List[BaseException] = []
        self.commit_failures: List[BaseException] = []
        mock_stub.CheckIdempotencyCache.side_effect = self.check
        mock_stub.CommitStep.side_effect = self.commit
        mock_stub.ReleaseLease.side_effect = self.release

    def check(self, req: Any, **kwargs: Any) -> Any:
        if req.HasField("sequence"):
            position = req.sequence
            holder = self.claims.get(position)
            if holder is not None and holder != req.idempotency_key:
                raise EngineError(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"position {position} is claimed by another lease",
                )
            self.claims[position] = req.idempotency_key
        return acquired(next(self.tokens), 10_000)

    def commit(self, req: Any, **kwargs: Any) -> Any:
        if f":{self.fail_release_of}:" in req.idempotency_key and self.commit_failures:
            raise self.commit_failures.pop(0)
        position = req.step_result.sequence
        if position != self.committed + 1:
            raise EngineError(grpc.StatusCode.FAILED_PRECONDITION, "Sequence mismatch")
        self.committed = position
        self.claims.pop(position, None)
        return service_pb2.CommitStepResponse(session_id=req.session_id)

    def release(self, req: Any, **kwargs: Any) -> Any:
        key = req.idempotency_key
        if f":{self.fail_release_of}:" in key and self.release_failures:
            raise self.release_failures.pop(0)
        for position, holder in list(self.claims.items()):
            if holder == key:
                del self.claims[position]
        return idempotency_pb2.ReleaseLeaseResponse(released=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_refused_step_whose_release_failed_once_leaves_the_session_in_order(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """The reviewer's scenario against an engine that orders strictly. Without
    the owed release, `notify`'s lease at 1 is refused as divergent (the
    refused step's lease still claims it), and `audit` lands at 2 over an empty
    1, to be refused at its commit after its side effect."""
    name = "charge" if asynchronous else "charge_sync"
    engine = StrictEngine(mock_stub, name)
    engine.release_failures.append(
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
    )
    known: List[bool] = []

    @step
    async def charge() -> str:
        raise refusal()

    @step
    async def notify() -> str:
        return "notified"

    @step
    async def audit() -> str:
        return "audited"

    @step
    async def close() -> str:
        return "closed"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            await charge()
        except AllowanceExceeded:
            pass
        await notify()
        await audit()
        done: str = await close()
        known.append(get_context().history_known)
        return done

    @step
    def charge_sync() -> str:
        raise refusal()

    @step
    def notify_sync() -> str:
        return "notified"

    @step
    def audit_sync() -> str:
        return "audited"

    @step
    def close_sync() -> str:
        return "closed"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge_sync()
        except AllowanceExceeded:
            pass
        notify_sync()
        audit_sync()
        done: str = close_sync()
        known.append(get_context().history_known)
        return done

    assert await _run(checkout_async if asynchronous else checkout) == "closed"
    assert [c[:2] for c in _commits(mock_stub)] == [
        (1, "notify" if asynchronous else "notify_sync"),
        (2, "audit" if asynchronous else "audit_sync"),
        (3, "close" if asynchronous else "close_sync"),
    ]
    assert known == [True]


@pytest.mark.parametrize(
    "answer",
    [
        idempotency_pb2.ReleaseLeaseResponse(released=False),
        EngineError(grpc.StatusCode.NOT_FOUND, "no such lease"),
    ],
    ids=["answered as no longer the call's", "refused for good"],
)
def test_an_owed_release_answered_either_way_is_dropped_and_the_call_goes_on(
    mock_stub: MagicMock, answer: Any
) -> None:
    _release_answers(
        mock_stub,
        "charge",
        [EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"), answer],
    )

    @step
    def charge() -> str:
        raise refusal()

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            return notify()  # type: ignore[no-any-return]

    assert checkout() == "notified"
    assert len(_releases_for(mock_stub, "charge")) == 2
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


def test_a_lapsed_owed_release_is_dropped_unsent() -> None:
    client = MagicMock()
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert ctx.begin_call() == 1
    lapsed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": -1.0}
    ctx.give_back_refused_position(1, lapsed)
    ctx.end_call()
    assert ctx.begin_call() == 1
    client.release_lease.assert_not_called()


def test_an_unclaimed_owed_release_stays_owed() -> None:
    """Below a committed call, nothing claims its position again: the release
    stays owed, and the lease lapses on its own."""
    client = MagicMock()
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    owed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 60.0}
    ctx.give_back_refused_position(1, owed)
    ctx.end_call()
    ctx.end_call()  # 2 committed
    assert ctx.begin_call() == 3
    assert list(ctx._owed) == [1]
    client.release_lease.assert_not_called()


def test_a_takeover_at_the_top_steps_down_through_vacated_positions() -> None:
    ctx = _fresh_context()
    assert (ctx.begin_call(), ctx.begin_call()) == (1, 2)
    _note(ctx, 1, refusal())
    ctx.end_call()
    cellaflow.decorators._note_failure(
        ctx, 2, "charge", refusal(), None, "d", "k", 0.0, 0.0, taken_over=True
    )
    ctx.end_call()
    assert ctx.begin_call() == 1
    assert ctx.history_known


# --- Owed releases are polled, never waited out blind ---------------------------


class _Sleeps:
    """`time` as `cellaflow.context` sees it, with every sleep recorded."""

    def __init__(self) -> None:
        self.slept: List[float] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        time.sleep(seconds)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> _Sleeps:
    shim = _Sleeps()
    monkeypatch.setattr(cellaflow.context, "time", shim)
    return shim


def _refused_then_notify(
    mock_stub: MagicMock, ttl_ms: Optional[int], unanswered: int
) -> Tuple[Callable[[], str], List[float]]:
    """A workflow whose `charge` is refused by its own engine call, with its
    release unanswered `unanswered` times in a row, then answered; `notify`
    follows, and how long it waited is recorded."""
    _release_answers(
        mock_stub,
        "charge",
        [EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")] * unanswered,
    )
    waited: List[float] = []

    def charge_body() -> str:
        raise refusal()

    charge_body.__name__ = "charge"
    charge = step(lease_ttl_ms=ttl_ms)(charge_body) if ttl_ms else step(charge_body)

    @step
    def notify() -> str:
        return "notified"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            return charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            started = time.monotonic()
            done: str = notify()
            waited.append(time.monotonic() - started)
            return done

    return checkout, waited


def test_an_owed_release_is_sent_again_on_every_pass_of_at_most_a_second(
    mock_stub: MagicMock, sleeps: _Sleeps
) -> None:
    """Unanswered on the step's release and on the next call's first two
    passes, then answered: four releases, each pass at most a second."""
    checkout, waited = _refused_then_notify(mock_stub, None, unanswered=3)
    assert checkout() == "notified"
    assert [r.fencing_token for r in _releases_for(mock_stub, "charge")] == [456] * 4
    assert sleeps.slept and max(sleeps.slept) <= 1.0
    assert 1.9 <= waited[0] < 3.0
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


@pytest.mark.parametrize("ttl_ms", [300_000, MAX_LEASE_TTL_MS])
def test_an_answer_mid_wait_ends_it_long_before_the_lease_would_lapse(
    mock_stub: MagicMock, sleeps: _Sleeps, ttl_ms: int
) -> None:
    """At a five-minute TTL the lease would lapse after five minutes; at
    `MAX_LEASE_TTL_MS` a sleep that long cannot even be asked for (it raised
    OverflowError). The release is answered on the second pass, a second in."""
    checkout, waited = _refused_then_notify(mock_stub, ttl_ms, unanswered=2)
    assert checkout() == "notified"
    assert 0.9 <= waited[0] < 2.0
    assert max(sleeps.slept) <= 1.0
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


@pytest.mark.asyncio
async def test_an_async_call_owing_a_release_at_the_longest_ttl_does_not_wait_forever(
    mock_stub: MagicMock,
) -> None:
    _release_answers(
        mock_stub,
        "charge",
        [EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")] * 2,
    )

    @step(lease_ttl_ms=MAX_LEASE_TTL_MS)  # type: ignore[untyped-decorator]
    async def charge() -> str:
        raise refusal()

    @step
    async def notify() -> str:
        return "notified"

    waited: List[float] = []

    @workflow(version="1.0.0")
    async def checkout() -> str:
        try:
            return await charge()  # type: ignore[no-any-return]
        except AllowanceExceeded:
            started = time.monotonic()
            done: str = await notify()
            waited.append(time.monotonic() - started)
            return done

    assert await asyncio.wait_for(checkout(), 10.0) == "notified"
    # A pass of about a second between the unanswered release and the one
    # answered: waited, not sent again at once.
    assert 0.9 <= waited[0] < 3.0
    assert len(_releases_for(mock_stub, "charge")) == 3
    assert _commits(mock_stub) == [(1, "notify", SUCCESS)]


def test_a_lease_whose_token_is_unknown_is_waited_out_in_passes(
    sleeps: _Sleeps,
) -> None:
    client = MagicMock()
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert ctx.begin_call() == 1
    unknown = {"agent_id": "a", "key": "k", "token": 0, "lapses_in_s": 1.5}
    ctx.give_back_refused_position(1, unknown)
    ctx.end_call()
    assert ctx.begin_call() == 1
    # A full pass, then one cut to what was left of the lease's 1.5 s.
    assert len(sleeps.slept) == 2
    assert sleeps.slept[0] == pytest.approx(1.0, abs=0.01)
    assert 0.4 < sleeps.slept[1] < 0.55
    client.release_lease.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_commit_that_may_have_landed_owes_its_unanswered_release(
    mock_stub: MagicMock, asynchronous: bool
) -> None:
    """The first commit gets no answer (and did not land), the resend is
    refused for the allowance, and the release gets no answer (and did not
    happen). Against an engine that orders strictly, the next step must send
    the release first, then commit at 1, and the one after at 2."""
    name = "charge" if asynchronous else "charge_sync"
    engine = StrictEngine(mock_stub, name)
    engine.commit_failures += [
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"),
        over_allowance(),
    ]
    engine.release_failures.append(
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
    )

    @step(lease_ttl_ms=5000)  # type: ignore[untyped-decorator]
    async def charge() -> str:
        return "charged"

    @step
    async def notify() -> str:
        return "notified"

    @step
    async def audit() -> str:
        return "audited"

    @workflow(version="1.0.0")
    async def checkout_async() -> str:
        try:
            await charge()
        except AllowanceExceeded:
            pass
        await notify()
        return await audit()  # type: ignore[no-any-return]

    @step(lease_ttl_ms=5000)  # type: ignore[untyped-decorator]
    def charge_sync() -> str:
        return "charged"

    @step
    def notify_sync() -> str:
        return "notified"

    @step
    def audit_sync() -> str:
        return "audited"

    @workflow(version="1.0.0")
    def checkout() -> str:
        try:
            charge_sync()
        except AllowanceExceeded:
            pass
        notify_sync()
        return audit_sync()  # type: ignore[no-any-return]

    assert await _run(checkout_async if asynchronous else checkout) == "audited"
    assert engine.committed == 2
    suffix = "" if asynchronous else "_sync"
    assert [c[:2] for c in _commits(mock_stub)][-2:] == [
        (1, "notify" + suffix),
        (2, "audit" + suffix),
    ]


@pytest.mark.asyncio
async def test_an_async_claim_adopts_the_engines_position_before_an_owed_release() -> (
    None
):
    """As a sync claim does: the position the engine reported moves the counter
    first, so a release owed below it is not sent, and stays owed."""
    client = MagicMock()
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert await ctx.begin_call_async() == 1
    owed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 60.0}
    ctx.give_back_refused_position(1, owed)
    ctx.end_call()
    ctx.record_engine_sequence(5)
    assert await ctx.begin_call_async() == 6
    client.release_lease.assert_not_called()
    assert list(ctx._owed) == [1]


class _SlowReleases:
    """A client whose releases block until `gate` is set, then refuse for the
    allowance while `refuse` is set."""

    def __init__(self) -> None:
        self.releases = 0
        self.refuse = True
        self.gate = threading.Event()

    def release_lease(self, **kwargs: Any) -> Any:
        self.releases += 1
        assert self.gate.wait(5.0)
        if self.refuse:
            raise refusal()
        return idempotency_pb2.ReleaseLeaseResponse(released=True)


async def _owing_one(client: Any) -> WorkflowContext:
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert await ctx.begin_call_async() == 1
    owed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 60.0}
    ctx.give_back_refused_position(1, owed)
    ctx.end_call()
    return ctx


@pytest.mark.asyncio
async def test_several_async_calls_share_one_owed_release_and_claim_in_order() -> None:
    client = _SlowReleases()
    client.refuse = False
    ctx = await _owing_one(client)
    tasks = [asyncio.ensure_future(ctx.begin_call_async()) for _ in range(3)]
    await _until(lambda: client.releases == 1)
    await asyncio.sleep(0.05)
    client.gate.set()
    assert await asyncio.gather(*tasks) == [1, 2, 3]
    assert client.releases == 1
    assert ctx._owed == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", ["one", "all"])
async def test_a_cancelled_waiter_on_a_refused_owed_release_logs_nothing(
    cancelled: str,
) -> None:
    """Three calls wait on one owed release, which the engine refuses for the
    allowance; one of them (or all) is cancelled meanwhile. The others fail
    with the refusal, the release stays owed, and nothing is reported to the
    loop: `asyncio.shield` reported such a refusal as an error from Python
    3.13, and a refusal nobody waited for as never retrieved."""
    errors: List[Any] = []
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda _, context: errors.append(context))
    try:
        client = _SlowReleases()
        ctx = await _owing_one(client)
        tasks = [asyncio.ensure_future(ctx.begin_call_async()) for _ in range(3)]
        await _until(lambda: client.releases == 1)
        for task in tasks if cancelled == "all" else tasks[1:2]:
            task.cancel()
        client.gate.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0.05)
        gc.collect()
        await asyncio.sleep(0.05)
        expected = (
            ["CancelledError"] * 3
            if cancelled == "all"
            else ["AllowanceExceeded", "CancelledError", "AllowanceExceeded"]
        )
        assert [type(r).__name__ for r in results] == expected
        assert client.releases == 1
        assert (ctx.sequence, ctx._in_flight, list(ctx._owed)) == (0, 0, [1])
        assert errors == []
        # Nor will it be when the settling is collected, which the cancelled
        # calls' frames put off: its refusal has been read, so asyncio does
        # not report it as never retrieved.
        settling = ctx._owing
        assert settling is not None and settling.done()
        assert not settling._log_traceback
        # The allowance is back: the next call releases the lease, then
        # claims the position.
        client.refuse = False
        assert await ctx.begin_call_async() == 1
        assert client.releases == 2
        # The failed settling is let go now, replaced by the one that went
        # through: a refusal nobody was left to see is not reported then either.
        gc.collect()
        await asyncio.sleep(0.05)
        assert errors == []
    finally:
        loop.set_exception_handler(None)


@pytest.mark.asyncio
async def test_adopting_the_engines_position_keeps_a_release_owed_below_it(
    mock_stub: MagicMock,
) -> None:
    """A (1) is refused while B (2) runs, and its release gets no answer, so 1
    is vacated with A's release owed. B is answered from the cache, and the
    counter adopts the engine's position, 0. C then claims 1: A's release is
    sent first, so the engine does not refuse C's lease there as divergent, and
    D lands in order after it."""
    engine = StrictEngine(mock_stub, "a_step")
    engine.release_failures.append(
        EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable")
    )
    strict_check = engine.check
    b_goes = threading.Event()

    def check(req: Any, **kwargs: Any) -> Any:
        if ":b_step:" in req.idempotency_key:
            assert b_goes.wait(5.0)
            return idempotency_pb2.CheckCacheResponse(
                status=idempotency_pb2.CACHE_STATUS_HIT,
                cached_result=common_pb2.StepResult(
                    output_payload=serialize({"result": "from the cache"})
                ),
                current_sequence=engine.committed,
            )
        return strict_check(req, **kwargs)

    mock_stub.CheckIdempotencyCache.side_effect = check
    a_goes = asyncio.Event()
    entered: List[str] = []

    @step
    async def a_step() -> str:
        entered.append("a")
        await a_goes.wait()
        raise refusal()

    @step
    async def b_step() -> str:
        return "b"  # pragma: no cover

    @step
    async def c_step() -> str:
        return "c"

    @step
    async def d_step() -> str:
        return "d"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-adopt",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        a = asyncio.ensure_future(a_step())
        await _until(lambda: entered == ["a"])
        b = asyncio.ensure_future(b_step())
        await _until(lambda: ctx.sequence == 2)
        a_goes.set()
        with pytest.raises(AllowanceExceeded):
            await a
        assert ctx._vacated == {1} and list(ctx._owed) == [1]
        b_goes.set()
        assert await b == "from the cache"
        assert await c_step() == "c"
        assert await d_step() == "d"
    assert engine.committed == 2
    assert [c[:2] for c in _commits(mock_stub)] == [(1, "c_step"), (2, "d_step")]


@pytest.mark.asyncio
async def test_a_call_arriving_mid_release_does_not_block_the_loop() -> None:
    """The owed release is sent on a worker thread, which holds the session's
    lock until the engine answers (0.4 s here, the first time without an
    answer). A second call arriving meanwhile waits on that settling instead of
    taking the lock on the event loop, so other tasks go on running."""

    class SlowReleases:
        def __init__(self) -> None:
            self.releases = 0

        def release_lease(self, **kwargs: Any) -> Any:
            self.releases += 1
            time.sleep(0.4)
            if self.releases == 1:
                raise EngineError(grpc.StatusCode.DEADLINE_EXCEEDED, "deadline")
            return idempotency_pb2.ReleaseLeaseResponse(released=True)

    client = SlowReleases()
    ctx = await _owing_one(client)
    worst = [0.0]

    async def ticker() -> None:
        while True:
            started = time.monotonic()
            await asyncio.sleep(0.01)
            worst[0] = max(worst[0], time.monotonic() - started - 0.01)

    tick = asyncio.ensure_future(ticker())
    first = asyncio.ensure_future(ctx.begin_call_async())
    await _until(lambda: client.releases == 1)
    second = asyncio.ensure_future(ctx.begin_call_async())
    assert list(await asyncio.gather(first, second)) == [1, 2]
    tick.cancel()
    assert client.releases == 2
    assert worst[0] < 0.2


def test_an_unanswered_owed_release_is_polled_up_to_the_lapse_and_no_further(
    sleeps: _Sleeps,
) -> None:
    """The release goes unanswered every time. The call polls a full second,
    then only what is left of the lease's 1.5 s, and each release is given no
    longer than the lease has left to live."""
    client = MagicMock()
    client.release_lease.side_effect = EngineError(
        grpc.StatusCode.UNAVAILABLE, "unavailable"
    )
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert ctx.begin_call() == 1
    owed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 1.5}
    ctx.give_back_refused_position(1, owed)
    ctx.end_call()
    assert ctx.begin_call() == 1
    assert len(sleeps.slept) == 2
    assert sleeps.slept[0] == pytest.approx(1.0, abs=0.01)
    assert 0.4 < sleeps.slept[1] < 0.55
    timeouts = [c.kwargs["timeout"] for c in client.release_lease.call_args_list]
    assert len(timeouts) == 2
    assert 1.4 < timeouts[0] <= 1.5 and 0.4 < timeouts[1] < 0.55


def test_a_lease_that_lapses_while_its_release_is_sent_is_dropped_at_once(
    sleeps: _Sleeps,
) -> None:
    """The release takes longer than the lease has left, and gets no answer:
    the lease has lapsed by then, so it is dropped and the call claims at once,
    without a wait counted from before the release."""

    class Slow:
        def __init__(self) -> None:
            self.releases = 0

        def release_lease(self, **kwargs: Any) -> Any:
            self.releases += 1
            time.sleep(0.3)
            raise EngineError(grpc.StatusCode.DEADLINE_EXCEEDED, "deadline")

    client: Any = Slow()
    ctx = WorkflowContext(
        client=client, session_id="s", workflow_version="1", history_known=True
    )
    assert ctx.begin_call() == 1
    owed = {"agent_id": "a", "key": "k", "token": 7, "lapses_in_s": 0.2}
    ctx.give_back_refused_position(1, owed)
    ctx.end_call()
    assert ctx.begin_call() == 1
    assert client.releases == 1
    assert sleeps.slept == []
    assert ctx._owed == {}


class TwoOwedEngine(StrictEngine):
    """`StrictEngine` with releases of several steps failing, each with its
    own errors in turn."""

    def __init__(
        self, mock_stub: MagicMock, failures: Dict[str, List[BaseException]]
    ) -> None:
        super().__init__(mock_stub, "no-such-step")
        self.failures = failures

    def release(self, req: Any, **kwargs: Any) -> Any:
        for name, errors in self.failures.items():
            if f":{name}:" in req.idempotency_key and errors:
                raise errors.pop(0)
        return super().release(req, **kwargs)


def _two_owed_failures() -> Dict[str, List[BaseException]]:
    """Each refused step's release goes unanswered, and so does the first
    pass of the call that owes it, so that call waits before sending it again."""
    return {
        name: [
            EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"),
            EngineError(grpc.StatusCode.UNAVAILABLE, "unavailable"),
        ]
        for name in ("first", "second")
    }


@pytest.mark.asyncio
async def test_two_owed_positions_claimed_together_async(mock_stub: MagicMock) -> None:
    """`first` (1) and `second` (2) are both refused, their releases unanswered,
    so releases are owed at 1 and at 2. `third` and `fourth` are then called
    together: each settles the release owed at the position it is about to
    claim, so `third` commits at 1 and `fourth` at 2, neither refused as
    divergent."""
    engine = TwoOwedEngine(mock_stub, _two_owed_failures())
    first_goes, second_goes = asyncio.Event(), asyncio.Event()
    entered: List[str] = []

    @step
    async def first() -> str:
        entered.append("first")
        await first_goes.wait()
        raise refusal()

    @step
    async def second() -> str:
        entered.append("second")
        await second_goes.wait()
        raise refusal()

    @step
    async def third() -> str:
        return "third"

    @step
    async def fourth() -> str:
        return "fourth"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-two-owed",
        workflow_version="1.0.0",
        history_known=True,
    )
    with ctx.bind():
        one = asyncio.ensure_future(first())
        await _until(lambda: entered == ["first"])
        two = asyncio.ensure_future(second())
        await _until(lambda: entered == ["first", "second"])
        first_goes.set()
        with pytest.raises(AllowanceExceeded):
            await one
        second_goes.set()
        with pytest.raises(AllowanceExceeded):
            await two
        assert ctx.sequence == 0 and sorted(ctx._owed) == [1, 2]
        assert list(await asyncio.gather(third(), fourth())) == ["third", "fourth"]
    assert [c[:2] for c in _commits(mock_stub)] == [(1, "third"), (2, "fourth")]
    assert engine.committed == 2


def test_two_owed_positions_claimed_together_sync(mock_stub: MagicMock) -> None:
    """As the async test, with the calls on threads: whichever claims first
    takes 1, after the release owed there, and the other 2."""
    engine = TwoOwedEngine(mock_stub, _two_owed_failures())
    goes = {"first": threading.Event(), "second": threading.Event()}
    entered: List[str] = []
    outcomes: Dict[str, Any] = {}

    def refused_body(name: str) -> Callable[[], str]:
        def body() -> str:
            entered.append(name)
            assert goes[name].wait(5.0)
            raise refusal()

        body.__name__ = name
        return body

    first, second = step(refused_body("first")), step(refused_body("second"))

    @step
    def third() -> str:
        return "third"

    @step
    def fourth() -> str:
        return "fourth"

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="s-two-owed-sync",
        workflow_version="1.0.0",
        history_known=True,
    )

    def run(name: str, call: Callable[[], str]) -> threading.Thread:
        def target() -> None:
            with ctx.bind():
                try:
                    outcomes[name] = call()
                except BaseException as e:
                    outcomes[name] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        return thread

    one = run("first", first)
    _wait_for(lambda: entered == ["first"])
    two = run("second", second)
    _wait_for(lambda: entered == ["first", "second"])
    goes["first"].set()
    one.join(5.0)
    goes["second"].set()
    two.join(5.0)
    assert isinstance(outcomes["first"], AllowanceExceeded)
    assert isinstance(outcomes["second"], AllowanceExceeded)
    assert ctx.sequence == 0 and sorted(ctx._owed) == [1, 2]
    threads = [run("third", third), run("fourth", fourth)]
    for thread in threads:
        thread.join(10.0)
    assert (outcomes["third"], outcomes["fourth"]) == ("third", "fourth")
    committed = sorted(c[:2] for c in _commits(mock_stub))
    assert [c[0] for c in committed] == [1, 2]
    assert {c[1] for c in committed} == {"third", "fourth"}
    assert engine.committed == 2
