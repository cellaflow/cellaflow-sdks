"""Sending an API key: what reaches the engine, where it comes from, and where
it must never appear."""

import logging
from concurrent import futures
from typing import Any, Iterator, List, Tuple

import grpc
import pytest

from cellaflow import CellaflowClient
from cellaflow import client as client_module
from cellaflow.client import resolve_api_key
from cellaflow.v1 import service_pb2

KEY = "cf_test_" + "k" * 43


class Recorder:
    """An engine stand-in that records the metadata of every call, and refuses
    calls without the expected key the way the engine does."""

    def __init__(self, expected: str = KEY) -> None:
        self.metadata: List[List[Tuple[str, str]]] = []
        self.expected = expected

    def start_session(self, request: Any, context: Any) -> Any:
        received = list(context.invocation_metadata())
        self.metadata.append(received)
        if ("authorization", f"Bearer {self.expected}") not in received:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid or missing API key / authorization token",
            )
        return service_pb2.StartSessionResponse(
            session_id=request.session_id or "s", version=request.version
        )


@pytest.fixture
def engine() -> Iterator[Tuple[str, Recorder]]:
    recorder = Recorder()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    handler = grpc.method_handlers_generic_handler(
        "cellaflow.v1.WorkflowEngineService",
        {
            "StartSession": grpc.unary_unary_rpc_method_handler(
                recorder.start_session,
                request_deserializer=service_pb2.StartSessionRequest.FromString,
                response_serializer=service_pb2.StartSessionResponse.SerializeToString,
            )
        },
    )
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("localhost:0")
    server.start()
    try:
        yield f"localhost:{port}", recorder
    finally:
        server.stop(None)


@pytest.fixture(autouse=True)
def no_key_in_environment(monkeypatch: Any) -> None:
    for name in client_module.API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_the_key_is_sent_as_a_bearer_token_on_every_call(
    engine: Tuple[str, Recorder],
) -> None:
    target, recorder = engine
    client = CellaflowClient(target=target, api_key=KEY)
    client.start_session("wf", "1.0.0", session_id="a")
    client.start_session("wf", "1.0.0", session_id="b")
    assert len(recorder.metadata) == 2
    for received in recorder.metadata:
        assert ("authorization", f"Bearer {KEY}") in received


def test_without_a_key_nothing_is_sent_and_a_secured_engine_refuses(
    engine: Tuple[str, Recorder],
) -> None:
    target, recorder = engine
    with pytest.raises(grpc.RpcError) as refused:
        CellaflowClient(target=target).start_session("wf", "1.0.0")
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert not any(name == "authorization" for name, _ in recorder.metadata[0])


def test_the_key_is_read_from_the_environment(
    engine: Tuple[str, Recorder], monkeypatch: Any
) -> None:
    """So existing code works against a secured engine without changes."""
    target, recorder = engine
    monkeypatch.setenv("CELLAFLOW_API_KEY", KEY)
    CellaflowClient(target=target).start_session("wf", "1.0.0")
    assert ("authorization", f"Bearer {KEY}") in recorder.metadata[0]


def test_the_variables_are_read_in_the_engines_order(monkeypatch: Any) -> None:
    """So with both set, the SDK sends the token the engine expects."""
    monkeypatch.setenv("CELLAFLOW_API_KEY", "from-sdk-var")
    assert resolve_api_key() == "from-sdk-var"
    monkeypatch.setenv("CELLAFLOW_AUTH_TOKEN", "from-engine-var")
    assert resolve_api_key() == "from-engine-var"
    assert resolve_api_key("passed") == "passed"
    # An empty argument means no key, even with the variables set.
    assert resolve_api_key("") is None


def test_a_blank_variable_means_no_key_and_is_not_skipped_as_the_engine_does(
    monkeypatch: Any,
) -> None:
    """The engine stops at the first variable that is set, even blank, and then
    has no token. Reading on to the next would send a key it does not have."""
    monkeypatch.setenv("CELLAFLOW_AUTH_TOKEN", "  ")
    monkeypatch.setenv("CELLAFLOW_API_KEY", "the-key")
    assert resolve_api_key() is None
    assert resolve_api_key("   ") is None


def test_generic_variables_are_never_read(monkeypatch: Any) -> None:
    """A variable named API_KEY often holds another service's secret, which
    would then be sent to the engine."""
    monkeypatch.setenv("API_KEY", "someone-elses-secret")
    monkeypatch.setenv("AUTH_TOKEN", "someone-elses-secret")
    assert resolve_api_key() is None


@pytest.mark.parametrize(
    "bad", ["MARKER\n", "MAR\tKER", "MAR\x7fKER", "MARKER\x00", "MARKÉR"]
)
def test_a_key_that_cannot_be_a_header_is_refused_without_showing_it(
    bad: str, monkeypatch: Any
) -> None:
    """Including a tab, DEL or NUL, which gRPC would otherwise refuse on every
    call with an unhelpful "Invalid metadata"."""
    opened: List[str] = []
    monkeypatch.setattr(grpc, "insecure_channel", lambda target: opened.append(target))
    with pytest.raises(ValueError) as refused:
        CellaflowClient(api_key=bad)
    assert "MAR" not in str(refused.value)
    assert "the api_key argument" in str(refused.value)
    assert opened == []  # refused before a channel was opened


def test_a_refused_key_names_the_variable_it_came_from(monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_API_KEY", "from-a-secret-file\n")
    with pytest.raises(ValueError, match="from CELLAFLOW_API_KEY"):
        CellaflowClient()


def test_every_kind_of_call_gets_the_key() -> None:
    """Streaming calls too, so one added later cannot go out without it."""
    from cellaflow.client import _BearerKey

    interceptor = _BearerKey(KEY)
    for kind in (
        grpc.UnaryUnaryClientInterceptor,
        grpc.UnaryStreamClientInterceptor,
        grpc.StreamUnaryClientInterceptor,
        grpc.StreamStreamClientInterceptor,
    ):
        assert isinstance(interceptor, kind)


def test_the_key_never_appears_in_repr_logs_or_a_refusal(
    engine: Tuple[str, Recorder], caplog: Any
) -> None:
    target, recorder = engine
    recorder.expected = "the-right-key"
    with caplog.at_level(logging.DEBUG):
        client = CellaflowClient(target="engine.example.com:50051", api_key=KEY)
        local = CellaflowClient(target=target, api_key=KEY)
        with pytest.raises(grpc.RpcError) as refused:
            local.start_session("wf", "1.0.0")
    for text in (
        repr(client),
        repr(vars(client)),
        repr(vars(local)),
        caplog.text,
        str(refused.value),
        repr(refused.value),
    ):
        assert KEY not in text


def test_sending_a_key_without_tls_to_another_host_warns_once(caplog: Any) -> None:
    client_module._warned_plaintext.clear()
    with caplog.at_level(logging.WARNING, logger="cellaflow.client"):
        CellaflowClient(target="engine.example.com:50051", api_key=KEY)
        CellaflowClient(target="engine.example.com:50051", api_key=KEY)
        CellaflowClient(target="localhost:50051", api_key=KEY)
        CellaflowClient(target="engine.example.com:50051", secure=True, api_key=KEY)
    assert caplog.text.count("without TLS") == 1
    assert "engine.example.com:50051" in caplog.text


@pytest.mark.parametrize(
    "target",
    [
        "localhost:50051",
        "127.0.0.1:50051",
        "[::1]:50051",
        "::1",
        "unix:/tmp/cellaflow.sock",
        "unix:///tmp/cellaflow.sock",
        "dns:///localhost:50051",
        "ipv4:127.0.0.1:50051",
        "ipv6:[::1]:50051",
        "ipv4:127.0.0.1:50051,127.0.0.2:50051",
        "LOCALHOST:50051",
        "localhost.:50051",
        "127.0.0.2:50051",
    ],
)
def test_a_local_target_in_any_form_gets_no_warning(target: str, caplog: Any) -> None:
    client_module._warned_plaintext.clear()
    with caplog.at_level(logging.WARNING, logger="cellaflow.client"):
        CellaflowClient(target=target, api_key=KEY)
    assert "without TLS" not in caplog.text


@pytest.mark.parametrize(
    "target",
    [
        "ipv4:127.0.0.1:50051,10.0.0.5:50051",
        "ipv6:[::1]:50051,[2001:db8::1]:50051",
        "dns:///engine.example.com:50051",
    ],
)
def test_a_target_that_can_reach_another_host_is_warned_about(
    target: str, caplog: Any
) -> None:
    """gRPC may send calls to any address in a list, so one remote address is
    enough for the key to cross the network."""
    client_module._warned_plaintext.clear()
    with caplog.at_level(logging.WARNING, logger="cellaflow.client"):
        CellaflowClient(target=target, api_key=KEY)
    assert "without TLS" in caplog.text


def test_the_key_helpers_are_exported_from_the_package() -> None:
    import cellaflow

    assert cellaflow.resolve_api_key is resolve_api_key
    assert cellaflow.API_KEY_ENV_VARS == ("CELLAFLOW_AUTH_TOKEN", "CELLAFLOW_API_KEY")
