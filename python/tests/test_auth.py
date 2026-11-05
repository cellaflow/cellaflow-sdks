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


def test_the_engines_own_variable_is_the_fallback(monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_AUTH_TOKEN", "from-engine-var")
    assert resolve_api_key() == "from-engine-var"
    monkeypatch.setenv("CELLAFLOW_API_KEY", "from-sdk-var")
    assert resolve_api_key() == "from-sdk-var"
    assert resolve_api_key("passed") == "passed"
    # An empty argument means no key, even with the variables set.
    assert resolve_api_key("") is None


def test_a_key_that_cannot_be_a_header_is_refused_without_showing_it() -> None:
    with pytest.raises(ValueError) as refused:
        CellaflowClient(api_key="secret-value\n")
    assert "secret-value" not in str(refused.value)


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
