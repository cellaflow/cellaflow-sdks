"""Sending an API key: what reaches the engine, where it comes from, and where
it must never appear."""

import asyncio
import logging
import socket
import threading
from concurrent import futures
from typing import Any, Iterator, List, Optional, Tuple

import grpc
import pytest

from cellaflow import CellaflowClient
from cellaflow import client as client_module
from cellaflow import decorators, durable, execution, langgraph
from cellaflow.client import resolve_api_key
from cellaflow.durable import durable_tools
from cellaflow.execution import async_execution_lease, execution_lease, task_lease
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


def _serve(address: str, recorder: Recorder) -> Tuple[Any, int]:
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
    port = server.add_insecure_port(address)
    server.start()
    return server, port


@pytest.fixture
def engine() -> Iterator[Tuple[str, Recorder]]:
    recorder = Recorder()
    server, port = _serve("localhost:0", recorder)
    try:
        yield f"localhost:{port}", recorder
    finally:
        server.stop(None)


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


def test_only_cellaflow_api_key_is_read(monkeypatch: Any) -> None:
    """The engine's variables configure the token it checks against, not a
    caller's key, and one named API_KEY often holds another service's secret,
    which would then be sent to the engine."""
    for name in ("CELLAFLOW_AUTH_TOKEN", "AUTH_TOKEN", "API_KEY"):
        monkeypatch.setenv(name, "not-this-one")
    assert resolve_api_key() is None
    monkeypatch.setenv("CELLAFLOW_API_KEY", "the-key")
    assert resolve_api_key() == "the-key"
    assert resolve_api_key("passed") == "passed"


def test_a_blank_key_means_none(monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_API_KEY", "  ")
    assert resolve_api_key() is None
    monkeypatch.setenv("CELLAFLOW_API_KEY", "the-key")
    # An empty argument means no key, even with the variable set.
    assert resolve_api_key("") is None
    assert resolve_api_key("   ") is None


@pytest.mark.parametrize(
    "bad", ["MARKER\n", "MAR\tKER", "MAR\x7fKER", "MARKER\x00", "MARKÉR"]
)
def test_a_key_that_cannot_be_a_header_is_refused_without_showing_it(
    bad: str, monkeypatch: Any
) -> None:
    """Including a tab, DEL or NUL, which gRPC would otherwise refuse on every
    call with an unhelpful "Invalid metadata"."""
    opened: List[str] = []
    # Both kinds: a key to this machine goes on a secure_channel with local
    # credentials, and anything else on an insecure one.
    monkeypatch.setattr(grpc, "insecure_channel", lambda target: opened.append(target))
    monkeypatch.setattr(
        grpc, "secure_channel", lambda target, *a, **k: opened.append(target)
    )
    with pytest.raises(ValueError) as refused:
        CellaflowClient(api_key=bad)
    assert "MAR" not in str(refused.value)
    assert "the api_key argument" in str(refused.value)
    assert opened == []  # refused before a channel was opened


def test_a_refused_key_names_the_variable_it_came_from(monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_API_KEY", "from-a-secret-file\n")
    with pytest.raises(ValueError, match="from CELLAFLOW_API_KEY"):
        CellaflowClient()


def test_a_decorator_refuses_a_bad_key_when_it_is_applied() -> None:
    """Not on the first call, after whatever ran before it."""
    with pytest.raises(ValueError, match="the api_key argument"):
        decorators.workflow(api_key="MARKER\n")
    with pytest.raises(ValueError, match="the api_key argument"):
        task_lease("task:1", worker_id="w", api_key="MARKER\n")
    # Whether a key may go without TLS depends on the environment when the
    # client is built (an opt-in set by load_dotenv after import, say), so
    # that is not decided at import.
    remote = "engine.example.com:50051"
    decorators.workflow(target=remote, api_key=KEY)
    task_lease("task:1", worker_id="w", target=remote, api_key=KEY)
    # A blank key means none, so there is nothing to refuse.
    decorators.workflow(api_key="")
    task_lease("task:1", worker_id="w", target=remote, api_key="  ")


class _Built(Exception):
    """Raised by the stand-in client, once it has seen the key."""


@pytest.fixture
def keys_received(monkeypatch: Any) -> List[Any]:
    """Replaces the client in every module that builds one, recording the
    `api_key` each is given."""
    keys: List[Any] = []

    def build(*args: Any, **kwargs: Any) -> None:
        keys.append(kwargs.get("api_key"))
        raise _Built

    for module in (decorators, durable, execution, langgraph):
        monkeypatch.setattr(module, "CellaflowClient", build)
    return keys


def test_every_entry_point_passes_the_key_to_its_client(
    keys_received: List[Any],
) -> None:
    @decorators.workflow(api_key=KEY)
    async def a_workflow() -> None:
        pass

    @task_lease("task:1", worker_id="w", api_key=KEY)
    def a_task() -> None:
        pass

    @task_lease("task:1", worker_id="w", api_key=KEY)
    async def an_async_task() -> None:
        pass

    async def in_an_async_lease() -> None:
        async with async_execution_lease("task:1", worker_id="w", api_key=KEY):
            pass

    with pytest.raises(_Built):
        asyncio.run(a_workflow())
    with pytest.raises(_Built):
        a_task()
    with pytest.raises(_Built):
        asyncio.run(an_async_task())
    with pytest.raises(_Built):
        asyncio.run(in_an_async_lease())
    with pytest.raises(_Built):
        with execution_lease("task:1", worker_id="w", api_key=KEY):
            pass
    with pytest.raises(_Built):
        with durable_tools("thread-1", api_key=KEY):
            pass
    if langgraph.HAS_LANGGRAPH:
        with pytest.raises(_Built):
            langgraph.CellaflowSaver(api_key=KEY)
    assert keys_received and all(key == KEY for key in keys_received)


@pytest.mark.skipif(not langgraph.HAS_LANGGRAPH, reason="needs langgraph")
def test_the_saver_refuses_a_key_it_would_ignore() -> None:
    """A client sends its own key, so one passed beside it would never go out."""
    client = CellaflowClient()
    with pytest.raises(ValueError, match="both a client and an api_key"):
        langgraph.CellaflowSaver(client=client, api_key=KEY)
    # A blank one means no key, so nothing would be ignored.
    langgraph.CellaflowSaver(client=client, api_key="")


def test_every_kind_of_call_gets_the_key() -> None:
    """Streaming calls too, so one added later cannot go out without it. Each
    interceptor method is called as grpc calls it, and what it passes on is
    checked; the service has no streaming method to call for real."""
    from cellaflow.client import _BearerKey

    interceptor = _BearerKey(KEY)
    details = client_module._CallDetails(
        "/cellaflow.v1.WorkflowEngineService/StartSession",
        None,
        [("x-trace", "1")],
        None,
        None,
        None,
    )
    for method in (
        interceptor.intercept_unary_unary,
        interceptor.intercept_unary_stream,
        interceptor.intercept_stream_unary,
        interceptor.intercept_stream_stream,
    ):
        passed_on: List[Any] = []
        method(lambda d, r: passed_on.append(d), details, object())
        (sent,) = passed_on
        assert ("authorization", f"Bearer {KEY}") in sent.metadata
        assert ("x-trace", "1") in sent.metadata
        assert KEY not in repr(sent)  # grpc keeps it in a failed call's frame


def test_the_key_replaces_an_authorization_the_caller_set() -> None:
    """The engine reads only the first `authorization`, so one the caller set
    would otherwise win over the key."""
    from cellaflow.client import _BearerKey

    details = client_module._CallDetails(
        "/m",
        None,
        [("Authorization", "Bearer other"), ("x-trace", "1")],
        None,
        None,
        None,
    )
    sent = _BearerKey(KEY)._with_key(details)
    assert [v for k, v in sent.metadata if k.lower() == "authorization"] == [
        f"Bearer {KEY}"
    ]
    assert ("x-trace", "1") in sent.metadata


def test_the_key_never_appears_in_our_reprs_logs_or_refusals(
    engine: Tuple[str, Recorder], caplog: Any
) -> None:
    target, recorder = engine
    recorder.expected = "the-right-key"
    with caplog.at_level(logging.DEBUG):
        client = CellaflowClient(
            target="engine.example.com:50051", api_key=KEY, allow_plaintext_key=True
        )
        local = CellaflowClient(target=target, api_key=KEY)
        with pytest.raises(ValueError) as plaintext:
            CellaflowClient(target="engine.example.com:50051", api_key=KEY)
        with pytest.raises(grpc.RpcError) as refused:
            local.start_session("wf", "1.0.0")
    for text in (
        repr(client),
        repr(local.channel._interceptor),  # the object that holds the key
        caplog.text,
        str(refused.value),
        repr(refused.value),
        str(plaintext.value),
    ):
        assert KEY not in text


def test_a_key_is_not_sent_without_tls_to_another_host_unless_allowed(
    monkeypatch: Any,
) -> None:
    """Forgetting TLS must not be how a key crosses the network, as forgetting
    a token cannot be how the engine serves an open port."""
    # No key, nothing to protect.
    CellaflowClient(target="engine.example.com:50051")
    opened: List[str] = []
    # Both kinds: a key to this machine goes on a secure_channel with local
    # credentials, and anything else on an insecure one.
    monkeypatch.setattr(grpc, "insecure_channel", lambda target: opened.append(target))
    monkeypatch.setattr(
        grpc, "secure_channel", lambda target, *a, **k: opened.append(target)
    )
    with pytest.raises(ValueError, match="CELLAFLOW_ALLOW_PLAINTEXT_KEY") as refused:
        CellaflowClient(target="engine.example.com:50051", api_key=KEY)
    assert "engine.example.com:50051" in str(refused.value)
    assert opened == []  # refused before a channel was opened
    # The key from the environment is held to the same rule.
    monkeypatch.setenv("CELLAFLOW_API_KEY", KEY)
    with pytest.raises(ValueError, match="without TLS"):
        CellaflowClient(target="engine.example.com:50051")
    assert opened == []


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes", " true "])
def test_the_variable_allows_it_the_way_the_engine_reads_its_opt_out(
    value: str, monkeypatch: Any
) -> None:
    monkeypatch.setenv("CELLAFLOW_ALLOW_PLAINTEXT_KEY", value)
    CellaflowClient(target="engine.example.com:50051", api_key=KEY)


@pytest.mark.parametrize("value", ["", "false", "0", "no", "on"])
def test_anything_else_in_the_variable_does_not(value: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_ALLOW_PLAINTEXT_KEY", value)
    with pytest.raises(ValueError, match="without TLS"):
        CellaflowClient(target="engine.example.com:50051", api_key=KEY)


def test_the_argument_decides_over_the_variable(monkeypatch: Any) -> None:
    monkeypatch.setenv("CELLAFLOW_ALLOW_PLAINTEXT_KEY", "true")
    with pytest.raises(ValueError, match="without TLS"):
        CellaflowClient(
            target="engine.example.com:50051", api_key=KEY, allow_plaintext_key=False
        )


def test_an_allowed_plaintext_key_warns_once_per_target(caplog: Any) -> None:
    client_module._warned_plaintext.clear()
    with caplog.at_level(logging.WARNING, logger="cellaflow.client"):
        for _ in range(2):
            CellaflowClient(
                target="engine.example.com:50051",
                api_key=KEY,
                allow_plaintext_key=True,
            )
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
        "[::ffff:127.0.0.1]:50051",
        "ipv6:[::ffff:127.0.0.1]:50051",
        "ipv4:///127.0.0.1:50051",
        "DNS:///localhost:50051",
        "unix-abstract:cellaflow",
    ],
)
def test_a_local_target_in_any_form_is_allowed_without_a_warning(
    target: str, caplog: Any
) -> None:
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
        "[::ffff:10.0.0.5]:50051",
        "[fe80::1%eth0]:50051",
    ],
)
def test_a_target_that_can_reach_another_host_is_refused(target: str) -> None:
    """gRPC may send calls to any address in a list, so one remote address is
    enough for the key to cross the network."""
    with pytest.raises(ValueError, match="without TLS"):
        CellaflowClient(target=target, api_key=KEY)


def test_a_key_to_a_local_target_bypasses_a_proxy(
    engine: Tuple[str, Recorder], monkeypatch: Any
) -> None:
    """gRPC sends even `localhost` through `https_proxy`, which would put the
    key on the network in plaintext."""
    target, recorder = engine
    proxy = socket.socket()
    proxy.bind(("127.0.0.1", 0))
    proxy.listen(1)
    proxy.settimeout(5)
    reached: List[bytes] = []
    finished = threading.Event()

    def accept() -> None:
        try:
            connection, _ = proxy.accept()
            if not finished.is_set():  # not the connection that wakes it
                reached.append(connection.recv(200))
            connection.close()
        except OSError:
            pass

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    monkeypatch.setenv("https_proxy", f"http://127.0.0.1:{proxy.getsockname()[1]}")
    monkeypatch.setenv("grpc_proxy", f"http://127.0.0.1:{proxy.getsockname()[1]}")
    try:
        CellaflowClient(target=target, api_key=KEY).start_session("wf", "1.0.0")
    finally:
        # Closing the socket does not wake accept(); a connection of our own
        # does, so a passing run does not wait out the timeout.
        finished.set()
        socket.create_connection(proxy.getsockname(), timeout=1).close()
        thread.join(timeout=5)
        proxy.close()
    assert ("authorization", f"Bearer {KEY}") in recorder.metadata[0]
    assert reached == []


def _lan_address() -> Optional[str]:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("10.255.255.255", 1))
            address = str(probe.getsockname()[0])
    except OSError:
        return None
    return None if address.startswith("127.") else address


def test_a_target_that_only_looks_local_fails_to_connect_and_sends_nothing(
    monkeypatch: Any,
) -> None:
    """The name is only a first answer; gRPC checks the real peer. A target
    judged local that is not (a resolver answering for `localhost` with another
    host, say) must not get the key."""
    lan = _lan_address()
    if lan is None:
        pytest.skip("needs a non-loopback address on this machine")
    recorder = Recorder()
    server, port = _serve("0.0.0.0:0", recorder)
    try:
        monkeypatch.setattr(client_module, "_is_local", lambda target: True)
        client = CellaflowClient(target=f"{lan}:{port}", api_key=KEY)
        with pytest.raises(grpc.RpcError) as refused:
            client.stub.StartSession(
                service_pb2.StartSessionRequest(workflow_id="wf", version="1"),
                timeout=5,
            )
    finally:
        server.stop(None)
    assert refused.value.code() == grpc.StatusCode.UNAVAILABLE
    assert recorder.metadata == []


def test_the_refusal_names_the_variable_every_entry_point_reads(
    monkeypatch: Any,
) -> None:
    """Only CellaflowClient takes the argument; the variable works everywhere."""
    with pytest.raises(ValueError) as refused:
        CellaflowClient(target="engine.example.com:50051", api_key=KEY)
    message = str(refused.value)
    assert message.index("CELLAFLOW_ALLOW_PLAINTEXT_KEY=true") < message.index(
        "allow_plaintext_key=True"
    )
    assert "building a CellaflowClient yourself" in message


def test_the_key_helpers_are_exported_from_the_package() -> None:
    import cellaflow

    assert cellaflow.resolve_api_key is resolve_api_key
    assert cellaflow.API_KEY_ENV_VAR == "CELLAFLOW_API_KEY"
    assert cellaflow.ALLOW_PLAINTEXT_KEY_ENV_VAR == "CELLAFLOW_ALLOW_PLAINTEXT_KEY"
