import ipaddress
import logging
import os
import re
import threading
from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple, cast

import grpc

from cellaflow.v1 import service_pb2, service_pb2_grpc
from cellaflow.v1 import common_pb2, idempotency_pb2
from cellaflow.serialization import serialize, deserialize


logger = logging.getLogger(__name__)

#: Where an API key is read from when none is passed, in the engine's own order,
#: so the SDK sends the token the engine expects when both are set. The engine
#: also falls back to the generic `AUTH_TOKEN` and `API_KEY`; the SDK does not,
#: because a variable that generic often holds another service's secret, which
#: would then be sent to the engine.
API_KEY_ENV_VARS = ("CELLAFLOW_AUTH_TOKEN", "CELLAFLOW_API_KEY")

_HEADER_SAFE = re.compile(r"[\x20-\x7e]*")
_warned_plaintext: "set[str]" = set()
_warned_lock = threading.Lock()


def _api_key_and_source(api_key: Optional[str]) -> Tuple[Optional[str], str]:
    """The API key to send and where it came from.

    Exactly the engine's rule: the first of the variables that is set decides,
    even if it is blank, and a blank value means no key. Skipping a blank one
    to read the next would send a key to an engine that, reading the same
    environment, has none.
    """
    if api_key is not None:
        return (api_key if api_key.strip() else None), "the api_key argument"
    for name in API_KEY_ENV_VARS:
        api_key = os.environ.get(name)
        if api_key is not None:
            return (api_key if api_key.strip() else None), name
    return None, ""


def resolve_api_key(api_key: Optional[str] = None) -> Optional[str]:
    """The API key to send: `api_key` if given, else the first of
    `API_KEY_ENV_VARS` that is set. A blank value means none, and a blank
    variable is not skipped for the next, as the engine does not skip it."""
    return _api_key_and_source(api_key)[0]


def _check_api_key(api_key: str, source: str) -> None:
    """Refuses a key gRPC cannot send as a header, naming where it came from
    but never what it is."""
    if api_key != api_key.strip() or not _HEADER_SAFE.fullmatch(api_key):
        raise ValueError(
            f"The Cellaflow API key from {source} cannot be sent in a header: it "
            f"has surrounding whitespace or a character outside printable ASCII "
            f"(a trailing newline from a secret file, say)."
        )


def _host_is_local(host: str) -> bool:
    host = host.lower().rstrip(".")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback  # all of 127.0.0.0/8, ::1
    except ValueError:
        return False


def _host_of(address: str) -> str:
    """The host in `host:port`, `[v6]:port` or a bare IPv6 address."""
    if address.startswith("["):
        end = address.find("]")
        return address[1:end] if end != -1 else address
    if address.count(":") > 1:
        return address
    return address.rsplit(":", 1)[0]


def _is_local(target: str) -> bool:
    """Whether every address `target` can reach is this machine, in any form
    gRPC accepts."""
    if target.startswith(("unix:", "unix-abstract:")):
        return True
    rest = target
    if rest.startswith("dns:"):
        rest = rest[4:]
        if rest.startswith("//"):  # dns://[authority]/host:port
            start = rest.find("/", 2) + 1
            rest = rest[start:] if start else rest[2:]
    elif rest.startswith(("ipv4:", "ipv6:")):
        # A list: gRPC may send calls to any of them.
        return all(_host_is_local(_host_of(a)) for a in rest[5:].split(","))
    return _host_is_local(_host_of(rest))


class _CallDetails(
    namedtuple(
        "_CallDetails",
        (
            "method",
            "timeout",
            "metadata",
            "credentials",
            "wait_for_ready",
            "compression",
        ),
    ),
    grpc.ClientCallDetails,  # type: ignore[misc]
):
    pass


class _BearerKey(
    grpc.UnaryUnaryClientInterceptor,  # type: ignore[misc]
    grpc.UnaryStreamClientInterceptor,  # type: ignore[misc]
    grpc.StreamUnaryClientInterceptor,  # type: ignore[misc]
    grpc.StreamStreamClientInterceptor,  # type: ignore[misc]
):
    """Adds `authorization: Bearer <key>` to every call on the channel, so no
    call site can forget it. Its `repr` never shows the key."""

    def __init__(self, api_key: str) -> None:
        self._header = ("authorization", f"Bearer {api_key}")

    def __repr__(self) -> str:
        return "<API key: not shown>"

    def _with_key(self, details: Any) -> Any:
        metadata = list(details.metadata or []) + [self._header]
        return _CallDetails(
            details.method,
            details.timeout,
            metadata,
            details.credentials,
            getattr(details, "wait_for_ready", None),
            getattr(details, "compression", None),
        )

    def intercept_unary_unary(
        self, continuation: Any, details: Any, request: Any
    ) -> Any:
        return continuation(self._with_key(details), request)

    def intercept_unary_stream(
        self, continuation: Any, details: Any, request: Any
    ) -> Any:
        return continuation(self._with_key(details), request)

    def intercept_stream_unary(
        self, continuation: Any, details: Any, requests: Any
    ) -> Any:
        return continuation(self._with_key(details), requests)

    def intercept_stream_stream(
        self, continuation: Any, details: Any, requests: Any
    ) -> Any:
        return continuation(self._with_key(details), requests)


def _warn_if_sent_in_the_clear(target: str) -> None:
    if _is_local(target):
        return
    with _warned_lock:
        if target in _warned_plaintext:
            return
        _warned_plaintext.add(target)
    logger.warning(
        "Cellaflow: sending an API key to %s without TLS, so anyone on the path "
        "can read it. Pass secure=True to send it encrypted.",
        target,
    )


class CellaflowClient:
    """
    gRPC Client for the Cellaflow Engine.
    Handles communication with the engine and strictly uses
    MessagePack for state payloads.

    `api_key` authenticates every call, sent as `authorization: Bearer <key>`.
    When it is not given, it is read from `CELLAFLOW_AUTH_TOKEN`, then
    `CELLAFLOW_API_KEY`, the engine's own order; with neither set, calls go
    unauthenticated, which an engine started without a token accepts. The key
    never appears in logs, exceptions or `repr`.
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        secure: bool = False,
        api_key: Optional[str] = None,
    ) -> None:
        # Checked before a channel is opened, so a refused key leaves none behind.
        # Held only in variables named `api_key`, which error reporters that
        # record a frame's locals scrub by name.
        api_key, source = _api_key_and_source(api_key)
        if api_key is not None:
            _check_api_key(api_key, source)
            if not secure:
                _warn_if_sent_in_the_clear(target)
        if secure:
            channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        else:
            channel = grpc.insecure_channel(target)
        if api_key is not None:
            channel = grpc.intercept_channel(channel, _BearerKey(api_key))
        self.channel = channel
        self.stub = service_pb2_grpc.WorkflowEngineServiceStub(self.channel)

    def start_session(
        self,
        workflow_id: str,
        version: str,
        session_id: Optional[str] = None,
        coordination_id: Optional[str] = None,
    ) -> service_pb2.StartSessionResponse:
        """Starts or resumes a session.

        `coordination_id` is the session's default coordination domain. The
        engine stores it with the session, and every step that does not name its
        own is recorded under it. Resuming with a different value is refused
        with `FAILED_PRECONDITION`; omitting it on resume is allowed, and the
        stored value comes back on the response.
        """
        req = service_pb2.StartSessionRequest(
            workflow_id=workflow_id,
            version=version,
        )
        if session_id:
            req.session_id = session_id
        if coordination_id:
            req.coordination_id = coordination_id

        return cast(service_pb2.StartSessionResponse, self.stub.StartSession(req))

    def commit_step(
        self,
        session_id: str,
        sequence: int,
        name: str,
        status: "common_pb2.StepStatus.ValueType",
        output_payload: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        idempotency_fencing_token: Optional[int] = None,
        coordination_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> service_pb2.CommitStepResponse:
        """Commits a step.

        `coordination_id` records the step under its own coordination domain.
        When omitted, the engine records it under the session's default.
        `timeout` bounds the RPC in seconds.
        """
        # Strictly serialize dict to MessagePack
        serialized_state = serialize(output_payload)

        step_result = common_pb2.StepResult(
            sequence=sequence,
            name=name,
            status=status,
            output_payload=serialized_state,
        )
        if coordination_id:
            step_result.coordination_id = coordination_id

        req = service_pb2.CommitStepRequest(
            session_id=session_id,
            step_result=step_result,
        )
        if idempotency_key is not None:
            req.idempotency_key = idempotency_key
            if idempotency_fencing_token is None:
                raise ValueError(
                    "idempotency_fencing_token required if idempotency_key is set"
                )
            req.idempotency_fencing_token = idempotency_fencing_token

        return cast(
            service_pb2.CommitStepResponse, self.stub.CommitStep(req, timeout=timeout)
        )

    def get_graph(
        self,
        session_id: str,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Returns a tuple of (list of deserialized step results, next_cursor).
        Each step result is a dictionary representation of the StepResult proto,
        with the output_payload fully deserialized into a Python dictionary.
        `timeout` bounds the RPC in seconds.
        """
        req = service_pb2.GetGraphRequest(session_id=session_id)
        if limit is not None:
            req.limit = limit
        if cursor is not None:
            req.cursor = cursor

        resp: service_pb2.GetGraphResponse = self.stub.GetGraph(req, timeout=timeout)

        results = []
        for step in resp.steps:
            results.append(
                {
                    "sequence": step.sequence,
                    "name": step.name,
                    "status": step.status,
                    "output_payload": deserialize(step.output_payload),
                    "idempotency_key": (
                        step.idempotency_key
                        if step.HasField("idempotency_key")
                        else None
                    ),
                    "coordination_id": (
                        step.coordination_id
                        if step.HasField("coordination_id")
                        else None
                    ),
                }
            )

        next_cursor = resp.next_cursor if resp.next_cursor else None
        return results, next_cursor

    def check_idempotency_cache(
        self,
        agent_id: str,
        idempotency_key: str,
        wait_timeout_ms: Optional[int] = None,
        lease_ttl_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        sequence: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> idempotency_pb2.CheckCacheResponse:
        """Arbitrates the lease for `idempotency_key`.

        Supplying `session_id` also asks the engine for that session's committed
        position, returned as `current_sequence` on every status. The idempotency
        key is opaque to the engine, so the session cannot be inferred from it —
        without this the engine has nothing to answer from.

        Supplying `sequence` — the position this caller intends to write to —
        additionally lets the engine refuse a lease that would authorise a side
        effect at an already-committed position, instead of rejecting the commit
        afterwards once the side effect has happened. Raises `FAILED_PRECONDITION`
        when refused. Both fields are optional on the wire; omitting `sequence`
        keeps the position unguarded.
        """
        req = idempotency_pb2.CheckCacheRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
        )
        if wait_timeout_ms is not None:
            req.wait_timeout_ms = wait_timeout_ms
        if lease_ttl_ms is not None:
            req.lease_ttl_ms = lease_ttl_ms
        if session_id is not None:
            req.session_id = session_id
        if sequence is not None:
            req.sequence = sequence

        return cast(
            idempotency_pb2.CheckCacheResponse,
            self.stub.CheckIdempotencyCache(req, timeout=timeout),
        )

    def renew_lease(
        self,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        extend_ms: int,
        timeout: Optional[float] = None,
    ) -> idempotency_pb2.RenewLeaseResponse:
        """Renews a held lease.

        `timeout` bounds the RPC in seconds. Heartbeat callers must supply one:
        without a deadline a black-holed connection parks the calling thread
        indefinitely, and the shutdown path that joins that thread parks with it.
        """
        req = idempotency_pb2.RenewLeaseRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
            fencing_token=fencing_token,
            extend_ms=extend_ms,
        )

        return cast(
            idempotency_pb2.RenewLeaseResponse,
            self.stub.RenewLease(req, timeout=timeout),
        )

    def release_lease(
        self,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        reason: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> idempotency_pb2.ReleaseLeaseResponse:
        """Releases a held lease. `timeout` bounds the RPC in seconds."""
        req = idempotency_pb2.ReleaseLeaseRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
            fencing_token=fencing_token,
        )
        if reason:
            req.reason = reason

        return cast(
            idempotency_pb2.ReleaseLeaseResponse,
            self.stub.ReleaseLease(req, timeout=timeout),
        )

    def close(self) -> None:
        self.channel.close()
