import functools
import ipaddress
import logging
import os
import threading
from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple, cast

import grpc

from cellaflow.v1 import service_pb2, service_pb2_grpc
from cellaflow.v1 import common_pb2, idempotency_pb2
from cellaflow.serialization import serialize, deserialize


logger = logging.getLogger(__name__)

#: Where an API key is read from when none is passed, and the only variable
#: read: the same one the engine reads its token from. The engine also accepts
#: older names for its token (`CELLAFLOW_AUTH_TOKEN`, and the generic
#: `AUTH_TOKEN` and `API_KEY`); the SDK reads none of them, because a variable
#: as generic as `API_KEY` often holds another service's secret, which would
#: then be sent to the engine.
API_KEY_ENV_VAR = "CELLAFLOW_API_KEY"

#: Set to `true` (or `1`, `yes`) to allow sending a key without TLS to another
#: host, on a network you trust. Without it, that is refused, as the engine
#: refuses to serve without a token unless told to.
ALLOW_PLAINTEXT_KEY_ENV_VAR = "CELLAFLOW_ALLOW_PLAINTEXT_KEY"

_warned_plaintext: "set[str]" = set()
_warned_lock = threading.Lock()


def _api_key_and_source(api_key: Optional[str]) -> Tuple[Optional[str], str]:
    """The API key to send and where it came from: `api_key` if given, else
    `CELLAFLOW_API_KEY`. A blank value means no key."""
    source = "the api_key argument"
    if api_key is None:
        api_key, source = os.environ.get(API_KEY_ENV_VAR), API_KEY_ENV_VAR
    if api_key is None or not api_key.strip():
        return None, source
    return api_key, source


def resolve_api_key(api_key: Optional[str] = None) -> Optional[str]:
    """The API key to send: `api_key` if given, else `CELLAFLOW_API_KEY`. A
    blank value means none."""
    return _api_key_and_source(api_key)[0]


def _check_api_key(api_key: str, source: str) -> None:
    """Refuses a key gRPC cannot send as a header, naming where it came from
    but never what it is."""
    printable_ascii = api_key.isascii() and api_key.isprintable()
    if api_key != api_key.strip() or not printable_ascii:
        raise ValueError(
            f"The Cellaflow API key from {source} cannot be sent in a header: it "
            f"has surrounding whitespace or a character outside printable ASCII "
            f"(a trailing newline from a secret file, say)."
        )


def _checked_api_key(api_key: Optional[str]) -> Optional[str]:
    """The key to send, refusing one gRPC cannot send: the one place both rules
    are applied, for clients and decorators alike."""
    api_key, source = _api_key_and_source(api_key)
    if api_key is not None:
        _check_api_key(api_key, source)
    return api_key


def _check_api_key_argument(api_key: Optional[str]) -> None:
    """Refuses an `api_key` argument gRPC cannot send. Decorators call it when
    they are applied, so a bad key fails at import rather than on the first
    call, after whatever ran before it. Whether a key may go without TLS
    depends on the environment when the client is built, so that is checked
    then, not at import."""
    if api_key is not None:
        _checked_api_key(api_key)


def _host_is_local(host: str) -> bool:
    host = host.lower().rstrip(".")
    if host == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        # ::ffff:127.0.0.1, which Python before 3.13 does not call loopback.
        return address.ipv4_mapped.is_loopback
    return address.is_loopback  # all of 127.0.0.0/8, ::1


def _host_of(address: str) -> str:
    """The host in `host:port`, `[v6]:port` or a bare IPv6 address."""
    if address.startswith("["):
        end = address.find("]")
        return address[1:end] if end != -1 else address
    if address.count(":") > 1:
        return address
    return address.rsplit(":", 1)[0]


def _without_authority(rest: str) -> str:
    """What follows a scheme, without a `//authority/` part."""
    if rest.startswith("//"):
        start = rest.find("/", 2) + 1
        return rest[start:] if start else rest[2:]
    return rest


def _is_unix(target: str) -> bool:
    return target.lower().startswith(("unix:", "unix-abstract:"))


@functools.lru_cache(maxsize=256)
def _is_local(target: str) -> bool:
    """Whether `target` names only this machine, in any form gRPC accepts.

    A judgement from the name alone, so it is only the first, readable answer.
    A name can look local and not be: gRPC sends even `localhost` through
    `https_proxy`, and `dns://<server>/localhost` asks that server where
    `localhost` is. A key sent on this judgement therefore goes on a channel
    that checks the real peer; see `_plaintext_channel_for_key`.
    """
    if _is_unix(target):
        return True
    scheme, sep, rest = target.partition(":")
    scheme = scheme.lower()
    if sep and scheme in ("ipv4", "ipv6"):
        # A list: gRPC may send calls to any of them.
        addresses = _without_authority(rest).split(",")
        return all(_host_is_local(_host_of(a)) for a in addresses)
    if sep and scheme == "dns":
        return _host_is_local(_host_of(_without_authority(rest)))
    return _host_is_local(_host_of(target))


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
    """The call details with the key added. grpc keeps one in a frame of every
    failed call's traceback, so its `repr` leaves the metadata out."""

    def __repr__(self) -> str:
        return f"_CallDetails(method={self.method!r}, metadata=<not shown>)"


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
        # Replaces any `authorization` the caller set: the engine reads only the
        # first one, which would otherwise win over the key.
        metadata = [
            (name, value)
            for name, value in details.metadata or []
            if name.lower() != "authorization"
        ] + [self._header]
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


def _plaintext_key_allowed(allow_plaintext_key: Optional[bool]) -> bool:
    """`allow_plaintext_key` if given, else `CELLAFLOW_ALLOW_PLAINTEXT_KEY`,
    read the way the engine reads `CELLAFLOW_ALLOW_UNAUTHENTICATED`."""
    if allow_plaintext_key is not None:
        return allow_plaintext_key
    value = os.environ.get(ALLOW_PLAINTEXT_KEY_ENV_VAR, "")
    return value.strip().lower() in ("1", "true", "yes")


def _plaintext_channel_for_key(
    target: str, allow_plaintext_key: Optional[bool]
) -> grpc.Channel:
    """A channel to send a key on without TLS, or a refusal.

    A target that names this machine gets gRPC's local credentials, which
    refuse the connection unless the peer really is this machine, and no proxy.
    gRPC otherwise sends even `localhost` through `https_proxy`, putting the key
    on the network in plaintext. A name that only looks local (a proxy, a
    resolver that answers with another host) then fails to connect, and the key
    is never sent.

    Any other target is refused unless allowed, so forgetting TLS cannot be how
    a key crosses the network. An allowed one warns once per target.
    """
    if _is_local(target):
        kind = (
            grpc.LocalConnectionType.UDS
            if _is_unix(target)
            else grpc.LocalConnectionType.LOCAL_TCP
        )
        return grpc.secure_channel(
            target,
            grpc.local_channel_credentials(kind),
            options=[("grpc.enable_http_proxy", 0)],
        )
    if not _plaintext_key_allowed(allow_plaintext_key):
        raise ValueError(
            f"Refusing to send the Cellaflow API key to {target} without TLS, "
            f"where anyone on the path could read it. Pass secure=True, or, on a "
            f"network you trust, set {ALLOW_PLAINTEXT_KEY_ENV_VAR}=true (or, "
            f"building a CellaflowClient yourself, pass allow_plaintext_key=True)."
        )
    with _warned_lock:
        first = target not in _warned_plaintext
        _warned_plaintext.add(target)
    if first:
        logger.warning(
            "Cellaflow: sending an API key to %s without TLS, as allowed, so "
            "anyone on the path can read it.",
            target,
        )
    return grpc.insecure_channel(target)


class CellaflowClient:
    """
    gRPC Client for the Cellaflow Engine.
    Handles communication with the engine and strictly uses
    MessagePack for state payloads.

    `api_key` authenticates every call, sent as `authorization: Bearer <key>`.
    When it is not given, it is read from `CELLAFLOW_API_KEY`; with neither,
    calls go unauthenticated, which an engine started without a token accepts.

    A key is not sent without TLS to anything but this machine: the client
    refuses it unless `allow_plaintext_key=True` is passed or
    `CELLAFLOW_ALLOW_PLAINTEXT_KEY` is `true`, for a network you trust. To this
    machine, gRPC checks the connection really is local, and bypasses any
    proxy; one that is not fails to connect rather than send the key.

    The SDK never puts the key in a log message, an exception message or a
    `repr`. grpc's own stack frames do hold the outgoing header, though, so a
    tool that records a traceback's local variables (loguru's `diagnose`,
    Sentry's frame variables, `pytest -l`) can show it. Turn that off wherever
    the key matters.
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        secure: bool = False,
        api_key: Optional[str] = None,
        allow_plaintext_key: Optional[bool] = None,
    ) -> None:
        # Checked before a channel is opened, so a refused key leaves none behind.
        # Held here only in a variable named `api_key`, which error reporters
        # that record a frame's locals scrub by name. grpc's frames hold the
        # header itself; see the class docstring.
        api_key = _checked_api_key(api_key)
        if secure:
            channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        elif api_key is None:
            channel = grpc.insecure_channel(target)
        else:
            channel = _plaintext_channel_for_key(target, allow_plaintext_key)
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
