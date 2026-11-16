"""The idempotency key a step body sees through current_lease(): the key its
call's lease was taken under, the same on every attempt, and the helpers for
sending it to a destination that deduplicates. Uses a scripted engine client."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import asyncio
import hashlib
from typing import Any, Dict, List

import pytest

from cellaflow import IdempotencyScope, current_lease, execution_lease, tool
from cellaflow.execution import LeaseHandle
from cellaflow.v1 import idempotency_pb2

from tests.test_lease_check import ScriptedClient, session


@pytest.mark.parametrize(
    "options, args",
    [
        ({}, ("4417",)),
        ({"scope": IdempotencyScope.SCOPE_STEP_LOCAL}, ("4417",)),
        ({"scope": IdempotencyScope.SCOPE_SHARED, "shared_on": ["order"]}, ("4417",)),
        ({"scope": IdempotencyScope.SCOPE_AGENT_PRIVATE}, ("4417",)),
        ({"idempotency_key": "refund:4417"}, ("4417",)),
    ],
)
def test_the_key_is_the_one_the_lease_was_taken_under(
    options: Dict[str, Any], args: Any
) -> None:
    client = ScriptedClient()

    @tool(**options)
    def refund(order: str) -> str:
        return current_lease().key

    with session(client).bind():
        key = refund(*args)
    assert key and key == client.acquires[0]["idempotency_key"]
    if "idempotency_key" in options:
        assert key == "refund:4417"


@pytest.mark.asyncio
async def test_an_async_step_sees_its_key_too() -> None:
    client = ScriptedClient()

    @tool
    async def refund(order: str) -> str:
        await asyncio.sleep(0)
        return current_lease().key

    with session(client).bind():
        key = await refund("4417")
    assert key == client.acquires[0]["idempotency_key"]


def test_two_attempts_at_the_same_call_share_the_key_not_the_token() -> None:
    # A shared call: the holder in one session, and the worker that takes it
    # over in another after the holder's lease lapsed.
    seen: List[Any] = []

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order"])
    def refund(order: str) -> str:
        lease = current_lease()
        seen.append((lease.key, lease.fencing_token))
        return order

    holder = ScriptedClient()
    with session(holder).bind():
        refund("4417")
    taker = ScriptedClient(
        [
            idempotency_pb2.CheckCacheResponse(
                status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
                fencing_token=42,
                heartbeat_interval_ms=250,
            )
        ]
    )
    other = session(taker)
    other.session_id = "s-taker"
    with other.bind():
        refund("4417")
    assert seen[0][0] == seen[1][0]
    assert seen[0][1] != seen[1][1]


def test_the_digest_is_the_keys_sha256() -> None:
    lease = LeaseHandle(1, key="shared:order-4417:refund:abc")
    expected = hashlib.sha256(b"shared:order-4417:refund:abc").hexdigest()
    assert lease.key_digest == expected and len(expected) == 64


def test_idempotency_headers_send_the_digest_by_default() -> None:
    lease = LeaseHandle(1, key="refund:4417")
    assert lease.idempotency_headers() == {"Idempotency-Key": lease.key_digest}
    assert lease.idempotency_headers(raw=True) == {"Idempotency-Key": "refund:4417"}
    assert lease.idempotency_headers(header="PayPal-Request-Id") == {
        "PayPal-Request-Id": lease.key_digest
    }


def test_idempotency_headers_options_are_keyword_only() -> None:
    lease = LeaseHandle(1, key="refund:4417")
    with pytest.raises(TypeError):
        getattr(lease, "idempotency_headers")("X-Request-Id")


def test_a_key_that_is_not_ascii_goes_as_its_digest() -> None:
    lease = LeaseHandle(1, key="shared:订单-4417:refund:abc")
    expected = hashlib.sha256("shared:订单-4417:refund:abc".encode("utf-8")).hexdigest()
    assert lease.key_digest == expected
    assert lease.idempotency_headers() == {"Idempotency-Key": expected}
    with pytest.raises(ValueError, match="cannot carry"):
        lease.idempotency_headers(raw=True)


def test_the_key_cannot_be_reassigned() -> None:
    lease = LeaseHandle(1, key="refund:4417")
    with pytest.raises(AttributeError):
        lease.key = "other"  # type: ignore[misc]


def test_a_handle_with_no_key_refuses_to_send_one() -> None:
    lease = LeaseHandle(1)
    with pytest.raises(ValueError, match="no idempotency key"):
        lease.key_digest
    with pytest.raises(ValueError, match="no idempotency key"):
        lease.idempotency_headers()
    with pytest.raises(ValueError, match="no idempotency key"):
        lease.idempotency_headers(raw=True)


def test_a_call_under_its_own_coordination_id_sees_the_key_derived_from_it() -> None:
    client = ScriptedClient()

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order"])
    def refund(order: str) -> str:
        return current_lease().key

    with session(client).bind():
        key = refund("4417", _coordination_id="customer-88")
    assert key == client.acquires[0]["idempotency_key"]
    assert "customer-88" in key


@pytest.mark.asyncio
async def test_an_async_shared_step_sees_its_key() -> None:
    client = ScriptedClient()

    @tool(scope=IdempotencyScope.SCOPE_SHARED, shared_on=["order"])
    async def refund(order: str) -> str:
        await asyncio.sleep(0)
        return current_lease().key

    with session(client).bind():
        key = await refund("4417")
    assert key == client.acquires[0]["idempotency_key"]


def test_a_task_lease_with_a_derived_key_carries_it(monkeypatch: Any) -> None:
    from cellaflow import task_lease

    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)

    @task_lease(lambda task_id: f"task:{task_id}", worker_id="w-1", ttl_ms=2000)
    def process(task_id: str) -> str:
        return current_lease().key

    assert process("abc") == "task:abc"
    assert process("def") == "task:def"


def test_an_execution_lease_carries_its_key(monkeypatch: Any) -> None:
    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    with execution_lease("task:abc-123", worker_id="w-1", ttl_ms=2000) as lease:
        assert lease.key == "task:abc-123"


@pytest.mark.asyncio
async def test_an_async_execution_lease_carries_its_key(monkeypatch: Any) -> None:
    from cellaflow import async_execution_lease

    client = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: client)
    async with async_execution_lease(
        "task:abc-123", worker_id="w-1", ttl_ms=2000
    ) as lease:
        assert lease.key == "task:abc-123"


@pytest.mark.parametrize("bad", ["a\r\nb", " padded", "padded ", "tab\there"])
def test_a_raw_key_a_header_cannot_carry_is_refused(bad: str) -> None:
    lease = LeaseHandle(1, key=bad)
    with pytest.raises(ValueError, match="cannot carry"):
        lease.idempotency_headers(raw=True)
    assert len(lease.idempotency_headers()["Idempotency-Key"]) == 64


def test_a_raw_key_with_an_inner_space_is_sent() -> None:
    lease = LeaseHandle(1, key="refund 4417")
    assert lease.idempotency_headers(raw=True) == {"Idempotency-Key": "refund 4417"}


@pytest.mark.parametrize("bad", ["Idempotency Key", "X-Key:", "", "Kéy"])
def test_a_header_name_that_is_not_a_token_is_refused(bad: str) -> None:
    with pytest.raises(ValueError, match="not a valid header name"):
        LeaseHandle(1, key="refund:4417").idempotency_headers(header=bad)


def test_a_body_without_a_lease_of_its_own_sees_no_enclosing_lease(
    monkeypatch: Any,
) -> None:
    # An answer this SDK does not know runs the body without a lease; inside an
    # execution lease, the lock's key must not reach it in the call's name.
    unknown = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_UNSPECIFIED
    )
    outer = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: outer)
    client = ScriptedClient([unknown])

    @tool
    def charge_card() -> str:
        try:
            return current_lease().key
        except LookupError:
            return "no lease"

    with execution_lease("lock:account-7", worker_id="w-1", ttl_ms=2000) as lease:
        with session(client).bind():
            assert charge_card() == "no lease"
        # And the enclosing lease is back once the step returns.
        assert current_lease() is lease


@pytest.mark.parametrize(
    "key, digest",
    [
        (
            "refund:4417",
            "393b2ae43cab6d289df9fc2c73f49ffb273b85ed5f2ca410005df7df803d210f",
        ),
        (
            "shared:订单-4417:refund:abc",
            "84e2e882b60d5980b7021b9db7d656b8d8f7a28ddee9343c77675e88c7f24443",
        ),
    ],
)
def test_the_digest_matches_the_typescript_sdk(key: str, digest: str) -> None:
    # Pinned values, shared with the TypeScript tests: agents in either
    # language send the same digest for the same key.
    assert LeaseHandle(1, key=key).key_digest == digest


@pytest.mark.asyncio
async def test_an_async_body_without_a_lease_of_its_own_sees_no_enclosing_lease(
    monkeypatch: Any,
) -> None:
    from cellaflow import async_execution_lease

    unknown = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_UNSPECIFIED
    )
    outer = ScriptedClient()
    monkeypatch.setattr("cellaflow.execution.CellaflowClient", lambda **kw: outer)
    client = ScriptedClient([unknown])

    @tool
    async def charge_card() -> str:
        await asyncio.sleep(0)
        try:
            return current_lease().key
        except LookupError:
            return "no lease"

    async with async_execution_lease(
        "lock:account-7", worker_id="w-1", ttl_ms=2000
    ) as lease:
        with session(client).bind():
            assert await charge_card() == "no lease"
        assert current_lease() is lease


def test_each_of_several_requests_gets_its_own_key() -> None:
    lease = LeaseHandle(1, key="refund:4417")
    again = LeaseHandle(2, key="refund:4417")
    assert lease.key_digest_for("payment") == again.key_digest_for("payment")
    assert lease.key_digest_for("payment") != lease.key_digest_for("customer")
    assert lease.key_digest_for("payment") != lease.key_digest
    # Pinned, shared with the TypeScript tests.
    assert (
        lease.key_digest_for("payment")
        == "531f4ed05fa09544dfd9db3ebea89affec6e983821c53a6914edec69592d5b14"
    )
    assert lease.idempotency_headers(part="payment") == {
        "Idempotency-Key": lease.key_digest_for("payment")
    }
    with pytest.raises(TypeError):
        lease.idempotency_headers(part="payment", raw=True)
    for bad_type in (None, 7):
        with pytest.raises(TypeError):
            lease.key_digest_for(bad_type)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            lease.idempotency_headers(part=bad_type)  # type: ignore[arg-type]
    for bad_value in ("", "\ud800"):
        with pytest.raises(ValueError):
            lease.key_digest_for(bad_value)
        with pytest.raises(ValueError):
            lease.idempotency_headers(part=bad_value)
    # A character outside the BMP is fine, and pinned with the TypeScript tests.
    assert (
        lease.key_digest_for("\U0001f600")
        == "390afccc84e99e8dd21ced1c6182206223f4781427658e5ab1a44c5ad1939d5f"
    )
    assert lease.idempotency_headers(part="payment", header="PayPal-Request-Id") == {
        "PayPal-Request-Id": lease.key_digest_for("payment")
    }
