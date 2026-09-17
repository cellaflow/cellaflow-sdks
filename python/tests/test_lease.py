import pytest
import asyncio
import threading
import time
from unittest.mock import MagicMock

from cellaflow.lease import LeaseHeartbeat
from cellaflow.v1 import idempotency_pb2


def test_lease_heartbeat_sync() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1000)
    mock_client.renew_lease.return_value = mock_resp

    # Small interval so it fires quickly
    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        # 200ms -> interval_sec = max(0.1, (200-100)/1000) = 0.1
        heartbeat_interval_ms=200,
    )

    hb.start_sync()
    time.sleep(0.25)  # Give it time to tick twice
    hb.stop_sync()

    assert mock_client.renew_lease.call_count >= 1
    call_args = mock_client.renew_lease.call_args[1]
    assert call_args["agent_id"] == "agent1"
    assert call_args["fencing_token"] == 123


@pytest.mark.asyncio
async def test_lease_heartbeat_async() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1000)
    mock_client.renew_lease.return_value = mock_resp

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        heartbeat_interval_ms=200,
    )

    hb.start_async()
    await asyncio.sleep(0.25)
    await hb.stop_async()

    assert mock_client.renew_lease.call_count >= 1
    call_args = mock_client.renew_lease.call_args[1]
    assert call_args["agent_id"] == "agent1"


def test_lease_heartbeat_sync_renewal_failure() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(
        renewed=False,
        failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_EXPIRED,
    )
    mock_client.renew_lease.return_value = mock_resp

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        heartbeat_interval_ms=150,
    )
    hb.start_sync()
    time.sleep(0.2)
    hb.stop_sync()
    assert mock_client.renew_lease.call_count >= 1


@pytest.mark.asyncio
async def test_lease_heartbeat_async_renewal_failure() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(
        renewed=False,
        failure_reason=idempotency_pb2.RENEW_FAILURE_REASON_EXPIRED,
    )
    mock_client.renew_lease.return_value = mock_resp

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        heartbeat_interval_ms=150,
    )
    hb.start_async()
    await asyncio.sleep(0.2)
    await hb.stop_async()
    assert mock_client.renew_lease.call_count >= 1


def test_lease_heartbeat_exception_logged() -> None:
    mock_client = MagicMock()
    mock_client.renew_lease.side_effect = RuntimeError("Network partition")

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        heartbeat_interval_ms=150,
    )
    hb.start_sync()
    time.sleep(0.2)
    hb.stop_sync()
    assert mock_client.renew_lease.call_count >= 1


# ---------------------------------------------------------------------------
# Loss detection is time-based, not error-count-based
# ---------------------------------------------------------------------------


def test_transient_errors_do_not_lose_a_lease_that_is_still_valid() -> None:
    """A failed renewal means "could not confirm", not "lost".

    Counting errors instead would abort work that still holds a perfectly good
    lease: with a 15s TTL and a 5s interval, one blip has nine seconds of
    validity left. Loss is declared on elapsed time, so a recoverable hiccup
    recovers.
    """
    mock_client = MagicMock()
    lost = []
    mock_client.renew_lease.side_effect = [
        ConnectionError("transient 1"),
        ConnectionError("transient 2"),
        ConnectionError("transient 3"),
        idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1000),
    ]

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=1,
        heartbeat_interval_ms=100,
        on_lease_lost=lambda: lost.append(True),
        lease_ttl_ms=60_000,  # far longer than this test runs
    )
    hb.start_sync()
    time.sleep(0.6)
    hb.stop_sync()

    assert mock_client.renew_lease.call_count >= 4, "must keep retrying"
    assert lost == [], "the lease was never actually lost"


def test_lease_is_declared_lost_once_the_ttl_elapses_without_a_renewal() -> None:
    mock_client = MagicMock()
    lost = []
    mock_client.renew_lease.side_effect = ConnectionError("engine unreachable")

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=1,
        heartbeat_interval_ms=100,
        on_lease_lost=lambda: lost.append(True),
        lease_ttl_ms=200,
    )
    hb.start_sync()
    time.sleep(0.8)
    hb.stop_sync()

    assert lost == [True], "loss must be declared once the TTL is exhausted"


def test_renewals_extend_by_the_lease_ttl() -> None:
    """`extend_ms` derived from the interval silently shrank a custom TTL."""
    hb = LeaseHeartbeat(
        client=MagicMock(),
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=5_000,
        lease_ttl_ms=60_000,
    )
    assert hb.extend_ms == 60_000


def test_ttl_defaults_to_four_times_the_interval_when_unstated() -> None:
    """`@step` states no TTL, so its historical behaviour must be preserved."""
    hb = LeaseHeartbeat(
        client=MagicMock(),
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=5_000,
    )
    assert hb.lease_ttl_ms == 20_000
    assert hb.extend_ms == 20_000


def test_renewals_carry_an_rpc_deadline() -> None:
    """Without one, a black-holed connection parks the thread indefinitely."""
    mock_client = MagicMock()
    mock_client.renew_lease.return_value = idempotency_pb2.RenewLeaseResponse(
        renewed=True, new_expires_at_ms=1000
    )

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=100,
    )
    hb.start_sync()
    time.sleep(0.25)
    hb.stop_sync()

    assert mock_client.renew_lease.call_count > 0
    timeout = mock_client.renew_lease.call_args[1]["timeout"]
    assert timeout is not None and timeout > 0


def test_stop_sync_is_bounded_when_a_renewal_hangs() -> None:
    """`@step` calls this in its `finally`; an unbounded join hangs every step."""
    release = threading.Event()

    def _hanging_renew(**_kwargs: object) -> object:
        release.wait(timeout=30)
        return idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1)

    mock_client = MagicMock()
    mock_client.renew_lease.side_effect = _hanging_renew

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=100,
    )
    hb.stop_timeout_sec = 0.3  # keep the test quick; the bound is what matters
    hb.start_sync()
    time.sleep(0.2)  # let the loop enter the hanging RPC

    started = time.monotonic()
    hb.stop_sync()
    elapsed = time.monotonic() - started

    release.set()
    assert (
        elapsed < 2.0
    ), f"stop_sync must not block on a hung RPC (took {elapsed:.1f}s)"


@pytest.mark.asyncio
async def test_stop_async_is_bounded_when_a_renewal_hangs() -> None:
    release = threading.Event()

    def _hanging_renew(**_kwargs: object) -> object:
        release.wait(timeout=30)
        return idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1)

    mock_client = MagicMock()
    mock_client.renew_lease.side_effect = _hanging_renew

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="a",
        idempotency_key="k",
        fencing_token=1,
        heartbeat_interval_ms=100,
    )
    hb.stop_timeout_sec = 0.3
    hb.start_async()
    await asyncio.sleep(0.2)

    started = time.monotonic()
    await hb.stop_async()
    elapsed = time.monotonic() - started

    release.set()
    assert elapsed < 2.0, f"stop_async must not block on a hung RPC ({elapsed:.1f}s)"
