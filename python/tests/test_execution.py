"""Tests for cellaflow.execution — execution_lease and async_execution_lease."""

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from cellaflow.execution import (
    LeaseNotAcquired,
    async_execution_lease,
    execution_lease,
)
from cellaflow.v1 import idempotency_pb2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_acquired_response(fencing_token: int = 42, heartbeat_interval_ms: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status = idempotency_pb2.CACHE_STATUS_ACQUIRED
    resp.fencing_token = fencing_token
    resp.heartbeat_interval_ms = heartbeat_interval_ms
    return resp


def _make_not_acquired_response() -> MagicMock:
    resp = MagicMock()
    resp.status = idempotency_pb2.CACHE_STATUS_IN_PROGRESS
    return resp


def _make_renew_ok() -> MagicMock:
    resp = MagicMock()
    resp.renewed = True
    resp.new_expires_at_ms = 9999
    return resp


def _make_renew_failed() -> MagicMock:
    resp = MagicMock()
    resp.renewed = False
    resp.failure_reason = idempotency_pb2.RENEW_FAILURE_REASON_EXPIRED
    return resp


# ---------------------------------------------------------------------------
# Sync tests
# ---------------------------------------------------------------------------

def test_execution_lease_acquire_and_release() -> None:
    """Happy path: acquires lease, heartbeats, releases on exit."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease("task:123", worker_id="w-1") as token:
            assert token == 42
            time.sleep(0.05)  # let heartbeat tick

    mock_client.check_idempotency_cache.assert_called_once()
    call_kwargs = mock_client.check_idempotency_cache.call_args[1]
    assert call_kwargs["agent_id"] == "w-1"
    assert call_kwargs["idempotency_key"] == "task:123"

    mock_client.release_lease.assert_called_once()
    release_kwargs = mock_client.release_lease.call_args[1]
    assert release_kwargs["fencing_token"] == 42

    mock_client.close.assert_called_once()


def test_execution_lease_not_acquired() -> None:
    """Raises LeaseNotAcquired immediately when engine returns non-acquired."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired):
            with execution_lease("task:123", worker_id="w-1"):
                pass  # should not be reached

    # Release should NOT be called — we never held the lease.
    mock_client.release_lease.assert_not_called()
    # Client must still be closed.
    mock_client.close.assert_called_once()


def test_execution_lease_heartbeat_starts() -> None:
    """Verifies LeaseHeartbeat is spawned with correct parameters."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        fencing_token=99, heartbeat_interval_ms=300
    )
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with patch("cellaflow.execution.LeaseHeartbeat") as MockHB:
            mock_hb_instance = MagicMock()
            MockHB.return_value = mock_hb_instance

            with execution_lease("task:abc", worker_id="w-2") as token:
                assert token == 99

            MockHB.assert_called_once_with(
                client=mock_client,
                agent_id="w-2",
                idempotency_key="task:abc",
                fencing_token=99,
                heartbeat_interval_ms=300,
                on_lease_lost=None,
            )
            mock_hb_instance.start_sync.assert_called_once()
            mock_hb_instance.stop_sync.assert_called_once()


def test_execution_lease_cleans_up_on_exception() -> None:
    """Lease is released even when the body raises."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(ValueError, match="body error"):
            with execution_lease("task:123", worker_id="w-1"):
                raise ValueError("body error")

    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


def test_execution_lease_owns_client() -> None:
    """The context manager creates its own client and closes it on exit."""
    created_clients: list = []

    def fake_client_factory(target: str, secure: bool) -> MagicMock:
        c = MagicMock()
        c.check_idempotency_cache.return_value = _make_acquired_response()
        c.renew_lease.return_value = _make_renew_ok()
        created_clients.append(c)
        return c

    with patch("cellaflow.execution.CellaflowClient", side_effect=fake_client_factory):
        with execution_lease("task:123", worker_id="w-1", target="myhost:50051"):
            pass

    assert len(created_clients) == 1, "Expected exactly one client to be created"
    created_clients[0].close.assert_called_once()


def test_execution_lease_on_lease_lost_callback_sync() -> None:
    """on_lease_lost is invoked when heartbeat renewal is denied."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=150
    )
    mock_client.renew_lease.return_value = _make_renew_failed()

    lost_event = threading.Event()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease(
            "task:123",
            worker_id="w-1",
            on_lease_lost=lost_event.set,
        ):
            lost_event.wait(timeout=1.0)

    assert lost_event.is_set(), "on_lease_lost should have been called"


# ---------------------------------------------------------------------------
# Async tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_execution_lease_acquire_and_release() -> None:
    """Async happy path: acquires, heartbeats, releases on exit."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        async with async_execution_lease("task:123", worker_id="w-1") as token:
            assert token == 42
            await asyncio.sleep(0.05)

    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_async_execution_lease_preemption() -> None:
    """Lease loss mid-execution cancels the calling task (CancelledError)."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=150
    )
    # First renewal succeeds; subsequent ones fail.
    mock_client.renew_lease.side_effect = [
        _make_renew_ok(),
        _make_renew_failed(),
    ]

    cancelled = False

    async def run() -> None:
        nonlocal cancelled
        with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
            try:
                async with async_execution_lease("task:123", worker_id="w-1"):
                    await asyncio.sleep(5)  # will be cancelled
            except asyncio.CancelledError:
                cancelled = True

    task = asyncio.create_task(run())
    await asyncio.wait_for(task, timeout=3.0)

    assert cancelled, "CancelledError should have been raised when lease was lost"
