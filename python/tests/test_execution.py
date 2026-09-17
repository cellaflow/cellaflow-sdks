"""Tests for cellaflow.execution — execution_lease and async_execution_lease."""

import asyncio
import threading
import time
import warnings
from unittest.mock import MagicMock, patch

import pytest

from cellaflow.execution import (
    LeaseLostError,
    LeaseNotAcquired,
    async_execution_lease,
    execution_lease,
)
from cellaflow.v1 import idempotency_pb2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_acquired_response(fencing_token: int = 42, heartbeat_interval_ms: int = 100) -> MagicMock:
    resp = MagicMock()
    resp.status = idempotency_pb2.CACHE_STATUS_ACQUIRED
    resp.fencing_token = fencing_token
    resp.heartbeat_interval_ms = heartbeat_interval_ms
    return resp


def _make_not_acquired_response(status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS) -> MagicMock:
    resp = MagicMock()
    resp.status = status
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
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(heartbeat_interval_ms=50)
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease("task:123", worker_id="w-1") as lease:
            assert lease.fencing_token == 42
            time.sleep(0.15)  # wait longer than heartbeat_interval_ms to guarantee a tick
            lease.check() # Should not raise

    mock_client.check_idempotency_cache.assert_called_once()
    call_kwargs = mock_client.check_idempotency_cache.call_args[1]
    assert call_kwargs["agent_id"] == "w-1"
    assert call_kwargs["idempotency_key"] == "task:123"

    assert mock_client.renew_lease.call_count > 0, "Heartbeat should have fired"

    mock_client.release_lease.assert_called_once()
    release_kwargs = mock_client.release_lease.call_args[1]
    assert release_kwargs["fencing_token"] == 42

    mock_client.close.assert_called_once()


def test_execution_lease_not_acquired_in_progress() -> None:
    """Raises LeaseNotAcquired immediately when engine returns IN_PROGRESS."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response(idempotency_pb2.CACHE_STATUS_IN_PROGRESS)

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired, match="held by a live worker"):
            with execution_lease("task:123", worker_id="w-1"):
                pass  # should not be reached


def test_execution_lease_not_acquired_hit() -> None:
    """Raises LeaseNotAcquired with specific message when engine returns HIT."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response(idempotency_pb2.CACHE_STATUS_HIT)

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired, match="already completed"):
            with execution_lease("task:123", worker_id="w-1"):
                pass  # should not be reached


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

            with execution_lease("task:abc", worker_id="w-2") as lease:
                assert lease.fencing_token == 99

            MockHB.assert_called_once()
            kwargs = MockHB.call_args[1]
            assert kwargs["client"] == mock_client
            assert kwargs["agent_id"] == "w-2"
            assert kwargs["idempotency_key"] == "task:abc"
            assert kwargs["fencing_token"] == 99
            assert kwargs["heartbeat_interval_ms"] == 300
            assert kwargs["max_network_errors"] == 1 # Execution leases fail fast
            
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


def test_sync_lease_check_raises() -> None:
    """LeaseHandle.check() raises LeaseLostError if the lease is lost."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(heartbeat_interval_ms=50)
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseLostError, match="Execution lease was lost"):
            with execution_lease("task:123", worker_id="w-1") as lease:
                time.sleep(0.15) # wait for heartbeat to fail
                lease.check()


def test_sync_lease_warns_if_unchecked() -> None:
    """Emits RuntimeWarning if the lease was lost but check() was never called."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(heartbeat_interval_ms=50)
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with execution_lease("task:123", worker_id="w-1") as lease:
                time.sleep(0.15) # wait for heartbeat to fail
                # Intentionally not calling lease.check()

            assert len(w) == 1
            assert issubclass(w[-1].category, RuntimeWarning)
            assert "was lost during execution, but lease.check() was never called" in str(w[-1].message)


def test_sync_lease_no_warn_if_checked() -> None:
    """Does not emit RuntimeWarning if check() was called."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(heartbeat_interval_ms=50)
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(LeaseLostError):
                with execution_lease("task:123", worker_id="w-1") as lease:
                    time.sleep(0.15)
                    lease.check() # Calls check(), raises exception

            # No warning should be emitted because they checked it
            assert len(w) == 0


# ---------------------------------------------------------------------------
# Async tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_execution_lease_acquire_and_release() -> None:
    """Async happy path: acquires, heartbeats, releases on exit."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(heartbeat_interval_ms=50)
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        async with async_execution_lease("task:123", worker_id="w-1") as lease:
            assert lease.fencing_token == 42
            await asyncio.sleep(0.15)
            lease.check()

    assert mock_client.renew_lease.call_count > 0, "Heartbeat should have fired"
    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_async_execution_lease_preemption() -> None:
    """Lease loss mid-execution cancels the calling task (CancelledError)."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
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
                async with async_execution_lease("task:123", worker_id="w-1") as lease:
                    await asyncio.sleep(5)  # will be cancelled at an await point
            except asyncio.CancelledError:
                cancelled = True

    task = asyncio.create_task(run())
    await asyncio.wait_for(task, timeout=3.0)

    assert cancelled, "CancelledError should have been raised when lease was lost"


@pytest.mark.asyncio
async def test_async_cleanup_runs_on_cancel() -> None:
    """If the body is cancelled (externally or by lease loss), cleanup still runs."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    async def run() -> None:
        with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
            async with async_execution_lease("task:123", worker_id="w-1"):
                await asyncio.sleep(5) # will be externally cancelled

    task = asyncio.create_task(run())
    
    # Wait for context manager to enter
    await asyncio.sleep(0.05)
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task
        
    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()
