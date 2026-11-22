"""Tests for cellaflow.execution — execution_lease and async_execution_lease."""

import asyncio
import contextlib
import dataclasses
import pickle
import time
import types
import warnings
from unittest.mock import MagicMock, patch

import pytest

import cellaflow
from cellaflow.execution import (
    LeaseHolderInfo,
    LeaseLostError,
    LeaseNotAcquired,
    async_execution_lease,
    current_lease,
    execution_lease,
    task_lease,
)
from cellaflow.failures import capture, rebuild
from cellaflow.v1 import idempotency_pb2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_acquired_response(
    fencing_token: int = 42, heartbeat_interval_ms: int = 100
) -> MagicMock:
    resp = MagicMock()
    resp.status = idempotency_pb2.CACHE_STATUS_ACQUIRED
    resp.fencing_token = fencing_token
    resp.heartbeat_interval_ms = heartbeat_interval_ms
    return resp


def _make_not_acquired_response(
    status: int = idempotency_pb2.CACHE_STATUS_IN_PROGRESS,
) -> MagicMock:
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
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease("task:123", worker_id="w-1") as lease:
            assert lease.fencing_token == 42
            time.sleep(0.15)  # longer than the interval, to guarantee a tick
            lease.check()  # Should not raise

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
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response(
        idempotency_pb2.CACHE_STATUS_IN_PROGRESS
    )

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired, match="held by a live worker"):
            with execution_lease("task:123", worker_id="w-1"):
                pass  # should not be reached


def test_execution_lease_not_acquired_hit() -> None:
    """Raises LeaseNotAcquired with specific message when engine returns HIT."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response(
        idempotency_pb2.CACHE_STATUS_HIT
    )

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired, match="already completed"):
            with execution_lease("task:123", worker_id="w-1"):
                pass  # should not be reached


def _refusal(resp: idempotency_pb2.CheckCacheResponse) -> LeaseNotAcquired:
    """What execution_lease raises when the engine answers with `resp`."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = resp
    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired) as excinfo:
            with execution_lease("task:4417", worker_id="agent-b"):
                pytest.fail("the body ran")
    return excinfo.value


def _in_progress(
    holder: "idempotency_pb2.LeaseHolder | None",
) -> idempotency_pb2.CheckCacheResponse:
    resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS,
        lease_expires_at_ms=1_763_800_030_000,
    )
    if holder is not None:
        resp.current_holder.CopyFrom(holder)
    return resp


def test_refusal_names_the_worker_holding_the_lease() -> None:
    err = _refusal(
        _in_progress(
            idempotency_pb2.LeaseHolder(
                agent_id="agent-a",
                holder_id="holder-1",
                acquired_at_ms=1_763_800_000_000,
                expires_at_ms=1_763_800_030_000,
                hard_expires_at_ms=1_763_800_600_000,
            )
        )
    )
    assert err.holder == LeaseHolderInfo(
        worker_id="agent-a",
        holder_id="holder-1",
        acquired_at_ms=1_763_800_000_000,
        expires_at_ms=1_763_800_030_000,
        hard_expires_at_ms=1_763_800_600_000,
    )
    # The message is unchanged, so nothing matching on it breaks.
    assert str(err) == (
        "Lease 'task:4417' is held by a live worker. Another worker is "
        "actively heartbeating this task."
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        err.holder.worker_id = "someone-else"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_async_refusal_names_the_worker_holding_the_lease() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _in_progress(
        idempotency_pb2.LeaseHolder(
            agent_id="agent-a", holder_id="holder-1", expires_at_ms=2
        )
    )
    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseNotAcquired) as excinfo:
            async with async_execution_lease("task:4417", worker_id="agent-b"):
                pytest.fail("the body ran")
    assert excinfo.value.holder is not None
    assert excinfo.value.holder.worker_id == "agent-a"


def test_refusal_reports_what_was_not_recorded_as_none() -> None:
    err = _refusal(
        _in_progress(
            idempotency_pb2.LeaseHolder(holder_id="holder-1", expires_at_ms=2)
        )
    )
    assert err.holder == LeaseHolderInfo(
        worker_id=None,
        holder_id="holder-1",
        acquired_at_ms=None,
        expires_at_ms=2,
        hard_expires_at_ms=None,
    )


def test_refusal_without_a_reported_holder_has_none() -> None:
    """An engine too old to send `current_holder`."""
    assert _refusal(_in_progress(None)).holder is None


def test_refusal_from_a_plain_test_double_has_no_holder() -> None:
    """A stand-in response without protobuf's HasField is still a refusal."""
    err = _refusal(
        types.SimpleNamespace(  # type: ignore[arg-type]
            status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS
        )
    )
    assert err.holder is None


def test_completed_key_has_no_holder() -> None:
    # A holder in the answer too, so this fails if a HIT were read for one.
    resp = idempotency_pb2.CheckCacheResponse(status=idempotency_pb2.CACHE_STATUS_HIT)
    resp.current_holder.CopyFrom(
        idempotency_pb2.LeaseHolder(agent_id="agent-a", holder_id="holder-1")
    )
    err = _refusal(resp)
    assert "already completed" in str(err)
    assert err.holder is None


def test_lease_not_acquired_keeps_its_contract() -> None:
    err = LeaseNotAcquired("busy")
    assert isinstance(err, RuntimeError)
    assert str(err) == "busy"
    assert err.holder is None
    # Built as before: no arguments, several, or as a mock's side effect.
    assert LeaseNotAcquired().args == ()
    assert LeaseNotAcquired("a", "b").args == ("a", "b")
    with pytest.raises(LeaseNotAcquired):
        MagicMock(side_effect=LeaseNotAcquired)()
    assert {"LeaseHolderInfo", "LeaseNotAcquired"} <= set(cellaflow.__all__)
    assert cellaflow.LeaseHolderInfo is LeaseHolderInfo


def test_task_lease_refusal_names_the_holder() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _in_progress(
        idempotency_pb2.LeaseHolder(
            agent_id="agent-a", holder_id="holder-1", expires_at_ms=2
        )
    )
    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease("task:4417", worker_id="agent-b")
        def process() -> None:
            pytest.fail("the body ran")

        with pytest.raises(LeaseNotAcquired) as excinfo:
            process()
    assert excinfo.value.holder is not None
    assert excinfo.value.holder.worker_id == "agent-a"


def test_a_recorded_refusal_replays_with_its_holder() -> None:
    """A step that raised the refusal, caught by the workflow, is recorded, and
    replay reads the same holder, so a handler that logged `holder.worker_id`
    behaves the same."""
    holder = LeaseHolderInfo(
        worker_id="agent-a",
        holder_id="holder-1",
        acquired_at_ms=1,
        expires_at_ms=2,
        hard_expires_at_ms=None,
    )
    recorded = capture(LeaseNotAcquired("busy", holder=holder))
    assert "holder" not in recorded.get("unavailable_attrs", [])
    assert "_holder" not in recorded.get("unavailable_attrs", [])
    replayed = rebuild(recorded, "refund", 1)
    assert isinstance(replayed, LeaseNotAcquired)
    assert str(replayed) == "busy"
    assert replayed.holder == holder

    without = rebuild(capture(LeaseNotAcquired("busy")), "refund", 1)
    assert isinstance(without, LeaseNotAcquired)
    assert without.holder is None

    # A record written before the holder existed has no `_holder` at all.
    older = capture(LeaseNotAcquired("busy"))
    older["attrs"] = {}
    replayed_older = rebuild(older, "refund", 1)
    assert isinstance(replayed_older, LeaseNotAcquired)
    assert replayed_older.holder is None

    # A field a later version recorded is dropped rather than refused.
    newer = capture(LeaseNotAcquired("busy", holder=holder))
    newer["attrs"]["_holder"]["region"] = "eu-west-1"
    replayed_newer = rebuild(newer, "refund", 1)
    assert isinstance(replayed_newer, LeaseNotAcquired)
    assert replayed_newer.holder == holder


def test_a_refusal_recorded_by_this_version_still_replays() -> None:
    """The recorded shape, written out by hand: a test that rebuilds what
    `capture` writes today would change along with a renamed attribute or a
    new required field and miss that records already written break."""
    recorded = capture(LeaseNotAcquired("busy"))
    recorded["attrs"] = {
        "_holder": {
            "worker_id": "agent-a",
            "holder_id": "holder-1",
            "acquired_at_ms": 1,
            "expires_at_ms": 2,
            "hard_expires_at_ms": None,
        }
    }
    replayed = rebuild(recorded, "refund", 1)
    assert isinstance(replayed, LeaseNotAcquired)
    assert replayed.holder == LeaseHolderInfo(
        worker_id="agent-a",
        holder_id="holder-1",
        acquired_at_ms=1,
        expires_at_ms=2,
        hard_expires_at_ms=None,
    )


def test_holder_can_be_set_after_construction() -> None:
    err = LeaseNotAcquired("busy")
    holder = LeaseHolderInfo(
        worker_id="agent-a",
        holder_id="holder-1",
        acquired_at_ms=None,
        expires_at_ms=2,
        hard_expires_at_ms=None,
    )
    err.holder = holder
    assert err.holder == holder
    err.holder = None
    assert err.holder is None


def test_lease_not_acquired_pickles_with_its_holder() -> None:
    """A refusal raised in a worker process reaches the parent intact."""
    holder = LeaseHolderInfo(
        worker_id="agent-a",
        holder_id="holder-1",
        acquired_at_ms=1,
        expires_at_ms=2,
        hard_expires_at_ms=None,
    )
    copy = pickle.loads(pickle.dumps(LeaseNotAcquired("busy", holder=holder)))
    assert str(copy) == "busy"
    assert copy.holder == holder


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
            assert kwargs["lease_ttl_ms"] == 15_000, (
                "the caller's TTL must reach the heartbeat, or renewals extend "
                "by a value derived from the interval instead"
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


def test_sync_lease_check_raises() -> None:
    """LeaseHandle.check() raises LeaseLostError if the lease is lost."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with pytest.raises(LeaseLostError, match="Execution lease was lost"):
            with execution_lease("task:123", worker_id="w-1") as lease:
                time.sleep(0.15)  # wait for heartbeat to fail
                lease.check()


def test_sync_lease_warns_if_unchecked() -> None:
    """Emits RuntimeWarning if the lease was lost but check() was never called."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with execution_lease("task:123", worker_id="w-1"):
                time.sleep(0.15)  # wait for heartbeat to fail
                # Intentionally not calling lease.check()

            assert len(w) == 1
            assert issubclass(w[-1].category, RuntimeWarning)
            assert (
                "was lost during execution, but lease.check() was never called"
                in str(w[-1].message)
            )


def test_sync_lease_no_warn_if_checked() -> None:
    """Does not emit RuntimeWarning if check() was called."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(LeaseLostError):
                with execution_lease("task:123", worker_id="w-1") as lease:
                    time.sleep(0.15)
                    lease.check()  # Calls check(), raises exception

            # No warning should be emitted because they checked it
            assert len(w) == 0


# ---------------------------------------------------------------------------
# Async tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_execution_lease_acquire_and_release() -> None:
    """Async happy path: acquires, heartbeats, releases on exit."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
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
                async with async_execution_lease("task:123", worker_id="w-1"):
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
                await asyncio.sleep(5)  # will be externally cancelled

    task = asyncio.create_task(run())

    # Wait for context manager to enter
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


# ---------------------------------------------------------------------------
# Preemption policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_lease_lost_does_not_disable_preemption() -> None:
    """A notification callback must not silently switch preemption off.

    Supplying `on_lease_lost` used to *replace* the cancellation, so adding a
    log line or a metric quietly turned off the abort this primitive exists to
    provide. The callback is a notification; `preempt` is the policy.
    """
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.side_effect = [_make_renew_ok(), _make_renew_failed()]

    notified = []
    cancelled = False

    async def run() -> None:
        nonlocal cancelled
        with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
            try:
                async with async_execution_lease(
                    "task:123",
                    worker_id="w-1",
                    on_lease_lost=lambda: notified.append(True),
                ):
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled = True

    await asyncio.wait_for(asyncio.create_task(run()), timeout=3.0)

    assert notified == [True], "the callback should still be invoked"
    assert cancelled, "supplying a callback must not disable preemption"


@pytest.mark.asyncio
async def test_preempt_false_leaves_the_caller_running() -> None:
    """`preempt=False` is the explicit opt-out, and it marks the handle lost."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.side_effect = [_make_renew_ok(), _make_renew_failed()]

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        async with async_execution_lease(
            "task:123", worker_id="w-1", preempt=False
        ) as lease:
            await asyncio.sleep(0.3)
            assert lease.is_lost, "the handle must still report the loss"
            with pytest.raises(LeaseLostError):
                lease.check()


def test_no_unchecked_warning_when_the_body_raises() -> None:
    """A failing body is the story; "you never checked" is noise over it."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.return_value = _make_renew_failed()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(ValueError, match="body error"):
                with execution_lease("task:123", worker_id="w-1"):
                    time.sleep(0.15)  # let the heartbeat fail
                    raise ValueError("body error")

            assert [x for x in w if issubclass(x.category, RuntimeWarning)] == []


# ---------------------------------------------------------------------------
# current_lease
# ---------------------------------------------------------------------------


def test_current_lease_inside_block() -> None:
    """Nested code reaches the fencing token without it being passed down."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        fencing_token=77
    )
    mock_client.renew_lease.return_value = _make_renew_ok()

    def deep_helper() -> int:
        return current_lease().fencing_token

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease("task:123", worker_id="w-1"):
            assert deep_helper() == 77


def test_current_lease_outside_block_raises() -> None:
    with pytest.raises(LookupError, match="No execution lease is active"):
        current_lease()


def test_current_lease_is_reset_on_exit() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
        with execution_lease("task:123", worker_id="w-1"):
            pass

    with pytest.raises(LookupError):
        current_lease()


# ---------------------------------------------------------------------------
# @task_lease
# ---------------------------------------------------------------------------


def test_task_lease_sync_runs_under_a_lease() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        fencing_token=7
    )
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease("task:static", worker_id="w-1")
        def process() -> int:
            return current_lease().fencing_token

        assert process() == 7

    assert mock_client.check_idempotency_cache.call_args[1]["idempotency_key"] == (
        "task:static"
    )
    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


def test_task_lease_derives_the_key_from_arguments() -> None:
    """A fixed key would let an entry point lock only one task, ever."""
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease(lambda task_id: f"task:{task_id}", worker_id="w-1")
        def process(task_id: str) -> str:
            return task_id

        assert process("abc") == "abc"
        assert process(task_id="xyz") == "xyz"

    keys = [
        c[1]["idempotency_key"]
        for c in mock_client.check_idempotency_cache.call_args_list
    ]
    assert keys == ["task:abc", "task:xyz"]


def test_task_lease_preserves_function_metadata() -> None:
    @task_lease("task:1", worker_id="w-1")
    def process(task_id: str) -> str:
        """Original docstring."""
        return task_id

    assert process.__name__ == "process"
    assert process.__doc__ == "Original docstring."


def test_task_lease_does_not_run_the_body_when_not_acquired() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_not_acquired_response(
        idempotency_pb2.CACHE_STATUS_IN_PROGRESS
    )

    ran = []

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease("task:1", worker_id="w-1")
        def process() -> None:
            ran.append(True)

        with pytest.raises(LeaseNotAcquired):
            process()

    assert ran == [], "the body must not run without the lease"
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_task_lease_async_runs_under_a_lease() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        fencing_token=9
    )
    mock_client.renew_lease.return_value = _make_renew_ok()

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease(lambda task_id: f"task:{task_id}", worker_id="w-1")
        async def process(task_id: str) -> int:
            await asyncio.sleep(0)
            return current_lease().fencing_token

        assert await process("abc") == 9

    assert mock_client.check_idempotency_cache.call_args[1]["idempotency_key"] == (
        "task:abc"
    )
    mock_client.release_lease.assert_called_once()
    mock_client.close.assert_called_once()


@pytest.mark.asyncio
async def test_task_lease_async_preempts_on_loss() -> None:
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response(
        heartbeat_interval_ms=50
    )
    mock_client.renew_lease.side_effect = [_make_renew_ok(), _make_renew_failed()]

    with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):

        @task_lease("task:1", worker_id="w-1")
        async def process() -> None:
            await asyncio.sleep(5)

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.create_task(process()), timeout=3.0)


@pytest.mark.asyncio
async def test_client_is_not_closed_until_the_release_completes() -> None:
    """Cleanup owns the close, so a re-cancelled caller cannot race it.

    Cleanup is shielded, so it survives cancellation — but a shielded await is
    itself interruptible. A *second* cancellation delivered while it is pending
    returns control to the canceller with the release still in flight, and an
    outer `finally` would then close the client underneath it. One cancel is not
    enough to show this, which is why this test sends two: the first at the body's
    await, the second once the shielded cleanup is running.

    Two cancellations is not a contrived shape — `asyncio.wait_for`, `TaskGroup`
    teardown, and a supervisor cancelling a task this module has already
    preempted all produce it.
    """
    calls = []
    mock_client = MagicMock()
    mock_client.check_idempotency_cache.return_value = _make_acquired_response()
    mock_client.renew_lease.return_value = _make_renew_ok()

    def _slow_release(**_kwargs: object) -> None:
        time.sleep(0.15)  # widen the window the old ordering lost
        calls.append("release")

    mock_client.release_lease.side_effect = _slow_release
    mock_client.close.side_effect = lambda: calls.append("close")

    async def run() -> None:
        with patch("cellaflow.execution.CellaflowClient", return_value=mock_client):
            async with async_execution_lease("task:123", worker_id="w-1"):
                await asyncio.sleep(5)

    task = asyncio.create_task(run())
    await asyncio.sleep(0.05)
    task.cancel()  # delivered at the body's await
    await asyncio.sleep(0)  # let it reach the shielded cleanup
    task.cancel()  # delivered at the shielded await
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Let the detached cleanup settle before judging the order.
    await asyncio.sleep(0.5)
    assert calls == ["release", "close"], f"wrong cleanup order: {calls}"
