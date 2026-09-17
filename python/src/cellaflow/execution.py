"""Distributed execution lease — mutual exclusion with liveness heartbeating.

This is distinct from ``@tool`` / ``@step``, which **commit** a result on
success (memoisation / idempotency). An execution lease **releases without
committing**: its job is to ensure only one worker runs a task at a time, and
to make a crashed worker's task reclaimable promptly via heartbeat expiry.

Typical usage::

    # Synchronous
    from cellaflow import execution_lease, LeaseNotAcquired

    try:
        with execution_lease("task:123", worker_id="w-1") as lease:
            do_work_item(1)
            lease.check()  # raises LeaseLostError if the lease was lost
            do_work_item(2)
    except LeaseNotAcquired:
        print("Another live worker holds this task")
    except LeaseLostError:
        print("Lease lost mid-execution")

    # Asynchronous — lease loss injects CancelledError into the caller
    from cellaflow import async_execution_lease

    try:
        async with async_execution_lease("task:123", worker_id="w-1") as lease:
            await do_work()
    except LeaseNotAcquired:
        print("Another live worker holds this task")
    except asyncio.CancelledError:
        print("Lease was lost mid-execution; aborted")

Key design decisions
--------------------
* The context manager **always creates and owns its own** ``CellaflowClient``.
  Accepting a caller-supplied client would introduce a lifecycle footgun: if
  the context manager closed it on exit it would break other callers; if it
  did not, it would silently leak the connection.

* The idempotency key must be an **explicit business identifier** supplied by
  the caller (e.g. ``"task:{task_id}"``). Hashing function arguments — what
  ``@tool`` does — is the wrong model here: an execution lease must identify
  the *same resource* across different workers, not distinguish between
  *different calls*.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import warnings
from typing import Callable, Iterator, Optional, AsyncIterator

from cellaflow.client import CellaflowClient
from cellaflow.lease import LeaseHeartbeat
from cellaflow.v1 import idempotency_pb2

logger = logging.getLogger(__name__)

# Sensible defaults matching the Skyvern benchmark integration:
# short enough that a crashed worker's task frees up promptly,
# long enough that a heartbeat every third of the TTL never races expiry.
_DEFAULT_TTL_MS = 15_000
_DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000


class LeaseNotAcquired(RuntimeError):
    """Raised when the requested execution lease cannot be acquired."""


class LeaseLostError(RuntimeError):
    """Raised by LeaseHandle.check() if the lease was lost."""


class LeaseHandle:
    """Handle to an active execution lease.
    
    Provides access to the fencing token and a check() method for cooperative
    cancellation in synchronous code.
    """
    
    def __init__(self, fencing_token: int) -> None:
        self.fencing_token = fencing_token
        self.is_lost = False
        self._checked_after_loss = False

    def check(self) -> None:
        """Checks if the lease is still held.
        
        Raises
        ------
        LeaseLostError
            If the lease has been lost (e.g., server denied renewal, or network
            partition).
        """
        if self.is_lost:
            self._checked_after_loss = True
            raise LeaseLostError("Execution lease was lost")


@contextlib.contextmanager
def execution_lease(
    key: str,
    *,
    worker_id: str,
    target: str = "localhost:50051",
    secure: bool = False,
    ttl_ms: int = _DEFAULT_TTL_MS,
    heartbeat_interval_ms: int = _DEFAULT_HEARTBEAT_INTERVAL_MS,
    on_lease_lost: Optional[Callable[[], None]] = None,
) -> Iterator[LeaseHandle]:
    """Synchronous distributed lock with liveness heartbeating.

    Acquires an exclusive lease on ``key`` for ``worker_id``. While the block
    is open, a daemon thread renews the lease every ``heartbeat_interval_ms``
    milliseconds. If the process dies, heartbeats stop, and the lease expires
    after ``ttl_ms`` milliseconds — making the task reclaimable by another
    worker without any manual cleanup.

    Yields a ``LeaseHandle``. In synchronous code, you should periodically
    call ``lease.check()`` to cooperatively abort if the lease is lost. If the
    lease is lost and the block exits without ever calling ``check()``, a
    ``RuntimeWarning`` is emitted.

    Parameters
    ----------
    key:
        Idempotency key that identifies the resource being locked.  Must be a
        stable, human-readable business identifier, e.g. ``"task:abc-123"``.
    worker_id:
        Identifies this worker. Used as ``agent_id`` in the engine API.
    target:
        CellaFlow engine address. Defaults to ``localhost:50051``.
    secure:
        Whether to use TLS. Defaults to ``False``.
    ttl_ms:
        Lease time-to-live in milliseconds. After a crash, the task becomes
        reclaimable once this elapses without a heartbeat. Defaults to 15 s.
    heartbeat_interval_ms:
        How often the background thread renews the lease. Must be well below
        ``ttl_ms``; the default (5 s) is one-third of the default TTL.
    on_lease_lost:
        Optional callback invoked by the heartbeat thread if the lease cannot
        be renewed (server denied renewal or network partition).

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        resp = client.check_idempotency_cache(
            agent_id=worker_id,
            idempotency_key=key,
            lease_ttl_ms=ttl_ms,
        )
        if resp.status == idempotency_pb2.CACHE_STATUS_IN_PROGRESS:
            raise LeaseNotAcquired(
                f"Lease {key!r} is held by a live worker. Another worker is "
                f"actively heartbeating this task."
            )
        elif resp.status == idempotency_pb2.CACHE_STATUS_HIT:
            raise LeaseNotAcquired(f"Lease {key!r} was already completed.")
        elif resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
            raise LeaseNotAcquired(f"Lease {key!r} could not be acquired (status={resp.status}).")

        token = resp.fencing_token
        interval_ms = resp.heartbeat_interval_ms or heartbeat_interval_ms
        handle = LeaseHandle(token)

        def _on_lost() -> None:
            handle.is_lost = True
            if on_lease_lost is not None:
                on_lease_lost()

        hb = LeaseHeartbeat(
            client=client,
            agent_id=worker_id,
            idempotency_key=key,
            fencing_token=token,
            heartbeat_interval_ms=interval_ms,
            on_lease_lost=_on_lost,
            max_network_errors=1, # Fail fast for execution leases
        )
        hb.start_sync()
        try:
            yield handle
        finally:
            hb.stop_sync()
            
            # If the lease was lost and the user never checked it, warn them.
            if handle.is_lost and not handle._checked_after_loss:
                warnings.warn(
                    f"Execution lease {key!r} was lost during execution, but "
                    f"lease.check() was never called. The block ran to completion "
                    f"without a live lease.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                
            with contextlib.suppress(Exception):
                client.release_lease(
                    agent_id=worker_id,
                    idempotency_key=key,
                    fencing_token=token,
                )
    finally:
        with contextlib.suppress(Exception):
            client.close()


@contextlib.asynccontextmanager
async def async_execution_lease(
    key: str,
    *,
    worker_id: str,
    target: str = "localhost:50051",
    secure: bool = False,
    ttl_ms: int = _DEFAULT_TTL_MS,
    heartbeat_interval_ms: int = _DEFAULT_HEARTBEAT_INTERVAL_MS,
    on_lease_lost: Optional[Callable[[], None]] = None,
) -> AsyncIterator[LeaseHandle]:  # type: ignore[misc]
    """Asynchronous distributed lock with liveness heartbeating.

    Identical to :func:`execution_lease` but designed for ``asyncio``
    environments. The heartbeat runs as an ``asyncio.Task`` rather than a
    daemon thread.

    If the lease is lost (renewal denied or network error), the calling task
    is cancelled by default — ``asyncio.CancelledError`` is injected at the next
    ``await`` point, cooperatively aborting the work. Supply ``on_lease_lost``
    to override this behaviour.

    Parameters
    ----------
    key, worker_id, target, secure, ttl_ms, heartbeat_interval_ms:
        Same as :func:`execution_lease`.
    on_lease_lost:
        Callback invoked when the lease is lost. Defaults to cancelling the
        current ``asyncio.Task``, which raises ``CancelledError`` in the block
        body. Override to suppress cancellation or take a different action.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    asyncio.CancelledError
        If the lease is lost mid-execution (default ``on_lease_lost`` behaviour).
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        # Offload sync gRPC call to a thread
        resp = await asyncio.to_thread(
            client.check_idempotency_cache,
            agent_id=worker_id,
            idempotency_key=key,
            lease_ttl_ms=ttl_ms,
        )
        if resp.status == idempotency_pb2.CACHE_STATUS_IN_PROGRESS:
            raise LeaseNotAcquired(
                f"Lease {key!r} is held by a live worker. Another worker is "
                f"actively heartbeating this task."
            )
        elif resp.status == idempotency_pb2.CACHE_STATUS_HIT:
            raise LeaseNotAcquired(f"Lease {key!r} was already completed.")
        elif resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
            raise LeaseNotAcquired(f"Lease {key!r} could not be acquired (status={resp.status}).")

        token = resp.fencing_token
        interval_ms = resp.heartbeat_interval_ms or heartbeat_interval_ms
        handle = LeaseHandle(token)

        caller_task = asyncio.current_task()
        effective_on_lease_lost = on_lease_lost
        
        if effective_on_lease_lost is None and caller_task is not None:
            def _cancel_caller() -> None:
                logger.warning(
                    "Lease %s lost; cancelling calling task %s",
                    key,
                    caller_task.get_name(),
                )
                caller_task.cancel()
            effective_on_lease_lost = _cancel_caller

        def _on_lost() -> None:
            handle.is_lost = True
            if effective_on_lease_lost is not None:
                effective_on_lease_lost()

        hb = LeaseHeartbeat(
            client=client,
            agent_id=worker_id,
            idempotency_key=key,
            fencing_token=token,
            heartbeat_interval_ms=interval_ms,
            on_lease_lost=_on_lost,
            max_network_errors=1, # Fail fast for execution leases
        )
        hb.start_async()
        try:
            yield handle
        finally:
            # We use shield so that if the caller task is cancelled (e.g. by our 
            # own _cancel_caller or external), the cleanup still runs properly.
            async def _cleanup() -> None:
                await hb.stop_async()
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(
                        client.release_lease,
                        agent_id=worker_id,
                        idempotency_key=key,
                        fencing_token=token,
                    )
            await asyncio.shield(_cleanup())
    finally:
        with contextlib.suppress(Exception):
            client.close()
