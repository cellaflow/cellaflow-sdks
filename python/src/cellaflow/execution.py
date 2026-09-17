"""Distributed execution lease — mutual exclusion with liveness heartbeating.

This is distinct from ``@tool`` / ``@step``, which **commit** a result on
success (memoisation / idempotency). An execution lease **releases without
committing**: its job is to ensure only one worker runs a task at a time, and
to make a crashed worker's task reclaimable promptly via heartbeat expiry.

Typical usage::

    # Synchronous
    from cellaflow import execution_lease, LeaseNotAcquired

    try:
        with execution_lease("task:123", worker_id="w-1") as token:
            do_work()           # only one worker enters this block at a time
    except LeaseNotAcquired:
        print("Another live worker holds this task")

    # Asynchronous — lease loss injects CancelledError into the caller
    from cellaflow import async_execution_lease

    try:
        async with async_execution_lease("task:123", worker_id="w-1") as token:
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
from typing import Callable, Iterator, Optional

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
    """Raised when another live worker holds the requested execution lease.

    A status column in a database reports a held lease and a dead one
    identically; a heartbeated lease distinguishes them. If you see this
    exception the engine confirmed that a holder is actively renewing —
    it is not a stale record from a crashed process.
    """


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
) -> Iterator[int]:
    """Synchronous distributed lock with liveness heartbeating.

    Acquires an exclusive lease on ``key`` for ``worker_id``. While the block
    is open, a daemon thread renews the lease every ``heartbeat_interval_ms``
    milliseconds. If the process dies, heartbeats stop, and the lease expires
    after ``ttl_ms`` milliseconds — making the task reclaimable by another
    worker without any manual cleanup.

    Yields the fencing token, which can be passed downstream (e.g. to a
    database write) to guard against stale writes from fenced-out workers.

    Parameters
    ----------
    key:
        Idempotency key that identifies the resource being locked.  Must be a
        stable, human-readable business identifier, e.g. ``"task:abc-123"``.
        Do **not** hash function arguments here — the whole point is that
        different workers converge on the same key for the same resource.
    worker_id:
        Identifies this worker. Used as ``agent_id`` in the engine API.
        A process-scoped value (e.g. ``f"worker-{os.getpid()}"``) is typical.
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
        be renewed (server denied renewal or three consecutive network errors).
        Use this to signal the main thread to abort, e.g. by setting an
        ``threading.Event``. If ``None``, a warning is logged and the heartbeat
        thread exits silently — the main thread continues until it finishes,
        at which point the release will fail.

    Raises
    ------
    LeaseNotAcquired
        If the engine returns any status other than ``CACHE_STATUS_ACQUIRED``
        (i.e. another live worker holds the lease).
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        resp = client.check_idempotency_cache(
            agent_id=worker_id,
            idempotency_key=key,
            lease_ttl_ms=ttl_ms,
        )
        if resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
            raise LeaseNotAcquired(
                f"Lease {key!r} is held by a live worker "
                f"(status={resp.status}). Another worker is actively "
                f"heartbeating this task."
            )

        token = resp.fencing_token
        interval_ms = resp.heartbeat_interval_ms or heartbeat_interval_ms

        hb = LeaseHeartbeat(
            client=client,
            agent_id=worker_id,
            idempotency_key=key,
            fencing_token=token,
            heartbeat_interval_ms=interval_ms,
            on_lease_lost=on_lease_lost,
        )
        hb.start_sync()
        try:
            yield token
        finally:
            hb.stop_sync()
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
) -> Iterator[int]:  # type: ignore[misc]
    """Asynchronous distributed lock with liveness heartbeating.

    Identical to :func:`execution_lease` but designed for ``asyncio``
    environments. The heartbeat runs as an ``asyncio.Task`` rather than a
    daemon thread.

    If the lease is lost (renewal denied or three consecutive network errors),
    the calling task is cancelled by default — ``asyncio.CancelledError`` is
    injected at the next ``await`` point, aborting the work cleanly. Supply
    ``on_lease_lost`` to override this behaviour.

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
        If another live worker holds the lease.
    asyncio.CancelledError
        If the lease is lost mid-execution (default ``on_lease_lost``
        behaviour).
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        resp = client.check_idempotency_cache(
            agent_id=worker_id,
            idempotency_key=key,
            lease_ttl_ms=ttl_ms,
        )
        if resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
            raise LeaseNotAcquired(
                f"Lease {key!r} is held by a live worker "
                f"(status={resp.status}). Another worker is actively "
                f"heartbeating this task."
            )

        token = resp.fencing_token
        interval_ms = resp.heartbeat_interval_ms or heartbeat_interval_ms

        # Default: cancel the calling task so the body gets CancelledError.
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

        hb = LeaseHeartbeat(
            client=client,
            agent_id=worker_id,
            idempotency_key=key,
            fencing_token=token,
            heartbeat_interval_ms=interval_ms,
            on_lease_lost=effective_on_lease_lost,
        )
        hb.start_async()
        try:
            yield token
        finally:
            await hb.stop_async()
            with contextlib.suppress(Exception):
                client.release_lease(
                    agent_id=worker_id,
                    idempotency_key=key,
                    fencing_token=token,
                )
    finally:
        with contextlib.suppress(Exception):
            client.close()
