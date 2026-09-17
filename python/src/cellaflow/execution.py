"""Distributed execution lease — mutual exclusion with liveness heartbeating.

This is distinct from ``@tool`` / ``@step``, which **commit** a result on
success (memoisation / idempotency). An execution lease **releases without
committing**: its job is to ensure only one worker runs a task at a time, and
to make a crashed worker's task reclaimable promptly via heartbeat expiry.

Typical usage::

    # Synchronous
    from cellaflow import execution_lease, LeaseNotAcquired, LeaseLostError

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
    import asyncio
    from cellaflow import async_execution_lease, LeaseNotAcquired

    try:
        async with async_execution_lease("task:123", worker_id="w-1") as lease:
            await do_work()
    except LeaseNotAcquired:
        print("Another live worker holds this task")
    except asyncio.CancelledError:
        print("Lease was lost mid-execution; aborted")

    # Decorator form, for an entry point that owns a whole task
    from cellaflow import task_lease, current_lease

    @task_lease(lambda task_id: f"task:{task_id}", worker_id="w-1")
    def process(task_id: str) -> None:
        write_row(task_id, fence=current_lease().fencing_token)

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
import contextvars
import functools
import inspect
import logging
import warnings
from typing import Any, AsyncIterator, Callable, Iterator, Optional, Union

from cellaflow.client import CellaflowClient
from cellaflow.lease import LeaseHeartbeat
from cellaflow.v1 import idempotency_pb2

logger = logging.getLogger(__name__)

# Sensible defaults matching the Skyvern benchmark integration:
# short enough that a crashed worker's task frees up promptly,
# long enough that a heartbeat every third of the TTL never races expiry.
_DEFAULT_TTL_MS = 15_000
_DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000

# The lease held by the innermost enclosing block, so code called from inside it
# can reach the fencing token without it being threaded through every signature.
# This is what makes `@task_lease` usable: a decorator has nowhere to yield a
# handle to.
_current_lease: contextvars.ContextVar[Optional["LeaseHandle"]] = (
    contextvars.ContextVar("cellaflow_current_lease", default=None)
)

KeySource = Union[str, Callable[..., str]]


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


def current_lease() -> LeaseHandle:
    """Returns the lease held by the enclosing block.

    Lets code inside an ``execution_lease`` block — or inside a ``@task_lease``
    function, which has no handle to yield — reach the fencing token to pass
    downstream.

    Raises
    ------
    LookupError
        If no execution lease is active on this context.
    """
    handle = _current_lease.get()
    if handle is None:
        raise LookupError(
            "No execution lease is active. current_lease() is only valid inside "
            "an execution_lease / async_execution_lease block or a @task_lease "
            "function."
        )
    return handle


def _acquire(
    client: CellaflowClient,
    key: str,
    worker_id: str,
    ttl_ms: int,
) -> idempotency_pb2.CheckCacheResponse:
    """Claims the lease, or raises LeaseNotAcquired explaining why not."""
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
    if resp.status == idempotency_pb2.CACHE_STATUS_HIT:
        raise LeaseNotAcquired(f"Lease {key!r} was already completed.")
    if resp.status != idempotency_pb2.CACHE_STATUS_ACQUIRED:
        raise LeaseNotAcquired(
            f"Lease {key!r} could not be acquired (status={resp.status})."
        )
    return resp


def _resolve_interval(resp: Any, requested_ms: int) -> int:
    """Picks the heartbeat interval, preferring the engine's advertised value."""
    advertised = resp.heartbeat_interval_ms
    if advertised and advertised != requested_ms:
        logger.debug(
            "Engine advertised a %dms heartbeat interval; using it instead of the "
            "requested %dms.",
            advertised,
            requested_ms,
        )
    return int(advertised or requested_ms)


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

    There is no preemption here, unlike the async form: a running thread cannot
    be interrupted safely from outside, so cooperative ``check()`` — or
    ``on_lease_lost`` — is the only sound mechanism.

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
        Lease time-to-live in milliseconds, applied to the initial acquisition
        and to every renewal. After a crash, the task becomes reclaimable once
        this elapses without a heartbeat. Defaults to 15 s.
    heartbeat_interval_ms:
        How often the background thread renews the lease. Must be well below
        ``ttl_ms``; the default (5 s) is one-third of the default TTL. If the
        engine advertises its own interval, that value is used instead.
    on_lease_lost:
        Optional callback invoked by the heartbeat thread if the lease is lost
        (renewal denied, or unreachable for a full ``ttl_ms``). Runs on the
        heartbeat thread, so it must be thread-safe.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        resp = _acquire(client, key, worker_id, ttl_ms)
        token = resp.fencing_token
        interval_ms = _resolve_interval(resp, heartbeat_interval_ms)
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
            lease_ttl_ms=ttl_ms,
        )
        hb.start_sync()
        var_token = _current_lease.set(handle)
        body_failed = False
        try:
            yield handle
        except BaseException:
            body_failed = True
            raise
        finally:
            _current_lease.reset(var_token)
            hb.stop_sync()

            # Warn only on an otherwise-clean exit. On a failing one the
            # exception is the story, and "you never called check()" is noise
            # layered over it.
            if handle.is_lost and not handle._checked_after_loss and not body_failed:
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
                    timeout=hb.rpc_timeout_sec,
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
    preempt: bool = True,
) -> AsyncIterator[LeaseHandle]:
    """Asynchronous distributed lock with liveness heartbeating.

    Identical to :func:`execution_lease` but designed for ``asyncio``
    environments. The heartbeat runs as an ``asyncio.Task`` rather than a
    daemon thread.

    If the lease is lost, the calling task is cancelled —
    ``asyncio.CancelledError`` is raised at the next ``await`` point,
    cooperatively aborting the work.

    Parameters
    ----------
    key, worker_id, target, secure, ttl_ms, heartbeat_interval_ms:
        Same as :func:`execution_lease`.
    on_lease_lost:
        Optional callback invoked when the lease is lost. This is a
        *notification*, not a replacement for preemption — supplying it does not
        stop the caller being cancelled. Use ``preempt=False`` for that.
    preempt:
        Whether losing the lease cancels the calling task. Defaults to ``True``.
        Set ``False`` to handle loss yourself via ``on_lease_lost`` or
        ``lease.check()``; the work then keeps running without a live lease, so
        anything irreversible it does afterwards is unprotected.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    asyncio.CancelledError
        If the lease is lost mid-execution and ``preempt`` is ``True``.
    """
    client = CellaflowClient(target=target, secure=secure)
    try:
        # Offload sync gRPC call to a thread
        resp = await asyncio.to_thread(_acquire, client, key, worker_id, ttl_ms)
    except BaseException:
        with contextlib.suppress(Exception):
            client.close()
        raise

    token = resp.fencing_token
    interval_ms = _resolve_interval(resp, heartbeat_interval_ms)
    handle = LeaseHandle(token)
    caller_task = asyncio.current_task()

    def _on_lost() -> None:
        handle.is_lost = True
        if on_lease_lost is not None:
            try:
                on_lease_lost()
            except Exception as e:
                logger.error("on_lease_lost callback raised an error: %s", e)
        if preempt and caller_task is not None:
            logger.warning(
                "Lease %s lost; cancelling calling task %s",
                key,
                caller_task.get_name(),
            )
            caller_task.cancel()

    hb = LeaseHeartbeat(
        client=client,
        agent_id=worker_id,
        idempotency_key=key,
        fencing_token=token,
        heartbeat_interval_ms=interval_ms,
        on_lease_lost=_on_lost,
        lease_ttl_ms=ttl_ms,
    )
    hb.start_async()
    var_token = _current_lease.set(handle)

    async def _cleanup() -> None:
        # Closing the client belongs here rather than in an outer `finally`.
        # Cleanup is shielded so it survives the caller's cancellation, but a
        # shielded await *returns* to the canceller immediately — so an outer
        # close would run while this release is still in flight. Owning the
        # close keeps the ordering intact on the cancellation path, which is
        # the path preemption makes routine.
        try:
            await hb.stop_async()
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    client.release_lease,
                    agent_id=worker_id,
                    idempotency_key=key,
                    fencing_token=token,
                    timeout=hb.rpc_timeout_sec,
                )
        finally:
            with contextlib.suppress(Exception):
                client.close()

    try:
        yield handle
    finally:
        _current_lease.reset(var_token)
        await asyncio.shield(_cleanup())


def task_lease(
    key: KeySource,
    *,
    worker_id: str,
    target: str = "localhost:50051",
    secure: bool = False,
    ttl_ms: int = _DEFAULT_TTL_MS,
    heartbeat_interval_ms: int = _DEFAULT_HEARTBEAT_INTERVAL_MS,
    on_lease_lost: Optional[Callable[[], None]] = None,
    preempt: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Runs the decorated function under an execution lease.

    Wraps coroutines with :func:`async_execution_lease` and plain functions with
    :func:`execution_lease`, chosen by ``inspect.iscoroutinefunction`` the same
    way ``@step`` picks its wrapper.

    ``key`` may be a literal string, or a callable receiving the decorated
    function's own arguments and returning the key. The callable form is the
    common one: an entry point locks *its* task, not one fixed task::

        @task_lease(lambda task_id: f"task:{task_id}", worker_id="w-1")
        def process(task_id: str) -> None:
            ...

    The decorated function has no handle yielded to it; call
    :func:`current_lease` inside the body to reach the fencing token.

    Parameters
    ----------
    key:
        The lease key, or a callable ``(*args, **kwargs) -> str`` building it
        from the call's arguments.
    worker_id, target, secure, ttl_ms, heartbeat_interval_ms, on_lease_lost:
        Same as :func:`execution_lease`.
    preempt:
        Async functions only; same as :func:`async_execution_lease`. Ignored for
        synchronous functions, which cannot be preempted.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired, raised in place of calling the function.
    """

    def _key_for(args: Any, kwargs: Any) -> str:
        if callable(key):
            return key(*args, **kwargs)
        return key

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async with async_execution_lease(
                    _key_for(args, kwargs),
                    worker_id=worker_id,
                    target=target,
                    secure=secure,
                    ttl_ms=ttl_ms,
                    heartbeat_interval_ms=heartbeat_interval_ms,
                    on_lease_lost=on_lease_lost,
                    preempt=preempt,
                ):
                    return await func(*args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            with execution_lease(
                _key_for(args, kwargs),
                worker_id=worker_id,
                target=target,
                secure=secure,
                ttl_ms=ttl_ms,
                heartbeat_interval_ms=heartbeat_interval_ms,
                on_lease_lost=on_lease_lost,
            ):
                return func(*args, **kwargs)

        return sync_wrapper

    return decorator
