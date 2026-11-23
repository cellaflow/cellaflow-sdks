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
import dataclasses
import functools
import hashlib
import inspect
import logging
import re
import threading
import time
import warnings
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Union

from cellaflow.client import CellaflowClient, _check_api_key_argument
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


@dataclasses.dataclass(frozen=True)
class LeaseHolderInfo:
    """Who holds a lease, as the engine last read it: recorded when the lease
    was granted, with the expiry renewals have since moved it to.

    Carried on :class:`LeaseNotAcquired` when another worker holds the lease.
    Its fields are stored in recorded step failures, so a field added later
    needs a default, or a resumed run cannot read records already written.

    Attributes
    ----------
    worker_id:
        The worker that acquired the lease, by the ``worker_id`` it acquired
        it with. That is what the worker said it was, not an authenticated
        identity. ``None`` when the engine did not record one, or the worker
        sent an empty one.
    holder_id:
        The engine's id for this grant of the lease, kept through its renewals;
        a reclaim is a new grant with a new id. It identifies the grant, not
        the worker (that is ``worker_id``).
    acquired_at_ms:
        When the lease was acquired, epoch ms on the engine's clock. ``None``
        when not recorded.
    expires_at_ms:
        When the lease lapses unless renewed, epoch ms on the engine's clock.
    hard_expires_at_ms:
        The lease's ceiling, independent of renewals, epoch ms on the engine's
        clock. ``None`` when it has none.
    """

    worker_id: Optional[str]
    holder_id: str
    acquired_at_ms: Optional[int]
    expires_at_ms: int
    hard_expires_at_ms: Optional[int]

    @classmethod
    def _from_proto(cls, holder: idempotency_pb2.LeaseHolder) -> "LeaseHolderInfo":
        # Absent rather than empty or 0, so a caller never mistakes "not
        # recorded" for a worker named "" or a lease granted at the epoch.
        return cls(
            worker_id=holder.agent_id or None,
            holder_id=holder.holder_id,
            acquired_at_ms=holder.acquired_at_ms or None,
            expires_at_ms=holder.expires_at_ms,
            hard_expires_at_ms=holder.hard_expires_at_ms or None,
        )


class LeaseNotAcquired(RuntimeError):
    """Raised when the requested execution lease cannot be acquired.

    Attributes
    ----------
    holder:
        Who holds the lease. Set only when it was refused because another
        worker holds it and the engine said who; ``None`` otherwise, including
        for a key that is already completed and for an engine too old to
        report it.
    """

    # The holder as plain fields rather than the dataclass. When a step raises
    # this refusal and the workflow catches it, the step's failure record keeps
    # only plain data, so this is what a resumed run reads the holder back
    # from. Records store it under this name: renaming it, or giving
    # LeaseHolderInfo a field without a default, breaks reading records
    # already written. The class default covers a record that has none.
    _holder: Optional[Dict[str, Any]] = None

    def __init__(self, *args: Any, holder: Optional[LeaseHolderInfo] = None) -> None:
        super().__init__(*args)
        self.holder = holder

    @property
    def holder(self) -> Optional[LeaseHolderInfo]:
        """Who holds the lease, or ``None``; see the class docstring."""
        stored = self._holder
        if stored is None:
            return None
        # Fields a later SDK version recorded and this one does not know are
        # dropped rather than refused.
        known = {f.name for f in dataclasses.fields(LeaseHolderInfo)}
        return LeaseHolderInfo(**{k: v for k, v in stored.items() if k in known})

    @holder.setter
    def holder(self, value: Optional[LeaseHolderInfo]) -> None:
        self._holder = dataclasses.asdict(value) if value is not None else None


_NO_RESULT: Any = object()
_NO_PART: Any = object()

#: A header name (an RFC 9110 token), and a header value this SDK will send
#: as it is: printable ASCII, with no space at either end.
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_HEADER_VALUE = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]*[\x21-\x7e])?")

#: How often `cancel_on_loss` asks whether the lease has passed its deadline.
_CANCEL_POLL_S = 0.05


class LeaseLostError(RuntimeError):
    """Raised by LeaseHandle.check() if the lease was lost.

    Raised by a `@tool` / `@step` call when the engine shows another worker
    holds it, too. `result` is then what the body returned, kept for logging:
    the engine did not record it. It is `None` when the body returned nothing or
    the loss was found another way; `has_result` tells the two apart. Neither is
    kept in the session's history, so a resumed run that replays this failure
    raises it with `has_result` false.
    """

    #: Not kept in the session's history; see `failures.capture`.
    _cellaflow_not_recorded = ("result", "has_result")
    result: Any = None
    has_result: bool = False

    def __init__(self, *args: Any, result: Any = _NO_RESULT) -> None:
        super().__init__(*args)
        self.has_result = result is not _NO_RESULT
        self.result = None if result is _NO_RESULT else result


class LeaseHandle:
    """Handle to an active execution lease.

    Provides access to the fencing token, the idempotency key the lease is
    held under (`key`, `key_digest`, `key_digest_for()`, `idempotency_headers()`),
    a check() method
    for cooperative cancellation in synchronous code, and `cancel_on_loss()`
    for asyncio code.

    Loss is judged on the clock whenever it is asked about, when the handle has
    a `probe`: a holder resumed past its lease's deadline (a stopped process, a
    suspended host) finds it lost before any renewal has to answer.
    """

    def __init__(
        self,
        fencing_token: int,
        probe: Optional[Callable[[], bool]] = None,
        key: str = "",
    ) -> None:
        self.fencing_token = fencing_token
        self._key = key
        self._lost = False
        self._released = False
        self._checked_after_loss = False
        self._probe = probe
        self._listeners: List[Callable[[], None]] = []
        self._lock = threading.Lock()

    @property
    def key(self) -> str:
        """The idempotency key the lease is held under: the key a `@tool` /
        `@step` call derived (or was given), or the `execution_lease` key.

        A step call's key is the same on every attempt at the same call, as long
        as its arguments are the same, so it can be forwarded to a destination
        that deduplicates on it; see `idempotency_headers`. Under
        `SCOPE_STEP_LOCAL` the key includes the call's position, which calling
        the step again after a failure, or a resumed `durable_tools` thread,
        can change, so prefer the session-wide or shared scopes, or an explicit
        key, for forwarding.

        An `execution_lease` key names a lock, which is released when the block
        ends and taken again by the next run: forward it only when it names
        exactly one operation (a task id), or add a run id of your own.

        A derived key carries the ids it is made of (the session or thread id,
        or the coordination id, the agent id and the step's name, by scope), so
        `idempotency_headers` sends `key_digest` unless asked for the key.
        """
        return self._key

    @property
    def key_digest(self) -> str:
        """SHA-256 of the UTF-8 `key`, as 64 lowercase hex characters: the same
        on every attempt, like the key, ASCII and within the length destinations
        allow (Stripe takes up to 255), and without the ids the key is made of
        in readable form. It is not a secret: anyone who can guess the inputs of
        a key can compute its digest."""
        return hashlib.sha256(self._require_key().encode("utf-8")).hexdigest()

    def key_digest_for(self, part: str) -> str:
        """A digest for one of several requests a body sends under this lease:
        the SHA-256 of `key_digest` followed by `part` (UTF-8), as 64 lowercase
        hex characters, the same on every attempt, and different for each
        `part`, which must be a non-empty string of whole characters (no
        unpaired surrogates). A destination that
        deduplicates refuses (Stripe) or answers from its first request
        (others) a key reused for a different request, so each request needs
        its own::

            customer = lease.key_digest_for("customer")
            payment = lease.key_digest_for("payment")
            stripe.Customer.create(..., idempotency_key=customer)
            stripe.PaymentIntent.create(..., idempotency_key=payment)

        The same value as the TypeScript SDK's `keyDigestFor(part)`.
        """
        if not isinstance(part, str):
            raise TypeError(
                "key_digest_for: part must be a non-empty string, so requests "
                "without one do not all share the call's key."
            )
        if not part:
            raise ValueError(
                "key_digest_for: part must be a non-empty string, so requests "
                "without one do not all share the call's key."
            )
        try:
            encoded = (self.key_digest + part).encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError(
                "key_digest_for: part must be a string of whole characters."
            ) from None
        # The digest is fixed-length, so no key and part can run into another's.
        return hashlib.sha256(encoded).hexdigest()

    def idempotency_headers(
        self,
        *,
        part: str = _NO_PART,
        raw: bool = False,
        header: str = "Idempotency-Key",
    ) -> Dict[str, str]:
        """Returns `{header: key_digest}` to merge into a request's headers, so
        a destination that deduplicates on it applies a request repeated after
        a crash once::

            httpx.post(url, json=body, headers=current_lease().idempotency_headers())

        `part` names one of several requests the body sends, and the header then
        carries `key_digest_for(part)`. `raw=True` sends the key itself, which
        must then be printable ASCII with no space at either end to go in a
        header, and cannot be combined with `part`. `header` names another
        header (PayPal: `PayPal-Request-Id`).

        Merging into a plain dict is case-sensitive: drop any other spelling of
        the header from your own headers first, or the request carries both.
        """
        if not _HEADER_NAME.fullmatch(header):
            raise ValueError(f"{header!r} is not a valid header name.")
        # `part=None` given explicitly is refused, not taken for the call's own
        # key: requests without a part would all share it.
        if part is not _NO_PART:
            if raw:
                raise TypeError(
                    "part sends a digest, so it cannot be combined with raw."
                )
            return {header: self.key_digest_for(part)}
        if not raw:
            return {header: self.key_digest}
        key = self._require_key()
        if not _HEADER_VALUE.fullmatch(key):
            raise ValueError(
                "This lease's key contains characters a header cannot carry as "
                "they are. Send its digest instead (the default)."
            )
        return {header: key}

    def _require_key(self) -> str:
        if not self._key:
            raise ValueError(
                "This lease has no idempotency key to send: a handle made "
                "without one would make unrelated operations look like repeats."
            )
        return self._key

    @property
    def is_lost(self) -> bool:
        """True once the lease is lost, or released because its call returned."""
        if self._released or self._lost:
            return True
        # Its own answer counts: another thread may be part-way through
        # declaring the loss, and must not let a check pass meanwhile.
        if self._probe is not None and self._probe():
            return True
        return self._lost

    @is_lost.setter
    def is_lost(self, value: bool) -> None:
        if not value:
            self._lost = False
            return
        with self._lock:
            if self._lost:
                return
            self._lost = True
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener()
            except Exception:
                logger.exception("A lease-loss listener raised")

    def _mark_released(self) -> None:
        """The call or block is over: `is_lost` reads true from now on, without
        counting as a loss, so loss listeners are not called. A
        `cancel_on_loss()` block still open in work the call left running is
        cancelled at its next poll, with `LeaseLostError` saying the lease was
        released: that work no longer holds it."""
        self._released = True

    def check(self) -> None:
        """Checks if the lease is still held.

        Call this before anything irreversible. It judges by the clock, not only
        by the last heartbeat, so a process that was stopped past its deadline
        fails here before any renewal answers.

        Raises
        ------
        LeaseLostError
            If the lease has been lost (e.g., server denied renewal, or network
            partition), can no longer be assumed held, or was released because
            its call returned.
        """
        if self.is_lost:
            self._checked_after_loss = True
            if self._released and not self._lost:
                raise LeaseLostError(
                    "Execution lease was released when its call or block ended"
                )
            raise LeaseLostError("Execution lease was lost")

    @contextlib.asynccontextmanager
    async def cancel_on_loss(self) -> AsyncIterator[None]:
        """Cancels the awaits inside the block if the lease is lost meanwhile,
        raising `LeaseLostError` in place of the cancellation::

            lease = current_lease()
            lease.check()
            async with lease.cancel_on_loss():
                await payments.refund(order, amount)

        The asyncio counterpart of handing TypeScript's `signal` to a request.
        The lease is also judged on the clock while the block runs, so it is
        cancelled once the deadline passes even if no renewal has answered. It
        cancels on this side only: a request that has already left can still
        arrive. Checks the lease on entry. A cancellation from elsewhere passes
        through as `CancelledError`; on Python 3.10, which cannot count
        cancellations, one that arrives in the same moment as the loss is
        raised as `LeaseLostError` instead.
        """
        self.check()
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("cancel_on_loss() needs a running asyncio task")
        loop = asyncio.get_running_loop()
        cancelling = getattr(task, "cancelling", None)
        entry_count = cancelling() if cancelling is not None else 0
        state = {"active": True, "fired": False, "released": False}

        def cancel() -> None:
            # On the loop, so it cannot race the block's exit below.
            if state["active"] and not state["fired"]:
                state["fired"] = True
                assert task is not None
                task.cancel()

        def on_lost() -> None:
            # Called from the heartbeat, which may be on another thread.
            try:
                loop.call_soon_threadsafe(cancel)
            except RuntimeError:
                pass  # the loop is closed: nothing left to cancel

        timer: List[Optional[asyncio.TimerHandle]] = [None]

        def tick() -> None:
            # Asking judges the lease on the clock, and a loss found that way
            # reaches `on_lost` like any other.
            if not state["active"]:
                return
            if self.is_lost:
                state["released"] = self._released and not self._lost
                cancel()
                return
            timer[0] = loop.call_later(_CANCEL_POLL_S, tick)

        with self._lock:
            self._listeners.append(on_lost)
        try:
            # Lost between the check above and registering: nothing was
            # awaited yet, so raise rather than cancel.
            if self._lost:
                self._checked_after_loss = True
                raise LeaseLostError("Execution lease was lost")
            timer[0] = loop.call_later(_CANCEL_POLL_S, tick)
            yield
        except asyncio.CancelledError:
            if not state["fired"]:
                raise
            self._consume_cancel(task, entry_count)
            self._checked_after_loss = True
            if state["released"]:
                raise LeaseLostError(
                    "Execution lease was released when its call or block ended; "
                    "the awaits inside cancel_on_loss() were cancelled"
                ) from None
            raise LeaseLostError(
                "Execution lease was lost; the awaits inside cancel_on_loss() "
                "were cancelled"
            ) from None
        else:
            if state["fired"]:
                # Something inside swallowed the cancellation. The lease is
                # still lost, and the block must not end as if it were not.
                self._consume_cancel(task, entry_count)
                self._checked_after_loss = True
                raise LeaseLostError(
                    "Execution lease was lost while cancel_on_loss() was open"
                )
        finally:
            state["active"] = False
            if timer[0] is not None:
                timer[0].cancel()
            with self._lock:
                if on_lost in self._listeners:
                    self._listeners.remove(on_lost)

    @staticmethod
    def _consume_cancel(task: "asyncio.Task[Any]", entry_count: int) -> None:
        """Takes back the one cancellation `cancel_on_loss` made. If another
        is still outstanding (a cancellation from elsewhere arrived too), it
        passes through as `CancelledError`, as `asyncio.timeout` does."""
        uncancel = getattr(task, "uncancel", None)
        if uncancel is None:
            return
        if uncancel() > entry_count:
            raise asyncio.CancelledError()


def current_lease() -> LeaseHandle:
    """Returns the lease held by the enclosing block.

    Lets code inside an ``execution_lease`` block, a ``@task_lease`` function,
    or the body of a ``@tool`` / ``@step`` call that holds its lease, reach the
    lease to check it, cancel on its loss, or pass its fencing token downstream.

    Raises
    ------
    LookupError
        If no execution lease is active on this context.
    """
    handle = _current_lease.get()
    if handle is None:
        raise LookupError(
            "No execution lease is active. current_lease() is only valid inside "
            "an execution_lease / async_execution_lease block, a @task_lease "
            "function, or the body of a @tool / @step call that holds its lease."
        )
    return handle


def _holder_of(resp: Any) -> Optional[LeaseHolderInfo]:
    """The holder an IN_PROGRESS answer names, if the engine sent one.

    A stand-in response names none unless it carries a real ``LeaseHolder``
    message: a plain object, a ``MagicMock`` or an autospec of
    ``CheckCacheResponse`` must still give ``LeaseNotAcquired``, as it did
    before the holder existed, never an error from reading a field it lacks.
    """
    try:
        if not resp.HasField("current_holder"):
            return None
        holder = resp.current_holder
    except Exception:
        return None
    if not isinstance(holder, idempotency_pb2.LeaseHolder):
        return None
    return LeaseHolderInfo._from_proto(holder)


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
            f"actively heartbeating this task.",
            holder=_holder_of(resp),
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
    api_key: Optional[str] = None,
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
        Identifies this worker. Used as ``agent_id`` in the engine API. Shown
        to any worker refused this lease (``LeaseNotAcquired.holder``), so
        keep it free of anything private.
    target:
        CellaFlow engine address. Defaults to ``localhost:50051``.
    secure:
        Whether to use TLS. Defaults to ``False``.
    api_key:
        Authenticates with the engine. When omitted it is read from
        ``CELLAFLOW_API_KEY``. See :class:`~cellaflow.client.CellaflowClient`.
    ttl_ms:
        Lease time-to-live in milliseconds, applied to the initial acquisition
        and to every renewal. After a crash, the task becomes reclaimable once
        this elapses without a heartbeat. Defaults to 15 s.
    heartbeat_interval_ms:
        How often the background thread renews the lease. Must be well below
        ``ttl_ms``; the default (5 s) is one-third of the default TTL. If the
        engine advertises its own interval, that value is used instead.
    on_lease_lost:
        Optional callback invoked once if the lease is lost (renewal denied,
        or its deadline passed without a confirmed renewal). Runs on the
        heartbeat thread, or on whichever thread's ``check()`` found the loss,
        so it must be thread-safe.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    """
    client = CellaflowClient(target=target, secure=secure, api_key=api_key)
    try:
        sent = (time.monotonic(), time.time())
        resp = _acquire(client, key, worker_id, ttl_ms)
        token = resp.fencing_token
        interval_ms = _resolve_interval(resp, heartbeat_interval_ms)
        handle = LeaseHandle(token, probe=lambda: hb.past_deadline(), key=key)

        def _on_lost() -> None:
            # Reported after the block ended: the lease was let go, not lost.
            if handle._released:
                return
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
            acquired_sent_at=sent,
            acquired_expires_at_ms=resp.lease_expires_at_ms,
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
            # Before stopping: work the block left running finds the lease
            # over from here on, rather than lost when its deadline passes.
            lost_in_block = handle._lost
            handle._mark_released()
            hb.stop_sync()

            # Warn only on an otherwise-clean exit. On a failing one the
            # exception is the story, and "you never called check()" is noise
            # layered over it.
            if lost_in_block and not handle._checked_after_loss and not body_failed:
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
    api_key: Optional[str] = None,
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
    key, worker_id, target, secure, api_key, ttl_ms, heartbeat_interval_ms:
        Same as :func:`execution_lease`.
    on_lease_lost:
        Optional callback invoked when the lease is lost. This is a
        *notification*, not a replacement for preemption — supplying it does not
        stop the caller being cancelled. Use ``preempt=False`` for that.
    preempt:
        Whether losing the lease cancels the calling task. Defaults to ``True``.
        That includes a loss found by the caller's own ``lease.check()``: the
        check raises ``LeaseLostError``, and the block then ends in
        ``CancelledError``. Set ``False`` to handle loss yourself via
        ``on_lease_lost`` or ``lease.check()``; the work then keeps running
        without a live lease, so anything irreversible it does afterwards is
        unprotected.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired.
    asyncio.CancelledError
        If the lease is lost mid-execution and ``preempt`` is ``True``.
    """
    client = CellaflowClient(target=target, secure=secure, api_key=api_key)
    sent = (time.monotonic(), time.time())
    try:
        # Offload sync gRPC call to a thread
        resp = await asyncio.to_thread(_acquire, client, key, worker_id, ttl_ms)
    except BaseException:
        with contextlib.suppress(Exception):
            client.close()
        raise

    token = resp.fencing_token
    interval_ms = _resolve_interval(resp, heartbeat_interval_ms)
    handle = LeaseHandle(token, probe=lambda: hb.past_deadline(), key=key)
    caller_task = asyncio.current_task()
    caller_loop = asyncio.get_running_loop()

    def _on_lost() -> None:
        # Reported after the block ended: the lease was let go, not lost, and
        # the caller has moved on to other work.
        if handle._released:
            return
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
            # A check() from a worker thread (asyncio.to_thread) can be what
            # finds the loss: cancel on the loop, never from that thread.
            try:
                on_loop = asyncio.get_running_loop() is caller_loop
            except RuntimeError:
                on_loop = False
            if on_loop:
                caller_task.cancel()
            else:
                with contextlib.suppress(RuntimeError):
                    caller_loop.call_soon_threadsafe(caller_task.cancel)

    hb = LeaseHeartbeat(
        client=client,
        agent_id=worker_id,
        idempotency_key=key,
        fencing_token=token,
        heartbeat_interval_ms=interval_ms,
        on_lease_lost=_on_lost,
        lease_ttl_ms=ttl_ms,
        acquired_sent_at=sent,
        acquired_expires_at_ms=resp.lease_expires_at_ms,
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
        handle._mark_released()
        await asyncio.shield(_cleanup())


def task_lease(
    key: KeySource,
    *,
    worker_id: str,
    target: str = "localhost:50051",
    secure: bool = False,
    api_key: Optional[str] = None,
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
    worker_id, target, secure, api_key, ttl_ms, heartbeat_interval_ms,
    on_lease_lost:
        Same as :func:`execution_lease`. An ``api_key`` that cannot be sent as
        a header is refused here, when the decorator is applied.
    preempt:
        Async functions only; same as :func:`async_execution_lease`. Ignored for
        synchronous functions, which cannot be preempted.

    Raises
    ------
    LeaseNotAcquired
        If the lease cannot be acquired, raised in place of calling the function.
    """

    _check_api_key_argument(api_key)

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
                    api_key=api_key,
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
                api_key=api_key,
                ttl_ms=ttl_ms,
                heartbeat_interval_ms=heartbeat_interval_ms,
                on_lease_lost=on_lease_lost,
            ):
                return func(*args, **kwargs)

        return sync_wrapper

    return decorator
