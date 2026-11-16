import asyncio
import contextlib
import threading
import logging
import time
from typing import Callable, List, Optional, Any, Tuple
from cellaflow.client import CellaflowClient

logger = logging.getLogger(__name__)

#: The shortest lease TTL a step or tool may ask for, in milliseconds. Shorter
#: leases leave too little time between renewals for a holder that is merely
#: slow, rather than gone, to keep its lease.
MIN_LEASE_TTL_MS = 1000

#: The longest renewal interval the heartbeat waits, in milliseconds: 2^31 - 1,
#: as in the TypeScript SDK. A longer wait gains nothing, since the engine's
#: maximum lease lifetime ends the lease long before, and a wait past what
#: `threading.Event.wait` accepts would stop the heartbeat with an error.
_MAX_INTERVAL_MS = 2**31 - 1

#: The longest TTL the SDK asks for, in milliseconds: 2^53 - 1, the largest
#: TypeScript can send exactly, so both SDKs accept the same values. The engine's
#: maximum lease lifetime caps far below it.
MAX_LEASE_TTL_MS = 2**53 - 1

#: The longest a renewal may take before it is abandoned, in seconds. A renewal
#: that has not answered by then is not coming back, and a long TTL would
#: otherwise make a step's return wait up to a quarter of it for a hung one.
_MAX_RENEWAL_WAIT_S = 10.0


def check_lease_ttl_ms(
    lease_ttl_ms: Optional[int], where: str, field: str = "lease_ttl_ms"
) -> Optional[int]:
    """Refuses a lease TTL the SDK will not ask for, naming where it was given.

    None means the default (the engine's) and passes through. Anything else must
    be a whole number of milliseconds, from `MIN_LEASE_TTL_MS` to
    `MAX_LEASE_TTL_MS`.
    """
    if lease_ttl_ms is None:
        return None
    if not isinstance(lease_ttl_ms, int) or isinstance(lease_ttl_ms, bool):
        raise TypeError(
            f"{where}: {field} must be a whole number of milliseconds, from "
            f"{MIN_LEASE_TTL_MS} to {MAX_LEASE_TTL_MS}; got "
            f"{type(lease_ttl_ms).__name__} {lease_ttl_ms!r}."
        )
    if not MIN_LEASE_TTL_MS <= lease_ttl_ms <= MAX_LEASE_TTL_MS:
        raise ValueError(
            f"{where}: {field} must be a whole number of milliseconds, from "
            f"{MIN_LEASE_TTL_MS} to {MAX_LEASE_TTL_MS}; got {lease_ttl_ms}."
        )
    return lease_ttl_ms


def _renew_failure_detail(reason: int) -> str:
    """Explains a renewal denial in terms of what the holder should do."""
    from cellaflow.v1 import idempotency_pb2 as _idem

    if reason == _idem.RENEW_FAILURE_REASON_MAX_LIFETIME_EXCEEDED:
        return (
            "the engine reclaimed it after the maximum lease lifetime "
            "(CELLAFLOW_MAX_LEASE_LIFETIME_MS). This step kept heartbeating but ran "
            "too long, so the position was released to other workers. Its commit "
            "will be rejected as a stale lease. If the step is legitimately this "
            "slow, raise the engine's limit; if it is hung, that is the bug."
        )
    if reason == _idem.RENEW_FAILURE_REASON_SUPERSEDED:
        return "a newer worker holds the lease; this one has been fenced out"
    if reason == _idem.RENEW_FAILURE_REASON_EXPIRED:
        return "it expired before this renewal arrived"
    if reason == _idem.RENEW_FAILURE_REASON_COMPLETED:
        return "the step was already committed by another worker"
    if reason == _idem.RENEW_FAILURE_REASON_NOT_FOUND:
        return "the key no longer exists"
    return f"unrecognised reason {reason}"


#: Heartbeats being stopped in the background by `stop_soon`, held so the event
#: loop does not drop them before they finish.
_stopping: "set[asyncio.Task[Any]]" = set()


class LeaseHeartbeat:
    """
    Manages the background heartbeat (RenewLease) for an actively running step.
    Supports both sync (daemon thread) and async (asyncio task) environments.
    """

    def __init__(
        self,
        client: CellaflowClient,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        heartbeat_interval_ms: int,
        on_lease_lost: Optional[Callable[[], None]] = None,
        lease_ttl_ms: Optional[int] = None,
        acquired_sent_at: Optional[Tuple[float, float]] = None,
        acquired_expires_at_ms: int = 0,
    ) -> None:
        """`acquired_sent_at` is when the acquisition was sent, as
        `(time.monotonic(), time.time())`: the lease's deadline counts from it.
        Omitted, it counts from now, which is later than the grant and so less
        cautious; callers that hold a lease pass it."""
        self.client = client
        self.agent_id = agent_id
        self.idempotency_key = idempotency_key
        self.fencing_token = fencing_token
        self.on_lease_lost = on_lease_lost
        # Renew slightly before expiration. Capped, so a very long TTL does not
        # ask for a wait the timers cannot hold.
        self.interval_sec = max(
            0.1, (min(heartbeat_interval_ms, _MAX_INTERVAL_MS) - 100) / 1000.0
        )
        # Renewals extend by the real TTL. Deriving `extend_ms` from the
        # interval instead would silently override a caller's `ttl_ms` on the
        # first heartbeat — a 60s lease renewed for 20s because the interval
        # happened to be 5s. Callers that do not state a TTL keep the historical
        # 4x-interval assumption.
        self.lease_ttl_ms = (
            lease_ttl_ms if lease_ttl_ms is not None else heartbeat_interval_ms * 4
        )
        self.extend_ms = self.lease_ttl_ms
        # A renewal must not outlive the lease it is renewing, and must not
        # outlive the interval either — otherwise renewals queue up behind a
        # stalled connection. Capped, so a long TTL does not make a step's
        # return wait on a hung renewal, and floored so a short test interval
        # stays workable.
        self.rpc_timeout_sec = max(
            1.0,
            min(self.interval_sec, self.lease_ttl_ms / 1000.0, _MAX_RENEWAL_WAIT_S),
        )
        # How long a shutdown waits for an in-flight renewal before giving up on
        # the thread. Bounded by construction: the RPC carries a deadline.
        self.stop_timeout_sec = self.rpc_timeout_sec + 1.0

        # The lease is judged on the clock, not only on renewal answers: it can
        # no longer be assumed held once a TTL has passed since the last
        # confirmed renewal was *sent* (or the acquisition, before any), less a
        # margin of half an interval, so this side gives up no later than the
        # engine hands the lease on. Kept on both the monotonic and the wall
        # clock: the wall clock keeps running through a host suspend, as the
        # engine's does, where the monotonic one may stand still.
        self.margin_sec = min(
            min(heartbeat_interval_ms, _MAX_INTERVAL_MS) / 2000.0,
            self.lease_ttl_ms / 2000.0,
        )
        sent = (
            acquired_sent_at
            if acquired_sent_at is not None
            else (time.monotonic(), time.time())
        )
        self._deadline: Tuple[float, float] = (0.0, 0.0)
        self._set_deadline(sent, self.lease_ttl_ms / 1000.0)
        # Set once the lease is known or judged lost; `on_lease_lost` runs once.
        self.lost = False
        # Why it was lost, the first time.
        self.lost_detail: Optional[str] = None
        self._lost_lock = threading.Lock()
        # Set once the heartbeat is told to stop: the lease is then being let
        # go, not lost, and the clock no longer judges it.
        self._stopped = False
        # Wall time minus engine time, in ms, for recent renewals known to have
        # been full extensions, and the latest renewal not yet known to be. See
        # `_renewal_duration`.
        self._full_offsets: List[float] = []
        self._pending: Optional[Tuple[float, float]] = None
        if acquired_expires_at_ms:
            # The grant's own expiry is the first reference candidate, so a
            # first renewal the engine already shortens is noticed too.
            self._pending = (
                sent[1] * 1000.0 + self.lease_ttl_ms - acquired_expires_at_ms,
                float(acquired_expires_at_ms),
            )

        # State tracking
        self._stop_event_sync = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None

        self._stop_event_async: Optional[asyncio.Event] = None
        self._async_task: Optional[asyncio.Task[Any]] = None

        # Monotonic timestamp of the last confirmed renewal, used to tell
        # "we know the lease is gone" from "we could not reach the engine".
        self._last_confirmed = time.monotonic()

    def _set_deadline(self, sent: Tuple[float, float], duration_sec: float) -> None:
        """Counts the lease's deadline from `sent`, on both clocks."""
        held = duration_sec - self.margin_sec
        self._deadline = (sent[0] + held, sent[1] + held)

    def _renewal_duration(self, sent_wall: float, expires_at_ms: int) -> float:
        """How long, in seconds from its send, a confirmed renewal keeps the
        lease held.

        Counted from the send, a full extension cannot outlive the engine's. But
        near the engine's maximum lease lifetime it grants less than it was
        asked, and only its expiry says so. That expiry is mapped onto the wall
        clock through a renewal known to have been full, and the shorter time
        counts. A renewal is known to have been full once a later one expires
        strictly later: a shortened one ends at the lease's fixed ceiling, which
        no later renewal can pass. Among the last few, the one with the least
        latency is the reference. Until one exists, the full extension counts.
        """
        full_ms = float(self.extend_ms)
        if not expires_at_ms:
            return full_ms / 1000.0
        if self._pending is not None and expires_at_ms > self._pending[1]:
            self._full_offsets.append(self._pending[0])
            if len(self._full_offsets) > 4:
                self._full_offsets.pop(0)
        sent_ms = sent_wall * 1000.0
        self._pending = (sent_ms + full_ms - expires_at_ms, float(expires_at_ms))
        if not self._full_offsets:
            return full_ms / 1000.0
        mapped_ms = expires_at_ms + max(self._full_offsets) - sent_ms
        return min(full_ms, mapped_ms) / 1000.0

    def past_deadline(self) -> bool:
        """True once the lease can no longer be assumed held, judged on the clock
        now; declares it lost the first time. Cheap enough to call before
        anything irreversible, and called by every `LeaseHandle` probe. False
        once the heartbeat has been told to stop: the lease is being let go."""
        if self.lost:
            return True
        if self._stopped:
            return False
        mono, wall = self._deadline
        if time.monotonic() < mono and time.time() < wall:
            return False
        self._notify_lost(
            "no renewal was confirmed in time: the deadline counted from the "
            "last one sent has passed"
        )
        return True

    def _notify_lost(self, detail: str) -> None:
        """Marks the lease lost and runs the caller's callback, once."""
        with self._lost_lock:
            if self.lost:
                # Already judged lost on the clock; the engine's own reason is
                # still worth a line.
                logger.info("Lease %s: %s", self.idempotency_key, detail)
                return
            self.lost = True
            self.lost_detail = detail
        logger.warning("Lease %s lost: %s", self.idempotency_key, detail)
        if self.on_lease_lost is None:
            return
        try:
            self.on_lease_lost()
        except Exception as e:
            logger.error("on_lease_lost callback raised an error: %s", e)

    def _ttl_exhausted(self) -> bool:
        """True once the lease can no longer be assumed held.

        Renewal errors are not themselves proof of loss — a failed RPC means we
        could not confirm the lease, while the engine may still hold it for us
        until the TTL runs out. Declaring loss on an error count instead would
        abort work that still holds a perfectly valid lease, so loss is declared
        on elapsed time since the last *confirmed* renewal.
        """
        elapsed_ms = (time.monotonic() - self._last_confirmed) * 1000.0
        return elapsed_ms >= self.lease_ttl_ms

    def confirm_grant(self) -> None:
        """Counts the lease from a renewal rather than from the request that
        won it. Called before the heartbeat starts.

        After the engine waited before granting, the lease can be much younger
        than that request, and a deadline counted from it would read a fresh
        lease as nearly expired. A renewal sent now gives a deadline counted
        from a known time. One failed attempt is not a loss, so this tries a
        few, backing off, while the lease, counted from when the grant arrived,
        could still be live. If none is confirmed, the deadline counted from the
        request still bounds the lease from below, since the grant came after
        it. A refusal declares the lease lost.
        """
        window_end = time.monotonic() + self.lease_ttl_ms / 1000.0 - self.margin_sec
        for attempt in range(3):
            # Told to stop: the call was cancelled, and the lease handed back.
            if self.lost or self._stopped:
                return
            sent = (time.monotonic(), time.time())
            try:
                resp = self.client.renew_lease(
                    agent_id=self.agent_id,
                    idempotency_key=self.idempotency_key,
                    fencing_token=self.fencing_token,
                    extend_ms=self.extend_ms,
                    timeout=self.rpc_timeout_sec,
                )
            except Exception as e:
                logger.info(
                    "Lease %s: renewal after the grant failed: %s",
                    self.idempotency_key,
                    e,
                )
            else:
                if not resp.renewed:
                    self._notify_lost(_renew_failure_detail(resp.failure_reason))
                    return
                self._last_confirmed = time.monotonic()
                self._set_deadline(
                    sent, self._renewal_duration(sent[1], resp.new_expires_at_ms)
                )
                return
            # No pause after the last attempt: it would only delay the body,
            # or the hand-back, while the lease runs down.
            if attempt == 2:
                return
            pause = min(0.1 * 2**attempt, window_end - time.monotonic())
            if pause <= 0:
                return
            time.sleep(pause)

    def start_sync(self) -> None:
        """Starts a daemon thread for synchronous execution."""
        self._stop_event_sync.clear()
        self._last_confirmed = time.monotonic()
        self._sync_thread = threading.Thread(
            target=self._sync_loop, daemon=True, name=f"lease-hb-{self.fencing_token}"
        )
        self._sync_thread.start()

    def stop_sync(self) -> None:
        """Stops the daemon thread, waiting briefly for an in-flight renewal.

        The wait is bounded. An unbounded join would hang the caller whenever a
        connection black-holes, and `@step` runs this in its `finally`, so the
        hang would reach every synchronous step. `renew_lease` carries a deadline
        precisely so this wait can stay bounded and still, in practice, outlast
        the RPC rather than closing the client underneath it.
        """
        self._stopped = True
        self._stop_event_sync.set()
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=self.stop_timeout_sec)
            if self._sync_thread.is_alive():
                logger.warning(
                    "Heartbeat thread for lease %s did not stop within %.1fs; "
                    "abandoning it. A renewal RPC may still be in flight.",
                    self.idempotency_key,
                    self.stop_timeout_sec,
                )

    def _sync_loop(self) -> None:
        while not self._stop_event_sync.wait(timeout=self.interval_sec):
            # A holder resumed past its deadline reports the loss before asking,
            # and renews anyway: the engine may still hold the lease, and work
            # that went ahead without checking keeps it for as long as the
            # engine agrees, so its result can still be recorded. Renewing
            # stops when the engine refuses a renewal, or on stop.
            self.past_deadline()
            sent = (time.monotonic(), time.time())
            try:
                resp = self.client.renew_lease(
                    agent_id=self.agent_id,
                    idempotency_key=self.idempotency_key,
                    fencing_token=self.fencing_token,
                    extend_ms=self.extend_ms,
                    timeout=self.rpc_timeout_sec,
                )
                if not resp.renewed:
                    self._notify_lost(_renew_failure_detail(resp.failure_reason))
                    break
                self._last_confirmed = time.monotonic()
                self._set_deadline(
                    sent, self._renewal_duration(sent[1], resp.new_expires_at_ms)
                )
            except Exception as e:
                logger.error("Lease renewal error for %s: %s", self.idempotency_key, e)
                if self._ttl_exhausted():
                    self._notify_lost(
                        f"could not be renewed for {self.lease_ttl_ms}ms "
                        f"(last error: {e})"
                    )
                    break

    def start_async(self) -> None:
        """Starts an asyncio task for asynchronous execution."""
        self._stop_event_async = asyncio.Event()
        self._last_confirmed = time.monotonic()
        loop = asyncio.get_running_loop()
        self._async_task = loop.create_task(self._async_loop())

    def stop_soon(self) -> None:
        """Stops the heartbeat without waiting, once its step has committed.

        After the commit nothing may be awaited before the step returns: an
        `await` is where a cancellation lands, and a step cancelled after its
        commit would be recorded as cancelled while its history says it
        succeeded. So the stop is signalled here and finished in the background,
        with the same bounded wait as `stop_async`.
        """
        self._stopped = True
        if self._stop_event_async:
            self._stop_event_async.set()
        task = self._async_task
        if task is None or task.done():
            return
        finisher = asyncio.get_running_loop().create_task(self._finish_stopping(task))
        _stopping.add(finisher)
        finisher.add_done_callback(_stopping.discard)

    async def stop_async(self) -> None:
        """Stops the asyncio task, waiting briefly for an in-flight renewal.

        Cancelling outright would not help: the renewal runs in a worker thread
        via `asyncio.to_thread`, and cancelling the coroutine awaiting it leaves
        that thread running the RPC after the caller believes the heartbeat has
        stopped — with the client often closed next. So the stop event is set and
        the task is given a bounded chance to notice, and only then cancelled.
        """
        self._stopped = True
        if self._stop_event_async:
            self._stop_event_async.set()
        task = self._async_task
        if task is None or task.done():
            return
        await self._finish_stopping(task)

    async def _finish_stopping(self, task: "asyncio.Task[Any]") -> None:
        # `asyncio.wait` neither cancels the task on timeout nor re-raises its
        # exception, which is exactly the "give it a chance" semantics wanted.
        done, _ = await asyncio.wait({task}, timeout=self.stop_timeout_sec)
        if not done:
            logger.warning(
                "Heartbeat task for lease %s did not stop within %.1fs; "
                "cancelling it. A renewal RPC may still be in flight.",
                self.idempotency_key,
                self.stop_timeout_sec,
            )
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _async_loop(self) -> None:
        if not self._stop_event_async:
            return

        while True:
            try:
                await asyncio.wait_for(
                    self._stop_event_async.wait(), timeout=self.interval_sec
                )
                # Stop event was set — clean shutdown.
                break
            except asyncio.TimeoutError:
                # Interval elapsed: do heartbeat. A holder resumed past its
                # deadline reports the loss before asking, and renews anyway;
                # see `_sync_loop`.
                self.past_deadline()
                sent = (time.monotonic(), time.time())
                try:
                    # Offload the sync gRPC call so it does not block the loop.
                    resp = await asyncio.to_thread(
                        self.client.renew_lease,
                        agent_id=self.agent_id,
                        idempotency_key=self.idempotency_key,
                        fencing_token=self.fencing_token,
                        extend_ms=self.extend_ms,
                        timeout=self.rpc_timeout_sec,
                    )
                    if not resp.renewed:
                        self._notify_lost(_renew_failure_detail(resp.failure_reason))
                        break
                    self._last_confirmed = time.monotonic()
                    self._set_deadline(
                        sent, self._renewal_duration(sent[1], resp.new_expires_at_ms)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        "Lease renewal error for %s: %s", self.idempotency_key, e
                    )
                    if self._ttl_exhausted():
                        self._notify_lost(
                            f"could not be renewed for {self.lease_ttl_ms}ms "
                            f"(last error: {e})"
                        )
                        break
            except asyncio.CancelledError:
                break
