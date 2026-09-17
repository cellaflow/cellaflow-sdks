import asyncio
import contextlib
import threading
import logging
import time
from typing import Callable, Optional, Any
from cellaflow.client import CellaflowClient

logger = logging.getLogger(__name__)


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
    ) -> None:
        self.client = client
        self.agent_id = agent_id
        self.idempotency_key = idempotency_key
        self.fencing_token = fencing_token
        self.on_lease_lost = on_lease_lost
        # Renew slightly before expiration
        self.interval_sec = max(0.1, (heartbeat_interval_ms - 100) / 1000.0)
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
        # stalled connection. Floored so a short test interval stays workable.
        self.rpc_timeout_sec = max(
            1.0, min(self.interval_sec, self.lease_ttl_ms / 1000.0)
        )
        # How long a shutdown waits for an in-flight renewal before giving up on
        # the thread. Bounded by construction: the RPC carries a deadline.
        self.stop_timeout_sec = self.rpc_timeout_sec + 1.0

        # State tracking
        self._stop_event_sync = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None

        self._stop_event_async: Optional[asyncio.Event] = None
        self._async_task: Optional[asyncio.Task[Any]] = None

        # Monotonic timestamp of the last confirmed renewal, used to tell
        # "we know the lease is gone" from "we could not reach the engine".
        self._last_confirmed = time.monotonic()

    def _notify_lost(self, detail: str) -> None:
        """Marks the lease lost and runs the caller's callback, if any."""
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

    async def stop_async(self) -> None:
        """Stops the asyncio task, waiting briefly for an in-flight renewal.

        Cancelling outright would not help: the renewal runs in a worker thread
        via `asyncio.to_thread`, and cancelling the coroutine awaiting it leaves
        that thread running the RPC after the caller believes the heartbeat has
        stopped — with the client often closed next. So the stop event is set and
        the task is given a bounded chance to notice, and only then cancelled.
        """
        if self._stop_event_async:
            self._stop_event_async.set()
        task = self._async_task
        if task is None or task.done():
            return

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
                # Interval elapsed — do heartbeat.
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
