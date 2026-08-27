import asyncio
import threading
import logging
from typing import Optional, Any
from cellaflow.client import CellaflowClient

logger = logging.getLogger(__name__)


def _renew_failure_detail(reason: int) -> str:
    """Explains a renewal denial in terms of what the holder should do.

    The raw enum value alone is close to useless in a log: the step body is still
    running at this point and is about to attempt a commit that will be rejected,
    so the message has to say why the lease is gone.
    """
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
    ) -> None:
        self.client = client
        self.agent_id = agent_id
        self.idempotency_key = idempotency_key
        self.fencing_token = fencing_token
        # Renew slightly before expiration
        self.interval_sec = max(0.1, (heartbeat_interval_ms - 100) / 1000.0)
        self.extend_ms = (
            heartbeat_interval_ms * 4
        )  # Usually TTL is 4x heartbeat interval

        # State tracking
        self._stop_event_sync = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None

        self._stop_event_async: Optional[asyncio.Event] = None
        self._async_task: Optional[asyncio.Task[Any]] = None

    def start_sync(self) -> None:
        """Starts a daemon thread for synchronous execution."""
        self._stop_event_sync.clear()
        self._sync_thread = threading.Thread(
            target=self._sync_loop, daemon=True, name=f"lease-hb-{self.fencing_token}"
        )
        self._sync_thread.start()

    def stop_sync(self) -> None:
        """Stops the daemon thread cleanly."""
        self._stop_event_sync.set()
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=1.0)

    def _sync_loop(self) -> None:
        while not self._stop_event_sync.wait(timeout=self.interval_sec):
            try:
                resp = self.client.renew_lease(
                    agent_id=self.agent_id,
                    idempotency_key=self.idempotency_key,
                    fencing_token=self.fencing_token,
                    extend_ms=self.extend_ms,
                )
                if not resp.renewed:
                    logger.warning(
                        "Lease %s failed to renew: %s",
                        self.idempotency_key,
                        _renew_failure_detail(resp.failure_reason),
                    )
                    break
            except Exception as e:
                logger.error(f"Lease renewal error: {e}")

    def start_async(self) -> None:
        """Starts an asyncio task for asynchronous execution."""
        self._stop_event_async = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._async_task = loop.create_task(self._async_loop())

    async def stop_async(self) -> None:
        """Stops the asyncio task cleanly."""
        if self._stop_event_async:
            self._stop_event_async.set()
        if self._async_task and not self._async_task.done():
            self._async_task.cancel()
            try:
                await self._async_task
            except asyncio.CancelledError:
                pass

    async def _async_loop(self) -> None:
        if not self._stop_event_async:
            return

        # We use asyncio.wait_for to wait for the stop event, with a timeout
        while True:
            try:
                await asyncio.wait_for(
                    self._stop_event_async.wait(), timeout=self.interval_sec
                )
                # If we get here, the stop event was set
                break
            except asyncio.TimeoutError:
                # Timeout means interval elapsed, do heartbeat
                try:
                    resp = self.client.renew_lease(
                        agent_id=self.agent_id,
                        idempotency_key=self.idempotency_key,
                        fencing_token=self.fencing_token,
                        extend_ms=self.extend_ms,
                    )
                    if not resp.renewed:
                        logger.warning(
                            "Lease %s failed to renew: %s",
                            self.idempotency_key,
                            _renew_failure_detail(resp.failure_reason),
                        )
                        break
                except Exception as e:
                    logger.error(f"Lease renewal error: {e}")
            except asyncio.CancelledError:
                break
