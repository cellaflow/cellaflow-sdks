import asyncio
import threading
import logging
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
        max_network_errors: int = 3,
    ) -> None:
        self.client = client
        self.agent_id = agent_id
        self.idempotency_key = idempotency_key
        self.fencing_token = fencing_token
        self.on_lease_lost = on_lease_lost
        self.max_network_errors = max_network_errors
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
            # Join without timeout because closing the client while a 
            # renew_lease RPC is still in flight will cause a gRPC panic.
            # The daemon thread will exit once the RPC times out or finishes.
            self._sync_thread.join()

    def _sync_loop(self) -> None:
        consecutive_errors = 0
        while not self._stop_event_sync.wait(timeout=self.interval_sec):
            try:
                resp = self.client.renew_lease(
                    agent_id=self.agent_id,
                    idempotency_key=self.idempotency_key,
                    fencing_token=self.fencing_token,
                    extend_ms=self.extend_ms,
                )
                consecutive_errors = 0
                if not resp.renewed:
                    logger.warning(
                        "Lease %s failed to renew: %s",
                        self.idempotency_key,
                        _renew_failure_detail(resp.failure_reason),
                    )
                    if self.on_lease_lost is not None:
                        try:
                            self.on_lease_lost()
                        except Exception as e:
                            logger.error("on_lease_lost callback raised an error: %s", e)
                    break
            except Exception as e:
                consecutive_errors += 1
                logger.error("Lease renewal error for %s: %s", self.idempotency_key, e)
                if consecutive_errors >= self.max_network_errors:
                    if self.on_lease_lost is not None:
                        try:
                            self.on_lease_lost()
                        except Exception as e:
                            logger.error("on_lease_lost callback raised an error: %s", e)
                    break

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

        consecutive_errors = 0
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
                    # Offload sync gRPC call to a thread so it doesn't block the event loop
                    resp = await asyncio.to_thread(
                        self.client.renew_lease,
                        agent_id=self.agent_id,
                        idempotency_key=self.idempotency_key,
                        fencing_token=self.fencing_token,
                        extend_ms=self.extend_ms,
                    )
                    consecutive_errors = 0
                    if not resp.renewed:
                        logger.warning(
                            "Lease %s failed to renew: %s",
                            self.idempotency_key,
                            _renew_failure_detail(resp.failure_reason),
                        )
                        if self.on_lease_lost is not None:
                            try:
                                self.on_lease_lost()
                            except Exception as e:
                                logger.error("on_lease_lost callback raised an error: %s", e)
                        break
                except Exception as e:
                    consecutive_errors += 1
                    logger.error("Lease renewal error for %s: %s", self.idempotency_key, e)
                    if consecutive_errors >= self.max_network_errors:
                        if self.on_lease_lost is not None:
                            try:
                                self.on_lease_lost()
                            except Exception as cb_e:
                                logger.error("on_lease_lost callback raised an error: %s", cb_e)
                        break
            except asyncio.CancelledError:
                break
