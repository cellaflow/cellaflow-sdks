import pytest
import asyncio
import time
from unittest.mock import MagicMock

from cellaflow.lease import LeaseHeartbeat
from cellaflow.v1 import idempotency_pb2


def test_lease_heartbeat_sync() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1000)
    mock_client.renew_lease.return_value = mock_resp

    # Small interval so it fires quickly
    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        # 200ms -> interval_sec = max(0.1, (200-100)/1000) = 0.1
        heartbeat_interval_ms=200,
    )

    hb.start_sync()
    time.sleep(0.25)  # Give it time to tick twice
    hb.stop_sync()

    assert mock_client.renew_lease.call_count >= 1
    call_args = mock_client.renew_lease.call_args[1]
    assert call_args["agent_id"] == "agent1"
    assert call_args["fencing_token"] == 123


@pytest.mark.asyncio
async def test_lease_heartbeat_async() -> None:
    mock_client = MagicMock()
    mock_resp = idempotency_pb2.RenewLeaseResponse(renewed=True, new_expires_at_ms=1000)
    mock_client.renew_lease.return_value = mock_resp

    hb = LeaseHeartbeat(
        client=mock_client,
        agent_id="agent1",
        idempotency_key="key1",
        fencing_token=123,
        heartbeat_interval_ms=200,
    )

    hb.start_async()
    await asyncio.sleep(0.25)
    await hb.stop_async()

    assert mock_client.renew_lease.call_count >= 1
    call_args = mock_client.renew_lease.call_args[1]
    assert call_args["agent_id"] == "agent1"
