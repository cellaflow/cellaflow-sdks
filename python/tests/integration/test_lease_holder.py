"""Who holds a lease, against a real engine: a second worker refused while the
first holds the lease is told the first worker's id, and once the first
releases it a third worker takes the lease with nothing to refuse."""

import uuid

import pytest

from cellaflow import LeaseNotAcquired, execution_lease


def test_a_refused_worker_learns_who_holds_the_lease(target: str) -> None:
    key = f"task:{uuid.uuid4()}"
    first = f"agent-a-{uuid.uuid4().hex[:8]}"

    with execution_lease(key, worker_id=first, target=target, ttl_ms=10_000):
        with pytest.raises(LeaseNotAcquired) as excinfo:
            with execution_lease(key, worker_id="agent-b", target=target):
                pytest.fail("the second worker was granted a held lease")

    holder = excinfo.value.holder
    assert holder is not None
    assert holder.worker_id == first
    assert holder.engine_holder_id
    # Times are on the engine's clock, which need not be this one, so they are
    # checked against each other rather than against time.time().
    assert holder.acquired_at_ms is not None
    # The holder's own lease: its 10 s TTL (plus a renewal, every 2.5 s, on a
    # slow runner), not the refused worker's 30 s default, under the ceiling.
    assert 9_000 <= holder.expires_at_ms - holder.acquired_at_ms <= 14_000
    assert holder.hard_expires_at_ms is not None
    assert holder.hard_expires_at_ms > holder.expires_at_ms

    # Released when the block exited, so the key is free again.
    with execution_lease(key, worker_id="agent-c", target=target) as lease:
        assert lease.fencing_token > 0
