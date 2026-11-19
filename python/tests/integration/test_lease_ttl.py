"""A step's lease_ttl_ms against a real engine: a holder killed mid-call is taken
over within its TTL, not the engine's 20 s default."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import subprocess
import sys
import textwrap
import threading
import time
import uuid
from typing import Any, List, Optional, Tuple


from cellaflow import IdempotencyScope, durable_tools, tool

TTL_MS = 3000
#: The engine asks for a renewal every quarter of the TTL.
RENEW_EVERY_MS = TTL_MS // 4
#: A TTL that did not reach the engine would leave the taker polling until the
#: engine's default lapses, or its maximum lease lifetime: fail rather than hang.
GIVE_UP_S = 30.0
#: How soon after the latest possible lapse a waiting caller must run the call.
#: Below what polling takes here (at least the 600 ms the mid-call kill leaves
#: after a renewal), with room for a loaded CI engine.
TAKEN_OVER_WITHIN_MS = 500


def holder(target: str, order_id: str) -> "subprocess.Popen[str]":
    """Starts a process that takes the lease on `order_id`'s shared tool and then
    holds it, renewing, until killed. The TTL comes from the block's
    `lease_ttl_ms`, so the block default reaching the tool is what this measures.
    The process prints the wall time of each confirmed grant and renewal: the
    lease lapses no later than a TTL after the last.
    """
    script = textwrap.dedent(
        f"""
        import threading
        import time
        from cellaflow import IdempotencyScope, durable_tools, tool
        from cellaflow.client import CellaflowClient
        from cellaflow.v1 import idempotency_pb2

        renew = CellaflowClient.renew_lease
        def renew_lease(self, *args, **kwargs):
            print("sending", time.time(), flush=True)
            resp = renew(self, *args, **kwargs)
            if resp.renewed:
                print("confirmed", time.time(), flush=True)
            return resp
        CellaflowClient.renew_lease = renew_lease

        check = CellaflowClient.check_idempotency_cache
        def check_idempotency_cache(self, *args, **kwargs):
            resp = check(self, *args, **kwargs)
            if resp.status == idempotency_pb2.CACHE_STATUS_ACQUIRED:
                print("confirmed", time.time(), flush=True)
            return resp
        CellaflowClient.check_idempotency_cache = check_idempotency_cache

        @tool(tool_name="charge_card", scope=IdempotencyScope.SCOPE_SHARED,
              shared_on=["order_id"])
        def charge_card(order_id):
            print("holding", flush=True)
            threading.Event().wait()

        with durable_tools("holder-{uuid.uuid4().hex[:8]}", target={target!r},
                           coordination_id={order_id!r}, lease_ttl_ms={TTL_MS}):
            charge_card({order_id!r})
        """
    )
    return subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
    )


def takeover(
    target: str, hold_s: float, after_renewal_s: Optional[float] = None
) -> Tuple[float, float]:
    """Kills a holder `hold_s` after it took the lease (and, with
    `after_renewal_s`, that long after its next confirmed renewal). Returns how
    long another caller waited before running the call itself, from the kill
    and from the latest possible lapse, in ms."""
    order_id = f"order-{uuid.uuid4().hex[:8]}"
    child = holder(target, order_id)
    confirmed: List[float] = []
    sent: List[float] = []
    renewed = threading.Event()
    holding = threading.Event()

    def read() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            if line.startswith("sending "):
                sent.append(float(line.split()[1]))
            elif line.startswith("confirmed "):
                confirmed.append(float(line.split()[1]))
                renewed.set()
            elif line.strip() == "holding":
                holding.set()

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        assert holding.wait(GIVE_UP_S), f"the holder exited ({child.poll()}) first"
        time.sleep(hold_s)
        if after_renewal_s is not None:
            renewed.clear()
            assert renewed.wait(GIVE_UP_S), "the holder stopped renewing"
            time.sleep(after_renewal_s)
        assert child.poll() is None, "the holder exited before it was killed"
    finally:
        # Also when the holder never took the lease, so a failure cannot leave
        # it running.
        child.kill()
        killed_at = time.monotonic()
        killed_wall = time.time()
        child.wait()
        reader.join(GIVE_UP_S)
        if child.stdout is not None:
            child.stdout.close()

    ran_at: Any = None

    @tool(
        tool_name="charge_card",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
        lease_ttl_ms=TTL_MS,
    )
    def charge_card(order_id: str) -> str:
        nonlocal ran_at
        ran_at = time.monotonic()
        return "charged-here"

    results: Any = []

    def take() -> None:
        with durable_tools(
            f"taker-{uuid.uuid4().hex[:8]}", target=target, coordination_id=order_id
        ):
            results.append(charge_card(order_id))

    taker = threading.Thread(target=take, daemon=True)
    taker.start()
    taker.join(GIVE_UP_S)
    assert not taker.is_alive(), f"no takeover within {GIVE_UP_S:.0f} s"
    assert results == ["charged-here"]
    ran_wall = killed_wall + (ran_at - killed_at)
    last = max(at for at in confirmed if at <= killed_wall)
    # A renewal sent but not confirmed before the kill may still have reached
    # the engine, as late as the kill: the lease is then counted from the kill.
    if any(last < at <= killed_wall for at in sent):
        last = killed_wall
    after_lapse = (ran_wall - (last + TTL_MS / 1000.0)) * 1000.0
    return float((ran_at - killed_at) * 1000.0), after_lapse


def check_bounds(waited: float, after_lapse: float) -> None:
    # The lease lapses no later than a TTL after the last confirmation the
    # holder printed before it was killed. A caller waiting in the engine is
    # granted it within the engine's 100 ms check of that, plus the grant and
    # the renewal that confirms a late grant. One polling once a second, started
    # at the kill, lands up to a second after the lapse.
    assert after_lapse <= TAKEN_OVER_WITHIN_MS, f"took over {after_lapse:.0f} ms late"
    # And not while the lease was still live: the last renewal (or the
    # acquisition) came at most an interval before the kill.
    assert waited >= TTL_MS - 2 * RENEW_EVERY_MS, f"took over after {waited:.0f} ms"


def test_a_holder_killed_before_its_first_renewal_is_taken_over_within_its_ttl(
    target: str, capsys: Any
) -> None:
    # Only the acquisition carried the TTL, so this is what proves it was sent.
    waited, after_lapse = takeover(target, 0.05)
    with capsys.disabled():
        print(f"\n  took over {waited:.0f} ms after the kill, {after_lapse:.0f} late")
    check_bounds(waited, after_lapse)


def test_a_holder_killed_mid_call_is_taken_over_within_its_ttl(
    target: str, capsys: Any
) -> None:
    # Two TTLs: past the first, the holder can only still own the lease because
    # its renewals extended it. Killed 600 ms after a renewal, so the lease
    # lapses 600 ms before a caller polling once a second from the kill would
    # ask.
    waited, after_lapse = takeover(target, 2 * TTL_MS / 1000.0, 0.6)
    with capsys.disabled():
        print(f"\n  took over {waited:.0f} ms after the kill, {after_lapse:.0f} late")
    check_bounds(waited, after_lapse)
