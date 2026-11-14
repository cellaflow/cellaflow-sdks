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
from typing import Any


from cellaflow import IdempotencyScope, durable_tools, tool

TTL_MS = 3000
#: The engine asks for a renewal every quarter of the TTL.
RENEW_EVERY_MS = TTL_MS // 4
#: A TTL that did not reach the engine would leave the taker polling until the
#: engine's default lapses, or its maximum lease lifetime: fail rather than hang.
GIVE_UP_S = 30.0


def holder(target: str, order_id: str) -> "subprocess.Popen[str]":
    """Starts a process that takes the lease on `order_id`'s shared tool and then
    holds it, renewing, until killed. The TTL comes from the block's
    `lease_ttl_ms`, so the block default reaching the tool is what this measures.
    """
    script = textwrap.dedent(
        f"""
        import threading
        from cellaflow import IdempotencyScope, durable_tools, tool

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


def takeover(target: str, hold_s: float) -> float:
    """Kills a holder `hold_s` after it took the lease, then returns how long
    another caller waited before running the call itself, in ms."""
    order_id = f"order-{uuid.uuid4().hex[:8]}"
    child = holder(target, order_id)
    try:
        assert child.stdout is not None
        line = child.stdout.readline()
        assert "holding" in line, f"the holder exited ({child.poll()}) first"
        time.sleep(hold_s)
        assert child.poll() is None, "the holder exited before it was killed"
    finally:
        # Also when the holder never took the lease, so a failure cannot leave
        # it running.
        child.kill()
        killed_at = time.monotonic()
        child.wait()
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
    return float((ran_at - killed_at) * 1000.0)


def check_bounds(waited: float) -> None:
    # The holder's lease lapses no later than a TTL after the kill, and a waiting
    # caller asks again about once a second.
    assert waited <= TTL_MS + 1000 + 500, f"took over after {waited:.0f} ms"
    # And not while the lease was still live: the last renewal (or the
    # acquisition) came at most an interval before the kill.
    assert waited >= TTL_MS - 2 * RENEW_EVERY_MS, f"took over after {waited:.0f} ms"


def test_a_holder_killed_before_its_first_renewal_is_taken_over_within_its_ttl(
    target: str, capsys: Any
) -> None:
    # Only the acquisition carried the TTL, so this is what proves it was sent.
    waited = takeover(target, 0.05)
    with capsys.disabled():
        print(f"\n  took over {waited:.0f} ms after the kill")
    check_bounds(waited)


def test_a_holder_killed_mid_call_is_taken_over_within_its_ttl(
    target: str, capsys: Any
) -> None:
    # Two TTLs: past the first, the holder can only still own the lease because
    # its renewals extended it.
    waited = takeover(target, 2 * TTL_MS / 1000.0)
    with capsys.disabled():
        print(f"\n  took over {waited:.0f} ms after the kill")
    check_bounds(waited)
