"""A stand-in for a real, irreversible side effect: a shop that places orders.

The point of this module is that it is deliberately *not* idempotent and knows
nothing about Cellaflow. Like many order, booking and checkout endpoints, it takes
no idempotency key, so every call places one more order. Each call appends one
line to a ledger file, and counting the lines afterwards is how the demo proves
how many orders were actually placed -- no trust in log statements required.

The ledger is a plain append-only file guarded by ``flock`` so that agents running
as separate OS processes can share it safely.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_LEDGER = Path(__file__).parent / ".ledger.jsonl"

# How long placing an order takes. This is the race window: it has to be wide
# enough that every agent is inside the critical section at the same time,
# otherwise the naive run might accidentally serialize and understate the problem.
ORDER_LATENCY_S = 0.4


def ledger_path() -> Path:
    return Path(os.environ.get("CELLAFLOW_DEMO_LEDGER", str(DEFAULT_LEDGER)))


def reset_ledger() -> None:
    path = ledger_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("")


def read_ledger() -> List[Dict[str, Any]]:
    path = ledger_path()
    if not path.exists():
        return []
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    return [json.loads(ln) for ln in lines]


def place_order(order_id: str, total_cents: int, placed_by: str = "agent") -> Dict[str, Any]:
    """Place the order and bill the customer. Not idempotent, not retry-safe.

    Returns a fresh confirmation id on every call -- which is what makes the
    coordinated run legible: if all agents report the *same* confirmation id, they
    are all looking at a single order rather than at N orders that happen to
    have equal totals.
    """
    time.sleep(ORDER_LATENCY_S)

    entry = {
        "confirmation_id": f"ord_{uuid.uuid4().hex[:12]}",
        "order_id": order_id,
        "total_cents": total_cents,
        "placed_by": placed_by,
        "at": time.time(),
    }

    path = ledger_path()
    with open(path, "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            fh.write(json.dumps(entry) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)

    # Only the fields an agent would actually keep. Must stay MessagePack-safe
    # (plain str/int) because this becomes a committed step payload.
    return {
        "confirmation_id": entry["confirmation_id"],
        "order_id": order_id,
        "total_cents": total_cents,
        # Carried back so a caller can tell whether it placed this order or
        # received someone else's. The SDK itself does not report which path a
        # step took, so the demo infers it from the payload -- see README.
        "placed_by": placed_by,
    }
