"""A stand-in for a real, irreversible side effect.

The point of this module is that it is deliberately *not* idempotent and knows
nothing about CellaFlow. Every call to :func:`issue_refund` appends one line to a
ledger file. Counting the lines afterwards is how the demo proves how many times
the side effect actually happened -- no trust in log statements required.

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

# How long a "refund" takes. This is the race window: it has to be wide enough
# that every agent is inside the critical section at the same time, otherwise the
# naive run might accidentally serialize and understate the problem.
REFUND_LATENCY_S = 0.4


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


def issue_refund(ticket_id: str, amount_cents: int, charged_by: str) -> Dict[str, Any]:
    """Move real money. Not idempotent, not retry-safe, not reversible.

    Returns a fresh confirmation id on every call -- which is what makes the
    coordinated run legible: if all agents report the *same* confirmation id, they
    are all looking at a single charge rather than at N charges that happen to
    have equal amounts.
    """
    time.sleep(REFUND_LATENCY_S)

    entry = {
        "confirmation_id": f"rf_{uuid.uuid4().hex[:12]}",
        "ticket_id": ticket_id,
        "amount_cents": amount_cents,
        "charged_by": charged_by,
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
        "ticket_id": ticket_id,
        "amount_cents": amount_cents,
        # Carried back so a caller can tell whether it performed this charge or
        # received someone else's. The SDK itself does not report which path a
        # step took, so the demo infers it from the payload -- see README.
        "charged_by": charged_by,
    }
