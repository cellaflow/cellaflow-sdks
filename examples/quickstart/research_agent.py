"""The landing-page quickstart: a step that moves money, and a crash on top of it.

    python research_agent.py                  # charges, then the pod dies
    python research_agent.py <session-id>     # resumes -- does NOT charge again
    cat charges.log                           # one line, both times

The ledger file is the adjudicator. Not what the script says about itself.
"""

import os
import sys
import uuid
from pathlib import Path

from cellaflow import step, tool, workflow

LEDGER = Path("charges.log")


@tool(tool_name="charge_card")
def charge_card(order_id: str, cents: int) -> dict:
    """The irreversible one. Leased, so it happens at most once per session."""
    confirmation = f"ch_{uuid.uuid4().hex[:10]}"
    with LEDGER.open("a") as fh:
        fh.write(f"{confirmation} {order_id} {cents}\n")
    print(f"   💳 CHARGED {cents} to {order_id} -> {confirmation}")
    return {"confirmation": confirmation, "cents": cents}


@step
def build_receipt(charge: dict, order_id: str) -> dict:
    print("   🧾 Building receipt...")
    return {"order_id": order_id, "confirmation": charge["confirmation"]}


@workflow(version="1.0.0")
def checkout(order_id: str, die_after_charging: bool = False) -> dict:
    charge = charge_card(order_id, 2499)

    if die_after_charging:
        # The money has moved and nothing durable records the receipt yet.
        # This is the worst possible moment for the pod to go away.
        print("   💥 pod died")
        sys.stdout.flush()
        os._exit(17)

    return build_receipt(charge, order_id)


if __name__ == "__main__":
    resuming = len(sys.argv) > 1
    session_id = sys.argv[1] if resuming else str(uuid.uuid4())

    if resuming:
        print(f"\n♻️  Resuming session {session_id}\n")
    else:
        LEDGER.unlink(missing_ok=True)
        print(f"\n▶️  Session {session_id}")
        print("   (save that id -- you need it to resume)\n")

    result = checkout(
        "ORD-1001", die_after_charging=not resuming, _session_id=session_id
    )

    charges = LEDGER.read_text().splitlines()
    print(f"\n✅ {result}")
    print(f"\n   charge_card was NOT re-executed -- replayed from the durable log.")
    print(f"   charges.log holds {len(charges)} charge, across both runs.\n")
