"""The landing-page quickstart: a step that moves money, and a crash on top of it.

    python research_agent.py                  # charges, then the pod dies
    python research_agent.py <session-id>     # resumes -- does NOT charge again
    cat charges.log                           # one line, both times

The ledger file is the adjudicator, and the counter below lives INSIDE the tool
body -- so this script can only report what actually happened, not what it hopes
happened.
"""

import os
import sys
import uuid
from pathlib import Path

from cellaflow import step, tool, workflow

LEDGER = Path("charges.log")

#: Incremented inside the tool body. Stays 0 when the step is replayed, because
#: a replayed step never enters its body.
EXECUTED = {"charges": 0}


@tool(tool_name="charge_card")
def charge_card(order_id: str, cents: int) -> dict:
    """The irreversible one. Leased, so it happens at most once per session."""
    EXECUTED["charges"] += 1
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

    total = len(LEDGER.read_text().splitlines())
    print(f"\n✅ {result}")

    if resuming and EXECUTED["charges"] == 0:
        print("\n   charge_card did NOT run -- replayed from the durable log.")
    elif resuming:
        print(
            "\n   ⚠️  charge_card DID run. That session id had no history on this"
            "\n       engine, so this started a new run rather than resuming one."
            "\n       Use the id printed by your own first run."
        )
    print(f"   charges.log holds {total} charge(s).\n")
