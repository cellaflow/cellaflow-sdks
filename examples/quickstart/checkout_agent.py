"""The landing-page quickstart: a step that places an order, and a crash on top of it.

    python checkout_agent.py                  # places the order, then the pod dies
    python checkout_agent.py <session-id>     # resumes -- does NOT place it again
    cat orders.log                            # one line for that session, both times

The ledger is append-only and every line carries its session id, because the
engine outlives any single run. A ledger that reset itself would disagree with
the engine the moment you resumed an older session, and then it could no longer
adjudicate anything.

The counter below lives INSIDE the tool body, so this script can only report
what actually happened rather than what it hopes happened.
"""

import os
import sys
import uuid
from pathlib import Path

from cellaflow import step, tool, workflow

LEDGER = Path("orders.log")

#: Every example honours this, so any of them can be pointed at an engine
#: somewhere else.
ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")

#: Incremented inside the tool body. Stays 0 when the step is replayed, because
#: a replayed step never enters its body.
EXECUTED = {"orders": 0}

#: Set before the workflow runs so the ledger line can name its session.
SESSION = {"id": ""}


def orders_for(session_id: str) -> list[str]:
    if not LEDGER.exists():
        return []
    return [
        line
        for line in LEDGER.read_text().splitlines()
        if line.startswith(session_id)
    ]


@tool(tool_name="place_order")
def place_order(order_id: str, total_cents: int) -> dict:
    """The irreversible one: the shop takes no idempotency key. Leased."""
    EXECUTED["orders"] += 1
    confirmation = f"ord_{uuid.uuid4().hex[:10]}"
    with LEDGER.open("a") as fh:
        fh.write(f"{SESSION['id']} {confirmation} {order_id} {total_cents}\n")
    print(f"   🛒 PLACED {order_id} for {total_cents} -> {confirmation}")
    return {"confirmation": confirmation, "total_cents": total_cents}


@step
def build_receipt(order: dict, order_id: str) -> dict:
    print("   🧾 Building receipt...")
    return {"order_id": order_id, "confirmation": order["confirmation"]}


@workflow(version="1.0.0", target=ENGINE)
def checkout(order_id: str, die_after_ordering: bool = False) -> dict:
    order = place_order(order_id, 2499)

    if die_after_ordering:
        # The order is placed and recorded; the receipt is not built yet.
        print("   💥 pod died")
        sys.stdout.flush()
        os._exit(17)

    return build_receipt(order, order_id)


if __name__ == "__main__":
    resuming = len(sys.argv) > 1
    session_id = sys.argv[1] if resuming else str(uuid.uuid4())

    SESSION["id"] = session_id

    if resuming:
        print(f"\n♻️  Resuming session {session_id}\n")
    else:
        print(f"\n▶️  Session {session_id}")
        print("   (save that id -- you need it to resume)\n")

    result = checkout(
        "ORD-1001", die_after_ordering=not resuming, _session_id=session_id
    )

    mine = orders_for(session_id)
    total = len(LEDGER.read_text().splitlines()) if LEDGER.exists() else 0
    print(f"\n✅ {result}")

    if resuming and EXECUTED["orders"] == 0:
        print("\n   place_order did NOT run -- replayed from the durable log.")
        if not mine:
            print(
                "   (No ledger line here for that session: it was placed by a run"
                "\n    in another directory. The engine still has the record, which"
                "\n    is why the confirmation above is the original one.)"
            )
    elif resuming:
        print(
            "\n   ⚠️  place_order DID run. That session id had no history on this"
            "\n       engine, so this started a new run rather than resuming one."
            "\n       Use the id printed by your own first run."
        )

    print(f"   this session: {len(mine)} order(s)")
    print(f"   orders.log:   {total} line(s), every run in this directory\n")
