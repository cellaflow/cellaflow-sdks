#!/usr/bin/env python3
"""Does a leased tool call actually prevent a duplicate charge? Run it and see.

Two arms. Identical graph, identical crash, identical Postgres checkpointer.
The only difference is whether the tool call holds a lease.

    A   plain LangGraph      ordinary function, no CellaFlow
    B   the same, leased     @tool inside durable_tools

A two-node booking agent reserves a seat, then charges a card, then the process
is killed the instant after the charge lands and before the checkpoint recording
it is written -- the worst moment for a crash, because the money has moved and
nothing durable says so. A second run resumes the thread from cold.

Exits non-zero unless arm A charges twice and arm B charges once. A proof that
cannot fail is not a proof.

    docker compose up -d
    pip install -r requirements.txt
    python proof.py
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph

from cellaflow import durable_tools, tool

DB_URI = os.environ.get(
    "PROOF_DB_URI",
    "postgresql://postgres:proof@localhost:55433/proof?sslmode=disable",
)
ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")

# Every real gateway hit appends here. Counters live INSIDE the tool bodies, so
# they count executions and never cache hits -- if the lease works, the body is
# not entered and nothing is appended.
SEATS: List[str] = []
CHARGES: List[str] = []
_DIE = {"now": True}


@dataclass
class Booking:
    booking: str = ""
    seat: Dict[str, Any] = field(default_factory=dict)
    receipt: Dict[str, Any] = field(default_factory=dict)


def reserve_seat(booking: str) -> Dict[str, Any]:
    SEATS.append(booking)
    print(f"        >>> SEAT RESERVED   {booking}")
    return {"kind": "seat", "booking": booking}


def charge_card(booking: str, cents: int) -> Dict[str, Any]:
    CHARGES.append(booking)
    print(f"        >>> CARD CHARGED    {cents} for {booking}")
    return {"kind": "charge", "booking": booking, "cents": cents}


@contextlib.contextmanager
def no_lease(_config: Dict[str, Any]) -> Iterator[None]:
    """Arm A's stand-in for durable_tools: does nothing at all."""
    yield


def run_arm(
    label: str,
    reserve: Callable[[str], Dict[str, Any]],
    charge: Callable[[str, int], Dict[str, Any]],
    wrap: Callable[..., Any],
) -> int:
    SEATS.clear()
    CHARGES.clear()
    _DIE["now"] = True

    def node_reserve(state: Booking) -> Dict[str, Any]:
        return {"seat": reserve(state.booking)}

    def node_pay(state: Booking) -> Dict[str, Any]:
        receipt = charge(state.booking, 2499)
        if _DIE["now"]:
            # The money has moved. This run now dies without recording it.
            raise RuntimeError("pod died after charging, before the checkpoint")
        return {"receipt": receipt}

    def build(checkpointer: Any) -> Any:
        graph = StateGraph(Booking)
        graph.add_node("reserve", node_reserve)
        graph.add_node("pay", node_pay)
        graph.add_edge(START, "reserve")
        graph.add_edge("reserve", "pay")
        graph.add_edge("pay", END)
        return graph.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": f"proof-{uuid.uuid4().hex[:10]}"}}
    print(f"\n  {label}")

    with PostgresSaver.from_conn_string(DB_URI) as cp:
        cp.setup()
        try:
            with wrap(config):
                build(cp).invoke(Booking(booking="BK-1"), config)
        except RuntimeError:
            print("      run 1: charged, then the process died")

    # A genuinely fresh saver, as a restarted pod would have.
    _DIE["now"] = False
    with PostgresSaver.from_conn_string(DB_URI) as cp:
        with wrap(config):
            build(cp).invoke(None, config)      # None resumes; input restarts
    print("      run 2: resumed from cold and completed")

    print(f"      gateway hits -> seats={len(SEATS)} charges={len(CHARGES)}")
    return len(CHARGES)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--arm", choices=("a", "b", "both"), default="both",
        help="run one arm only (default: both)",
    )
    args = parser.parse_args()

    print(f"\n  postgres: {DB_URI.rsplit('@', 1)[-1]}")
    print(f"  engine:   {ENGINE}")

    a = b = None
    if args.arm in ("a", "both"):
        a = run_arm(
            "ARM A  -  PostgresSaver only, no CellaFlow",
            reserve_seat, charge_card, no_lease,
        )
    if args.arm in ("b", "both"):
        leased_reserve = tool(tool_name="reserve_seat")(reserve_seat)
        leased_charge = tool(tool_name="charge_card")(charge_card)
        b = run_arm(
            "ARM B  -  identical graph, leased with durable_tools",
            leased_reserve, leased_charge, durable_tools,
        )

    print("\n  " + "=" * 58)
    if a is not None:
        print(f"  Arm A   PostgresSaver alone .............. {a} charge(s)")
    if b is not None:
        print(f"  Arm B   + CellaFlow leased tools ......... {b} charge(s)")
    print("  " + "=" * 58)

    if args.arm != "both":
        return 0

    if a == 2 and b == 1:
        print("\n  The checkpointer worked correctly in both arms -- one seat")
        print("  reserved each time, because the committed node was skipped on")
        print("  resume. Every duplicate came from the pending node's side")
        print("  effect, which is the part a checkpointer does not cover.\n")
        return 0

    print(f"\n  DID NOT REPRODUCE: expected A=2 B=1, got A={a} B={b}\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
