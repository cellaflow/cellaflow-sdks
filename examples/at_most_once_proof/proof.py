#!/usr/bin/env python3
"""Does a leased tool call actually prevent a duplicate charge? Run it and see.

Two arms. Identical graph, identical crash, identical Postgres checkpointer.
The only difference is whether the tool call holds a lease.

    A   plain LangGraph      ordinary function, no Cellaflow
    B   the same, leased     @tool inside durable_tools

A two-node booking agent reserves a seat, then charges a card, then the process
is killed the instant after the charge lands and before the checkpoint recording
it is written -- the worst moment for a crash, because the money has moved and
nothing durable says so. A second run resumes the thread from cold.

Every arm asserts its own expected count, so `--arm b` on its own still fails
if the lease stops working. A proof that cannot fail is not a proof.

    docker compose up -d
    pip install -r requirements.txt
    python proof.py
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import os
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, ContextManager, Dict, Iterator, List

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

# What each arm must produce, asserted per arm rather than only on the pair.
# Checking only the pair meant `--arm b` returned 0 whatever it counted, which
# is a permanent green for anyone who wires a single arm into CI.
EXPECTED_A = 2  # the charge repeats: the checkpointer never saw it happen
EXPECTED_B = 1  # the resumed call is answered from the lease's record


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


# The one thing the two arms vary: a context manager taking the LangGraph
# config. Arm A's does nothing. Arm B's opens a Cellaflow session, and every
# @tool called while it is open takes a lease.
LeaseScope = Callable[[Dict[str, Any]], ContextManager[Any]]


@contextlib.contextmanager
def no_lease(_config: Dict[str, Any]) -> Iterator[None]:
    """Arm A's stand-in for durable_tools: does nothing at all."""
    yield


def run_arm(
    label: str,
    reserve: Callable[[str], Dict[str, Any]],
    charge: Callable[[str, int], Dict[str, Any]],
    scope: LeaseScope,
) -> int:
    """Runs one arm twice and returns how many times the card was charged.

    Run 1 is killed the instant after the charge lands. Run 2 resumes the same
    thread from a fresh saver, the way a restarted pod would.

    `scope` is the only argument the arms differ on: `no_lease` for A, and
    `durable_tools` bound to the engine for B. `reserve` and `charge` are the
    same two functions in both arms, handed to B through `@tool`, so neither
    arm can be doing different work from the other.
    """
    SEATS.clear()
    CHARGES.clear()

    # Flipped to False before run 2. `node_pay` reads it on every call rather
    # than capturing it, so the assignment further down is what turns the crash
    # off for the resume.
    die_after_charge = True

    def node_reserve(state: Booking) -> Dict[str, Any]:
        return {"seat": reserve(state.booking)}

    def node_pay(state: Booking) -> Dict[str, Any]:
        receipt = charge(state.booking, 2499)
        if die_after_charge:
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
            with scope(config):
                build(cp).invoke(Booking(booking="BK-1"), config)
        except RuntimeError:
            print("      run 1: charged, then the process died")

    # A genuinely fresh saver, as a restarted pod would have.
    die_after_charge = False
    with PostgresSaver.from_conn_string(DB_URI) as cp:
        with scope(config):
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
            "ARM A  -  PostgresSaver only, no Cellaflow",
            reserve_seat, charge_card, no_lease,
        )
    if args.arm in ("b", "both"):
        leased_reserve = tool(tool_name="reserve_seat")(reserve_seat)
        leased_charge = tool(tool_name="charge_card")(charge_card)
        # Bound to ENGINE, not left on the default: the line above prints the
        # engine this proof claims to be using, and a proof that reports one
        # address while talking to another is not evidence of anything.
        leased = functools.partial(durable_tools, target=ENGINE)
        b = run_arm(
            "ARM B  -  identical graph, leased with durable_tools",
            leased_reserve, leased_charge, leased,
        )

    print("\n  " + "=" * 58)
    if a is not None:
        print(f"  Arm A   PostgresSaver alone .............. {a} charge(s)")
    if b is not None:
        print(f"  Arm B   + Cellaflow leased tools ......... {b} charge(s)")
    print("  " + "=" * 58)

    missed = []
    if a is not None and a != EXPECTED_A:
        missed.append(f"arm A expected {EXPECTED_A} charge(s), counted {a}")
    if b is not None and b != EXPECTED_B:
        missed.append(f"arm B expected {EXPECTED_B} charge(s), counted {b}")

    if missed:
        print("\n  DID NOT REPRODUCE:")
        for line in missed:
            print(f"    {line}")
        print()
        return 1

    if args.arm == "both":
        print("\n  The checkpointer worked correctly in both arms -- one seat")
        print("  reserved each time, because the committed node was skipped on")
        print("  resume. Every duplicate came from the pending node's side")
        print("  effect, which is the part a checkpointer does not cover.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
