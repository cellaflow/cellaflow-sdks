#!/usr/bin/env python3
"""Does a leased tool call actually prevent a duplicate booking? Run it and see.

Two arms. Identical graph, identical crash, identical Postgres checkpointer.
The only difference is whether the tool call holds a lease.

    A   plain LangGraph      ordinary function, no Cellaflow
    B   the same, leased     @tool inside durable_tools

A two-node booking agent holds a seat in local inventory, then confirms that
booking with the supplier, then the process is killed the instant after the
confirmation lands and before the checkpoint recording it is written -- the
worst moment for a crash, because the supplier has the booking and nothing
durable says so. A second run resumes the thread from cold.

The confirmation is the interesting call because there is no header that would
have saved you. A payment API would hand you an idempotency key; a seat
inventory, a partner endpoint or another team's service will not.

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
HELD: List[str] = []
CONFIRMED: List[str] = []

# What each arm must produce, asserted per arm rather than only on the pair.
# Checking only the pair meant `--arm b` returned 0 whatever it counted, which
# is a permanent green for anyone who wires a single arm into CI.
EXPECTED_A = 2  # the confirmation repeats: the checkpointer never saw it happen
EXPECTED_B = 1  # the resumed call is answered from the lease's record


@dataclass
class Booking:
    booking: str = ""
    seat: Dict[str, Any] = field(default_factory=dict)
    receipt: Dict[str, Any] = field(default_factory=dict)


def hold_seat(booking: str) -> Dict[str, Any]:
    HELD.append(booking)
    print(f"        >>> SEAT HELD            {booking}")
    return {"kind": "hold", "booking": booking}


def confirm_booking(booking: str, seat: str) -> Dict[str, Any]:
    CONFIRMED.append(booking)
    print(f"        >>> BOOKING CONFIRMED    {seat} for {booking}")
    return {"kind": "confirmation", "booking": booking, "seat": seat}


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
    hold: Callable[[str], Dict[str, Any]],
    confirm: Callable[[str, str], Dict[str, Any]],
    scope: LeaseScope,
) -> int:
    """Runs one arm twice and returns how many times the booking was confirmed.

    Run 1 is killed the instant after the confirmation lands. Run 2 resumes the
    same thread from a fresh saver, the way a restarted pod would.

    `scope` is the only argument the arms differ on: `no_lease` for A, and
    `durable_tools` bound to the engine for B. `hold` and `confirm` are the
    same two functions in both arms, handed to B through `@tool`, so neither
    arm can be doing different work from the other.
    """
    HELD.clear()
    CONFIRMED.clear()

    # Flipped to False before run 2. `node_confirm` reads it on every call rather
    # than capturing it, so the assignment further down is what turns the crash
    # off for the resume.
    die_after_confirm = True

    def node_hold(state: Booking) -> Dict[str, Any]:
        return {"seat": hold(state.booking)}

    def node_confirm(state: Booking) -> Dict[str, Any]:
        receipt = confirm(state.booking, "14A")
        if die_after_confirm:
            # The supplier has the booking. This run dies without recording it.
            raise RuntimeError("pod died after confirming, before the checkpoint")
        return {"receipt": receipt}

    def build(checkpointer: Any) -> Any:
        graph = StateGraph(Booking)
        graph.add_node("hold", node_hold)
        graph.add_node("confirm", node_confirm)
        graph.add_edge(START, "hold")
        graph.add_edge("hold", "confirm")
        graph.add_edge("confirm", END)
        return graph.compile(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": f"proof-{uuid.uuid4().hex[:10]}"}}
    print(f"\n  {label}")

    with PostgresSaver.from_conn_string(DB_URI) as cp:
        cp.setup()
        try:
            with scope(config):
                build(cp).invoke(Booking(booking="BK-1"), config)
        except RuntimeError:
            print("      run 1: confirmed, then the process died")

    # A genuinely fresh saver, as a restarted pod would have.
    die_after_confirm = False
    with PostgresSaver.from_conn_string(DB_URI) as cp:
        with scope(config):
            build(cp).invoke(None, config)      # None resumes; input restarts
    print("      run 2: resumed from cold and completed")

    print(f"      supplier hits -> held={len(HELD)} confirmed={len(CONFIRMED)}")
    return len(CONFIRMED)


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
            hold_seat, confirm_booking, no_lease,
        )
    if args.arm in ("b", "both"):
        leased_hold = tool(tool_name="hold_seat")(hold_seat)
        leased_confirm = tool(tool_name="confirm_booking")(confirm_booking)
        # Bound to ENGINE, not left on the default: the line above prints the
        # engine this proof claims to be using, and a proof that reports one
        # address while talking to another is not evidence of anything.
        leased = functools.partial(durable_tools, target=ENGINE)
        b = run_arm(
            "ARM B  -  identical graph, leased with durable_tools",
            leased_hold, leased_confirm, leased,
        )

    print("\n  " + "=" * 58)
    if a is not None:
        print(f"  Arm A   PostgresSaver alone .............. {a} confirmation(s)")
    if b is not None:
        print(f"  Arm B   + Cellaflow leased tools ......... {b} confirmation(s)")
    print("  " + "=" * 58)

    missed = []
    if a is not None and a != EXPECTED_A:
        missed.append(f"arm A expected {EXPECTED_A} confirmation(s), counted {a}")
    if b is not None and b != EXPECTED_B:
        missed.append(f"arm B expected {EXPECTED_B} confirmation(s), counted {b}")

    if missed:
        print("\n  DID NOT REPRODUCE:")
        for line in missed:
            print(f"    {line}")
        print()
        return 1

    if args.arm == "both":
        print("\n  The checkpointer worked correctly in both arms -- one seat")
        print("  held each time, because the committed node was skipped on")
        print("  resume. Every duplicate came from the pending node's side")
        print("  effect, which is the part a checkpointer does not cover.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
