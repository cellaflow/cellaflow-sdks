#!/usr/bin/env python3
"""Run the same refund ticket through an agent swarm, twice.

    python run_demo.py                 # 5 agents, both scenarios
    python run_demo.py --agents 8
    python run_demo.py --scenario coordinated

Scenario 1 has no coordination and charges the customer once per replica.
Scenario 2 routes the same work through CellaFlow and charges exactly once.

The verdict is read off the gateway's append-only ledger file, not off anything
the agents report about themselves.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import sys
import time
import uuid
from typing import Any, Callable, Dict, List

import gateway
import swarm

TICKET_ID = "TICKET-4417"
AMOUNT_CENTS = 24_99


def _worker(fn: Callable[..., Dict[str, Any]], args: tuple, queue: Any) -> None:
    queue.put(fn(*args))


def _committed_steps(session_id: str) -> int:
    """How many steps this session already has on disk.

    Used to decide what the run *should* do: a session the engine has never seen
    must produce exactly one charge, whereas one that already recorded the refund
    must produce none. The engine answers NOT_FOUND for an unknown session.
    """
    import grpc
    from cellaflow.client import CellaflowClient

    client = CellaflowClient(target=swarm.ENGINE_TARGET)
    try:
        steps, _ = client.get_graph(session_id)
        return len(steps)
    except grpc.RpcError as exc:
        if exc.code() == grpc.StatusCode.NOT_FOUND:
            return 0
        raise
    finally:
        client.close()


def _run_swarm(
    fn: Callable[..., Dict[str, Any]], per_agent_args: List[tuple]
) -> List[Dict[str, Any]]:
    ctx = mp.get_context("spawn")
    queue: Any = ctx.Queue()
    barrier = ctx.Barrier(len(per_agent_args))

    procs = [
        ctx.Process(target=_worker, args=(fn, args + (barrier,), queue))
        for args in per_agent_args
    ]
    for p in procs:
        p.start()

    results = [queue.get(timeout=120) for _ in procs]
    for p in procs:
        p.join(timeout=30)
    return sorted(results, key=lambda r: r["agent_id"])


def _print_results(title: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # The ledger was reset immediately before this run, so anything in it was
    # charged by this run. That -- not what an agent says about itself -- decides
    # who actually moved money.
    charges = gateway.read_ledger()
    charged_now = {(c["confirmation_id"], c["charged_by"]) for c in charges}

    print(f"\n  {title}")
    print(f"  {'-' * 68}")
    print(f"  {'agent':<10} {'outcome':<26} {'confirmation':<20} {'secs':>6}")
    for r in results:
        if not r["ok"]:
            outcome, conf = "FAILED", r["error"][:18]
        else:
            conf = r["confirmation_id"]
            mine = (conf, r["agent_id"]) in charged_now
            outcome = "charged the customer" if mine else "reused cached charge"
        print(f"  {r['agent_id']:<10} {outcome:<26} {conf:<20} {r['elapsed_s']:>6}")

    print(f"  {'-' * 68}")
    print(f"  REAL CHARGES ON THE LEDGER: {len(charges)}")
    total = sum(c["amount_cents"] for c in charges)
    print(f"  Customer was billed:        ${total / 100:,.2f}")
    return charges


def scenario_naive(n_agents: int) -> int:
    print("\n" + "=" * 72)
    print(f"  SCENARIO 1 - {n_agents} agents, no coordination")
    print("=" * 72)
    print(
        "\n  Every replica picked up the same ticket and refunded it directly.\n"
        "  Nothing tells any of them the others exist."
    )

    gateway.reset_ledger()
    args = [(f"agent-{i}", TICKET_ID, AMOUNT_CENTS) for i in range(1, n_agents + 1)]
    results = _run_swarm(swarm.run_naive_agent, args)
    charges = _print_results("Result", results)
    return len(charges)


def scenario_coordinated(
    n_agents: int, session_id: str | None = None
) -> tuple[int, int]:
    """Returns (charges made, charges expected)."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 2 - {n_agents} agents, coordinated through CellaFlow")
    print("=" * 72)
    session_id = session_id or f"refund-{TICKET_ID}-{uuid.uuid4().hex[:8]}"
    # Resumed is a property of the engine's state, not of whether the caller
    # passed an id -- a supplied id may well be brand new.
    resumed = _committed_steps(session_id) > 0
    if resumed:
        print(
            "\n  Resuming an existing session. The refund already happened, so no\n"
            "  agent should reach the gateway at all -- the engine answers from\n"
            f"  durable state.\n\n  session_id = {session_id}"
        )
    else:
        print(
            "\n  Identical agent code. The only change: the refund is a @tool, and\n"
            f"  every replica joins the same session.\n\n  session_id = {session_id}"
        )

    gateway.reset_ledger()
    # Every replica gets byte-identical arguments. That is what makes them
    # converge: the derived idempotency key hashes the step inputs, so identical
    # inputs produce one shared key and the engine can pick a single winner.
    args = [
        (f"agent-{i}", session_id, TICKET_ID, AMOUNT_CENTS)
        for i in range(1, n_agents + 1)
    ]
    results = _run_swarm(swarm.run_coordinated_agent, args)
    charges = _print_results("Result", results)

    confirmations = {r["confirmation_id"] for r in results if r["ok"]}
    if len(confirmations) == 1:
        print(
            f"  All {sum(1 for r in results if r['ok'])} agents returned the same "
            f"confirmation id."
        )
    return len(charges), (0 if resumed else 1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agents", type=int, default=5, help="swarm size (default 5)")
    parser.add_argument(
        "--scenario",
        choices=("both", "naive", "coordinated"),
        default="both",
    )
    parser.add_argument(
        "--session-id",
        help=(
            "resume an existing session instead of starting a fresh one. Re-run "
            "with the same value -- optionally after restarting the engine -- to "
            "show the refund is not repeated. Implies --scenario coordinated."
        ),
    )
    args = parser.parse_args()
    if args.session_id:
        args.scenario = "coordinated"

    if args.agents < 2:
        parser.error("--agents must be at least 2; the point is contention")

    print(f"\n  engine: {swarm.ENGINE_TARGET}")
    print(f"  ledger: {gateway.ledger_path()}")

    naive_charges = None
    coordinated_charges = expected = None
    started = time.time()

    if args.scenario in ("both", "naive"):
        naive_charges = scenario_naive(args.agents)
    if args.scenario in ("both", "coordinated"):
        coordinated_charges, expected = scenario_coordinated(
            args.agents, args.session_id
        )

    print("\n" + "=" * 72)
    print("  VERDICT")
    print("=" * 72)
    if naive_charges is not None:
        print(f"  Without CellaFlow: {naive_charges} charges for 1 refund request")
    if coordinated_charges is not None:
        noun = "charge" if coordinated_charges == 1 else "charges"
        suffix = " (already refunded before this run)" if expected == 0 else ""
        print(
            f"  With CellaFlow:    {coordinated_charges} {noun} for 1 refund "
            f"request{suffix}"
        )
    print(f"\n  ({time.time() - started:.1f}s total)\n")

    if coordinated_charges is not None and coordinated_charges != expected:
        # Fresh session must charge exactly once; an already-refunded one must
        # not reach the gateway at all.
        print(
            f"  UNEXPECTED: coordinated run produced {coordinated_charges} charges, "
            f"expected exactly {expected}.\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
