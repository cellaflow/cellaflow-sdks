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
import os
import sys
import time
import uuid
from typing import Any, Callable, Dict, List

import gateway
import langgraph_agent
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


def scenario_divergent(n_agents: int) -> tuple[int, int]:
    """Replicas that disagree about the amount. Returns (charges, expected)."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 3 - {n_agents} agents that disagree about the amount")
    print("=" * 72)
    print(
        "\n  Same ticket, same session -- but each replica reasoned its way to a\n"
        "  different refund amount, as agents reading the same ticket do. Different\n"
        "  arguments mean different idempotency keys, so nothing about the operation\n"
        "  itself makes them contend. Only the graph position they both target does."
    )

    gateway.reset_ledger()
    session_id = f"divergent-{TICKET_ID}-{uuid.uuid4().hex[:8]}"
    # Deliberately spread: 24.99, 25.00, 25.01, ... Every replica is confident.
    args = [
        (f"agent-{i}", session_id, TICKET_ID, AMOUNT_CENTS + (i - 1))
        for i in range(1, n_agents + 1)
    ]
    results = _run_swarm(swarm.run_divergent_agent, args)

    charges = gateway.read_ledger()
    print(f"\n  Result")
    print(f"  {'-' * 68}")
    print(f"  {'agent':<10} {'proposed':>9}  {'outcome':<38}")
    for r in results:
        if r.get("refused"):
            outcome = "REFUSED before reaching the gateway"
        elif r["ok"]:
            outcome = "charged the customer"
        else:
            outcome = f"FAILED: {r['error'][:34]}"
        print(f"  {r['agent_id']:<10} {r['amount_cents'] / 100:>8.2f}  {outcome:<38}")

    print(f"  {'-' * 68}")
    print(f"  REAL CHARGES ON THE LEDGER: {len(charges)}")
    print(f"  Customer was billed:        ${sum(c['amount_cents'] for c in charges) / 100:,.2f}")
    refused = sum(1 for r in results if r.get("refused"))
    print(
        f"\n  {refused} of {n_agents} were stopped *before* charging. Without this they\n"
        f"  would each have charged, and only then been told the position was taken."
    )
    return len(charges), 1


def scenario_heterogeneous(n_agents: int) -> tuple[int, int]:
    """Different agents, different sessions, one shared refund."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 4 - {n_agents} different agents, {n_agents} separate sessions")
    print("=" * 72)
    print(
        "\n  Not replicas this time. A support agent, a fraud detector and a billing\n"
        "  reconciler each concluded independently that this order needs refunding.\n"
        "  Different workflows, different versions, no shared session -- so there is\n"
        "  no shared graph position to arbitrate.\n\n"
        "  What they share is a coordination domain they each name."
    )

    gateway.reset_ledger()
    coordination_id = f"refund-{TICKET_ID}-{uuid.uuid4().hex[:6]}"
    roles = [
        ("support-agent", "handle_support_ticket", "1.0.0"),
        ("fraud-detector", "review_flagged_order", "2.4.1"),
        ("billing-recon", "reconcile_billing_run", "3.0.0"),
        ("csat-followup", "close_the_loop", "1.2.0"),
        ("ops-sweeper", "sweep_stuck_orders", "0.9.0"),
    ]
    args = [
        (name, wf, ver, coordination_id, TICKET_ID, AMOUNT_CENTS)
        for name, wf, ver in roles[:n_agents]
    ]
    results = _run_swarm(swarm.run_shared_agent, args)

    charges = gateway.read_ledger()
    charged_now = {(c["confirmation_id"], c["charged_by"]) for c in charges}

    print(f"\n  coordination_id = {coordination_id}\n")
    print(f"  {'agent':<16} {'workflow':<24} {'ver':<7} {'outcome':<22}")
    print(f"  {'-' * 72}")
    for r in results:
        role = next((x for x in roles if x[0] == r["agent_id"]), None)
        wf, ver = (role[1], role[2]) if role else ("?", "?")
        if not r["ok"]:
            outcome = f"FAILED: {r['error'][:14]}"
        else:
            mine = (r["confirmation_id"], r["agent_id"]) in charged_now
            outcome = "charged the customer" if mine else "reused shared charge"
        print(f"  {r['agent_id']:<16} {wf:<24} {ver:<7} {outcome:<22}")

    print(f"  {'-' * 72}")
    print(f"  REAL CHARGES ON THE LEDGER: {len(charges)}")
    succeeded = [r for r in results if r["ok"]]
    confirmations = {r["confirmation_id"] for r in succeeded}
    if len(confirmations) == 1:
        print(f"  All {len(succeeded)} agents hold the same confirmation id.")
    return len(charges), 1


def scenario_langgraph() -> tuple[int, int]:
    """A LangGraph node charges the customer, dies, and resumes."""
    print("\n" + "=" * 72)
    print("  SCENARIO 5 - a LangGraph node that crashes after moving money")
    print("=" * 72)
    print(
        "\n  The refund now happens inside a LangGraph node, which is where a real\n"
        "  agent's side effects live. The first process is killed the instant after\n"
        "  the gateway is charged and before the checkpoint recording it lands --\n"
        "  so the money has moved and nothing durable says so.\n\n"
        "  A second process then resumes the same thread from cold. LangGraph\n"
        "  re-runs the pending node, so the tool is reached again and has to\n"
        "  decline."
    )

    gateway.reset_ledger()
    thread_id = f"lg-{TICKET_ID}-{uuid.uuid4().hex[:6]}"
    ctx = mp.get_context("spawn")

    # Phase 1: crash after the charge. Runs in its own process because it exits
    # hard -- there is no returning from it.
    crash_env = dict(os.environ, **{langgraph_agent.CRASH_ENV: "1"})
    proc = ctx.Process(
        target=_crash_worker,
        args=(thread_id, TICKET_ID, AMOUNT_CENTS, "lg-agent", crash_env),
    )
    proc.start()
    proc.join(timeout=120)

    after_crash = len(gateway.read_ledger())
    print(f"\n  phase 1: process exited with {proc.exitcode}, "
          f"ledger has {after_crash} charge(s)")

    # Phase 2: a cold process resumes the thread.
    queue: Any = ctx.Queue()
    resume = ctx.Process(
        target=_worker,
        args=(langgraph_agent.run_phase,
              (thread_id, TICKET_ID, AMOUNT_CENTS, "lg-agent"), queue),
    )
    resume.start()
    result = queue.get(timeout=120)
    resume.join(timeout=30)

    charges = gateway.read_ledger()
    print(f"  phase 2: resumed and completed, ledger has {len(charges)} charge(s)")
    print(f"\n  {'-' * 68}")
    print(f"  thread:              {thread_id}")
    print(f"  confirmation:        {result['confirmation_id']}")
    print(f"  graph reached:       {' -> '.join(result['notes']) or '(none)'}")
    print(f"  {'-' * 68}")
    print(f"  REAL CHARGES ON THE LEDGER: {len(charges)}")
    if len(charges) == 1 and after_crash == 1:
        print("  The node ran twice. The customer was charged once.")
    return len(charges), 1


def _crash_worker(
    thread_id: str, ticket_id: str, amount: int, agent_id: str, env: Dict[str, str]
) -> None:
    """Runs phase 1 with the crash flag set, in a process expected to die."""
    os.environ.update(env)
    try:
        langgraph_agent.run_phase(thread_id, ticket_id, amount, agent_id)
    except SystemExit:
        raise
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agents", type=int, default=5, help="swarm size (default 5)")
    parser.add_argument(
        "--scenario",
        choices=(
            "both", "naive", "coordinated", "divergent",
            "heterogeneous", "langgraph", "all",
        ),
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
    if args.scenario in ("heterogeneous", "all") and args.agents > 5:
        parser.error(
            "--scenario heterogeneous defines 5 distinct agent roles; "
            "use --agents 5 or fewer"
        )

    print(f"\n  engine: {swarm.ENGINE_TARGET}")
    print(f"  ledger: {gateway.ledger_path()}")

    naive_charges = None
    coordinated_charges = expected = None
    started = time.time()

    if args.scenario in ("both", "all", "naive"):
        naive_charges = scenario_naive(args.agents)
    if args.scenario in ("both", "all", "coordinated"):
        coordinated_charges, expected = scenario_coordinated(
            args.agents, args.session_id
        )

    divergent_charges = heterogeneous_charges = langgraph_charges = None
    if args.scenario in ("all", "divergent"):
        divergent_charges, _ = scenario_divergent(args.agents)
    if args.scenario in ("all", "heterogeneous"):
        heterogeneous_charges, _ = scenario_heterogeneous(args.agents)
    if args.scenario in ("all", "langgraph"):
        langgraph_charges, _ = scenario_langgraph()

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
    if divergent_charges is not None:
        print(
            f"  Replicas that disagreed: {divergent_charges} charge for "
            f"{args.agents} proposed amounts"
        )
    if heterogeneous_charges is not None:
        print(
            f"  {args.agents} unrelated agents:  {heterogeneous_charges} charge across "
            f"{args.agents} separate sessions"
        )
    if langgraph_charges is not None:
        print(
            f"  LangGraph node:    {langgraph_charges} charge, across a crash "
            f"and a resume"
        )
    print(f"\n  ({time.time() - started:.1f}s total)\n")

    for label, got in (("divergent", divergent_charges),
                       ("heterogeneous", heterogeneous_charges),
                       ("langgraph", langgraph_charges)):
        if got is not None and got != 1:
            print(f"  UNEXPECTED: {label} scenario produced {got} charges, expected 1.\n")
            return 1

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
