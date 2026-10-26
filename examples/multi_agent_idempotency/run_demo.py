#!/usr/bin/env python3
"""Send the same customer order through an agent swarm, twice.

    python run_demo.py                 # 5 agents, both scenarios
    python run_demo.py --agents 8
    python run_demo.py --scenario coordinated

Scenario 1 has no coordination and places the order once per replica.
Scenario 2 routes the same work through Cellaflow, and one order reaches the
shop.

The verdict is read off the shop's append-only ledger file, not off anything
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

import shop
import swarm

ORDER_ID = "ORDER-4417"
TOTAL_CENTS = 24_99


def _worker(fn: Callable[..., Dict[str, Any]], args: tuple, queue: Any) -> None:
    queue.put(fn(*args))


def _committed_steps(session_id: str) -> int:
    """How many steps this session already has on disk.

    Used to decide what the run *should* do: a session the engine has never seen
    must produce exactly one order, whereas one that already recorded the order
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
    # placed by this run. That -- not what an agent says about itself -- decides
    # who actually placed an order.
    orders = shop.read_ledger()
    placed_now = {(c["confirmation_id"], c["placed_by"]) for c in orders}

    print(f"\n  {title}")
    print(f"  {'-' * 68}")
    print(f"  {'agent':<10} {'outcome':<26} {'confirmation':<20} {'secs':>6}")
    for r in results:
        if not r["ok"]:
            outcome, conf = "FAILED", r["error"][:18]
        else:
            conf = r["confirmation_id"]
            mine = (conf, r["agent_id"]) in placed_now
            outcome = "placed the order" if mine else "reused the placed order"
        print(f"  {r['agent_id']:<10} {outcome:<26} {conf:<20} {r['elapsed_s']:>6}")

    print(f"  {'-' * 68}")
    print(f"  REAL ORDERS AT THE SHOP:    {len(orders)}")
    total = sum(c["total_cents"] for c in orders)
    print(f"  Customer was billed:        ${total / 100:,.2f}")
    return orders


def scenario_naive(n_agents: int) -> int:
    print("\n" + "=" * 72)
    print(f"  SCENARIO 1 - {n_agents} agents, no coordination")
    print("=" * 72)
    print(
        "\n  Every replica picked up the same order and placed it directly.\n"
        "  Nothing tells any of them the others exist."
    )

    shop.reset_ledger()
    args = [(f"agent-{i}", ORDER_ID, TOTAL_CENTS) for i in range(1, n_agents + 1)]
    results = _run_swarm(swarm.run_naive_agent, args)
    orders = _print_results("Result", results)
    return len(orders)


def scenario_coordinated(
    n_agents: int, session_id: str | None = None
) -> tuple[int, int]:
    """Returns (orders placed, orders expected)."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 2 - {n_agents} agents, coordinated through Cellaflow")
    print("=" * 72)
    session_id = session_id or f"{ORDER_ID.lower()}-{uuid.uuid4().hex[:8]}"
    # Resumed is a property of the engine's state, not of whether the caller
    # passed an id -- a supplied id may well be brand new.
    resumed = _committed_steps(session_id) > 0
    if resumed:
        print(
            "\n  Resuming an existing session. The order was already placed, so no\n"
            "  agent should reach the shop at all -- the engine answers from\n"
            f"  durable state.\n\n  session_id = {session_id}"
        )
    else:
        print(
            "\n  Identical agent code. The only change: placing the order is a @tool, and\n"
            f"  every replica joins the same session.\n\n  session_id = {session_id}"
        )

    shop.reset_ledger()
    # Every replica gets byte-identical arguments. That is what makes them
    # converge: the derived idempotency key hashes the step inputs, so identical
    # inputs produce one shared key and the engine can pick a single winner.
    args = [
        (f"agent-{i}", session_id, ORDER_ID, TOTAL_CENTS)
        for i in range(1, n_agents + 1)
    ]
    results = _run_swarm(swarm.run_coordinated_agent, args)
    orders = _print_results("Result", results)

    confirmations = {r["confirmation_id"] for r in results if r["ok"]}
    if len(confirmations) == 1:
        print(
            f"  All {sum(1 for r in results if r['ok'])} agents returned the same "
            f"confirmation id."
        )
    return len(orders), (0 if resumed else 1)


def scenario_divergent(n_agents: int) -> tuple[int, int]:
    """Replicas that disagree about the total. Returns (orders, expected)."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 3 - {n_agents} agents that disagree about the total")
    print("=" * 72)
    print(
        "\n  Same order, same session -- but each replica priced it differently\n"
        "  (shipping, tax, a coupon), as agents reading the same cart do. Different\n"
        "  arguments mean different idempotency keys, so nothing about the operation\n"
        "  itself makes them contend. Only the graph position they both target does."
    )

    shop.reset_ledger()
    session_id = f"divergent-{ORDER_ID}-{uuid.uuid4().hex[:8]}"
    # Deliberately spread: 24.99, 25.00, 25.01, ... Every replica is confident.
    args = [
        (f"agent-{i}", session_id, ORDER_ID, TOTAL_CENTS + (i - 1))
        for i in range(1, n_agents + 1)
    ]
    results = _run_swarm(swarm.run_divergent_agent, args)

    orders = shop.read_ledger()
    print(f"\n  Result")
    print(f"  {'-' * 68}")
    print(f"  {'agent':<10} {'proposed':>9}  {'outcome':<38}")
    for r in results:
        if r.get("refused"):
            outcome = "REFUSED before reaching the shop"
        elif r["ok"]:
            outcome = "placed the order"
        else:
            outcome = f"FAILED: {r['error'][:34]}"
        print(f"  {r['agent_id']:<10} {r['total_cents'] / 100:>8.2f}  {outcome:<38}")

    print(f"  {'-' * 68}")
    print(f"  REAL ORDERS AT THE SHOP:    {len(orders)}")
    print(f"  Customer was billed:        ${sum(c['total_cents'] for c in orders) / 100:,.2f}")
    refused = sum(1 for r in results if r.get("refused"))
    print(
        f"\n  {refused} of {n_agents} were stopped *before* placing an order. Without this\n"
        f"  they would each have placed one, and only then been told the position was taken."
    )
    return len(orders), 1


def scenario_heterogeneous(n_agents: int) -> tuple[int, int]:
    """Different agents, different sessions, one shared order."""
    print("\n" + "=" * 72)
    print(f"  SCENARIO 4 - {n_agents} different agents, {n_agents} separate sessions")
    print("=" * 72)
    print(
        "\n  Not replicas this time. A checkout agent, a cart-recovery agent and a\n"
        "  reorder bot each concluded independently that this order should be placed.\n"
        "  Different workflows, different versions, no shared session -- so there is\n"
        "  no shared graph position to arbitrate.\n\n"
        "  They also disagree about the total, because they priced it separately.\n"
        "  What they share is a coordination domain they each name, and a\n"
        "  declaration of which argument identifies the work."
    )

    shop.reset_ledger()
    coordination_id = f"{ORDER_ID.lower()}-{uuid.uuid4().hex[:6]}"
    roles = [
        ("checkout-agent", "complete_checkout", "1.0.0"),
        ("cart-recovery", "recover_abandoned_cart", "2.4.1"),
        ("reorder-bot", "run_scheduled_reorder", "3.0.0"),
        ("support-agent", "handle_support_ticket", "1.2.0"),
        ("ops-sweeper", "sweep_stuck_orders", "0.9.0"),
    ]
    # Each agent priced the order itself. Five agents that independently
    # concluded this order should be placed will not have arrived at the same
    # total -- that is what makes them independent. Handing them an identical
    # total would test a weaker version of this scenario's own premise, and it
    # is what let a real defect through: the derived key hashed every argument,
    # so divergent totals split the key and every agent placed an order.
    args = [
        (name, wf, ver, coordination_id, ORDER_ID, TOTAL_CENTS + i)
        for i, (name, wf, ver) in enumerate(roles[:n_agents])
    ]
    results = _run_swarm(swarm.run_shared_agent, args)

    orders = shop.read_ledger()
    placed_now = {(c["confirmation_id"], c["placed_by"]) for c in orders}

    print(f"\n  coordination_id = {coordination_id}\n")
    print(f"  {'agent':<16} {'workflow':<23} {'proposed':>9}  {'outcome':<22}")
    print(f"  {'-' * 75}")
    for r in results:
        role = next((x for x in roles if x[0] == r["agent_id"]), None)
        wf = role[1] if role else "?"
        proposed = next(
            (a[5] for a in args if a[0] == r["agent_id"]), TOTAL_CENTS
        )
        if not r["ok"]:
            outcome = f"FAILED: {r['error'][:14]}"
        else:
            mine = (r["confirmation_id"], r["agent_id"]) in placed_now
            outcome = "placed the order" if mine else "adopted the winner's"
        print(
            f"  {r['agent_id']:<16} {wf:<23} {proposed / 100:>8.2f}  {outcome:<22}"
        )

    print(f"  {'-' * 75}")
    print(f"  REAL ORDERS AT THE SHOP:    {len(orders)}")
    succeeded = [r for r in results if r["ok"]]
    confirmations = {r["confirmation_id"] for r in succeeded}
    if len(confirmations) == 1:
        print(f"  All {len(succeeded)} agents hold the same confirmation id.")

    # The part that is easy to leave out, and should not be. `shared_on` says
    # the order id identifies the work and the total does not -- so the agents
    # that lost are holding a confirmation for a total they never proposed.
    # That is the trade-off convergence buys, not a free win, and a demo that
    # prints only "1 order" is hiding it.
    if orders:
        won = orders[0]["total_cents"]
        adopted = [
            a[0] for a in args if a[5] != won and a[0] != orders[0]["placed_by"]
        ]
        if adopted:
            print(
                f"\n  {len(adopted)} agent(s) proposed a different total and adopted "
                f"${won / 100:,.2f}:"
            )
            print(f"    {', '.join(adopted)}")
            print(
                "  Convergence means they accept a result computed from arguments\n"
                "  they did not supply. If that is wrong for your workflow, make the\n"
                "  disputed value its own leased step so they agree before ordering."
            )
    return len(orders), 1


def scenario_langgraph() -> tuple[int, int]:
    """A LangGraph node places the order, dies, and resumes."""
    # Imported here, not at module scope: LangGraph is an optional extra and the
    # other four scenarios must still run without it installed.
    import langgraph_agent

    print("\n" + "=" * 72)
    print("  SCENARIO 5 - a LangGraph node that crashes after placing the order")
    print("=" * 72)
    print(
        "\n  The order is now placed inside a LangGraph node, which is where a real\n"
        "  agent's side effects live. The first process is killed after the leased\n"
        "  call returns and before LangGraph's checkpoint for the node lands -- so\n"
        "  the order exists, Cellaflow has recorded it, and the graph does not know.\n\n"
        "  A second process then resumes the same thread from cold. LangGraph\n"
        "  re-runs the pending node, so the tool is reached again, and the engine\n"
        "  answers it from the record instead of placing a second order."
    )

    shop.reset_ledger()
    thread_id = f"lg-{ORDER_ID}-{uuid.uuid4().hex[:6]}"
    ctx = mp.get_context("spawn")

    # Phase 1: crash after the order. Runs in its own process because it exits
    # hard -- there is no returning from it.
    crash_env = dict(os.environ, **{langgraph_agent.CRASH_ENV: "1"})
    proc = ctx.Process(
        target=_crash_worker,
        args=(thread_id, ORDER_ID, TOTAL_CENTS, "lg-agent", crash_env),
    )
    proc.start()
    proc.join(timeout=120)

    after_crash = len(shop.read_ledger())
    print(f"\n  phase 1: process exited with {proc.exitcode}, "
          f"shop has {after_crash} order(s)")

    # Phase 2: a cold process resumes the thread.
    queue: Any = ctx.Queue()
    resume = ctx.Process(
        target=_worker,
        args=(langgraph_agent.run_phase,
              (thread_id, ORDER_ID, TOTAL_CENTS, "lg-agent"), queue),
    )
    resume.start()
    result = queue.get(timeout=120)
    resume.join(timeout=30)

    orders = shop.read_ledger()
    print(f"  phase 2: resumed and completed, shop has {len(orders)} order(s)")
    print(f"\n  {'-' * 68}")
    print(f"  thread:              {thread_id}")
    print(f"  confirmation:        {result['confirmation_id']}")
    print(f"  graph reached:       {' -> '.join(result['notes']) or '(none)'}")
    print(f"  {'-' * 68}")
    print(f"  REAL ORDERS AT THE SHOP:    {len(orders)}")
    if len(orders) == 1 and after_crash == 1:
        print("  The node ran twice. The order was placed once.")
    return len(orders), 1


def _crash_worker(
    thread_id: str, order_id: str, total: int, agent_id: str, env: Dict[str, str]
) -> None:
    """Runs phase 1 with the crash flag set, in a process expected to die."""
    import langgraph_agent

    os.environ.update(env)
    try:
        langgraph_agent.run_phase(thread_id, order_id, total, agent_id)
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
            "show the order is not placed again. Implies --scenario coordinated."
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
    print(f"  ledger: {shop.ledger_path()}")

    naive_orders = None
    coordinated_orders = expected = None
    started = time.time()

    if args.scenario in ("both", "all", "naive"):
        naive_orders = scenario_naive(args.agents)
    if args.scenario in ("both", "all", "coordinated"):
        coordinated_orders, expected = scenario_coordinated(
            args.agents, args.session_id
        )

    divergent_orders = heterogeneous_orders = langgraph_orders = None
    if args.scenario in ("all", "divergent"):
        divergent_orders, _ = scenario_divergent(args.agents)
    if args.scenario in ("all", "heterogeneous"):
        heterogeneous_orders, _ = scenario_heterogeneous(args.agents)
    if args.scenario in ("all", "langgraph"):
        langgraph_orders, _ = scenario_langgraph()

    print("\n" + "=" * 72)
    print("  VERDICT")
    print("=" * 72)
    if naive_orders is not None:
        print(f"  Without Cellaflow: {naive_orders} orders for 1 purchase")
    if coordinated_orders is not None:
        noun = "order" if coordinated_orders == 1 else "orders"
        suffix = " (already placed before this run)" if expected == 0 else ""
        print(f"  With Cellaflow:    {coordinated_orders} {noun} for 1 purchase{suffix}")
    if divergent_orders is not None:
        print(
            f"  Replicas that disagreed: {divergent_orders} order for "
            f"{args.agents} proposed totals"
        )
    if heterogeneous_orders is not None:
        print(
            f"  {args.agents} unrelated agents:  {heterogeneous_orders} order across "
            f"{args.agents} separate sessions"
        )
    if langgraph_orders is not None:
        print(
            f"  LangGraph node:    {langgraph_orders} order, across a crash "
            f"and a resume"
        )
    print(f"\n  ({time.time() - started:.1f}s total)\n")

    for label, got in (("divergent", divergent_orders),
                       ("heterogeneous", heterogeneous_orders),
                       ("langgraph", langgraph_orders)):
        if got is not None and got != 1:
            print(f"  UNEXPECTED: {label} scenario produced {got} orders, expected 1.\n")
            return 1

    if coordinated_orders is not None and coordinated_orders != expected:
        # A fresh session must produce one order; one that already placed it must
        # not reach the shop at all.
        print(
            f"  UNEXPECTED: coordinated run produced {coordinated_orders} orders, "
            f"expected exactly {expected}.\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
