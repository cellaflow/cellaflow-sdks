#!/usr/bin/env python3
"""Three ways to stop a LangGraph node charging twice. Two of them don't.

    docker compose up -d
    pip install -r requirements.txt
    python benchmark.py

Same graph, same crash, same Postgres. The arms differ only in what guards the
irreversible call:

    A  PostgresSaver alone                    the default
    B  + pg_advisory_lock                     mutual exclusion only
    D  + advisory lock AND a durable marker   the careful hand-rolled fix
    C  + CellaFlow leased tool

Two scenarios, because an arm that only ever loses is not a fair arm:

    crash        the pod dies between the charge and the checkpoint
    contention   N workers race, nobody dies

Arm B wins contention and loses the crash: a lock is released when its holder
dies, which is precisely when you needed it. Arm D adds the durable marker that
fixes that, and is included because leaving it out would make the comparison
dishonest -- a competent team writes D, not B. Where D still differs from C is
recorded in the README.

The ledger file adjudicates. Not what any arm says about itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

DSN = os.environ.get(
    "BENCH_DSN", "postgresql://postgres:bench@localhost:55434/bench?sslmode=disable"
)
ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")
LEDGER = Path("ledger.jsonl")

ARMS = {
    "A": "PostgresSaver alone",
    "B": "+ pg_advisory_lock",
    "D": "+ advisory lock and durable marker",
    "C": "+ CellaFlow leased tool",
}
ARM_ORDER = ["A", "B", "D", "C"]


def _record(arm: str, order_id: str) -> str:
    """The irreversible act. Appends to the ledger; only ever called for real."""
    conf = f"ch_{uuid.uuid4().hex[:10]}"
    with LEDGER.open("a") as fh:
        fh.write(json.dumps({"arm": arm, "order": order_id, "conf": conf}) + "\n")
    return conf


def charges(arm: str, order_id: str) -> List[str]:
    if not LEDGER.exists():
        return []
    out = []
    for line in LEDGER.read_text().splitlines():
        r = json.loads(line)
        if r["arm"] == arm and r["order"] == order_id:
            out.append(r["conf"])
    return out


# --------------------------------------------------------------------------
# The three guards. Each takes the same node body and decides whether it runs.
# --------------------------------------------------------------------------

def guard_none(arm, order_id, body):
    """Arm A. Nothing stops a second execution."""
    return body()


def guard_advisory_lock(arm, order_id, body):
    """Arm B: mutual exclusion and nothing else.

    Correct for concurrency -- workers serialise. Useless across a crash:
    Postgres releases the lock when the connection drops, so the retry takes it
    cleanly and runs the body again. Nothing durable records that it already ran.
    """
    import psycopg

    key = abs(hash(f"{arm}:{order_id}")) % (2**31)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (key,))
        try:
            return body()
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def guard_lock_and_marker(arm, order_id, body):
    """Arm D: the careful hand-rolled fix -- lock plus a durable marker.

    Included so the comparison is honest. A competent team writes this, not B,
    and it survives both scenarios in this harness.

    What it does not cover is the gap between the side effect and the marker.
    The body runs, then the INSERT commits; a crash in between leaves the money
    moved and nothing recording it, and the retry charges again. This harness
    cannot hit that window reliably -- see the README.
    """
    import psycopg

    key = abs(hash(f"{arm}:{order_id}")) % (2**31)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (key,))
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS bench_done (k text primary key)")
            k = f"{arm}:{order_id}"
            if conn.execute("SELECT 1 FROM bench_done WHERE k=%s", (k,)).fetchone():
                return "already-done"
            result = body()
            conn.execute("INSERT INTO bench_done VALUES (%s)", (k,))
            return result
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def guard_cellaflow(arm, order_id, body):
    """Arm C. The lease is held by the engine, not by this process."""
    from cellaflow import tool

    leased = tool(tool_name=f"charge_{arm}")(lambda _o: body())
    return leased(order_id)


# --------------------------------------------------------------------------
# The graph. Identical across all three arms.
# --------------------------------------------------------------------------

@dataclass
class State:
    order_id: str = ""
    receipt: Dict[str, Any] = field(default_factory=dict)


def build_app(checkpointer, arm: str, die: bool):
    from langgraph.graph import END, START, StateGraph

    guard = {
        "A": guard_none,
        "B": guard_advisory_lock,
        "D": guard_lock_and_marker,
        "C": guard_cellaflow,
    }[arm]

    def pay(state: State) -> Dict[str, Any]:
        def body():
            conf = _record(arm, state.order_id)
            print(f"        CHARGED {state.order_id} -> {conf}")
            return conf

        conf = guard(arm, state.order_id, body)
        if die:
            # Money has moved; the checkpoint recording it has not landed.
            print("        pod died")
            sys.stdout.flush()
            os._exit(17)
        return {"receipt": {"conf": conf}}

    g = StateGraph(State)
    g.add_node("pay", pay)
    g.add_edge(START, "pay")
    g.add_edge("pay", END)
    return g.compile(checkpointer=checkpointer)


def _run(arm: str, thread: str, order: str, die: bool, resume: bool) -> None:
    """One invocation, in its own process so a crash is a real crash."""
    from langgraph.checkpoint.postgres import PostgresSaver

    with PostgresSaver.from_conn_string(DSN) as cp:
        cp.setup()
        config = {"configurable": {"thread_id": thread}}
        app = build_app(cp, arm, die)
        payload = None if resume else State(order_id=order)

        if arm == "C":
            from cellaflow import durable_tools

            with durable_tools(config, target=ENGINE):
                app.invoke(payload, config)
        else:
            app.invoke(payload, config)


def _worker(args) -> None:
    try:
        _run(*args)
    except Exception as exc:                      # a losing arm may raise
        print(f"        {type(exc).__name__}: {str(exc)[:70]}")


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

def scenario_crash(arm: str) -> int:
    """Charge, die, resume from cold. How many times was the customer charged?"""
    thread, order = f"crash-{arm}-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    p = ctx.Process(target=_worker, args=((arm, thread, order, True, False),))
    p.start(); p.join(120)

    p = ctx.Process(target=_worker, args=((arm, thread, order, False, True),))
    p.start(); p.join(120)

    return len(charges(arm, order))


def scenario_contention(arm: str, n: int) -> int:
    """N workers race for the same order. Nobody crashes."""
    # One thread id for all N workers. Arm C derives its lease session from the
    # thread, so giving each worker its own thread would mean they never contend
    # -- the harness would report 5 charges and call it a loss when nothing was
    # actually racing.
    order = f"ORD-{uuid.uuid4().hex[:6]}"
    thread = f"cont-{arm}-{uuid.uuid4().hex[:8]}"
    jobs = [(arm, thread, order, False, False) for _ in range(n)]
    ctx = __import__("multiprocessing").get_context("spawn")
    procs = [ctx.Process(target=_worker, args=(j,)) for j in jobs]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join(180)
    return len(charges(arm, order))


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--arms", default="ABDC", help="subset of ABDC (default: all)")
    ap.add_argument(
        "--writers", default="1,5,10,25",
        help="contention sweep, comma-separated (default: 1,5,10,25)",
    )
    ap.add_argument("--skip-contention", action="store_true")
    args = ap.parse_args()

    arms = [a for a in ARM_ORDER if a in args.arms.upper()]
    LEDGER.unlink(missing_ok=True)
    print(f"\n  postgres: {DSN.rsplit('@', 1)[-1]}\n  engine:   {ENGINE}")

    print("\n" + "=" * 70)
    print("  CRASH: the pod dies between the charge and the checkpoint")
    print("=" * 70)
    crash = {}
    for a in arms:
        print(f"\n  Arm {a} - {ARMS[a]}")
        crash[a] = scenario_crash(a)
        verdict = "charged once" if crash[a] == 1 else f"charged {crash[a]} times"
        print(f"      -> {verdict}")

    cont: Dict[str, Dict[int, int]] = {}
    if not args.skip_contention:
        levels = [int(x) for x in args.writers.split(",")]
        print("\n" + "=" * 70)
        print("  CONTENTION: N workers race for one order, nobody dies")
        print("=" * 70)
        for a in arms:
            cont[a] = {}
            print(f"\n  Arm {a} - {ARMS[a]}")
            for n in levels:
                cont[a][n] = scenario_contention(a, n)
                print(f"      {n:>4} workers -> {cont[a][n]} charge(s)")

    print("\n" + "=" * 70)
    print("  RESULTS   (charges for one order; 1 is correct everywhere)")
    print("=" * 70)
    header = f"  {'arm':<38} {'crash':>7}"
    levels = sorted(next(iter(cont.values())).keys()) if cont else []
    for n in levels:
        header += f" {str(n) + 'w':>6}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for a in arms:
        row = f"  {a + '  ' + ARMS[a]:<38} {crash[a]:>7}"
        for n in levels:
            row += f" {cont[a][n]:>6}"
        print(row)

    print(
        "\n  B holds under contention and fails the crash -- a lock is released when"
        "\n  its holder dies, which is exactly when it was needed. D adds a durable"
        "\n  marker and survives both; where it still differs from C is in the README.\n"
    )

    expected_ok = crash.get("C") == 1 if "C" in arms else True
    if not expected_ok:
        print("  DID NOT REPRODUCE: arm C should charge once across a crash.\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
