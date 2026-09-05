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
import hashlib
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


def _lock_key(arm: str, order_id: str) -> int:
    digest = hashlib.sha256(f"{arm}:{order_id}".encode()).hexdigest()
    return int(digest[:8], 16) % (2**31)


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

    # sha256, not hash(): Python randomises string hashes per process, so
    # hash() would give every worker a DIFFERENT lock key and no exclusion at
    # all. This harness reported arm B and D as correct for exactly that reason
    # before it was caught.
    key = _lock_key(arm, order_id)
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

    # sha256, not hash(): Python randomises string hashes per process, so
    # hash() would give every worker a DIFFERENT lock key and no exclusion at
    # all. This harness reported arm B and D as correct for exactly that reason
    # before it was caught.
    key = _lock_key(arm, order_id)
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


def build_app(checkpointer, arm: str, die: bool, stall: float = 0.0):
    from langgraph.graph import END, START, StateGraph

    guard = {
        "A": guard_none,
        "B": guard_advisory_lock,
        "D": guard_lock_and_marker,
        "C": guard_cellaflow,
    }[arm]

    def pay(state: State) -> Dict[str, Any]:
        def body():
            if stall:
                __import__("time").sleep(stall)   # a model call that never returns
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


def _run(arm: str, thread: str, order: str, die: bool, resume: bool,
         stall: float = 0.0) -> None:
    """One invocation, in its own process so a crash is a real crash."""
    from langgraph.checkpoint.postgres import PostgresSaver

    with PostgresSaver.from_conn_string(DSN) as cp:
        cp.setup()
        config = {"configurable": {"thread_id": thread}}
        app = build_app(cp, arm, die, stall)
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

    p = ctx.Process(target=_worker, args=((arm, thread, order, True, False, 0.0),))
    p.start(); p.join(120)

    p = ctx.Process(target=_worker, args=((arm, thread, order, False, True, 0.0),))
    p.start(); p.join(120)

    return len(charges(arm, order))


def scenario_hung_holder(arm: str, n: int = 5, stall: float = 8.0) -> Dict[str, Any]:
    """One agent stalls mid-call. What happens to the other N-1?

    This is the multi-agent case, not a retry: the agents are different, all
    want the same work done once, and one of them is stuck on a model call that
    is not coming back.

    Measured result: at an 8-second stall, C and D behave identically -- both
    make the other agents wait the full stall.

    That is not a tie in disguise, and it is not a win either. The lease has a
    wall-clock ceiling (default one hour) and the lock has none, so the two
    diverge only once a stall exceeds that ceiling. Below it a lease waits
    exactly like a lock, by design: reclaiming a worker that is merely slow
    trades a starvation bug for a double charge, which is the worse trade.

    So the honest claim is bounded versus unbounded waiting, not less waiting.
    Demonstrating it needs a stall longer than the ceiling, or a lowered
    ceiling, and this scenario deliberately does not lower it to manufacture a
    favourable number.
    """
    import time

    order = f"ORD-{uuid.uuid4().hex[:6]}"
    thread = f"hung-{arm}-{uuid.uuid4().hex[:8]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    # The staller must acquire first, or this measures a start-order race
    # rather than the hung-holder case: if a fast agent wins the lock it
    # completes in milliseconds and the staller then finds the work already
    # done, never blocking anyone.
    t0 = time.time()
    slow = ctx.Process(target=_worker, args=((arm, thread, order, False, False, stall),))
    slow.start()
    time.sleep(2.0)          # let it get inside the critical section

    fast = [
        ctx.Process(target=_worker, args=((arm, thread, order, False, False, 0.0),))
        for _ in range(n - 1)
    ]
    for pr in fast:
        pr.start()
    for pr in fast:
        pr.join(stall + 60)
    waited = time.time() - t0 - 2.0     # how long the OTHERS took, after starting
    slow.join(stall + 60)
    return {"charges": len(charges(arm, order)), "elapsed": waited}


def scenario_contention(arm: str, n: int) -> int:
    """N workers race for the same order. Nobody crashes."""
    # One thread id for all N workers. Arm C derives its lease session from the
    # thread, so giving each worker its own thread would mean they never contend
    # -- the harness would report 5 charges and call it a loss when nothing was
    # actually racing.
    order = f"ORD-{uuid.uuid4().hex[:6]}"
    thread = f"cont-{arm}-{uuid.uuid4().hex[:8]}"
    jobs = [(arm, thread, order, False, False, 0.0) for _ in range(n)]
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

    print("\n" + "=" * 70)
    print("  HUNG HOLDER: one agent stalls mid-call; 4 others want the same work")
    print("=" * 70)
    hung = {}
    for a in arms:
        print(f"\n  Arm {a} - {ARMS[a]}")
        hung[a] = scenario_hung_holder(a)
        print(f"      -> {hung[a]['charges']} charge(s); "
              f"the other 4 agents took {hung[a]['elapsed']:.1f}s")

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
    header = f"  {'arm':<38} {'crash':>7} {'hung':>7} {'hung s':>7}"
    levels = sorted(next(iter(cont.values())).keys()) if cont else []
    for n in levels:
        header += f" {str(n) + 'w':>6}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for a in arms:
        row = (f"  {a + '  ' + ARMS[a]:<38} {crash[a]:>7} "
               f"{hung[a]['charges']:>7} {hung[a]['elapsed']:>6.1f}s")
        for n in levels:
            row += f" {cont[a][n]:>6}"
        print(row)

    print(
        "\n  B holds under contention and fails the crash -- a lock is released when"
        "\n  its holder dies, which is exactly when it was needed."
        "\n"
        "\n  D and C tie on every column here, including the hung holder. Where they"
        "\n  differ is in the README, and none of it is demonstrated by this table.\n"
    )

    expected_ok = crash.get("C") == 1 if "C" in arms else True
    if not expected_ok:
        print("  DID NOT REPRODUCE: arm C should charge once across a crash.\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
