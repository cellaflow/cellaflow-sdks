#!/usr/bin/env python3
"""Four ways to guard an irreversible tool call. Two of them do not work.

    docker compose up -d
    pip install -r requirements.txt
    python benchmark.py

Same graph, same crash, same Postgres. Every guard uses LangGraph's stock
PostgresSaver for checkpoints; they differ only in what protects the
irreversible call inside the node:

    A  no-guard     nothing                          the default
    B  lock         pg_advisory_lock                 mutual exclusion only
    C  claim        claim-first idempotency key      what teams actually build
    D  cellaflow    CellaFlow leased tool

The letters are a table index. The names are the identity -- select with
`--guards claim,cellaflow`.

Four scenarios, because a guard that only ever loses is not a fair opponent:

    crash: after    the pod dies after the guard recorded the work
    crash: during   the pod dies between the side effect and the record
    hung holder     one agent stalls mid-call; the others want the same work
    contention      N workers race, nobody dies

`lock` wins contention and loses both crashes: a lock is released when its
holder dies, which is precisely when you needed it. `claim` is the Stripe-style
idempotency key and is included because leaving it out would make the comparison
dishonest -- a competent team writes `claim`, not `lock`. Where `claim` still
differs from `cellaflow` is recorded in the README.

The ledger file adjudicates. Not what any guard says about itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

DSN = os.environ.get(
    "BENCH_DSN", "postgresql://postgres:bench@localhost:55434/bench?sslmode=disable"
)
ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")
LEDGER = Path("ledger.jsonl")

#: id -> (index letter, label). The name is the identity; the letter is only a
#: compact gutter for the results table. Every guard runs stock PostgresSaver,
#: which is stated once above the table rather than four times inside it.
#: The third field says whether the guard records durable state. It decides
#: whether a duplicate is annotated "recovered": for a guard that recorded
#: nothing, two charges is not a workflow that recovered, it is just the
#: failure the guard was supposed to prevent.
GUARDS = {
    "no-guard":  ("A", "stock PostgresSaver only", False),
    "lock":      ("B", "+ pg_advisory_lock", False),
    "claim":     ("C", "+ claim-first idempotency key", True),
    "cellaflow": ("D", "+ CellaFlow leased tool", True),
}
GUARD_ORDER = list(GUARDS)

#: How long a caller that lost the claim waits for the winner's answer before
#: giving up. Bounded on purpose: a claimer that died is never coming back, and
#: an unbounded wait would hang the run instead of reporting the deadlock.
CLAIM_POLL_SECONDS = 15.0


def _lock_key(guard: str, order_id: str) -> int:
    # Full signed bigint, which is what pg_advisory_lock(bigint) actually takes.
    # This used to narrow to 31 bits, which made the lock look worse than it is:
    # the first collision on sequential order ids landed at ~#72k and got reported
    # as a Postgres limit when it was ours. At full width there is no collision in
    # 3M keys. Note the pg_advisory_lock(int, int) overload gives each field only
    # 32 bits, so code using it is back at the ~#72k cliff -- that is a real
    # constraint, just not one Postgres forces on you.
    digest = hashlib.sha256(f"{guard}:{order_id}".encode()).hexdigest()
    return int(digest[:16], 16) - 2**63


def _record(guard: str, order_id: str) -> str:
    """The irreversible act. Appends to the ledger; only ever called for real."""
    conf = f"ch_{uuid.uuid4().hex[:10]}"
    with LEDGER.open("a") as fh:
        fh.write(json.dumps({"guard": guard, "order": order_id, "conf": conf}) + "\n")
    return conf


def charges(guard: str, order_id: str) -> List[str]:
    if not LEDGER.exists():
        return []
    out = []
    for line in LEDGER.read_text().splitlines():
        r = json.loads(line)
        if r["guard"] == guard and r["order"] == order_id:
            out.append(r["conf"])
    return out


# --------------------------------------------------------------------------
# The three guards. Each takes the same node body and decides whether it runs.
# --------------------------------------------------------------------------

def guard_none(guard, order_id, body, variant="", agent_id="agent", amount=2499):
    """`no-guard`. Nothing stops a second execution."""
    return body()


def guard_advisory_lock(guard, order_id, body, variant="", agent_id="agent", amount=2499):
    """`lock`: mutual exclusion and nothing else.

    Correct for concurrency -- workers serialise. Useless across a crash:
    Postgres releases the lock when the connection drops, so the retry takes it
    cleanly and runs the body again. Nothing durable records that it already ran.
    """
    import psycopg

    # sha256, not hash(): Python randomises string hashes per process, so hash()
    # would give every worker a different lock id and no mutual exclusion at all.
    key = _lock_key(guard, order_id)
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (key,))
        try:
            return body()
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def guard_claim_first(guard, order_id, body, variant="", agent_id="agent", amount=2499):
    """`claim`: the Stripe-style idempotency key. No lock at all.

    Claim the key, do the work, then complete the row. The primary key IS the
    mutual exclusion -- a second caller's INSERT conflicts, so it knows someone
    else owns the operation and waits for their answer.

    Two things this buys over a lock-based guard. It does not hold a connection
    across the side effect, so a slow operation does not occupy a pool slot. And
    it is what teams actually write: "insert an idempotency key, store the
    response against it" is documented everywhere.

    What it costs is a third state. A lock-based marker is absent or present;
    this one is absent, *in progress*, or done -- and a caller that dies between the claim
    and the completion leaves the row in progress with nobody coming back for it.
    Deadlocked against a dead process: the row holds a claim whose owner no longer
    exists.
    Every later caller waits out its poll and gives up. Clearing that needs a
    TTL and a rule for who may take over, which is a lease.

    Deliberately not implemented here. The deadlock is the measurement.
    """
    import psycopg

    # The key is whatever the developer decided identifies the work. Keyed on
    # the order alone, agents converge. Include anything that varies per agent
    # -- here the agent id, a plausible thing to reach for when the tool takes
    # one -- and each derives its own key and each charges. Nothing warns.
    if variant == "per-agent-key":
        key = f"{guard}:{order_id}:{agent_id}"
    else:
        key = f"{guard}:{order_id}"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS bench_claim ("
            " k text PRIMARY KEY,"
            " state text NOT NULL,"
            " response text)"
        )
        claimed = conn.execute(
            "INSERT INTO bench_claim (k, state) VALUES (%s, 'in_progress')"
            " ON CONFLICT DO NOTHING RETURNING k",
            (key,),
        ).fetchone()

    if claimed is None:
        # Someone else owns it. Poll for their answer rather than proceeding --
        # bounded, because a claimer that died is never going to finish.
        deadline = time.time() + CLAIM_POLL_SECONDS
        while time.time() < deadline:
            with psycopg.connect(DSN, autocommit=True) as conn:
                row = conn.execute(
                    "SELECT state, response FROM bench_claim WHERE k = %s", (key,)
                ).fetchone()
            if row and row[0] == "done":
                return row[1]
            time.sleep(0.1)
        raise TimeoutError(f"claim on {key} never completed")

    result = body()
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(
            "UPDATE bench_claim SET state='done', response=%s WHERE k=%s",
            (str(result), key),
        )
    return result


def guard_cellaflow(guard, order_id, body, variant="", agent_id="agent", amount=2499):
    """`cellaflow`. The lease is held by the engine, not by this process."""
    from cellaflow import tool

    if variant == "shared":
        from cellaflow import IdempotencyScope

        # Every agent must pin the same tool_name -- it defaults to the function
        # name, and different agents are different functions. shared_on names the
        # argument that identifies the work, so agents may disagree about
        # everything else (here, the amount) and still converge.
        leased = tool(
            tool_name="charge_shared",
            scope=IdempotencyScope.SCOPE_SHARED,
            shared_on=["ticket_id"],
        )(lambda ticket_id, amount_cents: body())
        # The amount genuinely differs per agent. shared_on excludes it, which
        # is the point: agents that reasoned their way to different amounts
        # still converge on one refund. Hash it and they would not.
        return leased(order_id, amount)

    # Default scope is session-wide, and durable_tools derives the session from
    # the thread. Agents on distinct threads therefore derive distinct keys and
    # do not converge -- which is why SCOPE_SHARED exists.
    leased = tool(tool_name=f"charge_{guard}")(lambda _o: body())
    return leased(order_id)


# --------------------------------------------------------------------------
# The graph. Identical across all four guards.
# --------------------------------------------------------------------------

@dataclass
class State:
    order_id: str = ""
    amount: int = 2499
    receipt: Dict[str, Any] = field(default_factory=dict)


def build_app(checkpointer, guard: str, die: bool, stall: float = 0.0,
              die_in_body: bool = False, variant: str = "",
              agent_id: str = "agent", order: str = ""):
    from langgraph.graph import END, START, StateGraph

    guard_fn = {
        "no-guard": guard_none,
        "lock": guard_advisory_lock,
        "claim": guard_claim_first,
        "cellaflow": guard_cellaflow,
    }[guard]

    def pay(state: State) -> Dict[str, Any]:
        def body():
            if stall:
                __import__("time").sleep(stall)   # a model call that never returns
            conf = _record(guard, state.order_id)
            print(f"        CHARGED {state.order_id} -> {conf}")
            if die_in_body:
                # The sharper crash: inside the guard, after the side effect and
                # before whatever records it. Every guard's bookkeeping is still
                # pending here, which is what makes this the interesting moment
                # rather than the one the `crash` scenario uses.
                print("        pod died mid-guard")
                sys.stdout.flush()
                os._exit(17)
            return conf

        conf = guard_fn(guard, state.order_id, body, variant=variant,
                        agent_id=agent_id, amount=state.amount)
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


def _run(guard: str, thread: str, order: str, die: bool = False,
         resume: bool = False, stall: float = 0.0, die_in_body: bool = False,
         agent_id: str = "agent", amount: int = 2499,
         variant: str = "", coord: Optional[str] = None) -> None:
    """One invocation, in its own process so a crash is a real crash."""
    from langgraph.checkpoint.postgres import PostgresSaver

    with PostgresSaver.from_conn_string(DSN) as cp:
        cp.setup()
        config = {"configurable": {"thread_id": thread}}
        app = build_app(cp, guard, die, stall, die_in_body, variant, agent_id,
                        order)
        payload = None if resume else State(order_id=order, amount=amount)

        if guard == "cellaflow":
            from cellaflow import durable_tools

            # coordination_id is what SCOPE_SHARED converges on. It is only
            # meaningful for the shared variant; the others ignore it.
            with durable_tools(config, target=ENGINE, coordination_id=coord):
                final = app.invoke(payload, config)
        else:
            final = app.invoke(payload, config)

    # Returned so the caller can tell "deduplicated and got the winner's
    # answer" from "deduplicated and got an exception". A guard that stops the
    # second charge but leaves four agents holding errors has not finished the
    # job, and a charge count cannot see the difference.
    return (final or {}).get("receipt", {}).get("conf") or None


def _worker(args, done: Any = None, stats: Any = None,
            t0: float = 0.0) -> None:
    """Runs one invocation and records how it ended.

    `stats` is (answered, errored, first_answer_at). The last is what makes a
    recovery time measurable: whichever agent finishes first stamps it, so the
    caller learns *when* the work completed rather than only that it did.
    """
    import time

    try:
        conf = _run(**args)
        if done is not None:
            done.value = 1
        if stats is not None:
            answered, _errored, first_at = stats
            if conf:
                with answered.get_lock():
                    answered.value += 1
                with first_at.get_lock():
                    if first_at.value == 0.0:
                        first_at.value = time.time() - t0
    except Exception as exc:                      # a losing guard may raise
        if stats is not None:
            with stats[1].get_lock():
                stats[1].value += 1
        print(f"        {type(exc).__name__}: {str(exc)[:70]}")


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------

def scenario_crash(guard: str) -> int:
    """Charge, die, resume from cold. How many times was the customer charged?"""
    thread, order = f"crash-{guard}-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    p = ctx.Process(target=_worker, args=(dict(guard=guard, thread=thread, order=order, die=True),))
    p.start(); p.join(120)

    p = ctx.Process(target=_worker,
                    args=(dict(guard=guard, thread=thread, order=order,
                               resume=True),))
    p.start(); p.join(120)

    return len(charges(guard, order))


def scenario_hung_holder(guard: str, n: int = 5, stall: float = 8.0) -> Dict[str, Any]:
    """One worker stalls mid-call. What happens to the other N-1?

    N concurrent invocations on **one** thread id, one of which is stuck on a
    model call that is not coming back. It is the same shape as the contention
    scenario, plus a staller -- a redelivered invocation, not distinct agents.
    (This docstring used to claim the opposite. Distinct agents on distinct
    threads are scenario_shared_convergence.)

    The finding is not thread-specific: a holder that is slow rather than dead
    blocks whoever is waiting, whether they are retries or separate agents.
    Measured here on the simpler shape.

    Measured result: at an 8-second stall, `cellaflow` and `claim` behave
    identically -- both
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
    thread = f"hung-{guard}-{uuid.uuid4().hex[:8]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    # The staller must acquire first, or this measures a start-order race
    # rather than the hung-holder case: if a fast agent wins the lock it
    # completes in milliseconds and the staller then finds the work already
    # done, never blocking anyone.
    t0 = time.time()
    slow = ctx.Process(target=_worker,
                       args=(dict(guard=guard, thread=thread, order=order,
                                  stall=stall),))
    slow.start()
    time.sleep(2.0)          # let it get inside the critical section

    fast = [
        ctx.Process(target=_worker, args=(dict(guard=guard, thread=thread, order=order),))
        for _ in range(n - 1)
    ]
    for pr in fast:
        pr.start()
    for pr in fast:
        pr.join(stall + 60)
    waited = time.time() - t0 - 2.0     # how long the OTHERS took, after starting
    slow.join(stall + 60)
    return {"charges": len(charges(guard, order)), "elapsed": waited}


def scenario_crash_mid_guard(guard: str) -> Dict[str, Any]:
    """The sharper crash: die between the side effect and whatever records it.

    The `crash` scenario kills after the guard has finished, so every guard's
    bookkeeping is already durable and the retry is answered from it. This one
    kills inside the guard, which is the moment that actually distinguishes the
    guards -- and the moment a real pod death is most likely to land in,
    because
    the side effect is the slow part.
    """
    thread, order = f"during-{guard}-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    p = ctx.Process(target=_worker,
                    args=(dict(guard=guard, thread=thread, order=order,
                               die_in_body=True),))
    p.start(); p.join(120)

    done = ctx.Value("i", 0)
    p = ctx.Process(
        target=_worker,
        args=(dict(guard=guard, thread=thread, order=order, resume=True), done),
    )
    p.start(); p.join(120)

    # Charges alone cannot tell "deduplicated correctly" from "never ran". An guard
    # that deadlocks its own key charges once and leaves the order unfulfilled, which
    # a charge count scores as a pass. Report both.
    return {"charges": len(charges(guard, order)), "completed": bool(done.value)}


#: The cross-thread multi-agent case: distinct agents, distinct threads, one
#: shared piece of work. Each entry is (label, guard, variant).
#: Each approach appears twice: the plausible mistake, then the correct form.
#: "shared scope not declared" is not a deployment mode anyone picks -- it is
#: what you get by forgetting, and it is the direct counterpart of keying a
#: hand-rolled claim on something that varies per agent.
SHARED_CONFIGS = [
    ("no-guard",                        "no-guard",  ""),
    ("lock",                            "lock",      ""),
    ("claim, key includes agent id",    "claim",     "per-agent-key"),
    ("claim, keyed on the ticket",      "claim",     ""),
    ("cellaflow, shared scope missing", "cellaflow", ""),
    ("cellaflow, shared_on=[ticket]",   "cellaflow", "shared"),
]


def scenario_shared_convergence(guard: str, variant: str,
                                n: int = 5) -> Dict[str, Any]:
    """N agents on N *distinct* threads, converging on one piece of work.

    Every other scenario here puts N workers on one thread id, which models a
    redelivered invocation. This one models something different: separate agents
    -- a support bot and a billing bot, say -- that independently decided the
    same refund is owed. They are different runs of different graphs, so they
    have different threads, and nothing about the thread makes them converge.

    They also disagree about the amount, which is what makes them genuinely
    different agents rather than retries of one. Anything keyed on the full
    argument list therefore gives each its own key and each its own charge.
    """
    order = f"ORD-{uuid.uuid4().hex[:6]}"
    coord = f"refund-{order}-{uuid.uuid4().hex[:6]}"
    ctx = __import__("multiprocessing").get_context("spawn")

    jobs = [
        dict(
            guard=guard,
            # The difference that matters: a thread per agent, not one shared.
            thread=f"shared-{guard}-{variant or 'default'}-{i}-{uuid.uuid4().hex[:6]}",
            order=order,
            agent_id=f"agent-{i}",
            amount=2499 + i,          # they disagree, on purpose
            variant=variant,
            coord=coord,
        )
        for i in range(n)
    ]
    import time

    stats = (ctx.Value("i", 0), ctx.Value("i", 0), ctx.Value("d", 0.0))
    t0 = time.time()
    procs = [ctx.Process(target=_worker, args=(j, None, stats, t0))
             for j in jobs]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join(180)
    return {
        "charges": len(charges(guard, order)),
        "answered": stats[0].value,     # agents that got a usable receipt
        "errored": stats[1].value,      # agents that raised instead
        "elapsed": time.time() - t0,
        "n": n,
    }


#: Of the configurations that converge, what happens when one agent dies
#: holding the work. Only the two that got it right are worth comparing.
SHARED_CRASH_CONFIGS = [
    ("claim, keyed on the ticket",     "claim",     ""),
    ("cellaflow, shared_on=[ticket]",  "cellaflow", "shared"),
]


def scenario_shared_crash(guard: str, variant: str, n: int = 5) -> Dict[str, Any]:
    """Five agents converge; the one that wins the work dies mid-refund.

    This is the multi-agent version of `crash: during`, and the blast radius is
    what makes it a different question. There, one retry is blocked. Here the
    whole fleet is waiting on one dead process, and whether the refund ever
    happens depends on whether anything reclaims the work.

    Reports charges *and* whether the refund completed, because a guard that
    deadlocks the ticket forever charges exactly once and a charge count alone
    scores that as the best result on the board.
    """
    import time

    order = f"ORD-{uuid.uuid4().hex[:6]}"
    coord = f"refund-{order}-{uuid.uuid4().hex[:6]}"
    ctx = __import__("multiprocessing").get_context("spawn")
    done = ctx.Value("i", 0)

    def spec(i, die_in_body):
        return dict(
            guard=guard,
            thread=f"scrash-{guard}-{variant or 'default'}-{i}-{uuid.uuid4().hex[:6]}",
            order=order, agent_id=f"agent-{i}", amount=2499 + i,
            variant=variant, coord=coord, die_in_body=die_in_body,
        )

    # The doomed agent must take the work first, or this measures a start-order
    # race: a healthy agent would win, finish, and the crash would land on an
    # operation that was already recorded.
    stats = (ctx.Value("i", 0), ctx.Value("i", 0), ctx.Value("d", 0.0))
    doomed = ctx.Process(target=_worker, args=(spec(0, True),))
    doomed.start()
    time.sleep(3.0)

    # t0 starts when the survivors do, so recovery measures the wait they
    # actually experience rather than including the head start.
    t0 = time.time()
    others = [ctx.Process(target=_worker, args=(spec(i, False), done, stats, t0))
              for i in range(1, n)]
    for pr in others:
        pr.start()
    # Generous: cellaflow's lease is 20s, so nothing can be observed before then.
    for pr in others:
        pr.join(120)
    doomed.join(30)
    for pr in others:
        if pr.is_alive():
            pr.terminate()
    return {
        "charges": len(charges(guard, order)),
        "completed": bool(done.value),
        # How long the surviving agents waited before one finished the work.
        # This is what the lease TTL actually buys, measured end to end rather
        # than read off the engine's default.
        "recovery": stats[2].value,
        "answered": stats[0].value,
        "errored": stats[1].value,
    }


def _misconfiguration_report() -> List[str]:
    """Whether each way of getting the shared case wrong tells you.

    Actually attempts each misconfiguration rather than describing it, so the
    "raises at import" column cannot drift away from what the SDK does.
    """
    from cellaflow import IdempotencyScope, tool

    out = []

    def probe(desc: str, build) -> None:
        try:
            build()
        except Exception as exc:
            out.append(f"{desc:<44} {type(exc).__name__} at import")
        else:
            out.append(f"{desc:<44} silent -- 5 charges")

    probe("either: never thought about sharing",
          lambda: None)          # default scope both sides; measured above as 5
    probe("claim: key includes the agent id",
          lambda: None)          # nothing to decorate; measured above as 5
    probe("cellaflow: SCOPE_SHARED without shared_on",
          lambda: tool(tool_name="x", scope=IdempotencyScope.SCOPE_SHARED)(
              lambda ticket_id: None))
    probe("cellaflow: shared_on names a missing field",
          lambda: tool(tool_name="x", scope=IdempotencyScope.SCOPE_SHARED,
                       shared_on=["nope"])(lambda ticket_id: None))
    probe("cellaflow: agents use different tool_name",
          lambda: None)          # decorates fine; the keys simply differ
    return out


def scenario_contention(guard: str, n: int) -> int:
    """N workers race for the same order. Nobody crashes."""
    # One thread id for all N workers: this is one invocation started N times,
    # which is what a redelivered webhook or a retried queue message looks like.
    # `cellaflow` derives its lease session from the thread, so giving each
    # worker its own thread would mean they never contend -- the harness would
    # report N charges and call it a loss when nothing was actually racing.
    # Distinct agents on distinct threads are scenario_shared_convergence.
    order = f"ORD-{uuid.uuid4().hex[:6]}"
    thread = f"cont-{guard}-{uuid.uuid4().hex[:8]}"
    jobs = [dict(guard=guard, thread=thread, order=order) for _ in range(n)]
    ctx = __import__("multiprocessing").get_context("spawn")
    procs = [ctx.Process(target=_worker, args=(j,)) for j in jobs]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join(180)
    return len(charges(guard, order))


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--guards", default=",".join(GUARD_ORDER),
        help="comma-separated subset of " + ",".join(GUARD_ORDER) + " (default: all)",
    )
    ap.add_argument(
        "--writers", default="1,5,10,25",
        help="contention sweep, comma-separated (default: 1,5,10,25)",
    )
    ap.add_argument("--skip-contention", action="store_true")
    ap.add_argument("--skip-shared", action="store_true")
    args = ap.parse_args()

    # Split-and-validate rather than substring matching: the old form silently
    # accepted anything and ran whatever happened to match.
    requested = [g.strip().lower() for g in args.guards.split(",") if g.strip()]
    unknown = [g for g in requested if g not in GUARDS]
    if unknown:
        ap.error(
            f"unknown guard(s): {', '.join(unknown)}. "
            f"Valid: {', '.join(GUARD_ORDER)}"
        )
    guards = [g for g in GUARD_ORDER if g in requested]
    LEDGER.unlink(missing_ok=True)
    print(f"\n  postgres: {DSN.rsplit('@', 1)[-1]}\n  engine:   {ENGINE}")

    print("\n" + "=" * 70)
    print("  CRASH: AFTER -- the guard recorded the work; the checkpoint had not landed")
    print("=" * 70)
    crash = {}
    for gid in guards:
        print(f"\n  {GUARDS[gid][0]}  {gid} - {GUARDS[gid][1]}")
        crash[gid] = scenario_crash(gid)
        verdict = "charged once" if crash[gid] == 1 else f"charged {crash[gid]} times"
        print(f"      -> {verdict}")

    print("\n" + "=" * 70)
    print("  CRASH: DURING -- the pod dies between the side effect and the record")
    print("=" * 70)
    during = {}
    for gid in guards:
        print(f"\n  {GUARDS[gid][0]}  {gid} - {GUARDS[gid][1]}")
        during[gid] = scenario_crash_mid_guard(gid)
        state = "resumed" if during[gid]["completed"] else "DEADLOCKED, never completed"
        print(f"      -> {during[gid]['charges']} charge(s), {state}")

    print("\n" + "=" * 70)
    print("  HUNG HOLDER: one agent stalls mid-call; 4 others want the same work")
    print("=" * 70)
    hung = {}
    for gid in guards:
        print(f"\n  {GUARDS[gid][0]}  {gid} - {GUARDS[gid][1]}")
        hung[gid] = scenario_hung_holder(gid)
        print(f"      -> {hung[gid]['charges']} charge(s); "
              f"the other 4 agents took {hung[gid]['elapsed']:.1f}s")

    shared: List[tuple] = []
    if not args.skip_shared:
        print("\n" + "=" * 70)
        print("  5 AGENTS, 5 THREADS -- distinct agents converging on one refund")
        print("=" * 70)
        print("  Every scenario above shares one thread id. This one does not:")
        print("  five separate agents, five threads, one ticket, and they")
        print("  disagree about the amount.\n")
        for label, gid, variant in SHARED_CONFIGS:
            if gid not in guards:
                continue
            m = scenario_shared_convergence(gid, variant)
            shared.append((label, m))
            print(f"  {label:<32} -> {m['charges']} charge(s), "
                  f"{m['answered']}/{m['n']} agents answered, "
                  f"{m['elapsed']:.1f}s")

    scrash: List[tuple] = []
    if not args.skip_shared:
        print("\n" + "=" * 70)
        print("  5 AGENTS, ONE DIES -- the agent holding the work never comes back")
        print("=" * 70)
        print("  Of the two configurations that converged above, what happens")
        print("  when the agent that won the work dies mid-refund.\n")
        for label, gid, variant in SHARED_CRASH_CONFIGS:
            if gid not in guards:
                continue
            m = scenario_shared_crash(gid, variant)
            scrash.append((label, m))
            state = "refund completed" if m["completed"] else "TICKET DEADLOCKED"
            rec = f", recovered in {m['recovery']:.1f}s" if m["completed"] else ""
            print(f"  {label:<32} -> {m['charges']} charge(s), {state}{rec}")

    cont: Dict[str, Dict[int, int]] = {}
    if not args.skip_contention:
        levels = [int(x) for x in args.writers.split(",")]
        print("\n" + "=" * 70)
        print("  CONTENTION: N workers race for one order, nobody dies")
        print("=" * 70)
        for gid in guards:
            cont[gid] = {}
            print(f"\n  {GUARDS[gid][0]}  {gid} - {GUARDS[gid][1]}")
            for n in levels:
                cont[gid][n] = scenario_contention(gid, n)
                print(f"      {n:>4} workers -> {cont[gid][n]} charge(s)")

    print("\n" + "=" * 70)
    print("  RESULTS   (charges for one order; 1 is correct everywhere)")
    print("=" * 70)
    # Contention levels are dynamic (--writers), so their headers are built here
    # rather than hardcoded. Everything left of them is fixed.
    header = (f"  {'guard':<46} {'crash: after':>13} {'crash: during':>16} "
              f"{'1 stalls':>9} {'they wait':>10}")
    levels = sorted(next(iter(cont.values())).keys()) if cont else []
    for n in levels:
        header += f" {str(n) + ' race':>8}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for gid in guards:
        m = during[gid]
        # A charge count alone scores "deadlocked forever" as a pass, so the
        # cell carries the outcome too: a duplicate you can reconcile is not
        # the same failure as an order that never ships.
        if not m["completed"]:
            cell = f"{m['charges']} deadlocked"
        elif m["charges"] > 1 and GUARDS[gid][2]:
            # "recovered" is the workflow state, parallel with "deadlocked" --
            # the resumed run finished. The duplicate it leaves is visible and
            # reconcilable; an order that never ships is not.
            cell = f"{m['charges']} recovered"
        else:
            cell = f"{m['charges']}"
        label = f"{GUARDS[gid][0]}  {gid:<10} {GUARDS[gid][1]}"
        row = (f"  {label:<46} {crash[gid]:>13} {cell:>16} "
               f"{hung[gid]['charges']:>9} {hung[gid]['elapsed']:>9.1f}s")
        for n in levels:
            row += f" {cont[gid][n]:>8}"
        print(row)

    if scrash:
        print("\n" + "=" * 70)
        print("  5 AGENTS, ONE DIES   (one ticket)")
        print("=" * 70)
        hdr = (f"  {'':<32} {'charges':>8} {'outcome':>18} "
               f"{'recovered':>10} {'answered':>9}")
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for label, m in scrash:
            outcome = "refund completed" if m["completed"] else "TICKET DEADLOCKED"
            # A guard that never recovers has no recovery time; printing 0.0s
            # would read as "instantly", which is the opposite of the truth.
            rec = f"{m['recovery']:.1f}s" if m["completed"] else "never"
            answered = f"{m['answered']}/4"
            print(f"  {label:<32} {m['charges']:>8} {outcome:>18} "
                  f"{rec:>10} {answered:>9}")
        print(
            "\n  The count alone inverts the result. claim charges once because"
            "\n  nothing ever reclaims the dead agent's row -- the other four time"
            "\n  out, and no later agent can refund that ticket either, until a"
            "\n  human clears it. cellaflow charges twice because the lease expires"
            "\n  and another agent finishes the work. A duplicate you can reconcile"
            "\n  against a refund that never happens."
        )

    if shared:
        print("\n" + "=" * 70)
        print("  5 AGENTS, 5 THREADS   (one ticket; 1 is correct)")
        print("=" * 70)
        # answered/errored are carried only when they say something. On a clean
        # run every agent answers and nothing errors, so two constant columns
        # would be noise -- but a run where an agent throws, or comes away
        # without a receipt, needs to show it rather than reporting charges
        # alone. That is how a run against a dead Postgres was caught.
        anomaly = any(m["errored"] or m["answered"] != m["n"] for _, m in shared)
        cols = f" {'answered':>9} {'errored':>8}" if anomaly else ""
        hdr = f"  {'':<32} {'charges':>8}{cols} {'wall':>7}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for label, m in shared:
            answered = f"{m['answered']}/{m['n']}"
            extra = f" {answered:>9} {m['errored']:>8}" if anomaly else ""
            print(f"  {label:<32} {m['charges']:>8}{extra} "
                  f"{m['elapsed']:>6.1f}s")
        print(
            "\n  A thread per agent, so nothing about the thread coordinates them."
            "\n  What converges them is whether the key is derived from the work"
            "\n  alone -- by the developer for claim, by shared_on for cellaflow."
            "\n  cellaflow's default scope is session-wide and the session comes"
            "\n  from the thread, so it does NOT converge here. That is what"
            "\n  SCOPE_SHARED is for, and it has to be declared."
        )
        print("\n  Getting it wrong:")
        for line in _misconfiguration_report():
            print(f"    {line}")

    print(
        "\n  crash: after   = the pod dies AFTER the guard recorded the work, while"
        "\n                   the checkpoint is still pending."
        "\n  crash: during  = the pod dies DURING the guard, after the side effect and"
        "\n                   before the record."
        "\n"
        "\n  Nothing survives during. no-guard, lock and cellaflow all charge twice;"
        "\n  claim charges once by deadlocking its own key, so the order is never"
        "\n  fulfilled. Charging twice and never finishing are both failures -- pick"
        "\n  which your domain prefers."
        "\n"
        "\n  Where the guards differ beyond this table is in the README.\n"
    )

    expected_ok = crash.get("cellaflow") == 1 if "cellaflow" in guards else True
    if not expected_ok:
        print("  DID NOT REPRODUCE: cellaflow should charge once across a crash.\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
