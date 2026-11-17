# Does a leased tool call actually prevent a duplicate charge?

Run it and see. Two arms, identical in every respect except one.

```bash
docker compose up -d
pip install -r requirements.txt
python proof.py
```

```
  ARM A  -  PostgresSaver only, no Cellaflow
        >>> SEAT RESERVED   BK-1
        >>> CARD CHARGED    2499 for BK-1
      run 1: charged, then the process died
        >>> CARD CHARGED    2499 for BK-1
      run 2: resumed from cold and completed
      gateway hits -> seats=1 charges=2

  ARM B  -  identical graph, leased with durable_tools
        >>> SEAT RESERVED   BK-1
        >>> CARD CHARGED    2499 for BK-1
      run 1: charged, then the process died
      run 2: resumed from cold and completed
      gateway hits -> seats=1 charges=1
```

Each arm asserts its own count, so `--arm a` or `--arm b` on its own still exits
non-zero if that arm stops behaving. A proof that cannot fail is not a proof.

## What it does

A two-node LangGraph booking agent. Node one reserves a seat; node two charges a
card. The process is killed the instant after the charge lands and before the
checkpoint recording it is written, the worst possible moment, because the money
has moved and nothing durable says so. A second run then resumes the same thread
from cold.

Both arms use **`PostgresSaver`** for checkpoints. The only variable is whether the
tool call holds a lease.

## The number that matters is `seats`

Look at the seats column, not the charges. **One reservation in both arms.**

That is the checkpointer working exactly as designed: node one had committed, so
the resumed run correctly skipped it and re-ran only the pending node. Nothing was
lost, nothing was corrupted, no state drifted.

So this is not a demonstration that `PostgresSaver` is broken. It is a
demonstration that it succeeds at precisely what it promises, and that the promise
stops at the boundary of your own function body. A checkpointer records what your
agent *decided*. It cannot record what your agent *did* to the outside world,
because it never saw it happen.

## What it does not measure

Worth being explicit, because a benchmark that overstates its scope is worse than
none:

- **Not a performance benchmark.** No throughput, no latency, no comparison of
  storage engines. It is a correctness demonstration and it is deterministic:
  the charge either repeated or it did not, on any hardware.
- **Not a claim about concurrency.** One writer, one thread, one crash. Replicas
  racing the same node is a different scenario.
- **Not a general durability claim.** It kills a process. It does not cut power to
  the host.

## How the counting works

The counters live *inside* the tool bodies, so they record executions and never
cache hits. If the lease works, the body is never entered and nothing is appended,
so there is no way for a suppressed call to be counted as a success.

## Versions

The Python side is pinned in `requirements.txt`, because the output above has to
be something you can reproduce rather than something that happened once here:

| | |
| :--- | :--- |
| `cellaflow` | 0.11.0 |
| `langgraph` | 1.2.22 |
| `langgraph-checkpoint-postgres` | 3.2.1 |
| `psycopg[binary]` | 3.3.9 |

Note it is `cellaflow`, not `cellaflow[langgraph]`. That extra pulls in
`CellaflowSaver`, the checkpointer, which this proof deliberately does not use:
both arms checkpoint with LangGraph's own `PostgresSaver` so the only variable is
the lease. `durable_tools` and `tool` import from the base package and are not
tied to any framework.

The engine is deliberately **not** pinned. It is the published
`ghcr.io/cellaflow/cellaflow:latest` image, not a local build, so you are running
what everyone else runs. The output above was produced against `latest` resolving
to the 0.3.0 engine. If a later engine ever changes these numbers, that is a
result worth having rather than one worth hiding behind a tag.

## If you already have an engine on 50051

That is the Cellaflow default everywhere, so it is a likely collision, and the
failure is quiet: you do not get a refused bind, you get a proof that connected
to the other engine and printed a table that looks fine.

The compose file and the script both take overrides, and all four values have to
agree:

```bash
PROOF_ENGINE_PORT=50071 PROOF_PG_PORT=55443 docker compose up -d

CELLAFLOW_TARGET=localhost:50071 \
PROOF_DB_URI="postgresql://postgres:proof@localhost:55443/proof?sslmode=disable" \
python proof.py
```

The script prints the engine and database it is actually using on its first two
lines. Check them against what you started.

## Reading the code

`proof.py` is one file, under 200 lines. `run_arm()` takes the two tool functions
and a context manager; arm A passes `no_lease`, which does nothing, and arm B
passes `durable_tools`. Everything else (the graph, the crash, the resume, the
checkpointer) is shared between them, so there is nowhere for a thumb to rest on
the scale.
