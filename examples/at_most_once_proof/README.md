# Does a leased tool call actually prevent a duplicate charge?

Run it and see. Two arms, identical in every respect except one.

```bash
docker compose up -d
pip install -r requirements.txt
python proof.py
```

```
  ARM A  -  PostgresSaver only, no CellaFlow
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

It exits non-zero unless arm A charges twice and arm B charges once. A proof that
cannot fail is not a proof.

## What it does

A two-node LangGraph booking agent. Node one reserves a seat; node two charges a
card. The process is killed the instant after the charge lands and before the
checkpoint recording it is written — the worst possible moment, because the money
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
  storage engines. It is a correctness demonstration and it is deterministic —
  the charge either repeated or it did not, on any hardware.
- **Not a claim about concurrency.** One writer, one thread, one crash. Replicas
  racing the same node is a different scenario.
- **Not a general durability claim.** It kills a process. It does not cut power to
  the host.

## How the counting works

The counters live *inside* the tool bodies, so they record executions and never
cache hits. If the lease works, the body is never entered and nothing is appended
— there is no way for a suppressed call to be counted as a success.

## Versions

Pinned in `requirements.txt` and `docker-compose.yml`. The engine is the published
`ghcr.io/cellaflow/cellaflow:latest` image, not a local build, so you are running
what everyone else runs.

## Reading the code

`proof.py` is one file, about 150 lines. `run_arm()` takes the two tool functions
and a context manager; arm A passes `no_lease`, which does nothing, and arm B
passes `durable_tools`. Everything else — the graph, the crash, the resume, the
checkpointer — is shared between them, so there is nowhere for a thumb to rest on
the scale.
