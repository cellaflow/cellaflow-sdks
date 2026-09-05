# Three ways to stop a LangGraph node charging twice

```bash
docker compose up -d
pip install -r requirements.txt
python benchmark.py
```

Same graph, same crash, same Postgres. The arms differ only in what guards the
irreversible call.

```
  arm                                      crash   hung  hung s     5w
  ---------------------------------------------------------------------
  A  PostgresSaver alone                       2      5    9.9s      5
  B  + pg_advisory_lock                        2      5   10.7s      5
  D  + advisory lock and durable marker        1      1    7.8s      1
  C  + CellaFlow leased tool                   1      1    7.8s      1
```

Charges for one order. **1 is correct everywhere.**

The **hung holder** column starts one agent stalling inside the critical
section and then starts four more that want the same work.

## Read this before quoting the table

**Arm D — a hand-rolled advisory lock plus a durable marker — matches CellaFlow
on every cell here, including the multi-agent hung-holder case.** That is the honest result and it is deliberately not
hidden. If your side effects fit what arm D covers, arm D is roughly forty lines
in a database you already run, and you do not need this project.

The table is a floor, not a scoreboard.

## What each arm actually does

**A — `PostgresSaver` alone.** Nothing guards the call. Charges once per
execution, so it fails as soon as anything retries.

**B — `pg_advisory_lock`.** Mutual exclusion and nothing else. Correct under
concurrency at 1 worker; loses at 5 and 10 here because without a durable marker
each serialised worker still charges in turn. Loses the crash because Postgres
releases the lock when the holder's connection drops — precisely the moment it
was needed.

**D — advisory lock plus a durable marker.** The version a careful team writes.
Checks and sets a committed marker inside the critical section, so it survives
both scenarios in this harness.

**C — CellaFlow leased tool.** One `with durable_tools(config):` around the
invocation, and `@tool` on the function.

## What the multi-agent cases showed, including where the claim failed

Three claims were made for C over D before any of them were measured. One
survived, one is narrower than stated, and one was simply wrong at the scale
tested. All three are recorded because the wrong one is the useful one.

**Hung holder — the claim was wrong as stated.** At an 8-second stall, C and D
both make the other agents wait the full 8 seconds. The lease ceiling defaults
to one hour, so it never fires; below the ceiling a lease waits exactly like a
lock. That is deliberate — CellaFlow's own design note says reclaiming a merely
slow worker trades a starvation bug for a double charge, which is worse. The
real difference is **bounded versus unbounded waiting**, not less waiting, and
showing it needs a stall longer than the ceiling. This harness does not lower
the ceiling to manufacture a favourable number.

**Cross-thread coordination — not expressible in D.** A lock keyed on a thread
cannot deduplicate two agents on different threads that must act once between
them. `SCOPE_SHARED` can. Not in this table because arm D has no way to enter
the comparison.

**Fencing — untested here.** If D's holder is presumed dead and another worker
proceeds, nothing stops the first completing if it wakes. CellaFlow rejects the
stale writer's commit by fencing token. Reproducing that reliably needs process
control this harness does not have, so no number is claimed.

## Where D and C differ, and why this harness cannot show it

Arm D writes its marker *after* the side effect:

```python
result = body()                              # money moves
conn.execute("INSERT INTO bench_done ...")   # <- crash here and it is lost
```

A crash in that window leaves the charge made and nothing recording it, so the
retry charges again. The window is small and this harness cannot hit it
reliably, so **no arm-D failure is reported for it.** Claiming one without
measuring it would be exactly the kind of unearned number this benchmark exists
to avoid.

The same gap covers a holder that hangs without dying — it keeps the lock while
another worker waits, indefinitely, with no ceiling.

CellaFlow closes both: the lease and the completion record are written in one
transaction, and a lease has a wall-clock ceiling independent of heartbeats.
Whether that difference is worth a second service is a judgement about your
failure model, not something this table decides for you.

## What it does not measure

- **Not a performance benchmark.** No throughput, no latency. Correctness only.
- **Not a distributed-systems claim.** One host, one Postgres, one engine.
- **Not a claim about your workload.** Run it against yours: the guards are
  three small functions and the graph is one node.

## How the counting works

Every arm appends to `ledger.jsonl` from inside the charge path. The ledger
adjudicates — never an arm's own report of what it did. Same discipline as
`examples/at_most_once_proof`.

## Sweeping further

```bash
python benchmark.py --writers 1,5,10,25,50,100
python benchmark.py --arms AC --skip-contention
```

Two harness bugs are worth knowing about, because both produced quotable numbers
that were false:

**`hash()` is randomised per process.** The lock key was derived with Python's
`hash()`, so every spawned worker locked a *different* key and arms B and D had
no mutual exclusion at all. Now `sha256`. Nothing in the output looked wrong.

**The hung-holder case was a start-order race.** If a fast agent won the lock it
finished in milliseconds and the staller then found the work already done — the
scenario silently measured nothing. The staller is now given a two-second head
start so it reliably holds the lock when the others arrive.

Contention shares one `thread_id` across all N workers deliberately. Giving each
worker its own thread means arm C derives a different lease session per worker
and nothing contends — the harness would report N charges and call it a loss
when nothing was actually racing. That bug was in the first version of this file.
