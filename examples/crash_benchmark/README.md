# Your agent charged the customer. Then the pod died.

> **The agent that started the work may not be the agent that finishes it.** CellaFlow
> makes the work itself durable, giving side effects an identity that survives
> crashes, retries, agent replacement, and independent agents converging on the same
> operation.
>
> It doesn't replace your agent framework or promise exactly-once execution against
> arbitrary external systems. Instead, it provides the execution layer that agent
> frameworks don't: durable ownership, idempotency, recovery, fencing, and a shared
> identity for work that several agents arrive at independently.

Four ways to guard an irreversible tool call inside a **LangGraph** node, measured
against six failures. The checkpointer is not the variable; it works correctly in
every one of them, and the gap is somewhere else.

```bash
docker compose up -d
pip install -r requirements.txt
python benchmark.py
```

## Everything this file measures

| scenario | failure injected | no-guard | lock | claim | cellaflow |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrent retries** | **5 workers**, one thread. 5 identical invocations at once, **same work identity** | 5× | 5× | 1× | 1× |
| **Crash before checkpoint** | **1 worker, then one retry**; it dies after the guard recorded, before LangGraph checkpoints | 2× | 2× | 1× | 1× |
| **Crash after side effect** | **1 worker, then one retry**; it dies after the charge, before the result is recorded | 2× / re-runs | 2× / re-runs | 1× / **stuck** | 2× / **recovers** |
| **Slow owner** | **5 workers**, one thread. The owner stalls 8s, four arrive behind it | 5× | 5× | 1× / waits | 1× / waits |
| **Independent agents** | **5 agents**, 5 threads, one ticket. **No shared work identity** | 5× | 5× | 1×\* | 1×\* |
| **Owner disappears** | **5 agents**, 5 threads. The one holding the work dies and never returns | not run | not run | 1× / **stuck** | 2× / **recovers** |

**×** external side effects: how many times the customer was charged.
**re-runs** the retry simply executes the node again; nothing was owned, so nothing
was reclaimed.
**stuck** no later worker can finish the operation without a human.
**recovers** the dead owner's lease is reclaimed and the work is finished by whoever
takes it, distinct from *re-runs*, and visible in the clock: the same scenario takes
4.6s under `no-guard` and 23.2s under `cellaflow`, the gap being the lease expiring.
**\*** only when the key is derived from the shared business operation: `shared_on`
for `cellaflow`, a hand-chosen key string for `claim`. Neither does it by default.

**The worker count differs by row, which is why the unguarded numbers do.** The two
crash rows run one worker and then one retry, so two attempts is the ceiling and an
unguarded run charges `2×`. The other four run five workers, so unguarded charges
`5×`. A guard that works pins the column to `1×` whatever the row's ceiling happens
to be.

Every cell comes from a run of `benchmark.py`; *not run* means the harness does not
exercise that pair, not that it was omitted.

**On `Crash after side effect`, CellaFlow charges exactly as many times as no guard at
all.** Two charges either way; the customer is out the same money. The charge had
already left the process, and nothing downstream can unsend it.

What differs is underneath the count. `no-guard` re-runs immediately because it owned
nothing. `cellaflow` waits out the dead holder's lease and reclaims the work, which is
measurable: 4.6s against 23.2s for the identical scenario. That buys a durable record
of what was attempted, not a smaller number in the charges column. That row is not a CellaFlow win and is not
presented as one. The charge already left the process; nothing downstream can unsend
it. What differs is only that the attempt is durably recorded, which is the difference
between a duplicate you can find and a duplicate you cannot.

**Two rows separate the guards**, and they are the same failure seen twice: the worker
that owns the work stops existing. `claim` charges once and stops dead; `cellaflow`
charges again and the run finishes. The rest are table stakes, and they are here
because a comparison that only shows the cells you win is not a comparison.

## Five properties, often called one thing

"Idempotency" and "durability" get used interchangeably, and the rows above separate
because they are not the same property. Each guard here supplies some of these:

| | |
| :--- | :--- |
| **Idempotency** | invoked many times, the external effect happens once |
| **Mutual exclusion** | only one worker executes the operation at a time |
| **Durable execution** | if the worker disappears, another can safely continue |
| **Fencing** | a worker that comes back cannot act as the current owner |
| **Shared work identity** | independent agents can identify that they are acting on the same business operation |

An advisory lock gives you the second. A Stripe-style `Idempotency-Key` gives you the
first, at one gateway. A claim-first table gives you the first and second, and the
third only once you add a TTL, which, done correctly, requires the fourth. The fifth is not
something a single agent framework can provide at all, because it has to hold across
frameworks, and note it is *identity*, not coordination: the agents never talk to each
other. What they share is a name for the work.

The interesting claim is not that CellaFlow does any one of these. It is that the five
have to hold together, and the failures below are what it looks like when one is
missing.

The letters in the tables are an index; the names are how you select guards
(`python benchmark.py --guards claim,cellaflow`).


---

# Part 1 · One agent, invoked many times

**The homogeneous case: one agent, running more than once.** Identical code, identical
arguments, one `thread_id` between them: a redelivered webhook, a retried queue
message, a pod restarted mid-flight. Every scenario in this half has that shape, and
the `1 race` / `5 race` / `10 race` / `25 race` columns sweep how many arrive at once
(`--writers`).

**With stock `PostgresSaver` and nothing else, the card is charged twice on either
crash and once per invocation under either concurrency case.** Not because the
checkpointer is broken. It records completed nodes and skips them on resume, exactly
as designed, but that guarantee begins *after* a node returns, and the money moves
inside it.


## What is being tested

A LangGraph agent with one node that does something it cannot take back: charging
a card. The charge is a stand-in for any side effect you cannot undo from your own
database: sending the email, posting to Slack, reserving the inventory, provisioning
the instance, calling the partner API.

Every guard runs the *same* graph and uses LangGraph's stock `PostgresSaver` for
checkpoints. They differ only in what guards the call inside the node.

The checkpointer is not the variable here. It works correctly under every guard: it
records completed nodes and skips them on resume, exactly as designed. What it does
not do is know anything about a payment gateway, which is the gap these guards are
trying to fill.

### The four failures

| Scenario | What happens |
| :--- | :--- |
| **crash: after** | The pod dies *after* the guard recorded the work, while the LangGraph checkpoint is still pending. |
| **crash: during** | The pod dies *inside* the guard: after the charge, before anything records it. |
| **1 stalls** | Concurrent invocations of the same work, one of which stalls inside the guard for 8 seconds while the others arrive. |
| **5 race** | Concurrent invocations of the same work, all at once. Nobody crashes, nobody stalls. |

Both concurrent scenarios are homogeneous, as described above: one `thread_id`, one
order. That is deliberate: LangGraph runs one invocation per thread, so heterogeneous
agents get their own threads and would never contend here at all. Part 2 measures
that case, and sets the two side by side.

### Reading the numbers

Every cell counts **charges for one order**, so **1 is correct everywhere**. The
`they wait` column is how long the four non-stalled agents took to finish; it
measures blocking, not correctness.

## What CellaFlow does here

One context manager around the invocation, and `@tool` on the function that moves
money:

```python
from dataclasses import dataclass, field
from cellaflow import durable_tools, tool

@dataclass
class State:
    order_id: str = ""
    receipt: dict = field(default_factory=dict)

@tool(tool_name="charge_card")
def charge_card(order_id: str, cents: int) -> dict:
    return gateway.charge(order_id, cents)      # irreversible

def pay_node(state: State) -> dict:
    return {"receipt": charge_card(state.order_id, 2499)}

app = builder.compile(checkpointer=PostgresSaver.from_conn_string(DSN))

# durable_tools takes its session from this thread_id, the same one
# the checkpointer is keyed on.
config = {"configurable": {"thread_id": "order-123"}}

with durable_tools(config):
    app.invoke(State(order_id="order-123"), config)
```

Your checkpointer does not change. `durable_tools` derives a session from the
graph's `thread_id` and puts a **lease** in front of the tool call: the engine
records that this operation was started, by whom, and with what fencing token,
before the gateway is touched. A retry that arrives later gets the recorded result
instead of a second charge.

The lease is a *record in the CellaFlow engine*, not a lock held by your process.
Three properties come with it:

- **A ceiling.** A holder that stalls without dying is reclaimed rather than blocking
  everyone behind it forever.
- **A heartbeat.** A holder that is merely slow keeps its claim, so being slow is not
  treated as being dead.
- **A fencing token.** A superseded holder that wakes up is refused, rather than
  allowed to commit over the top of its replacement. **This fences the record, not
  the side effect**; see below.

Each is straightforward on its own. The work is in making all three correct
*together*, and in keeping them correct, which is what a hand-rolled TTL turns into
the moment you try to make it safe. That argument is
[below](#i-would-just-put-a-ttl-on-the-claim-and-then-you-are-building-a-lease).

**What fencing does not do.** The token is checked when a holder tries to *commit*,
which is after its side effect has run. If a stale worker already sent the HTTP
request, the customer was already charged; refusing its commit cannot unsend it. No
fencing scheme can, unless the far side participates, which is what a gateway's own
`Idempotency-Key` is. So fencing protects the durable record and the graph from a
worker that should no longer be acting; it does not protect the payment. Anyone who
says *"your fencing token doesn't fence the actual payment"* is right, and that is the
boundary the `Crash after side effect` row measures.

An agent framework can persist state. A database can serialise work. A gateway can
offer an idempotency key. A workflow engine can do all three, but only for the workflows you
moved into it, identified by an id its callers had to agree on in advance.

That last condition is the one that breaks here. Two agents that independently decided
the same refund is owed never agreed on anything; if they had, they would not be
independent. An id someone assigns is no help when nobody is there to assign it, which
is why `claim` converges only when the developer happens to derive the key from the
work, and duplicates silently when they do not.

**The identity has to be derived, not assigned**: computed from what the work *is*,
by each agent separately, arriving at the same answer without conferring. That is what
`shared_on` does, and it is the claim this file is testing. It is a different claim
from "stops duplicate charges", and a larger one.

The rest measures it against three guards you would otherwise write yourself, under
failures you do not get to schedule.

## The result

```
  guard                                           crash: after    crash: during  1 stalls  they wait   5 race
  -----------------------------------------------------------------------------------------------------------
  A  no-guard    stock PostgresSaver only                    2                2         5       2.3s        5
  B  lock        + pg_advisory_lock                          2                2         5       7.9s        5
  C  claim       + claim-first idempotency key               1     1 deadlocked         1       8.2s        1
  D  cellaflow   + CellaFlow leased tool                     1      2 recovered         1       8.7s        1
```

## Reading this table

**One column needs reading before the rest.** No guard here makes an external side
effect exactly-once, and none anywhere can; that would take cooperation from the
gateway. This is the two-generals boundary in its everyday form: the charge is not a
database write, so no lock, lease or transaction can bracket it, and the acknowledgement
can be lost after the money has moved. Exactly-once against an uncooperative external
system is not a thing anyone ships; it is a thing people approximate. What a guard *can* decide is the state you are left in when the answer is
ambiguous, and that is what `crash: during` measures.

`claim` charges once and leaves the key deadlocked: no retry proceeds, and no later
agent can act on that ticket, until a human clears the row. `cellaflow` charges twice,
recovers the *execution*, and makes the ambiguous attempt durable.

Be precise about what that second one means, because it is easy to overstate.
**Execution recovery is not business-outcome recovery.** The external system now holds
two refunds and CellaFlow has not resolved that; it has finished the run and left a
record saying what was attempted and when. The duplicate remains a reconciliation
problem at the business level. The trade on offer is *a duplicate you can find*
against *an order that never ships and stays silent until someone notices*, not
correctness against incorrectness. [The limit nobody clears](#the-limit-nobody-clears)
has the full treatment.

The table answers one question: *does the side effect happen twice, under four
failures, on one host?*

Three questions it does not answer matter more once you are running this for real,
and on those the guards are not equivalent:

- **Behaviour at concurrency.** An advisory lock pins a database connection for the
  whole side effect. Ten slow operations against a pool of five take twice as long
  as they should; against a pool of two, five times. Measured below.
- **What happens when a holder dies and nobody clears up.** The obvious fix, a TTL
  on the claim, is where a hand-rolled guard quietly becomes unsafe, and where
  making it safe turns it into the thing it was avoiding.
  [*"I would just put a TTL on the claim"*](#i-would-just-put-a-ttl-on-the-claim-and-then-you-are-building-a-lease).
- **Behaviour when the agent holding the work never comes back.** The guards fail
  differently here, and the failures are not equivalent, one leaves a duplicate you
  can reconcile, the other leaves an order that never ships. With several agents
  converging on one piece of work it is the whole fleet that stops, not one retry.
  Measured below, and it is the clearest separation in this file.

And a fourth no benchmark can show. Any guard that writes something durable before it
acts gets the right answer under these four failures, CellaFlow does, and so does a
carefully written idempotency key. If a single irreversible call is all you need to
protect, a few dozen lines against a database you already run will do it.

But those few dozen lines are a few dozen lines *per guarantee*. Add fencing, a lease ceiling, cross-language keys and observability and
you are maintaining a small durable-execution library. That may be the right call.
It is worth making deliberately.

## What each guard does

**A: `no-guard`.** The node charges, and nothing stops it charging again. Fails as
soon as anything retries.

**B: `lock`.** Mutual exclusion and nothing else, hand-rolled around the
tool call; `PostgresSaver` itself is untouched and still doing its job. Workers
serialise, but without a durable record each one still charges in turn, so it loses
`5 race` as well as both crashes. Postgres releases the lock when the holder's
connection drops, which is precisely the moment it was needed.

**C: `claim`.** The Stripe-style idempotency key, and the one most teams
actually write. `INSERT … ON CONFLICT DO NOTHING` claims the key, the work runs, then
an `UPDATE` completes the row. **No lock at all**, the primary key is the mutual
exclusion. A caller that loses the claim polls for the winner's stored response.

**D: `cellaflow`.** One `with durable_tools(config):` around the
invocation, and `@tool` on the function, shown at the top of this file. The engine
records the operation, its owner, and a fencing token before the gateway is called.
Nothing is hand-rolled: the three states, the bounded wait, the stored response, the
reclaim after a holder dies, and the refusal of a superseded worker are the engine's
behaviour, not code in the example. That is the whole of the guard's implementation , 
compare it against `claim`'s claim, poll, complete and its `UniqueViolation` path.

## What the lock costs, measured

**An advisory lock holds a connection for the whole side effect.**
`pg_advisory_lock` is session-scoped, so the connection is pinned from acquire to
release, across the work. Ten operations of 3 seconds each, varying the pool:

| pool size | lock-based guard | CellaFlow |
| ---: | ---: | ---: |
| 2 | 15.1s | 3.0s |
| 5 | 6.2s | 3.0s |
| 20 | 3.3s | 3.0s |

The lock-based guard is `ceil(10 / pool) × 3s`, exactly pool-bound. CellaFlow is
flat, because a lease is a record and the SDK multiplexes one gRPC channel. If your
side effects are slow, that is a concurrency ceiling with nothing to do with your
database's capacity. `claim` avoids it too: it releases the connection after the claim.

**And it is mutual exclusion, not ownership.** A lock says who may act right now. It
says nothing about whether the work was already done, who is responsible for finishing
it, or what should happen when the holder never comes back, which is why `lock`
duplicates on every row of the table at the top that is not purely about concurrency.
That is the whole of the case against it, and it does not need a keyspace argument to
land.

## When the holder is slow, not dead
Two findings the columns above do not carry, both about a holder that is *slow*
rather than dead, the case a crash test cannot reach.
**The others wait.** At an 8-second stall, a lock and a lease behave identically , 
both make the other agents wait the full 8 seconds. CellaFlow's lease ceiling defaults
to one hour, and below the ceiling a lease waits exactly like a lock. That is
deliberate: reclaiming a merely slow worker trades a starvation bug for a double
charge, which is the worse trade. The real difference is **bounded versus unbounded**
waiting, a lock has no ceiling to reach at all. This harness does not lower ours to
manufacture a favourable number.

**And the staller is refused when it wakes.** Stall the holder, supersede it, then let it wake and try to commit. A
hand-rolled guard refuses with a `UniqueViolation` from its primary key; CellaFlow
refuses with `Fenced out: stale lease`. Both refuse. The difference is that one
refuses by way of an unhandled database exception from a line that looks like
bookkeeping, which most implementations do not catch.

## Where each one breaks

The guards differ in *how* they fail, and the right one depends on which failure your
domain tolerates.

| guard | Fails at |
| :--- | :--- |
| `no-guard` | anything that retries |
| `lock` | anything that retries, the lock dies with its holder |
| `claim` | `crash: during` **deadlocks the key**, no retry can proceed until a human clears the row |
| `cellaflow` | `crash: during`, re-executes and completes, leaving a duplicate to reconcile |

Both failures are real, and they are not equivalent. A duplicate charge is visible,
reconcilable, and self-resolving on the next run. A deadlocked key is silent until
someone notices the order never shipped.

## The limit nobody clears

**`crash: during` is unsurvivable, for every approach here including ours.**

The window between performing a side effect and recording that it happened cannot be
closed by a lock or a lease, because the side effect does not live in your database.
Postgres can only roll back Postgres. Making the record transactional makes it
strictly worse: the write rolls back and the money stays moved.

What differs is what happens next, and the three outcomes are not one outcome.

`no-guard` and `lock` re-execute with nothing anywhere recording that they did, the
customer is charged twice and the system holds no trace of why.

`cellaflow` re-executes and **the run finishes**, against a durable record of the
attempt. Same charge count as `no-guard`, different position: the order is fulfilled
and the duplicate is one you can locate. The business inconsistency is still yours to
reconcile, what you are spared is discovering it by accident.

`claim` charges once and **deadlocks against a dead process**: the row holds a claim
whose owner no longer exists, every later caller waits on a `done` that never arrives,
and the order is never fulfilled until a human clears the row.

This is the floor for anything built on a database plus an external call. Closing it
entirely requires the side effect itself to be transactional with the record, which is
only possible when the side effect *is* a database write, and if it were, none of
these guards would be necessary.


---

# Part 2 · Many agents. One refund. No coordinator.

Part 1 was the homogeneous case, one agent, repeated. **This is the heterogeneous
one: different agents that independently decided the same work is owed.**

A support agent reads the ticket and concludes a refund is due. A billing agent,
reconciling a chargeback, concludes the same thing. Neither knows the other exists.
They run in separate sessions and they disagree about the amount, which is exactly
what makes them different agents rather than retries of one.

*How the harness models this:* a distinct `thread_id` per agent and a per-agent
amount. In a real deployment these agents would differ by more, a different prompt,
a different model, a different set of tools, but none of that changes the condition
being measured, which is simply that they arrive sharing no identity. A distinct
thread and a disagreeing argument are enough to produce it, and the mechanism does
not care how they came to differ. For genuinely distinct agent roles converging on
one side effect, see [`examples/multi_agent_idempotency`](../multi_agent_idempotency)
and its `--scenario heterogeneous`.

**The difference breaks the guard that worked in Part 1.** Every scenario there handed
the guards a shared `thread_id`, and that shared identity is what any durable record
matched on. Heterogeneous agents arrive carrying *nothing* in common. There is no
collision to arbitrate, so every guard lets every agent through and the customer is
refunded once per agent, correctly, by agents each behaving exactly as designed.

Something has to construct the shared identity that separate sessions do not supply.
That is what this half measures, and it is the case CellaFlow is built for: the
coordination has to come from outside any single agent framework, because no framework
can make another framework's agents converge on one side effect.

## When the agents are not the same agent

**Yes, the heterogeneous case is also a race**; that is the first thing people
ask. Both
scenarios launch the same number of processes at once and then wait; neither gives
anyone a head start, and the code is the same shape. They differ in exactly two controlled
variables:

| | `5 race`, **homogeneous** | `5 agents, 5 threads`, **heterogeneous** |
| :--- | :--- | :--- |
| how many run at once | 5 processes, simultaneously | **identical** |
| `thread_id` | one, shared by all | **one each** |
| the arguments | identical | **each agent has its own amount** |
| what it models | one invocation delivered more than once, a redelivered webhook, a retried queue message | separate agents that independently decided the same refund is owed |
| do they collide on one key? | yes, automatically | only if the key is derived from the work |
| `lock` | 5 | 5 |
| `claim` | 1 | 1 keyed on the ticket |
| `cellaflow` | 1 | **1 with `shared_on`** |

Both approaches land on 1 in the right-hand column, and both get there the same way:
by deriving the key from the work rather than from whoever is doing it. For `claim`
the developer writes that key. For `cellaflow` it is a declaration , 
`shared_on=["ticket_id"]`, and the declaration is what the rest of this section is
about.

> **Why a declaration is needed at all.** Without it, `cellaflow` does not *lose* the
> heterogeneous race, **it never enters it.** The session is derived from the
> `thread_id`, so separate sessions produce separate keys, and unrelated operations do
> not contend. There is no collision for any guard to arbitrate. `shared_on` is what
> tells the engine these are the same piece of work, and the measurement below is what
> that declaration is worth.

These are two different problems, not one problem at two sizes.
Homogeneous contenders arrive already sharing an identity, so any guard that writes
something durable has something to match on. Heterogeneous ones do not: the shared
identity does not exist until someone constructs it, and until then every agent
proceeds correctly and the customer is refunded again for each one.

The rest of this section measures the heterogeneous case.

```
  5 agents, 5 threads   (one ticket; 1 is correct)
  -------------------------------------------------
                                    charges    wall
  no-guard                                5    3.7s
  lock                                    5    3.6s
  claim, key includes agent id            5    3.9s
  claim, keyed on the ticket              1    3.9s
  cellaflow, shared scope missing         5    4.0s
  cellaflow, shared_on=[ticket]           1    5.3s
```

The harness also tracks, per agent, whether it came away with a usable receipt or an
exception, deduplicating to one charge while leaving the losers holding errors would
be a different result with the same `charges` number. Those columns are printed only
when they disagree with a clean run, and here they do not: every agent answers,
nothing errors. The wall clock is flat too, arbitration is not what costs you time
when nothing has gone wrong.

**Each approach appears twice: without the declaration, then with it.** The pairs are
counterparts, a hand-rolled key that includes something varying per agent is the same
omission as a scope never declared shared, and both cost one charge per agent. Neither
is a setting anyone chooses; they are what you get by not yet having decided the work
is shared.

The delta is the point. Declaring the scope is what constructs the identity that
separate sessions do not supply:

```python
@tool(
    # Pin this. It defaults to the function name, and heterogeneous agents
    # are different functions -- unpinned, they derive different keys and
    # never converge.
    tool_name="issue_refund",
    scope=IdempotencyScope.SCOPE_SHARED,
    shared_on=["ticket_id"],          # the amount deliberately does not count
)
def issue_refund(ticket_id: str, amount_cents: int) -> dict: ...
```

`claim, keyed on the ticket` charges once for the same reason, arrived at by hand: the
developer chose a key derived from the work. Nobody crashes in this scenario, and on
the number it is a tie.

### A lock can coordinate them. But nothing remembers the first charge.

Keying the lock on the *work* rather than the thread does coordinate heterogeneous
agents; they serialise, and that part takes about fifteen lines. Mutual exclusion
works exactly as advertised.

It is just not memory. Each agent waits its turn, finds nothing recording that the
refund already happened, and charges.

That is the same result `lock` produces in Part 1, for the same reason, mutual
exclusion orders the work, it does not remember it. The only thing this section adds
is that keying on the ticket instead of the thread does not rescue it. A question
worth asking here, because it is the one case where the key *could* have differed;
in Part 1 every worker already shares a thread.

Getting to one refund needs a durable record *alongside* the lock, and once that
record exists it is doing the work, which is what `claim` already is. The lock is
close to decorative.

### Then the agent holding the work dies

Same five agents, same ticket, but the one that wins the work is killed after the
refund and before anything records it. Only the two configurations that converged are
worth comparing:

```
  5 agents, one dies   (one ticket)
  ---------------------------------------------------------------------------------
                                    charges            outcome  recovered  answered
  claim, keyed on the ticket              1  TICKET DEADLOCKED      never       0/4
  cellaflow, shared_on=[ticket]           2   refund completed      19.8s       4/4
```

Two columns carry the result the charge count hides. **`recovered`** is how long the
surviving agents waited before one of them finished the work, 19.8s, which is the
20-second lease TTL observed end to end rather than read off a config default.
**`answered`** is how many of the four survivors came away with a usable receipt:
all four under `cellaflow`, **none** under `claim`, where they polled until they timed
out and raised.

**The count alone inverts the result.** `claim` charges once and looks like the
winner. What actually happened is that the dead agent's row is still `in_progress`,
the other four polled until they timed out, and **no agent will ever refund that
ticket**, not the four waiting, not a sixth arriving an hour later, until a human
clears the row by hand.

`cellaflow` charges twice because the dead holder stops heartbeating, its lease
expires, and another agent picks the work up and finishes it. Two refunds reach the
gateway and one of them is yours to reconcile, but the ticket is closed, and the
record says which attempts were made. Against `claim`, the comparison is not
*one refund versus two*. It is *a reconciliation you can act on* versus *a customer
still waiting and nothing in the system that knows it*.

This is the same trade as `crash: during` in the main table, but the blast radius is
what makes it a different question. There, one retry is blocked. Here one dead process
blocks the entire fleet, permanently.

Two things worth knowing before quoting this. The recovery takes about 20 seconds , 
the lease TTL, during which the other agents are waiting, and they wait on an
unbounded retry, so it is the lease ceiling rather than the caller that bounds them.
And `claim` is deliberately TTL-less here: adding one is the obvious fix, and
*"I would just put a TTL on the claim"* below is why the correct version of that fix
is a lease.

### What the declaration buys

A hand-rolled key and a `shared_on` declaration express the same rule. The difference
is what happens when the rule is expressed wrongly.

**A wrong key is silent.** It is a string a developer wrote, and nothing checks it
against anything. Include a field that varies per agent and each one derives its own
key, takes its own lease, and charges. The first sign is a customer charged once per
agent.

**A wrong declaration is refused before the process starts.** The harness attempts
both of these rather than describing them:

| | result |
| :--- | :--- |
| `SCOPE_SHARED` declared without `shared_on` | **`ValueError` at import** |
| `shared_on` names an argument the function does not have | **`ValueError` at import** |

Neither has a hand-rolled equivalent, because there is nothing to validate a key
string against. This is what the declaration buys: `shared_on=["ticket_id"]` says
once, in the signature, what identifies the work, and everything not named there is
something agents may disagree about while still converging. A key string is correct
until someone adds a field to it.

## "I would just put a TTL on the claim": and then you are building a lease

This is the first thing every engineer says about `claim` deadlocking, and it is the
right instinct. It is also where a hand-rolled guard stops being a weekend's work.

Add `expires_at = now() + interval '30 seconds'` to the claim row and let the next
caller take over an expired one. You have now built a lease, and a lease whose
expiry is the *only* thing authorising a takeover has two failure modes it cannot
tell apart:

- **The holder died after charging.** The claim expires, the next caller takes it
  and charges again. You are back to the duplicate you added the claim to prevent.
- **The holder is alive but was paused**, GC, a slow gateway, a network hiccup
  longer than the TTL. The claim expires, a second worker starts, and now *both* are
  executing. Split brain: the exact thing mutual exclusion was for.

Making it safe needs two more mechanisms. A **heartbeat**, so a holder that is merely
slow keeps its claim instead of being reclaimed. And a **fencing token**, so when a
holder is superseded and later wakes up, its write is refused rather than applied on
top of its replacement's. Without fencing, the paused worker resumes and commits over
the top; a monotonic token is what makes that refusable.

That is a lease with heartbeating and fencing, which is what `cellaflow` calls. The
argument for CellaFlow is not that a TTL is unthinkable. It is that a correct TTL is
a lease, and writing one yourself means owning the heartbeat interval, the reclaim
rule, the token comparison and their failure modes for as long as the code lives.

`claim` is in this benchmark without a TTL deliberately: it is the version teams
actually ship, and its deadlock is the honest consequence of stopping there.


---

# Running it yourself


## What it does not measure

- **Not a performance benchmark.** No throughput, no latency. Correctness only.
- **Not a distributed-systems claim.** One host, one Postgres, one engine.
- **Not a claim about your workload.** Run it against yours: the guards are three
  small functions and the graph is one node.
- **Not the answer when your gateway has one.** If the call accepts a Stripe-style
  `Idempotency-Key` header, use it; that closes the window at the gateway and beats
  guarding it from outside. Most tool calls an agent makes have no such header, which
  is the case these guards exist for.

## How the counting works

Every guard appends to `ledger.jsonl` from inside the charge path. The ledger
adjudicates, never a guard's own report of what it did. Same discipline as
`examples/at_most_once_proof`.

## Sweeping further

```bash
python benchmark.py --writers 1,5,10,25,50,100
python benchmark.py --guards no-guard,cellaflow --skip-contention
```

### If you modify this harness

Three details are load-bearing, and getting any of them wrong produces numbers that
look plausible and are not.

**Derive lock keys with `sha256`, not `hash()`.** Python randomises string hashes per
process, so `hash()` gives every spawned worker a different lock id and no mutual
exclusion at all. Nothing in the output looks wrong.

**Give the staller a head start in the `1 stalls` case.** Otherwise it is a
start-order race: a fast agent takes the lock, finishes in milliseconds, and the
staller finds the work already done, measuring nothing.

**Contention shares one `thread_id` across all N workers.** CellaFlow derives its
lease session from the thread, so per-worker threads mean nothing contends and the
harness reports N charges for a race that never happened.

## Using this in your own agent

`durable_tools` composes with the checkpointer you already run; this benchmark
keeps stock `PostgresSaver` under every guard, CellaFlow's included. There is no
migration: wrap the invocation, decorate the call that must not repeat.

```bash
pip install cellaflow
docker run -p 50051:50051 ghcr.io/cellaflow/cellaflow:latest
```

- [Quickstart](https://docs.cellaflow.com/quickstart)
- [LangGraph integration](https://docs.cellaflow.com/sdks/python/langgraph)
- [Idempotency scopes](https://docs.cellaflow.com/concepts), including
  `shared_on`, for several agents converging on one side effect
