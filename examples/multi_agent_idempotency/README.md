# One refund, many agents, one charge

Several agents decide the same customer needs refunding, at the same moment. Only one charge
reaches the payment provider — every other agent receives that charge's result.

Five scenarios, each a different way a charge gets duplicated:

| | Who is racing | What stops the double charge |
| --- | --- | --- |
| 1 | Five replicas, no coordination | Nothing. Five charges. |
| 2 | Five replicas, same session, same inputs | One derived key. One winner, four cache hits. |
| 3 | Five replicas that **disagree** about the amount | The graph position they all target. Four are refused *before* they charge. |
| 4 | Five **different** agents in five separate sessions | A coordination domain they each name. |
| 5 | One agent, killed **after** charging | A lease scoped to the thread. The resumed run reuses the charge. |

Scenario 5 is the one a checkpointer cannot do: the side effect happens inside a LangGraph node.

Every agent runs as its own OS process, released simultaneously, so they genuinely race.

## Run it

```bash
docker run -d --name cellaflow-demo \
  -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_DB_PATH=/data/cellaflow \
  -e CELLAFLOW_HOST=:: \
  ghcr.io/cellaflow/cellaflow:latest

pip install 'cellaflow[langgraph]'    # scenario 5 needs LangGraph; the rest do not
python run_demo.py                     # scenarios 1 and 2
python run_demo.py --scenario all      # all five
python run_demo.py --scenario langgraph   # the crash-mid-charge one
```

## What you'll see

```
  SCENARIO 1 - 5 agents, no coordination
  agent-1    charged the customer       rf_73357c475796
  agent-2    charged the customer       rf_3303f08f42eb
  ...                                   (five different confirmations)
  REAL CHARGES ON THE LEDGER: 5
  Customer was billed:        $124.95

  SCENARIO 2 - 5 agents, coordinated through CellaFlow
  agent-1    reused cached charge       rf_190ceb68d56c
  agent-2    reused cached charge       rf_190ceb68d56c
  agent-3    reused cached charge       rf_190ceb68d56c
  agent-4    charged the customer       rf_190ceb68d56c
  agent-5    reused cached charge       rf_190ceb68d56c
  REAL CHARGES ON THE LEDGER: 1
  Customer was billed:        $24.99
```

- **One execution.** One agent called the payment provider; four did not.
- **Everyone got the same answer** — the same confirmation id, not five equal-looking ones.
- **Counted from the provider's own ledger**, an append-only file the demo reads back, rather
  than from what the agents report about themselves.

## Survives a restart

```bash
python run_demo.py --agents 6 --session-id refund-4417   # 1 charge
docker restart cellaflow-demo
python run_demo.py --agents 6 --session-id refund-4417   # 0 charges
```

The second run charges nothing and returns the same confirmation id from before the restart.

## How it works

The refund is a `@tool`, and every replica joins the same session:

```python
@tool(tool_name="issue_refund", scope=IdempotencyScope.SCOPE_SESSION_WIDE)
def issue_refund_step(ticket_id: str, amount_cents: int) -> dict:
    return gateway.issue_refund(ticket_id, amount_cents)

@workflow(version="1.0.0")
def handle_refund_request(ticket_id: str, amount_cents: int) -> dict:
    return issue_refund_step(ticket_id, amount_cents)

handle_refund_request("TICKET-4417", 2499, _session_id="refund-4417")
```

`SCOPE_SESSION_WIDE` leaves agent identity out of the derived key, so every replica in the
session arrives at the same one. The engine grants exactly one of them the right to execute.

### When replicas disagree (scenario 3)

The key above includes a hash of the arguments, so replicas that reason their way to *different*
amounts derive *different* keys. From the engine's view those are two unrelated operations, and
nothing about the key makes them contend.

What they still share is the position in the session's graph that each is about to write. The
engine claims that position when it grants a lease, so the second agent is refused there —
before its function body runs:

```
DivergentStepError: Step 'issue_refund' at sequence 1 was refused: Sequence 1 in
session 'refund-4417' is already claimed by a different step identity ...
```

The durable fix is in the workflow rather than at the call site: put the value the replicas
disagree about inside its own `@tool`, and they converge on one amount before reaching the step
that spends it.

### When the agents are not replicas at all (scenario 4)

Five different agents in five different sessions share no position, so none of that applies.
`SCOPE_SHARED` derives a key from a domain the caller names instead — dropping both the session
and the workflow version, since these agents agree on neither:

```python
@tool(tool_name="issue_refund", scope=IdempotencyScope.SCOPE_SHARED)
def issue_refund_step(ticket_id: str, amount_cents: int) -> dict: ...

@workflow(version="2.4.1")
def review_flagged_order(ticket_id: str, amount_cents: int) -> dict:
    return issue_refund_step(ticket_id, amount_cents)

review_flagged_order("TICKET-4417", 2499, _coordination_id="refund-TICKET-4417")
```

The domain is required and has no default. A default would make two unrelated callers that
happen to make the same call deduplicate against each other, suppressing one of them with no
error anywhere.

### When the side effect lives inside a LangGraph node (scenario 5)

The first four scenarios call the leased tool directly. Real agents rarely do — the tool call sits
inside a graph node, which is exactly the place a checkpointer cannot help. LangGraph makes the
graph's *state* durable; nothing in it makes a node's *side effect* happen once.

Scenario 5 kills the process at the worst possible moment: after the gateway is charged, before
the checkpoint recording it lands. The money has moved and nothing durable says so. A second
process then resumes the thread from cold, LangGraph re-runs the pending node, and the tool is
reached a second time:

```python
from cellaflow import CellaflowSaver, durable_tools, tool

@tool(tool_name="issue_refund")
def issue_refund(ticket_id: str, amount_cents: int, agent_id: str) -> dict:
    return gateway.issue_refund(ticket_id, amount_cents, charged_by=agent_id)

def refund_node(state):
    return {"receipt": issue_refund(state.ticket_id, state.amount_cents, state.agent_id)}

app = graph.compile(checkpointer=CellaflowSaver(target=ENGINE_TARGET))

with durable_tools(config):
    app.invoke(None, config)          # None resumes; an input would start a second run
```

```
phase 1: process exited with 17, ledger has 1 charge(s)
phase 2: resumed and completed, ledger has 1 charge(s)
graph reached: refunded -> confirmed rf_91000dc621d8
```

The node ran twice and the customer was charged once.

`durable_tools` is doing one specific thing: deriving the session that owns the thread's leased
calls. It has to be **stable** across restarts, or the resumed run derives a different idempotency
key and the lease recognises nothing — which fails by charging again rather than by raising. It
also has to be **separate** from the checkpointer's own session, because the saver and `@step`
advance independent sequence counters and would otherwise compete for the same graph positions.
Deriving one from the other satisfies both, which is why this is a helper and not a paragraph of
instructions.

## Options

`--agents N` (default 5) · `--session-id ID` to resume ·
`--scenario naive|coordinated|both|divergent|heterogeneous|langgraph|all`.
Point at a different engine with `CELLAFLOW_TARGET=host:port`.

Scenario 4 defines five distinct agent roles, so `--scenario heterogeneous` accepts at most
`--agents 5`.

## What makes agents converge

Two things decide whether agents share an execution, and both are worth setting deliberately.

**Pin `tool_name`.** It defaults to the function name, so the five agents in scenario 4 — five
different functions — each derive their own key unless you set `tool_name="issue_refund"` on all
of them. Pinning it is what makes heterogeneous agents converge, and it is the first thing to
check if they are not.

**Arguments are part of the identity.** `issue_refund(ticket, 2499)` and
`issue_refund(ticket, 2500)` are different operations by construction — that is what scenario 3
turns on. It means an agent always receives a result for the arguments it actually asked for.

## Files

| File | |
| --- | --- |
| `run_demo.py` | Entry point — spawns the agents, prints the scoreboard |
| `swarm.py` | The two agent implementations, uncoordinated and coordinated |
| `gateway.py` | Stand-in payment provider; writes the ledger |
| `langgraph_agent.py` | The LangGraph agent for scenario 5, killed mid-charge and resumed |
| `raw_leases.py` | The same coordination at the gRPC level, one call at a time |
