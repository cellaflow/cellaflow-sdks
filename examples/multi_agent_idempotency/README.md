# One order, many agents, one placement

Several agents decide the same customer order should be placed, at the same moment. Only one
order reaches the shop. Every other agent receives that order's confirmation.

The shop here takes no idempotency key, like many order, booking and checkout endpoints, so
every request it receives places one more order. Nothing on the shop's side can tell a retry
from a new purchase.

Five scenarios, each a different way an order gets placed twice:

| | Who is racing | What stops the duplicate order |
| --- | --- | --- |
| 1 | Five replicas, no coordination | Nothing. Five orders. |
| 2 | Five replicas, same session, same inputs | One derived key. One winner, four cache hits. |
| 3 | Five replicas that **disagree** about the total | The graph position they all target. Four are refused *before* they order. |
| 4 | Five **different** agents that disagree about the total | A coordination domain, plus a declaration of what identifies the work. |
| 5 | One LangGraph agent, killed **after** the order is placed | A lease scoped to the thread. The resumed run reuses the order. |

Scenario 5 is the one a checkpointer cannot do: the side effect happens inside a LangGraph node.

Every agent runs as its own OS process, released simultaneously, so they genuinely race.

## Run it

```bash
git clone https://github.com/cellaflow/cellaflow-sdks
cd cellaflow-sdks/examples/multi_agent_idempotency

docker run -d --name cellaflow-demo \
  -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
  -e CELLAFLOW_DB_PATH=/data/cellaflow \
  -e CELLAFLOW_HOST=:: \
  ghcr.io/cellaflow/cellaflow:latest

pip install 'cellaflow[langgraph]'    # scenario 5 needs LangGraph; the rest do not
python run_demo.py                     # scenarios 1 and 2
python run_demo.py --scenario all      # all five, about 10 seconds
python run_demo.py --scenario langgraph   # the crash-mid-run one
```

## What you'll see

```
  SCENARIO 1 - 5 agents, no coordination
  agent-1    placed the order           ord_73357c475796
  agent-2    placed the order           ord_3303f08f42eb
  ...                                   (five different confirmations)
  REAL ORDERS AT THE SHOP:    5
  Customer was billed:        $124.95

  SCENARIO 2 - 5 agents, coordinated through Cellaflow
  agent-1    reused the placed order    ord_190ceb68d56c
  agent-2    reused the placed order    ord_190ceb68d56c
  agent-3    reused the placed order    ord_190ceb68d56c
  agent-4    placed the order           ord_190ceb68d56c
  agent-5    reused the placed order    ord_190ceb68d56c
  REAL ORDERS AT THE SHOP:    1
  Customer was billed:        $24.99
```

- **One execution.** One agent called the shop; four did not.
- **Everyone got the same answer:** the same confirmation id, not five equal-looking ones.
- **Counted from the shop's own ledger**, an append-only file the demo reads back, rather
  than from what the agents report about themselves.

## Survives a restart

```bash
python run_demo.py --agents 6 --session-id session-4417   # 1 order
docker restart cellaflow-demo
python run_demo.py --agents 6 --session-id session-4417   # 0 orders
```

The second run places nothing and returns the same confirmation id from before the restart.

## How it works

Placing the order is a `@tool`, and every replica joins the same session:

```python
from cellaflow import IdempotencyScope, tool, workflow

@tool(tool_name="place_order", scope=IdempotencyScope.SCOPE_SESSION_WIDE)
def place_order_step(order_id: str, total_cents: int) -> dict:
    return shop.place_order(order_id, total_cents)

@workflow(version="1.0.0")
def handle_order_request(order_id: str, total_cents: int) -> dict:
    return place_order_step(order_id, total_cents)

handle_order_request("ORDER-4417", 2499, _session_id="session-4417")
```

`SCOPE_SESSION_WIDE` leaves agent identity out of the derived key, so every replica in the
session arrives at the same one. The engine grants exactly one of them the right to execute.

### When replicas disagree (scenario 3)

The key above includes a hash of the arguments, so replicas that price the order *differently*
derive *different* keys. From the engine's view those are two unrelated operations, and nothing
about the key makes them contend.

What they still share is the position in the session's graph that each is about to write. The
engine claims that position when it grants a lease, so the second agent is refused there,
before its function body runs:

```
DivergentStepError: Step 'place_order' at sequence 1 was refused: Sequence 1 in
session 'session-4417-3f9a2c1b' is already claimed by a different step identity ...
```

The durable fix is in the workflow rather than at the call site: put the value the replicas
disagree about inside its own `@tool`, and they converge on one total before reaching the step
that places the order.

### When the agents are not replicas at all (scenario 4)

Five different agents in five different sessions share no graph position, so the check in
scenario 3 is not a backstop here: it is session-scoped. Across sessions the derived key is the
*only* thing standing between five agents and five orders, which is why what goes into it has to
be declared rather than guessed.

`SCOPE_SHARED` derives a key from a domain the caller names instead. It drops both the session
and the workflow version, since these agents agree on neither, and hashes only the arguments
named in `shared_on`:

```python
@tool(tool_name="place_order", scope=IdempotencyScope.SCOPE_SHARED,
      shared_on=["order_id"])
def place_order_step(order_id: str, total_cents: int) -> dict: ...

@workflow(version="2.4.1")
def recover_abandoned_cart(order_id: str, total_cents: int) -> dict:
    return place_order_step(order_id, total_cents)

recover_abandoned_cart("ORDER-4417", 2499, _coordination_id="order-4417")
```

The agents deliberately propose **different totals**, because five agents that independently
concluded the order should be placed will not have priced it the same way. That is what makes
them independent. An earlier version of this scenario gave them all the same total, which tested
a weaker version of its own premise and hid a real defect.

The output names what that costs:

```
  4 agent(s) proposed a different total and adopted $24.99:
    cart-recovery, reorder-bot, support-agent, ops-sweeper
```

Convergence is not free. Four agents accepted a result computed from arguments they did not
supply. That is the right answer when the order id identifies the work and the total is
incidental, and the wrong answer when the total *is* the disagreement. In that case, make the
disputed value its own leased step, so they agree on it before reaching the step that places
the order.

`shared_on` names what identifies the work: the order, not the total. That distinction is
load-bearing. Agents that priced the order differently must still converge on one order, and
hashing the total would give each of them its own key and its own order. It is required for
this scope, and omitting it raises at import.

The domain is required and has no default. A default would make two unrelated callers that
happen to make the same call deduplicate against each other, suppressing one of them with no
error anywhere.

### When the side effect lives inside a LangGraph node (scenario 5)

The first four scenarios call the leased tool directly. Real agents rarely do: the tool call sits
inside a graph node, which is exactly the place a checkpointer cannot help. LangGraph makes the
graph's *state* durable; nothing in it makes a node's *side effect* happen once.

Scenario 5 kills the process after the leased call returns and before LangGraph's checkpoint for
the node lands. The order exists and Cellaflow has recorded it, but the graph does not know the
node finished. A second process then resumes the thread from cold, LangGraph re-runs the pending
node, and the tool is reached a second time. The engine answers it from the record:

```python
from cellaflow import CellaflowSaver, durable_tools, tool

@tool(tool_name="place_order")
def place_order(order_id: str, total_cents: int, agent_id: str) -> dict:
    return shop.place_order(order_id, total_cents, placed_by=agent_id)

def order_node(state):
    return {"receipt": place_order(state.order_id, state.total_cents, state.agent_id)}

app = graph.compile(checkpointer=CellaflowSaver(target=ENGINE_TARGET))

with durable_tools(config):
    app.invoke(None, config)          # None resumes; an input would start a second run
```

```
phase 1: process exited with 17, shop has 1 order(s)
phase 2: resumed and completed, shop has 1 order(s)
graph reached: placed -> confirmed ord_91000dc621d8
```

The node ran twice and the order was placed once.

`durable_tools` is doing one specific thing: deriving the session that owns the thread's leased
calls. It has to be **stable** across restarts, or the resumed run derives a different idempotency
key and the lease recognises nothing, which fails by placing the order again rather than by
raising. It also has to be **separate** from the checkpointer's own session, because the saver and
`@step` advance independent sequence counters and would otherwise compete for the same graph
positions. Deriving one from the other satisfies both, which is why this is a helper and not a
paragraph of instructions.

## Options

`--agents N` (default 5) · `--session-id ID` to resume ·
`--scenario naive|coordinated|both|divergent|heterogeneous|langgraph|all`.
Point at a different engine with `CELLAFLOW_TARGET=host:port`.

Scenario 4 defines five distinct agent roles, so `--scenario heterogeneous` accepts at most
`--agents 5`.

## What makes agents converge

Two things decide whether agents share an execution, and both are worth setting deliberately.

**Pin `tool_name`.** It defaults to the function name, so the five agents in scenario 4, five
different functions, each derive their own key unless you set `tool_name="place_order"` on all
of them. Pinning it is what makes heterogeneous agents converge, and it is the first thing to
check if they are not.

**Arguments are part of the identity.** `place_order(order, 2499)` and
`place_order(order, 2500)` are different operations by construction; that is what scenario 3
turns on. It means an agent always receives a result for the arguments it actually asked for.

## Files

| File | |
| --- | --- |
| `run_demo.py` | Entry point: spawns the agents, prints the scoreboard |
| `swarm.py` | The agent implementations, uncoordinated and coordinated |
| `shop.py` | Stand-in shop that takes no idempotency key; writes the ledger |
| `langgraph_agent.py` | The LangGraph agent for scenario 5, killed mid-run and resumed |
| `raw_leases.py` | The same coordination at the gRPC level, one call at a time |
