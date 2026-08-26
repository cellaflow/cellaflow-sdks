# One refund, five agents, one charge

Five copies of the same agent pick up the same customer refund at the same time. Only one
charge reaches the payment provider — the other four receive that charge's result.

## The agents

Five **replicas of one agent**, all working the same session with the same inputs. This is the
shape you get when you run an agent redundantly for durability, or scale it horizontally and
more than one worker claims the same job.

They run as separate OS processes released simultaneously, so they genuinely race.

## Run it

```bash
docker run -d --name cellaflow-demo \
  -p 50051:50051 -p 9090:9090 \
  -e CELLAFLOW_DB_PATH=/data/cellaflow \
  -e CELLAFLOW_HOST=:: \
  ghcr.io/cellaflow/cellaflow:latest

pip install cellaflow
python run_demo.py
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

## Options

`--agents N` (default 5) · `--scenario naive|coordinated|both` · `--session-id ID` to resume.
Point at a different engine with `CELLAFLOW_TARGET=host:port`.

## Current scope

This example covers **replicas of a single agent sharing one session** — the shape you get from
running an agent redundantly, or from horizontal scaling where more than one worker claims the
same job.

Coordinating *different* agents, each running its own workflow in its own session, is a
separate capability and is not what this example shows.

## Files

| File | |
| --- | --- |
| `run_demo.py` | Entry point — spawns the agents, prints the scoreboard |
| `swarm.py` | The two agent implementations, uncoordinated and coordinated |
| `gateway.py` | Stand-in payment provider; writes the ledger |
| `raw_leases.py` | The same coordination at the gRPC level, one call at a time |
