# CrewAI

A dispatch agent with two tools. The one you cannot take back is leased.

## Starting the engine

The example needs one running. This is the whole of it:

```bash
docker run -d --name cellaflow \
  -p 50051:50051 \
  -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
  ghcr.io/cellaflow/cellaflow:latest
```

`CELLAFLOW_ALLOW_UNAUTHENTICATED` is not optional. The engine refuses to start
without either it or a token, so forgetting to configure auth cannot be how you
end up serving an open port. Do not carry that setting into anything reachable.

It takes a few seconds to come up. `docker ps` reports `(healthy)` when it is
ready:

```bash
docker ps --filter name=cellaflow
```

Afterwards, `docker rm -f cellaflow` takes it away again.

## Running the example

```bash
pip install -r requirements.txt
python agent.py
```

If your engine is somewhere other than `localhost:50051`, set
`CELLAFLOW_TARGET=host:port` and the example will use it.

```
  engine: localhost:50051
  model:  scripted (no OPENAI_API_KEY set)
  thread: dispatch-ddb2f74c

   lookup_order(ORD-4417)
   dispatch_order(ORD-4417, DHL)  <- the leased call

  agent: Dispatched ORD-4417 with DHL.
  courier hits: 1 dispatch(es)
```

## The integration

Two lines, both in `agent.py`.

**On the tool.** `@leased` is `cellaflow.tool`, renamed on import so it does not
collide with CrewAI's decorator of the same name.

```python
@crewai_tool("dispatch_order")
@leased(tool_name="dispatch_order")
def dispatch_order(order_id: str, courier: str) -> str:
    """Hands `order_id` to `courier` for delivery. Cannot be recalled."""
```

Read it bottom-up: Cellaflow leases the call, then CrewAI builds the schema the
agent sees. `lookup_order` is not leased, because repeating a read costs nothing
and a lease on it would buy nothing.

**Around the kickoff.** CrewAI has no config object to carry a thread id, so the
one your application already uses is passed straight in.

```python
with durable_tools(thread_id, target=ENGINE):
    result = crew.kickoff()
```

The same thread always derives the same session, which is what makes the lease
survive a restart.

## One thing to know about CrewAI specifically

CrewAI runs a tool on the caller's context on its **synchronous** path, which
`kickoff()` is, so the session opened around it reaches the tool body with
nothing further required.

Its asynchronous path does not carry the context. Where that happens Cellaflow
looks the session up in its registry of open sessions, which is unambiguous
while one session is open in the process. Running several at once needs each
tool to name its own:

```python
with durable_tools(thread_id, target=ENGINE) as session:
    ...

def dispatch_order(order_id: str, courier: str) -> str:
    with session.bind():
        return _dispatch_order(order_id, courier)
```

The [AutoGen example](../autogen_agent/) next door needs that fallback for every
call, not just concurrent ones, and its README says more about it.

## What the model does

`model.py` picks it. With `OPENAI_API_KEY` set the agent runs on `gpt-4o-mini`
through CrewAI's `LLM`. Without one it runs on a `BaseLLM` subclass returning a
fixed sequence of turns.

The two take different routes through CrewAI, which is worth knowing before you
read `model.py`. A model that advertises function calling is handed tool
schemas. One that does not, which includes the scripted one, gets CrewAI's ReAct
prompt and answers in its `Action:` / `Action Input:` text format. The tool is
invoked the same way at the end of both, and is leased either way.

The scripted model is not part of the integration. The agent really calls the
tools, and the leased tool really takes a lease; the only faked part is the
thing choosing which tool to call.

## Telemetry

CrewAI sends usage telemetry to its own collector by default, which is its
behaviour rather than anything this example adds. `CREWAI_DISABLE_TELEMETRY=true`
turns it off, as does `OTEL_SDK_DISABLED=true`.

## What this example does not show

It runs once, cleanly. Nothing crashes, so the output looks the same as it would
with no lease at all. To see the lease answer a repeated call, run
[`../../at_most_once_proof`](../../at_most_once_proof), which kills the process
between the confirmation and the checkpoint and exits non-zero unless the
leased arm confirms once.
