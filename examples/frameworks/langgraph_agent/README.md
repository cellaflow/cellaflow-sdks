# LangGraph

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
  thread: dispatch-05484de3

   lookup_order(ORD-4417)
   dispatch_order(ORD-4417, DHL)  <- the leased call

  agent: Dispatched ORD-4417 with DHL.
  courier hits: 1 dispatch(es)
```

## The integration

Two lines, both in `agent.py`.

**On the tool.** `@leased` is `cellaflow.tool`, renamed on import so it does not
collide with LangChain's decorator of the same name.

```python
@langchain_tool
@leased(tool_name="dispatch_order")
def dispatch_order(order_id: str, courier: str) -> str:
    """Hands `order_id` to `courier` for delivery. Cannot be recalled."""
```

Read it bottom-up: Cellaflow leases the call, then LangChain gives the model its
schema. `lookup_order` is not leased, because repeating a read costs nothing and
a lease on it would buy nothing.

**Around the run.**

```python
config = {"configurable": {"thread_id": "dispatch-05484de3"}}

with durable_tools(config, target=ENGINE):
    app.invoke({"messages": [...]}, config)
```

`durable_tools` reads `configurable.thread_id` from the config you already pass
to `invoke`, and derives the tool session from it. The same thread always
reaches the same leases, which is what makes the lease survive a restart: a
resumed run derives the same session and the same key.

## Why it works without anything further

LangGraph copies the calling context into the executor it runs a node on, so a
tool called from the `tools` node sees the session opened around `invoke`. Not
every framework does this. The AutoGen example in this directory covers the case
where the context does not survive.

## What the model does

`model.py` picks it. With `OPENAI_API_KEY` set the agent runs on `gpt-4o-mini`.
Without one it runs on a scripted model that returns a fixed sequence of turns:
call `lookup_order`, call `dispatch_order`, answer.

The scripted model is not part of the integration. The agent really calls the
tools either way, and the leased tool really takes a lease; the only faked part
is the thing choosing which tool to call. That keeps the example runnable
without an account, which the rest of `examples/` also does.

## What this example does not show

It runs once, cleanly. Nothing crashes, so the output looks the same as it would
with no lease at all. To see the lease answer a repeated call, run
[`../../at_most_once_proof`](../../at_most_once_proof), which kills the process
between the confirmation and the checkpoint and exits non-zero unless the
leased arm confirms once.
