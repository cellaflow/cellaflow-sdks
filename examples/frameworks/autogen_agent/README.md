# AutoGen

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
  thread: dispatch-b9b08d0f

   lookup_order(ORD-4417)
   dispatch_order(ORD-4417, DHL)  <- the leased call

  agent: Dispatched ORD-4417 with DHL.
  courier hits: 1 dispatch(es)
```

## The integration

Two lines, both in `agent.py`.

**On the tool.** AutoGen takes plain Python functions and builds the schema from
the type hints and the docstring, both of which survive the decorator.

```python
@leased(tool_name="dispatch_order")
def dispatch_order(order_id: str, courier: str) -> str:
    """Hands `order_id` to `courier` for delivery. Cannot be recalled."""
```

`lookup_order` is not leased, because repeating a read costs nothing and a lease
on it would buy nothing.

**Around the run.** AutoGen has no config object to carry a thread id, so the
one your application already uses is passed straight in.

```python
with durable_tools(thread_id, target=ENGINE):
    result = await agent.run(task="Dispatch order ORD-4417 to the courier.")
```

The same thread always derives the same session, which is what makes the lease
survive a restart.

## One thing to know about AutoGen specifically

AutoGen runs a tool on an executor the caller's context does not reach, so the
`ContextVar` `durable_tools` sets is not visible inside the tool body. Cellaflow
falls back to its registry of open sessions, which resolves it as long as **one
session is open in the process**. That is the case above, and it is why the
plain form works.

With two or more open at once there is nothing to say which session a tool call
belongs to, and the call raises rather than guessing:

```
Ambiguous workflow context: 2 sessions are open (...)
```

If you run concurrent sessions in one process, keep the session and name it
inside the tool:

```python
with durable_tools(thread_id, target=ENGINE) as session:
    ...

def dispatch_order(order_id: str, courier: str) -> str:
    with session.bind():
        return _dispatch_order(order_id, courier)
```

LangGraph does not need this; it copies the context into its executor. The
[LangGraph example](../langgraph/) next door shows that form.

## What the model does

`model.py` picks it. With `OPENAI_API_KEY` set the agent runs on `gpt-4o-mini`.
Without one it runs on AutoGen's own `ReplayChatCompletionClient`, returning a
fixed sequence of turns: call `lookup_order`, call `dispatch_order`, answer.

The replay client is not part of the integration. The agent really calls the
tools either way, and the leased tool really takes a lease; the only faked part
is the thing choosing which tool to call.

`max_tool_iterations` is set to 4 because the default of 1 stops after the first
batch of tool calls, and the agent needs the lookup result before it can ask for
the dispatch.

## What this example does not show

It runs once, cleanly. Nothing crashes, so the output looks the same as it would
with no lease at all. To see the lease answer a repeated call, run
[`../../at_most_once_proof`](../../at_most_once_proof), which kills the process
between the confirmation and the checkpoint and exits non-zero unless the
leased arm confirms once.
