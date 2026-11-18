# AutoGen

A refund agent with two tools. The one that moves money is leased.

```bash
pip install -r requirements.txt
python agent.py
```

An engine has to be running. See [the examples README](../../README.md) for the
one-line Docker command, and set `CELLAFLOW_TARGET` if yours is not on
`localhost:50051`.

```
  engine: localhost:50051
  model:  scripted (no OPENAI_API_KEY set)
  thread: refund-f523bd54

   lookup_order(ORD-4417)
   issue_refund(ORD-4417, 2499)  <- the leased call

  agent: Refunded ORD-4417 in full.
  gateway hits: 1 refund(s)
```

## The integration

Two lines, both in `agent.py`.

**On the tool.** AutoGen takes plain Python functions and builds the schema from
the type hints and the docstring, both of which survive the decorator.

```python
@leased(tool_name="issue_refund")
def issue_refund(order_id: str, cents: int) -> str:
    """Refunds `cents` to the card that paid for `order_id`. Irreversible."""
```

`lookup_order` is not leased, because repeating a read costs nothing and a lease
on it would buy nothing.

**Around the run.** AutoGen has no config object to carry a thread id, so the
one your application already uses is passed straight in.

```python
with durable_tools(thread_id, target=ENGINE):
    result = await agent.run(task="Refund order ORD-4417 in full.")
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

def issue_refund(order_id: str, cents: int) -> str:
    with session.bind():
        return _issue_refund(order_id, cents)
```

LangGraph does not need this; it copies the context into its executor. The
[LangGraph example](../langgraph/) next door shows that form.

## What the model does

`model.py` picks it. With `OPENAI_API_KEY` set the agent runs on `gpt-4o-mini`.
Without one it runs on AutoGen's own `ReplayChatCompletionClient`, returning a
fixed sequence of turns: call `lookup_order`, call `issue_refund`, answer.

The replay client is not part of the integration. The agent really calls the
tools either way, and the leased tool really takes a lease; the only faked part
is the thing choosing which tool to call.

`max_tool_iterations` is set to 4 because the default of 1 stops after the first
batch of tool calls, and the agent needs the lookup result before it can ask for
the refund.

## What this example does not show

It runs once, cleanly. Nothing crashes, so the output looks the same as it would
with no lease at all. To see the lease answer a repeated call, run
[`../../at_most_once_proof`](../../at_most_once_proof), which kills the process
between the charge and the checkpoint and exits non-zero unless the leased arm
charges once.
