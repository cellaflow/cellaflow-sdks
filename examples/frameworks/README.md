# Framework integrations

The same dispatch agent, wired up three ways. Each directory is self-contained:
one `agent.py`, the model it runs on, and a README.

| Example | Framework |
| --- | --- |
| [`langgraph_agent/`](langgraph_agent/) | LangGraph |
| [`crewai_agent/`](crewai_agent/) | CrewAI |
| [`autogen_agent/`](autogen_agent/) | AutoGen |

All three do the same thing. The agent is asked to dispatch an order, looks it
up, and hands it to a courier. `lookup_order` is an ordinary function.
`dispatch_order` cannot be recalled once the courier has it, so it is leased:

```python
@leased(tool_name="dispatch_order")
def dispatch_order(order_id: str, courier: str) -> str:
    ...
```

and the run happens inside a session derived from the thread id the application
already has:

```python
with durable_tools(thread_id, target=ENGINE):
    ...
```

That is the whole integration in every one of them. `@leased` is
`cellaflow.tool`, imported under another name because two of these frameworks
have a `tool` decorator of their own.

## The one place they differ

Frameworks do not agree on where a tool runs, which decides how the session
reaches the tool body.

| Framework | Does the calling context reach the tool? | What the example does |
| --- | --- | --- |
| LangGraph | Yes, it is copied into the node executor | nothing further |
| CrewAI | Yes on the synchronous path | nothing further |
| AutoGen | No | relies on the open-session registry |

Where the context does not arrive, Cellaflow looks the session up in its
registry of open sessions, which is unambiguous while one session is open in the
process. Running several at once needs each tool to name its own, with
`session.bind()`. The AutoGen README has that form.

## Running them

Each needs an engine:

```bash
docker run -d --name cellaflow \
  -p 50051:50051 \
  -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \
  ghcr.io/cellaflow/cellaflow:latest
```

Then, in any of the three directories:

```bash
pip install -r requirements.txt
python agent.py
```

Set `CELLAFLOW_TARGET=host:port` for an engine somewhere else. Set
`OPENAI_API_KEY` to drive the agent with `gpt-4o-mini`; without one each example
falls back to a scripted model that calls the same tools in the same order, so
the integration is exercised either way.

## What these do not show

Each runs once and nothing crashes, so the output is what you would get with no
lease at all. These answer "how do I wire this up", not "what does it buy me".

For the second question, [`../at_most_once_proof`](../at_most_once_proof) runs
one LangGraph graph twice, identical apart from the lease, kills the process
between the confirmation and the checkpoint, and exits non-zero unless the
leased arm confirms once and the unleased arm confirms twice.
