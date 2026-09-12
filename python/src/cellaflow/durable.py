"""Leasing tool calls, for any agent framework.

`durable_tools` is not tied to LangGraph and never was: it needs a session id
and tools invoked while the block is open. It lived in `langgraph.py` because
that is where it was first written, which made it invisible to anyone using
CrewAI, AutoGen, LlamaIndex, the OpenAI Agents SDK, or a plain agent loop.

The LangGraph-specific piece is `CellaflowSaver`, which stays in `langgraph.py`
and genuinely does require the framework installed. Nothing here imports it.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, Iterator, Optional, cast

from cellaflow.client import CellaflowClient

logger = logging.getLogger(__name__)

#: Appended to a caller's thread id to derive the tool session, keeping it
#: distinct from whatever session a checkpointer may be using for the same
#: thread.
_TOOL_SESSION_SUFFIX = "-cellaflow-tools"


def tool_session_id(thread_id: str) -> str:
    """Returns the session id holding `thread_id`'s leased tool calls.

    Deterministic, so a restart derives the same value and the lease taken
    before a crash is still recognised afterwards.

    Thread ids are chosen by the application and colons are reserved by the
    engine's key layout, so an id containing one is hashed rather than rejected:
    `"user:123"` is an ordinary way to namespace a thread, and refusing it would
    turn an engine storage detail into a constraint on the caller's naming.
    Hashing keeps the one property that matters -- the same thread always
    derives the same session -- at the cost of a session id that no longer reads
    back as the thread's name.
    """
    if not isinstance(thread_id, str) or not thread_id:
        raise ValueError(
            f"thread_id must be a non-empty string, got {thread_id!r}. Every "
            f"thread needs its own id: an empty one would put unrelated runs in "
            f"a single session, where they would deduplicate against each other."
        )
    if ":" in thread_id:
        digest = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()[:32]
        return f"lgthread-{digest}{_TOOL_SESSION_SUFFIX}"
    return f"{thread_id}{_TOOL_SESSION_SUFFIX}"


def _thread_id_from(config: Any) -> str:
    """Pulls the thread id out of a LangGraph config, or takes one directly.

    Accepts the `config` already being passed to `invoke`, which is the common
    case, or a bare thread id for callers who have nothing else to hand. The
    errors here exist because the raw failures are `KeyError: 'configurable'`
    and `TypeError: string indices must be integers`, neither of which names the
    thing that is actually missing.
    """
    if isinstance(config, str):
        return config

    if not isinstance(config, Mapping):
        raise TypeError(
            f"durable_tools() needs the LangGraph config you pass to invoke(), "
            f"or a thread id string; got {type(config).__name__}. Usage: "
            f'durable_tools({{"configurable": {{"thread_id": "..."}}}}).'
        )

    configurable = config.get("configurable")
    if not isinstance(configurable, Mapping) or "thread_id" not in configurable:
        raise ValueError(
            "durable_tools() needs a config carrying configurable.thread_id -- "
            "the same one you pass to invoke(). The thread id is what the tool "
            "session is derived from, so there is nothing to bind the lease to "
            "without it."
        )
    return cast(str, configurable["thread_id"])


@contextmanager
def durable_tools(
    config: Any,
    *,
    workflow_id: str = "langgraph",
    version: str = "1.0.0",
    target: str = "localhost:50051",
    secure: bool = False,
    coordination_id: Optional[str] = None,
) -> Iterator[Any]:
    """Leases the `@tool` calls made inside this block.

    Not LangGraph-specific, despite living here. The contract is a session id --
    a LangGraph config, or a bare string -- and tools invoked while the block is
    open. Verified against LangGraph, LlamaIndex, the OpenAI Agents SDK, CrewAI
    (both paths) and AutoGen.

    Frameworks differ in where they run a tool. Some call it on the caller's
    context; some use `asyncio.to_thread`, which copies it; and some use
    `loop.run_in_executor`, which does not. Where the context is lost and exactly
    one session is open, it is recovered from the open-session registry. Where
    several are open there is nothing to disambiguate them, so the call raises
    and the caller should bind explicitly:

        with durable_tools(config) as session:
            ...
        # inside a tool the framework dispatched off-context:
        with session.bind():
            charge(order_id)


    LangGraph copies the calling context into its executor, so a tool inside a
    node body reaches whatever context is active around `invoke`:

        with durable_tools(config):
            app.invoke({"ticket": "T-4417"}, config)

    which is what makes a node's side effect happen at most once across a crash
    and resume. Without it, a tool in a node either finds no context at all or
    -- if the graph is wrapped in a plain `@workflow` -- finds one whose session
    id is freshly generated per run, which derives a different idempotency key
    each time and so leases nothing across the restart that matters.

    Prefer this over hand-rolling the equivalent: the session derivation is the
    part that is easy to get wrong, and getting it wrong fails silently, by
    repeating the side effect rather than raising.

    Nested tool calls share one session per thread, so two concurrent
    invocations of the *same* thread contend for graph positions. That is the
    same constraint LangGraph itself imposes -- one execution per thread -- not
    an additional one.

    `config` is normally the same mapping passed to `invoke`, and only
    `configurable.thread_id` is read from it. A bare thread id string is
    accepted too, for callers who have no config to hand.
    """
    from cellaflow.context import (
        WorkflowContext,
        set_context,
        reset_context,
        _register_session,
        _deregister_session,
    )

    thread_id = _thread_id_from(config)
    session_id = tool_session_id(thread_id)

    client = CellaflowClient(target=target, secure=secure)
    resp = client.start_session(
        workflow_id=workflow_id, version=version, session_id=session_id
    )

    # Deliberately no `replayed_steps`, which is the one thing that must not be
    # carried over from `@workflow`.
    #
    # That replay is *positional*: the step at sequence N is answered from
    # whatever was committed at sequence N, which is sound only when the resumed
    # run re-executes from the start. `@workflow` does. **LangGraph resume does
    # not** -- it restores from a checkpoint and re-runs only the pending node,
    # so a second run reaches a *later* tool at an *earlier* position. Seeding
    # positional history here made a two-node graph answer `charge_card` with
    # whatever `reserve_seat` committed at sequence 1, which the name check in
    # `_replay` turns into a permanently unresumable thread.
    #
    # Nothing is lost by omitting it. Under the default `SCOPE_SESSION_WIDE` the
    # derived key sets `seq_part = "session_wide"` (`idempotency.py`), so the key
    # does **not** encode the position: the resumed call derives the same key,
    # the engine answers `CACHE_STATUS_HIT` from the committed result, and the
    # tool body never runs. Deduplication comes from the idempotency cache, which
    # is position-independent, instead of from a counter that cannot stay aligned.
    #
    # A hit also arbitrates no graph position (the engine's `Hit` arm returns
    # untouched, CEL-92) and reports `current_sequence`, which
    # `reconcile_sequence` adopts before the next step -- so the counter
    # re-converges on the engine's view rather than drifting.
    ctx = WorkflowContext(
        client=client,
        session_id=resp.session_id,
        workflow_version=resp.version,
        sequence=0,
        coordination_id=coordination_id,
    )
    # Registered as well as set: the ContextVar covers frameworks that dispatch
    # tools on the calling context, and the registry covers those that do not.
    # See `cellaflow.context.get_context`.
    token = set_context(ctx)
    _register_session(ctx)
    try:
        yield ctx
    finally:
        _deregister_session(ctx)
        reset_context(token)
