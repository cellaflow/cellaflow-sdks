import contextlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import contextvars
import threading

from cellaflow.client import CellaflowClient


@dataclass
class WorkflowContext:
    client: CellaflowClient
    session_id: str
    workflow_version: str
    sequence: int = 0
    # Map sequence number to the recorded step at that position, as
    # {"name": str, "payload": dict}. The name is retained so replay can
    # verify it is returning *this* step's result and not whatever happens to sit
    # at the same index.
    replayed_steps: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    # Names the work several agents are collaborating on -- a ticket, a
    # task, a tenant. Only SCOPE_SHARED reads it, and that scope requires it.
    coordination_id: Optional[str] = None
    # The session position the engine last reported, held from a cache hit
    # until the next step consumes it. See `reconcile_sequence`.
    _reported_sequence: Optional[int] = None

    def record_engine_sequence(self, sequence: int) -> None:
        """Notes the session position the engine reported alongside a cache hit.

        Held rather than applied immediately: a workflow whose last act is a
        shared tool — the common shape — should not pay for bookkeeping it will
        never use. `reconcile_sequence` consumes it at the start of the next
        step.
        """
        self._reported_sequence = sequence

    def reconcile_sequence(self) -> None:
        """Adopts the position the engine reported on the last cache hit.

        `@step` increments this counter before every step, but a cache hit
        returns *without committing*. The engine's sequence therefore did not
        advance while the local one did, and the next commit fails the ordering
        check — one step after the real cause.

        The same-session case survives on a coincidence rather than an
        invariant: a peer's commit advances the engine by exactly the amount
        this caller advanced locally, so the two happen to stay equal. Any
        asymmetry breaks it — a hit satisfied from a *different* session, or
        replicas that reached a shared tool after different numbers of steps.

        The SDK cannot tell those apart locally, because whether the engine
        advanced depends on *whose* commit satisfied the hit. The engine reports
        it on the response instead, so this costs nothing beyond a field read.

        A no-op when the engine reported nothing — an older engine predating the
        field. Behaviour then degrades to the original defect rather than to
        something new.
        """
        if self._reported_sequence is not None:
            self.sequence = self._reported_sequence
            self._reported_sequence = None

    @contextlib.contextmanager
    def bind(self) -> Iterator["WorkflowContext"]:
        """Re-establishes this context on the current thread or task.

        Needed when an agent framework dispatches a tool somewhere the caller's
        context does not reach -- `loop.run_in_executor` rather than
        `asyncio.to_thread`, say. With a single open session `get_context` can
        infer it; with several it cannot, and this is how the caller says which
        one the work belongs to:

            with durable_tools(config) as session:
                ...

            def my_tool(order_id):          # called by the framework
                with session.bind():
                    return charge(order_id)
        """
        token = set_context(self)
        try:
            yield self
        finally:
            reset_context(token)


_current_context: contextvars.ContextVar[WorkflowContext] = contextvars.ContextVar(
    "workflow_context"
)

#: Sessions with an open `durable_tools` block, newest last. Consulted only when
#: the ContextVar is unset.
#:
#: Agent frameworks do not agree on how a tool is dispatched. Some call it on the
#: calling context; some hand it to `asyncio.to_thread`, which copies the context;
#: and some use `loop.run_in_executor`, which does not. Measured: LlamaIndex and
#: the OpenAI Agents SDK propagate, CrewAI propagates on its sync path only, and
#: AutoGen does not propagate at all. On those paths the ContextVar a caller set
#: is simply not visible, and a `@tool` would fail despite being correctly wrapped.
#:
#: The ContextVar stays authoritative wherever it survives -- it is what keeps
#: concurrent sessions in one process apart. This list is the fallback for where
#: it does not, and it deliberately refuses to guess when more than one session
#: is open.
_open_sessions: List["WorkflowContext"] = []
_open_sessions_lock = threading.Lock()


def _register_session(context: WorkflowContext) -> None:
    with _open_sessions_lock:
        _open_sessions.append(context)


def _deregister_session(context: WorkflowContext) -> None:
    with _open_sessions_lock:
        for i in range(len(_open_sessions) - 1, -1, -1):
            if _open_sessions[i] is context:
                del _open_sessions[i]
                return


def get_context() -> WorkflowContext:
    try:
        return _current_context.get()
    except LookupError:
        pass

    with _open_sessions_lock:
        candidates = list(_open_sessions)

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise RuntimeError(
            "No active workflow context found. "
            "Are you calling a @step inside a @workflow, or a @tool inside "
            "durable_tools()?"
        )

    # Two or more sessions are open and the framework dispatched this tool off
    # the calling context, so there is nothing left to say which one it belongs
    # to. Picking either would attribute a side effect -- and its lease -- to the
    # wrong session, which is worse than refusing.
    sessions = ", ".join(c.session_id for c in candidates)
    raise RuntimeError(
        f"Ambiguous workflow context: {len(candidates)} sessions are open "
        f"({sessions}) and this call arrived without one.\n\n"
        "The agent framework dispatched this tool onto a thread that does not "
        "carry the caller's context -- `loop.run_in_executor` does this, unlike "
        "`asyncio.to_thread`. With a single open session that is recoverable; "
        "with several it is not.\n\n"
        "Bind the session explicitly around the call:\n\n"
        "    with durable_tools(config) as session:\n"
        "        ...\n"
        "    # inside the tool the framework dispatched:\n"
        "    with session.bind():\n"
        "        charge(order_id)"
    )


def set_context(context: WorkflowContext) -> contextvars.Token[WorkflowContext]:
    return _current_context.set(context)


def reset_context(token: contextvars.Token[WorkflowContext]) -> None:
    _current_context.reset(token)
