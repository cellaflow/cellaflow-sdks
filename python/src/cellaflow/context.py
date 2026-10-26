import contextlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import contextvars
import logging
import threading

import grpc

from cellaflow.client import CellaflowClient
from cellaflow.v1 import common_pb2, service_pb2

logger = logging.getLogger(__name__)


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
    # The session's default coordination domain: the ticket, task or tenant
    # several agents are collaborating on, as the engine stored it. Steps that
    # do not name their own are recorded under it, and SCOPE_SHARED derives its
    # keys from it.
    coordination_id: Optional[str] = None
    # The session position the engine last reported, held from a cache hit
    # until the next step consumes it. See `reconcile_sequence`.
    _reported_sequence: Optional[int] = None
    # Whether this run has accounted for every position the session holds up to
    # `sequence`: true for a fresh session, and for one `@workflow` replayed from
    # the start. False once the run resumes into history it did not replay, which
    # is what `durable_tools` and any hit-based resume do. Only while it holds can
    # a cache hit be recorded in this session without risking a second record of
    # a step the session already holds. See `defer_hit_record`.
    history_known: bool = True
    # A cache hit's result, waiting to be recorded at its position in this
    # session. Written when the next step starts. See `defer_hit_record`.
    _pending_hit: Optional[Dict[str, Any]] = None

    def record_engine_sequence(self, sequence: int) -> None:
        """Notes the session position the engine reported alongside a cache hit.

        Held rather than applied immediately: a workflow whose last act is a
        shared tool — the common shape — should not pay for bookkeeping it will
        never use. `reconcile_sequence` consumes it at the start of the next
        step.
        """
        self._reported_sequence = sequence

    def defer_hit_record(
        self,
        sequence: int,
        name: str,
        payload: Dict[str, Any],
        coordination_id: Optional[str],
    ) -> None:
        """Holds a cache hit's result to be recorded at `sequence` in this session.

        A hit satisfied from another session leaves nothing at its position in
        this one. Without a record there, the next step takes that position, and
        a later resume of this session finds that step where the shared one was
        expected and cannot replay. Recording the result keeps the session's
        history the same shape as the work it did.

        Deferred to the next step, like `record_engine_sequence`: the record only
        matters once something would otherwise take its position, so a workflow
        that ends on a hit still does no extra work.
        """
        self._pending_hit = {
            "sequence": sequence,
            "name": name,
            "payload": payload,
            "coordination_id": coordination_id,
        }

    def reconcile_sequence(self) -> None:
        """Records a deferred hit, then adopts the position the engine reported.

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
        pending = self._pending_hit
        if pending is not None:
            try:
                self.client.commit_step(
                    session_id=self.session_id,
                    sequence=pending["sequence"],
                    name=pending["name"],
                    status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                    output_payload=pending["payload"],
                    coordination_id=pending["coordination_id"],
                )
            except grpc.RpcError as exc:
                if exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
                    # Kept, so the step that hit this is retried with it. If the
                    # write did land, the engine takes an identical re-send at the
                    # same position as a retry.
                    raise
                # Another writer in this session took the position first. The run
                # no longer knows what the session holds: it stops recording hits
                # and continues from the session's real position.
                self._pending_hit = None
                self.history_known = False
                self.sequence = self._engine_position()
                logger.warning(
                    "Cellaflow: the result of %r was not recorded at position %d "
                    "of session %s, because another writer committed there first. "
                    "Continuing from position %d. A later resume of this session "
                    "may not replay that step.",
                    pending["name"],
                    pending["sequence"],
                    self.session_id,
                    self.sequence,
                )
                return
            self._pending_hit = None
            return
        if self._reported_sequence is not None:
            self.sequence = self._reported_sequence
            self._reported_sequence = None

    def _engine_position(self) -> int:
        """The session's highest committed position, read from its history."""
        highest = 0
        cursor: Optional[str] = None
        while True:
            steps, cursor = self.client.get_graph(
                self.session_id, limit=1000, cursor=cursor
            )
            for record in steps:
                highest = max(highest, int(record.get("sequence") or 0))
            if not cursor:
                return highest

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


def session_coordination_id(
    resp: service_pb2.StartSessionResponse, requested: Optional[str]
) -> Optional[str]:
    """The coordination domain a started or resumed session works under.

    The engine returns the value stored with the session, which is what lets a
    resumed agent that did not pass one adopt it and keep deriving the same
    shared keys as its peers. An engine that does not return the field keeps
    the requested value.
    """
    if resp.HasField("coordination_id"):
        return resp.coordination_id
    return requested


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
