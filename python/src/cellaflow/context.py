from dataclasses import dataclass, field
from typing import Any, Dict
import contextvars

import grpc

from cellaflow.client import CellaflowClient


@dataclass
class WorkflowContext:
    client: CellaflowClient
    session_id: str
    workflow_version: str
    sequence: int = 0
    # Map sequence number to a deserialized step payload for fast replay lookups
    replayed_steps: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    # CEL-98: set when a cache hit left this counter possibly ahead of the
    # engine's. Resolved lazily at the start of the next step rather than
    # eagerly on the hit, so a workflow that ends on a hit — the common case,
    # where the shared tool is the last thing it does — pays nothing.
    pending_reconcile: bool = False

    def reconcile_sequence(self) -> int:
        """Re-align the local counter with the engine's after a cache hit.

        CEL-98. `@step` increments this counter before every step, but a cache
        hit returns *without committing*. The engine's `current_sequence`
        therefore did not advance while the local one did, and the next commit
        fails with `Sequence mismatch` — one step after the real cause.

        The same-session case survives on a coincidence rather than an
        invariant: a peer's commit advances the engine by exactly the amount
        this caller advanced locally, so the two happen to stay equal. Any
        asymmetry breaks it — a hit satisfied from a *different* session, or
        replicas that reached a shared tool after different numbers of steps.

        The SDK cannot distinguish those cases locally, because whether the
        engine advanced depends on *whose* commit satisfied the hit. So it asks
        rather than guesses.

        Cost: one round trip per 1000 committed steps, paid only on a hit, and
        only when the workflow continues afterwards. Payloads are not decoded —
        see `CellaflowClient.get_latest_sequence`. The durable fix is for the
        engine to return its current sequence on the hit itself, which needs
        `session_id` on `CheckCacheRequest` — the same proto field CEL-92
        requires. This is the SDK-only stand-in until that lands.
        """
        try:
            self.sequence = self.client.get_latest_sequence(self.session_id)
        except grpc.RpcError as exc:
            # A session the engine has no events for. Nothing to align to; it is
            # still expecting sequence 1.
            if exc.code() != grpc.StatusCode.NOT_FOUND:
                raise
            self.sequence = 0

        return self.sequence


_current_context: contextvars.ContextVar[WorkflowContext] = contextvars.ContextVar(
    "workflow_context"
)


def get_context() -> WorkflowContext:
    try:
        return _current_context.get()
    except LookupError:
        raise RuntimeError(
            "No active workflow context found. "
            "Are you calling a @step inside a @workflow?"
        )


def set_context(context: WorkflowContext) -> contextvars.Token[WorkflowContext]:
    return _current_context.set(context)


def reset_context(token: contextvars.Token[WorkflowContext]) -> None:
    _current_context.reset(token)
