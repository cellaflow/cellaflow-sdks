from dataclasses import dataclass, field
from typing import Any, Dict
import contextvars

from cellaflow.client import CellaflowClient


@dataclass
class WorkflowContext:
    client: CellaflowClient
    session_id: str
    sequence: int = 0
    # Map sequence number to a deserialized step payload for fast replay lookups
    replayed_steps: Dict[int, Dict[str, Any]] = field(default_factory=dict)


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
