from typing import TYPE_CHECKING, Any

from cellaflow.client import (
    ALLOW_PLAINTEXT_KEY_ENV_VAR,
    API_KEY_ENV_VAR,
    CellaflowClient,
    resolve_api_key,
)
from cellaflow.decorators import (
    workflow,
    step,
    tool,
    DivergentStepError,
    FailureNotReplayableError,
    NondeterministicWorkflowError,
    ReplayDivergedError,
)
from cellaflow.idempotency import IdempotencyScope

# durable_tools works with any framework, so it does not come through the
# LangGraph module.
from cellaflow.durable import durable_tools, tool_session_id
from cellaflow.execution import (
    execution_lease,
    async_execution_lease,
    task_lease,
    current_lease,
    LeaseHandle,
    LeaseNotAcquired,
    LeaseLostError,
)
from cellaflow.lease import MAX_LEASE_TTL_MS, MIN_LEASE_TTL_MS

if TYPE_CHECKING:
    # Imported for type checkers only. At runtime `__getattr__` below loads it
    # on demand, so a process that never touches LangGraph never imports it --
    # but mypy still sees the real class rather than `Any`.
    from cellaflow.langgraph import CellaflowSaver as CellaflowSaver

__all__ = [
    "durable_tools",
    "tool_session_id",
    "DivergentStepError",
    "NondeterministicWorkflowError",
    "ReplayDivergedError",
    "FailureNotReplayableError",
    "CellaflowClient",
    "ALLOW_PLAINTEXT_KEY_ENV_VAR",
    "API_KEY_ENV_VAR",
    "resolve_api_key",
    "workflow",
    "step",
    "tool",
    "IdempotencyScope",
    "CellaflowSaver",
    "execution_lease",
    "async_execution_lease",
    "task_lease",
    "current_lease",
    "LeaseHandle",
    "LeaseNotAcquired",
    "LeaseLostError",
    "MIN_LEASE_TTL_MS",
    "MAX_LEASE_TTL_MS",
]


def __getattr__(name: str) -> Any:
    """Loads `CellaflowSaver` only when something asks for it.

    Importing it eagerly pulled LangGraph's serde machinery into every process
    that imported `cellaflow`, including the ones using CrewAI or no framework
    at all. It is the one export that genuinely needs LangGraph installed, so it
    is the one that waits to be asked for.
    """
    if name == "CellaflowSaver":
        from cellaflow.langgraph import CellaflowSaver

        return CellaflowSaver
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
