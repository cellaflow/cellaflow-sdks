import hashlib
import rfc8785
from typing import Any


from enum import Enum


class IdempotencyScope(Enum):
    SCOPE_UNSPECIFIED = 0
    SCOPE_SESSION_WIDE = 1  # Shared across all agents in the session (Default)
    SCOPE_AGENT_PRIVATE = 2  # Isolated to the executing agent
    SCOPE_STEP_LOCAL = 3  # Isolated to the specific superstep / node


def _hash_inputs(*args: Any, **kwargs: Any) -> str:
    """
    Hashes the inputs using RFC 8785 Canonical JSON and SHA-256.
    Returns the first 16 bytes of the hash, hex-encoded (32 characters).
    """
    payload = {"args": list(args), "kwargs": kwargs}

    # Serialize to canonical JSON bytes according to RFC 8785
    canonical_bytes = rfc8785.dumps(payload)  # type: ignore[arg-type]

    # Hash using SHA-256
    hash_obj = hashlib.sha256(canonical_bytes)

    # Return the first 16 bytes hex-encoded
    return hash_obj.digest()[:16].hex()


def derive_idempotency_key(
    session_id: str,
    workflow_version: str,
    step_sequence: int,
    agent_id: str,
    tool_name: str,
    scope: IdempotencyScope,
    *args: Any,
    **kwargs: Any,
) -> str:
    """
    Derives the canonical idempotency key for a step or tool execution.
    Format varies based on IdempotencyScope.
    """
    inputs_hash = _hash_inputs(*args, **kwargs)

    seq_part = "session_wide"
    agent_part = "session_wide"

    if scope == IdempotencyScope.SCOPE_AGENT_PRIVATE:
        agent_part = agent_id
    elif scope == IdempotencyScope.SCOPE_STEP_LOCAL:
        seq_part = str(step_sequence)
        agent_part = agent_id

    return (
        f"{session_id}:{workflow_version}:{seq_part}:"
        f"{agent_part}:{tool_name}:{inputs_hash}"
    )
