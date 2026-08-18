import hashlib
import rfc8785
from typing import Any


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
    *args: Any,
    **kwargs: Any,
) -> str:
    """
    Derives the canonical idempotency key for a step or tool execution.
    Format: [SessionID]:[WorkflowVersion]:[StepSequence]:
            [AgentID]:[ToolName]:[Hash(Inputs)]
    """
    inputs_hash = _hash_inputs(*args, **kwargs)
    return (
        f"{session_id}:{workflow_version}:{step_sequence}:"
        f"{agent_id}:{tool_name}:{inputs_hash}"
    )
