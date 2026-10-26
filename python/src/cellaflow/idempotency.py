import hashlib
import rfc8785
from typing import Any, Optional, Sequence


from enum import Enum


class IdempotencyScope(Enum):
    SCOPE_UNSPECIFIED = 0
    SCOPE_SESSION_WIDE = 1  # Shared across all agents in the session (Default)
    SCOPE_AGENT_PRIVATE = 2  # Isolated to the executing agent
    SCOPE_STEP_LOCAL = 3  # Isolated to the specific superstep / node
    # Shared across *sessions* within a declared coordination domain.
    # The only scope whose key omits session_id, so agents running different
    # workflows in different sessions can deduplicate one shared side effect.
    SCOPE_SHARED = 4


#: The longest coordination id the engine accepts, in UTF-8 bytes.
MAX_COORDINATION_ID_BYTES = 256


def normalize_coordination_id(value: Optional[str]) -> Optional[str]:
    """Applies the engine's rules for a coordination id, returning it unchanged.

    Empty means absent and returns `None`. A value that is not a string raises
    `TypeError`. Leading or trailing whitespace, colons, and values over 256
    bytes raise `ValueError`.

    The value is never trimmed or otherwise cleaned up. It is hashed into
    `SCOPE_SHARED` keys exactly as given, so a cleaned-up copy would record the
    work under one domain while deduplicating it under another.

    The engine applies the same rules, but a step's own coordination id only
    reaches it with the commit, after the step body has run. Checking here
    means an invalid value fails before the side effect rather than after it.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(
            f"coordination_id must be a string, got {type(value).__name__}; "
            f"pass str() of it to use it as one"
        )
    if not value:
        return None
    if value.strip() != value:
        raise ValueError(
            f"coordination_id {value!r} cannot be blank or have leading or "
            f"trailing whitespace"
        )
    if ":" in value:
        raise ValueError(f"coordination_id {value!r} cannot contain colons (:)")
    if len(value.encode("utf-8")) > MAX_COORDINATION_ID_BYTES:
        raise ValueError(f"coordination_id exceeds {MAX_COORDINATION_ID_BYTES} bytes")
    return value


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
    coordination_id: Optional[str],
    *args: Any,
    _shared_on: Optional[Sequence[str]] = None,
    **kwargs: Any,
) -> str:
    """
    Derives the canonical idempotency key for a step or tool execution.
    Format varies based on IdempotencyScope.
    """
    if _shared_on is not None:
        # Only the arguments that identify the shared work contribute.
        # Heterogeneous agents converge on one side effect precisely when they
        # disagree about everything else, so hashing everything they pass is the
        # one thing guaranteed to keep them apart.
        #
        # Underscored to keep it out of the caller's keyword namespace: every
        # other keyword here is a user argument being hashed, and a tool with a
        # parameter named `shared_on` would otherwise silently lose it.
        selected = {k: kwargs[k] for k in _shared_on if k in kwargs}
        inputs_hash = _hash_inputs(**selected)
    else:
        inputs_hash = _hash_inputs(*args, **kwargs)

    if scope == IdempotencyScope.SCOPE_SHARED:
        # The only scope that omits session_id, so agents in different
        # sessions converge on one key. It also omits workflow_version, because
        # the whole point is that *different* workflows share the operation and
        # they will not be on the same version.
        #
        # coordination_id is what keeps this from being too wide. Without it two
        # unrelated callers of send_email(to=X) would deduplicate, suppressing
        # one of them silently. It is required, and the caller must choose it.
        if not coordination_id:
            raise ValueError(
                "SCOPE_SHARED requires a coordination_id naming the work being "
                "shared -- a ticket, task, or tenant id. Pass it when invoking "
                "the workflow, e.g. my_agent(..., _coordination_id='ticket-4417'), "
                "or when calling the tool, e.g. refund(..., "
                "_coordination_id='customer-88'). It has no built-in default: a "
                "shared one would deduplicate unrelated callers that happen to "
                "make the same call."
            )
        return f"shared:{coordination_id}:{tool_name}:{inputs_hash}"

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


def record_idempotency_key(session_id: str, sequence: int, received_key: str) -> str:
    """The key a session records a result it received under, at `sequence`.

    The record is leased like any step, so the engine arbitrates its position: a
    record is refused at a position another lease has claimed, and a later lease
    is refused at a position the record holds. The key names the session and the
    position, so each session records its own copy, and the received result's
    key, so two different results contending for one position are told apart.
    The same record sent again finds its own completed entry.
    """
    digest = hashlib.sha256(received_key.encode("utf-8")).hexdigest()
    return f"{session_id}:record:{sequence}:{digest}"


def failure_idempotency_key(
    session_id: str, sequence: int, step_name: str, step_key: str
) -> str:
    """The key a session records a step's failure under, at `sequence`.

    Its own key, not the step's: completing the step's key with a failure would
    answer every later call with that key from the cache, so a retry of the tool
    would never run again. Leased like any step, so the engine arbitrates the
    position, as for `record_idempotency_key`.
    """
    digest = hashlib.sha256(f"{step_name}\n{step_key}".encode("utf-8")).hexdigest()
    return f"{session_id}:failure:{sequence}:{digest}"
