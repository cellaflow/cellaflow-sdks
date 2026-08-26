from cellaflow.idempotency import derive_idempotency_key, IdempotencyScope


def test_derive_idempotency_key_deterministic() -> None:
    session = "sess-123"
    version = "1.0.0"
    seq = 1
    agent = "test-agent"
    tool = "my-tool"

    # Derive key with some basic inputs
    key1 = derive_idempotency_key(
        session,
        version,
        seq,
        agent,
        tool,
        IdempotencyScope.SCOPE_STEP_LOCAL,
        None,
        "hello",
        42,
        foo="bar",
        active=True,
    )

    # Derive exactly the same way
    key2 = derive_idempotency_key(
        session,
        version,
        seq,
        agent,
        tool,
        IdempotencyScope.SCOPE_STEP_LOCAL,
        None,
        "hello",
        42,
        foo="bar",
        active=True,
    )

    assert key1 == key2

    # Should have the correct format
    parts = key1.split(":")
    assert len(parts) == 6
    assert parts[0] == session
    assert parts[1] == version
    assert parts[2] == str(seq)
    assert parts[3] == agent
    assert parts[4] == tool

    # Check hash format (hex encoded 16 bytes = 32 characters)
    assert len(parts[5]) == 32


def test_derive_idempotency_key_dict_ordering() -> None:
    session = "sess-123"
    version = "1.0.0"
    seq = 1
    agent = "test-agent"
    tool = "my-tool"

    # RFC 8785 guarantees dictionary key ordering
    key1 = derive_idempotency_key(
        session,
        version,
        seq,
        agent,
        tool,
        IdempotencyScope.SCOPE_STEP_LOCAL,
        None,
        {"z": 1, "a": 2},
    )
    key2 = derive_idempotency_key(
        session,
        version,
        seq,
        agent,
        tool,
        IdempotencyScope.SCOPE_STEP_LOCAL,
        None,
        {"a": 2, "z": 1},
    )

    assert key1 == key2


def test_shared_scope_omits_session_so_sessions_converge() -> None:
    """CEL-99: the whole point -- different sessions, one key."""
    keys = {
        derive_idempotency_key(
            f"session-{i}",
            f"1.{i}.0",  # different workflow versions too
            i,
            f"agent-{i}",
            "write_endpoint",
            IdempotencyScope.SCOPE_SHARED,
            "ticket-4417",
            "payload",
        )
        for i in range(3)
    }
    assert len(keys) == 1, (
        "three sessions on three versions must derive one key under "
        f"SCOPE_SHARED; got {keys}"
    )


def test_shared_scope_separates_coordination_domains() -> None:
    """Different domains must not deduplicate -- this is what keeps it safe."""

    def key(domain: str) -> str:
        return derive_idempotency_key(
            "s",
            "1.0.0",
            1,
            "a",
            "send_email",
            IdempotencyScope.SCOPE_SHARED,
            domain,
            to="x@example.com",
        )

    assert key("tenant-a") != key("tenant-b"), (
        "identical calls in different domains must stay distinct, or one "
        "tenant's operation would suppress another's"
    )


def test_shared_scope_requires_a_coordination_id() -> None:
    """No default: a shared one would silently widen deduplication."""
    import pytest

    with pytest.raises(ValueError) as excinfo:
        derive_idempotency_key(
            "s",
            "1.0.0",
            1,
            "a",
            "t",
            IdempotencyScope.SCOPE_SHARED,
            None,
            "payload",
        )
    message = str(excinfo.value)
    assert "coordination_id" in message
    assert "_coordination_id" in message, "the error must say how to supply it"


def test_other_scopes_ignore_the_coordination_id() -> None:
    """Passing one must not change keys for the scopes that do not use it."""
    for scope in (
        IdempotencyScope.SCOPE_SESSION_WIDE,
        IdempotencyScope.SCOPE_AGENT_PRIVATE,
        IdempotencyScope.SCOPE_STEP_LOCAL,
    ):
        without = derive_idempotency_key(
            "s", "1.0.0", 1, "a", "t", scope, None, "payload"
        )
        with_id = derive_idempotency_key(
            "s", "1.0.0", 1, "a", "t", scope, "ticket-1", "payload"
        )
        assert without == with_id, f"{scope} must ignore coordination_id"


def test_shared_scope_still_separates_different_inputs() -> None:
    """Sharing a domain must not collapse genuinely different operations."""

    def key(payload: str) -> str:
        return derive_idempotency_key(
            "s",
            "1.0.0",
            1,
            "a",
            "write",
            IdempotencyScope.SCOPE_SHARED,
            "ticket-1",
            payload,
        )

    assert key("alpha") != key("beta")
