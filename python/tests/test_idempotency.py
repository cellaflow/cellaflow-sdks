from cellaflow.idempotency import derive_idempotency_key


def test_derive_idempotency_key_deterministic() -> None:
    session = "sess-123"
    version = "1.0.0"
    seq = 1
    agent = "test-agent"
    tool = "my-tool"

    # Derive key with some basic inputs
    key1 = derive_idempotency_key(
        session, version, seq, agent, tool, "hello", 42, foo="bar", active=True
    )

    # Derive exactly the same way
    key2 = derive_idempotency_key(
        session, version, seq, agent, tool, "hello", 42, foo="bar", active=True
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
    key1 = derive_idempotency_key(session, version, seq, agent, tool, {"z": 1, "a": 2})
    key2 = derive_idempotency_key(session, version, seq, agent, tool, {"a": 2, "z": 1})

    assert key1 == key2
