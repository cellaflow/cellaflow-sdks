"""The SDK against a real engine that requires a token.

Skipped unless `CELLAFLOW_AUTH_TARGET` names such an engine and
`CELLAFLOW_AUTH_TEST_KEY` is its token, for example:

    docker run -d -p 50052:50051 -e CELLAFLOW_API_KEY=s3cret \\
        ghcr.io/cellaflow/cellaflow:0.2.0
    CELLAFLOW_TARGET=localhost:50051 CELLAFLOW_AUTH_TARGET=localhost:50052 \\
        CELLAFLOW_AUTH_TEST_KEY=s3cret pytest --no-cov tests/integration
"""

import os
from typing import Any, Iterator, Tuple

import grpc
import pytest

from cellaflow import durable_tools, execution_lease, step, tool, workflow

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

AUTH_TARGET = os.environ.get("CELLAFLOW_AUTH_TARGET")
AUTH_KEY = os.environ.get("CELLAFLOW_AUTH_TEST_KEY")

pytestmark = pytest.mark.skipif(
    not (AUTH_TARGET and AUTH_KEY),
    reason="needs an engine that requires a token: set CELLAFLOW_AUTH_TARGET "
    "and CELLAFLOW_AUTH_TEST_KEY",
)


@pytest.fixture
def secured() -> Iterator[Tuple[str, str]]:
    """The secured engine and its key. `tests/conftest.py` keeps the key out of
    the environment."""
    assert AUTH_TARGET and AUTH_KEY
    yield AUTH_TARGET, AUTH_KEY


def _greeting(target: str, api_key: Any) -> Any:
    @step
    def greet(name: str) -> str:
        return f"hello {name}"

    @workflow(version="1.0.0", target=target, api_key=api_key)
    def wf(name: str) -> str:
        return greet(name)  # type: ignore[no-any-return]

    return wf


def test_a_workflow_runs_with_the_right_key(
    secured: Tuple[str, str], unique: str
) -> None:
    target, key = secured
    wf: Any = _greeting(target, key)
    assert wf("ada", _session_id=f"auth-{unique}") == "hello ada"


@pytest.mark.parametrize("key", ["wrong-key", None], ids=["wrong key", "no key"])
def test_a_workflow_is_refused_without_the_right_key(
    secured: Tuple[str, str], unique: str, key: Any
) -> None:
    target, right_key = secured
    wf: Any = _greeting(target, key)
    with pytest.raises(grpc.RpcError) as refused:
        wf("ada", _session_id=f"auth-{unique}")
    assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert right_key not in str(refused.value)


def test_durable_tools_reads_the_key_from_the_environment(
    secured: Tuple[str, str], unique: str, monkeypatch: Any
) -> None:
    """So existing code works against a secured engine without changes."""
    target, key = secured
    monkeypatch.setenv("CELLAFLOW_API_KEY", key)

    @tool(tool_name="reserve_seat")
    def reserve_seat(flight: str) -> str:
        return f"seat on {flight}"

    thread = {"configurable": {"thread_id": f"auth-{unique}"}}
    with durable_tools(thread, target=target):
        assert reserve_seat("BA117") == "seat on BA117"


def test_an_execution_lease_runs_with_the_right_key(
    secured: Tuple[str, str], unique: str
) -> None:
    target, key = secured
    with execution_lease(
        f"task:{unique}", worker_id="w-1", target=target, api_key=key
    ) as lease:
        lease.check()
