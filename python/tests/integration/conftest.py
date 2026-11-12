"""Tests against a real Cellaflow engine.

Skipped unless `CELLAFLOW_TARGET` names one, for example:

    docker run -d -p 50051:50051 -e CELLAFLOW_ALLOW_UNAUTHENTICATED=true \\
        ghcr.io/cellaflow/cellaflow:0.3.0
    CELLAFLOW_TARGET=localhost:50051 pytest tests/integration

These check what a stubbed engine cannot: that the engine accepts the records
the SDK writes, refuses what it should, and that a resumed run replays what a
first run committed.
"""

import os
import uuid
from typing import Any, Iterator, List, Tuple

import pytest

from cellaflow import CellaflowClient

TARGET = os.environ.get("CELLAFLOW_TARGET")

#: How the engine reports a step's status.
STATUS = {2: "ok", 3: "FAILED"}


def pytest_collection_modifyitems(items: List[Any]) -> None:
    if TARGET:
        return
    skip = pytest.mark.skip(reason="needs an engine: set CELLAFLOW_TARGET")
    for item in items:
        if "tests/integration" in str(item.fspath).replace(os.sep, "/"):
            item.add_marker(skip)


@pytest.fixture
def target() -> str:
    assert TARGET
    return TARGET


@pytest.fixture
def unique() -> Iterator[str]:
    """A fresh suffix, so runs never share a session or a coordination id."""
    yield uuid.uuid4().hex[:8]


def history(target: str, session_id: str) -> List[Tuple[int, str, str]]:
    """A session's committed steps, as (position, name, ok or FAILED)."""
    steps, _ = CellaflowClient(target=target).get_graph(session_id)
    return [
        (s["sequence"], s["name"], STATUS.get(s["status"], str(s["status"])))
        for s in steps
    ]
