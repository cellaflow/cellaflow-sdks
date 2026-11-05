from typing import Any, Iterator

import pytest

from cellaflow.context import WorkflowContext, wait_for_record_writes


def pytest_configure(config: Any) -> None:
    config.addinivalue_line(
        "markers",
        "background_records: let a workflow's last failure record be written in "
        "the background, as in production, instead of before the workflow returns",
    )


@pytest.fixture(autouse=True)
def records_written_before_return(
    request: Any, monkeypatch: Any
) -> Iterator[None]:
    """A workflow's last failure record is written on a thread of its own, so
    the workflow's result never waits on it. Tests read the history and the log
    straight after the workflow returns, so here the write is waited for first.
    Tests of the background write itself opt out."""
    if request.node.get_closest_marker("background_records") is None:
        original = WorkflowContext._record_in_background

        def written_first(self: WorkflowContext) -> None:
            original(self)
            wait_for_record_writes()

        monkeypatch.setattr(WorkflowContext, "_record_in_background", written_first)
    yield
    wait_for_record_writes(timeout=10)
