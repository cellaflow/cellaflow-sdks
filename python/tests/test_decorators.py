import pytest
from unittest.mock import MagicMock
from typing import Any

from cellaflow.decorators import workflow, step
from cellaflow.v1 import service_pb2
from cellaflow.serialization import serialize


@pytest.fixture
def mock_stub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_client(mock_stub: MagicMock, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: mock_stub,
    )


def test_workflow_and_step_sync(mock_stub: MagicMock, mock_client: None) -> None:
    # Setup mock to return a normal start session
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step
    def my_step(x: int) -> int:
        return x * 2

    @workflow(version="1.0.0")
    def my_workflow(val: int) -> int:
        return my_step(val)

    # Execute workflow
    result = my_workflow(10)

    assert result == 20

    # Verify StartSession was called
    mock_stub.StartSession.assert_called_once()
    req = mock_stub.StartSession.call_args[0][0]
    assert req.workflow_id == "my_workflow"

    # Verify CommitStep was called for the step
    mock_stub.CommitStep.assert_called_once()
    commit_req = mock_stub.CommitStep.call_args[0][0]
    assert commit_req.session_id == "test-session"
    assert commit_req.step_result.sequence == 1
    assert commit_req.step_result.name == "my_step"


@pytest.mark.asyncio
async def test_workflow_and_step_async(mock_stub: MagicMock, mock_client: None) -> None:
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step
    async def my_step(x: int) -> int:
        return x * 2

    @workflow(version="1.0.0")
    async def my_workflow(val: int) -> int:
        return await my_step(val)

    # Execute workflow
    result = await my_workflow(10)

    assert result == 20
    mock_stub.StartSession.assert_called_once()
    mock_stub.CommitStep.assert_called_once()


def test_workflow_recovery_sync(mock_stub: MagicMock, mock_client: None) -> None:
    # Setup mock to return a recovered session
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=True
    )
    mock_stub.StartSession.return_value = mock_start

    # Mock get_graph to return a previously executed step 1
    from cellaflow.v1 import common_pb2

    mock_graph = service_pb2.GetGraphResponse(
        session_id="test-session",
        steps=[
            common_pb2.StepResult(
                sequence=1,
                name="my_step",
                status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                output_payload=serialize({"result": 99}),
            )
        ],
    )
    mock_stub.GetGraph.return_value = mock_graph

    executed = False

    @step
    def my_step(x: int) -> int:
        nonlocal executed
        executed = True
        return x * 2

    @workflow(version="1.0.0")
    def my_workflow(val: int) -> int:
        return my_step(val)

    # We pass the _session_id to trigger recovery on the same session
    result = my_workflow(10, _session_id="test-session")  # type: ignore[call-arg]

    # The result should be 99 from the mocked graph, not 20 from execution
    assert result == 99
    # The actual step function should not have been executed
    assert not executed

    # Verify CommitStep was NOT called because it was replayed
    mock_stub.CommitStep.assert_not_called()
