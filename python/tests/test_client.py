import pytest
from unittest.mock import MagicMock
from typing import Any

from cellaflow.client import CellaflowClient
from cellaflow.v1 import service_pb2
from cellaflow.serialization import deserialize


@pytest.fixture
def mock_stub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client(mock_stub: MagicMock, monkeypatch: Any) -> CellaflowClient:
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: mock_stub,
    )
    client = CellaflowClient()
    return client


def test_start_session(client: CellaflowClient, mock_stub: MagicMock) -> None:
    mock_response = service_pb2.StartSessionResponse(
        session_id="test-session", version="v1", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_response

    resp = client.start_session("workflow-1", "v1")

    assert resp.session_id == "test-session"
    assert not resp.is_recovered

    # Verify request payload
    mock_stub.StartSession.assert_called_once()
    req = mock_stub.StartSession.call_args[0][0]
    assert isinstance(req, service_pb2.StartSessionRequest)
    assert req.workflow_id == "workflow-1"
    assert req.version == "v1"


def test_commit_step(client: CellaflowClient, mock_stub: MagicMock) -> None:
    mock_response = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_response

    state = {"result": "success", "count": 1}

    from cellaflow.v1.common_pb2 import STEP_STATUS_SUCCESS

    resp = client.commit_step(
        session_id="test-session",
        sequence=1,
        name="tool-1",
        status=STEP_STATUS_SUCCESS,
        output_payload=state,
    )

    assert resp.next_sequence == 2

    mock_stub.CommitStep.assert_called_once()
    req = mock_stub.CommitStep.call_args[0][0]

    assert isinstance(req, service_pb2.CommitStepRequest)
    assert req.session_id == "test-session"
    assert req.step_result.sequence == 1
    assert req.step_result.name == "tool-1"
    assert req.step_result.status == STEP_STATUS_SUCCESS

    # Verify that the state payload was converted to MessagePack
    deserialized_state = deserialize(req.step_result.output_payload)
    assert deserialized_state == state


def test_get_graph(client: CellaflowClient, mock_stub: MagicMock) -> None:
    from cellaflow.v1 import common_pb2
    from cellaflow.serialization import serialize

    mock_response = service_pb2.GetGraphResponse(
        session_id="test-session",
        steps=[
            common_pb2.StepResult(
                sequence=1,
                name="t1",
                status=common_pb2.STEP_STATUS_SUCCESS,
                output_payload=serialize({"msg": "hello"}),
            )
        ],
        next_cursor="cursor-token",
    )
    mock_stub.GetGraph.return_value = mock_response

    steps, next_cursor = client.get_graph("test-session")

    assert next_cursor == "cursor-token"
    assert len(steps) == 1

    step = steps[0]
    assert step["sequence"] == 1
    assert step["name"] == "t1"
    assert step["status"] == common_pb2.STEP_STATUS_SUCCESS
    assert step["output_payload"] == {"msg": "hello"}

    mock_stub.GetGraph.assert_called_once()
    req = mock_stub.GetGraph.call_args[0][0]
    assert isinstance(req, service_pb2.GetGraphRequest)
    assert req.session_id == "test-session"
