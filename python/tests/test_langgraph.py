from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest.mock import MagicMock, patch
import pytest

from cellaflow import CellaflowSaver
from cellaflow.client import CellaflowClient
from cellaflow.serialization import serialize, deserialize
from langgraph.graph import StateGraph, START, END
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
)


@dataclass
class WorkflowState:
    count: int = 0
    path: List[str] = field(default_factory=list)


class MockCellaflowClient:
    """In-memory mock of CellaflowClient mimicking engine gRPC behavior."""

    def __init__(self) -> None:
        self.sessions: Dict[str, Dict[str, str]] = {}
        self.graphs: Dict[str, List[Dict[str, Any]]] = {}

    def start_session(
        self, workflow_id: str, version: str, session_id: Optional[str] = None
    ) -> MagicMock:
        sid = session_id or "default_session"
        self.sessions[sid] = {"workflow_id": workflow_id, "version": version}
        if sid not in self.graphs:
            self.graphs[sid] = []
        resp = MagicMock()
        resp.session_id = sid
        resp.version = version
        resp.is_recovered = False
        return resp

    def commit_step(
        self,
        session_id: str,
        sequence: int,
        name: str,
        status: Any,
        output_payload: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        idempotency_fencing_token: Optional[int] = None,
    ) -> MagicMock:
        if session_id not in self.graphs:
            self.graphs[session_id] = []

        # Validate MessagePack roundtrip fidelity
        packed = serialize(output_payload)
        unpacked = deserialize(packed)

        self.graphs[session_id].append(
            {
                "sequence": sequence,
                "name": name,
                "status": status,
                "output_payload": unpacked,
                "idempotency_key": idempotency_key,
            }
        )
        resp = MagicMock()
        resp.session_id = session_id
        resp.next_sequence = sequence + 1
        return resp

    def get_graph(
        self, session_id: str, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        steps = self.graphs.get(session_id, [])
        return list(steps), None


def test_init_default_client() -> None:
    saver = CellaflowSaver(
        target="localhost:50051", workflow_id="test_wf", version="v2"
    )
    assert saver.workflow_id == "test_wf"
    assert saver.version == "v2"
    assert saver.client is not None


def test_missing_dependency_raises() -> None:
    with patch("cellaflow.langgraph.BaseCheckpointSaver", object):
        with pytest.raises(ImportError, match="langgraph-checkpoint is required"):
            CellaflowSaver()


def test_put_and_get_tuple_basic() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "thread-1"}}
    checkpoint: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:00Z",
        "id": "cp-1",
        "channel_values": {"user": "Alice", "score": 10},
        "channel_versions": {"user": "1", "score": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    metadata: CheckpointMetadata = {
        "source": "input",
        "step": 1,
        "parents": {},
    }

    res_config = saver.put(config, checkpoint, metadata, {"user": "1", "score": "1"})
    assert res_config["configurable"]["checkpoint_id"] == "cp-1"
    assert res_config["configurable"]["thread_id"] == "thread-1"

    # Fetch back
    tup = saver.get_tuple(res_config)
    assert tup is not None
    assert tup.checkpoint["id"] == "cp-1"
    assert tup.checkpoint["channel_values"]["user"] == "Alice"
    assert tup.checkpoint["channel_values"]["score"] == 10
    assert tup.metadata["step"] == 1
    assert tup.parent_config is None


def test_put_multiple_and_recovery() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "thread-2"}}
    cp1: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:01Z",
        "id": "cp-1",
        "channel_values": {"val": 10},
        "channel_versions": {"val": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    saver.put(config, cp1, {"step": 1, "source": "input"}, {"val": "1"})

    config_with_parent: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-2",
            "checkpoint_id": "cp-1",
        }
    }
    cp2: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:02Z",
        "id": "cp-2",
        "channel_values": {"val": 20, "extra": "data"},
        "channel_versions": {"val": "2", "extra": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    saver.put(
        config_with_parent,
        cp2,
        {"step": 2, "source": "loop"},
        {"val": "2", "extra": "1"},
    )

    # Fetch latest without checkpoint_id
    latest_config: RunnableConfig = {"configurable": {"thread_id": "thread-2"}}
    latest = saver.get_tuple(latest_config)
    assert latest is not None
    assert latest.checkpoint["id"] == "cp-2"
    assert latest.checkpoint["channel_values"]["val"] == 20
    assert latest.checkpoint["channel_values"]["extra"] == "data"
    assert latest.parent_config is not None
    assert latest.parent_config["configurable"]["checkpoint_id"] == "cp-1"

    # Fetch historical cp-1 specifically
    hist_config: RunnableConfig = {
        "configurable": {"thread_id": "thread-2", "checkpoint_id": "cp-1"}
    }
    hist = saver.get_tuple(hist_config)
    assert hist is not None
    assert hist.checkpoint["id"] == "cp-1"
    assert hist.checkpoint["channel_values"]["val"] == 10
    assert "extra" not in hist.checkpoint["channel_values"]


def test_put_writes_and_retrieval() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "thread-3"}}
    cp1: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:00Z",
        "id": "cp-1",
        "channel_values": {"items": ["a"]},
        "channel_versions": {"items": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    saver.put(config, cp1, {"step": 1, "source": "input"}, {"items": "1"})

    # Put pending writes on cp-1
    write_config: RunnableConfig = {
        "configurable": {"thread_id": "thread-3", "checkpoint_id": "cp-1"}
    }
    saver.put_writes(
        write_config,
        [("items", "b"), ("items", "c")],
        task_id="task-100",
        task_path="path/to/task",
    )

    tup = saver.get_tuple(write_config)
    assert tup is not None
    assert tup.pending_writes is not None
    assert len(tup.pending_writes) == 2
    assert tup.pending_writes[0] == ("task-100", "items", "b")
    assert tup.pending_writes[1] == ("task-100", "items", "c")


def test_list_filters_and_pagination() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "thread-list"}}
    for i in range(1, 5):
        cp: Checkpoint = {
            "v": 1,
            "ts": f"2026-08-22T00:00:0{i}Z",
            "id": f"cp-{i:02d}",
            "channel_values": {"num": i},
            "channel_versions": {"num": str(i)},
            "versions_seen": {},
            "updated_channels": None,
        }
        step_config: RunnableConfig = {
            "configurable": {
                "thread_id": "thread-list",
                "checkpoint_id": f"cp-{i-1:02d}" if i > 1 else None,
            }
        }
        saver.put(
            step_config,
            cp,
            {"step": i, "source": "loop" if i % 2 == 0 else "input"},
            {"num": str(i)},
        )

    # List all
    all_items = list(saver.list(config))
    assert len(all_items) == 4
    assert [item.checkpoint["id"] for item in all_items] == [
        "cp-04",
        "cp-03",
        "cp-02",
        "cp-01",
    ]

    # Filter with limit
    limited = list(saver.list(config, limit=2))
    assert len(limited) == 2
    assert [item.checkpoint["id"] for item in limited] == ["cp-04", "cp-03"]

    # Filter by metadata
    inputs = list(saver.list(config, filter={"source": "input"}))
    assert len(inputs) == 2
    assert [item.checkpoint["id"] for item in inputs] == ["cp-03", "cp-01"]

    # Filter before
    before_config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-list",
            "checkpoint_id": "cp-03",
        }
    }
    before_cp3 = list(saver.list(config, before=before_config))
    assert [item.checkpoint["id"] for item in before_cp3] == ["cp-02", "cp-01"]

    # Specific checkpoint_id in config
    single_config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-list",
            "checkpoint_id": "cp-02",
        }
    }
    single = list(saver.list(single_config))
    assert len(single) == 1
    assert single[0].checkpoint["id"] == "cp-02"

    # Filter with different checkpoint_ns
    diff_ns_config: RunnableConfig = {
        "configurable": {
            "thread_id": "thread-list",
            "checkpoint_ns": "other-ns",
        }
    }
    diff_ns = list(saver.list(diff_ns_config))
    assert diff_ns == []

    # None config yields empty
    assert list(saver.list(None)) == []


def test_delete_thread() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    saver._next_sequence["thread-del"] = 5
    saver._session_started.add("thread-del")

    saver.delete_thread("thread-del")
    assert "thread-del" not in saver._next_sequence
    assert "thread-del" not in saver._session_started


def test_get_next_version() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)
    v1 = saver.get_next_version(None)
    assert v1.startswith("00000000000000000000000000000001.")

    v2 = saver.get_next_version(v1)
    assert v2.startswith("00000000000000000000000000000002.")

    v3 = saver.get_next_version(5)  # type: ignore[arg-type]
    assert v3.startswith("00000000000000000000000000000006.")


@pytest.mark.asyncio
async def test_async_methods() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "thread-async"}}
    cp: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:00Z",
        "id": "cp-async-1",
        "channel_values": {"msg": "async_hello"},
        "channel_versions": {"msg": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    metadata: CheckpointMetadata = {"step": 1, "source": "input"}

    # aput
    res_config = await saver.aput(config, cp, metadata, {"msg": "1"})
    assert res_config["configurable"]["checkpoint_id"] == "cp-async-1"

    # aput_writes
    await saver.aput_writes(res_config, [("msg", "async_write")], task_id="task-async")

    # aget_tuple
    tup = await saver.aget_tuple(res_config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["msg"] == "async_hello"
    assert tup.pending_writes is not None
    assert len(tup.pending_writes) == 1
    assert tup.pending_writes[0] == ("task-async", "msg", "async_write")

    # alist
    history = [item async for item in saver.alist(config)]
    assert len(history) == 1

    # adelete_thread
    await saver.adelete_thread("thread-async")
    assert "thread-async" not in saver._next_sequence


def test_get_tuple_empty_or_error() -> None:
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    # Empty thread
    empty_config: RunnableConfig = {"configurable": {"thread_id": "empty-thread"}}
    res = saver.get_tuple(empty_config)
    assert res is None

    # Unknown checkpoint_id requested
    valid_config: RunnableConfig = {"configurable": {"thread_id": "valid-thread"}}
    saver.put(
        valid_config,
        {
            "v": 1,
            "ts": "2026-08-22T00:00:00Z",
            "id": "cp-real",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
            "updated_channels": None,
        },
        {},
        {},
    )
    non_existent_config: RunnableConfig = {
        "configurable": {"thread_id": "valid-thread", "checkpoint_id": "non-existent"}
    }
    res_missing = saver.get_tuple(non_existent_config)
    assert res_missing is None

    # Error during get_graph
    mock_failing_client = MagicMock()
    mock_failing_client.get_graph.side_effect = RuntimeError("gRPC connection error")
    failing_saver = CellaflowSaver(client=cast(CellaflowClient, mock_failing_client))
    failing_config: RunnableConfig = {"configurable": {"thread_id": "failing-thread"}}
    res2 = failing_saver.get_tuple(failing_config)
    assert res2 is None


def test_recovery_from_existing_graph_and_exception_handling() -> None:
    raw_mock_client = MockCellaflowClient()
    # Pre-populate graph in engine
    raw_mock_client.graphs["pre-existing-thread"] = [
        {
            "sequence": 1,
            "name": "step-1",
            "status": 2,
            "output_payload": {"record_type": "unknown"},
        },
        {
            "sequence": 2,
            "name": "step-2",
            "status": 2,
            "output_payload": "invalid_payload_type",
        },
        {
            "sequence": 5,
            "name": "step-5",
            "status": 2,
            "output_payload": {
                "record_type": "checkpoint",
                "checkpoint_id": "cp-5",
                "checkpoint_ns": "",
                "checkpoint": {"type": "msgpack", "data": b""},
                "metadata": {"type": "msgpack", "data": b""},
                "blobs": {},
            },
        },
    ]

    # Initialize a new saver instance connecting to existing session
    new_saver = CellaflowSaver(client=cast(CellaflowClient, raw_mock_client))
    # put should determine seq = 6
    pre_existing_config: RunnableConfig = {
        "configurable": {"thread_id": "pre-existing-thread"}
    }
    res = new_saver.put(
        pre_existing_config,
        {
            "v": 1,
            "ts": "2026-08-22T00:00:06Z",
            "id": "cp-6",
            "channel_values": {},
            "channel_versions": {"empty_chan": "1"},
            "versions_seen": {},
            "updated_channels": None,
        },
        {},
        {"empty_chan": "1"},  # not in channel_values, triggers empty blob branch
    )
    assert res["configurable"]["checkpoint_id"] == "cp-6"
    assert new_saver._next_sequence["pre-existing-thread"] == 7

    # Test error in get_graph during sequence initialization
    err_client = MagicMock()
    err_client.get_graph.side_effect = RuntimeError("network down")
    err_client.start_session.side_effect = RuntimeError("session start failed")
    err_saver = CellaflowSaver(client=cast(CellaflowClient, err_client))
    seq = err_saver._ensure_session_and_sequence("err-thread")
    assert seq == 1
    assert "err-thread" not in err_saver._session_started


def test_stategraph_integration_and_resume() -> None:
    """
    End-to-end test with real LangGraph StateGraph, compiling with CellaflowSaver
    and executing multi-node branch cycles and session resumption.
    """
    raw_mock_client = MockCellaflowClient()
    saver = CellaflowSaver(client=cast(CellaflowClient, raw_mock_client))

    def node_a(state: WorkflowState) -> Dict[str, Any]:
        return {"count": state.count + 1, "path": state.path + ["A"]}

    def node_b(state: WorkflowState) -> Dict[str, Any]:
        return {"count": state.count * 2, "path": state.path + ["B"]}

    builder = StateGraph(WorkflowState)
    builder.add_node("node_a", node_a)
    builder.add_node("node_b", node_b)
    builder.add_edge(START, "node_a")
    builder.add_edge("node_a", "node_b")
    builder.add_edge("node_b", END)

    app = builder.compile(checkpointer=saver)

    config: RunnableConfig = {"configurable": {"thread_id": "integration-session-1"}}

    # Initial invocation
    out1 = cast(
        Dict[str, Any],
        app.invoke(WorkflowState(count=5, path=["start"]), config),
    )
    assert out1["count"] == 12  # (5 + 1) * 2
    assert out1["path"] == ["start", "A", "B"]

    # Verify commits were made to the mock engine
    steps = raw_mock_client.graphs["integration-session-1"]
    assert len(steps) > 0
    # Sequences should be strictly consecutive 1, 2, 3...
    sequences = [s["sequence"] for s in steps]
    assert sequences == list(range(1, len(steps) + 1))

    # Second invocation on the same thread (resume)
    out2 = cast(
        Dict[str, Any],
        app.invoke(WorkflowState(count=20, path=["resumed"]), config),
    )
    assert out2["count"] == 42  # (20 + 1) * 2
    assert out2["path"] == ["resumed", "A", "B"]

    # Verify history listing
    history = list(saver.list(config))
    assert len(history) >= 2
    latest_state = history[0].checkpoint["channel_values"]
    assert latest_state["count"] == 42
    assert latest_state["path"] == ["resumed", "A", "B"]


def test_non_lexicographical_checkpoint_ids() -> None:
    """
    Ensures that latest checkpoint selection and list ordering use the CellaFlow
    sequence number rather than lexicographical string comparison on checkpoint IDs.
    """
    mock_client = cast(CellaflowClient, MockCellaflowClient())
    saver = CellaflowSaver(client=mock_client)

    config: RunnableConfig = {"configurable": {"thread_id": "non-lex-thread"}}

    # First checkpoint with lexicographically larger ID 'zzz_older'
    cp1: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:01Z",
        "id": "zzz_older",
        "channel_values": {"version_name": "first"},
        "channel_versions": {"version_name": "1"},
        "versions_seen": {},
        "updated_channels": None,
    }
    saver.put(config, cp1, {"step": 1, "source": "input"}, {"version_name": "1"})

    # Second checkpoint with lexicographically smaller ID 'aaa_newer'
    cp2: Checkpoint = {
        "v": 1,
        "ts": "2026-08-22T00:00:02Z",
        "id": "aaa_newer",
        "channel_values": {"version_name": "second"},
        "channel_versions": {"version_name": "2"},
        "versions_seen": {},
        "updated_channels": None,
    }
    saver.put(
        {"configurable": {"thread_id": "non-lex-thread", "checkpoint_id": "zzz_older"}},
        cp2,
        {"step": 2, "source": "loop"},
        {"version_name": "2"},
    )

    # get_tuple should return 'aaa_newer' because sequence 2 > sequence 1
    latest = saver.get_tuple(config)
    assert latest is not None
    assert latest.checkpoint["id"] == "aaa_newer"
    assert latest.checkpoint["channel_values"]["version_name"] == "second"

    # list() should return ['aaa_newer', 'zzz_older'] in sequence order
    history = list(saver.list(config))
    assert len(history) == 2
    assert [cp.checkpoint["id"] for cp in history] == ["aaa_newer", "zzz_older"]
