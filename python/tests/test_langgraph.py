from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest.mock import MagicMock, patch
import grpc
import pytest

from cellaflow import CellaflowSaver, durable_tools, tool, tool_session_id
from cellaflow.context import get_context
from cellaflow.client import CellaflowClient
from cellaflow.serialization import serialize, deserialize
from cellaflow.v1.idempotency_pb2 import CACHE_STATUS_ACQUIRED, CACHE_STATUS_HIT
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


# grpc's stubs give RpcError type Any, so strict mode rejects the subclass.
class _RpcFailure(grpc.RpcError):  # type: ignore[misc]
    """A gRPC failure with a chosen status code, as the engine would raise it."""

    def __init__(self, code: Any, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> Any:
        return self._code

    def details(self) -> str:
        return self._details


def _FailedPrecondition(details: str) -> _RpcFailure:
    """What the engine returns when a commit is out of sequence order."""
    return _RpcFailure(grpc.StatusCode.FAILED_PRECONDITION, details)


class MockCellaflowClient:
    """In-memory mock of CellaflowClient mimicking engine gRPC behavior.

    Two behaviours here are load-bearing and were absent originally, which is
    why CEL-108 shipped: this double was *more capable* than the engine, so the
    suite passed against limits the real backend enforces.

    - ``get_graph`` pages. The engine returns at most ``limit`` events (default
      100, capped at 1000), oldest first, plus a cursor for the rest.
    - ``commit_step`` enforces ``sequence == current + 1`` and raises
      FAILED_PRECONDITION otherwise, as the session actor does.
    """

    #: Matches the engine's default page size (main.rs, GetGraph).
    DEFAULT_PAGE = 100
    MAX_PAGE = 1000

    def __init__(self) -> None:
        self.sessions: Dict[str, Dict[str, str]] = {}
        self.graphs: Dict[str, List[Dict[str, Any]]] = {}
        #: Every get_graph call, as (limit, cursor) — lets tests assert on paging.
        self.graph_calls: List[Tuple[Optional[int], Optional[str]]] = []
        #: Set to a status code to make get_graph fail, as the engine would.
        self.get_graph_fails_with: Optional[Any] = None

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

        # The engine's actor rejects anything that is not exactly the next
        # position, so a double that accepts any sequence hides sequence bugs.
        existing = self.graphs.setdefault(session_id, [])
        expected = (max(s["sequence"] for s in existing) + 1) if existing else 1
        if sequence != expected:
            raise _FailedPrecondition(
                f"Sequence mismatch: expected sequence {expected}, "
                f"but request specified {sequence}"
            )

        existing.append(
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
        self.graph_calls.append((limit, cursor))

        if self.get_graph_fails_with is not None:
            raise _RpcFailure(self.get_graph_fails_with, "injected failure")

        # The engine answers NOT_FOUND for a session it has never seen, rather
        # than an empty page, so a thread with no history is a distinct case
        # from a thread it could not read.
        if session_id not in self.graphs:
            raise _RpcFailure(
                grpc.StatusCode.NOT_FOUND, f"Session {session_id} not found"
            )

        page = (
            self.DEFAULT_PAGE if not limit or limit <= 0 else min(limit, self.MAX_PAGE)
        )
        start = int(cursor) if cursor else 1

        # Oldest first from `start`, exactly like the engine's forward seek over
        # the zero-padded event keyspace.
        ordered = sorted(self.graphs.get(session_id, []), key=lambda s: s["sequence"])
        remaining = [s for s in ordered if s["sequence"] >= start]

        steps = remaining[:page]
        # The cursor is the sequence of the first unread record, inclusive.
        next_cursor = (
            str(remaining[page]["sequence"]) if len(remaining) > page else None
        )
        return list(steps), next_cursor


def test_init_default_client() -> None:
    saver = CellaflowSaver(
        target="localhost:50051", workflow_id="test_wf", version="v2"
    )
    assert saver.workflow_id == "test_wf"
    assert saver.version == "v2"
    assert saver.client is not None


def test_missing_dependency_raises() -> None:
    with patch("cellaflow.langgraph.HAS_LANGGRAPH", False):
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

    # A read that fails is NOT an empty thread. This previously asserted None,
    # which is what made a transport failure look like a thread with no history
    # and start the run over. See CEL-109.
    mock_failing_client = MagicMock()
    mock_failing_client.get_graph.side_effect = RuntimeError("gRPC connection error")
    failing_saver = CellaflowSaver(client=cast(CellaflowClient, mock_failing_client))
    failing_config: RunnableConfig = {"configurable": {"thread_id": "failing-thread"}}
    with pytest.raises(RuntimeError, match="gRPC connection error"):
        failing_saver.get_tuple(failing_config)


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

    # A failed read during sequence initialisation must not resolve to 1.
    # This previously asserted seq == 1, which meant an unreadable engine
    # produced a confident guess at a position that is taken on any thread with
    # history. See CEL-109.
    err_client = MagicMock()
    err_client.get_graph.side_effect = RuntimeError("network down")
    err_client.start_session.side_effect = RuntimeError("session start failed")
    err_saver = CellaflowSaver(client=cast(CellaflowClient, err_client))
    with pytest.raises(RuntimeError, match="network down"):
        err_saver._ensure_session_and_sequence("err-thread")
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


def test_import_without_langgraph_dependency() -> None:
    """
    Verifies that cellaflow and cellaflow.langgraph import cleanly without raising
    TypeError or syntax errors when langgraph packages are missing from sys.modules.
    """
    import importlib
    import sys

    # Save original modules
    saved_modules = {
        k: sys.modules.get(k)
        for k in list(sys.modules.keys())
        if "langgraph" in k or "cellaflow" in k
    }

    try:
        # Mock absence of langgraph packages
        for k in list(sys.modules.keys()):
            if "cellaflow.langgraph" in k or "langgraph" in k:
                del sys.modules[k]

        for blocked_mod in [
            "langgraph",
            "langgraph.checkpoint",
            "langgraph.checkpoint.base",
            "langgraph.checkpoint.serde",
            "langgraph.checkpoint.serde.base",
        ]:
            sys.modules[blocked_mod] = None  # type: ignore[assignment]

        # Re-import cellaflow.langgraph dynamically
        import cellaflow.langgraph

        importlib.reload(cellaflow.langgraph)

        # Confirm module imported and HAS_LANGGRAPH is False
        assert cellaflow.langgraph.HAS_LANGGRAPH is False
        # Confirm instantiation raises clear ImportError
        with pytest.raises(ImportError, match="langgraph-checkpoint is required"):
            cellaflow.langgraph.CellaflowSaver()

    finally:
        # Restore sys.modules
        for k, v in saved_modules.items():
            if v is not None:
                sys.modules[k] = v
            elif k in sys.modules:
                del sys.modules[k]
        if "cellaflow.langgraph" in sys.modules:
            importlib.reload(sys.modules["cellaflow.langgraph"])


# ---------------------------------------------------------------------------
# CEL-108: a thread longer than one page of engine history
# ---------------------------------------------------------------------------
#
# The engine returns at most 100 events per GetGraph, oldest first, with a
# cursor for the rest. A saver that ignores the cursor sees only the beginning
# of a long thread, and is wrong about it in two separate ways.


def _long_thread(saver: CellaflowSaver, thread: str, n: int) -> None:
    """Writes `n` checkpoints to `thread` through one live saver."""
    parent = None
    for i in range(1, n + 1):
        cfg: RunnableConfig = {
            "configurable": {"thread_id": thread, "checkpoint_ns": ""}
        }
        if parent:
            cfg["configurable"]["checkpoint_id"] = parent
        ckpt = cast(
            Checkpoint,
            {
                "v": 1,
                "id": f"ckpt-{i:04d}",
                "ts": f"2026-08-28T00:00:{i % 60:02d}Z",
                "channel_values": {"n": i},
                "channel_versions": {"n": f"{i:032}.aaaaaaaa"},
                "versions_seen": {},
            },
        )
        saver.put(cfg, ckpt, CheckpointMetadata(), {"n": f"{i:032}.aaaaaaaa"})
        parent = f"ckpt-{i:04d}"


def test_reads_the_newest_checkpoint_past_one_page() -> None:
    """A reader must see the end of the thread, not the end of the first page."""
    client = MockCellaflowClient()
    thread = "long-thread"
    _long_thread(CellaflowSaver(client=cast(CellaflowClient, client)), thread, 130)

    # A fresh saver has no local cache and must rebuild purely from the engine.
    reader = CellaflowSaver(client=cast(CellaflowClient, client))
    tup = reader.get_tuple({"configurable": {"thread_id": thread}})

    assert tup is not None, "the thread has 130 checkpoints, so this cannot be empty"
    assert tup.checkpoint["id"] == "ckpt-0130", (
        "resumed from a stale checkpoint: only the first page of history was read, "
        "so LangGraph would re-execute every superstep after it"
    )


def test_a_new_saver_can_write_to_a_long_thread() -> None:
    """The severe half: a restart or a second replica must still be able to commit.

    A fresh saver seeds its sequence from the engine. Reading only the first page
    makes it propose a position that is long gone, and no amount of retrying can
    resolve that because every refresh re-reads the same page.
    """
    client = MockCellaflowClient()
    thread = "restartable"
    _long_thread(CellaflowSaver(client=cast(CellaflowClient, client)), thread, 130)

    successor = CellaflowSaver(client=cast(CellaflowClient, client))
    cfg: RunnableConfig = {
        "configurable": {
            "thread_id": thread,
            "checkpoint_ns": "",
            "checkpoint_id": "ckpt-0130",
        }
    }
    ckpt = cast(
        Checkpoint,
        {
            "v": 1,
            "id": "ckpt-0131",
            "ts": "2026-08-28T00:01:00Z",
            "channel_values": {"n": 131},
            "channel_versions": {"n": f"{131:032}.aaaaaaaa"},
            "versions_seen": {},
        },
    )

    successor.put(cfg, ckpt, CheckpointMetadata(), {"n": f"{131:032}.aaaaaaaa"})

    assert (
        max(s["sequence"] for s in client.graphs[thread]) > 130
    ), "a saver that did not build the thread could not append to it"


# ---------------------------------------------------------------------------
# CEL-109: an engine that cannot be read is not an empty thread
# ---------------------------------------------------------------------------
#
# `get_tuple` returning None tells LangGraph to start the run from the
# beginning. That is the right answer for a thread with no history and the
# most destructive possible answer for a thread it simply could not read.


def test_unreadable_engine_does_not_look_like_an_empty_thread() -> None:
    client = MockCellaflowClient()
    saver = CellaflowSaver(client=cast(CellaflowClient, client))

    cfg: RunnableConfig = {"configurable": {"thread_id": "has-state"}}
    ckpt = cast(
        Checkpoint,
        {
            "v": 1,
            "id": "cp-1",
            "ts": "2026-08-28T00:00:00Z",
            "channel_values": {"n": 1},
            "channel_versions": {"n": "1"},
            "versions_seen": {},
        },
    )
    saver.put(cfg, ckpt, CheckpointMetadata(), {"n": "1"})

    # The engine goes away. The thread still has state.
    client.get_graph_fails_with = grpc.StatusCode.UNAVAILABLE
    reader = CellaflowSaver(client=cast(CellaflowClient, client))

    with pytest.raises(grpc.RpcError):
        reader.get_tuple(cfg)


def test_a_thread_with_no_history_still_reads_as_empty() -> None:
    """The counterpart: NOT_FOUND is a real answer, not a failure.

    Every first run hits this, so treating it as an error would break the
    common path in the course of fixing the rare one.
    """
    client = MockCellaflowClient()
    saver = CellaflowSaver(client=cast(CellaflowClient, client))

    assert saver.get_tuple({"configurable": {"thread_id": "brand-new"}}) is None


def test_a_brand_new_thread_can_be_written() -> None:
    """Sequence seeding must survive NOT_FOUND for the same reason."""
    client = MockCellaflowClient()
    saver = CellaflowSaver(client=cast(CellaflowClient, client))

    cfg: RunnableConfig = {"configurable": {"thread_id": "first-run"}}
    ckpt = cast(
        Checkpoint,
        {
            "v": 1,
            "id": "cp-1",
            "ts": "2026-08-28T00:00:00Z",
            "channel_values": {"n": 1},
            "channel_versions": {"n": "1"},
            "versions_seen": {},
        },
    )
    saver.put(cfg, ckpt, CheckpointMetadata(), {"n": "1"})

    assert client.graphs["first-run"][0]["sequence"] == 1


def test_seeding_does_not_guess_a_position_it_could_not_read() -> None:
    """A failed seed read must not fall back to sequence 1.

    Position 1 is long taken on any thread with history, so guessing it commits
    into a collision and then spends the retry budget discovering that.
    """
    client = MockCellaflowClient()
    saver = CellaflowSaver(client=cast(CellaflowClient, client))

    cfg: RunnableConfig = {"configurable": {"thread_id": "existing"}}
    ckpt = cast(
        Checkpoint,
        {
            "v": 1,
            "id": "cp-1",
            "ts": "2026-08-28T00:00:00Z",
            "channel_values": {"n": 1},
            "channel_versions": {"n": "1"},
            "versions_seen": {},
        },
    )
    saver.put(cfg, ckpt, CheckpointMetadata(), {"n": "1"})

    # A new saver seeds from an engine that will not answer.
    client.get_graph_fails_with = grpc.StatusCode.UNAVAILABLE
    successor = CellaflowSaver(client=cast(CellaflowClient, client))

    with pytest.raises(grpc.RpcError):
        successor.put(cfg, ckpt, CheckpointMetadata(), {"n": "1"})


# ---------------------------------------------------------------------------
# durable_tools: leased tool calls inside LangGraph nodes
# ---------------------------------------------------------------------------


class LeasingMockClient(MockCellaflowClient):
    """Adds the lease arbitration `@tool` needs, keyed like the engine's cache.

    Grants a key once and answers HIT with the recorded result thereafter, which
    is the only part of the real arbitration these tests depend on.
    """

    def __init__(self) -> None:
        super().__init__()
        self.cache: Dict[str, Any] = {}
        self.granted: List[str] = []

    def check_idempotency_cache(
        self,
        agent_id: str,
        idempotency_key: str,
        wait_timeout_ms: Optional[int] = None,
        lease_ttl_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        sequence: Optional[int] = None,
    ) -> Any:
        resp = MagicMock()
        resp.HasField.return_value = False
        if idempotency_key in self.cache:
            resp.status = CACHE_STATUS_HIT
            resp.cached_result.output_payload = self.cache[idempotency_key]
            return resp
        self.granted.append(idempotency_key)
        resp.status = CACHE_STATUS_ACQUIRED
        resp.fencing_token = 1
        resp.heartbeat_interval_ms = 60000
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
    ) -> Any:
        if idempotency_key:
            self.cache[idempotency_key] = serialize(output_payload)
        return super().commit_step(
            session_id,
            sequence,
            name,
            status,
            output_payload,
            idempotency_key,
            idempotency_fencing_token,
        )

    def release_lease(
        self,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        reason: Optional[str] = None,
    ) -> Any:
        return MagicMock()

    def renew_lease(
        self, agent_id: str, idempotency_key: str, fencing_token: int, extend_ms: int
    ) -> Any:
        return MagicMock()


@dataclass
class PaymentState:
    amount: int = 0
    receipt: Dict[str, Any] = field(default_factory=dict)


def test_tool_session_is_derived_and_distinct_from_the_thread() -> None:
    """The two properties the whole mechanism rests on, asserted directly.

    Distinct, or the saver's checkpoints and the tool's steps compete for graph
    positions in one session and the loser is refused. Deterministic, or a
    restart derives a different idempotency key and the lease recognises
    nothing, which fails by repeating the side effect rather than by raising.
    """
    assert tool_session_id("t-1") != "t-1"
    assert tool_session_id("t-1") == tool_session_id("t-1")
    assert tool_session_id("t-1") != tool_session_id("t-2")


def test_tool_inside_a_node_resolves_the_enclosing_context() -> None:
    """The load-bearing claim: a @tool in a node body reaches the outer context.

    LangGraph runs sync nodes on a thread pool, and ContextVars do not cross
    threads on their own — this passes because LangGraph copies the calling
    context into the executor. If that ever changes, this test is how we find
    out, so it drives a real compiled graph rather than calling the node.
    """
    client = LeasingMockClient()
    seen: Dict[str, Any] = {}

    def _charge(amount: int) -> Dict[str, Any]:
        seen["session"] = get_context().session_id
        return {"charged": amount}

    charge = cast(Any, tool(tool_name="charge")(_charge))

    def node(state: PaymentState) -> Dict[str, Any]:
        return {"receipt": charge(state.amount)}

    graph = StateGraph(PaymentState)
    graph.add_node("pay", node)
    graph.add_edge(START, "pay")
    graph.add_edge("pay", END)
    app = graph.compile(
        checkpointer=CellaflowSaver(client=cast(CellaflowClient, client))
    )

    cfg: RunnableConfig = {"configurable": {"thread_id": "t-ctx"}}
    with patch("cellaflow.langgraph.CellaflowClient", return_value=client):
        with durable_tools(cfg):
            app.invoke(PaymentState(amount=2499), cfg)

    assert seen["session"] == tool_session_id("t-ctx")
    assert client.granted, "the tool never took a lease"


def test_a_tool_in_a_node_is_not_leased_without_the_helper() -> None:
    """Unwrapped, a node's tool call has no context and says so.

    Failing loudly here is the point: the alternative to a clear error is a tool
    that runs unleased and looks fine until a retry charges twice.
    """
    client = LeasingMockClient()

    def _charge(amount: int) -> Dict[str, Any]:
        return {"charged": amount}

    charge = cast(Any, tool(tool_name="charge")(_charge))

    def node(state: PaymentState) -> Dict[str, Any]:
        return {"receipt": charge(state.amount)}

    graph = StateGraph(PaymentState)
    graph.add_node("pay", node)
    graph.add_edge(START, "pay")
    graph.add_edge("pay", END)
    app = graph.compile(
        checkpointer=CellaflowSaver(client=cast(CellaflowClient, client))
    )

    cfg: RunnableConfig = {"configurable": {"thread_id": "t-bare"}}
    with pytest.raises(RuntimeError, match="No active workflow context"):
        app.invoke(PaymentState(amount=2499), cfg)


def test_durable_tools_does_not_seed_positional_replay() -> None:
    """Replaces a test that asserted the defect: it pinned that recovery seeded
    `replayed_steps` from history, which is correct for `@workflow` and wrong
    here.

    That replay answers the step at sequence N with whatever was committed at
    sequence N — sound only when the resumed run re-executes from the start.
    `@workflow` does; **LangGraph resume does not**. It restores from a
    checkpoint and re-runs only the pending node, so a later tool arrives at an
    earlier position and is answered with a different tool's result.

    Deduplication comes from the idempotency cache instead, which does not
    encode the position: under `SCOPE_SESSION_WIDE` the derived key sets
    `seq_part = "session_wide"`, so a resumed call derives the same key and the
    engine answers HIT wherever the counter happens to be.
    """
    client = LeasingMockClient()
    session = tool_session_id("t-replay")
    client.sessions[session] = {"workflow_id": "langgraph", "version": "1.0.0"}
    client.graphs[session] = [
        {
            "sequence": i,
            "name": "charge",
            "output_payload": {"result": {"charged": i}},
        }
        for i in range(1, 131)
    ]

    with patch("cellaflow.langgraph.CellaflowClient", return_value=client):
        with patch.object(client, "start_session") as start:
            start.return_value = MagicMock(
                session_id=session, version="1.0.0", is_recovered=True
            )
            cfg: RunnableConfig = {"configurable": {"thread_id": "t-replay"}}
            with durable_tools(cfg) as ctx:
                pass

    assert ctx.replayed_steps == {}, (
        "durable_tools seeded positional replay; a resumed LangGraph run reaches "
        "a later tool at an earlier position, so this answers it with the wrong "
        "tool's result"
    )


def test_a_second_tool_node_resumes_at_its_own_identity() -> None:
    """The case the original crash test missed.

    It used a single tool-bearing node, so the sequence lined up on resume by
    coincidence. With two, run 2 re-runs only the pending node — the second tool
    arrives at position 1, where the *first* tool's record sits.

    Asserts on the derived key rather than the counter: the key is what the
    engine deduplicates on, and it must be identical across both runs for the
    second tool despite the position differing.
    """
    from cellaflow.idempotency import IdempotencyScope, derive_idempotency_key

    session = tool_session_id("t-two")

    # Run 1: charge_card is the 2nd tool reached, so sequence 2.
    first_run = derive_idempotency_key(
        session,
        "1.0.0",
        2,
        "default",
        "charge_card",
        IdempotencyScope.SCOPE_SESSION_WIDE,
        None,
        "BK-77",
        2499,
    )
    # Run 2: LangGraph skips the completed node, so it is the 1st reached.
    second_run = derive_idempotency_key(
        session,
        "1.0.0",
        1,
        "default",
        "charge_card",
        IdempotencyScope.SCOPE_SESSION_WIDE,
        None,
        "BK-77",
        2499,
    )

    assert first_run == second_run, (
        "the key moved with the position, so a resumed run would not recognise "
        "the charge it already made"
    )

    # And the first tool must stay distinct, or resume would dedupe them together.
    other_tool = derive_idempotency_key(
        session,
        "1.0.0",
        1,
        "default",
        "reserve_seat",
        IdempotencyScope.SCOPE_SESSION_WIDE,
        None,
        "BK-77",
        2499,
    )
    assert other_tool != first_run, "two different tools collided on one key"


def test_durable_tools_accepts_a_config_or_a_bare_thread_id() -> None:
    """Only `configurable.thread_id` is read, so a bare id is equally complete."""
    client = LeasingMockClient()
    seen = []

    with patch("cellaflow.langgraph.CellaflowClient", return_value=client):
        cfg: RunnableConfig = {"configurable": {"thread_id": "t-forms"}}
        with durable_tools(cfg) as a:
            seen.append(a.session_id)
        with durable_tools("t-forms") as b:
            seen.append(b.session_id)

    assert seen[0] == seen[1] == tool_session_id("t-forms")


@pytest.mark.parametrize(
    "bad, exc, message",
    [
        ({"thread_id": "t-1"}, ValueError, "configurable.thread_id"),
        ({"configurable": {}}, ValueError, "configurable.thread_id"),
        ({"configurable": None}, ValueError, "configurable.thread_id"),
        (None, TypeError, "thread id string"),
        (42, TypeError, "thread id string"),
    ],
)
def test_durable_tools_rejects_a_malformed_config_by_name(
    bad: Any, exc: type, message: str
) -> None:
    """The raw failures were `KeyError: 'configurable'` and `TypeError: string
    indices must be integers` — neither names the thing that is missing, and
    both arrive from inside a context manager the caller just opened.
    """
    with pytest.raises(exc, match=message):
        with durable_tools(bad):
            pass


def test_a_thread_id_with_a_colon_still_gets_a_stable_session() -> None:
    """`user:123` is an ordinary way to namespace a thread; the engine reserves
    colons in session ids. Hashing keeps the property that matters — the same
    thread derives the same session — without making an engine storage detail
    into a constraint on what callers may name their threads.
    """
    got = tool_session_id("user:123")

    assert ":" not in got, "the engine rejects a session id containing a colon"
    assert got == tool_session_id("user:123"), "not stable across calls"
    assert got != tool_session_id("user:124"), "distinct threads collided"


def test_an_empty_thread_id_is_refused() -> None:
    """Silently accepting it maps every such caller onto one shared session,
    where unrelated runs would deduplicate against each other — a wrong answer
    that looks like a working one.
    """
    with pytest.raises(ValueError, match="non-empty string"):
        tool_session_id("")


def test_durable_tools_does_not_require_the_cellaflow_checkpointer() -> None:
    """Leased tools are independent of where checkpoints are stored.

    Worth pinning: it means a team already committed to PostgresSaver can adopt
    the leasing without moving their checkpoint storage, and it is what makes
    the colon handling useful today — `CellaflowSaver` uses the raw thread id as
    its session and so cannot accept one, but nothing here goes through it.
    """
    from langgraph.checkpoint.memory import MemorySaver

    client = LeasingMockClient()
    seen: Dict[str, Any] = {}

    def _charge(amount: int) -> Dict[str, Any]:
        seen["session"] = get_context().session_id
        return {"charged": amount}

    charge = cast(Any, tool(tool_name="charge")(_charge))

    def node(state: PaymentState) -> Dict[str, Any]:
        return {"receipt": charge(state.amount)}

    graph = StateGraph(PaymentState)
    graph.add_node("pay", node)
    graph.add_edge(START, "pay")
    graph.add_edge("pay", END)
    app = graph.compile(checkpointer=MemorySaver())

    cfg: RunnableConfig = {"configurable": {"thread_id": "tenant:acme/u-1"}}
    with patch("cellaflow.langgraph.CellaflowClient", return_value=client):
        with durable_tools(cfg):
            app.invoke(PaymentState(amount=2499), cfg)

    assert seen["session"] == tool_session_id("tenant:acme/u-1")
    assert client.granted, "the tool never took a lease"
