"""
LangGraph Checkpointer Integration for CellaFlow.

This module provides `CellaflowSaver`, a drop-in persistence checkpointer
for LangGraph workflows backed by the CellaFlow Engine gRPC service.

Optional Dependency Semantics:
    `langgraph-checkpoint` (and `langgraph`) is an optional dependency.
    This module can be imported safely even when LangGraph is not installed in the
    runtime environment. However, attempting to instantiate `CellaflowSaver` without
    `langgraph-checkpoint` installed will raise a descriptive `ImportError`.

To enable LangGraph support:
    pip install cellaflow[langgraph]
    # or:
    pip install langgraph-checkpoint>=2.0.0 langgraph>=0.2.0
"""

import asyncio
import logging
import random
import time
import secrets
from collections import defaultdict
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Set,
    Tuple,
    cast,
)

import grpc
from cellaflow.client import CellaflowClient
from cellaflow.v1 import common_pb2

logger = logging.getLogger(__name__)

# How many times a checkpoint commit re-derives its sequence and retries
# before giving up.
#
# Appending to a strictly-sequenced log from N concurrent writers is inherently
# O(N) rounds: every writer reads the same position, all propose it, and exactly
# one wins per round. So the budget has to exceed the contention level rather
# than sit at some small constant -- at 8 attempts a 100-writer test loses about
# a third of its commits, and those losses look like an engine defect rather
# than an exhausted retry budget.
#
# The cost of a generous ceiling is bounded work on a path that only runs under
# contention. The cost of a tight one is spurious failures.
_MAX_COMMIT_ATTEMPTS = 128

#: How many consecutive rejections of the *same* sequence count as a stall
#: rather than contention. Contention advances the log, so a repeat means the
#: position is not moving and retrying cannot help. Kept well above the number
#: of writers that could plausibly collide in one round.
_STALLED_SEQUENCE_ATTEMPTS = 8

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.base import (
        WRITES_IDX_MAP,
        BaseCheckpointSaver,
        ChannelVersions,
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
        get_checkpoint_id,
        get_checkpoint_metadata,
    )
    from langgraph.checkpoint.serde.base import SerializerProtocol

    _BaseSaver = BaseCheckpointSaver[str]
    HAS_LANGGRAPH = True
else:
    try:
        from langchain_core.runnables import RunnableConfig
        from langgraph.checkpoint.base import (
            WRITES_IDX_MAP,
            BaseCheckpointSaver,
            ChannelVersions,
            Checkpoint,
            CheckpointMetadata,
            CheckpointTuple,
            get_checkpoint_id,
            get_checkpoint_metadata,
        )
        from langgraph.checkpoint.serde.base import SerializerProtocol

        _BaseSaver = BaseCheckpointSaver
        HAS_LANGGRAPH = True
    except ImportError:  # pragma: no cover

        class _BaseSaver:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

        HAS_LANGGRAPH = False
        RunnableConfig = Dict[str, Any]
        ChannelVersions = Dict[str, Any]
        Checkpoint = Dict[str, Any]
        CheckpointMetadata = Dict[str, Any]
        SerializerProtocol = Any
        WRITES_IDX_MAP = {}

        def CheckpointTuple(*args: Any, **kwargs: Any) -> Any:
            return tuple(args)

        def get_checkpoint_id(
            config: Optional[RunnableConfig],
        ) -> Optional[str]:
            return None

        def get_checkpoint_metadata(
            config: RunnableConfig, metadata: CheckpointMetadata
        ) -> CheckpointMetadata:
            return metadata


#: Distinguishes the session holding a thread's leased tool calls from the one
#: holding its checkpoints. They cannot be the same session: the saver and
#: `@step` advance *independent* sequence counters, so pointing both at one
#: session makes them compete for graph positions and the loser is refused.
#: They also cannot be unrelated, because a tool's idempotency key is derived
#: from its session id -- an id that changes between runs derives a different
#: key, and a lease that cannot recognise the earlier attempt does not prevent
#: the side effect from happening twice.
#:
#: Deriving one from the other satisfies both: distinct, and identical on every
#: run of the same thread.
_TOOL_SESSION_SUFFIX = "-cellaflow-tools"


def tool_session_id(thread_id: str) -> str:
    """Returns the session id holding `thread_id`'s leased tool calls.

    Deterministic, so a restart derives the same value and the lease taken
    before a crash is still recognised afterwards.
    """
    return f"{thread_id}{_TOOL_SESSION_SUFFIX}"


@contextmanager
def durable_tools(
    config: "RunnableConfig",
    *,
    workflow_id: str = "langgraph",
    version: str = "1.0.0",
    target: str = "localhost:50051",
    secure: bool = False,
    coordination_id: Optional[str] = None,
) -> Iterator[Any]:
    """Runs a LangGraph invocation so `@tool` calls inside nodes are leased.

    A `@tool` resolves its context from a `ContextVar`, and LangGraph copies the
    calling context into its executor, so a tool inside a node body reaches
    whatever context is active around `invoke`. This establishes one bound to
    the graph's own thread:

        with durable_tools(config):
            app.invoke({"ticket": "T-4417"}, config)

    which is what makes a node's side effect happen at most once across a crash
    and resume. Without it, a tool in a node either finds no context at all or
    -- if the graph is wrapped in a plain `@workflow` -- finds one whose session
    id is freshly generated per run, which derives a different idempotency key
    each time and so leases nothing across the restart that matters.

    Prefer this over hand-rolling the equivalent: the session derivation is the
    part that is easy to get wrong, and getting it wrong fails silently, by
    repeating the side effect rather than raising.

    Nested tool calls share one session per thread, so two concurrent
    invocations of the *same* thread contend for graph positions. That is the
    same constraint LangGraph itself imposes -- one execution per thread -- not
    an additional one.
    """
    from cellaflow.context import WorkflowContext, set_context, reset_context

    thread_id = config["configurable"]["thread_id"]
    session_id = tool_session_id(thread_id)

    client = CellaflowClient(target=target, secure=secure)
    resp = client.start_session(
        workflow_id=workflow_id, version=version, session_id=session_id
    )

    replayed_steps = {}
    if resp.is_recovered:
        steps, cursor = client.get_graph(session_id=resp.session_id)
        while True:
            for step_info in steps:
                replayed_steps[step_info["sequence"]] = {
                    "name": step_info["name"],
                    "payload": step_info["output_payload"],
                }
            if not cursor:
                break
            steps, cursor = client.get_graph(session_id=resp.session_id, cursor=cursor)

    ctx = WorkflowContext(
        client=client,
        session_id=resp.session_id,
        workflow_version=resp.version,
        sequence=0,
        replayed_steps=replayed_steps,
        coordination_id=coordination_id,
    )
    token = set_context(ctx)
    try:
        yield ctx
    finally:
        reset_context(token)


class CellaflowSaver(_BaseSaver):
    """
    Drop-in LangGraph checkpointer backed by the CellaFlow Engine gRPC service.

    Persists workflow checkpoints and intermediate task writes as immutable,
    sequence-ordered events with native MessagePack serialization. Provides
    full support for LangGraph multi-turn session persistence, state time travel,
    human-in-the-loop inspection, and distributed workflow resumption.

    Example:
        ```python
        from langgraph.graph import StateGraph, START, END
        from cellaflow import CellaflowSaver

        checkpointer = CellaflowSaver(target="localhost:50051")
        builder = StateGraph(state_schema=MyState)
        # ... add nodes & edges ...
        app = builder.compile(checkpointer=checkpointer)

        config = {"configurable": {"thread_id": "session-123"}}
        result = app.invoke({"input": "hello"}, config)
        ```
    """

    def __init__(
        self,
        client: Optional[CellaflowClient] = None,
        target: str = "localhost:50051",
        secure: bool = False,
        workflow_id: str = "langgraph_workflow",
        version: str = "v1",
        *,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        """
        Initializes the CellaflowSaver checkpointer.

        Args:
            client: Optional pre-configured `CellaflowClient` instance. If None,
                a client is automatically constructed using `target` and `secure`.
            target: gRPC target endpoint for the CellaFlow Engine.
            secure: Whether to use TLS encryption for gRPC channel communication.
            workflow_id: CellaFlow workflow identifier tag.
            version: Workflow schema version string.
            serde: Optional custom serializer conforming to `SerializerProtocol`.

        Raises:
            ImportError: If `langgraph-checkpoint` is not installed in the environment.
        """
        if not HAS_LANGGRAPH:
            raise ImportError(
                "langgraph-checkpoint is required to use CellaflowSaver. "
                "Install it with: pip install cellaflow[langgraph] or "
                "pip install langgraph-checkpoint"
            )
        super().__init__(serde=serde)
        self.client = (
            client
            if client is not None
            else CellaflowClient(target=target, secure=secure)
        )
        self.workflow_id = workflow_id
        self.version = version
        self._next_sequence: Dict[str, int] = {}
        self._session_started: Set[str] = set()

    def _iter_steps(self, session_id: str) -> Iterator[Dict[str, Any]]:
        """Yields every step of a session, following the engine's page cursor.

        `get_graph` returns a bounded page and a cursor for the rest, so a single
        call sees only the beginning of a long thread. The cursor it returns is
        the sequence of the first unread record and is inclusive, so passing it
        straight back resumes exactly where the previous page stopped.

        Yields rather than returning a list because the two sequence callers want
        only the highest sequence and have no reason to hold the whole thread in
        memory to find it.

        A thread the engine has never seen answers NOT_FOUND rather than an empty
        page, and that is a real answer: it means no history, which is what every
        first run looks like. Every *other* failure means the history could not be
        read, which is a different thing entirely and is allowed to propagate.
        Collapsing the two is what CEL-109 was.
        """
        cursor: Optional[str] = None
        yielded = False
        while True:
            try:
                steps, cursor = self.client.get_graph(
                    session_id=session_id, cursor=cursor
                )
            except grpc.RpcError as exc:
                # NOT_FOUND partway through means the session went away while we
                # were reading it, which is a failure rather than an empty thread.
                if exc.code() == grpc.StatusCode.NOT_FOUND and not yielded:
                    return
                raise

            for step in steps:
                yielded = True
                yield step
            if not cursor:
                return

    def _latest_sequence(self, session_id: str) -> int:
        """The session's highest committed sequence, or 0 if it has none."""
        latest = 0
        for step in self._iter_steps(session_id):
            seq = cast(int, step.get("sequence", 0))
            if seq > latest:
                latest = seq
        return latest

    def _ensure_session_and_sequence(self, session_id: str) -> int:
        """
        Ensures the session has been started with the CellaFlow Engine and
        initializes the next sequence number based on existing history if recovering.
        """
        if session_id not in self._next_sequence:
            logger.debug(
                "Querying graph history for session %s to determine sequence",
                session_id,
            )
            # Deliberately unguarded. Position 1 is already taken on any thread
            # with history, so guessing it after a failed read commits into a
            # collision and then spends the retry budget discovering that. A read
            # that failed means the position is unknown, and unknown is not 1.
            # _iter_steps already treats "session does not exist" as no history,
            # so reaching here with an error means a genuine read failure.
            latest = self._latest_sequence(session_id)
            if latest:
                self._next_sequence[session_id] = latest + 1
                self._session_started.add(session_id)
                logger.info(
                    "Resuming session %s at next sequence %d",
                    session_id,
                    self._next_sequence[session_id],
                )
            else:
                self._next_sequence[session_id] = 1

        if session_id not in self._session_started:
            try:
                self.client.start_session(
                    workflow_id=self.workflow_id,
                    version=self.version,
                    session_id=session_id,
                )
                self._session_started.add(session_id)
                logger.info(
                    "Successfully started session %s on CellaFlow Engine "
                    "(workflow=%s, version=%s)",
                    session_id,
                    self.workflow_id,
                    self.version,
                )
            except grpc.RpcError as rpc_err:
                if rpc_err.code() == grpc.StatusCode.ALREADY_EXISTS:
                    self._session_started.add(session_id)
                    logger.info(
                        "Session %s already exists on engine, continuing",
                        session_id,
                    )
                else:
                    logger.warning(
                        "Failed to start session %s on engine: %s",
                        session_id,
                        rpc_err,
                    )
            except Exception as err:
                logger.warning(
                    "Unexpected error starting session %s: %s",
                    session_id,
                    err,
                )

        seq = self._next_sequence[session_id]
        self._next_sequence[session_id] = seq + 1
        return seq

    def _refresh_sequence(self, session_id: str) -> int:
        """Re-reads the session's true position from the engine.

        `_next_sequence` is process-local. Concurrent savers on one
        thread each seed it from the same history and then increment
        independently, so they all propose the same sequence and the engine
        accepts exactly one. Re-reading is how a loser finds out where the log
        actually got to.
        """
        try:
            latest = self._latest_sequence(session_id)
        except Exception:  # pragma: no cover - engine unreachable
            return self._next_sequence.get(session_id, 1)

        self._next_sequence[session_id] = latest + 1
        return latest + 1

    def _commit_with_retry(
        self, session_id: str, name: str, payload: Dict[str, Any]
    ) -> int:
        """Commits, re-deriving the sequence and retrying on an ordering conflict.

        Concurrent writers on one thread are *not* duplicates. Eight
        savers writing eight different checkpoints should produce eight events
        at successive positions -- they are appending to a shared log, not
        contending over one operation. So deduplication is the wrong tool and no
        idempotency key is used here; what is needed is for a writer that lost
        the race to pick up the next free position and try again.

        That is optimistic concurrency control, which is exactly what the
        engine's `sequence == current + 1` check already implements. This
        supplies the missing half.

        Bounded, and on exhaustion the underlying error is re-raised rather than
        a synthesised one, so the cause stays visible.

        Retrying only helps while the position is genuinely moving. If a refresh
        keeps proposing the same rejected sequence the log is not advancing and
        further attempts cannot succeed, so the loop gives up early rather than
        spending its whole budget on a stall.
        """
        last_error: Optional[Exception] = None
        stalled_at: Optional[int] = None
        stalls = 0

        for attempt in range(_MAX_COMMIT_ATTEMPTS):
            seq = self._ensure_session_and_sequence(session_id)
            try:
                self.client.commit_step(
                    session_id=session_id,
                    sequence=seq,
                    name=name,
                    status=common_pb2.STEP_STATUS_SUCCESS,
                    output_payload=payload,
                )
                return seq
            except grpc.RpcError as exc:
                if exc.code() != grpc.StatusCode.FAILED_PRECONDITION:
                    raise
                last_error = exc

                # Under real contention the winner advances the log, so the next
                # refresh yields a different position. The same one coming back
                # repeatedly means nothing is moving.
                stalls = stalls + 1 if seq == stalled_at else 0
                stalled_at = seq
                if stalls >= _STALLED_SEQUENCE_ATTEMPTS:
                    raise RuntimeError(
                        f"Could not commit to session {session_id!r}: the engine "
                        f"rejected sequence {seq} on {stalls + 1} consecutive "
                        f"attempts and re-reading its position returned the same "
                        f"value each time, so retrying cannot resolve it. "
                        f"Engine said: {exc.details()}"
                    ) from exc

                logger.debug(
                    "Sequence %d taken for %s (attempt %d/%d); refreshing",
                    seq,
                    session_id,
                    attempt + 1,
                    _MAX_COMMIT_ATTEMPTS,
                )
                # Jitter before re-reading. Without it every loser wakes, reads
                # the same position and proposes it simultaneously, so each
                # round still admits one writer and the rest burn an attempt.
                time.sleep(random.uniform(0, 0.01 * min(attempt + 1, 10)))
                self._refresh_sequence(session_id)

        assert last_error is not None
        raise last_error

    def _fetch_history(self, thread_id: str) -> Tuple[
        Dict[
            str,
            Dict[
                str,
                Tuple[int, Tuple[str, bytes], Tuple[str, bytes], Optional[str]],
            ],
        ],
        Dict[Tuple[str, str, str, str], Tuple[str, bytes]],
        Dict[
            Tuple[str, str, str],
            Dict[Tuple[str, int], Tuple[str, str, Tuple[str, bytes], str]],
        ],
    ]:
        """
        Reconstructs checkpoints, blobs, and writes from the engine's step event ledger.
        """
        storage: Dict[
            str,
            Dict[
                str,
                Tuple[int, Tuple[str, bytes], Tuple[str, bytes], Optional[str]],
            ],
        ] = defaultdict(dict)
        blobs: Dict[Tuple[str, str, str, str], Tuple[str, bytes]] = {}
        writes: Dict[
            Tuple[str, str, str],
            Dict[Tuple[str, int], Tuple[str, str, Tuple[str, bytes], str]],
        ] = defaultdict(dict)

        # Deliberately unguarded. `get_tuple` returning nothing tells LangGraph to
        # start the run from the beginning, which is right for a thread with no
        # history and destructive for one that merely could not be read -- it
        # re-executes every node against state that already exists. A read failure
        # therefore has to reach the caller. `_iter_steps` already reports a
        # never-seen session as no history, so anything raising here is real.
        logger.debug("Fetching graph history for thread %s", thread_id)
        steps = list(self._iter_steps(thread_id))
        logger.debug(
            "Successfully fetched %d steps for thread %s",
            len(steps),
            thread_id,
        )

        for step in steps:
            payload = step.get("output_payload", {})
            if not isinstance(payload, dict):
                continue
            record_type = payload.get("record_type")
            step_seq = cast(int, step.get("sequence", 0))
            if record_type == "checkpoint":
                cp_id = cast(str, payload["checkpoint_id"])
                cp_ns = cast(str, payload.get("checkpoint_ns", ""))
                parent_id = cast(Optional[str], payload.get("parent_checkpoint_id"))
                ckpt_info = payload.get("checkpoint", {})
                meta_info = payload.get("metadata", {})
                ckpt_tuple = (
                    cast(str, ckpt_info.get("type", "msgpack")),
                    cast(bytes, ckpt_info.get("data", b"")),
                )
                meta_tuple = (
                    cast(str, meta_info.get("type", "msgpack")),
                    cast(bytes, meta_info.get("data", b"")),
                )
                storage[cp_ns][cp_id] = (step_seq, ckpt_tuple, meta_tuple, parent_id)

                for k, blob_info in payload.get("blobs", {}).items():
                    ver = str(blob_info.get("version", ""))
                    b_type = cast(str, blob_info.get("type", "empty"))
                    b_data = cast(bytes, blob_info.get("data", b""))
                    blobs[(thread_id, cp_ns, k, ver)] = (b_type, b_data)

            elif record_type == "writes":
                cp_id = cast(str, payload["checkpoint_id"])
                cp_ns = cast(str, payload.get("checkpoint_ns", ""))
                outer_key = (thread_id, cp_ns, cp_id)
                task_id = cast(str, payload.get("task_id", ""))
                task_path = cast(str, payload.get("task_path", ""))
                for w in payload.get("writes", []):
                    c = cast(str, w["channel"])
                    t = cast(str, w.get("type", "msgpack"))
                    d = cast(bytes, w.get("data", b""))
                    idx = cast(int, w.get("idx", 0))
                    writes[outer_key][(task_id, idx)] = (
                        task_id,
                        c,
                        (t, d),
                        task_path,
                    )

        return storage, blobs, writes

    def _load_blobs(
        self,
        blobs: Dict[Tuple[str, str, str, str], Tuple[str, bytes]],
        thread_id: str,
        checkpoint_ns: str,
        versions: ChannelVersions,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for k, ver in versions.items():
            key = (thread_id, checkpoint_ns, k, str(ver))
            if key not in blobs:
                continue
            t, data = blobs[key]
            if t == "empty":
                continue
            result[k] = self.serde.loads_typed((t, data))
        return result

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        storage, blobs, writes = self._fetch_history(thread_id)

        ns_storage = storage.get(checkpoint_ns, {})
        if not ns_storage:
            return None

        requested_id = get_checkpoint_id(config)
        if requested_id:
            if requested_id not in ns_storage:
                return None
            checkpoint_id = requested_id
        else:
            checkpoint_id = max(ns_storage.keys(), key=lambda cid: ns_storage[cid][0])

        (
            _seq,
            checkpoint_tuple,
            metadata_tuple,
            parent_checkpoint_id,
        ) = ns_storage[checkpoint_id]
        checkpoint_dict: Checkpoint = self.serde.loads_typed(checkpoint_tuple)
        metadata_dict: CheckpointMetadata = self.serde.loads_typed(metadata_tuple)

        channel_values = self._load_blobs(
            blobs,
            thread_id,
            checkpoint_ns,
            checkpoint_dict["channel_versions"],
        )
        writes_for_checkpoint = writes.get(
            (thread_id, checkpoint_ns, checkpoint_id), {}
        )

        pending_writes = [
            (tid, ch, self.serde.loads_typed(val_typed))
            for (tid, ch, val_typed, _) in writes_for_checkpoint.values()
        ]

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint={
                **checkpoint_dict,
                "channel_values": channel_values,
            },
            metadata=metadata_dict,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_checkpoint_id,
                    }
                }
                if parent_checkpoint_id
                else None
            ),
            pending_writes=pending_writes,
        )

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        if not config:
            return

        thread_id: str = config["configurable"]["thread_id"]
        config_checkpoint_ns = config["configurable"].get("checkpoint_ns")
        config_checkpoint_id = get_checkpoint_id(config)
        before_checkpoint_id = get_checkpoint_id(before) if before else None

        storage, blobs, writes = self._fetch_history(thread_id)

        count = 0
        for checkpoint_ns, ns_storage in storage.items():
            if (
                config_checkpoint_ns is not None
                and checkpoint_ns != config_checkpoint_ns
            ):
                continue

            before_seq = (
                ns_storage[before_checkpoint_id][0]
                if before_checkpoint_id and before_checkpoint_id in ns_storage
                else None
            )

            for checkpoint_id in sorted(
                ns_storage.keys(),
                key=lambda cid: ns_storage[cid][0],
                reverse=True,
            ):
                if config_checkpoint_id and checkpoint_id != config_checkpoint_id:
                    continue
                if (
                    before_seq is not None
                    and ns_storage[checkpoint_id][0] >= before_seq
                ):
                    continue

                (
                    _seq,
                    checkpoint_tuple,
                    metadata_tuple,
                    parent_checkpoint_id,
                ) = ns_storage[checkpoint_id]
                metadata_dict: CheckpointMetadata = self.serde.loads_typed(
                    metadata_tuple
                )

                if filter:
                    if not all(metadata_dict.get(k) == v for k, v in filter.items()):
                        continue

                checkpoint_dict: Checkpoint = self.serde.loads_typed(checkpoint_tuple)
                channel_values = self._load_blobs(
                    blobs,
                    thread_id,
                    checkpoint_ns,
                    checkpoint_dict["channel_versions"],
                )
                writes_for_checkpoint = writes.get(
                    (thread_id, checkpoint_ns, checkpoint_id), {}
                )

                pending_writes = [
                    (tid, ch, self.serde.loads_typed(val_typed))
                    for (tid, ch, val_typed, _) in writes_for_checkpoint.values()
                ]

                yield CheckpointTuple(
                    config={
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": checkpoint_id,
                        }
                    },
                    checkpoint={
                        **checkpoint_dict,
                        "channel_values": channel_values,
                    },
                    metadata=metadata_dict,
                    parent_config=(
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "checkpoint_ns": checkpoint_ns,
                                "checkpoint_id": parent_checkpoint_id,
                            }
                        }
                        if parent_checkpoint_id
                        else None
                    ),
                    pending_writes=pending_writes,
                )
                count += 1
                if limit is not None and count >= limit:
                    return

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        c = dict(checkpoint)
        values: Dict[str, Any] = cast(Dict[str, Any], c.pop("channel_values", {}))
        blobs_dict: Dict[str, Dict[str, Any]] = {}
        for k, v in new_versions.items():
            if k in values:
                t, b = self.serde.dumps_typed(values[k])
                blobs_dict[k] = {"type": t, "data": b, "version": str(v)}
            else:
                blobs_dict[k] = {"type": "empty", "data": b"", "version": str(v)}

        ckpt_type, ckpt_bytes = self.serde.dumps_typed(c)
        meta_type, meta_bytes = self.serde.dumps_typed(
            get_checkpoint_metadata(config, metadata)
        )

        payload = {
            "record_type": "checkpoint",
            "checkpoint_id": checkpoint["id"],
            "checkpoint_ns": checkpoint_ns,
            "parent_checkpoint_id": config["configurable"].get("checkpoint_id"),
            "checkpoint": {"type": ckpt_type, "data": ckpt_bytes},
            "metadata": {"type": meta_type, "data": meta_bytes},
            "new_versions": {k: str(v) for k, v in new_versions.items()},
            "blobs": blobs_dict,
        }

        try:
            seq = self._commit_with_retry(
                thread_id, f"checkpoint:{checkpoint['id']}", payload
            )
            logger.debug(
                "Committed checkpoint %s for thread %s at sequence %d",
                checkpoint["id"],
                thread_id,
                seq,
            )
        except Exception as e:
            # Deliberately does not claim a number of attempts. Most failures
            # here never entered the retry loop -- an unreachable engine raises
            # on the first call -- and naming the ceiling sends whoever reads
            # this looking for contention that did not happen.
            logger.error(
                "Failed to commit checkpoint %s for thread %s: %s",
                checkpoint["id"],
                thread_id,
                e,
            )
            raise

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id: str = config["configurable"]["checkpoint_id"]
        writes_list = []
        for idx, (c, v) in enumerate(writes):
            t, b = self.serde.dumps_typed(v)
            inner_idx = WRITES_IDX_MAP.get(c, idx)
            writes_list.append(
                {
                    "task_id": task_id,
                    "channel": c,
                    "type": t,
                    "data": b,
                    "task_path": task_path,
                    "idx": inner_idx,
                }
            )

        payload = {
            "record_type": "writes",
            "checkpoint_id": checkpoint_id,
            "checkpoint_ns": checkpoint_ns,
            "task_id": task_id,
            "task_path": task_path,
            "writes": writes_list,
        }

        try:
            seq = self._commit_with_retry(
                thread_id, f"writes:{checkpoint_id}:{task_id}", payload
            )
            logger.debug(
                "Committed %d writes for task %s (checkpoint %s, thread %s) "
                "at sequence %d",
                len(writes),
                task_id,
                checkpoint_id,
                thread_id,
                seq,
            )
        except Exception as e:
            logger.error(
                "Failed to commit writes for task %s (checkpoint %s, thread %s): %s",
                task_id,
                checkpoint_id,
                thread_id,
                e,
            )
            raise

    def delete_thread(self, thread_id: str) -> None:
        """
        Cleans up local session sequence cache.
        """
        logger.info("Clearing local thread sequence cache for thread %s", thread_id)
        self._next_sequence.pop(thread_id, None)
        self._session_started.discard(thread_id)

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await asyncio.to_thread(
            lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(
            self.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)

    def get_next_version(
        self, current: Optional[str], channel: Optional[str] = None
    ) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = secrets.token_hex(8)
        return f"{next_v:032}.{next_h}"
