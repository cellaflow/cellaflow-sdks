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
import secrets
from collections import defaultdict
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

    def _ensure_session_and_sequence(self, session_id: str) -> int:
        """
        Ensures the session has been started with the CellaFlow Engine and
        initializes the next sequence number based on existing history if recovering.
        """
        if session_id not in self._next_sequence:
            try:
                logger.debug(
                    "Querying graph history for session %s to determine sequence",
                    session_id,
                )
                steps, _ = self.client.get_graph(session_id=session_id)
                if steps:
                    max_seq = max(cast(int, step["sequence"]) for step in steps)
                    self._next_sequence[session_id] = max_seq + 1
                    self._session_started.add(session_id)
                    logger.info(
                        "Resuming session %s at next sequence %d "
                        "from existing %d steps",
                        session_id,
                        self._next_sequence[session_id],
                        len(steps),
                    )
                else:
                    self._next_sequence[session_id] = 1
            except Exception as e:
                logger.debug(
                    "Unable to query existing graph for session %s: %s",
                    session_id,
                    e,
                )
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

    def _fetch_history(
        self, thread_id: str
    ) -> Tuple[
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

        try:
            logger.debug("Fetching graph history for thread %s", thread_id)
            steps, _ = self.client.get_graph(session_id=thread_id)
            logger.debug(
                "Successfully fetched %d steps for thread %s",
                len(steps),
                thread_id,
            )
        except Exception as e:
            logger.warning(
                "Failed to fetch graph history for thread %s: %s",
                thread_id,
                e,
            )
            return storage, blobs, writes

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
            checkpoint_id = max(
                ns_storage.keys(), key=lambda cid: ns_storage[cid][0]
            )

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
        seq = self._ensure_session_and_sequence(thread_id)

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
            self.client.commit_step(
                session_id=thread_id,
                sequence=seq,
                name=f"checkpoint:{checkpoint['id']}",
                status=common_pb2.STEP_STATUS_SUCCESS,
                output_payload=payload,
            )
            logger.debug(
                "Committed checkpoint %s for thread %s at sequence %d",
                checkpoint["id"],
                thread_id,
                seq,
            )
        except Exception as e:
            logger.error(
                "Failed to commit checkpoint %s for thread %s at sequence %d: %s",
                checkpoint["id"],
                thread_id,
                seq,
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
        seq = self._ensure_session_and_sequence(thread_id)

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
            self.client.commit_step(
                session_id=thread_id,
                sequence=seq,
                name=f"writes:{checkpoint_id}:{task_id}",
                status=common_pb2.STEP_STATUS_SUCCESS,
                output_payload=payload,
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
                "Failed to commit writes for task %s (checkpoint %s, thread %s) "
                "at sequence %d: %s",
                task_id,
                checkpoint_id,
                thread_id,
                seq,
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
            lambda: list(
                self.list(config, filter=filter, before=before, limit=limit)
            )
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
