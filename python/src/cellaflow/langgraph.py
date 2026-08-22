import asyncio
import random
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

from cellaflow.client import CellaflowClient
from cellaflow.v1 import common_pb2

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
    except ImportError:  # pragma: no cover
        BaseCheckpointSaver = object
        RunnableConfig = Dict[str, Any]
        ChannelVersions = Dict[str, Any]
        Checkpoint = Dict[str, Any]
        CheckpointMetadata = Dict[str, Any]
        CheckpointTuple = Any
        SerializerProtocol = Any
        WRITES_IDX_MAP = {}

        def get_checkpoint_id(
            config: Optional[RunnableConfig],
        ) -> Optional[str]:
            return None

        def get_checkpoint_metadata(
            config: RunnableConfig, metadata: CheckpointMetadata
        ) -> CheckpointMetadata:
            return metadata


class CellaflowSaver(BaseCheckpointSaver[str]):
    """
    Drop-in LangGraph checkpointer backed by the CellaFlow Engine gRPC service.
    Persists checkpoints and intermediate task writes as immutable,
    sequence-ordered events with native MessagePack serialization.
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
        if BaseCheckpointSaver is object:
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
                steps, _ = self.client.get_graph(session_id=session_id)
                if steps:
                    max_seq = max(cast(int, step["sequence"]) for step in steps)
                    self._next_sequence[session_id] = max_seq + 1
                    self._session_started.add(session_id)
                else:
                    self._next_sequence[session_id] = 1
            except Exception:
                self._next_sequence[session_id] = 1

        if session_id not in self._session_started:
            try:
                self.client.start_session(
                    workflow_id=self.workflow_id,
                    version=self.version,
                    session_id=session_id,
                )
            except Exception:
                pass
            self._session_started.add(session_id)

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
                Tuple[Tuple[str, bytes], Tuple[str, bytes], Optional[str]],
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
                Tuple[Tuple[str, bytes], Tuple[str, bytes], Optional[str]],
            ],
        ] = defaultdict(dict)
        blobs: Dict[Tuple[str, str, str, str], Tuple[str, bytes]] = {}
        writes: Dict[
            Tuple[str, str, str],
            Dict[Tuple[str, int], Tuple[str, str, Tuple[str, bytes], str]],
        ] = defaultdict(dict)

        try:
            steps, _ = self.client.get_graph(session_id=thread_id)
        except Exception:
            return storage, blobs, writes

        for step in steps:
            payload = step.get("output_payload", {})
            if not isinstance(payload, dict):
                continue
            record_type = payload.get("record_type")
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
                storage[cp_ns][cp_id] = (ckpt_tuple, meta_tuple, parent_id)

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
            checkpoint_id = max(ns_storage.keys())

        checkpoint_tuple, metadata_tuple, parent_checkpoint_id = ns_storage[
            checkpoint_id
        ]
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

            for checkpoint_id in sorted(ns_storage.keys(), reverse=True):
                if config_checkpoint_id and checkpoint_id != config_checkpoint_id:
                    continue
                if before_checkpoint_id and checkpoint_id >= before_checkpoint_id:
                    continue

                checkpoint_tuple, metadata_tuple, parent_checkpoint_id = ns_storage[
                    checkpoint_id
                ]
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

        self.client.commit_step(
            session_id=thread_id,
            sequence=seq,
            name=f"checkpoint:{checkpoint['id']}",
            status=common_pb2.STEP_STATUS_SUCCESS,
            output_payload=payload,
        )

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

        self.client.commit_step(
            session_id=thread_id,
            sequence=seq,
            name=f"writes:{checkpoint_id}:{task_id}",
            status=common_pb2.STEP_STATUS_SUCCESS,
            output_payload=payload,
        )

    def delete_thread(self, thread_id: str) -> None:
        """
        Cleans up local session sequence cache.
        """
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

    def get_next_version(self, current: Optional[str], channel: None = None) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"
