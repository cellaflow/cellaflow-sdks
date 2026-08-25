import functools
import inspect
import uuid
import asyncio
import time
import logging
from typing import Any, Callable, TypeVar, cast, Optional

from cellaflow.client import CellaflowClient
from cellaflow.context import WorkflowContext, set_context, reset_context, get_context
from cellaflow.idempotency import derive_idempotency_key, IdempotencyScope
from cellaflow.lease import LeaseHeartbeat
from cellaflow.serialization import deserialize
from cellaflow.v1.common_pb2 import STEP_STATUS_SUCCESS
from cellaflow.v1.idempotency_pb2 import (
    CACHE_STATUS_HIT,
    CACHE_STATUS_ACQUIRED,
    CACHE_STATUS_IN_PROGRESS,
)

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def workflow(
    version: str = "1.0.0", target: str = "localhost:50051", secure: bool = False
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            # Pop _session_id if provided for recovery, else generate new
            session_id = kwargs.pop("_session_id", str(uuid.uuid4()))

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__, version=version, session_id=session_id
            )

            replayed_steps = {}
            if resp.is_recovered:
                steps, _ = client.get_graph(resp.session_id)
                for step_info in steps:
                    replayed_steps[step_info["sequence"]] = step_info["output_payload"]

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
                workflow_version=resp.version,
                sequence=0,
                replayed_steps=replayed_steps,
            )
            token = set_context(ctx)
            try:
                return await func(*args, **kwargs)
            finally:
                reset_context(token)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            session_id = kwargs.pop("_session_id", str(uuid.uuid4()))

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__, version=version, session_id=session_id
            )

            replayed_steps = {}
            if resp.is_recovered:
                steps, _ = client.get_graph(resp.session_id)
                for step_info in steps:
                    replayed_steps[step_info["sequence"]] = step_info["output_payload"]

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
                workflow_version=resp.version,
                sequence=0,
                replayed_steps=replayed_steps,
            )
            token = set_context(ctx)
            try:
                return func(*args, **kwargs)
            finally:
                reset_context(token)

        if inspect.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        return cast(F, sync_wrapper)

    return decorator


def step(
    func: Optional[F] = None,
    *,
    idempotency_key: Optional[str] = None,
    agent_id: str = "default",
    tool_name: Optional[str] = None,
    scope: IdempotencyScope = IdempotencyScope.SCOPE_SESSION_WIDE,
) -> Any:
    if func is None:
        return functools.partial(
            step,
            idempotency_key=idempotency_key,
            agent_id=agent_id,
            tool_name=tool_name,
            scope=scope,
        )

    actual_tool_name = tool_name or func.__name__

    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # CEL-98: a previous cache hit may have left this counter ahead of the
        # engine's. Adopt the position it reported before claiming the next
        # sequence, or this commit fails the ordering check and names the wrong
        # step. Deferred to here rather than done on the hit itself so a workflow
        # that ends on a hit does no extra work.
        ctx.reconcile_sequence()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return ctx.replayed_steps[seq].get("result")

        # Derive idempotency key
        ikey = idempotency_key
        if not ikey:
            ikey = derive_idempotency_key(
                ctx.session_id,
                ctx.workflow_version,
                seq,
                agent_id,
                actual_tool_name,
                scope,
                *args,
                **kwargs,
            )

        fencing_token = 0
        hb: Optional[LeaseHeartbeat] = None

        while True:
            resp = ctx.client.check_idempotency_cache(
                agent_id=agent_id,
                idempotency_key=ikey,
                session_id=ctx.session_id,
            )
            if resp.status == CACHE_STATUS_HIT:
                # CEL-98: this path returns without committing, so record where
                # the engine says the session sits. The next step adopts it.
                if resp.HasField("current_sequence"):
                    ctx.record_engine_sequence(resp.current_sequence)
                if resp.cached_result and resp.cached_result.output_payload:
                    deserialized = deserialize(resp.cached_result.output_payload)
                    return deserialized.get("result")
                return None
            elif resp.status == CACHE_STATUS_IN_PROGRESS:
                retry_ms = resp.retry_after_ms or 1000
                logger.info(
                    "Step %s is in progress by another worker. Sleeping %d ms...",
                    actual_tool_name,
                    retry_ms,
                )
                await asyncio.sleep(retry_ms / 1000.0)
            elif resp.status == CACHE_STATUS_ACQUIRED:
                fencing_token = resp.fencing_token or 0
                interval_ms = resp.heartbeat_interval_ms or 5000
                hb = LeaseHeartbeat(
                    client=ctx.client,
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    fencing_token=fencing_token,
                    heartbeat_interval_ms=interval_ms,
                )
                hb.start_async()
                break
            else:
                break

        try:
            result = await func(*args, **kwargs)
            ctx.client.commit_step(
                session_id=ctx.session_id,
                sequence=seq,
                name=actual_tool_name,
                status=STEP_STATUS_SUCCESS,
                output_payload={"result": result},
                idempotency_key=ikey,
                idempotency_fencing_token=fencing_token,
            )
            return result
        except Exception as e:
            if fencing_token > 0:
                ctx.client.release_lease(
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    fencing_token=fencing_token,
                    reason="TOOL_ERROR",
                )
            raise e
        finally:
            if hb:
                await hb.stop_async()

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        # CEL-98: a previous cache hit may have left this counter ahead of the
        # engine's. Adopt the position it reported before claiming the next
        # sequence, or this commit fails the ordering check and names the wrong
        # step. Deferred to here rather than done on the hit itself so a workflow
        # that ends on a hit does no extra work.
        ctx.reconcile_sequence()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return ctx.replayed_steps[seq].get("result")

        # Derive idempotency key
        ikey = idempotency_key
        if not ikey:
            ikey = derive_idempotency_key(
                ctx.session_id,
                ctx.workflow_version,
                seq,
                agent_id,
                actual_tool_name,
                scope,
                *args,
                **kwargs,
            )

        fencing_token = 0
        hb: Optional[LeaseHeartbeat] = None

        while True:
            resp = ctx.client.check_idempotency_cache(
                agent_id=agent_id,
                idempotency_key=ikey,
                session_id=ctx.session_id,
            )
            if resp.status == CACHE_STATUS_HIT:
                # CEL-98: this path returns without committing, so record where
                # the engine says the session sits. The next step adopts it.
                if resp.HasField("current_sequence"):
                    ctx.record_engine_sequence(resp.current_sequence)
                if resp.cached_result and resp.cached_result.output_payload:
                    deserialized = deserialize(resp.cached_result.output_payload)
                    return deserialized.get("result")
                return None
            elif resp.status == CACHE_STATUS_IN_PROGRESS:
                retry_ms = resp.retry_after_ms or 1000
                logger.info(
                    "Step %s is in progress by another worker. Sleeping %d ms...",
                    actual_tool_name,
                    retry_ms,
                )
                time.sleep(retry_ms / 1000.0)
            elif resp.status == CACHE_STATUS_ACQUIRED:
                fencing_token = resp.fencing_token or 0
                interval_ms = resp.heartbeat_interval_ms or 5000
                hb = LeaseHeartbeat(
                    client=ctx.client,
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    fencing_token=fencing_token,
                    heartbeat_interval_ms=interval_ms,
                )
                hb.start_sync()
                break
            else:
                break

        try:
            result = func(*args, **kwargs)
            ctx.client.commit_step(
                session_id=ctx.session_id,
                sequence=seq,
                name=actual_tool_name,
                status=STEP_STATUS_SUCCESS,
                output_payload={"result": result},
                idempotency_key=ikey,
                idempotency_fencing_token=fencing_token,
            )
            return result
        except Exception as e:
            if fencing_token > 0:
                ctx.client.release_lease(
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    fencing_token=fencing_token,
                    reason="TOOL_ERROR",
                )
            raise e
        finally:
            if hb:
                hb.stop_sync()

    if inspect.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    return cast(F, sync_wrapper)


def tool(
    func: Optional[F] = None,
    *,
    idempotency_key: Optional[str] = None,
    agent_id: str = "default",
    tool_name: Optional[str] = None,
    scope: IdempotencyScope = IdempotencyScope.SCOPE_SESSION_WIDE,
) -> Any:
    """
    Decorator for tool steps that require idempotency tracking.
    Functionally identical to @step for now, but semantically distinct.
    """
    return step(
        func=func,
        idempotency_key=idempotency_key,
        agent_id=agent_id,
        tool_name=tool_name,
        scope=scope,
    )
