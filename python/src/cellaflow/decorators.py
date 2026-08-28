import functools
import inspect
import uuid
import asyncio
import time
import logging
from typing import Any, Callable, TypeVar, cast, Optional

import grpc

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
            # Identifies the shared work, not the session. Agents in
            # different sessions pass the same value to converge on one key.
            coordination_id = kwargs.pop("_coordination_id", None)

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__, version=version, session_id=session_id
            )

            replayed_steps = {}
            if resp.is_recovered:
                steps, _ = client.get_graph(resp.session_id)
                for step_info in steps:
                    # Keep the name, not just the payload. Replay is
                    # keyed on position, and without the name there is no way to
                    # tell a legitimate replay from a different step landing on
                    # the same index.
                    replayed_steps[step_info["sequence"]] = {
                        "name": step_info["name"],
                        "payload": step_info["output_payload"],
                    }

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
                return await func(*args, **kwargs)
            finally:
                reset_context(token)

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            session_id = kwargs.pop("_session_id", str(uuid.uuid4()))
            # Identifies the shared work, not the session. Agents in
            # different sessions pass the same value to converge on one key.
            coordination_id = kwargs.pop("_coordination_id", None)

            client = CellaflowClient(target=target, secure=secure)
            resp = client.start_session(
                workflow_id=func.__name__, version=version, session_id=session_id
            )

            replayed_steps = {}
            if resp.is_recovered:
                steps, _ = client.get_graph(resp.session_id)
                for step_info in steps:
                    # Keep the name, not just the payload. Replay is
                    # keyed on position, and without the name there is no way to
                    # tell a legitimate replay from a different step landing on
                    # the same index.
                    replayed_steps[step_info["sequence"]] = {
                        "name": step_info["name"],
                        "payload": step_info["output_payload"],
                    }

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
                return func(*args, **kwargs)
            finally:
                reset_context(token)

        if inspect.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        return cast(F, sync_wrapper)

    return decorator


class DivergentStepError(RuntimeError):
    """Raised when the engine refuses a lease because the position is committed.

    Two replicas of one agent reached the same step with *different* arguments.
    Different arguments hash to different idempotency keys, so from the engine's
    view these are unrelated operations and neither blocks the other — the
    divergence is only detectable against the graph position they both target.

    The first replica committed there. This one is refused *before* running its
    tool body, so only one replica reaches the side effect.
    was only rejected at commit time, after the irreversible action had already
    happened.

    The fix is in the workflow, not the call: whatever the replicas disagreed
    about must itself be a leased step, so they converge on one value before
    reaching the step that acts on it.
    """


class NondeterministicWorkflowError(RuntimeError):
    """Raised when a resumed run reaches a step that does not match history.

    Replay is positional: the step at sequence N is answered from whatever was
    committed at sequence N. That is only sound if the resumed run performs the
    same steps in the same order, which is why the workflow body must be
    deterministic given the session's inputs — all nondeterminism belongs inside
    step bodies, never in the code deciding what to call.

    Reaching this means the two diverged. Continuing would hand back another
    step's result, so the run stops instead.
    """


def _replay(ctx: WorkflowContext, seq: int, expected_name: str) -> Any:
    """Returns the recorded result at `seq`, or raises if it is a different step.

    Replay is positional, so the step recorded at a sequence is only the right
    answer if the resumed run reached that sequence by the same path. Matching on
    the step name establishes that before the recorded result is returned.

    A record whose name is empty is treated as unverifiable and rejected rather
    than replayed.
    """
    recorded = ctx.replayed_steps[seq]
    recorded_name = recorded.get("name")

    # Fail closed on an unverifiable record. Every step this SDK commits carries a
    # name -- `@step` falls back to `func.__name__`, the LangGraph saver builds
    # one, and engine-generated timer events carry their own -- so an empty name
    # means the record cannot be checked, not that checking is unnecessary.
    # Replaying it would be positional replay, which is what this guards against.
    if not recorded_name:
        raise NondeterministicWorkflowError(
            f"Cannot verify replay at sequence {seq}: the recorded step has no "
            f"name, so there is no way to confirm it is {expected_name!r} rather "
            f"than a different step at the same position. Every step this SDK "
            f"commits records its name, so this history did not come from it."
        )

    if recorded_name != expected_name:
        raise NondeterministicWorkflowError(
            f"Workflow diverged from its recorded history at sequence {seq}: "
            f"expected step {expected_name!r}, but {recorded_name!r} was "
            f"committed there. A resumed run must perform the same steps in the "
            f"same order — move any branching on model output, clocks, or "
            f"network reads inside a step so its result is replayed too."
        )

    payload = recorded.get("payload")
    if payload is None:
        return None
    return payload.get("result")


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
        # A cache hit may have left this counter ahead of the
        # engine's. Adopt the position it reported before claiming the next
        # sequence, or this commit fails the ordering check and names the wrong
        # step. Deferred to here rather than done on the hit itself so a workflow
        # that ends on a hit does no extra work.
        ctx.reconcile_sequence()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return _replay(ctx, seq, actual_tool_name)

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
                ctx.coordination_id,
                *args,
                **kwargs,
            )

        fencing_token = 0
        hb: Optional[LeaseHeartbeat] = None

        while True:
            try:
                resp = ctx.client.check_idempotency_cache(
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    session_id=ctx.session_id,
                    # Tell the engine where this call intends to write,
                    # so a lease at an already-committed position is refused
                    # before the tool body runs rather than after.
                    sequence=seq,
                )
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise DivergentStepError(
                        f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                        f"{exc.details()}"
                    ) from exc
                raise
            if resp.status == CACHE_STATUS_HIT:
                # This path returns without committing, so record where
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
        # A cache hit may have left this counter ahead of the
        # engine's. Adopt the position it reported before claiming the next
        # sequence, or this commit fails the ordering check and names the wrong
        # step. Deferred to here rather than done on the hit itself so a workflow
        # that ends on a hit does no extra work.
        ctx.reconcile_sequence()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return _replay(ctx, seq, actual_tool_name)

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
                ctx.coordination_id,
                *args,
                **kwargs,
            )

        fencing_token = 0
        hb: Optional[LeaseHeartbeat] = None

        while True:
            try:
                resp = ctx.client.check_idempotency_cache(
                    agent_id=agent_id,
                    idempotency_key=ikey,
                    session_id=ctx.session_id,
                    # Tell the engine where this call intends to write,
                    # so a lease at an already-committed position is refused
                    # before the tool body runs rather than after.
                    sequence=seq,
                )
            except grpc.RpcError as exc:
                if exc.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    raise DivergentStepError(
                        f"Step '{actual_tool_name}' at sequence {seq} was refused: "
                        f"{exc.details()}"
                    ) from exc
                raise
            if resp.status == CACHE_STATUS_HIT:
                # This path returns without committing, so record where
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
