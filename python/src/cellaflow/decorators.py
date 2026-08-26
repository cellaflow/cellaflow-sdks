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
            # CEL-99: identifies the shared work, not the session. Agents in
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
                    # CEL-102: keep the name, not just the payload. Replay is
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
            # CEL-99: identifies the shared work, not the session. Agents in
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
                    # CEL-102: keep the name, not just the payload. Replay is
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

    CEL-102: before this check the SDK matched on position alone, so a resumed
    run that took a different branch silently received a foreign step's output.
    The engine has never allowed this — CEL-71 added a content hash so a
    different step at a consumed sequence is rejected rather than answered — but
    the SDK's replay path predated that discipline.

    A record whose name is empty is treated as unverifiable and rejected, not
    replayed — see the comment on that branch for why no legitimate history
    reaches it.

    Matching is by step name. Two stronger options are unavailable rather than
    rejected:

    - The engine's `content_identity` covers the output payload, which is the
      step's *result* and therefore unknown before it runs. It cannot be
      computed at replay time by construction.
    - The derived idempotency key would additionally catch same-name-different-
      arguments, and `get_graph` appears to return it. But `commit_step` only
      populates the *request-level* `idempotency_key`, never
      `StepResult.idempotency_key`, so the recorded value is always absent and
      the comparison would silently pass. Worth revisiting once CEL-84 settles
      which field is authoritative.
    """
    recorded = ctx.replayed_steps[seq]
    recorded_name = recorded.get("name")

    # Fail closed on an unverifiable record. An earlier version of this check
    # skipped the comparison when the name was empty, justified as tolerating
    # history from before names were retained. That history does not exist:
    # `commit_step` has set `name` and `get_graph` has returned it since the
    # first commit of client.py, `@step` falls back to `func.__name__` so the
    # name can never be empty, the LangGraph saver builds it from an f-string,
    # and engine-generated TimerFired events carry their own. What CEL-102
    # describes as discarded was the SDK's in-memory index, not the engine's
    # record.
    #
    # Skipping the check therefore protected nothing, while reverting to exactly
    # the positional replay this exists to prevent — silently — whenever a name
    # arrived empty for any *other* reason. That is the failure the ticket calls
    # the worst of the family, reintroduced through the escape hatch meant to be
    # harmless.
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
        # CEL-98: a previous cache hit may have left this counter ahead of the
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
