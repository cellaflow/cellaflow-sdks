import functools
import inspect
import uuid
from typing import Any, Callable, TypeVar, cast

from cellaflow.client import CellaflowClient
from cellaflow.context import WorkflowContext, set_context, reset_context, get_context
from cellaflow.v1.common_pb2 import STEP_STATUS_SUCCESS

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
                for step in steps:
                    replayed_steps[step["sequence"]] = step["output_payload"]

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
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
                for step in steps:
                    replayed_steps[step["sequence"]] = step["output_payload"]

            ctx = WorkflowContext(
                client=client,
                session_id=resp.session_id,
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


def step(func: F) -> F:
    @functools.wraps(func)
    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return ctx.replayed_steps[seq].get("result")

        result = await func(*args, **kwargs)
        ctx.client.commit_step(
            session_id=ctx.session_id,
            sequence=seq,
            name=func.__name__,
            status=STEP_STATUS_SUCCESS,
            output_payload={"result": result},
        )
        return result

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = get_context()
        ctx.sequence += 1
        seq = ctx.sequence

        if seq in ctx.replayed_steps:
            return ctx.replayed_steps[seq].get("result")

        result = func(*args, **kwargs)
        ctx.client.commit_step(
            session_id=ctx.session_id,
            sequence=seq,
            name=func.__name__,
            status=STEP_STATUS_SUCCESS,
            output_payload={"result": result},
        )
        return result

    if inspect.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    return cast(F, sync_wrapper)


def tool(func: F) -> F:
    """
    Decorator for tool steps that require idempotency tracking.
    Functionally identical to @step for now, but semantically distinct.
    """
    return step(func)
