import pytest
from unittest.mock import MagicMock
from typing import Any, List, Optional, Tuple

from cellaflow.decorators import workflow, step
from cellaflow.v1 import service_pb2
from cellaflow.serialization import serialize
from cellaflow.v1 import idempotency_pb2


@pytest.fixture(autouse=True)
def mock_cache(mock_stub: MagicMock) -> None:
    from cellaflow.v1 import idempotency_pb2

    mock_resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=456,
        heartbeat_interval_ms=10000,
    )
    mock_stub.CheckIdempotencyCache.return_value = mock_resp


@pytest.fixture
def mock_stub() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_client(mock_stub: MagicMock, monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "cellaflow.client.service_pb2_grpc.WorkflowEngineServiceStub",
        lambda channel: mock_stub,
    )


def test_workflow_and_step_sync(mock_stub: MagicMock, mock_client: None) -> None:
    # Setup mock to return a normal start session
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step
    def my_step(x: int) -> int:
        return x * 2

    @workflow(version="1.0.0")
    def my_workflow(val: int) -> int:
        return my_step(val)  # type: ignore[no-any-return]

    # Execute workflow
    result = my_workflow(10)

    assert result == 20

    # Verify StartSession was called
    mock_stub.StartSession.assert_called_once()
    req = mock_stub.StartSession.call_args[0][0]
    assert req.workflow_id == "my_workflow"

    # Verify CommitStep was called for the step
    mock_stub.CommitStep.assert_called_once()
    commit_req = mock_stub.CommitStep.call_args[0][0]
    assert commit_req.session_id == "test-session"
    assert commit_req.step_result.sequence == 1
    assert commit_req.step_result.name == "my_step"


@pytest.mark.asyncio
async def test_workflow_and_step_async(mock_stub: MagicMock, mock_client: None) -> None:
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step
    async def my_step(x: int) -> int:
        return x * 2

    @workflow(version="1.0.0")
    async def my_workflow(val: int) -> int:
        return await my_step(val)  # type: ignore[no-any-return]

    # Execute workflow
    result = await my_workflow(10)

    assert result == 20
    mock_stub.StartSession.assert_called_once()
    mock_stub.CommitStep.assert_called_once()


def test_workflow_recovery_sync(mock_stub: MagicMock, mock_client: None) -> None:
    # Setup mock to return a recovered session
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=True
    )
    mock_stub.StartSession.return_value = mock_start

    # Mock get_graph to return a previously executed step 1
    from cellaflow.v1 import common_pb2

    mock_graph = service_pb2.GetGraphResponse(
        session_id="test-session",
        steps=[
            common_pb2.StepResult(
                sequence=1,
                name="my_step",
                status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                output_payload=serialize({"result": 99}),
            )
        ],
    )
    mock_stub.GetGraph.return_value = mock_graph

    executed = False

    @step
    def my_step(x: int) -> int:
        nonlocal executed
        executed = True
        return x * 2

    @workflow(version="1.0.0")
    def my_workflow(val: int) -> int:
        return my_step(val)  # type: ignore[no-any-return]

    # We pass the _session_id to trigger recovery on the same session
    result = my_workflow(10, _session_id="test-session")  # type: ignore[call-arg]

    # The result should be 99 from the mocked graph, not 20 from execution
    assert result == 99
    # The actual step function should not have been executed
    assert not executed

    # Verify CommitStep was NOT called because it was replayed
    mock_stub.CommitStep.assert_not_called()


def test_custom_idempotency_key(mock_stub: MagicMock, mock_client: None) -> None:
    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step(idempotency_key="my-custom-key")  # type: ignore[untyped-decorator]
    def my_step(x: int) -> int:
        return x * 2

    @workflow(version="1.0.0")
    def my_workflow(val: int) -> int:
        return my_step(val)  # type: ignore[no-any-return]

    my_workflow(10)

    # Verify CommitStep was called with the custom idempotency key
    mock_stub.CommitStep.assert_called_once()
    commit_req = mock_stub.CommitStep.call_args[0][0]
    assert commit_req.idempotency_key == "my-custom-key"


def test_step_cache_hit(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.v1 import idempotency_pb2, common_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    # Mock cache HIT with serialized result = 777
    cached_step = common_pb2.StepResult(
        sequence=1,
        name="my_step",
        status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
        output_payload=serialize({"result": 777}),
    )
    mock_hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        cached_result=cached_step,
    )
    mock_stub.CheckIdempotencyCache.return_value = mock_hit

    executed = False

    @step
    def my_step() -> int:
        nonlocal executed
        executed = True
        return 42

    @workflow(version="1.0.0")
    def my_workflow() -> int:
        return my_step()  # type: ignore[no-any-return]

    result = my_workflow()
    assert result == 777
    assert not executed
    mock_stub.CommitStep.assert_not_called()


def test_step_release_lease_on_error(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.v1 import idempotency_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_acquired = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=999,
        heartbeat_interval_ms=5000,
    )
    mock_stub.CheckIdempotencyCache.return_value = mock_acquired
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=True
    )

    @step
    def failing_step() -> int:
        raise ValueError("Simulated tool crash")

    @workflow(version="1.0.0")
    def my_workflow() -> int:
        return failing_step()  # type: ignore[no-any-return]

    with pytest.raises(ValueError, match="Simulated tool crash"):
        my_workflow()

    # Verify ReleaseLease was called with reason="TOOL_ERROR"
    mock_stub.ReleaseLease.assert_called_once()
    rel_req = mock_stub.ReleaseLease.call_args[0][0]
    assert rel_req.fencing_token == 999
    assert rel_req.reason == "TOOL_ERROR"


def test_step_scopes(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.idempotency import IdempotencyScope

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_commit = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )
    mock_stub.CommitStep.return_value = mock_commit

    @step(
        scope=IdempotencyScope.SCOPE_AGENT_PRIVATE,
        agent_id="agent-007",
    )  # type: ignore[untyped-decorator]
    def scoped_step(val: int) -> int:
        return val * 3

    @workflow(version="1.0.0")
    def my_workflow(v: int) -> int:
        return scoped_step(v)  # type: ignore[no-any-return]

    my_workflow(5)

    mock_stub.CommitStep.assert_called_once()
    commit_req = mock_stub.CommitStep.call_args[0][0]
    parts = commit_req.idempotency_key.split(":")
    assert len(parts) == 6
    assert parts[0] == "test-session"
    assert parts[2] == "session_wide"  # AGENT_PRIVATE ignores sequence
    assert parts[3] == "agent-007"  # AGENT_PRIVATE preserves agent_id
    assert parts[4] == "scoped_step"


@pytest.mark.asyncio
async def test_step_async_cache_hit(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.v1 import idempotency_pb2, common_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    cached_step = common_pb2.StepResult(
        sequence=1,
        name="async_step",
        status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
        output_payload=serialize({"result": 888}),
    )
    mock_hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        cached_result=cached_step,
    )
    mock_stub.CheckIdempotencyCache.return_value = mock_hit

    executed = False

    @step
    async def async_step() -> int:
        nonlocal executed
        executed = True
        return 123

    @workflow(version="1.0.0")
    async def my_workflow() -> int:
        return await async_step()  # type: ignore[no-any-return]

    result = await my_workflow()
    assert result == 888
    assert not executed


@pytest.mark.asyncio
async def test_step_async_release_lease_on_error(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow.v1 import idempotency_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    mock_acquired = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=777,
        heartbeat_interval_ms=5000,
    )
    mock_stub.CheckIdempotencyCache.return_value = mock_acquired
    mock_stub.ReleaseLease.return_value = idempotency_pb2.ReleaseLeaseResponse(
        released=True
    )

    @step
    async def failing_async_step() -> int:
        raise RuntimeError("Async tool failure")

    @workflow(version="1.0.0")
    async def my_workflow() -> int:
        return await failing_async_step()  # type: ignore[no-any-return]

    with pytest.raises(RuntimeError, match="Async tool failure"):
        await my_workflow()

    mock_stub.ReleaseLease.assert_called_once()
    rel_req = mock_stub.ReleaseLease.call_args[0][0]
    assert rel_req.fencing_token == 777
    assert rel_req.reason == "TOOL_ERROR"


def test_step_in_progress_polling_sync(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.v1 import idempotency_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    in_progress_resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS,
        retry_after_ms=50,
    )
    acquired_resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=333,
        heartbeat_interval_ms=5000,
    )
    mock_stub.CheckIdempotencyCache.side_effect = [
        in_progress_resp,
        acquired_resp,
    ]
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )

    @step
    def busy_step() -> str:
        return "finally executed"

    @workflow(version="1.0.0")
    def my_workflow() -> str:
        return busy_step()  # type: ignore[no-any-return]

    res = my_workflow()
    assert res == "finally executed"
    assert mock_stub.CheckIdempotencyCache.call_count == 2


@pytest.mark.asyncio
async def test_step_in_progress_polling_async(
    mock_stub: MagicMock, mock_client: None
) -> None:
    from cellaflow.v1 import idempotency_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start

    in_progress_resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_IN_PROGRESS,
        retry_after_ms=50,
    )
    acquired_resp = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=444,
        heartbeat_interval_ms=5000,
    )
    mock_stub.CheckIdempotencyCache.side_effect = [
        in_progress_resp,
        acquired_resp,
    ]
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )

    @step
    async def async_busy_step() -> str:
        return "async executed"

    @workflow(version="1.0.0")
    async def my_workflow() -> str:
        return await async_busy_step()  # type: ignore[no-any-return]

    res = await my_workflow()
    assert res == "async executed"
    assert mock_stub.CheckIdempotencyCache.call_count == 2


@pytest.mark.asyncio
async def test_workflow_recovery_async(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.v1 import common_pb2

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=True
    )
    mock_stub.StartSession.return_value = mock_start

    mock_graph = service_pb2.GetGraphResponse(
        session_id="test-session",
        steps=[
            common_pb2.StepResult(
                sequence=1,
                name="async_step",
                status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                output_payload=serialize({"result": 555}),
            )
        ],
    )
    mock_stub.GetGraph.return_value = mock_graph

    executed = False

    @step
    async def async_step() -> int:
        nonlocal executed
        executed = True
        return 100

    @workflow(version="1.0.0")
    async def my_workflow() -> int:
        return await async_step()  # type: ignore[no-any-return]

    res = await my_workflow()
    assert res == 555
    assert not executed
    mock_stub.CommitStep.assert_not_called()


def test_step_scope_step_local(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.idempotency import IdempotencyScope

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )

    @step(
        scope=IdempotencyScope.SCOPE_STEP_LOCAL,
        agent_id="agent-turn",
    )  # type: ignore[untyped-decorator]
    def local_step(x: int) -> int:
        return x * 10

    @workflow(version="1.0.0")
    def my_workflow(v: int) -> int:
        return local_step(v)  # type: ignore[no-any-return]

    my_workflow(2)
    commit_req = mock_stub.CommitStep.call_args[0][0]
    parts = commit_req.idempotency_key.split(":")
    assert parts[2] == "1"  # STEP_LOCAL preserves exact sequence number
    assert parts[3] == "agent-turn"


def test_tool_decorator_direct(mock_stub: MagicMock, mock_client: None) -> None:
    from cellaflow.decorators import tool

    mock_start = service_pb2.StartSessionResponse(
        session_id="test-session", version="1.0.0", is_recovered=False
    )
    mock_stub.StartSession.return_value = mock_start
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="test-session", next_sequence=2
    )

    @tool
    def search_engine(q: str) -> str:
        return f"result for {q}"

    @workflow(version="1.0.0")
    def workflow_run(query: str) -> str:
        return search_engine(query)  # type: ignore[no-any-return]

    res = workflow_run("test")
    assert res == "result for test"
    mock_stub.CommitStep.assert_called_once()


def _hit_then_acquire(
    reported_sequence: Optional[int],
) -> List["idempotency_pb2.CheckCacheResponse"]:
    """A cache HIT carrying `reported_sequence`, then an ACQUIRED for the next step."""
    from cellaflow.v1 import common_pb2

    cached = common_pb2.StepResult(
        sequence=1,
        name="shared",
        status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
        output_payload=serialize({"result": "from-elsewhere"}),
    )
    hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT, cached_result=cached
    )
    if reported_sequence is not None:
        hit.current_sequence = reported_sequence

    return [
        hit,
        idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
            fencing_token=456,
            heartbeat_interval_ms=10000,
        ),
    ]


def _two_step_workflow() -> Any:
    @step
    def shared() -> str:
        return "executed-locally"

    @step
    def local_followup() -> str:
        return "mine"

    @workflow(version="1.0.0")
    def wf() -> str:
        shared()
        return local_followup()  # type: ignore[no-any-return]

    return wf


def test_adopts_engine_sequence_after_cross_session_hit(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """CEL-98: a hit satisfied from another session must not desync the counter.

    The hit returns without committing, so this session's engine-side sequence is
    still 0. Without adopting the reported position the next step would ask for 2
    and be rejected — one step after the real cause.
    """
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-b", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _hit_then_acquire(0)
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-b", next_sequence=2
    )

    assert _two_step_workflow()() == "mine"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 1, (
        "after a cross-session hit the next commit must target sequence 1, "
        f"got {committed.step_result.sequence}"
    )
    # And the engine must have been told which session was asking.
    assert (
        mock_stub.CheckIdempotencyCache.call_args_list[0][0][0].session_id == "sess-b"
    )


def test_preserves_sequence_after_same_session_hit(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """CEL-98 must not break the case that already worked.

    A peer in the *same* session committed at 1, so the engine did advance and
    the next step legitimately targets 2.
    """
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-a", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _hit_then_acquire(1)
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-a", next_sequence=3
    )

    assert _two_step_workflow()() == "mine"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 2, (
        "a same-session hit means the engine already advanced; the next commit "
        f"must target sequence 2, got {committed.step_result.sequence}"
    )


def test_tolerates_engine_without_current_sequence(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """An engine predating the field must not break the client.

    Behaviour degrades to the original defect — the next commit is off by one —
    rather than to a crash or a wrong sequence.
    """
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-old", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _hit_then_acquire(None)
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-old", next_sequence=3
    )

    assert _two_step_workflow()() == "mine"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 2, (
        "with no reported position the client keeps its own count — the old "
        f"behaviour, not a new failure; got {committed.step_result.sequence}"
    )


def test_no_reconciliation_when_workflow_ends_on_a_hit(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The hit itself must not fetch anything; only a following step consumes it.

    A workflow whose last act is a shared tool is the common shape, and it should
    cost nothing extra.
    """
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-end", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _hit_then_acquire(7)

    @step
    def shared() -> str:
        return "executed-locally"

    @workflow(version="1.0.0")
    def wf() -> str:
        return shared()  # type: ignore[no-any-return]

    assert wf() == "from-elsewhere"

    mock_stub.GetGraph.assert_not_called()
    mock_stub.CommitStep.assert_not_called()


def test_consecutive_cache_hits_reconcile_correctly(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """CEL-98 acceptance criterion: correct when several hits occur in a row.

    Each hit re-reports the same position because the engine has not advanced,
    so the counter must not creep forward once per hit.
    """
    from cellaflow.v1 import idempotency_pb2, common_pb2

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-multi", version="1.0.0", is_recovered=False
    )

    def hit(payload: str) -> "idempotency_pb2.CheckCacheResponse":
        r = idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_HIT,
            cached_result=common_pb2.StepResult(
                sequence=1,
                name="shared",
                status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                output_payload=serialize({"result": payload}),
            ),
        )
        # The engine is stuck at 0 throughout: nothing this session did committed.
        r.current_sequence = 0
        return r

    mock_stub.CheckIdempotencyCache.side_effect = [
        hit("one"),
        hit("two"),
        hit("three"),
        idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
            fencing_token=456,
            heartbeat_interval_ms=10000,
        ),
    ]
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-multi", next_sequence=2
    )

    @step
    def a() -> str:
        return "x"

    @step
    def b() -> str:
        return "x"

    @step
    def c() -> str:
        return "x"

    @step
    def mine() -> str:
        return "mine"

    @workflow(version="1.0.0")
    def wf() -> str:
        a()
        b()
        c()
        return mine()  # type: ignore[no-any-return]

    assert wf() == "mine"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 1, (
        "three consecutive hits must not advance the counter three times — the "
        f"first real commit still belongs at sequence 1, got "
        f"{committed.step_result.sequence}"
    )


def test_reconciliation_does_not_disturb_replayed_steps(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Reconciling must not rewind into already-replayed history.

    The reported position is always *this* session's own, so it can never be
    lower than what has been replayed from it. Pinned because the two mechanisms
    both move `ctx.sequence` and the interaction is not obvious.
    """
    from cellaflow.v1 import idempotency_pb2, common_pb2

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-replay", version="1.0.0", is_recovered=True
    )
    replayed = common_pb2.StepResult(
        sequence=1,
        name="already_done",
        status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
        output_payload=serialize({"result": "replayed"}),
    )
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=[replayed], next_cursor=""
    )

    hit = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        cached_result=common_pb2.StepResult(
            sequence=1,
            name="shared",
            status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
            output_payload=serialize({"result": "cached"}),
        ),
    )
    hit.current_sequence = 1  # this session committed step 1 itself
    mock_stub.CheckIdempotencyCache.side_effect = [
        hit,
        idempotency_pb2.CheckCacheResponse(
            status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
            fencing_token=456,
            heartbeat_interval_ms=10000,
        ),
    ]
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-replay", next_sequence=3
    )

    @step
    def already_done() -> str:
        raise AssertionError("a replayed step must not execute")

    @step
    def shared() -> str:
        raise AssertionError("a cache hit must not execute")

    @step
    def mine() -> str:
        return "mine"

    @workflow(version="1.0.0")
    def wf() -> str:
        already_done()
        shared()
        return mine()  # type: ignore[no-any-return]

    assert wf() == "mine"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 2, (
        "reconciling to the session's own position must not rewind into replayed "
        f"history; expected the next commit at 2, got {committed.step_result.sequence}"
    )


def _recovered_session(mock_stub: MagicMock, session_id: str, names: List[str]) -> None:
    """Seeds a recovered session whose history is `names`, committed at 1..n."""
    from cellaflow.v1 import common_pb2

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id=session_id, version="1.0.0", is_recovered=True
    )
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=[
            common_pb2.StepResult(
                sequence=i,
                name=name,
                status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
                output_payload=serialize({"result": f"{name}-result"}),
            )
            for i, name in enumerate(names, start=1)
        ],
        next_cursor="",
    )


def test_replay_rejects_a_different_step_at_the_same_sequence(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """CEL-102: a resumed run that skips a branch must fail loudly.

    Before this check it silently received the *previous* step's result — warmup's
    output returned for shared — and carried on computing with foreign data.
    """
    from cellaflow import NondeterministicWorkflowError

    _recovered_session(mock_stub, "sess-diverge", ["warmup", "shared", "after"])

    @step
    def shared() -> str:
        return "executed"

    @workflow(version="1.0.0")
    def wf() -> str:
        # History has warmup at 1; this run reaches `shared` there instead.
        return shared()  # type: ignore[no-any-return]

    with pytest.raises(NondeterministicWorkflowError) as excinfo:
        wf()

    message = str(excinfo.value)
    assert "sequence 1" in message
    assert "'shared'" in message, "the error must name the step that was expected"
    assert "'warmup'" in message, "the error must name the step that was found"


def test_replay_returns_the_recorded_result_when_identity_matches(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The normal crash-recovery path must be untouched — replay, no execution."""
    _recovered_session(mock_stub, "sess-resume", ["first", "second"])

    executed = []

    @step
    def first() -> str:
        executed.append("first")
        return "fresh"

    @step
    def second() -> str:
        executed.append("second")
        return "fresh"

    @workflow(version="1.0.0")
    def wf() -> Tuple[str, str]:
        return (first(), second())

    assert wf() == ("first-result", "second-result")
    assert executed == [], "replayed steps must not re-execute"
    mock_stub.CommitStep.assert_not_called()


def test_replay_allows_extending_history_with_new_steps(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A run that matches the recorded prefix then continues must work.

    This is resumption after a crash partway through, which is the whole point
    of replay — the check must not make it stricter than it should be.
    """
    _recovered_session(mock_stub, "sess-extend", ["first"])
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-extend", next_sequence=3
    )

    executed = []

    @step
    def first() -> str:
        executed.append("first")
        return "fresh"

    @step
    def second() -> str:
        executed.append("second")
        return "new-work"

    @workflow(version="1.0.0")
    def wf() -> Tuple[str, str]:
        return (first(), second())

    assert wf() == ("first-result", "new-work")
    assert executed == ["second"], "only the step beyond history should run"

    committed = mock_stub.CommitStep.call_args[0][0]
    assert committed.step_result.sequence == 2
    assert committed.step_result.name == "second"


def test_replay_tolerates_history_without_names(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """History written before names were retained must still replay.

    Absent a recorded name there is nothing to compare, so the old positional
    behaviour stands rather than the run failing on data it cannot check.
    """
    _recovered_session(mock_stub, "sess-legacy", ["recorded"])
    # Simulate an index built without the name, as the previous SDK produced.
    original = mock_stub.GetGraph.return_value
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=[
            type(original.steps[0])(
                sequence=1,
                name="",
                status=original.steps[0].status,
                output_payload=original.steps[0].output_payload,
            )
        ],
        next_cursor="",
    )

    @step
    def anything() -> str:
        raise AssertionError("must replay, not execute")

    @workflow(version="1.0.0")
    def wf() -> str:
        return anything()  # type: ignore[no-any-return]

    assert wf() == "recorded-result"
