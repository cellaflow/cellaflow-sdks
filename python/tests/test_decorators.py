import pytest
from unittest.mock import MagicMock
from typing import Callable, Any, Dict, List, Optional, Tuple

from cellaflow.decorators import workflow, step, tool
from cellaflow.idempotency import IdempotencyScope
from cellaflow.v1 import service_pb2
from cellaflow.serialization import deserialize, serialize
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


def test_records_a_cross_session_hit_at_its_position(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A hit satisfied from another session is recorded at its own position.

    The session sits at 0, so nothing in it holds position 1 yet. The shared
    result is recorded there as a plain step before the next step runs, and the
    next step takes position 2. Letting the next step take position 1 instead is
    what made such a session impossible to resume: replay found it where the
    shared step was expected.
    """
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-b", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _hit_then_acquire(0)
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-b", next_sequence=2
    )

    assert _two_step_workflow()() == "mine"

    commits = [c[0][0] for c in mock_stub.CommitStep.call_args_list]
    assert [(c.step_result.sequence, c.step_result.name) for c in commits] == [
        (1, "shared"),
        (2, "local_followup"),
    ]
    recorded = commits[0]
    # A plain step: the result, under the tool's name, with no lease behind it.
    assert deserialize(recorded.step_result.output_payload) == {
        "result": "from-elsewhere"
    }
    assert not recorded.step_result.HasField("idempotency_key")
    assert not recorded.HasField("idempotency_fencing_token")
    # And the engine must have been told which session was asking.
    assert (
        mock_stub.CheckIdempotencyCache.call_args_list[0][0][0].session_id == "sess-b"
    )


def test_preserves_sequence_after_same_session_hit(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Adopting the engine's position must not break the same-session case.

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
    """Adopting the engine's position stays correct over several hits in a row.

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

    # The session sits at 1 after the replayed step, so the hit is recorded at 2
    # and the next step follows it at 3.
    commits = [c[0][0].step_result for c in mock_stub.CommitStep.call_args_list]
    assert [(c.sequence, c.name) for c in commits] == [(2, "shared"), (3, "mine")]


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
    """A resumed run that skips a branch must fail loudly.

    Without this check it would receive the *previous* step's result (warmup's
    output returned for shared) and carry on computing with foreign data.
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


def test_replay_refuses_history_it_cannot_verify(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A record with no name is unverifiable, so replay must refuse it.

    Every writer sets a name, so a nameless record cannot come from this SDK.
    Replaying it by position would hand back whatever happens to sit there,
    which is exactly what checking the name on replay exists to prevent.
    """
    from cellaflow import NondeterministicWorkflowError

    _recovered_session(mock_stub, "sess-unnamed", ["recorded"])
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
        raise AssertionError("must not execute — the record is unverifiable")

    @workflow(version="1.0.0")
    def wf() -> str:
        return anything()  # type: ignore[no-any-return]

    with pytest.raises(NondeterministicWorkflowError) as excinfo:
        wf()

    message = str(excinfo.value)
    assert "sequence 1" in message
    assert "no name" in message
    assert "'anything'" in message, "the error must name the step that was expected"


def _refusal(details: str) -> Exception:
    """A grpc.RpcError carrying FAILED_PRECONDITION, as the engine sends it."""
    import grpc

    # grpc's stubs give RpcError type Any, so strict mode rejects the subclass.
    class _Refused(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> Any:
            return grpc.StatusCode.FAILED_PRECONDITION

        def details(self) -> str:
            return details

    return _Refused()


def test_step_sends_its_intended_sequence(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The engine cannot guard a position it was not told about."""
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="s", version="1.0.0", is_recovered=False
    )
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="s", next_sequence=2
    )

    @step
    def first(x: int) -> int:
        return x

    @step
    def second(x: int) -> int:
        return x

    @workflow(version="1.0.0")
    def wf() -> None:
        first(1)
        second(2)

    wf()

    sent = [c.args[0].sequence for c in mock_stub.CheckIdempotencyCache.call_args_list]
    assert sent == [1, 2], (
        "each step must report the position it intends to write, not a constant; "
        f"got {sent}"
    )


def test_refused_lease_raises_divergent_step_error(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A refusal must name the step, not surface as a raw RpcError.

    The refusal arrives *before* the tool body runs -- that is the whole point --
    so the test also asserts the body never executed.
    """
    from cellaflow.decorators import DivergentStepError

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="s", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = _refusal(
        "Sequence 1 in session 's' is already committed; the session is at 1."
    )

    executed: List[str] = []

    @step
    def issue_refund(amount: int) -> int:
        executed.append("ran")
        return amount

    @workflow(version="1.0.0")
    def wf() -> None:
        issue_refund(3000)

    with pytest.raises(DivergentStepError) as excinfo:
        wf()

    message = str(excinfo.value)
    assert "issue_refund" in message, "the error must name the step that was refused"
    assert "already committed" in message, "it must carry the engine's explanation"
    assert (
        executed == []
    ), "the refusal must arrive before the side effect; the body must not have run"


def test_non_precondition_rpc_errors_are_not_reinterpreted(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Only FAILED_PRECONDITION means divergence; everything else passes through."""
    import grpc

    from cellaflow.decorators import DivergentStepError

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="s", version="1.0.0", is_recovered=False
    )

    class _Unavailable(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> Any:
            return grpc.StatusCode.UNAVAILABLE

        def details(self) -> str:
            return "engine down"

    mock_stub.CheckIdempotencyCache.side_effect = _Unavailable()

    @step
    def anything() -> int:
        return 1

    @workflow(version="1.0.0")
    def wf() -> None:
        anything()

    with pytest.raises(grpc.RpcError) as excinfo:
        wf()
    assert not isinstance(
        excinfo.value, DivergentStepError
    ), "a transport failure is not a divergence and must not be relabelled as one"


def test_shared_scope_refuses_a_tool_that_has_not_named_its_work() -> None:
    """SCOPE_SHARED cannot derive a safe key on its own, so it must not guess.

    Hashing every argument splits the key across agents that disagree, and each
    then performs the side effect. An agent cannot detect that locally -- it
    sees only its own arguments -- so the decision belongs to the caller, made
    once, at import rather than in production.
    """
    with pytest.raises(ValueError, match="which of its arguments identify"):

        def issue_refund(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
            return {}

        tool(tool_name="issue_refund", scope=IdempotencyScope.SCOPE_SHARED)(
            issue_refund
        )


def test_shared_on_typo_is_caught_at_import() -> None:
    """A misspelled argument name would silently hash nothing and converge
    everything, so it fails at decoration with the real parameter list."""
    with pytest.raises(ValueError, match="not parameters of"):

        def issue_refund(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
            return {}

        tool(
            tool_name="issue_refund",
            scope=IdempotencyScope.SCOPE_SHARED,
            shared_on=["tickett_id"],
        )(issue_refund)


def test_shared_scope_accepts_an_explicit_key_instead() -> None:
    """A static key is still a valid way to say what the work is."""

    def issue_refund(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
        return {}

    wrapped = tool(
        tool_name="issue_refund",
        scope=IdempotencyScope.SCOPE_SHARED,
        idempotency_key="refund:fixed",
    )(issue_refund)

    assert wrapped is not None


# --- Recording a shared result in the session that received it ---------------


def _hit(sequence_now: int, result: str = "from-elsewhere") -> Any:
    """A HIT from another session's commit, with this session at `sequence_now`."""
    from cellaflow.v1 import common_pb2

    r = idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_HIT,
        cached_result=common_pb2.StepResult(
            sequence=1,
            name="place_order",
            status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
            output_payload=serialize({"result": result}),
        ),
    )
    r.current_sequence = sequence_now
    return r


def _acquired() -> Any:
    return idempotency_pb2.CheckCacheResponse(
        status=idempotency_pb2.CACHE_STATUS_ACQUIRED,
        fencing_token=456,
        heartbeat_interval_ms=10000,
    )


def test_a_workflow_ending_on_a_cross_session_hit_records_nothing(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The record is deferred to the next step, so a run ending on the hit costs
    no extra call. Nothing follows it to take its position."""
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-end-0", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = [_hit(0)]

    @step
    def place_order() -> str:
        raise AssertionError("a cache hit must not execute")

    @workflow(version="1.0.0")
    def wf() -> str:
        return place_order()  # type: ignore[no-any-return]

    assert wf() == "from-elsewhere"
    mock_stub.CommitStep.assert_not_called()


def test_a_session_with_a_recorded_hit_resumes_by_replay(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The reported bug: a session that received a shared result and then did
    another step could not be resumed. With the hit recorded at its position,
    the resumed run replays all three steps and calls nothing."""
    from cellaflow.v1 import common_pb2

    history = [
        common_pb2.StepResult(
            sequence=i,
            name=name,
            status=common_pb2.StepStatus.STEP_STATUS_SUCCESS,
            output_payload=serialize({"result": result}),
        )
        for i, (name, result) in enumerate(
            [
                ("price_cart", 2499),
                ("place_order", "ORD-1"),
                ("send_confirmation", "emailed"),
            ],
            start=1,
        )
    ]
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-resume", version="1.0.0", is_recovered=True
    )
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=history, next_cursor=""
    )

    @step
    def price_cart() -> int:
        raise AssertionError("replayed")

    @step
    def place_order() -> str:
        raise AssertionError("replayed")

    @step
    def send_confirmation() -> str:
        raise AssertionError("replayed")

    @workflow(version="1.0.0")
    def _agent() -> str:
        price_cart()
        place_order()
        return send_confirmation()  # type: ignore[no-any-return]

    # Typed loosely so the call can pass the `_session_id` the decorator consumes.
    agent: Callable[..., str] = _agent
    assert agent(_session_id="sess-resume") == "emailed"
    mock_stub.CheckIdempotencyCache.assert_not_called()
    mock_stub.CommitStep.assert_not_called()


def test_a_position_taken_first_stops_recording_hits(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """If another writer in the session took the position before the record, the
    run no longer knows what the session holds. It reads the session's real
    position, carries on from there without recording later hits, and says so."""
    import grpc

    class Taken(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.FAILED_PRECONDITION

        def details(self) -> str:
            return "sequence already committed"

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-race", version="1.0.0", is_recovered=False
    )
    # place_order hits at 1; the record of it is refused; mine acquires at 2.
    mock_stub.CheckIdempotencyCache.side_effect = [_hit(0), _acquired()]
    mock_stub.CommitStep.side_effect = [
        Taken(),
        service_pb2.CommitStepResponse(session_id="sess-race", next_sequence=4),
    ]
    # The other writer committed positions 1 and 2 meanwhile.
    from cellaflow.v1 import common_pb2

    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(
        steps=[
            common_pb2.StepResult(
                sequence=i, name="peer", output_payload=serialize({"result": None})
            )
            for i in (1, 2)
        ],
        next_cursor="",
    )

    @step
    def place_order() -> str:
        raise AssertionError("a cache hit must not execute")

    @step
    def mine() -> str:
        return "mine"

    @workflow(version="1.0.0")
    def wf() -> str:
        place_order()
        return mine()  # type: ignore[no-any-return]

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert wf() == "mine"
    commits = [c[0][0].step_result for c in mock_stub.CommitStep.call_args_list]
    assert [(c.sequence, c.name) for c in commits] == [
        (1, "place_order"),
        (3, "mine"),
    ]
    assert "was not recorded at position 1" in caplog.text


def test_a_hit_into_history_the_run_did_not_count_is_not_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A session further along than the run counted holds steps the run never
    saw, so a later hit may already be recorded there. The run adopts the
    engine's position and records nothing, as before."""
    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-ahead", version="1.0.0", is_recovered=False
    )
    # First hit: the session is at 5 while this call is at 1. Second hit: the
    # session is at 5 and this call at 6, which would otherwise look recordable.
    mock_stub.CheckIdempotencyCache.side_effect = [
        _hit(5),
        _hit(5, "again"),
        _acquired(),
    ]
    mock_stub.CommitStep.return_value = service_pb2.CommitStepResponse(
        session_id="sess-ahead", next_sequence=7
    )

    @step
    def first() -> str:
        raise AssertionError("hit")

    @step
    def second() -> str:
        raise AssertionError("hit")

    @step
    def mine() -> str:
        return "mine"

    @workflow(version="1.0.0")
    def wf() -> str:
        first()
        second()
        return mine()  # type: ignore[no-any-return]

    assert wf() == "mine"
    commits = [c[0][0].step_result for c in mock_stub.CommitStep.call_args_list]
    assert [(c.sequence, c.name) for c in commits] == [(6, "mine")]


def test_a_failed_record_is_kept_and_sent_again(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A transient error writing the record keeps it, so retrying the step that
    hit it sends it again before anything else runs. Dropping it would let the
    retried step take the shared step's position."""
    import grpc

    class Unavailable(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.UNAVAILABLE

    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(), session_id="sess-retry", workflow_version="1.0.0"
    )
    ctx.sequence = 1
    ctx.defer_hit_record(1, "place_order", {"result": "ORD-1"}, None)
    mock_stub.CommitStep.side_effect = [
        Unavailable(),
        service_pb2.CommitStepResponse(session_id="sess-retry", next_sequence=2),
    ]

    with pytest.raises(grpc.RpcError):
        ctx.reconcile_sequence()
    ctx.reconcile_sequence()

    sent = [c[0][0].step_result for c in mock_stub.CommitStep.call_args_list]
    assert [(c.sequence, c.name) for c in sent] == [(1, "place_order")] * 2
    assert ctx.sequence == 1


def test_deferring_a_record_never_moves_the_counter(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Parallel calls in one session may have taken later positions by the time a
    hit is deferred, so deferring must not move the counter back to the hit."""
    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(), session_id="sess-par", workflow_version="1.0.0"
    )
    ctx.sequence = 5
    ctx.defer_hit_record(3, "place_order", {"result": "ORD-1"}, None)
    assert ctx.sequence == 5


# --- Calls in flight together ------------------------------------------------
#
# A hit is recorded only while it is the only call in flight. With overlap, the
# run behaves as it did before hits were recorded: it adopts the position the
# engine reported, and the next call claims from there.


def _sent(mock_stub: MagicMock) -> List[Tuple[int, str, bool]]:
    """Every commit as (position, step name, whether it carried a lease key)."""
    reqs = [c[0][0] for c in mock_stub.CommitStep.call_args_list]
    return [
        (r.step_result.sequence, r.step_result.name, r.HasField("idempotency_key"))
        for r in reqs
    ]


def _accept_commits(mock_stub: MagicMock) -> None:
    mock_stub.CommitStep.side_effect = (
        lambda req, **_: service_pb2.CommitStepResponse(
            session_id=req.session_id, next_sequence=req.step_result.sequence + 1
        )
    )


def test_two_hits_in_flight_together_record_nothing(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """Two hits answered while both are in flight both report the session at 0.
    Neither is recorded, and the next call claims position 1, the session's real
    next position. Recording one of them moved the counter past it."""
    import threading
    import time

    from cellaflow import durable_tools

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-two-hits", version="1.0.0", is_recovered=False
    )
    both_in_flight = threading.Barrier(2)
    hits_left = [2]
    hits_lock = threading.Lock()

    def check(req: Any, **_: Any) -> Any:
        with hits_lock:
            hits_left[0] -= 1
            if hits_left[0] < 0:
                return _acquired()
        both_in_flight.wait(timeout=5)
        if req.sequence == 2:
            time.sleep(0.2)  # the first hit is decided first
        return _hit(0)

    mock_stub.CheckIdempotencyCache.side_effect = check
    _accept_commits(mock_stub)

    @tool
    def first() -> str:
        raise AssertionError("a cache hit must not execute")

    @tool
    def second() -> str:
        raise AssertionError("a cache hit must not execute")

    @tool
    def mine() -> str:
        return "mine"

    with durable_tools({"configurable": {"thread_id": "t-two-hits"}}):
        threads = [threading.Thread(target=first), threading.Thread(target=second)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert mine() == "mine"

    assert _sent(mock_stub) == [(1, "mine", True)]


def test_a_hit_while_another_call_runs_is_not_recorded(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The other call may commit before or after the hit's position, so the hit
    cannot know the position is still free. It records nothing and stops the run
    recording later hits."""
    import asyncio

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-overlap", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = [_acquired(), _hit(0), _hit(1)]
    _accept_commits(mock_stub)
    hit_answered = asyncio.Event()

    @step
    async def slow() -> str:
        await hit_answered.wait()
        return "slow"

    @step
    async def place_order() -> str:
        raise AssertionError("a cache hit must not execute")

    @step
    async def send_receipt() -> str:
        raise AssertionError("a cache hit must not execute")

    @workflow(version="1.0.0")
    async def wf() -> List[str]:
        async def order_then_release() -> str:
            result: str = await place_order()
            hit_answered.set()
            return result

        results = list(await asyncio.gather(slow(), order_then_release()))
        # Alone now, but the run no longer knows what the session holds.
        results.append(await send_receipt())
        return results

    assert asyncio.run(wf()) == ["slow", "from-elsewhere", "from-elsewhere"]
    assert _sent(mock_stub) == [(1, "slow", True)]


def test_a_recorded_hit_is_written_once_before_concurrent_calls_claim(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A hit recorded while alone is written by whichever call claims next, before
    it claims. The calls after it take the following positions, once each."""
    import asyncio

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-then-par", version="1.0.0", is_recovered=False
    )
    mock_stub.CheckIdempotencyCache.side_effect = [_hit(0), _acquired(), _acquired()]
    _accept_commits(mock_stub)

    @step
    async def place_order() -> str:
        raise AssertionError("a cache hit must not execute")

    @step
    async def a() -> str:
        await asyncio.sleep(0)
        return "a"

    @step
    async def b() -> str:
        return "b"

    @workflow(version="1.0.0")
    async def wf() -> List[str]:
        await place_order()
        return list(await asyncio.gather(a(), b()))

    assert asyncio.run(wf()) == ["a", "b"]
    sent = _sent(mock_stub)
    assert sent[0] == (1, "place_order", False)
    assert sorted(sent[1:]) == [(2, "a", True), (3, "b", True)]


def test_threads_claim_distinct_consecutive_positions(
    mock_stub: MagicMock, mock_client: None
) -> None:
    import threading

    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(), session_id="sess-threads", workflow_version="1.0.0"
    )
    start = threading.Barrier(16)
    claimed: List[int] = []

    def claim() -> None:
        start.wait(timeout=5)
        for _ in range(100):
            claimed.append(ctx.begin_call())
            ctx.end_call()

    threads = [threading.Thread(target=claim) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(claimed) == list(range(1, 1601))
    assert ctx._in_flight == 0


def test_a_record_the_engine_refuses_is_dropped(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """Only failures that can pass on a retry keep the record. One the engine
    will never accept would fail every later step of the session, so it is
    dropped, the run continues from the session's real position, and says so."""
    import grpc

    class Refused(grpc.RpcError):  # type: ignore[misc]
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.INVALID_ARGUMENT

    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(), session_id="sess-refused", workflow_version="1.0.0"
    )
    ctx.sequence = 3
    ctx.defer_hit_record(3, "place_order", {"result": "ORD-1"}, None)
    mock_stub.CommitStep.side_effect = Refused()
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(next_cursor="")

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert ctx.begin_call() == 3

    # Only the tail from the record's position was read.
    assert mock_stub.GetGraph.call_args[0][0].cursor == "3"
    assert not ctx.history_known
    assert "the engine refused it (INVALID_ARGUMENT)" in caplog.text


def test_a_failed_position_read_keeps_the_record(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The session's position is read before anything changes, so a failed read
    leaves the record in place for the next step to try again."""
    import grpc

    class Code(grpc.RpcError):  # type: ignore[misc]
        def __init__(self, code: grpc.StatusCode) -> None:
            self._code = code

        def code(self) -> grpc.StatusCode:
            return self._code

    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="sess-read",
        workflow_version="1.0.0",
        history_known=True,
    )
    ctx.sequence = 1
    ctx.defer_hit_record(1, "place_order", {"result": "ORD-1"}, None)
    mock_stub.CommitStep.side_effect = [
        Code(grpc.StatusCode.FAILED_PRECONDITION),
        service_pb2.CommitStepResponse(session_id="sess-read", next_sequence=2),
    ]
    mock_stub.GetGraph.side_effect = Code(grpc.StatusCode.UNAVAILABLE)

    with pytest.raises(grpc.RpcError):
        ctx.begin_call()
    assert ctx.sequence == 1 and ctx.history_known and ctx._in_flight == 0

    assert ctx.begin_call() == 2
    sent = _sent(mock_stub)
    assert sent == [(1, "place_order", False)] * 2


def test_a_record_that_cannot_be_sent_is_dropped(
    mock_stub: MagicMock, mock_client: None, caplog: Any
) -> None:
    """A failure before the record reaches the engine, such as a payload that does
    not serialize, fails the same way on every retry. Keeping the record would
    fail every later step of the run, so it is dropped like a refused one."""
    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(),
        session_id="sess-unsendable",
        workflow_version="1.0.0",
        history_known=True,
    )
    ctx.sequence = 2
    ctx.defer_hit_record(2, "place_order", {"result": "ORD-1"}, None)
    mock_stub.CommitStep.side_effect = TypeError("cannot serialize")
    mock_stub.GetGraph.return_value = service_pb2.GetGraphResponse(next_cursor="")

    with caplog.at_level("WARNING", logger="cellaflow.context"):
        assert ctx.begin_call() == 2
    ctx.end_call()
    assert ctx.begin_call() == 3
    assert mock_stub.CommitStep.call_count == 1
    assert "it could not be sent (TypeError: cannot serialize)" in caplog.text


def test_a_context_built_by_hand_does_not_record_hits(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """A context that never read its session's history cannot know a hit's position
    is free, so recording is opt-in."""
    from cellaflow.client import CellaflowClient
    from cellaflow.context import WorkflowContext

    ctx = WorkflowContext(
        client=CellaflowClient(), session_id="sess-hand", workflow_version="1.0.0"
    )
    seq = ctx.begin_call()
    ctx.take_hit(seq, "place_order", {"result": "ORD-1"}, 0, None)
    ctx.end_call()
    assert ctx.begin_call() == 1
    mock_stub.CommitStep.assert_not_called()


def test_a_resumed_workflow_replays_every_page_of_history(
    mock_stub: MagicMock, mock_client: None
) -> None:
    """The engine returns history a page at a time. A step on a later page that was
    not replayed would run again."""
    from cellaflow.v1 import common_pb2

    mock_stub.StartSession.return_value = service_pb2.StartSessionResponse(
        session_id="sess-long", version="1.0.0", is_recovered=True
    )

    def page(req: Any, **_: Any) -> Any:
        start = int(req.cursor) if req.cursor else 1
        stop = min(start + 100, 251)
        return service_pb2.GetGraphResponse(
            steps=[
                common_pb2.StepResult(
                    sequence=i, name="tick", output_payload=serialize({"result": i})
                )
                for i in range(start, stop)
            ],
            next_cursor=str(stop) if stop <= 250 else "",
        )

    mock_stub.GetGraph.side_effect = page

    @step
    def tick() -> int:
        raise AssertionError("a recorded step must replay, not run")

    @workflow(version="1.0.0")
    def wf() -> List[int]:
        return [tick() for _ in range(250)]

    assert wf(_session_id="sess-long") == list(range(1, 251))  # type: ignore[call-arg]
    mock_stub.CommitStep.assert_not_called()
