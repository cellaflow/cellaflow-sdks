import grpc
from typing import Any, Dict, List, Optional, Tuple, cast

from cellaflow.v1 import service_pb2, service_pb2_grpc
from cellaflow.v1 import common_pb2, idempotency_pb2
from cellaflow.serialization import serialize, deserialize


class CellaflowClient:
    """
    gRPC Client for the Cellaflow Engine.
    Handles communication with the engine and strictly uses
    MessagePack for state payloads.
    """

    def __init__(self, target: str = "localhost:50051", secure: bool = False) -> None:
        if secure:
            self.channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())
        else:
            self.channel = grpc.insecure_channel(target)
        self.stub = service_pb2_grpc.WorkflowEngineServiceStub(self.channel)

    def start_session(
        self, workflow_id: str, version: str, session_id: Optional[str] = None
    ) -> service_pb2.StartSessionResponse:
        req = service_pb2.StartSessionRequest(
            workflow_id=workflow_id,
            version=version,
        )
        if session_id:
            req.session_id = session_id

        return cast(service_pb2.StartSessionResponse, self.stub.StartSession(req))

    def commit_step(
        self,
        session_id: str,
        sequence: int,
        name: str,
        status: "common_pb2.StepStatus.ValueType",
        output_payload: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        idempotency_fencing_token: Optional[int] = None,
    ) -> service_pb2.CommitStepResponse:
        # Strictly serialize dict to MessagePack
        serialized_state = serialize(output_payload)

        step_result = common_pb2.StepResult(
            sequence=sequence,
            name=name,
            status=status,
            output_payload=serialized_state,
        )

        req = service_pb2.CommitStepRequest(
            session_id=session_id,
            step_result=step_result,
        )
        if idempotency_key is not None:
            req.idempotency_key = idempotency_key
            if idempotency_fencing_token is None:
                raise ValueError(
                    "idempotency_fencing_token required if idempotency_key is set"
                )
            req.idempotency_fencing_token = idempotency_fencing_token

        return cast(service_pb2.CommitStepResponse, self.stub.CommitStep(req))

    def get_graph(
        self, session_id: str, limit: Optional[int] = None, cursor: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Returns a tuple of (list of deserialized step results, next_cursor).
        Each step result is a dictionary representation of the StepResult proto,
        with the output_payload fully deserialized into a Python dictionary.
        """
        req = service_pb2.GetGraphRequest(session_id=session_id)
        if limit is not None:
            req.limit = limit
        if cursor is not None:
            req.cursor = cursor

        resp: service_pb2.GetGraphResponse = self.stub.GetGraph(req)

        results = []
        for step in resp.steps:
            results.append(
                {
                    "sequence": step.sequence,
                    "name": step.name,
                    "status": step.status,
                    "output_payload": deserialize(step.output_payload),
                    "idempotency_key": (
                        step.idempotency_key
                        if step.HasField("idempotency_key")
                        else None
                    ),
                }
            )

        next_cursor = resp.next_cursor if resp.next_cursor else None
        return results, next_cursor

    def check_idempotency_cache(
        self,
        agent_id: str,
        idempotency_key: str,
        wait_timeout_ms: Optional[int] = None,
        lease_ttl_ms: Optional[int] = None,
        session_id: Optional[str] = None,
        sequence: Optional[int] = None,
    ) -> idempotency_pb2.CheckCacheResponse:
        """Arbitrates the lease for `idempotency_key`.

        Supplying `session_id` also asks the engine for that session's committed
        position, returned as `current_sequence` on every status. The idempotency
        key is opaque to the engine, so the session cannot be inferred from it —
        without this the engine has nothing to answer from.

        Supplying `sequence` — the position this caller intends to write to —
        additionally lets the engine refuse a lease that would authorise a side
        effect at an already-committed position, instead of rejecting the commit
        afterwards once the side effect has happened. Raises `FAILED_PRECONDITION`
        when refused. Both fields are optional on the wire; omitting `sequence`
        keeps the position unguarded.
        """
        req = idempotency_pb2.CheckCacheRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
        )
        if wait_timeout_ms is not None:
            req.wait_timeout_ms = wait_timeout_ms
        if lease_ttl_ms is not None:
            req.lease_ttl_ms = lease_ttl_ms
        if session_id is not None:
            req.session_id = session_id
        if sequence is not None:
            req.sequence = sequence

        return cast(
            idempotency_pb2.CheckCacheResponse,
            self.stub.CheckIdempotencyCache(req),
        )

    def renew_lease(
        self,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        extend_ms: int,
    ) -> idempotency_pb2.RenewLeaseResponse:
        req = idempotency_pb2.RenewLeaseRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
            fencing_token=fencing_token,
            extend_ms=extend_ms,
        )

        return cast(
            idempotency_pb2.RenewLeaseResponse,
            self.stub.RenewLease(req),
        )

    def release_lease(
        self,
        agent_id: str,
        idempotency_key: str,
        fencing_token: int,
        reason: Optional[str] = None,
    ) -> idempotency_pb2.ReleaseLeaseResponse:
        req = idempotency_pb2.ReleaseLeaseRequest(
            agent_id=agent_id,
            idempotency_key=idempotency_key,
            fencing_token=fencing_token,
        )
        if reason:
            req.reason = reason

        return cast(
            idempotency_pb2.ReleaseLeaseResponse,
            self.stub.ReleaseLease(req),
        )

    def close(self) -> None:
        self.channel.close()
