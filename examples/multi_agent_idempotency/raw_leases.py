#!/usr/bin/env python3
"""The same coordination, one RPC at a time, with the decorators taken away.

``run_demo.py`` shows the outcome. This shows the mechanism: every call the SDK
would have made on your behalf, in order, with the engine's actual replies
printed. Run it when you want to know *why* only one agent charged the customer,
or when you are building a client for another language.

    python raw_leases.py
"""

from __future__ import annotations

import os
import sys
import uuid

import grpc

from cellaflow.client import CellaflowClient
from cellaflow.serialization import deserialize
from cellaflow.v1.common_pb2 import STEP_STATUS_SUCCESS
from cellaflow.v1.idempotency_pb2 import (
    CACHE_STATUS_ACQUIRED,
    CACHE_STATUS_HIT,
    CACHE_STATUS_IN_PROGRESS,
    CacheStatus,
    IdempotencyCommitStatus,
    RenewFailureReason,
)

TARGET = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")

CACHE_STATUS_NAMES = {v.number: v.name for v in CacheStatus.DESCRIPTOR.values}
COMMIT_STATUS_NAMES = {
    v.number: v.name for v in IdempotencyCommitStatus.DESCRIPTOR.values
}
RENEW_REASON_NAMES = {v.number: v.name for v in RenewFailureReason.DESCRIPTOR.values}


def step(n: int, who: str, what: str) -> None:
    print(f"\n{n}. [{who}] {what}")


def reply(text: str) -> None:
    print(f"   -> {text}")


def main() -> int:
    session_id = f"raw-{uuid.uuid4().hex[:8]}"
    # The key is what the two workers must agree on. The SDK derives this for you
    # from (session, version, scope, tool name, hashed inputs); here it is spelled
    # out so nothing is hidden. The engine treats it as an opaque string.
    key = f"{session_id}:1.0.0:session_wide:session_wide:issue_refund:demo"

    a = CellaflowClient(target=TARGET)
    b = CellaflowClient(target=TARGET)

    print(f"engine     {TARGET}")
    print(f"session    {session_id}")
    print(f"key        {key}")

    try:
        step(1, "worker-a", "StartSession")
        resp = a.start_session(
            workflow_id="raw_leases", version="1.0.0", session_id=session_id
        )
        reply(f"session_id={resp.session_id} is_recovered={resp.is_recovered}")

        step(2, "worker-a", "CheckIdempotencyCache -- first to ask")
        ra = a.check_idempotency_cache(agent_id="worker-a", idempotency_key=key)
        reply(
            f"status={CACHE_STATUS_NAMES[ra.status]} "
            f"fencing_token={ra.fencing_token} "
            f"heartbeat_interval_ms={ra.heartbeat_interval_ms}"
        )
        if ra.status != CACHE_STATUS_ACQUIRED:
            print("\nExpected ACQUIRED for the first caller. Aborting.")
            return 1
        token = ra.fencing_token
        print("   worker-a holds the lease and is the one that may execute.")

        step(3, "worker-b", "CheckIdempotencyCache -- same key, while a is working")
        rb = b.check_idempotency_cache(agent_id="worker-b", idempotency_key=key)
        reply(
            f"status={CACHE_STATUS_NAMES[rb.status]} "
            f"retry_after_ms={rb.retry_after_ms} "
            f"current_holder_id={rb.current_holder_id}"
        )
        if rb.status != CACHE_STATUS_IN_PROGRESS:
            print("\nExpected IN_PROGRESS for the second caller. Aborting.")
            return 1
        print("   worker-b is told to wait rather than duplicating the work.")

        step(4, "worker-a", "RenewLease -- what the background heartbeat does")
        rr = a.renew_lease(
            agent_id="worker-a",
            idempotency_key=key,
            fencing_token=token,
            extend_ms=30_000,
        )
        reply(f"renewed={rr.renewed} new_expires_at_ms={rr.new_expires_at_ms}")

        step(5, "worker-a", "CommitStep -- result and graph event, one transaction")
        payload = {"result": {"confirmation_id": "rf_deadbeef", "amount_cents": 2499}}
        rc = a.commit_step(
            session_id=session_id,
            sequence=1,
            name="issue_refund",
            status=STEP_STATUS_SUCCESS,
            output_payload=payload,
            idempotency_key=key,
            idempotency_fencing_token=token,
        )
        reply(
            f"idempotency_status="
            f"{COMMIT_STATUS_NAMES.get(rc.idempotency_status, rc.idempotency_status)} "
            f"next_sequence={rc.next_sequence}"
        )

        step(6, "worker-b", "CheckIdempotencyCache -- asking again after the commit")
        rb2 = b.check_idempotency_cache(agent_id="worker-b", idempotency_key=key)
        reply(f"status={CACHE_STATUS_NAMES[rb2.status]}")
        if rb2.status == CACHE_STATUS_HIT and rb2.cached_result.output_payload:
            cached = deserialize(rb2.cached_result.output_payload)
            reply(f"cached_result={cached}")
            print("   worker-b never called the payment gateway.")

        step(7, "worker-a", "RenewLease again -- the lease is gone now")
        rr2 = a.renew_lease(
            agent_id="worker-a",
            idempotency_key=key,
            fencing_token=token,
            extend_ms=30_000,
        )
        reply(
            f"renewed={rr2.renewed} "
            f"failure_reason="
            f"{RENEW_REASON_NAMES.get(rr2.failure_reason, rr2.failure_reason)}"
        )
        print("   Completion ends the lease, so a straggler cannot keep holding it.")

        step(8, "worker-c", "CommitStep with a stale fencing token -- fencing check")
        try:
            rc2 = a.commit_step(
                session_id=session_id,
                sequence=2,
                name="issue_refund",
                status=STEP_STATUS_SUCCESS,
                output_payload={"result": {"confirmation_id": "rf_duplicate"}},
                idempotency_key=key,
                # A token from an earlier, superseded attempt.
                idempotency_fencing_token=max(token - 1, 1),
            )
            status2 = COMMIT_STATUS_NAMES.get(
                rc2.idempotency_status, rc2.idempotency_status
            )
            reply(f"idempotency_status={status2} next_sequence={rc2.next_sequence}")
        except grpc.RpcError as exc:
            reply(f"rejected: {exc.code().name}: {exc.details()}")
        print("   A superseded worker cannot overwrite the committed result.")

        print("\nThat is the whole protocol: one CheckIdempotencyCache decides who")
        print("executes, a fencing token proves it, and CommitStep writes the result")
        print("and the graph event together.\n")
        return 0
    finally:
        a.close()
        b.close()


if __name__ == "__main__":
    sys.exit(main())
