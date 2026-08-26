"""The two agent implementations the demo compares.

Both do the same job: handle one refund ticket. They differ only in whether they
coordinate through the CellaFlow engine before touching the payment gateway.

Each agent runs in its own OS process, so this is genuine cross-process
coordination -- not threads sharing a lock inside one interpreter.
"""

from __future__ import annotations

import os
import time
import traceback
from multiprocessing.synchronize import Barrier as BarrierT
from typing import Any, Dict

import gateway

ENGINE_TARGET = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")


def _wait_for_start(barrier: BarrierT | None) -> None:
    """Line every agent up so they enter the critical section simultaneously.

    Without this the processes drift apart by however long interpreter startup
    takes, which would let them accidentally serialize and make the naive run
    look better than it is.
    """
    if barrier is not None:
        barrier.wait()


# ---------------------------------------------------------------------------
# Variant 1: no coordination
# ---------------------------------------------------------------------------


def run_naive_agent(
    agent_id: str, ticket_id: str, amount_cents: int, barrier: BarrierT | None = None
) -> Dict[str, Any]:
    """Just does the work. This is what an agent looks like before durability."""
    _wait_for_start(barrier)
    started = time.time()
    try:
        result = gateway.issue_refund(ticket_id, amount_cents, charged_by=agent_id)
        return {
            "agent_id": agent_id,
            "ok": True,
            "confirmation_id": result["confirmation_id"],
            "charged_by": result["charged_by"],
            "elapsed_s": round(time.time() - started, 3),
        }
    except Exception as exc:  # pragma: no cover - demo diagnostics
        return {
            "agent_id": agent_id,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "trace": traceback.format_exc(limit=3),
            "elapsed_s": round(time.time() - started, 3),
        }


# ---------------------------------------------------------------------------
# Variant 2: coordinated through CellaFlow
# ---------------------------------------------------------------------------


def _build_coordinated_workflow(agent_id: str) -> Any:
    """Construct this agent's decorated workflow.

    The decorators are applied inside the function rather than at module scope
    because ``@tool`` takes ``agent_id`` at *decoration* time, and each replica
    needs its own identity. Building the workflow per process is what lets each
    one carry a distinct ``agent_id``.
    """
    from cellaflow.decorators import tool, workflow
    from cellaflow.idempotency import IdempotencyScope

    @tool(
        agent_id=agent_id,
        # Pinned explicitly: the derived idempotency key includes the tool name,
        # so every replica must agree on it or they will not contend at all.
        tool_name="issue_refund",
        # SESSION_WIDE means the key omits agent identity -- all replicas working
        # the same session collapse onto one key. That is the whole mechanism.
        scope=IdempotencyScope.SCOPE_SESSION_WIDE,
    )
    def issue_refund_step(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
        return gateway.issue_refund(ticket_id, amount_cents, charged_by=agent_id)

    @workflow(version="1.0.0", target=ENGINE_TARGET)
    def handle_refund_request(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
        return issue_refund_step(ticket_id, amount_cents)

    return handle_refund_request


def run_coordinated_agent(
    agent_id: str,
    session_id: str,
    ticket_id: str,
    amount_cents: int,
    barrier: BarrierT | None = None,
) -> Dict[str, Any]:
    """Same job, but arbitrated by the engine.

    Every replica passes the *same* ``_session_id``. Combined with SESSION_WIDE
    scope that makes all of them derive an identical idempotency key, so the
    engine can pick exactly one winner and hand the rest that winner's result.
    """
    handle_refund_request = _build_coordinated_workflow(agent_id)

    _wait_for_start(barrier)
    started = time.time()
    try:
        result = handle_refund_request(ticket_id, amount_cents, _session_id=session_id)
        if not isinstance(result, dict):
            return {
                "agent_id": agent_id,
                "ok": False,
                "error": f"unexpected result type {type(result).__name__}: {result!r}",
                "elapsed_s": round(time.time() - started, 3),
            }
        return {
            "agent_id": agent_id,
            "ok": True,
            "confirmation_id": result["confirmation_id"],
            # Who moved the money. Whether *this* process did is decided against
            # the ledger by the caller -- agent names repeat between runs, so
            # comparing to ``agent_id`` here would misreport on a resumed session.
            "charged_by": result["charged_by"],
            "elapsed_s": round(time.time() - started, 3),
        }
    except Exception as exc:  # pragma: no cover - demo diagnostics
        return {
            "agent_id": agent_id,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "trace": traceback.format_exc(limit=5),
            "elapsed_s": round(time.time() - started, 3),
        }
