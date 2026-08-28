"""The agent implementations the demo compares.

All of them do the same job: refund one customer exactly once. They differ in how
they reach the payment gateway -- not at all, through a shared session, or through
a shared coordination domain.

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


# ---------------------------------------------------------------------------
# Variant 3: replicas that disagree about the arguments
# ---------------------------------------------------------------------------
#
# Identical replicas converge on their own: same inputs, same derived key, one
# winner. But replicas that *reason* -- an LLM deciding a refund amount -- can
# reach different answers from the same ticket. Different arguments hash to
# different idempotency keys, so the engine sees two unrelated operations and
# neither blocks the other on the key alone.
#
# What they do share is the graph position they are both about to write. That is
# what the engine arbitrates, and it is why the loser is stopped *before* it
# charges rather than rejected afterwards.


def run_divergent_agent(
    agent_id: str,
    session_id: str,
    ticket_id: str,
    amount_cents: int,
    barrier: BarrierT | None = None,
) -> Dict[str, Any]:
    """Same workflow as the coordinated agent, but each replica brings its own amount."""
    from cellaflow.decorators import DivergentStepError

    handle_refund_request = _build_coordinated_workflow(agent_id)

    _wait_for_start(barrier)
    started = time.time()
    try:
        result = handle_refund_request(ticket_id, amount_cents, _session_id=session_id)
        return {
            "agent_id": agent_id,
            "ok": True,
            "refused": False,
            "amount_cents": amount_cents,
            "confirmation_id": result["confirmation_id"],
            "charged_by": result["charged_by"],
            "elapsed_s": round(time.time() - started, 3),
        }
    except DivergentStepError as exc:
        # Refused before the tool body ran. Nothing reached the gateway.
        return {
            "agent_id": agent_id,
            "ok": False,
            "refused": True,
            "amount_cents": amount_cents,
            "error": str(exc),
            "elapsed_s": round(time.time() - started, 3),
        }
    except Exception as exc:  # pragma: no cover - demo diagnostics
        return {
            "agent_id": agent_id,
            "ok": False,
            "refused": False,
            "amount_cents": amount_cents,
            "error": f"{type(exc).__name__}: {exc}",
            "trace": traceback.format_exc(limit=5),
            "elapsed_s": round(time.time() - started, 3),
        }


# ---------------------------------------------------------------------------
# Variant 4: different agents, different workflows, different sessions
# ---------------------------------------------------------------------------
#
# The variants above are replicas of one agent sharing one session. This is the
# other shape: three genuinely different agents, each with its own workflow, its
# own version and its own session, that happen to reach for the same external
# operation.
#
# They share no session and therefore no graph position, so none of the machinery
# above applies. What makes them converge is SCOPE_SHARED, whose derived key drops
# both session and workflow version and keys on a coordination domain the caller
# names instead.


def run_shared_agent(
    agent_id: str,
    workflow_name: str,
    version: str,
    coordination_id: str,
    ticket_id: str,
    amount_cents: int,
    barrier: BarrierT | None = None,
) -> Dict[str, Any]:
    """One of several unrelated agents that must not double-refund the customer."""
    from cellaflow.decorators import tool, workflow
    from cellaflow.idempotency import IdempotencyScope

    @tool(
        agent_id=agent_id,
        # Every agent must pin the SAME tool_name. It defaults to the function
        # name, and these agents are different functions -- so without this they
        # would derive different keys and never converge. This is the footgun.
        tool_name="issue_refund",
        scope=IdempotencyScope.SCOPE_SHARED,
    )
    def issue_refund_step(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
        return gateway.issue_refund(ticket_id, amount_cents, charged_by=agent_id)

    @workflow(version=version, target=ENGINE_TARGET)
    def agent_workflow(ticket_id: str, amount_cents: int) -> Dict[str, Any]:
        return issue_refund_step(ticket_id, amount_cents)

    agent_workflow.__name__ = workflow_name

    _wait_for_start(barrier)
    started = time.time()
    try:
        # No _session_id: each agent gets its own fresh session, which is the
        # point. _coordination_id is what they share instead.
        result = agent_workflow(
            ticket_id, amount_cents, _coordination_id=coordination_id
        )
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
            "trace": traceback.format_exc(limit=5),
            "elapsed_s": round(time.time() - started, 3),
        }
