"""A LangGraph agent whose node moves money, killed mid-run and resumed.

The other variants in this demo call a leased tool directly. This one puts the
same tool inside a LangGraph node, which is where a real agent's side effects
live -- and it is the case LangGraph itself has no answer for. Its checkpointer
makes the graph's *state* durable; nothing in it makes a node's *side effect*
happen once.

The run is deliberately killed after the gateway is charged and before the
checkpoint recording it lands, which is the worst moment for a crash: the money
has moved and nothing durable says so. On resume, LangGraph re-runs the pending
node, so the tool is reached a second time and must decline to charge again.

Each phase runs in its own OS process, so the resume genuinely starts from cold
-- no in-memory state carries over, exactly as a pod restart would.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict

import gateway

from cellaflow import CellaflowSaver, durable_tools, tool
from langgraph.graph import StateGraph, START, END

ENGINE_TARGET = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")

#: Set on the child process to make the node die after the charge.
CRASH_ENV = "CELLAFLOW_DEMO_CRASH"


@dataclass
class RefundState:
    ticket_id: str = ""
    amount_cents: int = 0
    agent_id: str = ""
    receipt: Dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@tool(tool_name="issue_refund")
def issue_refund(ticket_id: str, amount_cents: int, agent_id: str) -> Dict[str, Any]:
    """The irreversible step. Leased, so it happens at most once."""
    return gateway.issue_refund(ticket_id, amount_cents, charged_by=agent_id)


def refund_node(state: RefundState) -> Dict[str, Any]:
    receipt = issue_refund(state.ticket_id, state.amount_cents, state.agent_id)

    if os.environ.get(CRASH_ENV):
        # The money has moved. This process now dies without recording it --
        # the crash window a checkpointer alone cannot close.
        os._exit(17)

    return {"receipt": receipt, "notes": state.notes + ["refunded"]}


def confirm_node(state: RefundState) -> Dict[str, Any]:
    return {"notes": state.notes + [f"confirmed {state.receipt['confirmation_id']}"]}


def build_agent() -> Any:
    graph = StateGraph(RefundState)
    graph.add_node("refund", refund_node)
    graph.add_node("confirm", confirm_node)
    graph.add_edge(START, "refund")
    graph.add_edge("refund", "confirm")
    graph.add_edge("confirm", END)
    return graph.compile(checkpointer=CellaflowSaver(target=ENGINE_TARGET))


def run_phase(thread_id: str, ticket_id: str, amount_cents: int, agent_id: str) -> Dict[str, Any]:
    """Starts the run, or resumes it if the thread already has one in flight.

    `durable_tools` is what makes the node's tool call leased. Without it the
    tool would still run, but its lease would be scoped to a session that
    changes every process -- so the resumed run would derive a different key,
    match nothing, and charge the customer a second time.
    """
    app = build_agent()
    config = {"configurable": {"thread_id": thread_id}}

    with durable_tools(config, target=ENGINE_TARGET):
        existing = app.get_state(config)
        if existing.next:
            # A previous process left this thread mid-flight. Passing None
            # resumes it; passing an input would start a second run instead.
            final = app.invoke(None, config)
        else:
            final = app.invoke(
                RefundState(
                    ticket_id=ticket_id,
                    amount_cents=amount_cents,
                    agent_id=agent_id,
                ),
                config,
            )

    receipt = final.get("receipt") or {}
    return {
        "agent_id": agent_id,
        "ok": True,
        "confirmation_id": receipt.get("confirmation_id", "-"),
        "charged_by": receipt.get("charged_by", "-"),
        "notes": final.get("notes", []),
    }
