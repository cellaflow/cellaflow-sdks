"""A LangGraph agent whose node places an order, killed mid-run and resumed.

The other variants in this demo call a leased tool directly. This one puts the
same tool inside a LangGraph node, which is where a real agent's side effects
live -- and it is the case LangGraph itself has no answer for. Its checkpointer
makes the graph's *state* durable; nothing in it makes a node's *side effect*
happen once.

The run is killed after the leased call returns and before LangGraph's checkpoint
for the node lands. The order has been placed and Cellaflow has recorded it, but
the graph does not know the node finished. On resume, LangGraph re-runs the
pending node, so the tool is reached a second time, and the engine answers it from
that record instead of letting it place the order again.

Each phase runs in its own OS process, so the resume genuinely starts from cold
-- no in-memory state carries over, exactly as a pod restart would.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict

import shop

from cellaflow import CellaflowSaver, durable_tools, tool
from langgraph.graph import StateGraph, START, END

ENGINE_TARGET = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")

#: Set on the child process to make the node die after the order is placed.
CRASH_ENV = "CELLAFLOW_DEMO_CRASH"


@dataclass
class OrderState:
    order_id: str = ""
    total_cents: int = 0
    agent_id: str = ""
    receipt: Dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@tool(tool_name="place_order")
def place_order(order_id: str, total_cents: int, agent_id: str) -> Dict[str, Any]:
    """The irreversible step. Leased, so a second caller is answered from its record."""
    return shop.place_order(order_id, total_cents, placed_by=agent_id)


def order_node(state: OrderState) -> Dict[str, Any]:
    receipt = place_order(state.order_id, state.total_cents, state.agent_id)

    if os.environ.get(CRASH_ENV):
        # The order is placed and Cellaflow has recorded it, but this process dies
        # before the node returns, so LangGraph's checkpoint never says the node
        # finished. On resume it runs the node again.
        os._exit(17)

    return {"receipt": receipt, "notes": state.notes + ["placed"]}


def confirm_node(state: OrderState) -> Dict[str, Any]:
    return {"notes": state.notes + [f"confirmed {state.receipt['confirmation_id']}"]}


def build_agent() -> Any:
    graph = StateGraph(OrderState)
    graph.add_node("order", order_node)
    graph.add_node("confirm", confirm_node)
    graph.add_edge(START, "order")
    graph.add_edge("order", "confirm")
    graph.add_edge("confirm", END)
    return graph.compile(checkpointer=CellaflowSaver(target=ENGINE_TARGET))


def run_phase(thread_id: str, order_id: str, total_cents: int, agent_id: str) -> Dict[str, Any]:
    """Starts the run, or resumes it if the thread already has one in flight.

    `durable_tools` is what makes the node's tool call leased. Without it the
    tool would still run, but its lease would be scoped to a session that
    changes every process -- so the resumed run would derive a different key,
    match nothing, and place the order a second time.
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
                OrderState(
                    order_id=order_id,
                    total_cents=total_cents,
                    agent_id=agent_id,
                ),
                config,
            )

    receipt = final.get("receipt") or {}
    return {
        "agent_id": agent_id,
        "ok": True,
        "confirmation_id": receipt.get("confirmation_id", "-"),
        "placed_by": receipt.get("placed_by", "-"),
        "notes": final.get("notes", []),
    }
