#!/usr/bin/env python3
"""A LangGraph agent whose refund tool is leased by Cellaflow.

    pip install -r requirements.txt
    python agent.py

Set OPENAI_API_KEY to drive it with a real model. Without one it runs on the
scripted model in model.py, which calls the same tools in the same order.

The integration is two things:

    @leased(tool_name="issue_refund")   on the tool that moves money
    with durable_tools(config):         around invoke

LangGraph copies the calling context into the executor it runs a node on, so a
tool called from the `tools` node reaches the session opened around `invoke`.
Nothing else below is Cellaflow-specific.
"""

from __future__ import annotations

import os
import uuid
from typing import Any, Dict, List

from langchain_core.tools import tool as langchain_tool
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from cellaflow import durable_tools
from cellaflow import tool as leased

from model import build_model

ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")
ORDER_ID = "ORD-4417"

ORDERS = {
    ORDER_ID: {"customer": "A. Okonkwo", "cents": 2499, "status": "delivered"},
}

# Appended to inside the tool body, so it counts what the payment gateway was
# actually asked to do. A leased call answered from its record never enters the
# body and never lands here.
REFUNDS: List[str] = []


@langchain_tool
def lookup_order(order_id: str) -> str:
    """Returns the customer, amount in cents, and status of an order."""
    order = ORDERS.get(order_id)
    if order is None:
        return f"no such order: {order_id}"
    print(f"   lookup_order({order_id})")
    return f"{order['customer']}, {order['cents']} cents, {order['status']}"


# Read bottom-up: Cellaflow leases the call, LangChain gives the model its
# schema. Only the irreversible one is leased; `lookup_order` can be repeated
# all day without consequence, and a lease on it would buy nothing.
@langchain_tool
@leased(tool_name="issue_refund")
def issue_refund(order_id: str, cents: int) -> str:
    """Refunds `cents` to the card that paid for `order_id`. Irreversible."""
    REFUNDS.append(order_id)
    print(f"   issue_refund({order_id}, {cents})  <- the leased call")
    return f"refunded {cents} cents for {order_id}"


TOOLS = [lookup_order, issue_refund]


def build_agent(model: Any) -> Any:
    """An ordinary two-node agent: the model, and the tools it asks for."""
    bound = model.bind_tools(TOOLS)

    def call_model(state: MessagesState) -> Dict[str, Any]:
        return {"messages": [bound.invoke(state["messages"])]}

    graph = StateGraph(MessagesState)
    graph.add_node("model", call_model)
    graph.add_node("tools", ToolNode(TOOLS))
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", tools_condition, {"tools": "tools", END: END})
    graph.add_edge("tools", "model")
    return graph.compile()


def main() -> int:
    model, model_label = build_model(ORDER_ID)
    app = build_agent(model)

    # Any id the application already uses for the conversation works. Cellaflow
    # derives the tool session from it, so the same thread always reaches the
    # same leases.
    config = {"configurable": {"thread_id": f"refund-{uuid.uuid4().hex[:8]}"}}

    print(f"\n  engine: {ENGINE}")
    print(f"  model:  {model_label}")
    print(f"  thread: {config['configurable']['thread_id']}\n")

    with durable_tools(config, target=ENGINE):
        result = app.invoke(
            {"messages": [("user", f"Refund order {ORDER_ID} in full.")]},
            config,
        )

    print(f"\n  agent: {result['messages'][-1].content}")
    print(f"  gateway hits: {len(REFUNDS)} refund(s)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
