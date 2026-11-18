#!/usr/bin/env python3
"""A CrewAI agent whose refund tool is leased by Cellaflow.

    pip install -r requirements.txt
    python agent.py

Set OPENAI_API_KEY to drive it with a real model. Without one it runs on the
scripted model in model.py, which calls the same tools in the same order.

The integration is two things:

    @leased(tool_name="issue_refund")   on the tool that moves money
    with durable_tools(thread_id):      around the kickoff

CrewAI runs a tool on the calling context on its synchronous path, which
`kickoff()` is, so the tool body reaches the session opened around it.
Nothing else below is Cellaflow-specific.
"""

from __future__ import annotations

import os
import uuid
from typing import List

from crewai import Agent, Crew, Task
from crewai.tools import tool as crewai_tool

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


@crewai_tool("lookup_order")
def lookup_order(order_id: str) -> str:
    """Returns the customer, amount in cents, and status of an order."""
    order = ORDERS.get(order_id)
    if order is None:
        return f"no such order: {order_id}"
    print(f"   lookup_order({order_id})")
    return f"{order['customer']}, {order['cents']} cents, {order['status']}"


# Read bottom-up: Cellaflow leases the call, CrewAI builds the schema the agent
# sees. Only the irreversible one is leased; `lookup_order` can be repeated all
# day without consequence, and a lease on it would buy nothing.
@crewai_tool("issue_refund")
@leased(tool_name="issue_refund")
def issue_refund(order_id: str, cents: int) -> str:
    """Refunds `cents` to the card that paid for `order_id`. Irreversible."""
    REFUNDS.append(order_id)
    print(f"   issue_refund({order_id}, {cents})  <- the leased call")
    return f"refunded {cents} cents for {order_id}"


def build_crew(model: object) -> Crew:
    agent = Agent(
        role="Refunds agent",
        goal="Refund the order the customer asks about, for the right amount.",
        backstory="You handle refunds for a shop. You check an order before refunding it.",
        tools=[lookup_order, issue_refund],
        llm=model,
        verbose=False,
    )
    task = Task(
        description=f"Refund order {ORDER_ID} in full.",
        expected_output="One sentence confirming the refund.",
        agent=agent,
    )
    return Crew(agents=[agent], tasks=[task], verbose=False)


def main() -> int:
    model, model_label = build_model(ORDER_ID)
    crew = build_crew(model)

    # Any id the application already uses for the conversation works. Cellaflow
    # derives the tool session from it, so the same thread always reaches the
    # same leases. CrewAI has no config object to carry it, so it is passed
    # straight in as a string.
    thread_id = f"refund-{uuid.uuid4().hex[:8]}"

    print(f"\n  engine: {ENGINE}")
    print(f"  model:  {model_label}")
    print(f"  thread: {thread_id}\n")

    with durable_tools(thread_id, target=ENGINE):
        result = crew.kickoff()

    print(f"\n  agent: {result}")
    print(f"  gateway hits: {len(REFUNDS)} refund(s)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
