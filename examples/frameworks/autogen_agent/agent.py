#!/usr/bin/env python3
"""An AutoGen AssistantAgent whose dispatch tool is leased by Cellaflow.

    pip install -r requirements.txt
    python agent.py

Set OPENAI_API_KEY to drive it with a real model. Without one it runs on the
replay client in model.py, which calls the same tools in the same order.

The integration is two things:

    @leased(tool_name="dispatch_order")  on the call you cannot take back
    with durable_tools(thread_id):      around the run

AutoGen hands a tool to an executor the caller's context does not reach, so the
session is found through Cellaflow's open-session registry instead. That works
as long as one session is open in the process. If you run several at once, see
the README: each tool has to say which session it belongs to.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import List

from autogen_agentchat.agents import AssistantAgent

from cellaflow import durable_tools
from cellaflow import tool as leased

from model import build_model

ENGINE = os.environ.get("CELLAFLOW_TARGET", "localhost:50051")
ORDER_ID = "ORD-4417"

ORDERS = {
    ORDER_ID: {"customer": "A. Okonkwo", "items": 3, "status": "packed"},
}

# Appended to inside the tool body, so it counts what the courier was actually
# asked to do. A leased call answered from its record never enters the body and
# never lands here.
DISPATCHES: List[str] = []


def lookup_order(order_id: str) -> str:
    """Returns the customer, item count, and status of an order."""
    order = ORDERS.get(order_id)
    if order is None:
        return f"no such order: {order_id}"
    print(f"   lookup_order({order_id})")
    return f"{order['customer']}, {order['items']} items, {order['status']}"


# Only the irreversible one is leased. `lookup_order` can be repeated all day
# without consequence, and a lease on it would buy nothing.
@leased(tool_name="dispatch_order")
def dispatch_order(order_id: str, courier: str) -> str:
    """Hands `order_id` to `courier` for delivery. Cannot be recalled."""
    DISPATCHES.append(order_id)
    print(f"   dispatch_order({order_id}, {courier})  <- the leased call")
    return f"dispatched {order_id} with {courier}"


async def main() -> int:
    model, model_label = build_model(ORDER_ID)

    agent = AssistantAgent(
        "dispatch",
        model_client=model,
        tools=[lookup_order, dispatch_order],
        system_message=(
            "You handle dispatch. Look the order up first, then dispatch it "
            "with courier DHL."
        ),
        # Default is 1, which stops after the first batch of tool calls. The
        # agent needs the lookup result before it can ask for the dispatch.
        max_tool_iterations=4,
        reflect_on_tool_use=True,
    )

    # Any id the application already uses for the conversation works. Cellaflow
    # derives the tool session from it, so the same thread always reaches the
    # same leases. AutoGen has no config object to carry it, so it is passed
    # straight in as a string.
    thread_id = f"dispatch-{uuid.uuid4().hex[:8]}"

    print(f"\n  engine: {ENGINE}")
    print(f"  model:  {model_label}")
    print(f"  thread: {thread_id}\n")

    with durable_tools(thread_id, target=ENGINE):
        result = await agent.run(task=f"Dispatch order {ORDER_ID} to the courier.")

    print(f"\n  agent: {result.messages[-1].content}")
    print(f"  courier hits: {len(DISPATCHES)} dispatch(es)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
