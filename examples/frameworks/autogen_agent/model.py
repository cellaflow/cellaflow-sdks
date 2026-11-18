"""Picks the model client the agent runs on. Not part of the Cellaflow integration.

With `OPENAI_API_KEY` set, that is `gpt-4o-mini` through
`OpenAIChatCompletionClient`. Without one it is AutoGen's own
`ReplayChatCompletionClient`, replaying a fixed sequence of turns so the example
runs for anyone. Either way the agent really calls the tools, and the leased
tool really takes a lease: only the thing choosing the tools is faked.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Tuple

from autogen_core import EVENT_LOGGER_NAME, FunctionCall
from autogen_core.models import ChatCompletionClient, CreateResult, ModelInfo, RequestUsage
from autogen_ext.models.replay import ReplayChatCompletionClient

OPENAI_MODEL = "gpt-4o-mini"


def _call(name: str, args: dict, call_id: str) -> CreateResult:
    """One scripted turn in which the model asks for a tool."""
    return CreateResult(
        content=[FunctionCall(id=call_id, name=name, arguments=json.dumps(args))],
        finish_reason="function_calls",
        usage=RequestUsage(prompt_tokens=0, completion_tokens=0),
        cached=False,
    )


def build_model(order_id: str) -> Tuple[ChatCompletionClient, str]:
    """Returns the model client and a label naming it, for the agent's banner."""
    if os.environ.get("OPENAI_API_KEY"):
        from autogen_ext.models.openai import OpenAIChatCompletionClient

        return OpenAIChatCompletionClient(model=OPENAI_MODEL), OPENAI_MODEL

    # The replay client warns on every turn that it ignores `tool_choice` and
    # that it counted tokens on string content only. Both are true and neither
    # says anything about this example, so they are quietened rather than left
    # to bury the two lines that matter.
    logging.getLogger(EVENT_LOGGER_NAME).setLevel(logging.ERROR)

    scripted = ReplayChatCompletionClient(
        [
            _call("lookup_order", {"order_id": order_id}, "call_lookup"),
            _call("issue_refund", {"order_id": order_id, "cents": 2499}, "call_refund"),
            f"Refunded {order_id} in full.",
        ],
        model_info=ModelInfo(
            vision=False,
            function_calling=True,
            json_output=False,
            family="unknown",
            structured_output=False,
        ),
    )
    return scripted, "scripted (no OPENAI_API_KEY set)"
