"""Picks the model the agent runs on. Not part of the Cellaflow integration.

With `OPENAI_API_KEY` set, that is `gpt-4o-mini` through CrewAI's own `LLM`.
Without one it is the scripted model below, which returns a fixed sequence of
turns so the example runs for anyone. Either way the agent really calls the
tools, and the leased tool really takes a lease: only the thing choosing the
tools is faked.

The two take different routes through CrewAI. A model that advertises function
calling is handed tool schemas; one that does not, which includes this scripted
one, gets CrewAI's ReAct prompt and answers in its `Action:` / `Action Input:`
text format. The tool itself is invoked the same way in both.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, List, Tuple

from crewai import LLM
from crewai.llms.base_llm import BaseLLM

OPENAI_MODEL = "gpt-4o-mini"


def _action(tool_name: str, args: dict, thought: str) -> str:
    return f"Thought: {thought}\nAction: {tool_name}\nAction Input: {json.dumps(args)}"


class ScriptedLLM(BaseLLM):
    """Replays `turns` in order, ignoring whatever it is sent."""

    turns: List[str]
    cursor: int = 0

    def call(self, messages: Any, **kwargs: Any) -> str:
        turn = self.turns[min(self.cursor, len(self.turns) - 1)]
        self.cursor += 1
        return turn


def build_model(order_id: str) -> Tuple[Any, str]:
    """Returns the model and a label naming it, for the agent's banner."""
    if os.environ.get("OPENAI_API_KEY"):
        return LLM(model=OPENAI_MODEL), OPENAI_MODEL

    # CrewAI warns on every turn that it is estimating the token count, which it
    # is, because a scripted model publishes no tokeniser. True and irrelevant
    # here, and three of them bury the two lines that matter.
    logging.getLogger("crewai.llms.base_llm").setLevel(logging.ERROR)

    scripted = ScriptedLLM(
        model="scripted",
        turns=[
            _action(
                "lookup_order",
                {"order_id": order_id},
                "I should look the order up before sending anything.",
            ),
            _action(
                "dispatch_order",
                {"order_id": order_id, "courier": "DHL"},
                "It is packed, so I can dispatch it.",
            ),
            f"Thought: I now know the final answer\nFinal Answer: Dispatched {order_id} with DHL.",
        ],
    )
    return scripted, "scripted (no OPENAI_API_KEY set)"
