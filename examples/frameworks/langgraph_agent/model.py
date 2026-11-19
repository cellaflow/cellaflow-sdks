"""Picks the model the agent runs on. Not part of the Cellaflow integration.

With `OPENAI_API_KEY` set, that is `gpt-4o-mini`. Without one it is the scripted
model below, which replays a fixed sequence of assistant turns so the example
runs for anyone. Either way the agent really calls the tools, and the tool
really takes a lease: only the thing choosing the tools is faked.
"""

from __future__ import annotations

import os
from typing import Any, List, Optional, Sequence, Tuple

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

OPENAI_MODEL = "gpt-4o-mini"


class ScriptedChatModel(BaseChatModel):
    """Replays `turns` in order, ignoring whatever it is sent."""

    turns: List[AIMessage]
    cursor: int = 0

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> "ScriptedChatModel":
        # The script already names the tools it calls, so there is nothing to
        # bind. create_react_agent calls this whatever model it is given.
        return self

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        turn = self.turns[min(self.cursor, len(self.turns) - 1)]
        self.cursor += 1
        return ChatResult(generations=[ChatGeneration(message=turn)])


def build_model(order_id: str) -> Tuple[BaseChatModel, str]:
    """Returns the model and a label naming it, for the banner the agent prints."""
    if os.environ.get("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=OPENAI_MODEL, temperature=0), OPENAI_MODEL

    scripted = ScriptedChatModel(
        turns=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup_order",
                        "args": {"order_id": order_id},
                        "id": "call_lookup",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "dispatch_order",
                        "args": {"order_id": order_id, "courier": "DHL"},
                        "id": "call_dispatch",
                    }
                ],
            ),
            AIMessage(content=f"Dispatched {order_id} with DHL."),
        ]
    )
    return scripted, "scripted (no OPENAI_API_KEY set)"
