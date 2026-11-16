"""Many callers waiting on one shared call against a real engine: the engine
holds each request until the holder commits, so waiting costs about one request
per caller, not one per caller per second."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import threading
import time
import uuid
from typing import Any, List

import pytest

from cellaflow import IdempotencyScope, durable_tools, tool
from cellaflow.client import CellaflowClient

WAITERS = 100
#: Long enough that a caller polling once a second would ask three times or more.
HOLD_S = 3.0
GIVE_UP_S = 60.0


def test_100_callers_on_one_shared_call_get_the_holders_result_asking_about_once_each(
    target: str, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    order_id = f"order-{uuid.uuid4().hex[:8]}"

    # Count the requests that arbitrate the call's own key. The lease that
    # records a received result in each caller's session has a key of its own.
    asked: List[str] = []
    asked_lock = threading.Lock()
    original = CellaflowClient.check_idempotency_cache

    def counting(self: CellaflowClient, *args: Any, **kwargs: Any) -> Any:
        key = kwargs.get("idempotency_key", args[1] if len(args) > 1 else "")
        if key.startswith("shared:"):
            with asked_lock:
                asked.append(key)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(CellaflowClient, "check_idempotency_cache", counting)

    runs = 0
    holding = threading.Event()
    finish = threading.Event()

    @tool(
        tool_name="charge_card",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
    )
    def charge_card(order_id: str) -> str:
        nonlocal runs
        runs += 1
        holding.set()
        finish.wait(GIVE_UP_S)
        return f"charged-by-{runs}"

    results: List[str] = []
    errors: List[BaseException] = []

    def call(name: str) -> None:
        try:
            with durable_tools(
                f"{name}-{uuid.uuid4().hex[:8]}",
                target=target,
                coordination_id=order_id,
            ):
                results.append(charge_card(order_id))
        except BaseException as exc:  # reported below, not lost in the thread
            errors.append(exc)

    holder = threading.Thread(target=call, args=("holder",), daemon=True)
    waiters = [
        threading.Thread(target=call, args=(f"waiter-{i}",), daemon=True)
        for i in range(WAITERS)
    ]
    holder.start()
    try:
        assert holding.wait(GIVE_UP_S), "the holder never started the call"
        asked_before = len(asked)
        for waiter in waiters:
            waiter.start()
        time.sleep(HOLD_S)
    finally:
        finish.set()
    for thread in [holder, *waiters]:
        if thread.ident is None:
            continue
        thread.join(GIVE_UP_S)
        assert not thread.is_alive(), "a caller never returned"

    assert errors == []
    assert runs == 1, "the body ran more than once"
    assert set(results) == {"charged-by-1"} and len(results) == WAITERS + 1
    waiting = len(asked) - asked_before
    with capsys.disabled():
        print(f"\n  {WAITERS} waiters made {waiting} requests in a {HOLD_S:.0f} s hold")
    # Polling once a second would be at least three per waiter. Waiting in the
    # engine is one, answered when the holder commits; the slack allows a few
    # that started before the engine held them.
    assert waiting <= WAITERS * 1.5, f"{WAITERS} waiters made {waiting} requests"
