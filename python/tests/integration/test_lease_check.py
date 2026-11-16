"""current_lease().check() inside a step's body, against a real engine: a holder
frozen after taking the lease, and resumed after another caller took the call
over, does not send."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import uuid

from cellaflow import IdempotencyScope, durable_tools, tool

TTL_MS = 3000


def holder(target: str, order_id: str, go_file: str) -> "subprocess.Popen[str]":
    """Starts a process that takes the lease on `order_id`'s refund, then waits
    for `go_file` before it would send: checking its lease first, as a step body
    should before anything irreversible. Prints what happened."""
    script = textwrap.dedent(
        f"""
        import os, time
        from cellaflow import IdempotencyScope, current_lease, durable_tools, tool

        @tool(tool_name="refund", scope=IdempotencyScope.SCOPE_SHARED,
              shared_on=["order_id"])
        def refund(order_id):
            print("holding", flush=True)
            while not os.path.exists({go_file!r}):
                time.sleep(0.02)
            current_lease().check()
            print("sent", flush=True)
            return "refunded-by-holder"

        try:
            with durable_tools("holder-{uuid.uuid4().hex[:8]}", target={target!r},
                               coordination_id={order_id!r}, lease_ttl_ms={TTL_MS}):
                refund({order_id!r})
            print("outcome:returned", flush=True)
        except Exception as exc:
            print("outcome:" + type(exc).__name__, flush=True)
        """
    )
    return subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
    )


def test_a_frozen_holder_resumed_after_a_takeover_does_not_send(target: str) -> None:
    order_id = f"order-{uuid.uuid4().hex[:8]}"
    go_file = os.path.join(tempfile.mkdtemp(prefix="cellaflow-"), "go")
    child = holder(target, order_id, go_file)
    try:
        assert child.stdout is not None
        assert "holding" in child.stdout.readline()
        child.send_signal(signal.SIGSTOP)

        # Another caller takes the call over once the frozen holder's lease
        # lapses.
        runs = []

        @tool(
            tool_name="refund",
            scope=IdempotencyScope.SCOPE_SHARED,
            shared_on=["order_id"],
            lease_ttl_ms=TTL_MS,
        )
        def refund(order_id: str) -> str:
            runs.append(order_id)
            return "refunded-by-taker"

        with durable_tools(
            f"taker-{uuid.uuid4().hex[:8]}", target=target, coordination_id=order_id
        ):
            assert refund(order_id) == "refunded-by-taker"
        assert runs == [order_id]

        # The holder wakes up and reaches the point where it would send.
        open(go_file, "w").close()
        child.send_signal(signal.SIGCONT)
        out, _ = child.communicate(timeout=30)
        assert "sent" not in out, out
        assert "outcome:LeaseLostError" in out, out
    finally:
        child.kill()
        child.wait()
        if child.stdout is not None:
            child.stdout.close()


def _workflow_holder(
    target: str, order_id: str, go_file: str, session: str, mode: str
) -> str:
    return textwrap.dedent(
        f"""
        import os, time
        from cellaflow import IdempotencyScope, step, workflow

        @step(tool_name="refund", scope=IdempotencyScope.SCOPE_SHARED,
              shared_on=["order_id"], lease_ttl_ms={TTL_MS})
        def refund(order_id):
            print("holding", flush=True)
            while not os.path.exists({go_file!r}):
                time.sleep(0.02)
            if {mode!r} == "return":
                return "refunded-by-holder"
            raise RuntimeError("gateway timeout")

        @step(tool_name="notify")
        def notify(msg):
            return "notified"

        @workflow(version="1.0.0", target={target!r})
        def agent(order_id):
            try:
                return refund(order_id)
            except Exception as e:
                return notify(type(e).__name__)

        try:
            out = agent({order_id!r}, _session_id={session!r},
                        _coordination_id={order_id!r})
            print("outcome:" + str(out), flush=True)
        except BaseException as e:
            print("outcome:" + type(e).__name__ + ": " + str(e)[:200], flush=True)
        """
    )


def test_a_workflow_that_carries_on_after_a_takeover_resumes(target: str) -> None:
    """The step another worker took over is recorded where it ran, so a resume
    of the workflow replays it there instead of finding the next step."""
    for mode in ("raise", "return"):
        order_id = f"order-{uuid.uuid4().hex[:8]}"
        session = f"wf-{uuid.uuid4().hex[:8]}"
        go_file = os.path.join(tempfile.mkdtemp(prefix="cellaflow-"), "go")
        script = _workflow_holder(target, order_id, go_file, session, mode)
        child = subprocess.Popen(
            [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
        )
        try:
            assert child.stdout is not None
            assert "holding" in child.stdout.readline()
            child.send_signal(signal.SIGSTOP)

            @tool(
                tool_name="refund",
                scope=IdempotencyScope.SCOPE_SHARED,
                shared_on=["order_id"],
                lease_ttl_ms=TTL_MS,
            )
            def refund(order_id: str) -> str:
                return "refunded-by-taker"

            with durable_tools(
                f"taker-{uuid.uuid4().hex[:8]}", target=target, coordination_id=order_id
            ):
                refund(order_id)
            open(go_file, "w").close()
            child.send_signal(signal.SIGCONT)
            first, _ = child.communicate(timeout=60)
            assert "outcome:notified" in first, (mode, first)
        finally:
            child.kill()
            child.wait()
            if child.stdout is not None:
                child.stdout.close()
        resumed = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60
        )
        assert "outcome:notified" in resumed.stdout, (mode, resumed.stdout)
